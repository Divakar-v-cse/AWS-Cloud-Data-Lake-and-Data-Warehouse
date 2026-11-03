"""FastAPI analytics service for the Divakar Cloud Data Lake & Warehouse.

Serves the dbt marts (`marts.*`), the gold-layer catalog and the pipeline
observability tables (`ops.*`) to the React dashboard, and exposes a mock
partner REST endpoint used to demonstrate API-based ingestion.

Run locally:  uvicorn api.main:app --host 0.0.0.0 --port 8100
"""
from __future__ import annotations

import json
import math
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from src.config import ROOT, settings
from src.db import get_engine

app = FastAPI(
    title="Divakar Cloud Data Lake & Data Warehouse API",
    version="1.0.0",
    description="Analytics API over the Redshift-compatible dimensional warehouse.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------
def _jsonable(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return str(value)
    return value


def fetch(sql: str, params: dict | None = None) -> list[dict]:
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = [dict(r._mapping) for r in result]
    return [{k: _jsonable(v) for k, v in row.items()} for row in rows]


def fetch_one(sql: str, params: dict | None = None) -> dict:
    rows = fetch(sql, params)
    return rows[0] if rows else {}


# ---------------------------------------------------------------------------
# Health / metadata
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    started = time.perf_counter()
    try:
        fetch_one("select 1 as ok")
        ok = True
        error = None
    except Exception as exc:  # noqa: BLE001
        ok = False
        error = f"{type(exc).__name__}: {exc}"
    return {
        "status": "ok" if ok else "degraded",
        "warehouse": f"{settings.warehouse_host}:{settings.warehouse_port}/{settings.warehouse_db}",
        "lake_backend": settings.lake_backend,
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        "error": error,
    }


@app.get("/api/meta")
def meta():
    counts = {}
    for label, sql in {
        "fact_orders": "select count(*) c from dwh.fact_orders",
        "fact_order_items": "select count(*) c from dwh.fact_order_items",
        "fact_payments": "select count(*) c from dwh.fact_payments",
        "fact_shipments": "select count(*) c from dwh.fact_shipments",
        "fact_returns": "select count(*) c from dwh.fact_returns",
        "dim_customer": "select count(*) c from dwh.dim_customer where is_current",
        "dim_product": "select count(*) c from dwh.dim_product where is_current",
        "dim_date": "select count(*) c from dwh.dim_date",
    }.items():
        try:
            counts[label] = fetch_one(sql).get("c")
        except Exception:  # noqa: BLE001
            counts[label] = None

    marts = []
    try:
        marts = [
            r["table_name"]
            for r in fetch(
                "select table_name from information_schema.tables "
                "where table_schema = 'marts' order by table_name"
            )
        ]
    except Exception:  # noqa: BLE001
        pass

    span = {}
    try:
        span = fetch_one(
            "select min(d.full_date)::text as min_date, max(d.full_date)::text as max_date "
            "from dwh.fact_orders o join dwh.dim_date d using (date_key)"
        )
    except Exception:  # noqa: BLE001
        pass

    return {
        "project": "Divakar Cloud Data Lake and Data Warehouse Project",
        "warehouse_engine": "PostgreSQL 17 (Redshift-compatible SQL)",
        "lake_backend": settings.lake_backend,
        "lake_root": str(settings.lake_root),
        "buckets": [settings.bronze_bucket, settings.silver_bucket, settings.gold_bucket],
        "counts": counts,
        "marts": marts,
        "date_range": span,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


# ---------------------------------------------------------------------------
# Executive
# ---------------------------------------------------------------------------
@app.get("/api/executive")
def executive():
    try:
        kpis = fetch_one("select * from marts.mart_executive_kpis")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, f"mart_executive_kpis unavailable: {exc}") from exc

    # Contract aliases for the Overview tiles, which bind to orders /
    # completed_orders / aov / customers / products / refunds. The mart exposes
    # richer, differently named columns, and its total_orders deliberately counts
    # only revenue-bearing orders (completed + returned = 105,567). The tile is
    # labelled "Orders", so feed it the full fact count (120,000) to keep the
    # Overview header consistent with the Orders page, and report the completed
    # subset next to it.
    counts = fetch_one(
        "select count(*) as all_orders, "
        "count(*) filter (where status = 'completed') as completed_orders "
        "from dwh.fact_orders"
    )
    kpis.update({
        "orders": counts["all_orders"],
        "completed_orders": counts["completed_orders"],
        "aov": kpis.get("avg_order_value"),
        "customers": kpis.get("total_customers"),
        "products": kpis.get("units_sold"),
        "refunds": kpis.get("total_refunds"),
    })

    # The mart stores the month key as year_month; the dashboard contract has
    # always called it order_month, so alias at the edge rather than renaming
    # a column the charts already bind to.
    trend = fetch(
        "select year_month as order_month, net_revenue, orders, aov "
        "from marts.mart_monthly_revenue order by year_month desc limit 12"
    )
    trend.reverse()
    return {"kpis": kpis, "trend": trend}


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------
@app.get("/api/sales/daily")
def sales_daily(days: int = Query(365, ge=7, le=2000)):
    return fetch(
        "select order_date, orders, customers as active_customers, gross_revenue, refunds, "
        "net_revenue, aov, revenue_7d_ma "
        "from marts.mart_daily_sales order by order_date desc limit :days",
        {"days": days},
    )[::-1]


@app.get("/api/sales/monthly")
def sales_monthly():
    return fetch(
        "select year_month as order_month, month_start, orders, customers, units_sold, "
        "gross_revenue, refunds, net_revenue, aov, revenue_growth_pct, cumulative_revenue "
        "from marts.mart_monthly_revenue order by year_month"
    )


@app.get("/api/sales/regional")
def sales_regional():
    return fetch("select * from marts.mart_regional_sales order by revenue desc")


@app.get("/api/sales/category")
def sales_category():
    return fetch(
        "select category, sum(revenue) as revenue, sum(units_sold) as units_sold, "
        "round(avg(margin_pct), 2) as margin_pct "
        "from marts.mart_product_performance group by 1 order by revenue desc"
    )


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------
@app.get("/api/customers/segments")
def customer_segments():
    return fetch(
        "select segment, count(*) as customers, round(sum(monetary), 2) as revenue, "
        "round(avg(monetary), 2) as avg_ltv, round(avg(orders), 2) as avg_orders, "
        "round(avg(recency_days), 1) as avg_recency_days "
        "from marts.mart_customer_rfm group by 1 order by revenue desc"
    )


@app.get("/api/customers/summary")
def customer_summary():
    return fetch_one(
        "select count(*) as customers, "
        "sum(case when is_repeat_customer then 1 else 0 end) as repeat_customers, "
        "round(100.0 * sum(case when is_repeat_customer then 1 else 0 end) / nullif(count(*),0), 2) "
        "  as repeat_rate_pct, "
        "round(avg(monetary), 2) as avg_ltv, round(sum(monetary), 2) as total_revenue, "
        "round(avg(orders), 2) as avg_orders "
        "from marts.mart_customer_rfm"
    )


@app.get("/api/customers/rfm")
def customer_rfm(
    segment: str | None = None,
    order_by: str = Query("monetary", pattern="^(monetary|orders|recency_days|predicted_clv)$"),
    limit: int = Query(50, ge=1, le=500),
):
    where = "where segment = :segment" if segment else ""
    return fetch(
        f"select customer_id, customer_name, region, state, segment, orders, "
        f"round(monetary,2) as monetary, round(avg_order_value,2) as avg_order_value, "
        f"recency_days, r_score, f_score, m_score, round(predicted_clv,2) as predicted_clv "
        f"from marts.mart_customer_rfm {where} order by {order_by} desc limit :limit",
        {"segment": segment, "limit": limit},
    )


@app.get("/api/customers/cohorts")
def customer_cohorts():
    rows = fetch(
        "select cohort_month, cohort_size, month_index, customers, retention_pct "
        "from marts.mart_cohort_retention order by cohort_month, month_index"
    )
    for r in rows:
        r["cohort_month"] = str(r["cohort_month"])[:10]
    return rows


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
@app.get("/api/products/performance")
def product_performance(
    category: str | None = None,
    order_by: str = Query("revenue", pattern="^(revenue|profit|units_sold|return_rate|margin_pct)$"),
    limit: int = Query(50, ge=1, le=500),
):
    where = "where category = :category" if category else ""
    # The mart already stores the rate as a percentage (return_rate_pct); the
    # sort alias exposed to clients is the shorter name.
    sort_col = {"return_rate": "return_rate_pct"}.get(order_by, order_by)
    return fetch(
        f"select product_id, product_name, category, subcategory, brand, units_sold, orders, "
        f"round(revenue,2) as revenue, round(profit,2) as profit, round(margin_pct,2) as margin_pct, "
        f"round(return_rate_pct, 2) as return_rate_pct, revenue_rank "
        f"from marts.mart_product_performance {where} order by {sort_col} desc limit :limit",
        {"category": category, "limit": limit},
    )


@app.get("/api/products/categories")
def product_categories():
    return fetch(
        "select category, count(*) as products, round(sum(revenue),2) as revenue, "
        "round(sum(profit),2) as profit, round(avg(margin_pct),2) as margin_pct "
        "from marts.mart_product_performance group by 1 order by revenue desc"
    )


# ---------------------------------------------------------------------------
# Orders / payments / returns
# ---------------------------------------------------------------------------
@app.get("/api/orders/funnel")
def order_funnel():
    return fetch("select * from marts.mart_order_funnel order by year_month")


@app.get("/api/orders/status")
def order_status():
    return fetch(
        "select status, count(*) as orders, round(sum(order_total),2) as order_value, "
        "round(100.0 * count(*) / sum(count(*)) over (), 2) as share_pct "
        "from dwh.fact_orders group by 1 order by orders desc"
    )


@app.get("/api/orders/recent")
def recent_orders(limit: int = Query(25, ge=1, le=200)):
    return fetch(
        "select o.order_id, d.full_date::text as order_date, "
        "c.first_name || ' ' || c.last_name as customer_name, o.status, "
        "o.region, o.item_count, round(o.order_total,2) as order_total "
        "from dwh.fact_orders o "
        "join dwh.dim_date d using (date_key) "
        "join dwh.dim_customer c on c.customer_sk = o.customer_sk "
        "order by o.order_ts desc limit :limit",
        {"limit": limit},
    )


@app.get("/api/payments")
def payments():
    return fetch("select * from marts.mart_payment_analytics order by attempts desc")


@app.get("/api/returns")
def returns():
    return fetch("select * from marts.mart_return_analytics order by refund_amount desc")


@app.get("/api/returns/monthly")
def returns_monthly():
    return fetch(
        "select d.year_month, count(*) as returns, round(sum(r.refund_amount),2) as refund_amount "
        "from dwh.fact_returns r join dwh.dim_date d using (date_key) "
        "group by 1 order by 1"
    )


@app.get("/api/fulfilment")
def fulfilment():
    return fetch(
        "select carrier, count(*) as shipments, "
        "round(avg(delivery_days),2) as avg_delivery_days, "
        "sum(case when is_late then 1 else 0 end) as late_shipments, "
        "round(100.0 * sum(case when is_late then 1 else 0 end) / nullif(count(*),0), 2) as late_rate_pct "
        "from dwh.fact_shipments group by 1 order by shipments desc"
    )


# ---------------------------------------------------------------------------
# Operations / observability
# ---------------------------------------------------------------------------
@app.get("/api/ops/runs")
def ops_runs(limit: int = Query(40, ge=1, le=500)):
    return fetch(
        "select run_id, pipeline, stage, source, status, started_at, finished_at, "
        "duration_sec, records_in, records_out, records_rejected, bytes_written, details "
        "from ops.pipeline_runs order by started_at desc, run_id desc limit :limit",
        {"limit": limit},
    )


@app.get("/api/ops/quality")
def ops_quality(limit: int = Query(200, ge=1, le=1000)):
    return fetch(
        "select dataset, check_name, status, failed_rows, total_rows, checked_at "
        "from ops.data_quality_results order by checked_at desc, result_id desc limit :limit",
        {"limit": limit},
    )


@app.get("/api/ops/quality/summary")
def ops_quality_summary():
    return fetch(
        "select dataset, count(*) as checks, "
        "sum(case when status = 'passed' then 1 else 0 end) as passed, "
        "sum(case when status <> 'passed' then 1 else 0 end) as failed "
        "from ops.data_quality_results group by 1 order by 1"
    )


@app.get("/api/ops/watermarks")
def ops_watermarks():
    return fetch("select dataset, last_value, updated_at from ops.watermarks order by dataset")


@app.get("/api/catalog")
def catalog():
    path = Path(settings.lake_root) / settings.gold_bucket / "_catalog.json"
    if not path.exists():
        raise HTTPException(404, "gold catalog not built yet - run python -m src.gold")
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Mock partner REST API (used by ingest --format api)
# ---------------------------------------------------------------------------
@app.get("/mock-api/{source}")
def mock_api(source: str, since: str | None = None, limit: int = Query(5000, ge=1, le=500000)):
    csv_path = Path(settings.raw_dir) / source / f"{source}.csv"
    json_path = Path(settings.raw_dir) / source / f"{source}.json"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    elif json_path.exists():
        df = pd.read_json(json_path, lines=True)
    else:
        raise HTTPException(404, f"unknown source '{source}'")

    ts_cols = ["updated_at", "payment_ts", "shipped_ts", "return_ts", "order_ts"]
    if since:
        for col in ts_cols:
            if col in df.columns:
                df = df[pd.to_datetime(df[col], errors="coerce") > pd.Timestamp(since)]
                break

    total = len(df)
    df = df.head(limit)
    records = json.loads(df.to_json(orient="records", date_format="iso"))
    return {"source": source, "count": len(records), "available": total, "records": records}


# ---------------------------------------------------------------------------
# Static dashboard (built React app)
# ---------------------------------------------------------------------------
DIST = ROOT / "frontend" / "dist"
if (DIST / "index.html").exists():
    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        candidate = DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(DIST / "index.html")
else:
    @app.get("/")
    def root():
        return JSONResponse(
            {
                "service": "Divakar Cloud Data Lake & Warehouse API",
                "docs": "/docs",
                "note": "Frontend bundle not built yet (npm run build in ./frontend).",
            }
        )
