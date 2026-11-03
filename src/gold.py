"""Gold layer: business-ready aggregates for BI.

Definitions (used consistently across gold, the warehouse and dbt)
-------------------------------------------------------------------
gross_revenue  sum(order_total) for orders whose status is completed or returned
refunds        sum(refund_amount) for approved/completed returns
net_revenue    gross_revenue - refunds
aov            net_revenue / shipped_orders
return_rate    returned units / units sold
active customer  a customer with >= 1 shipped order in the period

Each dataset is written as Parquet, partitioned by year where the grain is
time-based, with a catalog entry that mirrors AWS Glue Data Catalog metadata.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from . import s3client as lake
from .config import settings
from .db import ensure_ops_schema
from .monitoring import track_run
from .silver import read_silver, reset_caches

SHIPPED = ("completed", "returned")


def _load() -> dict[str, pd.DataFrame]:
    reset_caches()
    return {
        "customers": read_silver("customers"),
        "products": read_silver("products"),
        "orders": read_silver("orders"),
        "order_items": read_silver("order_items"),
        "payments": read_silver("payments"),
        "shipments": read_silver("shipments"),
        "returns": read_silver("returns"),
    }


def _refunds_by_order(returns: pd.DataFrame) -> pd.Series:
    valid = returns[returns["status"].isin(["completed", "approved"])]
    return valid.groupby("order_id")["refund_amount"].sum()


def build_daily_sales(t: dict) -> pd.DataFrame:
    o = t["orders"]
    refunds = _refunds_by_order(t["returns"])
    o = o.assign(refund=o["order_id"].map(refunds).fillna(0.0))
    o["is_shipped"] = o["status"].isin(SHIPPED)

    g = o.groupby("order_date", as_index=False).agg(
        orders=("order_id", "count"),
        completed_orders=("status", lambda s: int((s == "completed").sum())),
        cancelled_orders=("status", lambda s: int((s == "cancelled").sum())),
        returned_orders=("status", lambda s: int((s == "returned").sum())),
        pending_orders=("status", lambda s: int((s == "pending").sum())),
        units_sold=("item_count", "sum"),
        gross_revenue=("order_total", lambda s: 0.0),  # replaced below
        refunds=("refund", "sum"),
        active_customers=("customer_id", "nunique"),
    )
    shipped = o[o["is_shipped"]].groupby("order_date", as_index=False).agg(
        shipped_orders=("order_id", "count"),
        gross_revenue=("order_total", "sum"),
    )
    g = g.drop(columns=["gross_revenue"]).merge(shipped, on="order_date", how="left").fillna({"shipped_orders": 0, "gross_revenue": 0.0})
    g["net_revenue"] = (g["gross_revenue"] - g["refunds"]).round(2)
    g["aov"] = np.where(g["shipped_orders"] > 0, g["net_revenue"] / g["shipped_orders"], 0).round(2)
    g["order_date"] = pd.to_datetime(g["order_date"])
    g["year"] = g["order_date"].dt.year
    g = g.sort_values("order_date")
    g["revenue_7d_ma"] = g["net_revenue"].rolling(7, min_periods=1).mean().round(2)
    return g[["order_date", "year", "orders", "completed_orders", "cancelled_orders",
              "returned_orders", "pending_orders", "shipped_orders", "units_sold",
              "gross_revenue", "refunds", "net_revenue", "aov", "active_customers",
              "revenue_7d_ma"]]


def build_monthly_revenue(t: dict) -> pd.DataFrame:
    o = t["orders"]
    refunds = _refunds_by_order(t["returns"])
    o = o.assign(refund=o["order_id"].map(refunds).fillna(0.0))
    o["is_shipped"] = o["status"].isin(SHIPPED)

    m = o.groupby("order_month", as_index=False).agg(
        orders=("order_id", "count"),
        refunds=("refund", "sum"),
        customers=("customer_id", "nunique"),
    )
    shipped = o[o["is_shipped"]].groupby("order_month", as_index=False).agg(
        shipped_orders=("order_id", "count"), gross_revenue=("order_total", "sum"))
    m = m.merge(shipped, on="order_month", how="left").fillna(0.0)
    m["net_revenue"] = (m["gross_revenue"] - m["refunds"]).round(2)
    m["aov"] = np.where(m["shipped_orders"] > 0, m["net_revenue"] / m["shipped_orders"], 0).round(2)
    m = m.sort_values("order_month").reset_index(drop=True)
    m["revenue_growth_pct"] = (m["net_revenue"].pct_change() * 100).round(2).fillna(0.0)
    m["year"] = m["order_month"].str[:4].astype(int)
    return m[["order_month", "year", "orders", "shipped_orders", "customers",
              "gross_revenue", "refunds", "net_revenue", "aov", "revenue_growth_pct"]]


def build_product_performance(t: dict) -> pd.DataFrame:
    oi = t["order_items"].merge(
        t["orders"][["order_id", "status", "order_date", "order_month"]], on="order_id", how="left")
    oi = oi[oi["status"].isin(SHIPPED)].copy()

    prod = t["products"][["product_id", "product_name", "category", "subcategory",
                          "brand", "unit_price", "unit_cost"]]
    p = oi.merge(prod, on="product_id", how="left", suffixes=("", "_p"))
    p["revenue"] = p["line_total"]
    p["profit"] = (p["line_total"] - p["unit_cost"] * p["quantity"]).round(2)

    ret = t["returns"][t["returns"]["status"].isin(["completed", "approved"])]
    ret_by_prod = ret.groupby("product_id").agg(returned_units=("quantity", "sum"),
                                                refund_amount=("refund_amount", "sum"))

    g = p.groupby(["product_id", "product_name", "category", "subcategory", "brand"], as_index=False).agg(
        units_sold=("quantity", "sum"),
        orders=("order_id", "nunique"),
        revenue=("revenue", "sum"),
        profit=("profit", "sum"),
        avg_selling_price=("unit_price", "mean"),
    )
    g = g.merge(ret_by_prod, left_on="product_id", right_index=True, how="left").fillna(
        {"returned_units": 0, "refund_amount": 0.0})
    g["return_rate"] = np.where(g["units_sold"] > 0, g["returned_units"] / g["units_sold"], 0).round(4)
    g["revenue"] = g["revenue"].round(2)
    g["profit"] = g["profit"].round(2)
    g["margin_pct"] = np.where(g["revenue"] > 0, g["profit"] / g["revenue"] * 100, 0).round(2)
    g["revenue_rank"] = g["revenue"].rank(ascending=False, method="min").astype(int)
    g = g.sort_values("revenue", ascending=False).reset_index(drop=True)
    return g


def build_customer_analytics(t: dict) -> pd.DataFrame:
    o = t["orders"]
    shipped = o[o["status"].isin(SHIPPED)].copy()
    snapshot = pd.Timestamp(o["order_date"].max())

    agg = shipped.groupby("customer_id").agg(
        first_order_date=("order_date", "min"),
        last_order_date=("order_date", "max"),
        orders=("order_id", "count"),
        monetary=("order_total", "sum"),
        avg_order_value=("order_total", "mean"),
        total_items=("item_count", "sum"),
    ).reset_index()
    agg["monetary"] = agg["monetary"].round(2)
    agg["avg_order_value"] = agg["avg_order_value"].round(2)
    agg["recency_days"] = (snapshot - pd.to_datetime(agg["last_order_date"])).dt.days
    agg["tenure_days"] = (pd.to_datetime(agg["last_order_date"]) - pd.to_datetime(agg["first_order_date"])).dt.days + 1
    agg["is_repeat_customer"] = agg["orders"] > 1
    agg["purchase_frequency_per_year"] = (
        agg["orders"] / (agg["tenure_days"] / 365.25).clip(lower=1 / 365.25)).round(2)
    agg["predicted_clv"] = (agg["avg_order_value"] * agg["purchase_frequency_per_year"] * 2).round(2)

    # RFM scoring on 1-5 quintiles
    for col, label, ascending in (("recency_days", "r_score", True),
                                  ("orders", "f_score", False),
                                  ("monetary", "m_score", False)):
        try:
            agg[label] = pd.qcut(agg[col].rank(method="first"), 5,
                                 labels=[5, 4, 3, 2, 1] if ascending else [1, 2, 3, 4, 5]).astype(int)
        except ValueError:
            agg[label] = 3
    agg["rfm_score"] = agg["r_score"] * 100 + agg["f_score"] * 10 + agg["m_score"]

    def segment(row) -> str:
        r, f, m = row["r_score"], row["f_score"], row["m_score"]
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        if r >= 3 and f >= 3:
            return "Loyal Customers"
        if r >= 4 and f <= 2:
            return "New Customers"
        if r >= 3 and f <= 2:
            return "Potential Loyalists"
        if r <= 2 and f >= 3:
            return "At Risk"
        if r <= 2 and m >= 3:
            return "Cant Lose Them"
        return "Hibernating"

    agg["segment"] = agg.apply(segment, axis=1)

    cust = t["customers"][["customer_id", "first_name", "last_name", "email", "city",
                           "state", "region", "signup_date", "loyalty_tier"]]
    out = agg.merge(cust, on="customer_id", how="left")
    out["customer_name"] = (out["first_name"].fillna("") + " " + out["last_name"].fillna("")).str.strip()
    return out.sort_values("monetary", ascending=False).reset_index(drop=True)


def build_regional_sales(t: dict) -> pd.DataFrame:
    o = t["orders"][t["orders"]["status"].isin(SHIPPED)]
    g = o.groupby(["region", "state"], as_index=False).agg(
        orders=("order_id", "count"),
        customers=("customer_id", "nunique"),
        revenue=("order_total", "sum"),
    )
    g["revenue"] = g["revenue"].round(2)
    g["aov"] = (g["revenue"] / g["orders"]).round(2)
    g["revenue_share_pct"] = (g["revenue"] / g["revenue"].sum() * 100).round(2)
    return g.sort_values("revenue", ascending=False).reset_index(drop=True)


def build_payment_analytics(t: dict) -> pd.DataFrame:
    p = t["payments"]
    attempts = p.groupby("payment_method", as_index=False).agg(
        attempts=("payment_id", "count"),
        amount=("amount", "sum"),
        success_count=("status", lambda s: int((s == "success").sum())),
        failed_count=("status", lambda s: int((s == "failed").sum())),
        refunded_count=("status", lambda s: int((s == "refunded").sum())),
        pending_count=("status", lambda s: int((s == "pending").sum())),
    )
    attempts["success_rate"] = (attempts["success_count"] / attempts["attempts"] * 100).round(2)
    attempts["failure_rate"] = (attempts["failed_count"] / attempts["attempts"] * 100).round(2)
    attempts["amount"] = attempts["amount"].round(2)

    status = p.groupby(["payment_method", "status"], as_index=False).agg(
        payments=("payment_id", "count"), amount=("amount", "sum"))
    status["amount"] = status["amount"].round(2)

    by_status = p.groupby("status", as_index=False).agg(
        payments=("payment_id", "count"), amount=("amount", "sum"))
    by_status["amount"] = by_status["amount"].round(2)
    by_status["method"] = "ALL"
    return {"by_method": attempts, "by_method_status": status, "by_status": by_status}


def build_return_analytics(t: dict) -> pd.DataFrame:
    # silver.returns already carries order_month (and its own status), so only the
    # product category has to be joined in; re-merging orders would create
    # order_month_x/_y and status_x/_y collisions.
    r = t["returns"].merge(t["products"][["product_id", "category"]], on="product_id", how="left")
    g = r.groupby(["category", "reason"], as_index=False).agg(
        returns=("return_id", "count"),
        units=("quantity", "sum"),
        refund_amount=("refund_amount", "sum"),
    )
    g["refund_amount"] = g["refund_amount"].round(2)

    monthly = r.groupby("order_month", as_index=False).agg(
        returns=("return_id", "count"), refund_amount=("refund_amount", "sum"))
    monthly["refund_amount"] = monthly["refund_amount"].round(2)

    by_status = r.groupby("status", as_index=False).agg(
        returns=("return_id", "count"), refund_amount=("refund_amount", "sum"))
    by_status["refund_amount"] = by_status["refund_amount"].round(2)
    return {"by_category_reason": g, "monthly": monthly, "by_status": by_status}


def build_order_analytics(t: dict) -> pd.DataFrame:
    o = t["orders"]
    status_counts = o.groupby("status", as_index=False).agg(
        orders=("order_id", "count"), revenue=("order_total", "sum"))
    status_counts["revenue"] = status_counts["revenue"].round(2)
    status_counts["share_pct"] = (status_counts["orders"] / len(o) * 100).round(2)
    return status_counts


def build_fulfilment(t: dict) -> pd.DataFrame:
    s = t["shipments"].merge(t["orders"][["order_id", "order_ts", "status"]], on="order_id", how="left")
    s = s[s["delivered_ts"].notna()]
    s["processing_days"] = (s["delivered_ts"] - pd.to_datetime(s["order_ts"])).dt.total_seconds() / 86400
    g = s.groupby("carrier", as_index=False).agg(
        shipments=("shipment_id", "count"),
        avg_processing_days=("processing_days", "mean"),
        avg_delivery_days=("delivery_days", "mean"),
        late_shipments=("is_late", "sum"),
    )
    g["avg_processing_days"] = g["avg_processing_days"].round(2)
    g["avg_delivery_days"] = g["avg_delivery_days"].round(2)
    g["late_rate"] = (g["late_shipments"] / g["shipments"] * 100).round(2)
    return g


def build_cohort_retention(t: dict) -> pd.DataFrame:
    o = t["orders"][t["orders"]["status"].isin(SHIPPED)].copy()
    o["order_month"] = pd.to_datetime(o["order_date"]).dt.to_period("M")
    first = o.groupby("customer_id")["order_month"].min().rename("cohort")
    o = o.join(first, on="customer_id")
    o["month_index"] = (
        (o["order_month"].dt.year - o["cohort"].dt.year) * 12
        + (o["order_month"].dt.month - o["cohort"].dt.month)
    )
    o["cohort"] = o["cohort"].astype(str)

    sizes = o.groupby("cohort")["customer_id"].nunique().rename("cohort_size")
    activity = o.groupby(["cohort", "month_index"])["customer_id"].nunique().rename("customers").reset_index()
    activity = activity.merge(sizes, on="cohort")
    activity["retention_pct"] = (activity["customers"] / activity["cohort_size"] * 100).round(2)
    return activity.sort_values(["cohort", "month_index"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
def _catalog_entry(name: str, df: pd.DataFrame, partition_col: str | None, description: str) -> dict:
    return {
        "dataset": name,
        "description": description,
        "rows": int(len(df)),
        "columns": [{"name": c, "type": str(df[c].dtype)} for c in df.columns],
        "partition_column": partition_col,
        "format": "PARQUET",
        "location": lake.lake_uri(settings.gold_bucket, f"{name}/"),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def run_gold() -> dict:
    ensure_ops_schema()
    summary: dict = {}

    with track_run("gold_build", "aggregate", "all") as run:
        t = _load()
        orders = t["orders"]

        datasets: dict[str, tuple[pd.DataFrame, str | None, str]] = {
            "daily_sales": (build_daily_sales(t), "year", "Daily order, revenue and refund metrics"),
            "monthly_revenue": (build_monthly_revenue(t), "year", "Monthly revenue with growth and AOV"),
            "product_performance": (build_product_performance(t), None, "Per-product revenue, profit, returns"),
            "customer_analytics": (build_customer_analytics(t), None, "Customer 360 with RFM, CLV, segmentation"),
            "regional_sales": (build_regional_sales(t), None, "Revenue by region and state"),
            "return_analytics": (build_return_analytics(t)["by_category_reason"], None, "Returns by category and reason"),
            "order_analytics": (build_order_analytics(t), None, "Order status distribution"),
            "fulfilment": (build_fulfilment(t), None, "Carrier performance and processing time"),
            "cohort_retention": (build_cohort_retention(t), None, "Monthly cohort retention matrix"),
        }
        # multi-frame payment output
        pay = build_payment_analytics(t)
        datasets["payment_analytics"] = (pay["by_method"], None, "Payment success by method")

        catalog = []
        total_rows = 0
        lake.ensure_bucket(settings.gold_bucket)

        for name, (df, part_col, desc) in datasets.items():
            key = f"{name}/{name}.parquet"
            lake.put_dataframe_parquet(settings.gold_bucket, key, df, compression="snappy")
            total_rows += len(df)
            summary[name] = {"rows": int(len(df)), "columns": len(df.columns),
                             "location": lake.lake_uri(settings.gold_bucket, key)}
            catalog.append(_catalog_entry(name, df, part_col, desc))

        # supporting frames
        extra = {
            "payment_analytics_by_status": pay["by_status"],
            "payment_analytics_by_method_status": pay["by_method_status"],
            "return_analytics_monthly": build_return_analytics(t)["monthly"],
            "return_analytics_by_status": build_return_analytics(t)["by_status"],
        }
        for name, df in extra.items():
            lake.put_dataframe_parquet(settings.gold_bucket, f"{name}/{name}.parquet", df, compression="snappy")
            summary[name] = {"rows": int(len(df)), "columns": len(df.columns)}
            catalog.append(_catalog_entry(name, df, None, name.replace("_", " ").title()))

        kpis = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "total_orders": int(len(orders)),
            "total_customers": int(orders["customer_id"].nunique()),
            "total_products": int(len(t["products"])),
            "date_range": [str(orders["order_date"].min().date()), str(orders["order_date"].max().date())],
        }
        lake.put_json(settings.gold_bucket, "_catalog.json", {"datasets": catalog, "kpis": kpis})

        run.records_in = int(len(orders))
        run.records_out = total_rows
        run.details.update({"datasets": len(datasets), "catalog": catalog and len(catalog)})

    return summary


if __name__ == "__main__":
    print(json.dumps(run_gold(), indent=2, default=str))
