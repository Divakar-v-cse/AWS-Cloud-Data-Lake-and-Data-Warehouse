"""Load the gold layer into the dimensional warehouse.

Pattern: ELT with staging tables (what Redshift/Glue users actually do)
    1. Land each gold dataset into ``staging.stg_<name>`` (truncate + bulk load)
    2. Resolve surrogate keys via joins to the SCD2 dimensions
    3. Upsert into the fact tables with ``ON CONFLICT`` so re-runs are idempotent
       (Redshift equivalent: MERGE INTO ... using staging)

SCD Type 2
----------
Customers and products are versioned. An MD5 hash over the tracked attributes is
compared with the current version: changed rows close the old version
(valid_to = now, is_current = false) and insert a new one. Unchanged rows are
left untouched, so history is preserved and lookups stay stable.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta

import pandas as pd
from sqlalchemy import text

from . import s3client as lake
from .config import settings
from .db import connection, ensure_ops_schema, get_engine, wait_for_warehouse
from .monitoring import track_run

SCHEMA_SQL = (__import__("pathlib").Path(__file__).resolve().parent.parent
              / "sql" / "warehouse_schema.sql")

PAYMENT_GROUPS = {
    "credit_card": "Card", "debit_card": "Card", "paypal": "Wallet",
    "upi": "Bank Transfer", "net_banking": "Bank Transfer",
    "gift_card": "Prepaid", "cod": "Cash", "unknown": "Other",
}


def read_gold(name: str) -> pd.DataFrame:
    key = f"{name}/{name}.parquet"
    return lake.get_dataframe_parquet(settings.gold_bucket, key)


def _hash_row(row: pd.Series, cols: list[str]) -> str:
    payload = "|".join("" if pd.isna(row[c]) else str(row[c]) for c in cols)
    return hashlib.md5(payload.encode()).hexdigest()


def _split_ddl(sql: str) -> list[str]:
    """Split a DDL script into statements.

    Line comments are stripped first: a ';' inside a comment (for example the
    pointer to sql/redshift_ddl.sql) would otherwise cut a statement in half.
    """
    lines = []
    for raw in sql.splitlines():
        line = raw.split("--", 1)[0].rstrip()
        if line:
            lines.append(line)
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def ensure_schema() -> None:
    wait_for_warehouse()
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    with connection() as conn:
        for stmt in _split_ddl(sql):
            conn.execute(text(stmt))


def load_dim_date() -> int:
    start, end = date(2023, 1, 1), date(2027, 12, 31)
    # Built row-by-row from native Python values on purpose: pandas' vectorised
    # .isocalendar().week yields a nullable UInt32 whose NA sentinel can leak
    # into the INSERT and break the NOT NULL constraint on week_of_year.
    rows = []
    for offset in range((end - start).days + 1):
        d = start + timedelta(days=offset)
        iso = d.isocalendar()
        rows.append({
            "full_date": d,
            "date_key": int(d.strftime("%Y%m%d")),
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "month_short": d.strftime("%b"),
            "day_of_month": d.day,
            "day_of_week": d.isoweekday(),
            "day_name": d.strftime("%A"),
            "week_of_year": int(iso[1]),
            "is_weekend": d.weekday() >= 5,
            "year_month": d.strftime("%Y-%m"),
        })
    df = pd.DataFrame(rows)
    df["week_of_year"] = df["week_of_year"].astype("int64")

    with get_engine().begin() as conn:
        # CASCADE clears dependent fact tables too: run_load() reloads every fact
        # afterwards, so a re-run is a clean, idempotent full refresh.
        conn.execute(text("TRUNCATE TABLE dwh.dim_date CASCADE"))
        df.to_sql("dim_date", conn, schema="dwh", if_exists="append", index=False,
                  method="multi", chunksize=2000)
    return len(df)


def load_payment_methods() -> int:
    rows = []
    for i, (name, group) in enumerate(sorted(PAYMENT_GROUPS.items()), start=1):
        rows.append({"method_key": i, "method_name": name, "method_group": group})
    with get_engine().begin() as conn:
        conn.execute(text("TRUNCATE TABLE dwh.dim_payment_method CASCADE"))
        pd.DataFrame(rows).to_sql("dim_payment_method", conn, schema="dwh",
                                  if_exists="append", index=False)
    return len(rows)


def _scd2(table: str, df: pd.DataFrame, key: str, attrs: list[str],
          hash_cols: list[str]) -> dict:
    """Generic SCD2 merge for a dimension table."""
    now = datetime.utcnow()
    df = df.copy()
    df["hash_diff"] = df.apply(lambda r: _hash_row(r, hash_cols), axis=1)
    df = df.drop_duplicates(subset=[key], keep="last")

    inserted = updated = 0
    with connection() as conn:
        current = pd.read_sql(
            text(f"SELECT {key}, hash_diff FROM dwh.{table} WHERE is_current"),
            conn,
        )
        # CHAR(n) columns come back space-padded, which would make every stored
        # hash compare unequal to the computed one and re-version every row on
        # every run. Strip defensively so SCD2 stays idempotent.
        current["hash_diff"] = current["hash_diff"].astype(str).str.strip()
        cur_map = dict(zip(current[key], current["hash_diff"]))

        new_rows = df[~df[key].isin(cur_map.keys())].assign(
            valid_from=now, valid_to=None, is_current=True)
        changed = df[df[key].isin(cur_map.keys()) & (df["hash_diff"] != df[key].map(cur_map))]

        if len(changed):
            # Set-based close (single round trip); the per-row UPDATE loop this
            # replaces cost ~20k statements on the first full load.
            conn.execute(
                text(f"""UPDATE dwh.{table}
                        SET valid_to = :now, is_current = FALSE
                        WHERE is_current AND {key} = ANY(:keys)"""),
                {"now": now, "keys": changed[key].tolist()},
            )
        changed = changed.assign(valid_from=now, valid_to=None, is_current=True)

        to_insert = pd.concat([new_rows, changed], ignore_index=True)
        if len(to_insert):
            payload = to_insert[
                [key] + attrs + ["valid_from", "valid_to", "is_current", "hash_diff"]
            ].copy()
            # An all-null valid_to makes pandas infer object dtype, which would be
            # bound as VARCHAR and rejected by the timestamptz column. Cast the
            # SCD2 bookkeeping columns explicitly so the INSERT types are stable.
            payload["valid_from"] = pd.to_datetime(payload["valid_from"])
            payload["valid_to"] = pd.to_datetime(payload["valid_to"])
            payload["is_current"] = payload["is_current"].astype(bool)
            payload.to_sql(table, conn, schema="dwh", if_exists="append",
                           index=False, method="multi", chunksize=500)
        inserted, updated = len(to_insert), len(changed)
    return {"inserted": inserted, "versions_closed": updated,
            "unchanged": len(df) - inserted}


def _pg_dsn() -> str:
    return (f"host={settings.warehouse_host} port={settings.warehouse_port} "
            f"dbname={settings.warehouse_db} user={settings.warehouse_user} "
            f"password={settings.warehouse_password}")


def _sql_type(col: pd.Series) -> str:
    kind = col.dtype.kind
    if kind == "b":
        return "BOOLEAN"
    if kind in "iu":
        return "BIGINT"
    if kind == "f":
        return "DOUBLE PRECISION"
    if kind == "M":
        return "TIMESTAMP"
    if kind == "O" and pd.api.types.is_datetime64_any_dtype(col):
        return "TIMESTAMP"
    return "TEXT"


def _stage(df: pd.DataFrame, name: str) -> None:
    """Bulk-load a frame into ``staging.stg_<name>`` via COPY.

    ``to_sql(method="multi")`` was the slowest part of the load by far: the
    statement builder spends its time formatting ~60k placeholders per chunk
    and Postgres re-parses each one. A single COPY stream is roughly two
    orders of magnitude faster and is also what a real warehouse load does.
    """
    import io

    import psycopg

    table = f"staging.stg_{name}"
    frame = df.copy()

    # CSV needs text; format timestamps explicitly so the COPY parser is
    # unambiguous, and let empty fields arrive as NULL.
    for col in frame.columns:
        if pd.api.types.is_datetime64_any_dtype(frame[col]):
            frame[col] = frame[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        elif frame[col].dtype == object:
            frame[col] = frame[col].map(lambda v: v if isinstance(v, str) else v)

    ddl = ", ".join(f'"{c}" {_sql_type(df[c])}' for c in df.columns)
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    columns = ", ".join(f'"{c}"' for c in df.columns)
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} ({ddl})")
            with cur.copy(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv)") as cp:
                cp.write(buffer.getvalue())
        conn.commit()


def _sk_map(conn, table: str, key: str, sk: str) -> dict:
    rows = conn.execute(text(f"SELECT {key}, {sk} FROM dwh.{table} WHERE is_current")).all()
    return {r[0]: r[1] for r in rows}


def load_fact_orders() -> int:
    orders = read_gold("daily_sales")  # only used to sanity check dates
    del orders
    from .silver import read_silver

    o = read_silver("orders")
    o = o[[
        "order_id", "customer_id", "order_ts", "status", "channel", "region", "state",
        "city", "item_count", "subtotal", "discount_amount", "shipping_cost",
        "tax_amount", "order_total", "updated_at",
    ]].copy()
    o["order_ts"] = pd.to_datetime(o["order_ts"])
    o["updated_at"] = pd.to_datetime(o["updated_at"])
    o["date_key"] = o["order_ts"].dt.strftime("%Y%m%d").astype(int)
    o["is_completed"] = o["status"] == "completed"

    _stage(o, "orders")
    with connection() as conn:
        sk = _sk_map(conn, "dim_customer", "customer_id", "customer_sk")
        conn.execute(text("""
            INSERT INTO dwh.fact_orders (
                order_id, customer_sk, date_key, order_ts, status, channel, region,
                state, city, item_count, subtotal, discount_amount, shipping_cost,
                tax_amount, order_total, is_completed, updated_at)
            SELECT s.order_id, c.customer_sk, s.date_key, s.order_ts, s.status, s.channel,
                   s.region, s.state, s.city, s.item_count, s.subtotal, s.discount_amount,
                   s.shipping_cost, s.tax_amount, s.order_total, s.is_completed, s.updated_at
            FROM staging.stg_orders s
            JOIN dwh.dim_customer c
              ON c.customer_id = s.customer_id AND c.is_current
            ON CONFLICT (order_id) DO UPDATE SET
                customer_sk = EXCLUDED.customer_sk,
                date_key = EXCLUDED.date_key,
                order_ts = EXCLUDED.order_ts,
                status = EXCLUDED.status,
                channel = EXCLUDED.channel,
                region = EXCLUDED.region,
                state = EXCLUDED.state,
                city = EXCLUDED.city,
                item_count = EXCLUDED.item_count,
                subtotal = EXCLUDED.subtotal,
                discount_amount = EXCLUDED.discount_amount,
                shipping_cost = EXCLUDED.shipping_cost,
                tax_amount = EXCLUDED.tax_amount,
                order_total = EXCLUDED.order_total,
                is_completed = EXCLUDED.is_completed,
                updated_at = EXCLUDED.updated_at
            WHERE dwh.fact_orders.updated_at IS NULL
               OR EXCLUDED.updated_at >= dwh.fact_orders.updated_at
        """))
        loaded = conn.execute(text("SELECT count(*) FROM dwh.fact_orders")).scalar_one()
    return int(loaded)


def load_fact_order_items() -> int:
    from .silver import read_silver

    oi = read_silver("order_items")
    o = read_silver("orders", columns=["order_id", "customer_id", "order_ts"])
    o["order_ts"] = pd.to_datetime(o["order_ts"])
    o["date_key"] = o["order_ts"].dt.strftime("%Y%m%d").astype(int)

    df = oi.merge(o, on="order_id", how="inner")
    df["line_profit"] = (df["line_total"] - df["unit_cost"] * df["quantity"]).round(2)
    keep = ["order_item_id", "order_id", "product_id", "customer_id", "date_key",
            "quantity", "unit_price", "unit_cost", "discount_pct", "line_subtotal",
            "line_total", "line_profit"]
    _stage(df[keep], "order_items")

    with connection() as conn:
        conn.execute(text("""
            INSERT INTO dwh.fact_order_items (
                order_item_id, order_id, product_sk, customer_sk, date_key, quantity,
                unit_price, unit_cost, discount_pct, line_subtotal, line_total, line_profit)
            SELECT s.order_item_id, s.order_id, p.product_sk, c.customer_sk, s.date_key,
                   s.quantity, s.unit_price, s.unit_cost, s.discount_pct,
                   s.line_subtotal, s.line_total, s.line_profit
            FROM staging.stg_order_items s
            JOIN dwh.dim_product p ON p.product_id = s.product_id AND p.is_current
            JOIN dwh.dim_customer c ON c.customer_id = s.customer_id AND c.is_current
            ON CONFLICT (order_item_id) DO UPDATE SET
                product_sk = EXCLUDED.product_sk,
                customer_sk = EXCLUDED.customer_sk,
                date_key = EXCLUDED.date_key,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                unit_cost = EXCLUDED.unit_cost,
                discount_pct = EXCLUDED.discount_pct,
                line_subtotal = EXCLUDED.line_subtotal,
                line_total = EXCLUDED.line_total,
                line_profit = EXCLUDED.line_profit
        """))
        return int(conn.execute(text("SELECT count(*) FROM dwh.fact_order_items")).scalar_one())


def load_fact_payments() -> int:
    from .silver import read_silver

    p = read_silver("payments")
    p["payment_ts"] = pd.to_datetime(p["payment_ts"])
    p["date_key"] = p["payment_ts"].dt.strftime("%Y%m%d").astype(int)
    keep = ["payment_id", "order_id", "date_key", "payment_ts", "payment_method",
            "amount", "status", "attempt_number"]
    _stage(p[keep], "payments")

    with connection() as conn:
        conn.execute(text("""
            INSERT INTO dwh.fact_payments (
                payment_id, order_id, date_key, payment_ts, payment_method,
                method_key, amount, status, attempt_number)
            SELECT s.payment_id, s.order_id, s.date_key, s.payment_ts, s.payment_method,
                   m.method_key, s.amount, s.status, s.attempt_number
            FROM staging.stg_payments s
            JOIN dwh.fact_orders f ON f.order_id = s.order_id
            LEFT JOIN dwh.dim_payment_method m ON m.method_name = s.payment_method
            ON CONFLICT (payment_id) DO UPDATE SET
                status = EXCLUDED.status, amount = EXCLUDED.amount,
                attempt_number = EXCLUDED.attempt_number,
                method_key = EXCLUDED.method_key
        """))
        return int(conn.execute(text("SELECT count(*) FROM dwh.fact_payments")).scalar_one())


def load_fact_shipments() -> int:
    from .silver import read_silver

    s = read_silver("shipments")
    s["shipped_ts"] = pd.to_datetime(s["shipped_ts"])
    s["delivered_ts"] = pd.to_datetime(s["delivered_ts"])
    s["date_key"] = s["shipped_ts"].dt.strftime("%Y%m%d").astype(int)
    s["is_late"] = s["is_late"].fillna(False).astype(bool)
    keep = ["shipment_id", "order_id", "date_key", "shipped_ts", "delivered_ts",
            "promised_date", "carrier", "status", "shipping_cost", "delivery_days", "is_late"]
    _stage(s[keep], "shipments")

    with connection() as conn:
        conn.execute(text("""
            INSERT INTO dwh.fact_shipments (
                shipment_id, order_id, date_key, shipped_ts, delivered_ts, promised_date,
                carrier, status, shipping_cost, delivery_days, is_late)
            SELECT s.shipment_id, s.order_id, s.date_key, s.shipped_ts, s.delivered_ts,
                   s.promised_date, s.carrier, s.status, s.shipping_cost,
                   s.delivery_days, s.is_late
            FROM staging.stg_shipments s
            JOIN dwh.fact_orders f ON f.order_id = s.order_id
            ON CONFLICT (shipment_id) DO UPDATE SET
                status = EXCLUDED.status, delivered_ts = EXCLUDED.delivered_ts,
                delivery_days = EXCLUDED.delivery_days, is_late = EXCLUDED.is_late
        """))
        return int(conn.execute(text("SELECT count(*) FROM dwh.fact_shipments")).scalar_one())


def load_fact_returns() -> int:
    from .silver import read_silver

    r = read_silver("returns")
    r["return_ts"] = pd.to_datetime(r["return_ts"])
    r["date_key"] = r["return_ts"].dt.strftime("%Y%m%d").astype(int)
    keep = ["return_id", "order_id", "order_item_id", "product_id", "date_key",
            "return_ts", "reason", "quantity", "refund_amount", "status", "restocked"]
    _stage(r[keep], "returns")

    with connection() as conn:
        conn.execute(text("""
            INSERT INTO dwh.fact_returns (
                return_id, order_id, order_item_id, product_sk, date_key, return_ts,
                reason, quantity, refund_amount, status, restocked)
            SELECT s.return_id, s.order_id, s.order_item_id, p.product_sk, s.date_key,
                   s.return_ts, s.reason, s.quantity, s.refund_amount, s.status, s.restocked
            FROM staging.stg_returns s
            JOIN dwh.fact_orders f ON f.order_id = s.order_id
            LEFT JOIN dwh.dim_product p ON p.product_id = s.product_id AND p.is_current
            ON CONFLICT (return_id) DO UPDATE SET
                status = EXCLUDED.status, refund_amount = EXCLUDED.refund_amount,
                product_sk = EXCLUDED.product_sk
        """))
        return int(conn.execute(text("SELECT count(*) FROM dwh.fact_returns")).scalar_one())


def run_load() -> dict:
    ensure_ops_schema()
    with track_run("warehouse_load", "load", "all") as run:
        ensure_schema()
        n_date = load_dim_date()
        n_pm = load_payment_methods()

        from .silver import read_silver

        cust = read_silver("customers")[[
            "customer_id", "first_name", "last_name", "email", "city", "state",
            "region", "country", "loyalty_tier", "signup_date"]].copy()
        cust["signup_date"] = pd.to_datetime(cust["signup_date"]).dt.date
        cust_stats = _scd2("dim_customer", cust, "customer_id",
                            ["first_name", "last_name", "email", "city", "state",
                             "region", "country", "loyalty_tier", "signup_date"],
                            ["first_name", "last_name", "email", "city", "state",
                             "region", "country", "loyalty_tier"])

        prod = read_silver("products")[[
            "product_id", "product_name", "sku", "category", "subcategory", "brand",
            "unit_cost", "unit_price", "is_active", "launch_date"]].copy()
        prod["launch_date"] = pd.to_datetime(prod["launch_date"]).dt.date
        prod_stats = _scd2("dim_product", prod, "product_id",
                           ["product_name", "sku", "category", "subcategory", "brand",
                            "unit_cost", "unit_price", "is_active", "launch_date"],
                           ["product_name", "category", "subcategory", "brand",
                            "unit_cost", "unit_price", "is_active"])

        n_orders = load_fact_orders()
        n_items = load_fact_order_items()
        n_pay = load_fact_payments()
        n_ship = load_fact_shipments()
        n_ret = load_fact_returns()

        run.records_in = n_orders
        run.records_out = n_orders + n_items + n_pay + n_ship + n_ret
        run.details.update({
            "dim_date": n_date, "dim_payment_method": n_pm,
            "dim_customer": cust_stats, "dim_product": prod_stats,
            "fact_orders": n_orders, "fact_order_items": n_items,
            "fact_payments": n_pay, "fact_shipments": n_ship, "fact_returns": n_ret,
        })

    return {
        "dim_date": n_date, "dim_payment_method": n_pm,
        "dim_customer": cust_stats, "dim_product": prod_stats,
        "fact_orders": n_orders, "fact_order_items": n_items,
        "fact_payments": n_pay, "fact_shipments": n_ship, "fact_returns": n_ret,
    }


if __name__ == "__main__":
    print(json.dumps(run_load(), indent=2, default=str))
