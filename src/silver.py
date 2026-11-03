"""Silver layer: clean, conform and validate bronze data.

Transformations applied
-----------------------
* Schema enforcement / type casting (numeric, boolean, timestamp)
* Timestamp normalisation to UTC-naive ``datetime64[ns]``
* Duplicate removal on the natural key (latest record wins)
* Missing-value handling with explicit, documented defaults
* Business-rule validation (non-negative money, valid enums, date ordering)
* Referential-integrity checks against parent entities
* Quarantine of rejected rows instead of silent data loss

Output: Parquet, partitioned by month for facts and by snapshot date for
dimensions, plus a per-dataset quality report in the lake.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from . import s3client as lake
from .config import settings
from .db import ensure_ops_schema
from .monitoring import record_quality_check, track_run

STATUS_ORDER = {"pending", "completed", "cancelled", "returned"}
PAYMENT_STATUS = {"success", "failed", "pending", "refunded"}
SHIPMENT_STATUS = {"delivered", "in_transit", "delayed", "lost"}
RETURN_STATUS = {"completed", "approved", "pending", "rejected"}
REGIONS = {"Northeast", "Midwest", "South", "West"}
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"

TS_COLUMNS = {
    "customers": ["signup_date"],
    "products": ["launch_date"],
    "orders": ["order_ts", "updated_at"],
    "order_items": [],
    "payments": ["payment_ts"],
    "shipments": ["shipped_ts", "delivered_ts", "promised_date"],
    "returns": ["return_ts"],
}

NUMERIC_COLUMNS = {
    "customers": [],
    "products": ["unit_cost", "unit_price", "weight_kg"],
    "orders": ["item_count", "subtotal", "discount_amount", "shipping_cost",
               "tax_amount", "order_total"],
    "order_items": ["quantity", "unit_price", "unit_cost", "discount_pct",
                    "line_subtotal", "line_total"],
    "payments": ["amount", "attempt_number"],
    "shipments": ["shipping_cost"],
    "returns": ["quantity", "refund_amount"],
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def read_bronze(source: str) -> pd.DataFrame:
    keys = [k for k in lake.list_keys(settings.bronze_bucket, f"{source}/")
            if k.endswith((".csv", ".json")) and "/_metadata.json" not in k]
    if not keys:
        raise FileNotFoundError(f"No bronze objects for {source}")

    frames = []
    for key in sorted(keys):
        raw = lake.get_bytes(settings.bronze_bucket, key)
        if key.endswith(".json"):
            from io import BytesIO

            frames.append(pd.read_json(BytesIO(raw), lines=True))
        else:
            from io import BytesIO

            frames.append(pd.read_csv(BytesIO(raw)))
    df = pd.concat(frames, ignore_index=True)

    ts = TS_COLUMNS.get(source) or []
    sort_col = next((c for c in ts if c in df.columns), None)
    if sort_col:
        df["_sort_ts"] = pd.to_datetime(df[sort_col], errors="coerce")
        df = df.sort_values("_sort_ts")
    return df.drop_duplicates(subset=_pk(source), keep="last").drop(columns=["_sort_ts"], errors="ignore")


def _pk(source: str) -> str:
    return {
        "customers": "customer_id", "products": "product_id", "orders": "order_id",
        "order_items": "order_item_id", "payments": "payment_id",
        "shipments": "shipment_id", "returns": "return_id",
    }[source]


class CheckLog:
    def __init__(self) -> None:
        self.entries: list[dict] = []

    def add(self, name: str, failed_mask: pd.Series | None, total: int,
            severity: str = "error", note: str = "") -> None:
        failed = int(failed_mask.sum()) if failed_mask is not None else 0
        self.entries.append({
            "check": name, "status": "passed" if failed == 0 else "failed",
            "failed_rows": failed, "total_rows": int(total),
            "severity": severity, "note": note,
        })

    @property
    def failed(self) -> int:
        return sum(e["failed_rows"] for e in self.entries if e["status"] == "failed")


def _nulls(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    for c in cols:
        if c in df.columns:
            mask |= df[c].is_null() if hasattr(df[c], "is_null") else df[c].isna()
    return mask


# ---------------------------------------------------------------------------
# per-source cleaning
# ---------------------------------------------------------------------------
def _clean_customers(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    df["email"] = df["email"].astype(str).str.strip().str.lower()
    df["region"] = df["region"].fillna("Unknown").replace("", "Unknown")
    for c in ("city", "state"):
        df[c] = df[c].fillna("Unknown")
    df["country"] = df["country"].fillna("US")
    df["is_active"] = df["is_active"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df["loyalty_tier"] = df["loyalty_tier"].fillna("Bronze")

    bad = pd.Series(False, index=df.index)
    bad |= ~df["email"].str.match(EMAIL_RE, na=False)
    bad |= df["signup_date"].isna()
    bad |= df["customer_id"].isna()
    checks.add("valid_email", ~df["email"].str.match(EMAIL_RE, na=False), len(df))
    checks.add("not_null:customer_id", df["customer_id"].isna(), len(df))
    checks.add("valid_signup_date", df["signup_date"].isna(), len(df))
    checks.add("accepted_values:region", ~df["region"].isin(REGIONS), len(df), "warning")
    return df, bad


def _clean_products(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    for c in df.columns:
        if c in NUMERIC_COLUMNS["products"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["unit_cost"] = df["unit_cost"].fillna(0.0)
    df["unit_price"] = df["unit_price"].fillna(0.0)
    df["product_name"] = df["product_name"].fillna("Unnamed product")
    df["category"] = df["category"].fillna("Uncategorised")
    df["is_active"] = df["is_active"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["launch_date"] = pd.to_datetime(df["launch_date"], errors="coerce")

    bad = (df["unit_price"] <= 0) | (df["unit_cost"] < 0)
    checks.add("business_rule:unit_price_gt_0", df["unit_price"] <= 0, len(df))
    checks.add("business_rule:unit_cost_gte_0", df["unit_cost"] < 0, len(df))
    checks.add("not_null:category", df["category"] == "Uncategorised", len(df), "warning")
    return df, bad


def _clean_orders(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    df["order_ts"] = pd.to_datetime(df["order_ts"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce").fillna(df["order_ts"])
    for c in NUMERIC_COLUMNS["orders"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["status"] = df["status"].astype(str).str.lower().str.strip()
    df["channel"] = df["channel"].fillna("unknown").astype(str).str.lower()
    df["region"] = df["region"].fillna("Unknown")

    # derived calendar attributes (warehouse + lake partitioning)
    df["order_date"] = df["order_ts"].dt.normalize()
    df["order_month"] = df["order_ts"].dt.strftime("%Y-%m")
    df["order_year"] = df["order_ts"].dt.year
    df["is_completed"] = df["status"] == "completed"

    bad = (
        df["order_ts"].isna()
        | ~df["status"].isin(STATUS_ORDER)
        | (df["order_total"] < 0)
        | (df["item_count"] < 1)
        | df["customer_id"].isna()
    )
    checks.add("valid_order_ts", df["order_ts"].isna(), len(df))
    checks.add("accepted_values:status", ~df["status"].isin(STATUS_ORDER), len(df))
    checks.add("business_rule:order_total_gte_0", df["order_total"] < 0, len(df))
    checks.add("business_rule:item_count_gte_1", df["item_count"] < 1, len(df))
    checks.add("not_null:customer_id", df["customer_id"].isna(), len(df))
    return df, bad


def _clean_order_items(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    for c in NUMERIC_COLUMNS["order_items"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    bad = (
        (df["quantity"] <= 0) | (df["quantity"] > 100)
        | (df["unit_price"] <= 0) | (df["line_total"] < 0)
        | df["order_id"].isna() | df["product_id"].isna()
    )
    checks.add("business_rule:quantity_range", (df["quantity"] <= 0) | (df["quantity"] > 100), len(df))
    checks.add("business_rule:line_total_gte_0", df["line_total"] < 0, len(df))
    checks.add("not_null:product_id", df["product_id"].isna(), len(df))
    return df, bad


def _clean_payments(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    df["payment_ts"] = pd.to_datetime(df["payment_ts"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["status"] = df["status"].astype(str).str.lower().str.strip()
    df["payment_method"] = df["payment_method"].fillna("unknown").astype(str).str.lower()
    df["attempt_number"] = pd.to_numeric(df["attempt_number"], errors="coerce").fillna(1).astype(int)

    bad = df["payment_ts"].isna() | ~df["status"].isin(PAYMENT_STATUS) | (df["amount"] < 0)
    checks.add("valid_payment_ts", df["payment_ts"].isna(), len(df))
    checks.add("accepted_values:payment_status", ~df["status"].isin(PAYMENT_STATUS), len(df))
    checks.add("business_rule:amount_gte_0", df["amount"] < 0, len(df))
    return df, bad


def _clean_shipments(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    for c in ("shipped_ts", "delivered_ts", "promised_date"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0.0)
    df["status"] = df["status"].astype(str).str.lower().str.strip()

    late_delivery = df["delivered_ts"].notna() & (df["delivered_ts"] > df["promised_date"])
    bad = (
        df["shipped_ts"].isna()
        | ~df["status"].isin(SHIPMENT_STATUS)
        | (df["delivered_ts"].notna() & (df["delivered_ts"] < df["shipped_ts"]))
    )
    df["is_late"] = late_delivery.fillna(False)
    df["delivery_days"] = (df["delivered_ts"] - df["shipped_ts"]).dt.total_seconds() / 86400

    checks.add("valid_shipped_ts", df["shipped_ts"].isna(), len(df))
    checks.add("accepted_values:shipment_status", ~df["status"].isin(SHIPMENT_STATUS), len(df))
    checks.add("business_rule:delivered_after_shipped",
               df["delivered_ts"].notna() & (df["delivered_ts"] < df["shipped_ts"]), len(df))
    checks.add("late_delivery_rate", pd.Series([not late_delivery.mean() < 0.2] * len(df)),
               len(df), "warning", f"late_rate={late_delivery.mean():.3f}")
    return df, bad


def _clean_returns(df: pd.DataFrame, checks: CheckLog) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()
    df["return_ts"] = pd.to_datetime(df["return_ts"], errors="coerce")
    df["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="coerce").fillna(0.0)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["status"] = df["status"].astype(str).str.lower().str.strip()
    df["restocked"] = df["restocked"].astype(str).str.lower().isin(["true", "1", "yes"])

    bad = (
        df["return_ts"].isna() | ~df["status"].isin(RETURN_STATUS)
        | (df["refund_amount"] < 0) | (df["quantity"] <= 0)
        | df["order_id"].isna() | df["product_id"].isna()
    )
    checks.add("valid_return_ts", df["return_ts"].isna(), len(df))
    checks.add("accepted_values:return_status", ~df["status"].isin(RETURN_STATUS), len(df))
    checks.add("business_rule:refund_gte_0", df["refund_amount"] < 0, len(df))
    checks.add("business_rule:return_qty_gt_0", df["quantity"] <= 0, len(df))
    return df, bad


CLEANERS = {
    "customers": _clean_customers, "products": _clean_products, "orders": _clean_orders,
    "order_items": _clean_order_items, "payments": _clean_payments,
    "shipments": _clean_shipments, "returns": _clean_returns,
}


# ---------------------------------------------------------------------------
# lake writes
# ---------------------------------------------------------------------------
def _write_silver(source: str, df: pd.DataFrame) -> tuple[int, dict]:
    """Write Parquet partitioned by month (facts) or snapshot (dimensions)."""
    bucket = settings.silver_bucket
    lake.ensure_bucket(bucket)
    partitions: dict[str, int] = {}
    total_bytes = 0

    if source in ("customers", "products"):
        part_col, part_prefix = None, f"snapshot_date={datetime.now(timezone.utc):%Y-%m-%d}"
        key = f"{source}/{part_prefix}/{source}.parquet"
        lake.put_dataframe_parquet(bucket, key, df, compression="snappy")
        total_bytes = len(lake.get_bytes(bucket, key))
        partitions[part_prefix] = len(df)
    else:
        if "order_month" in df.columns:
            groups = df.groupby("order_month")
        else:
            groups = [("unknown", df)]
        for month, part in groups:
            key = f"{source}/order_month={month}/{source}.parquet"
            lake.put_dataframe_parquet(bucket, key, part, compression="snappy")
            total_bytes += len(lake.get_bytes(bucket, key))
            partitions[str(month)] = int(len(part))
    return total_bytes, partitions


def run_silver(sources: list[str] | None = None) -> dict:
    ensure_ops_schema()
    sources = sources or list(CLEANERS)
    summary = {}

    for source in sources:
        with track_run("silver_transform", "clean", source) as run:
            raw = read_bronze(source)
            checks = CheckLog()
            clean, bad_mask = CLEANERS[source](raw, checks)
            clean = clean[~bad_mask].copy()
            rejected = raw[bad_mask].copy()

            # referential integrity against already-cleaned parents
            for parent, col in (("customers", "customer_id"), ("products", "product_id"),
                                ("orders", "order_id")):
                if col in clean.columns and parent != source:
                    valid = _reference_ids(parent)
                    if valid is not None:
                        orphan = ~clean[col].isin(valid)
                        checks.add(f"referential_integrity:{col}", orphan, len(clean))
                        clean = clean[~orphan]
                        rejected = pd.concat([rejected, raw.loc[orphan[orphan].index]], axis=0)

            checks.add(f"unique:{_pk(source)}", clean[_pk(source)].duplicated(), len(clean))

            # drop helper columns that only exist for event ordering
            clean = clean.drop(columns=["_sort_ts"], errors="ignore")

            # guarantee partition key for child fact tables
            if source in ("order_items", "payments", "shipments", "returns") and "order_month" not in clean.columns:
                months = _order_months()
                if months is not None and "order_id" in clean.columns:
                    clean["order_month"] = clean["order_id"].map(months)
                    clean["order_month"] = clean["order_month"].fillna("unknown")

            total_bytes, partitions = _write_silver(source, clean)

            if len(rejected):
                rkey = f"_quarantine/{source}/rejected.parquet"
                lake.put_dataframe_parquet(settings.silver_bucket, rkey, rejected)

            report = {
                "dataset": source,
                "rows_in": int(len(raw)),
                "rows_out": int(len(clean)),
                "rows_rejected": int(len(rejected)),
                "partition_count": len(partitions),
                "partitions": partitions,
                "checks": checks.entries,
                "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            lake.put_json(settings.silver_bucket, f"{source}/_quality_report.json", report)

            for entry in checks.entries:
                record_quality_check(run.run_id, source, entry["check"],
                                     entry["status"] == "passed", entry["failed_rows"],
                                     entry["total_rows"], {"severity": entry["severity"],
                                                           "note": entry["note"]})

            run.records_in = int(len(raw))
            run.records_out = int(len(clean))
            run.records_rejected = int(len(rejected))
            run.bytes_written = total_bytes
            run.details.update({"partitions": partitions, "checks": len(checks.entries)})
            summary[source] = report

    return summary


_REF_CACHE: dict[str, set] = {}


def _reference_ids(parent: str) -> set | None:
    if parent in _REF_CACHE:
        return _REF_CACHE[parent]
    pk = {"customers": "customer_id", "products": "product_id", "orders": "order_id"}[parent]
    try:
        pk_df = read_silver(parent, columns=[pk])
        ids = set(pk_df[pk].dropna().unique())
        _REF_CACHE[parent] = ids
        return ids
    except Exception:  # noqa: BLE001 - parent not built yet
        return None


def read_silver(source: str, columns: list[str] | None = None) -> pd.DataFrame:
    keys = [k for k in lake.list_keys(settings.silver_bucket, f"{source}/")
            if k.endswith(".parquet") and "/_quarantine/" not in k]
    if not keys:
        raise FileNotFoundError(f"No silver parquet for {source}")
    frames = [lake.get_dataframe_parquet(settings.silver_bucket, k) for k in keys]
    df = pd.concat(frames, ignore_index=True)
    return df[columns] if columns else df


_ORDER_MONTHS: dict | None = None


def _order_months() -> dict | None:
    global _ORDER_MONTHS
    if _ORDER_MONTHS is None:
        try:
            o = read_silver("orders", columns=["order_id", "order_month"])
            _ORDER_MONTHS = dict(zip(o["order_id"], o["order_month"]))
        except Exception:  # noqa: BLE001
            return None
    return _ORDER_MONTHS


def reset_caches() -> None:
    global _ORDER_MONTHS
    _ORDER_MONTHS = None
    _REF_CACHE.clear()


if __name__ == "__main__":
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "partitions"}
                      for k, v in run_silver().items()}, indent=2))
