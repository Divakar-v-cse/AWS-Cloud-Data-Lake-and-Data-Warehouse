"""Bronze ingestion layer.

Lands source data in the data lake exactly as received (no mutation), organised
by ``source / ingestion_date / data_type`` and accompanied by a metadata
sidecar (row count, checksum, schema, watermark).

Supported channels
------------------
* CSV files     - the transactional exports from the source systems
* JSON files    - newline-delimited JSON feeds (customers, payments)
* REST API      - incremental pull from the mock partner API endpoint
* Event-driven  - S3 object-created notifications routed to a Lambda handler
                  (see lambda/s3_ingest_handler.py), which calls run_source()

Incremental strategy
--------------------
* Append-only fact feeds  -> watermark on their event timestamp
* Snapshot dimension feeds -> full snapshot per run (idempotent overwrite)
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import s3client as lake
from .config import settings
from .db import ensure_ops_schema
from .monitoring import set_watermark, track_run

# source -> (primary key, incremental timestamp column, is_snapshot)
SOURCES: dict[str, tuple[str, str | None, bool]] = {
    "customers": ("customer_id", "signup_date", True),
    "products": ("product_id", "launch_date", True),
    "orders": ("order_id", "updated_at", False),
    "order_items": ("order_item_id", None, False),
    "payments": ("payment_id", "payment_ts", False),
    "shipments": ("shipment_id", "shipped_ts", False),
    "returns": ("return_id", "return_ts", False),
}

REQUIRED_COLUMNS = {
    "customers": ["customer_id", "email", "signup_date", "region"],
    "products": ["product_id", "product_name", "category", "unit_price", "unit_cost"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "order_total"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "line_total"],
    "payments": ["payment_id", "order_id", "payment_ts", "amount", "status"],
    "shipments": ["shipment_id", "order_id", "shipped_ts", "status"],
    "returns": ["return_id", "order_id", "order_item_id", "return_ts", "refund_amount"],
}


def _read_raw(source: str, fmt: str) -> pd.DataFrame:
    base = Path(settings.raw_dir) / source
    if fmt == "csv":
        return pd.read_csv(base / f"{source}.csv")
    if fmt == "json":
        return pd.read_json(base / f"{source}.json", lines=True)
    raise ValueError(f"Unsupported file format: {fmt}")


def _read_api(source: str, since: str | None) -> pd.DataFrame:
    """Pull incrementally from the mock partner REST API (FastAPI)."""
    import httpx

    url = f"http://127.0.0.1:8100/mock-api/{source}"
    params = {"limit": 500000}
    if since:
        params["since"] = since
    resp = httpx.get(url, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    return pd.DataFrame(payload["records"])


def _checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _schema_snapshot(df: pd.DataFrame) -> dict:
    return {c: str(df[c].dtype) for c in df.columns}


def ingest_source(
    source: str,
    fmt: str = "csv",
    since: str | None = None,
    ingest_date: str | None = None,
) -> dict:
    """Land one source into the bronze bucket. Returns a summary dict."""
    if source not in SOURCES:
        raise ValueError(f"Unknown source '{source}'")

    ensure_ops_schema()
    pk, ts_col, is_snapshot = SOURCES[source]
    ingest_date = ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with track_run("bronze_ingest", "ingest", source) as run:
        df = _read_api(source, since) if fmt == "api" else _read_raw(source, fmt)

        missing = [c for c in REQUIRED_COLUMNS[source] if c not in df.columns]
        if missing:
            raise ValueError(f"{source}: missing required columns {missing}")

        # Incremental filter (applied to the extraction, records stay unchanged)
        if since and ts_col and not is_snapshot:
            before = len(df)
            df = df[pd.to_datetime(df[ts_col], errors="coerce") > pd.Timestamp(since)]
            run.details["incremental_filter"] = {"column": ts_col, "since": since,
                                                 "rows_before": before,
                                                 "rows_after": int(len(df))}

        if df.empty:
            run.details["note"] = "no new records for this watermark"
            run.records_in = run.records_out = 0
            return {"source": source, "rows": 0, "skipped": True}

        # Serialize the untouched payload
        if fmt == "json" or fmt == "api":
            body = df.to_json(orient="records", lines=True, date_format="iso").encode()
            ext = "json"
        else:
            body = df.to_csv(index=False).encode()
            ext = "csv"

        key = f"{source}/ingest_date={ingest_date}/{source}.{ext}"
        lake.ensure_bucket(settings.bronze_bucket)
        lake.put_bytes(settings.bronze_bucket, key, body)

        meta = {
            "source": source,
            "channel": fmt,
            "ingest_date": ingest_date,
            "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rows": int(len(df)),
            "bytes": len(body),
            "sha256": _checksum(body),
            "format": ext,
            "schema": _schema_snapshot(df),
            "primary_key": pk,
            "incremental": not is_snapshot,
            "watermark_max": (str(pd.to_datetime(df[ts_col], errors="coerce").max())
                              if ts_col else None),
        }
        lake.put_json(settings.bronze_bucket, f"{source}/ingest_date={ingest_date}/_metadata.json", meta)

        run.records_in = run.records_out = int(len(df))
        run.bytes_written = len(body)
        run.details.update({
            "object": lake.lake_uri(settings.bronze_bucket, key),
            "sha256": meta["sha256"],
            "ingest_date": ingest_date,
        })

        if ts_col and not is_snapshot:
            set_watermark(source, pd.to_datetime(df[ts_col], errors="coerce").max())

    return {"source": source, "rows": int(len(df)), "object": key,
            "bytes": len(body), "run_id": run.run_id}


def ingest_all(fmt: str = "csv", since: str | None = None) -> list[dict]:
    results, failures = [], []
    for source in SOURCES:
        try:
            results.append(ingest_source(source, fmt=fmt, since=since))
        except Exception as exc:  # noqa: BLE001
            failures.append({"source": source, "error": f"{type(exc).__name__}: {exc}"})
    if failures:
        print("Ingestion failures:", failures)
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Bronze ingestion")
    ap.add_argument("--source", default="all")
    ap.add_argument("--format", default="csv", choices=["csv", "json", "api"])
    ap.add_argument("--since", default=None)
    args = ap.parse_args()

    if args.source == "all":
        out = ingest_all(args.format, args.since)
    else:
        out = [ingest_source(args.source, args.format, args.since)]
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
