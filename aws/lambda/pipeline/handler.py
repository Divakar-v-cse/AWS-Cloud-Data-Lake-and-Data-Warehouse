"""Pipeline orchestrator Lambda: bronze ingestion + Glue chain control.

Two responsibilities, dispatched on the ``task`` field of the event:

  task=ingest      land every source in bronze, then start bronze->silver
  task=run_gold    start silver->gold (triggered by the bronze job SUCCEEDED)

The final stage (Redshift load) is a separate function, triggered directly by
EventBridge on the gold job's SUCCEEDED state, so this function never needs
lambda:InvokeFunction.

Why this file does not import pandas
------------------------------------
The local pipeline (``src/ingest.py``) uses pandas because it runs next to a
full scientific stack. A Lambda package should not ship a 60 MB numpy/pandas
wheel to copy bytes from A to B, so the HTTP and replay channels here use only
``boto3`` (provided by the runtime) and the standard library. The *contract* is
identical: same key layout, same metadata sidecar, same required-column check.
The heavy lifting (typing, de-duplication, Parquet) happens later in Glue, which
does have Spark.

Two ingestion channels
----------------------
``PARTNER_API_BASE_URL`` set
    GET ``{base}/{source}?limit=...&since=...`` and store the returned records as
    newline-delimited JSON. This is the production channel and matches
    ``src/ingest.py --format api``.

``PARTNER_API_BASE_URL`` empty
    Replay ``s3://{ARTIFACTS_BUCKET}/raw/{source}/{source}.csv``, which
    ``scripts/deploy_aws.py`` uploads from the local ``data/raw`` tree. This is
    the channel used when the partner API is not reachable from AWS (the local
    mock API binds to 127.0.0.1:8100, so a deployed Lambda cannot call it).
    Bytes are copied unchanged: bronze is the untouched landing zone either way.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
glue = boto3.client("glue")

BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
ARTIFACTS_BUCKET = os.environ.get("ARTIFACTS_BUCKET", "")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")
PARTNER_API_BASE_URL = os.environ.get("PARTNER_API_BASE_URL", "").rstrip("/")
PARTNER_API_SECRET_ARN = os.environ.get("PARTNER_API_SECRET_ARN", "")
BRONZE_JOB = os.environ["BRONZE_JOB_NAME"]
GOLD_JOB = os.environ["GOLD_JOB_NAME"]
DEFAULT_SOURCES = "customers,products,orders,order_items,payments,shipments,returns"

# source -> (incremental timestamp column or None, is_snapshot)
# Mirrors src/ingest.py SOURCES so the two pipelines cannot drift silently.
SOURCES: dict[str, tuple[str | None, bool]] = {
    "customers": ("signup_date", True),
    "products": ("launch_date", True),
    "orders": ("updated_at", False),
    "order_items": (None, False),
    "payments": ("payment_ts", False),
    "shipments": ("shipped_ts", False),
    "returns": ("return_ts", False),
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


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _log(msg: str, **fields) -> None:
    print(json.dumps({"msg": msg, **fields}, default=str))


def _api_key() -> str | None:
    if not PARTNER_API_SECRET_ARN:
        return None
    sm = boto3.client("secretsmanager")
    raw = sm.get_secret_value(SecretId=PARTNER_API_SECRET_ARN)["SecretString"]
    try:
        return json.loads(raw).get("api_key") or raw
    except json.JSONDecodeError:
        return raw


def _http_get_json(url: str, api_key: str | None) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    if api_key:
        req.add_header("X-Api-Key", api_key)
    with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def _fetch_from_api(source: str, since: str | None) -> tuple[bytes, str, list[str], int]:
    """Returns (body, extension, columns, row_count)."""
    query = "?limit=500000" + (f"&since={since}" if since else "")
    payload = _http_get_json(f"{PARTNER_API_BASE_URL}/{source}{query}", _api_key())
    records = payload.get("records", payload if isinstance(payload, list) else [])
    if not records:
        return b"", "json", [], 0
    columns = list(records[0].keys())
    body = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
    return body, "json", columns, len(records)


def _fetch_from_s3(source: str) -> tuple[bytes, str, list[str], int]:
    """Replay the raw export uploaded by deploy_aws.py. Bytes are not altered."""
    for ext in ("csv", "json"):
        key = f"{RAW_PREFIX}{source}/{source}.{ext}"
        try:
            body = s3.get_object(Bucket=ARTIFACTS_BUCKET, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:
            continue
        if ext == "csv":
            text = body.decode("utf-8-sig", errors="replace")
            columns = next(csv.reader(io.StringIO(text)), [])
            rows = max(sum(1 for _ in csv.reader(io.StringIO(text))) - 1, 0)
        else:
            columns = []
            rows = 0
            for line in body.decode("utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                rows += 1
                if not columns:
                    columns = list(json.loads(line).keys())
        return body, ext, columns, rows
    raise FileNotFoundError(
        f"no replay export at s3://{ARTIFACTS_BUCKET}/{RAW_PREFIX}{source}/{source}.csv"
    )


def _write_bronze(source: str, body: bytes, ext: str, columns: list[str],
                  rows: int, channel: str, ingest_date: str) -> dict:
    key = f"{source}/ingest_date={ingest_date}/{source}.{ext}"
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body,
        ContentType="text/csv" if ext == "csv" else "application/x-ndjson",
        ServerSideEncryption="AES256",
    )
    ts_col, is_snapshot = SOURCES[source]
    meta = {
        "source": source,
        "channel": channel,
        "ingest_date": ingest_date,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": rows,
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "format": ext,
        "column_names": columns,
        "primary_key": REQUIRED_COLUMNS[source][0],
        "incremental": not is_snapshot,
        "watermark_column": ts_col,
        "writer": "lambda/pipeline",
    }
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=f"{source}/ingest_date={ingest_date}/_metadata.json",
        Body=json.dumps(meta, indent=2).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return {"source": source, "rows": rows, "bytes": len(body),
            "object": f"s3://{BRONZE_BUCKET}/{key}", "channel": channel}


# ---------------------------------------------------------------------------
# tasks
# ---------------------------------------------------------------------------
def task_ingest(event: dict) -> dict:
    ingest_date = event.get("ingest_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    since = event.get("since")
    sources = [s.strip() for s in (event.get("sources") or DEFAULT_SOURCES).split(",") if s.strip()]

    channel = "api" if PARTNER_API_BASE_URL else "replay"
    results, failures = [], []

    for source in sources:
        if source not in SOURCES:
            failures.append({"source": source, "error": "unknown source"})
            continue
        try:
            if channel == "api":
                body, ext, columns, rows = _fetch_from_api(source, since)
            else:
                body, ext, columns, rows = _fetch_from_s3(source)

            missing = [c for c in REQUIRED_COLUMNS[source] if c not in columns]
            if missing:
                raise ValueError(f"missing required columns {missing}")

            if rows == 0:
                _log("empty extract; skipping", source=source, channel=channel)
                results.append({"source": source, "rows": 0, "skipped": True})
                continue

            results.append(_write_bronze(source, body, ext, columns, rows, channel, ingest_date))
        except Exception as exc:  # noqa: BLE001 - one bad source must not stop the rest
            _log("ingest failed", source=source, error=f"{type(exc).__name__}: {exc}")
            failures.append({"source": source, "error": f"{type(exc).__name__}: {exc}"})

    written = [r for r in results if not r.get("skipped")]
    started = None
    if written:
        run = glue.start_job_run(
            JobName=BRONZE_JOB,
            Arguments={"--ingest_date": ingest_date},
        )
        started = run["JobRunId"]
        _log("started bronze->silver", job=BRONZE_JOB, job_run_id=started,
             sources=[r["source"] for r in written])

    if failures:
        # Fail loudly: EventBridge will not advance the chain on an error.
        raise RuntimeError(json.dumps({"channel": channel, "failures": failures,
                                       "succeeded": len(written)}))

    return {"task": "ingest", "channel": channel, "ingest_date": ingest_date,
            "sources_written": len(written), "rows": sum(r["rows"] for r in written),
            "bronze_job": BRONZE_JOB, "job_run_id": started, "results": results}


def task_run_gold(event: dict) -> dict:
    detail = event.get("detail", {})
    run = glue.start_job_run(
        JobName=GOLD_JOB,
        Arguments={"--source_job_run_id": detail.get("jobRunId", "manual")},
    )
    _log("started silver->gold", job=GOLD_JOB, job_run_id=run["JobRunId"])
    return {"task": "run_gold", "gold_job": GOLD_JOB, "job_run_id": run["JobRunId"],
            "triggered_by": detail.get("jobName", "manual")}


def lambda_handler(event, context):  # noqa: ANN001, ANN201
    task = (event or {}).get("task")
    if not task and (event or {}).get("source") == "aws.glue":
        task = "run_gold" if event["detail"].get("jobName") == BRONZE_JOB else None
    task = task or "ingest"

    _log("invoked", task=task, request_id=getattr(context, "aws_request_id", None))
    if task == "ingest":
        return task_ingest(event or {})
    if task == "run_gold":
        return task_run_gold(event or {})
    raise ValueError(f"unknown task '{task}'")
