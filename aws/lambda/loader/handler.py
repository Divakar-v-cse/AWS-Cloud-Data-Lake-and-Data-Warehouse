"""Redshift loader Lambda: executes sql/redshift_load.sql against Redshift
Serverless through the Amazon Redshift Data API.

Why the Data API and not psycopg2
---------------------------------
* No database driver or native build in the deployment package.
* No long-lived password: the namespace is created with
  ``ManageAdminPassword: true``, so AWS owns the credential in Secrets Manager
  and rotates it. This function only ever sees the secret ARN.
* No VPC attachment, so the function does not need NAT gateways to run.

The statement protocol
----------------------
The Data API executes exactly one statement per call, so the SQL file is split
on ``-- @@ <statement_name>`` marker lines. Each chunk is submitted in order and
polled to completion. A chunk that does not reach FINISHED aborts the load and
the failing statement name is reported, which is far more useful than a single
opaque "load failed".

Templating
----------
``{{GOLD_BUCKET}}`` and ``{{COPY_ROLE_ARN}}`` are substituted from the
environment before submission. No other substitution is supported on purpose:
the SQL stays inspectable and diffable outside the function.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

rs = boto3.client("redshift-data")
glue = boto3.client("glue")
s3 = boto3.client("s3")

WORKGROUP = os.environ["WORKGROUP_NAME"]
DATABASE = os.environ.get("DATABASE_NAME", "analytics")
DB_USER = os.environ.get("DB_USER", "")
SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")
GOLD_BUCKET = os.environ["GOLD_BUCKET"]
COPY_ROLE_ARN = os.environ["COPY_ROLE_ARN"]
CRAWLER_NAME = os.environ.get("GOLD_CRAWLER_NAME", "")
OPS_PREFIX = os.environ.get("OPS_PREFIX", "_ops/load_runs/")

SQL_PATH = Path(__file__).with_name("redshift_load.sql")
MAX_WAIT_SECONDS = int(os.environ.get("STATEMENT_TIMEOUT_SECONDS", "600"))
POLL_INTERVAL = 2.0
TERMINAL = {"FINISHED", "FAILED", "ABORTED"}


def _log(msg: str, **fields) -> None:
    print(json.dumps({"msg": msg, **fields}, default=str))


def _lit(value: str | None) -> str:
    """Quote a Python string for inclusion in a SQL literal."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# statement parsing
# ---------------------------------------------------------------------------
def load_statements(path: Path = SQL_PATH) -> list[tuple[str, str]]:
    """Return [(statement_name, sql), ...] in file order."""
    if not path.exists():
        raise FileNotFoundError(f"{path} is missing from the deployment package")
    statements: list[tuple[str, str]] = []
    name: str | None = None
    buffer: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("-- @@"):
            if name is not None:
                statements.append((name, "\n".join(buffer).strip()))
            name = line[len("-- @@"):].strip()
            buffer = []
            continue
        if name is not None:
            buffer.append(line)
    if name is not None:
        statements.append((name, "\n".join(buffer).strip()))
    # Drop chunks that contain no executable SQL (comments only).
    cleaned = []
    for stmt_name, sql in statements:
        body = "\n".join(
            ln for ln in sql.splitlines() if not ln.strip().startswith("--")
        ).strip()
        if body:
            cleaned.append((stmt_name, sql))
    return cleaned


def render(sql: str) -> str:
    return (sql.replace("{{GOLD_BUCKET}}", GOLD_BUCKET)
               .replace("{{COPY_ROLE_ARN}}", COPY_ROLE_ARN))


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------
def _submit(sql: str) -> str:
    kwargs = {
        "WorkgroupName": WORKGROUP,
        "Database": DATABASE,
        "Sql": sql,
        "WithEvent": False,
    }
    if SECRET_ARN:
        kwargs["SecretArn"] = SECRET_ARN
    elif DB_USER:
        kwargs["DbUser"] = DB_USER
    return rs.execute_statement(**kwargs)["Id"]


def _wait(statement_id: str, timeout: int = MAX_WAIT_SECONDS) -> dict:
    deadline = time.time() + timeout
    while True:
        desc = rs.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status in TERMINAL:
            return desc
        if time.time() > deadline:
            try:
                rs.cancel_statement(Id=statement_id)
            except Exception:  # noqa: BLE001 - best effort cancel
                pass
            raise TimeoutError(
                f"statement {statement_id} still {status} after {timeout}s")
        time.sleep(POLL_INTERVAL)


def run_sql(sql: str, label: str) -> dict:
    started = time.time()
    statement_id = _submit(sql)
    desc = _wait(statement_id)
    elapsed = round(time.time() - started, 2)
    if desc["Status"] != "FINISHED":
        raise RuntimeError(
            f"[{label}] {desc['Status']}: {desc.get('Error', 'no error message')}")
    result = {
        "statement": label,
        "status": desc["Status"],
        "seconds": elapsed,
        "rows": desc.get("ResultRows", 0),
        "statement_id": statement_id,
    }
    _log("statement finished", **result)
    return result


# ---------------------------------------------------------------------------
# ops bookkeeping (best effort: never fail a successful load over telemetry)
# ---------------------------------------------------------------------------
def _ops_insert(run_id: str, started_at: datetime) -> None:
    sql = (
        "INSERT INTO ops.pipeline_runs "
        "(run_id, pipeline, stage, source, status, started_at, details) VALUES ("
        f"{_lit(run_id)}, 'aws_pipeline', 'warehouse_load', 'redshift', 'running', "
        f"{_lit(started_at.isoformat(timespec='seconds'))}, "
        "JSON_PARSE('{\"executor\":\"lambda/loader\"}'))"
    )
    try:
        run_sql(sql, "ops_insert")
    except Exception as exc:  # noqa: BLE001
        _log("ops insert skipped", error=f"{type(exc).__name__}: {exc}")


def _ops_update(run_id: str, status: str, records: int, error: str | None,
                finished_at: datetime) -> None:
    sql = (
        "UPDATE ops.pipeline_runs SET "
        f"status = {_lit(status)}, "
        f"finished_at = {_lit(finished_at.isoformat(timespec='seconds'))}, "
        f"records_out = {int(records)}, "
        f"error_message = {_lit(error)} "
        f"WHERE run_id = {_lit(run_id)}"
    )
    try:
        run_sql(sql, "ops_update")
    except Exception as exc:  # noqa: BLE001
        _log("ops update skipped", error=f"{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# handler
# ---------------------------------------------------------------------------
def lambda_handler(event, context):  # noqa: ANN001, ANN201
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    detail = (event or {}).get("detail", {})
    _log("load started", run_id=run_id, workgroup=WORKGROUP, database=DATABASE,
         triggered_by=detail.get("jobName", "manual"))

    if CRAWLER_NAME:
        # Registers the refreshed gold datasets for Athena. Not awaited: the
        # warehouse load reads S3 directly and must not wait on the catalog.
        try:
            glue.start_crawler(Name=CRAWLER_NAME)
            _log("crawler started", crawler=CRAWLER_NAME)
        except Exception as exc:  # noqa: BLE001
            _log("crawler not started", crawler=CRAWLER_NAME,
                 error=f"{type(exc).__name__}: {exc}")

    statements = load_statements()
    _log("statements parsed", count=len(statements),
         names=[n for n, _ in statements])

    _ops_insert(run_id, started_at)

    executed: list[dict] = []
    try:
        for name, sql in statements:
            executed.append(run_sql(render(sql), name))
    except Exception as exc:  # noqa: BLE001
        message = f"{type(exc).__name__}: {exc}"
        _log("load failed", run_id=run_id, error=message, executed=executed)
        _ops_update(run_id, "failed", 0, message, datetime.now(timezone.utc))
        raise

    finished_at = datetime.now(timezone.utc)
    total_seconds = round((finished_at - started_at).total_seconds(), 2)
    summary = {
        "run_id": run_id,
        "workgroup": WORKGROUP,
        "database": DATABASE,
        "status": "succeeded",
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "seconds": total_seconds,
        "statements": executed,
        "triggered_by": detail.get("jobName", "manual"),
    }

    _ops_update(run_id, "succeeded", len(executed), None, finished_at)

    if GOLD_BUCKET:
        key = f"{OPS_PREFIX}{started_at.strftime('%Y-%m-%dT%H-%M-%SZ')}-{run_id}.json"
        try:
            s3.put_object(Bucket=GOLD_BUCKET, Key=key,
                          Body=json.dumps(summary, indent=2).encode("utf-8"),
                          ContentType="application/json",
                          ServerSideEncryption="AES256")
            summary["summary_object"] = f"s3://{GOLD_BUCKET}/{key}"
        except Exception as exc:  # noqa: BLE001
            _log("summary upload skipped", error=f"{type(exc).__name__}: {exc}")

    _log("load complete", run_id=run_id, seconds=total_seconds,
         statements=len(executed))
    return summary
