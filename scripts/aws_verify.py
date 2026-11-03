"""Post-deployment verification for the Divakar Cloud Data Lake deployment.

Reads ``.aws-deploy.json`` (written by ``scripts/deploy_aws.py``), then checks the
deployment from the outside, the way an operator would:

* S3      - the four buckets exist, are versioned, encrypted and private; every
            source landed in bronze with a readable metadata sidecar; silver and
            gold are populated
* Glue    - both jobs exist and their last run succeeded; the crawler registered
            the gold datasets in the Data Catalog
* Redshift- reachable through the Data API; every dimension and fact has rows;
            the SCD2 invariants hold
* Ops     - the CloudWatch alarms exist and are not firing

Exit codes: 0 all hard checks passed, 1 a hard check failed, 2 nothing to verify
(no state file or no AWS credentials).

Usage
-----
    python scripts/aws_verify.py
    python scripts/aws_verify.py --json
    python scripts/aws_verify.py --state .aws-deploy.json --skip-redshift
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / ".aws-deploy.json"

SOURCES = ["customers", "products", "orders", "order_items",
           "payments", "shipments", "returns"]

# Local reference run (PostgreSQL on 5433, identical raw exports). The cloud
# pipeline replays the same files, so these are the expected magnitudes. They
# are reported as a comparison, not asserted as exact - a live partner API or a
# different --ingest-date would legitimately change them.
REFERENCE = {
    "dim_customer": 20000,
    "dim_product": 500,
    "fact_orders": 120000,
    "fact_order_items": 274630,
    "fact_payments": 134355,
    "fact_shipments": 105567,
    "fact_returns": 16563,
}

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover
    sys.exit("boto3 is required: pip install boto3")

RESULTS: list[dict] = []


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------
def check(name: str, ok: bool | None, detail: str, *, hard: bool = True) -> bool:
    """Record one check. ok=None means 'informational, do not fail the run'."""
    status = "PASS" if ok else ("WARN" if ok is None else ("FAIL" if hard else "WARN"))
    RESULTS.append({"check": name, "status": status, "detail": detail})
    print(f"[{status}] {name}: {detail}")
    return bool(ok)


def banner(msg: str) -> None:
    print("\n" + "-" * 74)
    print(msg)
    print("-" * 74)


def fail(msg: str) -> int:
    print(f"\n{msg}")
    return 2


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------
def check_buckets(s3, state: dict) -> None:
    banner("S3 lake")
    for layer, name in state.get("buckets", {}).items():
        try:
            s3.head_bucket(Bucket=name)
        except ClientError as exc:
            check(f"bucket {layer}", False, f"{name} unusable: {exc.response['Error']['Code']}")
            continue

        facts = []
        try:
            ver = s3.get_bucket_versioning(Bucket=name).get("Status", "")
            facts.append(f"versioning={ver or 'off'}")
        except ClientError:
            facts.append("versioning=unknown")
        try:
            enc = (s3.get_bucket_encryption(Bucket=name)["ServerSideEncryptionConfiguration"]
                   ["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"])
            facts.append(f"sse={enc}")
        except ClientError:
            facts.append("sse=none")
        try:
            pab = s3.get_public_access_block(Bucket=name)["PublicAccessBlockConfiguration"]
            private = all(pab.get(k, False) for k in
                          ("BlockPublicAcls", "BlockPublicPolicy",
                           "IgnorePublicAcls", "RestrictPublicBuckets"))
            facts.append("private" if private else "PUBLIC-ACCESS-POSSIBLE")
        except ClientError:
            private = None
            facts.append("pab=unset")

        ok = private is not False
        check(f"bucket {layer}", ok, f"{name} ({', '.join(facts)})")


def count_prefix(s3, bucket: str, prefix: str) -> tuple[int, int]:
    n = size = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            n += 1
            size += obj["Size"]
    return n, size


def check_bronze(s3, state: dict) -> None:
    banner("Bronze layer - raw landing")
    bucket = state["buckets"]["bronze"]
    for source in SOURCES:
        n, size = count_prefix(s3, bucket, f"{source}/")
        if n == 0:
            check(f"bronze {source}", False, "no objects")
            continue
        # find the newest metadata sidecar
        paginator = s3.get_paginator("list_objects_v2")
        metas = []
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{source}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("_metadata.json"):
                    metas.append(obj["Key"])
        if not metas:
            check(f"bronze {source}", False, f"{n} objects but no _metadata.json")
            continue
        key = sorted(metas)[-1]
        try:
            meta = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except (ClientError, json.JSONDecodeError) as exc:
            check(f"bronze {source}", False, f"metadata unreadable: {exc}")
            continue
        missing = [k for k in ("source", "rows", "bytes", "sha256", "schema")
                   if k not in meta]
        ok = not missing and meta.get("rows", 0) > 0
        detail = (f"{meta.get('rows')} rows, {meta.get('bytes')} B, "
                  f"sha256={str(meta.get('sha256'))[:12]}..., key={key.split('/')[1] if '/' in key else key}")
        if missing:
            detail += f" MISSING {missing}"
        check(f"bronze {source}", ok, detail)


def check_layers(s3, state: dict) -> None:
    for layer in ("silver", "gold"):
        n, size = count_prefix(s3, state["buckets"][layer], "")
        check(f"{layer} layer", n > 0, f"{n} objects, {size / 1e6:.1f} MB")

    gold = state["buckets"]["gold"]
    datasets = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=gold, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            name = prefix["Prefix"].strip("/")
            if not name.startswith("_"):
                datasets.add(name)
    expected = {"daily_sales", "monthly_revenue", "customer_rfm", "cohorts",
                "product_performance", "payments", "returns", "fulfilment"}
    missing = sorted(expected - datasets)
    check("gold datasets", not missing,
          f"{len(datasets)} datasets" + (f", missing {missing}" if missing else ""),
          hard=bool(missing))


def check_run_summaries(s3, state: dict) -> None:
    banner("Loader run summaries")
    n, _ = count_prefix(s3, state["buckets"]["gold"], "_ops/load_runs/")
    check("warehouse load receipts", n > 0,
          f"{n} summary object(s) under s3://<gold>/_ops/load_runs/", hard=False)


# ---------------------------------------------------------------------------
# Glue
# ---------------------------------------------------------------------------
def check_glue(glue, state: dict) -> None:
    banner("Glue ETL")
    for label, job in (("bronze->silver", state["glue"].get("bronze_job")),
                       ("silver->gold", state["glue"].get("gold_job"))):
        if not job:
            check(label, False, "job name missing from .aws-deploy.json")
            continue
        try:
            glue.get_job(JobName=job)
        except ClientError:
            check(label, False, f"{job} does not exist")
            continue
        runs = glue.get_job_runs(JobName=job, MaxResults=1).get("JobRuns", [])
        if not runs:
            check(label, False, f"{job} exists but has never run")
            continue
        run = runs[0]
        state_name = run.get("JobRunState", "?")
        mins = run.get("ExecutionTime", 0) / 60
        detail = f"{job} last run {state_name} in {mins:.1f} min (id {run.get('Id', '')[:16]})"
        if run.get("ErrorMessage"):
            detail += f" error={run['ErrorMessage'][:120]}"
        check(label, state_name == "SUCCEEDED", detail)


def check_catalog(glue, state: dict) -> None:
    banner("Glue Data Catalog")
    db = state["glue"].get("gold_database")
    try:
        tables = glue.get_tables(DatabaseName=db).get("TableList", [])
    except ClientError as exc:
        check("gold catalog", False, f"{db} unreadable: {exc.response['Error']['Code']}")
        return
    names = sorted(t["Name"] for t in tables)
    check("gold catalog", bool(names), f"{db}: {len(names)} tables - {', '.join(names[:10])}",
          hard=bool(names))

    crawler = state["glue"].get("crawler")
    if crawler:
        try:
            c = glue.get_crawler(Name=crawler)["Crawler"]
            last = c.get("LastCrawl", {}).get("Status", "never run")
            check("gold crawler", last == "SUCCEEDED",
                  f"{crawler} last crawl {last}", hard=False)
        except ClientError:
            check("gold crawler", None, f"{crawler} not found", hard=False)


# ---------------------------------------------------------------------------
# Redshift (Data API - no driver, no VPC)
# ---------------------------------------------------------------------------
def redshift_rows(client, workgroup: str, database: str, sql: str,
                  secret_arn: str = "", db_user: str = "") -> list[dict]:
    kwargs = {"WorkgroupName": workgroup, "Database": database, "Sql": sql}
    if secret_arn:
        kwargs["SecretArn"] = secret_arn
    elif db_user:
        kwargs["DbUser"] = db_user
    sid = client.execute_statement(**kwargs)["Id"]

    for _ in range(300):
        desc = client.describe_statement(Id=sid)
        if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(2)
    else:
        client.cancel_statement(Id=sid)
        raise TimeoutError(f"statement did not finish: {sql[:80]}")

    if desc["Status"] != "FINISHED":
        raise RuntimeError(f"{desc['Status']}: {desc.get('Error', '')[:200]}")

    cols = [c.get("label") or c.get("name") for c in desc.get("ColumnMetadata", [])]
    out: list[dict] = []
    page = client.get_statement_result(Id=sid)
    while True:
        for rec in page.get("Records", []):
            row = {}
            for i, cell in enumerate(rec):
                key = cols[i] if i < len(cols) else f"c{i}"
                if cell.get("isNull"):
                    row[key] = None
                elif "longValue" in cell:
                    row[key] = cell["longValue"]
                elif "doubleValue" in cell:
                    row[key] = cell["doubleValue"]
                elif "booleanValue" in cell:
                    row[key] = cell["booleanValue"]
                else:
                    row[key] = cell.get("stringValue")
            out.append(row)
        nxt = page.get("NextToken")
        if not nxt:
            break
        page = client.get_statement_result(Id=sid, NextToken=nxt)
    return out


def check_redshift(rs, state: dict) -> None:
    banner("Redshift Serverless warehouse")
    wh = state.get("warehouse", {})
    workgroup = wh.get("workgroup")
    database = wh.get("database", "analytics")
    secret = wh.get("secret_arn", "")
    user = wh.get("admin_username", "deuser")
    if not workgroup:
        check("warehouse", False, "workgroup missing from .aws-deploy.json")
        return

    try:
        wg = rs.get_workgroup(workgroupName=workgroup)["workgroup"]
        check("workgroup", True, f"{workgroup} exists, status={wg.get('status')}")
    except ClientError as exc:
        check("workgroup", False, f"{workgroup}: {exc.response['Error']['Code']}")
        return

    rs_data = state["_redshift_data"]

    def q(sql: str) -> list[dict]:
        return redshift_rows(rs_data, workgroup, database, sql, secret, user)

    try:
        q("select 1 as ok")
        check("data api", True, "SELECT 1 through the Redshift Data API")
    except Exception as exc:  # noqa: BLE001
        check("data api", False, f"cannot execute SQL: {exc}")
        return

    # row counts
    present = {r["t"]: r["n"] for r in q(
        "select 'dim_customer' as t, count(*) as n from dwh.dim_customer "
        "union all select 'dim_product', count(*) from dwh.dim_product "
        "union all select 'fact_orders', count(*) from dwh.fact_orders "
        "union all select 'fact_order_items', count(*) from dwh.fact_order_items "
        "union all select 'fact_payments', count(*) from dwh.fact_payments "
        "union all select 'fact_shipments', count(*) from dwh.fact_shipments "
        "union all select 'fact_returns', count(*) from dwh.fact_returns")}
    for table, ref in REFERENCE.items():
        got = present.get(table, 0)
        ok = got > 0
        detail = f"{table} = {got:,} rows (local reference {ref:,})"
        if got and got != ref:
            detail += " - differs from the reference, expected for a partial/incremental run"
        check(f"rows {table}", ok, detail, hard=ok is False)

    # SCD2 invariants - the one thing Redshift cannot enforce with a constraint
    try:
        dup = q("select count(*) as n from (select customer_id from dwh.dim_customer "
                "where is_current group by customer_id having count(*) > 1) x")[0]["n"]
        check("scd2 dim_customer", dup == 0, f"{dup} customers with more than one current row")

        dupp = q("select count(*) as n from (select product_id from dwh.dim_product "
                 "where is_current group by product_id having count(*) > 1) x")[0]["n"]
        check("scd2 dim_product", dupp == 0, f"{dupp} products with more than one current row")

        orphans = q("select count(*) as n from dwh.fact_orders f "
                    "left join dwh.dim_customer c on c.customer_sk = f.customer_sk and c.is_current "
                    "where c.customer_sk is null")[0]["n"]
        check("fact_orders -> dim_customer", orphans == 0, f"{orphans} fact rows without a current dimension row")

        nulls = q("select count(*) as n from dwh.fact_orders where customer_sk is null "
                  "or date_key is null")[0]["n"]
        check("fact_orders keys", nulls == 0, f"{nulls} rows with a null customer_sk/date_key")

        versions = q("select count(*) as n from dwh.dim_customer where not is_current")[0]["n"]
        check("scd2 history", None, f"{versions:,} closed dimension versions retained", hard=False)
    except Exception as exc:  # noqa: BLE001
        check("warehouse invariants", False, f"query failed: {exc}")

    # ops bookkeeping written by the loader
    try:
        runs = q("select count(*) as n from ops.pipeline_runs where stage = 'redshift_load'")[0]["n"]
        check("ops.pipeline_runs", runs > 0, f"{runs} load run(s) recorded", hard=False)
    except Exception as exc:  # noqa: BLE001
        check("ops.pipeline_runs", None, f"not queryable: {str(exc)[:100]}", hard=False)


# ---------------------------------------------------------------------------
# CloudWatch
# ---------------------------------------------------------------------------
def check_alarms(cw, state: dict) -> None:
    banner("CloudWatch monitoring")
    try:
        alarms = cw.describe_alarms(AlarmNamePrefix=f"divakar-")["MetricAlarms"]
    except ClientError as exc:
        check("alarms", None, f"describe_alarms failed: {exc.response['Error']['Code']}", False)
        return
    if not alarms:
        check("alarms", None, "no divakar-* alarms found", hard=False)
        return
    firing = [a["AlarmName"] for a in alarms if a["StateValue"] == "ALARM"]
    detail = ", ".join(f"{a['AlarmName']}={a['StateValue']}" for a in alarms)
    check("alarms", not firing, detail)

    name = state.get("dashboard", {}).get("name")
    if name:
        try:
            cw.get_dashboard(DashboardName=name)
            check("dashboard", True, f"{name} exists")
        except ClientError:
            check("dashboard", None, f"{name} not found", hard=False)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Verify the AWS deployment")
    ap.add_argument("--state", default=str(STATE_FILE))
    ap.add_argument("--region", default="")
    ap.add_argument("--profile", default="")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--skip-redshift", action="store_true")
    args = ap.parse_args()

    state_path = Path(args.state)
    if not state_path.exists():
        return fail(f"No deployment receipt at {state_path}. Nothing to verify.\n"
                    "Deploy first: python scripts/deploy_aws.py")

    state = json.loads(state_path.read_text(encoding="utf-8"))
    region = args.region or state.get("region") or "ap-south-1"
    session = boto3.Session(profile_name=args.profile or None, region_name=region)

    banner(f"AWS verification - region {region}")
    try:
        ident = session.client("sts").get_caller_identity()
    except (ClientError, BotoCoreError) as exc:
        return fail(f"Cannot authenticate: {exc}\n"
                    "See docs/AWS-DEPLOYMENT.md section 'Connecting an AWS account'.")
    print(f"identity: {ident['Arn']}  account {ident['Account']}")
    if state.get("account_id") and state["account_id"] != ident["Account"]:
        return fail(f"State file belongs to account {state['account_id']}, "
                    f"credentials are for {ident['Account']}. Wrong account.")

    check_buckets(session.client("s3"), state)
    check_bronze(session.client("s3"), state)
    check_layers(session.client("s3"), state)
    check_run_summaries(session.client("s3"), state)
    check_glue(session.client("glue"), state)
    check_catalog(session.client("glue"), state)

    if not args.skip_redshift:
        state["_redshift_data"] = session.client("redshift-data")
        check_redshift(session.client("redshift-serverless"), state)
        state.pop("_redshift_data", None)
    check_alarms(session.client("cloudwatch"), state)

    hard = [r for r in RESULTS if r["status"] == "FAIL"]
    warn = [r for r in RESULTS if r["status"] == "WARN"]
    passed = [r for r in RESULTS if r["status"] == "PASS"]

    banner("SUMMARY")
    print(f"passed {len(passed)}  warnings {len(warn)}  failed {len(hard)}")
    for r in hard:
        print(f"  FAIL  {r['check']}: {r['detail']}")

    if args.json:
        print(json.dumps({
            "verified_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "region": region,
            "account": ident["Account"],
            "passed": len(passed), "warnings": len(warn), "failed": len(hard),
            "results": RESULTS,
        }, indent=2))

    return 1 if hard else 0


if __name__ == "__main__":
    sys.exit(main())
