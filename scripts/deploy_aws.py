"""Deploy the Divakar Cloud Data Lake and Warehouse to a real AWS account.

This script is the single entry point for the cloud deployment. It performs, in
order:

  1. identity check          sts:GetCallerIdentity - refuses to continue otherwise
  2. stack 1  storage        4 S3 buckets + 3 Glue Data Catalog databases
  3. artifact upload         Glue PySpark scripts, load SQL, raw replay exports
  4. stack 2  iam            least-privilege roles (glue / ingest / loader /
                             pipeline / redshift-copy)
  5. stack 3  redshift       Redshift Serverless namespace + workgroup
  6. stack 2  update         inject the managed admin-password secret ARN
  7. stack 4  glue           2 PySpark jobs + gold crawler
  8. lambda packages         build the two deployment zips and upload them
  9. stack 5  runtime        Lambda functions, EventBridge rules, alarms,
                             SNS topic, CloudWatch dashboard

Step 6 exists because of a genuine circular dependency: the Redshift namespace
wants the COPY role ARN, and the loader role wants the namespace's secret ARN.
The cycle is broken by creating stack 2 without the secret, then updating it.

Nothing here fabricates success. Every stack is created through CloudFormation
and the resulting resource identifiers are read back from the stack outputs and
from the service APIs, then written to .aws-deploy.json.

Usage
-----
    python scripts/deploy_aws.py --plan          # show what would happen
    python scripts/deploy_aws.py                 # deploy
    python scripts/deploy_aws.py --run-pipeline  # deploy, then trigger one run
    python scripts/deploy_aws.py --env prod --region us-east-1 --alert-email me@x.com

Prerequisites
-------------
Valid AWS credentials with the permissions in aws/iam/deploy-policy.json.
Run `python scripts/aws_preflight.py` first; it verifies the same identity call
this script depends on.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CFN_DIR = ROOT / "aws" / "cfn"
LAMBDA_DIR = ROOT / "aws" / "lambda"
GLUE_DIR = ROOT / "aws" / "glue"
SQL_DIR = ROOT / "sql"
RAW_DIR = ROOT / "data" / "raw"
BUILD_DIR = ROOT / "build"
STATE_FILE = ROOT / ".aws-deploy.json"

STACK_NAMES = {
    "storage": "divakar-01-storage",
    "iam": "divakar-02-iam",
    "redshift": "divakar-03-redshift",
    "glue": "divakar-04-glue",
    "runtime": "divakar-05-runtime",
}

FINAL_STATES_OK = {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"}
FAILED_STATES = {
    "CREATE_FAILED", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED",
    "UPDATE_ROLLBACK_FAILED", "DELETE_FAILED",
}

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    sys.exit("boto3 is required: pip install boto3")


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[deploy] {msg}", flush=True)


def banner(msg: str) -> None:
    print("\n" + "=" * 74)
    print(msg)
    print("=" * 74, flush=True)


def account_id(session) -> str:  # noqa: ANN001
    return session.client("sts").get_caller_identity()["Account"]


def bucket_names(env: str, account: str, region: str) -> dict[str, str]:
    base = f"divakar-lake-{{layer}}-{env}-{account}-{region}"
    return {layer: base.format(layer=layer)
            for layer in ("bronze", "silver", "gold", "artifacts")}


def public_ip() -> str | None:
    try:
        with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=5) as r:  # noqa: S310
            return r.read().decode().strip()
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# CloudFormation
# ---------------------------------------------------------------------------
class Stacks:
    def __init__(self, session, env: str) -> None:
        self.cfn = session.client("cloudformation")
        self.env = env
        self.results: dict[str, dict] = {}

    def exists(self, name: str) -> str | None:
        try:
            stacks = self.cfn.describe_stacks(StackName=name)["Stacks"]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return None
            raise
        return stacks[0]["StackStatus"] if stacks else None

    def outputs(self, key: str) -> dict[str, str]:
        name = STACK_NAMES[key]
        stack = self.cfn.describe_stacks(StackName=name)["Stacks"][0]
        return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}

    def deploy(self, key: str, params: dict[str, str], plan: bool = False) -> dict[str, str]:
        name = STACK_NAMES[key]
        template = CFN_DIR / {
            "storage": "10-storage.yaml",
            "iam": "20-iam.yaml",
            "redshift": "30-redshift.yaml",
            "glue": "40-glue.yaml",
            "runtime": "50-serverless.yaml",
        }[key]
        body = template.read_text(encoding="utf-8")
        overrides = [{"ParameterKey": k, "ParameterValue": str(v)}
                     for k, v in params.items()]
        status = self.exists(name)
        action = "create" if status is None else "update"

        if plan:
            log(f"PLAN {name}: would {action} ({template.name}) with "
                f"{len(params)} parameters")
            return {}

        if status is None:
            log(f"creating {name} ...")
            self.cfn.create_stack(
                StackName=name,
                TemplateBody=body,
                Parameters=overrides,
                Capabilities=["CAPABILITY_NAMED_IAM"],
                Tags=[{"Key": "Project", "Value": "divakar-data-platform"},
                      {"Key": "Environment", "Value": self.env}],
                OnFailure="DELETE",
            )
        elif status in FAILED_STATES:
            log(f"{name} is in {status}; deleting before re-creating")
            self.cfn.delete_stack(StackName=name)
            self._wait(name, ("DELETE_COMPLETE",))
            self.cfn.create_stack(
                StackName=name, TemplateBody=body, Parameters=overrides,
                Capabilities=["CAPABILITY_NAMED_IAM"], OnFailure="DELETE",
            )
        else:
            log(f"updating {name} (current {status}) ...")
            try:
                self.cfn.update_stack(
                    StackName=name, TemplateBody=body, Parameters=overrides,
                    Capabilities=["CAPABILITY_NAMED_IAM"],
                )
            except ClientError as exc:
                if "No updates are to be performed" in str(exc):
                    log(f"{name}: no changes")
                    self.results[key] = self._snapshot(name)
                    return self.outputs(key)
                raise

        stack = self._wait(name, FINAL_STATES_OK)
        self.results[key] = self._snapshot(name)
        log(f"{name}: {stack}")
        return self.outputs(key)

    def _snapshot(self, name: str) -> dict:
        s = self.cfn.describe_stacks(StackName=name)["Stacks"][0]
        return {"stack_id": s["StackId"], "status": s["StackStatus"],
                "outputs": {o["OutputKey"]: o["OutputValue"] for o in s.get("Outputs", [])}}

    def _wait(self, name: str, ok: set[str]) -> str:
        deadline = time.time() + 3600
        last = None
        while time.time() < deadline:
            try:
                status = self.cfn.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"]
            except ClientError:
                status = "DELETE_COMPLETE"
            if status != last:
                log(f"  {name}: {status}")
                last = status
            if status in ok or status == "DELETE_COMPLETE":
                return status
            if status.endswith("_FAILED") or status == "ROLLBACK_COMPLETE":
                events = self.cfn.describe_stack_events(StackName=name)["StackEvents"]
                for ev in events[:8]:
                    if "FAILED" in str(ev.get("ResourceStatus", "")):
                        log(f"  !! {ev['LogicalResourceId']}: {ev.get('ResourceStatusReason')}")
                raise RuntimeError(f"{name} failed with {status}")
            time.sleep(5)
        raise TimeoutError(f"{name} did not settle within 60 minutes")


# ---------------------------------------------------------------------------
# artifacts
# ---------------------------------------------------------------------------
def upload_artifacts(session, bucket: str, env: str, region: str, plan: bool) -> dict:
    s3 = session.client("s3")
    uploaded: list[str] = []

    def put(key: str, body: bytes, ctype: str) -> None:
        if plan:
            uploaded.append(f"(plan) s3://{bucket}/{key}  {len(body)} bytes")
            return
        s3.put_object(Bucket=bucket, Key=key, Body=body,
                      ContentType=ctype, ServerSideEncryption="AES256")
        uploaded.append(f"s3://{bucket}/{key}  {len(body)} bytes")

    banner("3/9  uploading Glue scripts, load SQL and raw replay exports")
    for script in sorted(GLUE_DIR.glob("*.py")):
        put(f"glue/{script.name}", script.read_bytes(), "text/x-python")
    for sql in ("redshift_load.sql", "redshift_ddl.sql"):
        path = SQL_DIR / sql
        if path.exists():
            put(f"sql/{sql}", path.read_bytes(), "text/plain")

    for source_dir in sorted(p for p in RAW_DIR.glob("*") if p.is_dir()):
        for ext in ("csv", "json"):
            src = source_dir / f"{source_dir.name}.{ext}"
            if src.exists():
                put(f"raw/{source_dir.name}/{src.name}", src.read_bytes(),
                    "text/csv" if ext == "csv" else "application/x-ndjson")
    return {"uploaded": uploaded}


def build_lambda_zip(name: str, extra: list[Path] | None = None) -> Path:
    BUILD_DIR.mkdir(exist_ok=True)
    target = BUILD_DIR / f"{name}.zip"
    source_dir = LAMBDA_DIR / name
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(source_dir.glob("*.py")):
            zf.write(path, path.name)
        for path in extra or []:
            if path.exists():
                zf.write(path, path.name)
    return target


def upload_lambdas(session, bucket: str, plan: bool) -> dict:
    s3 = session.client("s3")
    banner("8/9  packaging Lambda functions")
    out = {}
    for name, extra in (("pipeline", []),
                        ("loader", [SQL_DIR / "redshift_load.sql"])):
        if plan:
            out[name] = f"(plan) s3://{bucket}/lambda/{name}.zip"
            log(f"PLAN would package aws/lambda/{name}")
            continue
        archive = build_lambda_zip(name, extra)
        key = f"lambda/{name}.zip"
        s3.put_object(Bucket=bucket, Key=key, Body=archive.read_bytes(),
                      ContentType="application/zip", ServerSideEncryption="AES256")
        out[name] = f"s3://{bucket}/{key} ({archive.stat().st_size} bytes)"
        log(f"packaged {archive.name} ({archive.stat().st_size} bytes) -> s3://{bucket}/{key}")
    return out


# ---------------------------------------------------------------------------
# network + warehouse lookups
# ---------------------------------------------------------------------------
def default_network(session) -> tuple[str, str, str]:
    ec2 = session.client("ec2")
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise RuntimeError(
            "No default VPC found. Redshift Serverless needs a VPC with two "
            "subnets in different AZs. Create one and pass --vpc-id/--subnet-ids.")
    vpc_id = vpcs[0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
    by_az: dict[str, str] = {}
    for s in sorted(subnets, key=lambda s: s["AvailabilityZone"]):
        by_az.setdefault(s["AvailabilityZone"], s["SubnetId"])
    if len(by_az) < 2:
        raise RuntimeError(f"VPC {vpc_id} has subnets in {len(by_az)} AZ(s); 2 are required")
    azs = sorted(by_az)[:2]
    return vpc_id, by_az[azs[0]], by_az[azs[1]]


def redshift_details(session, workgroup: str, namespace: str) -> dict:
    rs = session.client("redshift-serverless")
    detail: dict = {"workgroup": workgroup, "namespace": namespace}
    for attempt in range(12):
        wg = rs.get_workgroup(workgroupName=workgroup)["workgroup"]
        ns = rs.get_namespace(namespaceName=namespace)["namespace"]
        endpoint = wg.get("endpoint") or {}
        detail.update({
            "host": endpoint.get("address", ""),
            "port": endpoint.get("port", 5439),
            "database": ns.get("dbName", "analytics"),
            "admin_username": ns.get("adminUsername", ""),
            "secret_arn": ns.get("adminPasswordSecretArn", ""),
            "status": wg.get("status", ""),
            "base_capacity": wg.get("baseCapacity"),
        })
        if detail["host"]:
            return detail
        log(f"  waiting for the workgroup endpoint (attempt {attempt + 1}/12, status {detail['status']})")
        time.sleep(10)
    return detail


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default=os.getenv("DIVAKAR_ENV", "dev"))
    ap.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-south-1")
    ap.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    ap.add_argument("--plan", action="store_true", help="print the plan and exit")
    ap.add_argument("--run-pipeline", action="store_true",
                    help="invoke the ingest Lambda once after deploying")
    ap.add_argument("--alert-email", default="", help="SNS alert recipient")
    ap.add_argument("--partner-api-url", default="",
                    help="partner REST API base URL; empty uses the raw replay channel")
    ap.add_argument("--allowed-cidr", default="", help="CIDR allowed to reach Redshift 5439")
    ap.add_argument("--no-schedule", action="store_true",
                    help="deploy the runtime with the daily schedule disabled")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--base-capacity", type=int, default=8)
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    banner("1/9  verifying AWS identity")
    try:
        ident = session.client("sts").get_caller_identity()
    except Exception as exc:  # noqa: BLE001
        print(f"FAILED: {type(exc).__name__}: {exc}")
        print("\nNo valid AWS credentials. Nothing was created.")
        print("Follow docs/AWS-DEPLOYMENT.md section 'Connecting an AWS account'.")
        return 2
    account = ident["Account"]
    log(f"account {account}  arn {ident['Arn']}  region {args.region}")

    buckets = bucket_names(args.env, account, args.region)
    cidr = args.allowed_cidr or (f"{public_ip()}/32" if public_ip() else "0.0.0.0/0")
    stacks = Stacks(session, args.env)
    state: dict = {
        "deployed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "region": args.region,
        "account_id": account,
        "caller_arn": ident["Arn"],
        "environment": args.env,
        "buckets": buckets,
    }

    banner("2/9  stack 1 of 5 - S3 lake + Glue Data Catalog")
    stacks.deploy("storage", {"Environment": args.env}, args.plan)

    if not args.plan:
        upload_artifacts(session, buckets["artifacts"], args.env, args.region, False)
    else:
        upload_artifacts(session, buckets["artifacts"], args.env, args.region, True)

    banner("4/9  stack 2 of 5 - IAM roles (first pass, no admin secret yet)")
    iam_out = stacks.deploy("iam", {
        "Environment": args.env,
        "BronzeBucket": buckets["bronze"],
        "SilverBucket": buckets["silver"],
        "GoldBucket": buckets["gold"],
        "ArtifactsBucket": buckets["artifacts"],
        "PartnerApiSecretArn": "",
        "AdminSecretArn": "",
    }, args.plan)
    copy_role = iam_out.get("RedshiftCopyRoleArn", "")

    banner("5/9  stack 3 of 5 - Redshift Serverless")
    if args.plan:
        log("PLAN would resolve the default VPC and two subnets in different AZs")
        rs_out = {}
    else:
        vpc_id, subnet1, subnet2 = default_network(session)
        log(f"vpc {vpc_id}  subnets {subnet1} / {subnet2}  allowed cidr {cidr}")
        rs_out = stacks.deploy("redshift", {
            "Environment": args.env,
            "VpcId": vpc_id,
            "SubnetId1": subnet1,
            "SubnetId2": subnet2,
            "AllowedCidr": cidr,
            "AdminUsername": "deuser",
            "DatabaseName": "analytics",
            "BaseCapacity": args.base_capacity,
            "CopyRoleArn": copy_role,
            "PubliclyAccessible": "true",
        }, args.plan)
        state["network"] = {"vpc_id": vpc_id, "subnets": [subnet1, subnet2],
                            "allowed_cidr": cidr}

    banner("6/9  stack 2 of 5 - IAM update with the managed admin secret")
    if args.plan:
        log("PLAN would read the namespace secret ARN and update stack 2")
        warehouse = {"workgroup": f"divakar-{args.env}",
                     "namespace": f"divakar-{args.env}",
                     "database": "analytics", "admin_username": "deuser",
                     "host": "(resolved after deploy)", "port": 5439,
                     "secret_arn": "(resolved after deploy)"}
    else:
        warehouse = redshift_details(session, rs_out.get("WorkgroupName", f"divakar-{args.env}"),
                                     rs_out.get("NamespaceName", f"divakar-{args.env}"))
        warehouse["database"] = rs_out.get("DatabaseName", warehouse.get("database", "analytics"))
        warehouse["admin_username"] = rs_out.get("AdminUsername", warehouse.get("admin_username", "deuser"))
        stacks.deploy("iam", {
            "Environment": args.env,
            "BronzeBucket": buckets["bronze"],
            "SilverBucket": buckets["silver"],
            "GoldBucket": buckets["gold"],
            "ArtifactsBucket": buckets["artifacts"],
            "PartnerApiSecretArn": "",
            "AdminSecretArn": warehouse.get("secret_arn", ""),
        }, args.plan)
        log(f"warehouse {warehouse['host']}:{warehouse['port']}/{warehouse['database']} "
            f"selected secret={bool(warehouse.get('secret_arn'))}")
    state["warehouse"] = warehouse

    banner("7/9  stack 4 of 5 - Glue jobs + gold crawler")
    glue_out = stacks.deploy("glue", {
        "Environment": args.env,
        "ScriptsBucket": buckets["artifacts"],
        "ScriptsPrefix": "glue",
        "GlueRoleArn": iam_out.get("GlueRoleArn", ""),
        "BronzeBucket": buckets["bronze"],
        "SilverBucket": buckets["silver"],
        "GoldBucket": buckets["gold"],
        "GoldDatabaseName": f"divakar_gold_{args.env}",
        "WorkerType": "G.1X",
        "NumberOfWorkers": args.workers,
        "JobTimeoutMinutes": 60,
    }, args.plan)
    bronze_job = glue_out.get("BronzeToSilverJobName", f"divakar-bronze-to-silver-{args.env}")
    gold_job = glue_out.get("SilverToGoldJobName", f"divakar-silver-to-gold-{args.env}")
    crawler = glue_out.get("GoldCrawlerName", f"divakar-gold-crawler-{args.env}")

    banner("8/9  packaging Lambda functions")
    loaders = upload_lambdas(session, buckets["artifacts"], args.plan)

    banner("9/9  stack 5 of 5 - Lambda, EventBridge, CloudWatch")
    rt_out = stacks.deploy("runtime", {
        "Environment": args.env,
        "ArtifactsBucket": buckets["artifacts"],
        "LambdaPrefix": "lambda",
        "PipelineCodeKey": "pipeline.zip",
        "LoaderCodeKey": "loader.zip",
        "PipelineRoleArn": iam_out.get("PipelineRoleArn", ""),
        "LoaderRoleArn": iam_out.get("LoaderRoleArn", ""),
        "BronzeBucket": buckets["bronze"],
        "GoldBucket": buckets["gold"],
        "BronzeJobName": bronze_job,
        "GoldJobName": gold_job,
        "GoldCrawlerName": crawler,
        "WorkgroupName": warehouse.get("workgroup", f"divakar-{args.env}"),
        "DatabaseName": warehouse.get("database", "analytics"),
        "DbUser": warehouse.get("admin_username", "deuser"),
        "RedshiftSecretArn": warehouse.get("secret_arn", ""),
        "CopyRoleArn": copy_role,
        "PartnerApiBaseUrl": args.partner_api_url,
        "PartnerApiSecretArn": "",
        "IngestSchedule": "cron(0 1 * * ? *)",
        "ScheduleEnabled": "false" if args.no_schedule else "true",
        "AlertEmail": args.alert_email,
        "LogRetentionDays": 30,
    }, args.plan)

    state.update({
        "glue": {"bronze_to_silver": bronze_job, "silver_to_gold": gold_job,
                 "gold_crawler": crawler,
                 "gold_database": f"divakar_gold_{args.env}"},
        "stacks": stacks.results,
        "lambda": {"pipeline": rt_out.get("PipelineFunctionName", ""),
                   "loader": rt_out.get("LoaderFunctionName", ""),
                   "packages": loaders},
        "schedule": {"rule": rt_out.get("IngestScheduleRuleName", ""),
                     "expression": "cron(0 1 * * ? *)",
                     "enabled": not args.no_schedule},
        "alerts": {"topic_arn": rt_out.get("AlertsTopicArn", "")},
        "dashboard": {"name": rt_out.get("DashboardName", ""),
                      "url": rt_out.get("DashboardUrl", "")},
        "ingestion_channel": "api" if args.partner_api_url else "replay",
    })

    if not args.plan:
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
        log(f"wrote {STATE_FILE.name}")

    if args.plan:
        banner("PLAN COMPLETE - nothing was created")
        return 0

    if args.run_pipeline:
        banner("triggering one end-to-end run")
        lam = session.client("lambda")
        resp = lam.invoke(FunctionName=state["lambda"]["pipeline"],
                          InvocationType="RequestResponse",
                          Payload=json.dumps({"task": "ingest"}).encode())
        payload = resp["Payload"].read().decode()
        log(f"ingest response: {payload[:1500]}")
        state["first_run"] = {"ingest_response": payload[:4000]}
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")

    banner("DEPLOYMENT COMPLETE")
    print(json.dumps({k: state[k] for k in
                      ("region", "account_id", "buckets", "warehouse", "glue",
                       "lambda", "schedule", "dashboard")}, indent=2))
    print("\nNext: python scripts/aws_verify.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
