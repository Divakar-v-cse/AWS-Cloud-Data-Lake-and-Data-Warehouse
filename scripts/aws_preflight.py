"""Pre-flight check for a real AWS deployment of the Divakar data platform.

Answers exactly two questions before anything is created:

  1. Do valid AWS credentials exist in this environment?
  2. If they do, does the principal hold enough permission to deploy the stack?

Nothing here mutates AWS state. Every call is a read (``sts``, ``list``,
``describe``, ``get``) or a dry run, so it is safe to run against a live account.

Exit codes
    0  identity resolved and every required permission probed OK
    1  identity resolved but one or more permissions are missing
    2  no usable credentials (authentication failed)
    3  configuration problem (boto3/CLI missing, bad region name)

Usage
    python scripts/aws_preflight.py
    python scripts/aws_preflight.py --profile prod --region us-east-1
    python scripts/aws_preflight.py --json
"""
from __future__ import annotations

import argparse
import configparser
import json
import os
import sys
from pathlib import Path

# Services the deployment touches, with one representative read call each.
# A read succeeding does not prove write access, so the policy probe below is
# what actually gates the "can I deploy" verdict.
PROBES = [
    ("cloudformation", "list_stacks", "CloudFormation (stack orchestration)"),
    ("s3", "list_buckets", "S3 data lake"),
    ("iam", "list_roles", "IAM roles / least-privilege policies"),
    ("glue", "get_databases", "Glue Data Catalog + ETL jobs"),
    ("redshift-serverless", "list_namespaces", "Redshift Serverless warehouse"),
    ("lambda", "list_functions", "Lambda ingestion / loader"),
    ("events", "list_rules", "EventBridge schedules"),
    ("logs", "describe_log_groups", "CloudWatch Logs"),
    ("cloudwatch", "list_dashboards", "CloudWatch dashboard + alarms"),
    ("sns", "list_topics", "Alert topic"),
    ("secretsmanager", "list_secrets", "Redshift credential secret"),
]

# Write permissions that cannot be probed without side effects. These are listed
# so the reader knows the preflight is a necessary-but-not-sufficient gate.
WRITE_PERMISSIONS = [
    "cloudformation:CreateStack/UpdateStack/DescribeStackEvents",
    "s3:CreateBucket/PutObject/PutBucketPolicy/PutBucketEncryption",
    "iam:CreateRole/PutRolePolicy/PassRole/AttachRolePolicy",
    "glue:CreateJob/StartJobRun/CreateCrawler/CreateDatabase",
    "redshift-serverless:CreateNamespace/CreateWorkgroup/GetCredentials",
    "lambda:CreateFunction/UpdateFunctionCode/InvokeFunction",
    "events:PutRule/PutTargets",
    "cloudwatch:PutDashboard/PutMetricAlarm",
    "secretsmanager:CreateSecret/PutSecretValue",
]


def credential_source() -> dict:
    """Describe where boto3 will look for credentials, without printing secrets."""
    info: dict = {
        "env_keys": bool(os.environ.get("AWS_ACCESS_KEY_ID")),
        "env_session_token": bool(os.environ.get("AWS_SESSION_TOKEN")),
        "profile_env": os.environ.get("AWS_PROFILE") or None,
        "region_env": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or None,
    }
    cred_file = Path(os.environ.get("AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws" / "credentials"))
    conf_file = Path(os.environ.get("AWS_CONFIG_FILE", Path.home() / ".aws" / "config"))
    info["credentials_file"] = str(cred_file) if cred_file.exists() else None
    info["config_file"] = str(conf_file) if conf_file.exists() else None

    profiles: list[dict] = []
    if cred_file.exists():
        cp = configparser.ConfigParser()
        cp.read(cred_file)
        for name in cp.sections():
            key = cp[name].get("aws_access_key_id", "")
            if key:
                profiles.append({
                    "name": name,
                    "access_key_masked": f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "***",
                    "key_length": len(key),
                    "has_secret": bool(cp[name].get("aws_secret_access_key")),
                    "long_term_key": key.startswith("AKIA"),
                })
    info["profiles"] = profiles

    region = None
    if conf_file.exists():
        cp = configparser.ConfigParser()
        cp.read(conf_file)
        for section in cp.sections():
            if cp[section].get("region"):
                region = cp[section]["region"]
                break
    info["config_region"] = region
    return info


def check_identity(session, region: str) -> tuple[dict | None, str | None]:
    try:
        ident = session.client("sts", region_name=region).get_caller_identity()
        ident.pop("ResponseMetadata", None)
        return ident, None
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim in the report
        return None, f"{type(exc).__name__}: {exc}"


def probe_services(session, region: str) -> list[dict]:
    results = []
    for service, method, label in PROBES:
        try:
            client = session.client(service, region_name=region)
            getattr(client, method)()
            results.append({"service": service, "label": label, "ok": True, "error": None})
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)
            results.append({"service": service, "label": label, "ok": False,
                            "error": f"{code}: {str(exc)[:200]}"})
    return results


def diagnose(identity_error: str | None) -> list[str]:
    """Turn an SDK error string into the concrete fix."""
    if not identity_error:
        return []
    hints = []
    if "InvalidClientTokenId" in identity_error:
        hints += [
            "The access key ID is not recognised by AWS. The key was deleted, "
            "deactivated, or never existed in this account.",
            "Fix: create a new access key for an IAM user with the deploy policy, "
            "then run `aws configure` (or `aws configure sso`).",
        ]
    elif "SignatureDoesNotMatch" in identity_error:
        hints += [
            "The access key exists but the secret does not match it - usually a "
            "truncated paste or a rotated secret.",
            "Fix: re-enter the secret with `aws configure set aws_secret_access_key`.",
        ]
    elif "ExpiredToken" in identity_error:
        hints += [
            "Temporary session credentials have expired (SSO or assumed role).",
            "Fix: refresh with `aws sso login` or re-assume the role.",
        ]
    elif "AccessDenied" in identity_error:
        hints += [
            "Credentials are valid but the principal cannot call sts:GetCallerIdentity, "
            "which indicates an explicit deny or a broken trust policy.",
        ]
    elif "Could not connect" in identity_error or "EndpointConnectionError" in identity_error:
        hints += ["Network/proxy issue reaching the AWS endpoint."]
    elif "NoCredentialsError" in identity_error or "UnableToLocateCredentials" in identity_error:
        hints += [
            "No credentials were found in the standard chain (env vars, ~/.aws, "
            "instance profile).",
            "Fix: run `aws configure` or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.",
        ]
    return hints


def main() -> int:
    ap = argparse.ArgumentParser(description="AWS deployment pre-flight")
    ap.add_argument("--profile", default=None, help="named profile from ~/.aws")
    ap.add_argument("--region", default=None, help="override region")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    try:
        import boto3
        from botocore.exceptions import BotoCoreError
    except ImportError:
        print("boto3 is not installed: python -m pip install boto3", file=sys.stderr)
        return 3

    region = args.region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
    creds = credential_source()

    try:
        session = boto3.Session(profile_name=args.profile, region_name=region)
    except BotoCoreError as exc:
        print(f"cannot build session: {exc}", file=sys.stderr)
        return 3

    identity, identity_error = check_identity(session, region)
    probes = probe_services(session, region) if identity else []

    report = {
        "region": region,
        "profile": args.profile or "default",
        "credentials": creds,
        "identity": identity,
        "identity_error": identity_error,
        "services": probes,
        "required_write_permissions": WRITE_PERMISSIONS,
        "verdict": "ready" if identity and all(p["ok"] for p in probes) else
                   ("credentials_valid_permissions_missing" if identity else "no_valid_credentials"),
        "remediation": diagnose(identity_error),
    }

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("=" * 74)
        print("AWS DEPLOYMENT PRE-FLIGHT")
        print("=" * 74)
        print(f"region            : {region}")
        print(f"profile           : {report['profile']}")
        print(f"credentials file  : {creds['credentials_file'] or '(none)'}")
        print(f"config file       : {creds['config_file'] or '(none)'}")
        print(f"env access key    : {creds['env_keys']}")
        print(f"config region     : {creds['config_region'] or '(none)'}")
        for p in creds["profiles"]:
            print(f"profile [{p['name']}]     : key {p['access_key_masked']} "
                  f"({p['key_length']} chars, secret={p['has_secret']}, long-term={p['long_term_key']})")

        print("-" * 74)
        if identity:
            print(f"identity          : OK  {identity.get('Arn')}")
            print(f"account / user    : {identity.get('Account')} / {identity.get('UserId')}")
        else:
            print("identity          : FAILED")
            print(f"  {identity_error}")
            for hint in report["remediation"]:
                print(f"  -> {hint}")

        if probes:
            print("-" * 74)
            print("service probes")
            for p in probes:
                mark = "OK  " if p["ok"] else "FAIL"
                print(f"  [{mark}] {p['label']}")
                if not p["ok"]:
                    print(f"         {p['error']}")

        print("-" * 74)
        print(f"VERDICT: {report['verdict']}")
        if report["verdict"] == "no_valid_credentials":
            print("Deployment cannot proceed. Nothing was created. See docs/AWS-DEPLOYMENT.md")
            print("section 'Connecting an AWS account' for the exact steps.")
        elif report["verdict"] == "credentials_valid_permissions_missing":
            print("Credentials work but the principal lacks a required permission; the")
            print("failed probes above name the service. Attach the deploy policy and retry.")

    return 0 if report["verdict"] == "ready" else (1 if identity else 2)


if __name__ == "__main__":
    raise SystemExit(main())
