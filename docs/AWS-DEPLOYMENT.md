# AWS deployment runbook — Divakar Cloud Data Lake & Data Warehouse

This is the operating manual for the cloud half of the project. The local stack
(PostgreSQL on 5433, FastAPI on 8020, React dashboard) is a separate, working
deployment and is **not** touched by anything in this document.

> **Status of this document.** Every template and script referenced here has been
> written and statically validated (`cfn-lint`, `py_compile`). None of it has been
> executed against a live AWS account, because the credentials available on this
> machine are invalid — see §0. Treat this as a tested-but-not-yet-run runbook,
> not as a description of existing resources.

---

## 0. Current status

| Component | File | Built | Static check | Deployed |
|---|---|---|---|---|
| S3 lake (4 buckets) + Glue Catalog DBs | `aws/cfn/10-storage.yaml` | yes | `cfn-lint` clean | **no** |
| IAM roles (5, least privilege) | `aws/cfn/20-iam.yaml` | yes | `cfn-lint` clean | **no** |
| Redshift Serverless namespace + workgroup | `aws/cfn/30-redshift.yaml` | yes | `cfn-lint` clean | **no** |
| Glue ETL jobs + gold crawler | `aws/cfn/40-glue.yaml` | yes | `cfn-lint` clean | **no** |
| Lambda, EventBridge, CloudWatch, SNS | `aws/cfn/50-serverless.yaml` | yes | `cfn-lint` clean | **no** |
| PySpark bronze→silver→gold | `aws/glue/*.py` | yes | compiled | **no** |
| Ingest orchestrator Lambda | `aws/lambda/pipeline/handler.py` | yes | compiled | **no** |
| Redshift COPY loader Lambda | `aws/lambda/loader/handler.py` | yes | compiled, 40 statements parsed | **no** |
| Redshift DDL + load SQL | `sql/redshift_ddl.sql`, `sql/redshift_load.sql` | yes | parsed | **no** |
| Deployment driver | `scripts/deploy_aws.py` | yes | `py_compile` ok, `--plan` ok | **not run** |
| Readiness gate | `scripts/aws_preflight.py` | yes | runs | **fails: no credentials** |
| Deployer IAM policy | `aws/iam/deploy-policy.json` | yes | valid JSON | n/a |

The pre-flight gate currently reports:

```
identity          : FAILED
  ClientError: An error occurred (InvalidClientTokenId) when calling the
  GetCallerIdentity operation: The security token included in the request is invalid.
VERDICT: no_valid_credentials
```

A credentials file exists at `C:\Users\divakar v\.aws\credentials`, but the access
key it contains has been deleted or deactivated in AWS. **Nothing was created in
any AWS account.** The rest of this document is the exact path to fix that.

---

## 1. Connecting an AWS account

You need an IAM principal that can create CloudFormation stacks, IAM roles, S3
buckets, Glue jobs, a Redshift Serverless namespace, Lambda functions, EventBridge
rules, CloudWatch alarms/dashboards and an SNS topic. The scope for exactly that
is checked in as `aws/iam/deploy-policy.json`.

### 1.1 Prerequisites

* AWS CLI v2 (`aws --version`)
* Python 3.10+ with `boto3` (already installed in this project's `.venv`)
* Permission in the target account to create IAM users/roles (or an existing
  admin to do step 1.2 for you)

### 1.2 Create a dedicated deployer identity

Do this once, from a principal that already has IAM admin rights.

```powershell
# 1. create the user
aws iam create-user --user-name divakar-deployer

# 2. attach the least-privilege deployment policy that ships with this repo
aws iam put-user-policy `
  --user-name divakar-deployer `
  --policy-name DivakarDeploy `
  --policy-document file://aws/iam/deploy-policy.json

# 3. create an access key (this is the only time the secret is shown)
aws iam create-access-key --user-name divakar-deployer
```

`deploy-policy.json` is scoped by name: CloudFormation stacks must match
`divakar-*`, roles must match `divakar-*`, buckets `divakar-lake-*`, Glue
resources `divakar-*`. It does **not** grant `AdministratorAccess`. The only
wildcard resources are the ones that AWS itself requires to be wildcards
(Redshift Data API, `ec2:Describe*`, CloudWatch dashboards).

### 1.3 Point the CLI at the new key

```powershell
aws configure --profile divakar
# AWS Access Key ID     : <from step 1.2>
# AWS Secret Access Key : <from step 1.2>
# Default region name   : ap-south-1
# Default output format : json
```

Then make it the default profile (this project's scripts read `AWS_PROFILE`,
falling back to `default`):

```powershell
$env:AWS_PROFILE = "divakar"
```

If you want it to persist for future shells:

```powershell
[Environment]::SetEnvironmentVariable("AWS_PROFILE","divakar","User")
```

### 1.4 Re-run the readiness gate

```powershell
python scripts/aws_preflight.py
```

Expected on success:

```
identity          : arn:aws:iam::<account>:user/divakar-deployer
region            : ap-south-1
VERDICT: ready
```

The gate checks, in order: credentials present → STS identity valid → region
resolvable → each service callable (`s3:ListBuckets`, `glue:GetJobs`,
`redshift-serverless:ListWorkgroups`, `lambda:ListFunctions`,
`events:ListRules`, `cloudwatch:DescribeAlarms`) → whether the `divakar-*`
stacks already exist. It exits `0` only when the whole chain passes.

If it fails, the output names the failing service and the missing permission; add
that action to `aws/iam/deploy-policy.json` and re-apply with
`aws iam put-user-policy ...` (same command as step 1.2).

> **Cost warning before you continue.** A Redshift Serverless workgroup with
> `BaseCapacity: 8` bills per second while queries run, at roughly
> `8 × $0.375/hour ≈ $3/hour` in `us-east-1` (check current regional pricing).
> Glue G.1X workers bill per DPU-hour. S3 storage for this dataset is a few
> hundred MB, i.e. cents. See §7 and §8 — do not leave the workgroup running
> indefinitely.

---

## 2. What the deployment creates

Five stacks, deployed in dependency order. Stack names are fixed:

| # | Stack | Template | Creates |
|---|---|---|---|
| 1 | `divakar-01-storage` | `aws/cfn/10-storage.yaml` | 4 S3 buckets, 3 Glue Data Catalog databases |
| 2 | `divakar-02-iam` | `aws/cfn/20-iam.yaml` | 5 IAM roles, deployed twice (§3.3) |
| 3 | `divakar-03-redshift` | `aws/cfn/30-redshift.yaml` | Security group, Serverless namespace + workgroup |
| 4 | `divakar-04-glue` | `aws/cfn/40-glue.yaml` | 2 Glue jobs, 1 crawler |
| 5 | `divakar-05-runtime` | `aws/cfn/50-serverless.yaml` | 2 Lambdas, 4 EventBridge rules, SNS topic, 2 alarms, 1 dashboard |

### 2.1 Resource names

With `Environment=dev`, `AccountId=123456789012`, `Region=ap-south-1`:

**Storage**

```
divakar-lake-bronze-dev-123456789012-ap-south-1
divakar-lake-silver-dev-123456789012-ap-south-1
divakar-lake-gold-dev-123456789012-ap-south-1
divakar-lake-artifacts-dev-123456789012-ap-south-1
```

* versioning on, SSE-S3 (`AES256`) with bucket keys, all public access blocked,
  `BucketOwnerEnforced` ownership
* bronze lifecycle: `STANDARD_IA` at 30 days, `GLACIER_IR` at 180 days,
  noncurrent versions expire at 90 days, incomplete multipart uploads at 7 days
* `DeletionPolicy: Retain` — deleting the stack does **not** delete your data

Glue databases: `divakar_bronze_dev`, `divakar_silver_dev`, `divakar_gold_dev`.

**IAM** (all named `divakar-<purpose>-dev`)

| Role | Used by | Scope |
|---|---|---|
| `divakar-glue-etl-dev` | Glue jobs + crawler | read bronze, write silver/gold, job scratch + Spark UI under `artifacts/tmp/*` and `artifacts/spark-ui/*`, catalog `divakar_*`, CloudWatch metrics in namespace `Glue` |
| `divakar-ingest-dev` | legacy ingest Lambda | write bronze only, logs |
| `divakar-pipeline-dev` | orchestrator Lambda | write bronze, read `artifacts/raw/*` (replay channel), start `divakar-*` Glue jobs/crawlers, logs |
| `divakar-redshift-loader-dev` | loader Lambda | Redshift Data API + `redshift-serverless:GetWorkgroup/GetNamespace/GetCredentials`, read silver/gold, start Glue jobs/crawlers, optional `secretsmanager:GetSecretValue` |
| `divakar-redshift-copy-dev` | assumed by Redshift itself | `s3:GetObject` + `s3:ListBucket` on silver/gold, `kms:Decrypt` only via S3 |

**Warehouse**

```
namespace  : divakar-dev
workgroup  : divakar-dev
database   : analytics
admin user : deuser
port       : 5439
capacity   : 8 RPU (minimum)
```

`ManageAdminPassword: true` — AWS generates the password, stores it in Secrets
Manager and rotates it. No password exists in any template, script or file.
`require_ssl`, `enable_user_activity_logging` and a 3600 s query timeout are set
as workgroup `ConfigParameters`. The endpoint host, port and secret ARN are not
CloudFormation outputs on purpose; `scripts/deploy_aws.py` reads them from
`redshift-serverless:GetWorkgroup` / `GetNamespace` and writes them to
`.aws-deploy.json`.

**Compute and orchestration**

```
divakar-bronze-to-silver-dev     Glue 4.0, G.1X × 2, 60 min timeout
divakar-silver-to-gold-dev       Glue 4.0, G.1X × 2, 60 min timeout
divakar-gold-crawler-dev         crawler over s3://<gold bucket>/
divakar-pipeline-dev             Lambda, python3.12, 512 MB, 300 s
divakar-redshift-loader-dev      Lambda, python3.12, 512 MB, 900 s, reserved concurrency 1
divakar-schedule-ingest-dev      cron(0 1 * * ? *)  daily 01:00 UTC
divakar-on-bronze-silver-success-dev   Glue SUCCEEDED → run gold
divakar-on-gold-success-dev            Glue SUCCEEDED → load warehouse
divakar-glue-failure-dev               Glue FAILED/TIMEOUT/ABORTED → SNS
divakar-alerts-dev                     SNS topic (+ optional email)
divakar-pipeline-errors-dev            alarm: pipeline Lambda Errors ≥ 1
divakar-loader-errors-dev              alarm: loader Lambda Errors ≥ 1
divakar-pipeline-dev                   CloudWatch dashboard (6 widgets)
```

---

## 3. Deploy

### 3.1 Dry run first

```powershell
python scripts\deploy_aws.py --plan
```

Prints every stack, parameter and upload it would perform, then exits without
creating anything. This still requires valid credentials for the identity check.

### 3.2 Real deploy

```powershell
python scripts\deploy_aws.py --alert-email you@example.com
```

| Flag | Default | Meaning |
|---|---|---|
| `--env` | `dev` | environment suffix; must match `^[a-z0-9-]{2,12}$` |
| `--region` | `ap-south-1` | target region |
| `--profile` | `$AWS_PROFILE` | named CLI profile |
| `--plan` | off | print and exit |
| `--run-pipeline` | off | after deploying, invoke one end-to-end run |
| `--alert-email` | none | SNS subscription for Glue/Lambda failures |
| `--partner-api-url` | none | switch ingestion from replay to a live HTTP API |
| `--allowed-cidr` | your public IP `/32` | who may reach Redshift on 5439 |
| `--no-schedule` | off | deploy with the daily rule `DISABLED` |
| `--workers` | `2` | Glue worker count |
| `--base-capacity` | `8` | Redshift RPU |

The driver performs nine steps and stops at the first failure:

1. verify identity
2. stack 1 — storage + catalog
3. upload Glue scripts, SQL, and the raw replay exports to `artifacts/`
4. stack 2 — IAM, first pass (no admin secret known yet)
5. stack 3 — Redshift Serverless (resolves the default VPC and two subnets in
   different AZs, and your public IP)
6. stack 2 again — IAM updated with the managed secret ARN
7. stack 4 — Glue jobs + crawler
8. build and upload both Lambda zips, stack 5 — runtime
9. write `.aws-deploy.json`

### 3.3 Why IAM is deployed twice

There is a genuine circular dependency:

```
namespace  needs  CopyRoleArn
loader role needs  the namespace's secret ARN
```

CloudFormation cannot express that. The driver deploys `divakar-02-iam` with
`AdminSecretArn=""`, creates the namespace, reads the secret ARN back from the
service API, then updates the same stack in place with the ARN filled in. The
conditional `HasAdminSecret` policy appears only on the second pass.

### 3.4 `.aws-deploy.json`

Written to the project root, mode-600 equivalent. It is a deployment receipt, not
a config file — `.gitignore` it if you commit this repo.

```json
{
  "region": "ap-south-1",
  "account_id": "123456789012",
  "caller_arn": "arn:aws:iam::123456789012:user/divakar-deployer",
  "environment": "dev",
  "buckets": { "bronze": "...", "silver": "...", "gold": "...", "artifacts": "..." },
  "network": { "vpc_id": "vpc-...", "subnets": ["subnet-a", "subnet-b"], "allowed_cidr": "203.0.113.7/32" },
  "stacks": { "storage": "divakar-01-storage", "...": "..." },
  "warehouse": {
    "workgroup": "divakar-dev", "namespace": "divakar-dev",
    "database": "analytics", "admin_username": "deuser",
    "host": "divakar-dev.123456789012.ap-south-1.redshift-serverless.amazonaws.com",
    "port": 5439, "secret_arn": "arn:aws:secretsmanager:..."
  },
  "glue": { "bronze_job": "divakar-bronze-to-silver-dev", "gold_job": "divakar-silver-to-gold-dev",
            "crawler": "divakar-gold-crawler-dev", "gold_database": "divakar_gold_dev" },
  "lambda": { "pipeline": "divakar-pipeline-dev", "loader": "divakar-redshift-loader-dev" },
  "schedule": { "rule": "divakar-schedule-ingest-dev", "enabled": true },
  "dashboard": { "name": "divakar-pipeline-dev", "url": "https://..." },
  "ingestion_channel": "replay"
}
```

---

## 4. End-to-end data flow

```
EventBridge  divakar-schedule-ingest-dev     (cron 0 1 * * ? *, daily 01:00 UTC)
   │  {"task":"ingest"}
   ▼
Lambda  divakar-pipeline-dev
   │  for each source in customers, products, orders, order_items,
   │                     payments, shipments, returns
   │     read channel:
   │       replay  -> s3://<artifacts>/raw/<source>/<source>.csv  (byte-for-byte)
   │       api     -> GET <PARTNER_API_BASE_URL>/<source>?limit=..&since=..
   │     validate REQUIRED_COLUMNS
   │     write s3://<bronze>/<source>/ingest_date=YYYY-MM-DD/<source>.<ext>
   │     write .../_metadata.json   (rows, bytes, sha256, schema, watermark)
   │
   │  glue:start_job_run divakar-bronze-to-silver-dev  --ingest_date YYYY-MM-DD
   ▼
Glue  divakar-bronze-to-silver-dev
   │  type, de-duplicate on the business key, drop null keys/timestamps
   │  write Parquet to s3://<silver>/orders/order_month=YYYY-MM/...
   ▼
EventBridge  divakar-on-bronze-silver-success-dev   (Glue Job State Change = SUCCEEDED)
   │  {"task":"run_gold"}
   ▼
Lambda  divakar-pipeline-dev  ->  glue:start_job_run divakar-silver-to-gold-dev
   ▼
Glue  divakar-silver-to-gold-dev
   │  business aggregates (daily sales, monthly revenue, RFM, cohorts,
   │  product performance, payments, returns, fulfilment)
   │  write Parquet to s3://<gold>/<dataset>/<dataset>.parquet
   ▼
EventBridge  divakar-on-gold-success-dev   (Glue SUCCEEDED)
   │  {"task":"load_warehouse"}
   ▼
Lambda  divakar-redshift-loader-dev
   │  optionally fire divakar-gold-crawler-dev (async, not awaited)
   │  Redshift Data API, 40 statements from sql/redshift_load.sql:
   │    ops schema -> staging DDL -> truncate -> 7 × COPY (PARQUET)
   │    -> dim_date -> dim_payment_method -> SCD2 close/insert on
   │       dim_customer and dim_product -> 5 × fact delete-then-insert -> ANALYZE
   │  write s3://<gold>/_ops/load_runs/<ts>-<run_id>.json
   ▼
dwh.dim_customer, dwh.dim_product, dwh.dim_payment_method, dwh.dim_date,
dwh.fact_orders, dwh.fact_order_items, dwh.fact_payments,
dwh.fact_shipments, dwh.fact_returns
   ▼
FastAPI (8120, AWS mode)  ->  React dashboard
```

Any Glue failure fires `divakar-glue-failure-dev`, which publishes a formatted
message to `divakar-alerts-dev`. Because the chain advances only on `SUCCEEDED`,
a failed stage leaves the warehouse at its last good state instead of loading a
partial batch.

### 4.1 The two ingestion channels

The local mock partner API binds to `127.0.0.1:8100` and is unreachable from a
deployed Lambda, so the default cloud channel is **replay**: the raw exports in
`data/raw/` are uploaded to `s3://<artifacts>/raw/` and copied unchanged into
bronze. This preserves the "bronze is the untouched landing zone" invariant and
exercises the whole pipeline with the real 120k-order dataset.

Setting `--partner-api-url https://api.example.com` switches the Lambda to the
**api** channel with no code change. The endpoint contract is
`GET <base>/<source>?limit=<n>&since=<iso8601>` returning newline-delimited JSON.

---

## 5. Verify the deployment

```powershell
python scripts\aws_verify.py
```

It checks, and prints PASS/FAIL per item:

1. STS identity and region
2. the four buckets exist, are versioned, encrypted, private
3. bronze object counts per source and every `_metadata.json` is readable
4. silver and gold prefixes are non-empty; gold contains the expected datasets
5. the two Glue jobs exist; their most recent run state and duration
6. the crawler's tables in `divakar_gold_dev`
7. Redshift reachable through the Data API; row counts for every dimension and fact
8. constraints that matter: exactly one `is_current` row per customer/product,
   no fact row whose `customer_sk` misses the dimension
9. CloudWatch alarms exist and are `OK`/`INSUFFICIENT_DATA` (not `ALARM`)

Manual equivalents:

```powershell
aws s3 ls s3://divakar-lake-bronze-dev-<acct>-ap-south-1/ --recursive --summarize
aws glue get-job-runs --job-name divakar-bronze-to-silver-dev --max-items 3
aws glue get-job-runs --job-name divakar-silver-to-gold-dev   --max-items 3
aws cloudformation describe-stacks --stack-name divakar-05-runtime `
  --query "Stacks[0].Outputs"
```

### 5.1 Athena over the gold layer

The crawler registers the gold datasets in `divakar_gold_dev`, so Athena works
without extra setup. `aws/athena/gold_tables.sql` has explicit DDL for the eight
core datasets (use it instead of the crawler when you want the schema under
version control), and `aws/athena/queries.sql` holds ten representative queries:
revenue trend, month-over-month growth, RFM mix, cohort retention, negative-margin
products, return reasons, payment success rates, carrier performance, regional
concentration, and a cross-dataset revenue reconciliation probe.

```powershell
$env:ATHENA_BUCKET = (Get-Content .aws-deploy.json | ConvertFrom-Json).buckets.artifacts
aws athena start-query-execution `
  --query-string "SELECT count(*) FROM divakar_gold_dev.daily_sales" `
  --result-configuration "OutputLocation=s3://$env:ATHENA_BUCKET/athena-results/"
```

Athena bills per TB scanned. These datasets are a few MB of Parquet, so each
query costs effectively nothing — which makes Athena the cheapest way to sanity
check the lake before paying for Redshift compute.

Row counts through the Data API (no VPC, no driver):

```powershell
$SQL = "select 'fact_orders' as t, count(*) from dwh.fact_orders union all select 'fact_payments', count(*) from dwh.fact_payments"
aws redshift-data execute-statement `
  --workgroup-name divakar-dev --database analytics --secret-arn <secret_arn> `
  --sql $SQL
# then, with the returned Id:
aws redshift-data get-statement-result --id <Id>
```

---

## 6. Connecting the dashboard to the deployed warehouse

The application already abstracts storage and warehouse through `src/config.py`,
so this is a configuration change, not a code change. **Keep the local instance
running** — start a second one on another port with a different environment.

### 6.1 Prepare AWS-mode settings

`src/config.py` reads everything from the environment. For Redshift you need TLS
(`require_ssl` is on) and a driver that speaks Redshift's protocol dialect.

```powershell
$env:AWS_MODE               = "true"
$env:WAREHOUSE_HOST         = "<host from .aws-deploy.json warehouse.host>"
$env:WAREHOUSE_PORT         = "5439"
$env:WAREHOUSE_DB           = "analytics"
$env:WAREHOUSE_USER         = "deuser"
$env:WAREHOUSE_PASSWORD     = "<read from Secrets Manager — see 6.2>"
$env:WAREHOUSE_SSL          = "true"
$env:WAREHOUSE_DRIVER       = "psycopg2"     # psycopg3 needs prepare_threshold=None on Redshift
$env:LAKE_BACKEND           = "s3"
$env:AWS_REGION             = "ap-south-1"
$env:BRONZE_BUCKET          = "divakar-lake-bronze-dev-<acct>-ap-south-1"
$env:SILVER_BUCKET          = "divakar-lake-silver-dev-<acct>-ap-south-1"
$env:GOLD_BUCKET            = "divakar-lake-gold-dev-<acct>-ap-south-1"
$env:ARTIFACTS_BUCKET       = "divakar-lake-artifacts-dev-<acct>-ap-south-1"
```

### 6.2 Get the admin password

The namespace uses `ManageAdminPassword`, so the password lives only in Secrets
Manager:

```powershell
$secret = (Get-Content .aws-deploy.json | ConvertFrom-Json).warehouse.secret_arn
$pw = aws secretsmanager get-secret-value --secret-id $secret --query SecretString --output text |
      ConvertFrom-Json
$env:WAREHOUSE_PASSWORD = $pw.password
```

Note the JSON shape: the managed secret stores `{"admin":"deuser","password":"..."}`,
not a bare string.

### 6.3 Network access

The workgroup's security group allows 5439 from `AllowedCidr`, which defaults to
your public IP at deploy time. If your IP changes:

```powershell
# find the current rule, then add the new /32
aws ec2 describe-security-groups --group-ids <sg-id> `
  --query "SecurityGroups[0].IpPermissions"
aws ec2 authorize-security-group-ingress --group-id <sg-id> `
  --protocol tcp --port 5439 --cidr <new-ip>/32
```

Redshift Serverless is `PubliclyAccessible: true` here so a laptop can reach it.
For a private setup, set `PubliclyAccessible: false` and run the API inside the
VPC (or reach it through a bastion / VPN) — the Data API path used by the loader
does not need VPC attachment either way.

### 6.4 Run the AWS-mode API on a second port

```powershell
python -m uvicorn api.main:app --host 127.0.0.1 --port 8120
```

`http://127.0.0.1:8120/api/health` should report the Redshift host and
`"lake_backend": "s3"`. The 8020 instance keeps serving the local PostgreSQL
warehouse untouched.

### 6.5 dbt against Redshift (optional)

`dbt/profiles.yml` targets `dbt-postgres`, which is installed. Redshift needs
`dbt-redshift` instead:

```powershell
pip install "dbt-redshift==1.9.*"
$env:DBT_PROFILES_DIR = "dbt"
dbt run  --target aws
dbt test --target aws
```

Without `dbt-redshift` the warehouse still works: the loader builds `dwh` and
`staging` directly from `sql/redshift_ddl.sql` and `sql/redshift_load.sql`. dbt is
the transformation layer for the local `marts` schema, not a deployment
dependency.

---

## 7. Cost

Order of magnitude for one full pipeline run plus an idle day, `ap-south-1`:

| Service | Driver | Rough cost |
|---|---|---|
| S3 | ~350 MB raw + Parquet, versioned | cents/month |
| Glue | 2 jobs × 2 × G.1X × ~5 min | ~$0.15 per run |
| Lambda | 2 invocations, < 5 min, 512 MB | < $0.01 per run |
| Redshift Serverless | 8 RPU while queries run | ~$3/hour of active query time |
| CloudWatch | 2 alarms, 1 dashboard, logs 30 days | ~$1/month |

The dominant cost is Redshift. Serverless charges while queries execute and does
not bill for idle compute, but the namespace itself must exist. Check current
regional pricing before leaving it up — prices change and vary by region.

---

## 8. Teardown

Delete in reverse dependency order. Buckets are `DeletionPolicy: Retain`, so
stack deletion leaves the data behind and the buckets must be emptied first if
you really want them gone.

```powershell
foreach ($s in @("divakar-05-runtime","divakar-04-glue","divakar-03-redshift",
                 "divakar-02-iam","divakar-01-storage")) {
  aws cloudformation delete-stack --stack-name $s
  aws cloudformation wait stack-delete-complete --stack-name $s
}
```

Then, only if you also want the data gone:

```powershell
foreach ($b in @("bronze","silver","gold","artifacts")) {
  $name = "divakar-lake-$b-dev-<acct>-ap-south-1"
  aws s3 rm "s3://$name" --recursive
  aws s3api delete-bucket --bucket $name
}
```

To stop the meter without losing data, keep everything and only disable the
schedule:

```powershell
aws events disable-rule --name divakar-schedule-ingest-dev
```

Deleting the namespace removes the managed password secret automatically.

---

## 9. Verified vs. untested

Being precise about this matters more than looking finished.

**Verified by an executed command**

* all five templates parse and lint clean (`cfn-lint 1.59.0`, `--format parseable`)
* `aws/lambda/pipeline/handler.py` and `aws/lambda/loader/handler.py` compile
* `sql/redshift_load.sql` parses into exactly 40 statements, in the documented order
* `scripts/deploy_aws.py` compiles and its `--plan` path reaches the identity gate
  and exits 2 with `No valid AWS credentials. Nothing was created.`
* `scripts/aws_preflight.py` reports `no_valid_credentials` against the key on this
  machine (`AKIA...Z3XS`), reproducibly
* the local stack still works: `http://127.0.0.1:8020/api/health` →
  `{"status":"ok","warehouse":"localhost:5433/analytics","lake_backend":"local"}`

**Not verified — needs a real account**

* any CloudFormation stack reaching `CREATE_COMPLETE`
* IAM role assumption by Glue, Lambda or Redshift
* Glue job scripts executing on Glue 4.0 (they were written against the Spark 3.3
  API; only local PySpark/pandas equivalence was reasoned about, not run)
* `COPY ... FORMAT AS PARQUET` against the actual gold layout
* SCD2 close-then-insert correctness at Redshift scale
* EventBridge rule matching for `Glue Job State Change`
* the CloudWatch dashboard's `SEARCH(...)` expressions rendering
* Redshift reachability from this laptop on 5439

Anything in the second list is a **hypothesis** until `scripts/aws_verify.py`
prints PASS on a real deployment.

---

## 10. Troubleshooting

**`InvalidClientTokenId` on every call.** The access key does not exist in AWS.
Recreate it (§1.2–1.3). Deleting a user's key in the console takes effect
immediately.

**`AccessDenied` on `iam:PassRole` while creating a stack.** The deployer needs
`PassRole` on the five `divakar-*` roles. It is in `deploy-policy.json` with a
`PassedToService` condition for CloudFormation and Lambda; add
`glue.amazonaws.com` / `redshift.amazonaws.com` if a service-specific pass fails.

**Stack 1 fails on bucket name.** Bucket names are globally unique and the
template already embeds account id and region. A collision means a leftover bucket
from an earlier run in another account — pick a different `--env`.

**Stack 3 fails with `InvalidParameterValue: subnet`.** Redshift Serverless needs
two subnets in **different** availability zones. `default_network()` picks them
automatically; if your account has no default VPC, pass explicit `VpcId` /
`SubnetId1` / `SubnetId2` by deploying `30-redshift.yaml` yourself after stack 2.

**Stack 3 fails with `AlreadyExists` for namespace `divakar-dev`.** A previous
namespace was deleted but not fully cleaned up, or `--env` was reused. Namespace
deletion is asynchronous; wait for `ListNamespaces` to stop returning it.

**Glue job fails with `AccessDenied` on `s3://.../tmp/`.** The
`JobScratchAndSparkUi` statement in `20-iam.yaml` covers `artifacts/tmp/*` and
`artifacts/spark-ui/*`. If you changed `ScriptsPrefix`, update that statement.

**Loader Lambda times out.** It is capped at 900 s and each statement has a 600 s
Data API timeout. A cold Redshift Serverless workgroup can take 30–60 s to resume;
the first load after a quiet period is the slow one. Raise `STATEMENT_TIMEOUT_SECONDS`
or pre-warm the workgroup before the scheduled run.

**Loader fails on `COPY`: `S3ServiceException ... Access Denied`.** The
`divakar-redshift-copy-dev` role must be in the namespace's `IamRoles` list — the
template does that — and the gold bucket must not be in a different region.

**Dashboard shows the local warehouse.** `AWS_MODE` alone changes nothing unless
`WAREHOUSE_HOST` is also set. Check `/api/health`; it echoes the resolved host.

**`/mock-api` 404s from outside this machine.** Expected: it is a local test
fixture bound to `127.0.0.1:8100`, not part of the cloud design. The deployed
pipeline uses the replay channel instead.
