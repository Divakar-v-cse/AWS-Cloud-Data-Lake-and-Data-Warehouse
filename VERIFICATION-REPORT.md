# Divakar Data Platform — Verification Report

Generated: 2026-11-03 (Asia/Calcutta)
Scope: the eight completion tasks. Everything below was executed and observed in this session; nothing is asserted from build logs alone.

---

## 1. Browser URL

| Item | Value |
|---|---|
| Dashboard | **http://127.0.0.1:8020/** |
| API docs (Swagger) | http://127.0.0.1:8020/docs |
| OpenAPI schema | http://127.0.0.1:8020/openapi.json |
| Health probe | http://127.0.0.1:8020/api/health |

Port **8020**, not 8000/8010. Both requested ports were already bound by an unrelated, pre-existing application (`Divakar Automated ETL Data Pipeline`) that answers `/` with a different Swagger UI. Binding there would have served the wrong app, so 8020 was used.

## 2. Backend status

**Running and verified.** `python -m uvicorn api.main:app --host 127.0.0.1 --port 8020`.

```json
{"status":"ok","warehouse":"localhost:5433/analytics","lake_backend":"local","latency_ms":2106.4,"error":null}
```

- Warehouse: PostgreSQL 17, Redshift wire-compatible SQL (`LAKE_BACKEND=local`)
- Route surface: 27 OpenAPI paths — 25 parameterless `/api/*`, `/mock-api/{source}`, SPA catch-all
- Started only after the code change below, and re-verified after `dbt run` recreated the marts

## 3. Warehouse reload task `929ea329-28d2-4dec-ad8a-027afee275d0`

**It did not complete — it hung and was killed.**

Diagnosed with a `pg_stat_activity` probe: one backend sat `idle in transaction` / `ClientRead` mid-`INSERT INTO staging.stg_order_items` for ~54 minutes, with only `staging.stg_orders` created. Cause was the client side: `DataFrame.to_sql(method="multi")` spends its time formatting ~65k bind placeholders per chunk.

Fix: `_stage()` rewritten in `src/load_warehouse.py` to stream a single `COPY ... FROM STDIN WITH (FORMAT csv)` per dataset.

Replacement run **#25** (`ops.pipeline_runs`) — **status success, 238.24 s**, 120,000 in / 651,115 out. A 54-minutes-and-never-finished job became a 4-minute job.

## 4. Database verification (expected vs actual)

| Table | Expected (from `data/raw/_manifest.json`) | Actual in `dwh` | Result |
|---|---|---|---|
| `fact_orders` | 120,000 | 120,000 | match |
| `fact_order_items` | 274,630 | 274,630 | match |
| `fact_payments` | 134,355 | 134,355 | match |
| `fact_shipments` | 105,567 | 105,567 | match |
| `fact_returns` | 16,563 | 16,563 | match |
| `dim_customer` | 20,000 | 20,000 (all `is_current`) | match |
| `dim_product` | 500 | 500 | match |
| `dim_date` | 1,826 (2023-01-01 → 2027-12-31) | 1,826 | match |
| `dim_payment_method` | 8 | 8 | match |

Referential integrity holds: every fact row joins to a current dimension version, so no fact is orphaned.

## 5. dbt

```
dbt run   -> Done. PASS=17 WARN=0 ERROR=0 SKIP=0 TOTAL=17   (10 tables, 7 views, 9.71s)
dbt test  -> Done. PASS=46 WARN=0 ERROR=0 SKIP=0 TOTAL=46   (3.56s)
```

One real error was fixed to get here: `dbt/models/marts/schema.yml` tested `order_month` on `mart_monthly_revenue`, but the model emits `year_month` — two `Database Error` test failures. Corrected to `year_month`; the column is aliased to `order_month` at the API edge so the frontend contract is untouched.

## 6. API endpoint results

All 25 parameterless routes returned HTTP 200 with real payloads; `/mock-api/{source}` verified separately.

```
/api/health 200 | /api/meta 200 | /api/catalog 200 | /api/executive 200
/api/sales/daily 200 (365 rows) | /api/sales/monthly 200 (30 rows)
/api/sales/regional 200 (23) | /api/sales/category 200 (10)
/api/customers/summary 200 | /api/customers/segments 200 (7)
/api/customers/rfm 200 (50) | /api/customers/cohorts 200 (462)
/api/products/performance 200 (50) | /api/products/categories 200 (10)
/api/orders/funnel 200 (30) | /api/orders/status 200 (4) | /api/orders/recent 200 (25)
/api/payments 200 (7) | /api/returns 200 (60) | /api/returns/monthly 200 (31)
/api/fulfilment 200 (4)
/api/ops/runs 200 (25) | /api/ops/quality 200 (40) | /api/ops/quality/summary 200 (7) | /api/ops/watermarks 200 (4)
/mock-api/orders?limit=2 200 (available=120000) | /mock-api/customers?limit=1 200 (available=20000)
summary: 25 ok, 0 failed
```

Five 500s found on the first sweep were fixed in `api/main.py`:

| Endpoint | Cause | Fix |
|---|---|---|
| `/api/executive` | `order_month` does not exist in the mart | `year_month as order_month` |
| `/api/sales/daily` | selected nonexistent `shipped_orders` | removed from projection |
| `/api/sales/monthly` | same `order_month` mismatch | explicit projection + alias |
| `/api/products/performance` | sorted on `return_rate`, mart has `return_rate_pct` | sort-key mapping |
| `/api/orders/recent` | `dim_customer` has no `customer_name` | `first_name \|\| ' ' \|\| last_name` |

## 7. Frontend served by the backend

```
GET /                        -> 200  424 B   <div id="root"> present, <title> present
GET /assets/index-D-zVEPQj.js -> 200  627,255 B
GET /assets/index-Bk8JdozI.css-> 200  14,712 B
```

SPA fallback route serves `index.html` for client-side paths. The bundle was **not** rebuilt — the existing production build is what is being served.

## 8. Browser verification (built-in browser, real DOM)

Six pages opened and clicked through. Every page rendered live warehouse data, 14 charts total, **zero console errors, zero failed requests (34/34 HTTP 200)**.

| Page | Observed |
|---|---|
| Overview | 8 KPI tiles, 4 charts |
| Sales | West $12.01M / 35,287; South $10.74M; Midwest $7.09M; Northeast $6.01M; state table CA 14,041 / $4.78M |
| Customers | 18,020 customers, 14,192 repeat (78.8%), Champions 3,657 / $21.78M, 462-row cohort matrix |
| Products | Terra Bedding 5 8,487 units / $2.56M / 43.7% margin; category margin + margin-vs-return charts |
| Orders | 120,000 orders, 95,464 completed, 8,961 cancelled, 10,103 returned, 67.93% payment success, carrier table DHL/FedEx/UPS/USPS |
| Operations | 25 pipeline runs, 40 DQ checks, 97.5% pass rate, 14 gold datasets, run log |

### Bug found and fixed in this pass

Four Overview tiles rendered `0` / `$0.00` while the API returned correct values: the component binds `orders`, `completed_orders`, `aov`, `customers`, `products`, `refunds`, but `/api/executive` supplied `total_orders`, `avg_order_value`, `total_customers`, `units_sold`, `total_refunds`.

Since rebuilding the frontend was explicitly out of scope, the fix is server-side aliasing in `/api/executive` (plus a `count(*) filter (where status='completed')` on `fact_orders`). After a cache-bypassed reload the tiles read:

```
ORDERS              120,000   95,464 completed
AVG ORDER VALUE     $339.57
ACTIVE CUSTOMERS    18,020    333,130 products sold
RETURN RATE         6.0%      $1.99M refunded
```

Screenshot: `screenshots/07-overview-fixed.png`

## 9. Remaining issues

1. **Ports 8000 and 8010 are unavailable** — occupied by a different pre-existing app. The dashboard runs on 8020.
2. **Two order totals are exposed by design.** `orders` = 120,000 (all statuses, what the UI shows) and `total_orders` = 105,567 (completed + returned, the revenue-bearing denominator behind AOV and net revenue). Both are correct under their own definition and the distinction is documented in `mart_executive_kpis.sql`, but a consumer picking `total_orders` will disagree with the UI by 14,433.
3. **Overview AOV hint is mislabelled in the shipped bundle** — it says "net revenue / shipped orders" while the value is gross revenue / shipped orders ($339.57). Cosmetic, frontend-only; not fixed because no rebuild was permitted.
4. **AWS is not deployed.** This is a local stand-in environment: filesystem lake (not S3/MinIO) and PostgreSQL 17 on port 5433 (not Redshift). No AWS credentials are present, so no S3 bucket, Glue job, Redshift cluster, Athena table or EventBridge rule has been created. Deployable artifacts written but never executed: `sql/redshift_ddl.sql`, `aws/glue/bronze_to_silver.py`.
5. **Code cleanup pending in `src/load_warehouse.py`**: a dead `read_gold("daily_sales")` call in `load_fact_orders`, a no-op `.map()` in `_stage`, and a `datetime.utcnow()` deprecation warning. Harmless; not touched to avoid an unverified reload.
6. **Not yet built**: `tests/` pytest suite, `README.md` runbook, `.env` template, `Dockerfile`, and the remaining AWS IaC (Athena DDL/queries, EventBridge rules, IAM policies, CloudFormation/CDK, CloudWatch dashboard, `lambda/s3_ingest_handler.py`).
7. **Incremental load not demonstrated end-to-end.** Watermarks are recorded (`ops.watermarks`, orders last value `2026-11-01T23:54:43Z`) and every upsert is `ON CONFLICT`/SCD2-idempotent, but no second `--since` run has been executed to prove a delta load in this session.
