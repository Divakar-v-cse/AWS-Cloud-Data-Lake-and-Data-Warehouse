-- ===========================================================================
-- Divakar Cloud Data Warehouse - Redshift load script
--
-- Executed statement-by-statement by aws/lambda/loader/handler.py through the
-- Redshift Data API. The Data API accepts exactly one statement per call and
-- does not understand psql/SQLWorkbench scripting, so statements are separated
-- by a marker line:
--
--     -- @@ <statement_name>
--
-- The handler splits on those markers, substitutes {{...}} placeholders, then
-- runs each statement and records its duration in ops.pipeline_runs.details.
--
-- Prerequisites
--   * sql/redshift_ddl.sql has already run (dwh schema + star schema tables)
--   * s3://{{GOLD_BUCKET}}/warehouse/<source>/ exists, written by the
--     silver_to_gold Glue job's export_warehouse step
--   * the Redshift namespace has {{COPY_ROLE_ARN}} attached so COPY can read S3
--
-- Order matters: staging -> dimensions -> facts. Facts reference dimension
-- surrogate keys, so dims must be current before the fact inserts run.
-- STATUS: not applied. No valid AWS credentials exist in this environment.
-- ===========================================================================

-- @@ 00_ops_schema
-- Checkpoint for this load run. Written even when a later statement fails, so a
-- failed load is visible in the ops table instead of only in Lambda logs.
CREATE SCHEMA IF NOT EXISTS ops;

-- @@ 01_ops_tables
-- Audit tables at the same grain as the local ops.* schema, so /api/ops/*
-- behaves identically in both environments.
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id          VARCHAR(64)  NOT NULL,
    pipeline        VARCHAR(60)  NOT NULL,
    stage           VARCHAR(40)  NOT NULL,
    source          VARCHAR(40)  NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    started_at      TIMESTAMPTZ  NOT NULL,
    finished_at     TIMESTAMPTZ,
    records_in      BIGINT       DEFAULT 0,
    records_out     BIGINT       DEFAULT 0,
    error_message   VARCHAR(2000),
    details         SUPER,
    PRIMARY KEY (run_id)
);

-- @@ 02_staging_customers_ddl
-- Column order matches aws/glue/silver_to_gold.py WAREHOUSE_EXPORT exactly;
-- COPY from Parquet maps positionally, so any drift here is a visible error
-- rather than silent column swapping.
CREATE TABLE IF NOT EXISTS staging.silver_customers (
    customer_id VARCHAR(20), first_name VARCHAR(60), last_name VARCHAR(60),
    email VARCHAR(160), phone VARCHAR(40), city VARCHAR(80), state VARCHAR(40),
    region VARCHAR(30), country VARCHAR(40), postal_code VARCHAR(20),
    signup_date TIMESTAMP, loyalty_tier VARCHAR(20), is_active BOOLEAN,
    updated_at TIMESTAMP
);

-- @@ 03_staging_products_ddl
CREATE TABLE IF NOT EXISTS staging.silver_products (
    product_id VARCHAR(20), sku VARCHAR(40), product_name VARCHAR(160),
    category VARCHAR(60), subcategory VARCHAR(60), brand VARCHAR(60),
    unit_cost NUMERIC(12,2), unit_price NUMERIC(12,2), weight_kg NUMERIC(10,3),
    is_active BOOLEAN, launch_date TIMESTAMP, updated_at TIMESTAMP
);

-- @@ 04_staging_orders_ddl
CREATE TABLE IF NOT EXISTS staging.silver_orders (
    order_id VARCHAR(20), customer_id VARCHAR(20), order_ts TIMESTAMP,
    status VARCHAR(20), channel VARCHAR(30), region VARCHAR(30), state VARCHAR(40),
    city VARCHAR(80), item_count INTEGER, subtotal NUMERIC(14,2),
    discount_amount NUMERIC(14,2), shipping_cost NUMERIC(12,2),
    tax_amount NUMERIC(14,2), order_total NUMERIC(14,2), updated_at TIMESTAMP
);

-- @@ 05_staging_order_items_ddl
CREATE TABLE IF NOT EXISTS staging.silver_order_items (
    order_item_id VARCHAR(24), order_id VARCHAR(20), product_id VARCHAR(20),
    quantity INTEGER, unit_price NUMERIC(12,2), unit_cost NUMERIC(12,2),
    discount_pct NUMERIC(6,4), line_subtotal NUMERIC(14,2), line_total NUMERIC(14,2),
    order_ts TIMESTAMP
);

-- @@ 06_staging_payments_ddl
CREATE TABLE IF NOT EXISTS staging.silver_payments (
    payment_id VARCHAR(24), order_id VARCHAR(20), payment_ts TIMESTAMP,
    payment_method VARCHAR(40), amount NUMERIC(14,2), status VARCHAR(20),
    attempt_number INTEGER
);

-- @@ 07_staging_shipments_ddl
CREATE TABLE IF NOT EXISTS staging.silver_shipments (
    shipment_id VARCHAR(24), order_id VARCHAR(20), shipped_ts TIMESTAMP,
    delivered_ts TIMESTAMP, promised_date TIMESTAMP, carrier VARCHAR(30),
    status VARCHAR(20), shipping_cost NUMERIC(12,2), delivery_days NUMERIC(8,2),
    is_late BOOLEAN
);

-- @@ 08_staging_returns_ddl
CREATE TABLE IF NOT EXISTS staging.silver_returns (
    return_id VARCHAR(24), order_id VARCHAR(20), order_item_id VARCHAR(24),
    product_id VARCHAR(20), return_ts TIMESTAMP, reason VARCHAR(60),
    quantity INTEGER, refund_amount NUMERIC(14,2), status VARCHAR(20), restocked BOOLEAN
);

-- @@ 10_truncate_staging
TRUNCATE TABLE staging.silver_customers;
-- @@ 11_truncate_staging
TRUNCATE TABLE staging.silver_products;
-- @@ 12_truncate_staging
TRUNCATE TABLE staging.silver_orders;
-- @@ 13_truncate_staging
TRUNCATE TABLE staging.silver_order_items;
-- @@ 14_truncate_staging
TRUNCATE TABLE staging.silver_payments;
-- @@ 15_truncate_staging
TRUNCATE TABLE staging.silver_shipments;
-- @@ 16_truncate_staging
TRUNCATE TABLE staging.silver_returns;

-- @@ 20_copy_customers
COPY staging.silver_customers
FROM 's3://{{GOLD_BUCKET}}/warehouse/customers/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 21_copy_products
COPY staging.silver_products
FROM 's3://{{GOLD_BUCKET}}/warehouse/products/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 22_copy_orders
COPY staging.silver_orders
FROM 's3://{{GOLD_BUCKET}}/warehouse/orders/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 23_copy_order_items
COPY staging.silver_order_items
FROM 's3://{{GOLD_BUCKET}}/warehouse/order_items/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 24_copy_payments
COPY staging.silver_payments
FROM 's3://{{GOLD_BUCKET}}/warehouse/payments/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 25_copy_shipments
COPY staging.silver_shipments
FROM 's3://{{GOLD_BUCKET}}/warehouse/shipments/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 26_copy_returns
COPY staging.silver_returns
FROM 's3://{{GOLD_BUCKET}}/warehouse/returns/'
IAM_ROLE '{{COPY_ROLE_ARN}}'
FORMAT AS PARQUET;

-- @@ 30_dim_date
-- 2023-01-01 .. 2027-12-31, same range and the same ISO week semantics as
-- src/load_warehouse.py:load_dim_date (day_of_week is ISO 1=Monday, week is ISO).
INSERT INTO dwh.dim_date (date_key, full_date, year, quarter, month, month_name,
                          month_short, day_of_month, day_of_week, day_name,
                          week_of_year, is_weekend, year_month)
SELECT to_char(d, 'YYYYMMDD')::int,
       d,
       EXTRACT(year FROM d)::smallint,
       EXTRACT(quarter FROM d)::smallint,
       EXTRACT(month FROM d)::smallint,
       to_char(d, 'FMMonth'),
       to_char(d, 'Mon'),
       EXTRACT(day FROM d)::smallint,
       -- Redshift EXTRACT(dow) is 0=Sunday..6=Saturday; ISO is 1=Monday..7=Sunday.
       CASE WHEN EXTRACT(dow FROM d) = 0 THEN 7 ELSE EXTRACT(dow FROM d) END::smallint,
       to_char(d, 'FMDay'),
       EXTRACT(week FROM d)::smallint,
       CASE WHEN EXTRACT(dow FROM d) IN (0, 6) THEN TRUE ELSE FALSE END,
       to_char(d, 'YYYY-MM')
FROM (SELECT dateadd(day, n, DATE '2023-01-01') AS d
      FROM (SELECT ROW_NUMBER() OVER () - 1 AS n
            FROM staging.silver_orders LIMIT 1827)) days
WHERE d <= DATE '2027-12-31'
  AND NOT EXISTS (SELECT 1 FROM dwh.dim_date x WHERE x.date_key = to_char(d, 'YYYYMMDD')::int);

-- @@ 31_dim_payment_method
INSERT INTO dwh.dim_payment_method (method_key, method_name, method_group)
SELECT ROW_NUMBER() OVER (ORDER BY payment_method)::smallint,
       payment_method,
       CASE WHEN payment_method IN ('card', 'credit_card', 'debit_card') THEN 'card'
            WHEN payment_method IN ('upi', 'netbanking', 'wallet') THEN 'bank'
            ELSE 'other' END
FROM (SELECT DISTINCT payment_method FROM staging.silver_payments) p
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_payment_method x
                  WHERE x.method_name = p.payment_method);

-- @@ 40_scd2_customer_close
-- Any row whose tracked attributes changed is closed; unchanged rows keep their
-- open version, so a repeat run is a no-op instead of a version explosion.
UPDATE dwh.dim_customer
   SET valid_to = CURRENT_TIMESTAMP, is_current = FALSE
 WHERE is_current
   AND customer_id IN (
       SELECT s.customer_id
         FROM staging.silver_customers s
         JOIN dwh.dim_customer d
           ON d.customer_id = s.customer_id AND d.is_current
        WHERE d.hash_diff <> md5(
              coalesce(s.first_name,'') || '|' || coalesce(s.last_name,'')  || '|' ||
              coalesce(s.email,'')      || '|' || coalesce(s.city,'')       || '|' ||
              coalesce(s.state,'')      || '|' || coalesce(s.region,'')     || '|' ||
              coalesce(s.country,'')    || '|' || coalesce(s.postal_code,'')|| '|' ||
              coalesce(s.loyalty_tier,'')|| '|' || coalesce(s.is_active::varchar,'')));

-- @@ 41_scd2_customer_insert
INSERT INTO dwh.dim_customer (customer_id, first_name, last_name, email, city, state,
                              region, country, loyalty_tier, signup_date, valid_from,
                              valid_to, is_current, hash_diff)
SELECT s.customer_id, s.first_name, s.last_name, s.email, s.city, s.state,
       s.region, s.country, s.loyalty_tier, s.signup_date::date, CURRENT_TIMESTAMP,
       NULL, TRUE,
       md5(coalesce(s.first_name,'') || '|' || coalesce(s.last_name,'')  || '|' ||
           coalesce(s.email,'')      || '|' || coalesce(s.city,'')       || '|' ||
           coalesce(s.state,'')      || '|' || coalesce(s.region,'')     || '|' ||
           coalesce(s.country,'')    || '|' || coalesce(s.postal_code,'')|| '|' ||
           coalesce(s.loyalty_tier,'')|| '|' || coalesce(s.is_active::varchar,''))
  FROM staging.silver_customers s
 WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_customer d
                    WHERE d.customer_id = s.customer_id AND d.is_current);

-- @@ 42_scd2_product_close
UPDATE dwh.dim_product
   SET valid_to = CURRENT_TIMESTAMP, is_current = FALSE
 WHERE is_current
   AND product_id IN (
       SELECT s.product_id
         FROM staging.silver_products s
         JOIN dwh.dim_product d ON d.product_id = s.product_id AND d.is_current
        WHERE d.hash_diff <> md5(
              coalesce(s.product_name,'') || '|' || coalesce(s.category,'')    || '|' ||
              coalesce(s.subcategory,'')  || '|' || coalesce(s.brand,'')       || '|' ||
              coalesce(s.unit_cost::varchar,'') || '|' ||
              coalesce(s.unit_price::varchar,'') || '|' ||
              coalesce(s.is_active::varchar,'')));

-- @@ 43_scd2_product_insert
INSERT INTO dwh.dim_product (product_id, sku, product_name, category, subcategory,
                             brand, unit_cost, unit_price, is_active, launch_date,
                             valid_from, valid_to, is_current, hash_diff)
SELECT s.product_id, s.sku, s.product_name, s.category, s.subcategory, s.brand,
       s.unit_cost, s.unit_price, s.is_active, s.launch_date::date, CURRENT_TIMESTAMP,
       NULL, TRUE,
       md5(coalesce(s.product_name,'') || '|' || coalesce(s.category,'')    || '|' ||
           coalesce(s.subcategory,'')  || '|' || coalesce(s.brand,'')       || '|' ||
           coalesce(s.unit_cost::varchar,'') || '|' ||
           coalesce(s.unit_price::varchar,'') || '|' ||
           coalesce(s.is_active::varchar,''))
  FROM staging.silver_products s
 WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_product d
                    WHERE d.product_id = s.product_id AND d.is_current);

-- @@ 50_fact_orders
-- Delete-then-insert keeps the load idempotent without a MERGE (Redshift MERGE
-- cannot be used for delete+insert of the same key set in one statement).
DELETE FROM dwh.fact_orders WHERE order_id IN (SELECT order_id FROM staging.silver_orders);

-- @@ 51_fact_orders_insert
INSERT INTO dwh.fact_orders (order_id, customer_sk, date_key, order_ts, status,
                             channel, region, state, city, item_count, subtotal,
                             discount_amount, shipping_cost, tax_amount, order_total,
                             is_completed, updated_at)
SELECT s.order_id, c.customer_sk, to_char(s.order_ts, 'YYYYMMDD')::int, s.order_ts,
       s.status, s.channel, s.region, s.state, s.city, s.item_count, s.subtotal,
       s.discount_amount, s.shipping_cost, s.tax_amount, s.order_total,
       CASE WHEN s.status = 'completed' THEN TRUE ELSE FALSE END, s.updated_at
  FROM staging.silver_orders s
  JOIN dwh.dim_customer c ON c.customer_id = s.customer_id AND c.is_current;

-- @@ 52_fact_order_items
DELETE FROM dwh.fact_order_items WHERE order_id IN (SELECT order_id FROM staging.silver_order_items);

-- @@ 53_fact_order_items_insert
INSERT INTO dwh.fact_order_items (order_item_id, order_id, product_sk, customer_sk,
                                  date_key, quantity, unit_price, unit_cost,
                                  discount_pct, line_subtotal, line_total, line_profit)
SELECT i.order_item_id, i.order_id, p.product_sk, o.customer_sk, o.date_key,
       i.quantity, i.unit_price, i.unit_cost, i.discount_pct, i.line_subtotal,
       i.line_total, round(i.line_total - i.unit_cost * i.quantity, 2)
  FROM staging.silver_order_items i
  JOIN dwh.fact_orders o     ON o.order_id = i.order_id
  JOIN dwh.dim_product p     ON p.product_id = i.product_id AND p.is_current;

-- @@ 54_fact_payments
DELETE FROM dwh.fact_payments WHERE payment_id IN (SELECT payment_id FROM staging.silver_payments);

-- @@ 55_fact_payments_insert
INSERT INTO dwh.fact_payments (payment_id, order_id, date_key, payment_ts,
                               payment_method, method_key, amount, status, attempt_number)
SELECT p.payment_id, p.order_id, to_char(p.payment_ts, 'YYYYMMDD')::int, p.payment_ts,
       p.payment_method, m.method_key, p.amount, p.status,
       coalesce(p.attempt_number, 1)
  FROM staging.silver_payments p
  JOIN dwh.fact_orders o        ON o.order_id = p.order_id
  LEFT JOIN dwh.dim_payment_method m ON m.method_name = p.payment_method;

-- @@ 56_fact_shipments
DELETE FROM dwh.fact_shipments WHERE shipment_id IN (SELECT shipment_id FROM staging.silver_shipments);

-- @@ 57_fact_shipments_insert
INSERT INTO dwh.fact_shipments (shipment_id, order_id, date_key, shipped_ts,
                                delivered_ts, promised_date, carrier, status,
                                shipping_cost, delivery_days, is_late)
SELECT s.shipment_id, s.order_id, to_char(s.shipped_ts, 'YYYYMMDD')::int, s.shipped_ts,
       s.delivered_ts, s.promised_date::date, s.carrier, s.status, s.shipping_cost,
       s.delivery_days, coalesce(s.is_late, FALSE)
  FROM staging.silver_shipments s
  JOIN dwh.fact_orders o ON o.order_id = s.order_id;

-- @@ 58_fact_returns
DELETE FROM dwh.fact_returns WHERE return_id IN (SELECT return_id FROM staging.silver_returns);

-- @@ 59_fact_returns_insert
INSERT INTO dwh.fact_returns (return_id, order_id, order_item_id, product_sk, date_key,
                              return_ts, reason, quantity, refund_amount, status, restocked)
SELECT r.return_id, r.order_id, r.order_item_id, p.product_sk,
       to_char(r.return_ts, 'YYYYMMDD')::int, r.return_ts, r.reason, r.quantity,
       r.refund_amount, r.status, r.restocked
  FROM staging.silver_returns r
  JOIN dwh.fact_orders o         ON o.order_id = r.order_id
  LEFT JOIN dwh.dim_product p    ON p.product_id = r.product_id AND p.is_current;

-- @@ 60_analyze
-- Refreshes statistics so the dashboard's aggregate queries plan well straight
-- after a load instead of waiting for auto-analyze.
ANALYZE dwh.fact_orders;
