-- ===========================================================================
-- Divakar Cloud Data Warehouse - Amazon Redshift physical model
--
-- This is the AWS-side companion to sql/warehouse_schema.sql (the portable
-- PostgreSQL version used by the local stand-in). Column names, types and
-- keys are identical, so every loader and every dbt model works unchanged;
-- the only differences are Redshift physical-design clauses:
--
--   DISTKEY  - co-locates a table's rows with the joined dimension/fact
--   SORTKEY  - cluster order for range filters (date_key almost always)
--   ENCODE   - column compression chosen from the observed data shape
--   IDENTITY - replaces BIGSERIAL for surrogate keys
--
-- STATUS: not applied. No valid AWS credentials are available in this
-- environment, so this file is a deployable artifact only. Nothing in
-- README.md or the API claims these objects exist in Redshift.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- Conformed date dimension (small, broadcast everywhere)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER      NOT NULL ENCODE az64,
    full_date       DATE         NOT NULL ENCODE az64,
    year            SMALLINT     NOT NULL ENCODE int2,
    quarter         SMALLINT     NOT NULL ENCODE int2,
    month           SMALLINT     NOT NULL ENCODE int2,
    month_name      VARCHAR(12)  NOT NULL ENCODE bytedict,
    month_short     VARCHAR(4)   NOT NULL ENCODE bytedict,
    day_of_month    SMALLINT     NOT NULL ENCODE int2,
    day_of_week     SMALLINT     NOT NULL ENCODE int2,
    day_name        VARCHAR(12)  NOT NULL ENCODE bytedict,
    week_of_year    SMALLINT     NOT NULL ENCODE int2,
    is_weekend      BOOLEAN      NOT NULL ENCODE raw,
    year_month      CHAR(7)      NOT NULL ENCODE bytedict,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

-- ---------------------------------------------------------------------------
-- SCD Type 2 dimensions
--
-- Redshift has no partial unique index, so "one current row per natural key"
-- is enforced by the loader's close-then-insert transaction plus the
-- dbt test unique_combination_of_columns(customer_id, is_current) filter.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_sk     BIGINT       IDENTITY(1,1) ENCODE az64,
    customer_id     VARCHAR(20)  NOT NULL ENCODE zstd,
    first_name      VARCHAR(60)           ENCODE zstd,
    last_name       VARCHAR(60)           ENCODE zstd,
    email           VARCHAR(160)          ENCODE zstd,
    city            VARCHAR(80)           ENCODE bytedict,
    state           VARCHAR(40)           ENCODE bytedict,
    region          VARCHAR(30)           ENCODE bytedict,
    country         VARCHAR(40)           ENCODE bytedict,
    loyalty_tier    VARCHAR(20)           ENCODE bytedict,
    signup_date     DATE                  ENCODE az64,
    valid_from      TIMESTAMPTZ  NOT NULL ENCODE az64,
    valid_to        TIMESTAMPTZ           ENCODE az64,
    is_current      BOOLEAN      NOT NULL ENCODE raw,
    hash_diff       VARCHAR(64)  NOT NULL ENCODE zstd,
    PRIMARY KEY (customer_sk)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (customer_id, is_current);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_sk      BIGINT       IDENTITY(1,1) ENCODE az64,
    product_id      VARCHAR(20)  NOT NULL ENCODE zstd,
    product_name    VARCHAR(200)          ENCODE zstd,
    sku             VARCHAR(40)           ENCODE zstd,
    category        VARCHAR(60)           ENCODE bytedict,
    subcategory     VARCHAR(60)           ENCODE bytedict,
    brand           VARCHAR(60)           ENCODE bytedict,
    unit_cost       NUMERIC(12,2)         ENCODE az64,
    unit_price      NUMERIC(12,2)         ENCODE az64,
    is_active       BOOLEAN               ENCODE raw,
    launch_date     DATE                  ENCODE az64,
    valid_from      TIMESTAMPTZ  NOT NULL ENCODE az64,
    valid_to        TIMESTAMPTZ           ENCODE az64,
    is_current      BOOLEAN      NOT NULL ENCODE raw,
    hash_diff       VARCHAR(64)  NOT NULL ENCODE zstd,
    PRIMARY KEY (product_sk)
)
DISTSTYLE ALL
SORTKEY (product_id, is_current);

-- ---------------------------------------------------------------------------
-- Junk dimension
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_payment_method (
    method_key      SMALLINT     NOT NULL ENCODE int2,
    method_name     VARCHAR(40)  NOT NULL ENCODE bytedict,
    method_group    VARCHAR(30)  NOT NULL ENCODE bytedict,
    PRIMARY KEY (method_key)
)
DISTSTYLE ALL;

-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id        VARCHAR(20)  NOT NULL ENCODE zstd,
    customer_sk     BIGINT       NOT NULL ENCODE az64,
    date_key        INTEGER      NOT NULL ENCODE az64,
    order_ts        TIMESTAMP    NOT NULL ENCODE az64,
    status          VARCHAR(20)  NOT NULL ENCODE bytedict,
    channel         VARCHAR(30)           ENCODE bytedict,
    region          VARCHAR(30)           ENCODE bytedict,
    state           VARCHAR(40)           ENCODE bytedict,
    city            VARCHAR(80)           ENCODE bytedict,
    item_count      INTEGER      NOT NULL ENCODE int4,
    subtotal        NUMERIC(14,2) NOT NULL ENCODE az64,
    discount_amount NUMERIC(14,2) NOT NULL ENCODE az64,
    shipping_cost   NUMERIC(12,2) NOT NULL ENCODE az64,
    tax_amount      NUMERIC(14,2) NOT NULL ENCODE az64,
    order_total     NUMERIC(14,2) NOT NULL ENCODE az64,
    is_completed    BOOLEAN      NOT NULL ENCODE raw,
    updated_at      TIMESTAMP             ENCODE az64,
    PRIMARY KEY (order_id)
)
DISTSTYLE KEY
DISTKEY (customer_sk)
COMPOUND SORTKEY (date_key, status);

CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    order_item_id   VARCHAR(24)  NOT NULL ENCODE zstd,
    order_id        VARCHAR(20)  NOT NULL ENCODE zstd,
    product_sk      BIGINT       NOT NULL ENCODE az64,
    customer_sk     BIGINT       NOT NULL ENCODE az64,
    date_key        INTEGER      NOT NULL ENCODE az64,
    quantity        INTEGER      NOT NULL ENCODE int4,
    unit_price      NUMERIC(12,2) NOT NULL ENCODE az64,
    unit_cost       NUMERIC(12,2) NOT NULL ENCODE az64,
    discount_pct    NUMERIC(6,4) NOT NULL ENCODE az64,
    line_subtotal   NUMERIC(14,2) NOT NULL ENCODE az64,
    line_total      NUMERIC(14,2) NOT NULL ENCODE az64,
    line_profit     NUMERIC(14,2) NOT NULL ENCODE az64,
    PRIMARY KEY (order_item_id)
)
DISTSTYLE KEY
DISTKEY (product_sk)
COMPOUND SORTKEY (date_key, order_id);

CREATE TABLE IF NOT EXISTS dwh.fact_payments (
    payment_id      VARCHAR(24)  NOT NULL ENCODE zstd,
    order_id        VARCHAR(20)  NOT NULL ENCODE zstd,
    date_key        INTEGER      NOT NULL ENCODE az64,
    payment_ts      TIMESTAMP    NOT NULL ENCODE az64,
    payment_method  VARCHAR(40)  NOT NULL ENCODE bytedict,
    method_key      SMALLINT              ENCODE int2,
    amount          NUMERIC(14,2) NOT NULL ENCODE az64,
    status          VARCHAR(20)  NOT NULL ENCODE bytedict,
    attempt_number  SMALLINT     NOT NULL ENCODE int2,
    PRIMARY KEY (payment_id)
)
DISTSTYLE KEY
DISTKEY (order_id)
COMPOUND SORTKEY (date_key, status);

CREATE TABLE IF NOT EXISTS dwh.fact_shipments (
    shipment_id     VARCHAR(24)  NOT NULL ENCODE zstd,
    order_id        VARCHAR(20)  NOT NULL ENCODE zstd,
    date_key        INTEGER      NOT NULL ENCODE az64,
    shipped_ts      TIMESTAMP    NOT NULL ENCODE az64,
    delivered_ts    TIMESTAMP             ENCODE az64,
    promised_date   DATE                  ENCODE az64,
    carrier         VARCHAR(30)           ENCODE bytedict,
    status          VARCHAR(20)  NOT NULL ENCODE bytedict,
    shipping_cost   NUMERIC(12,2) NOT NULL ENCODE az64,
    delivery_days   NUMERIC(8,2)          ENCODE az64,
    is_late         BOOLEAN      NOT NULL ENCODE raw,
    PRIMARY KEY (shipment_id)
)
DISTSTYLE KEY
DISTKEY (order_id)
COMPOUND SORTKEY (date_key, status);

CREATE TABLE IF NOT EXISTS dwh.fact_returns (
    return_id       VARCHAR(24)  NOT NULL ENCODE zstd,
    order_id        VARCHAR(20)  NOT NULL ENCODE zstd,
    order_item_id   VARCHAR(24)           ENCODE zstd,
    product_sk      BIGINT                ENCODE az64,
    date_key        INTEGER      NOT NULL ENCODE az64,
    return_ts       TIMESTAMP    NOT NULL ENCODE az64,
    reason          VARCHAR(60)           ENCODE bytedict,
    quantity        INTEGER      NOT NULL ENCODE int4,
    refund_amount   NUMERIC(14,2) NOT NULL ENCODE az64,
    status          VARCHAR(20)  NOT NULL ENCODE bytedict,
    restocked       BOOLEAN               ENCODE raw,
    PRIMARY KEY (return_id)
)
DISTSTYLE KEY
DISTKEY (order_id)
COMPOUND SORTKEY (date_key);

-- ---------------------------------------------------------------------------
-- Maintenance (run after the initial bulk load)
-- ---------------------------------------------------------------------------
-- ANALYZE COMPRESSION dwh.fact_orders;
-- VACUUM SORT ONLY dwh.fact_orders TO 95 PERCENT;
-- ANALYZE dwh.fact_orders;
