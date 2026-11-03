-- ===========================================================================
-- Divakar Cloud Data Warehouse - dimensional model
-- Target: PostgreSQL 16/17 locally, Amazon Redshift in AWS (same ANSI DDL).
-- Redshift-specific DISTKEY / SORTKEY / ENCODE variants live in
-- sql/redshift_ddl.sql; this file stays portable.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------------
-- Conformed date dimension
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER      PRIMARY KEY,          -- YYYYMMDD
    full_date       DATE         NOT NULL UNIQUE,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(12)  NOT NULL,
    month_short     VARCHAR(4)   NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,             -- 1 = Monday
    day_name        VARCHAR(12)  NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    year_month      CHAR(7)      NOT NULL
);

-- ---------------------------------------------------------------------------
-- SCD Type 2 dimensions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_sk     BIGSERIAL    PRIMARY KEY,
    customer_id     VARCHAR(20)  NOT NULL,
    first_name      VARCHAR(60),
    last_name       VARCHAR(60),
    email           VARCHAR(160),
    city            VARCHAR(80),
    state           VARCHAR(40),
    region          VARCHAR(30),
    country         VARCHAR(40),
    loyalty_tier    VARCHAR(20),
    signup_date     DATE,
    valid_from      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    hash_diff       VARCHAR(64)  NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dim_customer_nk ON dwh.dim_customer (customer_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current
    ON dwh.dim_customer (customer_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_sk      BIGSERIAL    PRIMARY KEY,
    product_id      VARCHAR(20)  NOT NULL,
    product_name    VARCHAR(200),
    sku             VARCHAR(40),
    category        VARCHAR(60),
    subcategory     VARCHAR(60),
    brand           VARCHAR(60),
    unit_cost       NUMERIC(12,2),
    unit_price      NUMERIC(12,2),
    is_active       BOOLEAN,
    launch_date     DATE,
    valid_from      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    hash_diff       VARCHAR(64)  NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dim_product_nk ON dwh.dim_product (product_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_product_current
    ON dwh.dim_product (product_id) WHERE is_current;

-- ---------------------------------------------------------------------------
-- Junk / small dimension
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.dim_payment_method (
    method_key      SMALLINT     PRIMARY KEY,
    method_name     VARCHAR(40)  NOT NULL UNIQUE,
    method_group    VARCHAR(30)  NOT NULL
);

-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id        VARCHAR(20)  PRIMARY KEY,
    customer_sk     BIGINT       NOT NULL REFERENCES dwh.dim_customer (customer_sk),
    date_key        INTEGER      NOT NULL REFERENCES dwh.dim_date (date_key),
    order_ts        TIMESTAMP    NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    channel         VARCHAR(30),
    region          VARCHAR(30),
    state           VARCHAR(40),
    city            VARCHAR(80),
    item_count      INTEGER      NOT NULL,
    subtotal        NUMERIC(14,2) NOT NULL,
    discount_amount NUMERIC(14,2) NOT NULL,
    shipping_cost   NUMERIC(12,2) NOT NULL,
    tax_amount      NUMERIC(14,2) NOT NULL,
    order_total     NUMERIC(14,2) NOT NULL,
    is_completed    BOOLEAN      NOT NULL,
    updated_at      TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_fact_orders_date ON dwh.fact_orders (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_orders_status ON dwh.fact_orders (status);

CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    order_item_id   VARCHAR(24)  PRIMARY KEY,
    order_id        VARCHAR(20)  NOT NULL REFERENCES dwh.fact_orders (order_id),
    product_sk      BIGINT       NOT NULL REFERENCES dwh.dim_product (product_sk),
    customer_sk     BIGINT       NOT NULL REFERENCES dwh.dim_customer (customer_sk),
    date_key        INTEGER      NOT NULL REFERENCES dwh.dim_date (date_key),
    quantity        INTEGER      NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    unit_cost       NUMERIC(12,2) NOT NULL,
    discount_pct    NUMERIC(6,4) NOT NULL,
    line_subtotal   NUMERIC(14,2) NOT NULL,
    line_total      NUMERIC(14,2) NOT NULL,
    line_profit     NUMERIC(14,2) NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_items_order ON dwh.fact_order_items (order_id);
CREATE INDEX IF NOT EXISTS ix_fact_items_product ON dwh.fact_order_items (product_sk);

CREATE TABLE IF NOT EXISTS dwh.fact_payments (
    payment_id      VARCHAR(24)  PRIMARY KEY,
    order_id        VARCHAR(20)  NOT NULL REFERENCES dwh.fact_orders (order_id),
    date_key        INTEGER      NOT NULL REFERENCES dwh.dim_date (date_key),
    payment_ts      TIMESTAMP    NOT NULL,
    payment_method  VARCHAR(40)  NOT NULL,
    method_key      SMALLINT     REFERENCES dwh.dim_payment_method (method_key),
    amount          NUMERIC(14,2) NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    attempt_number  SMALLINT     NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_fact_payments_order ON dwh.fact_payments (order_id);

CREATE TABLE IF NOT EXISTS dwh.fact_shipments (
    shipment_id     VARCHAR(24)  PRIMARY KEY,
    order_id        VARCHAR(20)  NOT NULL REFERENCES dwh.fact_orders (order_id),
    date_key        INTEGER      NOT NULL REFERENCES dwh.dim_date (date_key),
    shipped_ts      TIMESTAMP    NOT NULL,
    delivered_ts    TIMESTAMP,
    promised_date   DATE,
    carrier         VARCHAR(30),
    status          VARCHAR(20)  NOT NULL,
    shipping_cost   NUMERIC(12,2) NOT NULL,
    delivery_days   NUMERIC(8,2),
    is_late         BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dwh.fact_returns (
    return_id       VARCHAR(24)  PRIMARY KEY,
    order_id        VARCHAR(20)  NOT NULL REFERENCES dwh.fact_orders (order_id),
    order_item_id   VARCHAR(24),
    product_sk      BIGINT       REFERENCES dwh.dim_product (product_sk),
    date_key        INTEGER      NOT NULL REFERENCES dwh.dim_date (date_key),
    return_ts       TIMESTAMP    NOT NULL,
    reason          VARCHAR(60),
    quantity        INTEGER      NOT NULL,
    refund_amount   NUMERIC(14,2) NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    restocked       BOOLEAN
);
CREATE INDEX IF NOT EXISTS ix_fact_returns_order ON dwh.fact_returns (order_id);
