-- Athena / Glue Data Catalog DDL for the gold layer.
--
-- The stack's crawler (divakar-gold-crawler-<env>) registers these datasets
-- automatically. Run this file only when you want deterministic table
-- definitions instead of crawler inference - for example in a fresh account, or
-- in CI where you want the schema under version control.
--
-- Replace {{GOLD_BUCKET}} with the bucket from .aws-deploy.json, e.g.
--   divakar-lake-gold-dev-123456789012-ap-south-1
-- and create the database first:
--   CREATE DATABASE IF NOT EXISTS divakar_gold_dev;

CREATE DATABASE IF NOT EXISTS divakar_gold_dev;

-- ---------------------------------------------------------------------------
-- daily_sales - one row per order date (gold/daily_sales/daily_sales.parquet)
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.daily_sales (
  order_date        timestamp,
  year              int,
  orders            bigint,
  completed_orders  bigint,
  cancelled_orders  bigint,
  returned_orders   bigint,
  pending_orders    bigint,
  shipped_orders    bigint,
  units_sold        bigint,
  gross_revenue     double,
  refunds           double,
  net_revenue       double,
  aov               double,
  active_customers  bigint,
  revenue_7d_ma     double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/daily_sales/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ---------------------------------------------------------------------------
-- monthly_revenue
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.monthly_revenue (
  order_month         string,
  year                bigint,
  orders              bigint,
  shipped_orders      bigint,
  customers           bigint,
  gross_revenue       double,
  refunds             double,
  net_revenue         double,
  aov                 double,
  revenue_growth_pct  double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/monthly_revenue/';

-- ---------------------------------------------------------------------------
-- customer_analytics - RFM scores, segment, CLV
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.customer_analytics (
  customer_id                 string,
  first_order_date            timestamp,
  last_order_date             timestamp,
  orders                      bigint,
  monetary                    double,
  avg_order_value             double,
  total_items                 bigint,
  recency_days                bigint,
  tenure_days                 bigint,
  is_repeat_customer          boolean,
  purchase_frequency_per_year double,
  predicted_clv               double,
  r_score                     bigint,
  f_score                     bigint,
  m_score                     bigint,
  rfm_score                   bigint,
  segment                     string,
  first_name                  string,
  last_name                   string,
  email                       string,
  city                        string,
  state                       string,
  region                      string,
  signup_date                 timestamp,
  loyalty_tier                string,
  customer_name               string
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/customer_analytics/';

-- ---------------------------------------------------------------------------
-- cohort_retention - cohort month x month_index
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.cohort_retention (
  cohort        string,
  month_index   bigint,
  customers     bigint,
  cohort_size   bigint,
  retention_pct double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/cohort_retention/';

-- ---------------------------------------------------------------------------
-- product_performance
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.product_performance (
  product_id        string,
  product_name      string,
  category          string,
  subcategory       string,
  brand             string,
  units_sold        bigint,
  orders            bigint,
  revenue           double,
  profit            double,
  avg_selling_price double,
  returned_units    bigint,
  refund_amount     double,
  return_rate       double,
  margin_pct        double,
  revenue_rank      bigint
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/product_performance/';

-- ---------------------------------------------------------------------------
-- regional_sales
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.regional_sales (
  region             string,
  state              string,
  orders             bigint,
  customers          bigint,
  revenue            double,
  aov                double,
  revenue_share_pct  double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/regional_sales/';

-- ---------------------------------------------------------------------------
-- payment_analytics
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.payment_analytics (
  payment_method  string,
  attempts        bigint,
  amount          double,
  success_count   bigint,
  failed_count    bigint,
  refunded_count  bigint,
  pending_count   bigint,
  success_rate    double,
  failure_rate    double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/payment_analytics/';

-- ---------------------------------------------------------------------------
-- fulfilment - carrier performance
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.fulfilment (
  carrier               string,
  shipments             bigint,
  avg_processing_days   double,
  avg_delivery_days     double,
  late_shipments        bigint,
  late_rate             double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/fulfilment/';

-- ---------------------------------------------------------------------------
-- return_analytics
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS divakar_gold_dev.return_analytics (
  category       string,
  reason         string,
  returns        bigint,
  units          bigint,
  refund_amount  double
)
STORED AS PARQUET
LOCATION 's3://{{GOLD_BUCKET}}/return_analytics/';
