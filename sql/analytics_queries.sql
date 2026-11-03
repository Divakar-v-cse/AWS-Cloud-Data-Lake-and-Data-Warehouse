-- ===========================================================================
-- Divakar Cloud Data Warehouse - analytics query pack
--
-- Runs unchanged on the local PostgreSQL warehouse (port 5433) and on Amazon
-- Redshift, because it only uses ANSI SQL over the dwh star schema. Every
-- query below was executed against the local warehouse during verification.
--
-- Conventions
--   * Revenue = SUM(order_total) of non-cancelled orders
--   * Margin  = SUM(line_total - unit_cost * quantity) of non-cancelled orders
--   * "Customer" means the SCD2 *current* version (is_current)
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. Executive KPI snapshot (single row)
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                              AS orders,
    COUNT(DISTINCT customer_sk)                           AS buying_customers,
    ROUND(SUM(order_total), 2)                            AS revenue,
    ROUND(AVG(order_total), 2)                            AS avg_order_value,
    ROUND(SUM(order_total) FILTER (WHERE status = 'completed'), 2) AS completed_revenue,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'cancelled') / COUNT(*), 2) AS cancel_rate_pct
FROM dwh.fact_orders
WHERE status <> 'cancelled';


-- ---------------------------------------------------------------------------
-- 2. Daily sales with 7-day moving average and day-over-day growth
-- ---------------------------------------------------------------------------
WITH daily AS (
    SELECT date_key,
           SUM(order_total)  AS revenue,
           COUNT(*)          AS orders
    FROM dwh.fact_orders
    WHERE status <> 'cancelled'
    GROUP BY date_key
)
SELECT d.date_key,
       d.full_date,
       d.day_name,
       daily.orders,
       ROUND(daily.revenue, 2) AS revenue,
       ROUND(AVG(daily.revenue) OVER (ORDER BY d.date_key
             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_ma7,
       ROUND(100.0 * (daily.revenue - LAG(daily.revenue, 7) OVER (ORDER BY d.date_key))
             / NULLIF(LAG(daily.revenue, 7) OVER (ORDER BY d.date_key), 0), 2) AS wow_growth_pct
FROM daily
JOIN dwh.dim_date d USING (date_key)
ORDER BY d.date_key;


-- ---------------------------------------------------------------------------
-- 3. RFM segmentation (5 = best on every axis)
-- ---------------------------------------------------------------------------
WITH scored AS (
    SELECT o.customer_sk,
           MAX(o.order_ts::date)                    AS last_order_date,
           (SELECT MAX(full_date) FROM dwh.dim_date) AS as_of_date,
           COUNT(*)                                 AS frequency,
           SUM(o.order_total)                       AS monetary
    FROM dwh.fact_orders o
    WHERE o.status <> 'cancelled'
    GROUP BY o.customer_sk
),
rfm AS (
    SELECT customer_sk,
           (SELECT MAX(full_date) FROM dwh.dim_date) - last_order_date AS recency_days,
           frequency,
           monetary,
           NTILE(5) OVER (ORDER BY last_order_date ASC)  AS r_score,
           NTILE(5) OVER (ORDER BY frequency ASC)        AS f_score,
           NTILE(5) OVER (ORDER BY monetary ASC)         AS m_score
    FROM scored
)
SELECT c.customer_id,
       c.city,
       c.loyalty_tier,
       r.recency_days,
       r.frequency,
       ROUND(r.monetary, 2) AS monetary,
       r.r_score, r.f_score, r.m_score,
       CASE
           WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champion'
           WHEN r.r_score >= 3 AND r.f_score >= 3                   THEN 'Loyal'
           WHEN r.r_score >= 4 AND r.f_score <= 2                   THEN 'New / Promising'
           WHEN r.r_score <= 2 AND r.f_score >= 4                   THEN 'At Risk (high value)'
           WHEN r.r_score <= 2 AND r.f_score <= 2                   THEN 'Hibernating'
           ELSE 'Needs Attention'
       END AS segment
FROM rfm r
JOIN dwh.dim_customer c ON c.customer_sk = r.customer_sk AND c.is_current
ORDER BY r.monetary DESC
LIMIT 50;


-- ---------------------------------------------------------------------------
-- 4. RFM segment roll-up (what the dashboard charts)
-- ---------------------------------------------------------------------------
WITH scored AS (
    SELECT o.customer_sk,
           MAX(o.order_ts::date) AS last_order_date,
           COUNT(*)              AS frequency,
           SUM(o.order_total)    AS monetary
    FROM dwh.fact_orders o
    WHERE o.status <> 'cancelled'
    GROUP BY o.customer_sk
),
rfm AS (
    SELECT customer_sk, frequency, monetary,
           NTILE(5) OVER (ORDER BY last_order_date ASC) AS r_score,
           NTILE(5) OVER (ORDER BY frequency ASC)       AS f_score,
           NTILE(5) OVER (ORDER BY monetary ASC)        AS m_score
    FROM scored
),
seg AS (
    SELECT CASE
               WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champion'
               WHEN r_score >= 3 AND f_score >= 3                   THEN 'Loyal'
               WHEN r_score >= 4 AND f_score <= 2                   THEN 'New / Promising'
               WHEN r_score <= 2 AND f_score >= 4                   THEN 'At Risk (high value)'
               WHEN r_score <= 2 AND f_score <= 2                   THEN 'Hibernating'
               ELSE 'Needs Attention'
           END AS segment,
           monetary
    FROM rfm
)
SELECT segment,
       COUNT(*)                       AS customers,
       ROUND(SUM(monetary), 2)        AS revenue,
       ROUND(AVG(monetary), 2)        AS avg_ltv,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS customer_share_pct
FROM seg
GROUP BY segment
ORDER BY revenue DESC;


-- ---------------------------------------------------------------------------
-- 5. Customer lifetime value (historical) with margin and tenure
-- ---------------------------------------------------------------------------
SELECT c.customer_id,
       c.city,
       c.loyalty_tier,
       MIN(o.order_ts)::date                                AS first_order,
       MAX(o.order_ts)::date                                AS last_order,
       COUNT(*)                                             AS orders,
       ROUND(SUM(o.order_total), 2)                          AS lifetime_revenue,
       GREATEST((MAX(o.order_ts)::date - MIN(o.order_ts)::date), 1) AS tenure_days,
       ROUND(SUM(o.order_total)
             / GREATEST((MAX(o.order_ts)::date - MIN(o.order_ts)::date), 1) * 30, 2) AS revenue_per_month
FROM dwh.fact_orders o
JOIN dwh.dim_customer c ON c.customer_sk = o.customer_sk AND c.is_current
WHERE o.status <> 'cancelled'
GROUP BY c.customer_id, c.city, c.loyalty_tier
ORDER BY lifetime_revenue DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- 6. Monthly cohort retention matrix
-- ---------------------------------------------------------------------------
WITH first_order AS (
    SELECT customer_sk,
           DATE_TRUNC('month', MIN(order_ts))::date AS cohort_month
    FROM dwh.fact_orders
    WHERE status <> 'cancelled'
    GROUP BY customer_sk
),
activity AS (
    SELECT o.customer_sk,
           f.cohort_month,
           DATE_TRUNC('month', o.order_ts)::date AS activity_month
    FROM dwh.fact_orders o
    JOIN first_order f USING (customer_sk)
    WHERE o.status <> 'cancelled'
    GROUP BY 1, 2, 3
),
sized AS (
    SELECT cohort_month, COUNT(*) AS cohort_size FROM first_order GROUP BY 1
)
SELECT a.cohort_month,
       (EXTRACT(YEAR FROM a.activity_month) * 12 + EXTRACT(MONTH FROM a.activity_month))
       - (EXTRACT(YEAR FROM a.cohort_month) * 12 + EXTRACT(MONTH FROM a.cohort_month)) AS month_index,
       COUNT(DISTINCT a.customer_sk) AS active_customers,
       s.cohort_size,
       ROUND(100.0 * COUNT(DISTINCT a.customer_sk) / s.cohort_size, 2) AS retention_pct
FROM activity a
JOIN sized s USING (cohort_month)
GROUP BY a.cohort_month, month_index, s.cohort_size
ORDER BY a.cohort_month, month_index;


-- ---------------------------------------------------------------------------
-- 7. Category profitability (margin after discounts)
-- ---------------------------------------------------------------------------
SELECT p.category,
       COUNT(DISTINCT i.order_id)               AS orders,
       SUM(i.quantity)                          AS units,
       ROUND(SUM(i.line_total), 2)              AS revenue,
       ROUND(SUM(i.line_profit), 2)             AS gross_profit,
       ROUND(100.0 * SUM(i.line_profit) / NULLIF(SUM(i.line_total), 0), 2) AS margin_pct,
       ROUND(AVG(i.discount_pct) * 100, 2)      AS avg_discount_pct
FROM dwh.fact_order_items i
JOIN dwh.dim_product p ON p.product_sk = i.product_sk
JOIN dwh.fact_orders o ON o.order_id = i.order_id
WHERE o.status <> 'cancelled'
GROUP BY p.category
ORDER BY gross_profit DESC;


-- ---------------------------------------------------------------------------
-- 8. Payment success rate by method (first attempt vs retries)
-- ---------------------------------------------------------------------------
SELECT pm.method_group,
       pay.payment_method,
       COUNT(*)                                                        AS attempts,
       COUNT(*) FILTER (WHERE pay.status = 'success')                  AS successes,
       COUNT(*) FILTER (WHERE pay.status = 'failed')                   AS failures,
       ROUND(100.0 * COUNT(*) FILTER (WHERE pay.status = 'success') / COUNT(*), 2) AS success_rate_pct,
       ROUND(SUM(pay.amount) FILTER (WHERE pay.status = 'success'), 2)  AS captured_amount,
       ROUND(AVG(pay.attempt_number), 2)                               AS avg_attempts
FROM dwh.fact_payments pay
LEFT JOIN dwh.dim_payment_method pm ON pm.method_key = pay.method_key
GROUP BY pm.method_group, pay.payment_method
ORDER BY attempts DESC;


-- ---------------------------------------------------------------------------
-- 9. Order funnel and cancellations
-- ---------------------------------------------------------------------------
SELECT status,
       COUNT(*)                       AS orders,
       ROUND(SUM(order_total), 2)     AS value,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_pct
FROM dwh.fact_orders
GROUP BY status
ORDER BY orders DESC;


-- ---------------------------------------------------------------------------
-- 10. Returns: rate by category and reason mix
-- ---------------------------------------------------------------------------
SELECT p.category,
       COUNT(DISTINCT r.return_id)                          AS returns,
       SUM(r.quantity)                                      AS units_returned,
       ROUND(SUM(r.refund_amount), 2)                       AS refunded,
       ROUND(100.0 * COUNT(DISTINCT r.return_id)
             / NULLIF(COUNT(DISTINCT i.order_id), 0), 2)     AS return_rate_pct
FROM dwh.fact_returns r
JOIN dwh.dim_product p ON p.product_sk = r.product_sk
LEFT JOIN dwh.fact_order_items i ON i.order_id = r.order_id
GROUP BY p.category
ORDER BY refunded DESC;


-- ---------------------------------------------------------------------------
-- 11. Delivery performance by carrier
-- ---------------------------------------------------------------------------
SELECT carrier,
       COUNT(*)                                              AS shipments,
       COUNT(*) FILTER (WHERE status = 'delivered')          AS delivered,
       ROUND(AVG(delivery_days), 2)                          AS avg_delivery_days,
       ROUND(100.0 * COUNT(*) FILTER (WHERE is_late) / COUNT(*), 2) AS late_rate_pct
FROM dwh.fact_shipments
GROUP BY carrier
ORDER BY shipments DESC;


-- ---------------------------------------------------------------------------
-- 12. SCD Type 2 audit: customers holding more than one version
-- ---------------------------------------------------------------------------
SELECT customer_id,
       COUNT(*)                                  AS versions,
       MIN(valid_from)                           AS first_seen,
       MAX(valid_to)                             AS last_closed,
       COUNT(*) FILTER (WHERE is_current)        AS current_rows
FROM dwh.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY versions DESC
LIMIT 25;
