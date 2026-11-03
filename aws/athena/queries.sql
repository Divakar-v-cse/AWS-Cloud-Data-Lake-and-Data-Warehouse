-- Representative Athena queries over the gold layer.
--
-- Athena queries S3 directly, so these work even when the Redshift workgroup is
-- paused or deleted. They are the "cheap" path for ad-hoc analysis; the dashboard
-- and the API use Redshift for repeated, sub-second reads.
--
-- Result location: each query needs an output bucket. In the console Athena
-- defaults to one; from the CLI add:
--   --result-configuration OutputLocation=s3://<artifacts bucket>/athena-results/
-- The artifacts bucket already exists and is already the pipeline's scratch
-- space, so no extra bucket is required.

-- 1. Revenue trend with the 7-day moving average the API exposes as
--    /api/sales/daily. Compare this to the dashboard's "Revenue (7d MA)" line.
SELECT order_date,
       orders,
       net_revenue,
       revenue_7d_ma
FROM divakar_gold_dev.daily_sales
WHERE order_date >= current_date - interval '90' day
ORDER BY order_date;

-- 2. Month-over-month growth, highest first.
SELECT order_month,
       orders,
       customers,
       net_revenue,
       aov,
       revenue_growth_pct
FROM divakar_gold_dev.monthly_revenue
ORDER BY order_month DESC
LIMIT 24;

-- 3. RFM segment mix: customers, revenue and average CLV per segment.
SELECT segment,
       count(*)                            AS customers,
       round(sum(monetary), 2)             AS revenue,
       round(avg(avg_order_value), 2)      AS aov,
       round(avg(predicted_clv), 2)        AS avg_clv,
       round(avg(recency_days), 1)         AS avg_recency_days
FROM divakar_gold_dev.customer_analytics
GROUP BY segment
ORDER BY revenue DESC;

-- 4. Cohort retention heat map, cohort size >= 100 customers.
SELECT cohort,
       month_index,
       customers,
       cohort_size,
       round(retention_pct, 2) AS retention_pct
FROM divakar_gold_dev.cohort_retention
WHERE cohort_size >= 100
ORDER BY cohort, month_index;

-- 5. Products that sell well but lose money - the classic margin trap.
SELECT product_name,
       category,
       units_sold,
       round(revenue, 2)     AS revenue,
       round(profit, 2)      AS profit,
       round(margin_pct, 2)  AS margin_pct,
       round(return_rate, 2) AS return_rate_pct
FROM divakar_gold_dev.product_performance
WHERE profit < 0
   OR (margin_pct < 5 AND units_sold > 100)
ORDER BY revenue DESC
LIMIT 25;

-- 6. Return reasons by category, ranked, to see where refunds concentrate.
SELECT category,
       reason,
       returns,
       units,
       round(refund_amount, 2) AS refund_amount
FROM divakar_gold_dev.return_analytics
ORDER BY refund_amount DESC
LIMIT 25;

-- 7. Payment success rate per method, worst first.
SELECT payment_method,
       attempts,
       success_count,
       failed_count,
       round(success_rate, 3) AS success_rate,
       round(amount, 2)       AS amount
FROM divakar_gold_dev.payment_analytics
ORDER BY success_rate ASC;

-- 8. Carrier performance - late rate against the average delivery time.
SELECT carrier,
       shipments,
       round(avg_processing_days, 2) AS processing_days,
       round(avg_delivery_days, 2)   AS delivery_days,
       late_shipments,
       round(late_rate, 4)           AS late_rate
FROM divakar_gold_dev.fulfilment
ORDER BY late_rate DESC;

-- 9. Regional concentration: which states carry the revenue.
SELECT region,
       state,
       orders,
       customers,
       round(revenue, 2)           AS revenue,
       round(revenue_share_pct, 2) AS share_pct
FROM divakar_gold_dev.regional_sales
ORDER BY revenue DESC
LIMIT 20;

-- 10. Data-quality probe: the gold datasets should agree on total gross revenue
--     within the tolerance introduced by refunds and cancelled orders. A large
--     gap means one dataset was built from a stale silver partition.
SELECT 'daily_sales' AS dataset, round(sum(gross_revenue), 2) AS gross
FROM divakar_gold_dev.daily_sales
UNION ALL
SELECT 'monthly_revenue', round(sum(gross_revenue), 2)
FROM divakar_gold_dev.monthly_revenue
UNION ALL
SELECT 'regional_sales', round(sum(revenue), 2)
FROM divakar_gold_dev.regional_sales;
