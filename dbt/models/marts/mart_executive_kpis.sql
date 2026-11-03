{{ config(materialized='table') }}

with shipped as (
    select * from {{ ref('stg_orders') }} where status in ('completed', 'returned')
),
refunds as (
    select order_id, sum(refund_amount) as refund_amount
    from {{ ref('stg_returns') }}
    where status in ('completed', 'approved')
    group by 1
),
order_rev as (
    select s.*, coalesce(r.refund_amount, 0) as refund
    from shipped s
    left join refunds r using (order_id)
),
totals as (
    select
        sum(order_total)              as gross_revenue,
        sum(refund)                   as total_refunds,
        count(*)                      as total_orders,
        count(distinct customer_sk)   as total_customers,
        avg(order_total)              as aov,
        sum(item_count)               as units_sold
    from order_rev
),
all_orders as (
    select
        count(*)                                             as all_orders,
        sum(case when status = 'completed' then 1 else 0 end) as completed_orders,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_orders,
        sum(case when status = 'returned'  then 1 else 0 end) as returned_orders,
        sum(case when status = 'pending'   then 1 else 0 end) as pending_orders
    from {{ ref('stg_orders') }}
),
pays as (
    select
        count(*)                                                as payment_attempts,
        sum(case when status = 'success'  then 1 else 0 end)    as successful_payments,
        sum(case when status = 'failed'   then 1 else 0 end)    as failed_payments,
        sum(case when status = 'refunded' then 1 else 0 end)    as refunded_payments
    from {{ ref('stg_payments') }}
),
profit as (
    select coalesce(sum(i.line_profit), 0) as gross_profit
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o using (order_id)
    where o.status in ('completed', 'returned')
),
returned_units as (
    select coalesce(sum(quantity), 0) as units
    from {{ ref('stg_returns') }} where status in ('completed', 'approved')
),
products as (select count(*) as total_products from {{ ref('stg_products') }}),
bounds as (select max(order_date) as max_date from order_rev),
windowed as (
    select
        sum(case when order_date >  (select max_date from bounds) - interval '30 days'
                 then order_total else 0 end) as revenue_last_30d,
        sum(case when order_date <= (select max_date from bounds) - interval '30 days'
                  and order_date >  (select max_date from bounds) - interval '60 days'
                 then order_total else 0 end) as revenue_prev_30d
    from order_rev
)
select
    round(t.gross_revenue, 2)                                  as gross_revenue,
    round(t.total_refunds, 2)                                  as total_refunds,
    round(t.gross_revenue - t.total_refunds, 2)                as net_revenue,
    t.total_orders,
    round(t.aov, 2)                                            as avg_order_value,
    t.total_customers,
    p.total_products,
    round(100.0 * t.total_refunds / nullif(t.gross_revenue, 0), 2) as refund_rate_pct,
    round(100.0 * a.completed_orders / nullif(a.all_orders, 0), 2) as order_completion_rate_pct,
    round(100.0 * a.cancelled_orders / nullif(a.all_orders, 0), 2) as cancellation_rate_pct,
    round(100.0 * pays.successful_payments / nullif(pays.payment_attempts, 0), 2) as payment_success_rate_pct,
    round(100.0 * ru.units / nullif(t.units_sold, 0), 2)       as return_rate_pct,
    round(pr.gross_profit, 2)                                  as gross_profit,
    round(100.0 * pr.gross_profit / nullif(t.gross_revenue, 0), 2) as gross_margin_pct,
    round(w.revenue_last_30d, 2)                               as revenue_last_30d,
    round(w.revenue_prev_30d, 2)                               as revenue_prev_30d,
    round(100.0 * (w.revenue_last_30d - w.revenue_prev_30d)
          / nullif(w.revenue_prev_30d, 0), 2)                  as revenue_growth_pct,
    a.pending_orders,
    a.cancelled_orders,
    a.returned_orders,
    pays.failed_payments,
    t.units_sold
from totals t
cross join all_orders a
cross join pays
cross join profit pr
cross join returned_units ru
cross join products p
cross join windowed w
