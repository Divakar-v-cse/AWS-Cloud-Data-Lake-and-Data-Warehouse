{{ config(materialized='table') }}

with refunds as (
    select order_id, sum(refund_amount) as refund_amount
    from {{ ref('stg_returns') }}
    where status in ('completed', 'approved')
    group by 1
),
orders as (
    select o.*, coalesce(r.refund_amount, 0) as refund
    from {{ ref('stg_orders') }} o
    left join refunds r using (order_id)
),
shipped as (
    select * from orders where status in ('completed', 'returned')
),
daily as (
    select
        order_date,
        date_trunc('month', order_date)::date as month_start,
        count(*)                                as orders,
        count(distinct customer_sk)             as customers,
        sum(item_count)                         as units_sold,
        sum(order_total)                        as gross_revenue,
        sum(refund)                             as refunds,
        sum(order_total) - sum(refund)          as net_revenue,
        avg(order_total)                        as aov
    from shipped
    group by 1, 2
),
status_counts as (
    select order_date,
           sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_orders,
           sum(case when status = 'pending'   then 1 else 0 end) as pending_orders,
           sum(case when status = 'returned'  then 1 else 0 end) as returned_orders
    from orders
    group by 1
)
select
    d.order_date,
    d.month_start,
    d.orders,
    d.customers,
    d.units_sold,
    round(d.gross_revenue, 2)                                   as gross_revenue,
    round(d.refunds, 2)                                         as refunds,
    round(d.net_revenue, 2)                                     as net_revenue,
    round(d.aov, 2)                                             as aov,
    s.cancelled_orders,
    s.pending_orders,
    s.returned_orders,
    round(avg(d.net_revenue) over (order by d.order_date
          rows between 6 preceding and current row), 2)         as revenue_7d_ma
from daily d
join status_counts s using (order_date)
order by d.order_date
