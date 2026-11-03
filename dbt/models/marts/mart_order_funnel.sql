{{ config(materialized='table') }}

-- Order funnel, payments failure modes and fulfilment performance by month.
with orders as (
    select
        year_month,
        order_id,
        status,
        order_total
    from {{ ref('stg_orders') }}
),

status_mix as (
    select
        year_month,
        count(*)                                                          as orders,
        count(*) filter (where status = 'completed')                      as completed_orders,
        count(*) filter (where status = 'cancelled')                      as cancelled_orders,
        count(*) filter (where status = 'returned')                       as returned_orders,
        count(*) filter (where status = 'pending')                        as pending_orders,
        round(sum(order_total) filter (where status in ('completed', 'returned')), 2) as gross_revenue
    from orders
    group by 1
),

payments as (
    select
        o.year_month,
        count(*)                                                   as attempts,
        count(*) filter (where p.status = 'success')               as successful,
        count(*) filter (where p.status = 'failed')                as failed,
        count(*) filter (where p.status = 'refunded')              as refunded
    from {{ ref('stg_payments') }} p
    join orders o using (order_id)
    group by 1
),

shipments as (
    select
        o.year_month,
        count(*)                                        as shipments,
        avg(s.delivery_days)                            as avg_delivery_days,
        count(*) filter (where s.is_late)               as late_shipments
    from {{ ref('stg_shipments') }} s
    join orders o using (order_id)
    group by 1
)

select
    s.year_month,
    s.orders,
    s.completed_orders,
    s.cancelled_orders,
    s.returned_orders,
    s.pending_orders,
    s.gross_revenue,
    round(100.0 * s.completed_orders / nullif(s.orders, 0), 2)   as completion_rate_pct,
    round(100.0 * s.cancelled_orders / nullif(s.orders, 0), 2)   as cancellation_rate_pct,
    round(100.0 * s.returned_orders / nullif(s.orders, 0), 2)    as return_rate_pct,
    p.attempts                                                    as payment_attempts,
    p.successful                                                  as payment_success_count,
    p.failed                                                      as payment_failed_count,
    round(100.0 * p.successful / nullif(p.attempts, 0), 2)        as payment_success_rate_pct,
    sh.shipments,
    round(sh.avg_delivery_days, 2)                                as avg_delivery_days,
    sh.late_shipments,
    round(100.0 * sh.late_shipments / nullif(sh.shipments, 0), 2) as late_shipment_rate_pct
from status_mix s
left join payments p  using (year_month)
left join shipments sh using (year_month)
order by s.year_month
