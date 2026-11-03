{{ config(materialized='table') }}

with refunds as (
    select order_id, sum(refund_amount) as refund_amount
    from {{ ref('stg_returns') }}
    where status in ('completed', 'approved')
    group by 1
),
monthly as (
    select
        o.year_month,
        min(o.order_date)                       as month_start,
        count(*)                                as orders,
        count(distinct o.customer_sk)           as customers,
        sum(o.item_count)                       as units_sold,
        sum(o.order_total)                      as gross_revenue,
        sum(coalesce(r.refund_amount, 0))       as refunds
    from {{ ref('stg_orders') }} o
    left join refunds r using (order_id)
    where o.status in ('completed', 'returned')
    group by 1
),
final as (
    select
        year_month,
        month_start,
        orders,
        customers,
        units_sold,
        round(gross_revenue, 2)                       as gross_revenue,
        round(refunds, 2)                             as refunds,
        round(gross_revenue - refunds, 2)             as net_revenue,
        round((gross_revenue - refunds) / nullif(orders, 0), 2) as aov
    from monthly
)
select
    f.*,
    round(100.0 * (net_revenue - lag(net_revenue) over (order by year_month))
          / nullif(lag(net_revenue) over (order by year_month), 0), 2) as revenue_growth_pct,
    round(sum(net_revenue) over (order by year_month), 2)               as cumulative_revenue
from final f
order by year_month
