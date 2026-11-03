{{ config(materialized='table') }}

select
    region,
    state,
    count(*)                                                     as orders,
    count(distinct customer_sk)                                  as customers,
    round(sum(order_total), 2)                                   as revenue,
    round(sum(order_total) / nullif(count(*), 0), 2)             as aov,
    round(100.0 * sum(order_total) / nullif(sum(sum(order_total)) over (), 0), 2) as revenue_share_pct,
    rank() over (order by sum(order_total) desc)                 as revenue_rank
from {{ ref('stg_orders') }}
where status in ('completed', 'returned')
group by 1, 2
order by revenue desc
