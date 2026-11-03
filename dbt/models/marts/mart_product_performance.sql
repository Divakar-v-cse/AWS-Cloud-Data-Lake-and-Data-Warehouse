{{ config(materialized='table') }}

with sales as (
    select
        i.product_sk,
        sum(i.quantity)         as units_sold,
        count(distinct i.order_id) as orders,
        sum(i.line_total)       as revenue,
        sum(i.line_profit)      as profit,
        avg(i.unit_price)       as avg_selling_price
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o using (order_id)
    where o.status in ('completed', 'returned')
    group by 1
),
returns as (
    select product_sk,
           sum(quantity)      as returned_units,
           sum(refund_amount) as refund_amount
    from {{ ref('stg_returns') }}
    where status in ('completed', 'approved')
    group by 1
)
select
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.unit_price,
    p.unit_cost,
    coalesce(s.units_sold, 0)                                   as units_sold,
    coalesce(s.orders, 0)                                       as orders,
    round(coalesce(s.revenue, 0), 2)                            as revenue,
    round(coalesce(s.profit, 0), 2)                             as profit,
    round(100.0 * coalesce(s.profit, 0) / nullif(s.revenue, 0), 2) as margin_pct,
    coalesce(r.returned_units, 0)                               as returned_units,
    round(100.0 * coalesce(r.returned_units, 0) / nullif(s.units_sold, 0), 2) as return_rate_pct,
    round(coalesce(r.refund_amount, 0), 2)                      as refund_amount,
    rank() over (order by coalesce(s.revenue, 0) desc)          as revenue_rank
from {{ ref('stg_products') }} p
left join sales s   on s.product_sk = p.product_sk
left join returns r on r.product_sk = p.product_sk
order by revenue desc
