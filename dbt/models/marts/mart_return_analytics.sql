{{ config(materialized='table') }}

-- Returns by category and reason, plus refund exposure.
with returns as (
    select
        coalesce(category, 'UNKNOWN') as category,
        reason,
        return_id,
        quantity,
        refund_amount,
        status
    from {{ ref('stg_returns') }}
),

sold as (
    select
        p.category,
        sum(i.quantity) as units_sold
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_products') }} p using (product_sk)
    join {{ ref('stg_orders') }} o using (order_id)
    where o.status in ('completed', 'returned')
    group by 1
),

agg as (
    select
        category,
        reason,
        count(*)                                             as returns,
        sum(quantity)                                        as units_returned,
        round(sum(refund_amount), 2)                         as refund_amount,
        count(*) filter (where status in ('completed', 'approved')) as approved_returns
    from returns
    group by 1, 2
)

select
    a.category,
    a.reason,
    a.returns,
    a.units_returned,
    a.approved_returns,
    a.refund_amount,
    s.units_sold,
    round(100.0 * a.units_returned / nullif(s.units_sold, 0), 2) as return_rate_pct
from agg a
left join sold s using (category)
order by a.refund_amount desc
