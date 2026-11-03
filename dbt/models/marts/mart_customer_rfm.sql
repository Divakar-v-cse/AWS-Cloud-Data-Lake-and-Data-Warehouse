{{ config(materialized='table') }}

-- Customer 360: lifetime value, RFM-style scoring and segmentation.
with shipped as (
    select
        o.order_id,
        o.customer_sk,
        o.order_date,
        o.order_total
    from {{ ref('stg_orders') }} o
    where o.status in ('completed', 'returned')
),

refunds as (
    select
        order_id,
        sum(refund_amount) as refund_amount
    from {{ ref('stg_returns') }}
    where status in ('completed', 'approved')
    group by 1
),

lifetime as (
    select
        s.customer_sk,
        min(s.order_date)                                   as first_order_date,
        max(s.order_date)                                   as last_order_date,
        count(distinct s.order_id)                          as orders,
        sum(s.order_total)                                  as gross_monetary,
        coalesce(sum(r.refund_amount), 0)                   as refunds,
        sum(s.order_total) - coalesce(sum(r.refund_amount), 0) as monetary,
        sum(s.order_total) / count(distinct s.order_id)     as avg_order_value
    from shipped s
    left join refunds r on r.order_id = s.order_id
    group by 1
),

snapshot as (
    select max(order_date) as as_of_date from shipped
),

scored as (
    select
        l.*,
        c.customer_id,
        c.customer_name,
        c.email,
        c.city,
        c.state,
        c.region,
        c.loyalty_tier,
        c.signup_date,
        s.as_of_date,
        s.as_of_date - l.last_order_date as recency_days,
        (l.last_order_date - l.first_order_date) + 1 as tenure_days,
        case when l.orders > 1 then true else false end as is_repeat_customer,
        ntile(5) over (order by (s.as_of_date - l.last_order_date) desc) as r_score,
        ntile(5) over (order by l.orders asc)                    as f_score,
        ntile(5) over (order by l.monetary asc)                  as m_score
    from lifetime l
    join {{ ref('stg_customers') }} c on c.customer_sk = l.customer_sk
    cross join snapshot s
)

select
    customer_sk,
    customer_id,
    customer_name,
    email,
    city,
    state,
    region,
    loyalty_tier,
    signup_date,
    first_order_date,
    last_order_date,
    as_of_date,
    recency_days,
    tenure_days,
    orders,
    round(gross_monetary, 2)        as gross_monetary,
    round(refunds, 2)              as refunds,
    round(monetary, 2)             as monetary,
    round(avg_order_value, 2)      as avg_order_value,
    is_repeat_customer,
    r_score,
    f_score,
    m_score,
    (r_score * 100 + f_score * 10 + m_score) as rfm_score,
    round(avg_order_value * (orders::numeric / greatest(tenure_days, 1) * 365.0) * 2, 2) as predicted_clv,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 4 and f_score <= 2 then 'New Customers'
        when r_score >= 3 and f_score >= 3 then 'Loyal Customers'
        when r_score >= 3 and f_score <= 2 then 'Potential Loyalists'
        when r_score <= 2 and f_score >= 3 then 'At Risk'
        when r_score <= 2 and m_score >= 3 then 'Cant Lose Them'
        else 'Hibernating'
    end as segment
from scored
