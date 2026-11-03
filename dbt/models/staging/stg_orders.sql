{{ config(materialized='view') }}

select
    o.order_id,
    o.customer_sk,
    o.date_key,
    d.full_date              as order_date,
    o.order_ts,
    o.status,
    o.channel,
    o.region,
    o.state,
    o.city,
    o.item_count,
    o.subtotal,
    o.discount_amount,
    o.shipping_cost,
    o.tax_amount,
    o.order_total,
    o.is_completed,
    o.updated_at,
    d.year,
    d.month,
    d.year_month
from {{ source('dwh', 'fact_orders') }} o
join {{ source('dwh', 'dim_date') }} d using (date_key)
