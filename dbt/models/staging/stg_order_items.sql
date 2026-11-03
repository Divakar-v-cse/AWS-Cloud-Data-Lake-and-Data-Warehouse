{{ config(materialized='view') }}

select
    i.order_item_id,
    i.order_id,
    i.product_sk,
    i.customer_sk,
    i.date_key,
    d.full_date as order_date,
    d.year_month,
    i.quantity,
    i.unit_price,
    i.unit_cost,
    i.discount_pct,
    i.line_subtotal,
    i.line_total,
    i.line_profit
from {{ source('dwh', 'fact_order_items') }} i
join {{ source('dwh', 'dim_date') }} d using (date_key)
