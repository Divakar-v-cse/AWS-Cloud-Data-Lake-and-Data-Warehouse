{{ config(materialized='view') }}

select
    r.return_id,
    r.order_id,
    r.order_item_id,
    r.product_sk,
    r.date_key,
    d.full_date as return_date,
    r.return_ts,
    r.reason,
    r.quantity,
    r.refund_amount,
    r.status,
    r.restocked,
    p.category
from {{ source('dwh', 'fact_returns') }} r
join {{ source('dwh', 'dim_date') }} d using (date_key)
left join {{ source('dwh', 'dim_product') }} p
       on p.product_sk = r.product_sk and p.is_current
