{{ config(materialized='view') }}

select
    p.payment_id,
    p.order_id,
    p.date_key,
    d.full_date as payment_date,
    p.payment_ts,
    p.payment_method,
    p.amount,
    p.status,
    p.attempt_number,
    m.method_group
from {{ source('dwh', 'fact_payments') }} p
join {{ source('dwh', 'dim_date') }} d using (date_key)
left join {{ source('dwh', 'dim_payment_method') }} m using (method_key)
