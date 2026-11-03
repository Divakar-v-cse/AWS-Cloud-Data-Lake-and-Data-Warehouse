{{ config(materialized='view') }}

select
    s.shipment_id,
    s.order_id,
    s.date_key,
    d.full_date as shipped_date,
    s.shipped_ts,
    s.delivered_ts,
    s.promised_date,
    s.carrier,
    s.status,
    s.shipping_cost,
    s.delivery_days,
    s.is_late
from {{ source('dwh', 'fact_shipments') }} s
join {{ source('dwh', 'dim_date') }} d using (date_key)
