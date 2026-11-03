{{ config(materialized='view') }}

select
    customer_sk,
    customer_id,
    first_name,
    last_name,
    trim(coalesce(first_name, '') || ' ' || coalesce(last_name, '')) as customer_name,
    email,
    city,
    state,
    region,
    country,
    loyalty_tier,
    signup_date,
    valid_from,
    valid_to,
    is_current
from {{ source('dwh', 'dim_customer') }}
where is_current
