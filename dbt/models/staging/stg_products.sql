{{ config(materialized='view') }}

select
    product_sk,
    product_id,
    product_name,
    sku,
    category,
    subcategory,
    brand,
    unit_cost,
    unit_price,
    is_active,
    launch_date
from {{ source('dwh', 'dim_product') }}
where is_current
