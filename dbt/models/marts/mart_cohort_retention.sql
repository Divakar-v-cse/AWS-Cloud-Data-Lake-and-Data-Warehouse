{{ config(materialized='table') }}

-- Monthly acquisition cohorts and retention by month index.
with shipped as (
    select
        customer_sk,
        date_trunc('month', order_date)::date as order_month
    from {{ ref('stg_orders') }}
    where status in ('completed', 'returned')
),

cohorts as (
    select
        customer_sk,
        min(order_month) as cohort_month
    from shipped
    group by 1
),

activity as (
    select
        c.cohort_month,
        s.order_month,
        s.customer_sk,
        ((extract(year from s.order_month) - extract(year from c.cohort_month)) * 12
          + (extract(month from s.order_month) - extract(month from c.cohort_month)))::int as month_index
    from shipped s
    join cohorts c using (customer_sk)
),

sizes as (
    select cohort_month, count(distinct customer_sk) as cohort_size
    from cohorts
    group by 1
)

select
    a.cohort_month,
    s.cohort_size,
    a.month_index,
    count(distinct a.customer_sk)                                   as customers,
    round(100.0 * count(distinct a.customer_sk) / nullif(s.cohort_size, 0), 2) as retention_pct
from activity a
join sizes s using (cohort_month)
group by 1, 2, 3
order by 1, 3
