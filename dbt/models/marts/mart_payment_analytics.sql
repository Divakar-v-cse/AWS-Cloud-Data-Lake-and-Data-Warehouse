{{ config(materialized='table') }}

with attempts as (
    select
        payment_method,
        method_group,
        count(*)                                                     as attempts,
        count(*) filter (where status = 'success')                   as success_count,
        count(*) filter (where status = 'failed')                    as failed_count,
        count(*) filter (where status = 'refunded')                  as refunded_count,
        count(*) filter (where status = 'pending')                   as pending_count,
        round(sum(amount), 2)                                        as amount
    from {{ ref('stg_payments') }}
    group by 1, 2
)

select
    payment_method,
    method_group,
    attempts,
    success_count,
    failed_count,
    refunded_count,
    pending_count,
    amount,
    round(100.0 * success_count / nullif(attempts, 0), 2) as success_rate_pct,
    round(100.0 * failed_count / nullif(attempts, 0), 2)  as failure_rate_pct
from attempts
order by attempts desc
