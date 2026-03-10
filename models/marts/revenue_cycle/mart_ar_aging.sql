{{
  config(
    materialized='table',
    tags=['mart','rcm','finance']
  )
}}

/*
  mart_ar_aging.sql
  AR aging summary by payer and bucket.
  Grain: payer_id + aging_bucket (unique)
  Consumers: Finance team, executive dashboard, weekly AR review
*/

with open_ar as (
    select * from {{ ref('int_claims_enriched') }}
    where is_open_ar = true
),

by_payer_bucket as (
    select
        payer_id,
        payer_name,
        payer_type,
        aging_bucket,
        aging_bucket_sort,

        count(distinct claim_id)                            as claim_count,
        count(distinct patient_id)                          as patient_count,
        sum(outstanding_balance)                            as outstanding_balance,
        avg(outstanding_balance)                            as avg_balance_per_claim,
        sum(billed_amount)                                  as total_billed,
        avg(days_outstanding)                               as avg_days_outstanding,

        -- Denial metrics
        sum(case when has_open_denial then 1 else 0 end)    as claims_with_open_denial,
        sum(case when has_open_denial
            then outstanding_balance else 0 end)            as balance_with_open_denial

    from open_ar
    group by 1,2,3,4,5
),

with_totals as (
    select
        *,

        -- Payer total AR (for % calc)
        sum(outstanding_balance) over (
            partition by payer_id
        )                                                   as payer_total_ar,

        -- Grand total AR
        sum(outstanding_balance) over ()                    as grand_total_ar,

        -- % of payer's AR in this bucket
        round(
            outstanding_balance
            / nullif(sum(outstanding_balance) over (
                partition by payer_id), 0) * 100, 1
        )                                                   as pct_of_payer_ar,

        -- % of total AR
        round(
            outstanding_balance
            / nullif(sum(outstanding_balance) over (), 0)
            * 100, 1
        )                                                   as pct_of_total_ar,

        -- Open denial rate
        round(
            claims_with_open_denial
            / nullif(claim_count, 0) * 100, 1
        )                                                   as open_denial_rate_pct

    from by_payer_bucket
)

select
    -- Surrogate key
    payer_id || '|' || aging_bucket                         as aging_id,

    payer_id,
    payer_name,
    payer_type,
    aging_bucket,
    aging_bucket_sort,
    claim_count,
    patient_count,
    round(outstanding_balance, 2)                           as outstanding_balance,
    round(avg_balance_per_claim, 2)                         as avg_balance_per_claim,
    round(total_billed, 2)                                  as total_billed,
    round(avg_days_outstanding, 0)                          as avg_days_outstanding,
    round(payer_total_ar, 2)                                as payer_total_ar,
    round(grand_total_ar, 2)                                as grand_total_ar,
    pct_of_payer_ar,
    pct_of_total_ar,
    claims_with_open_denial,
    round(balance_with_open_denial, 2)                      as balance_with_open_denial,
    open_denial_rate_pct,
    current_timestamp                                       as dbt_updated_at

from with_totals
order by payer_name, aging_bucket_sort
