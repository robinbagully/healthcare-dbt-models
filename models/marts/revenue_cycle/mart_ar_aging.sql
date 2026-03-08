{{
  config(
    materialized = 'table',
    tags = ['mart', 'rcm', 'finance'],
    post_hook = "grant select on {{ this }} to role REPORTER"
  )
}}

/*
  mart_ar_aging.sql
  -----------------
  Final AR aging mart. One row per payer per aging bucket.
  Designed for use in executive dashboards and weekly AR review meetings.

  Grain: payer_id + ar_aging_bucket (unique)
  Consumers: Finance team, Revenue Integrity, Executive dashboard
*/

with base as (
    select * from {{ ref('int_claims_with_aging') }}
    where is_open_ar = true
),

by_payer_bucket as (
    select
        payer_id,
        payer_name,
        payer_type,
        ar_aging_bucket,
        ar_aging_bucket_order,

        count(distinct claim_id)                        as claim_count,
        count(distinct patient_id)                      as patient_count,
        sum(outstanding_balance)                        as outstanding_balance,
        avg(outstanding_balance)                        as avg_balance_per_claim,
        sum(billed_amount)                              as total_billed,
        sum(case when has_open_denial then 1 else 0 end) as claims_with_open_denial,
        sum(case when has_open_denial then outstanding_balance else 0 end)
                                                        as balance_with_open_denial

    from base
    group by 1,2,3,4,5
),

with_totals as (
    select
        *,
        sum(outstanding_balance) over (
            partition by payer_id
        ) as payer_total_ar,

        sum(outstanding_balance) over () as grand_total_ar,

        -- % of this payer's AR in this bucket
        round(
            outstanding_balance / nullif(
                sum(outstanding_balance) over (partition by payer_id), 0
            ) * 100, 2
        ) as pct_of_payer_ar,

        -- % of total AR
        round(
            outstanding_balance / nullif(
                sum(outstanding_balance) over (), 0
            ) * 100, 2
        ) as pct_of_total_ar,

        -- Denial rate for this bucket/payer
        round(
            claims_with_open_denial / nullif(claim_count, 0) * 100, 2
        ) as open_denial_rate_pct

    from by_payer_bucket
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['payer_id', 'ar_aging_bucket']) }}
                                as aging_id,

        payer_id,
        payer_name,
        payer_type,
        ar_aging_bucket,
        ar_aging_bucket_order,

        claim_count,
        patient_count,

        -- Rounded financials
        round(outstanding_balance, 2)       as outstanding_balance,
        round(avg_balance_per_claim, 2)     as avg_balance_per_claim,
        round(total_billed, 2)              as total_billed,
        round(payer_total_ar, 2)            as payer_total_ar,
        round(grand_total_ar, 2)            as grand_total_ar,

        pct_of_payer_ar,
        pct_of_total_ar,

        claims_with_open_denial,
        round(balance_with_open_denial, 2)  as balance_with_open_denial,
        open_denial_rate_pct,

        -- Load metadata
        current_timestamp                   as dbt_updated_at

    from with_totals
)

select * from final
