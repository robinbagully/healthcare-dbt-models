{{
  config(
    materialized = 'table',
    tags = ['mart', 'rcm', 'denials']
  )
}}

/*
  mart_denial_summary.sql
  -----------------------
  Denial trending and analysis mart.
  Powers denial management dashboards and appeal prioritization.

  Grain: denial_reason_code + payer_id + denial_month (unique)
  Consumers: Revenue Integrity, Billing team, Operations leadership
*/

with denials as (
    select * from {{ ref('stg_billing__denials') }}
    where denial_date >= dateadd('day', -{{ var('denial_lookback_days') }}, current_date)
),

claims as (
    select
        claim_id,
        payer_id,
        payer_name,
        payer_type,
        billed_amount,
        service_date
    from {{ ref('stg_billing__claims') }}
),

joined as (
    select
        d.denial_id,
        d.claim_id,
        d.patient_id,
        d.denial_reason_code,
        d.denial_reason_description,
        d.denial_category,
        d.denial_date,
        d.appeal_date,
        d.resolved_date,
        d.appeal_status,
        d.denied_amount,
        d.recovered_amount,
        d.is_recovered,
        d.is_open,

        -- From claims
        c.payer_id,
        c.payer_name,
        c.payer_type,
        c.billed_amount,
        c.service_date,

        -- Time dimensions
        date_trunc('month', d.denial_date)::date  as denial_month,
        date_trunc('week',  d.denial_date)::date  as denial_week,

        -- Days to resolve (if resolved)
        case
            when d.resolved_date is not null
            then datediff('day', d.denial_date, d.resolved_date)
        end as days_to_resolve

    from denials d
    left join claims c on d.claim_id = c.claim_id
),

aggregated as (
    select
        denial_month,
        payer_id,
        payer_name,
        payer_type,
        denial_reason_code,
        denial_reason_description,
        denial_category,

        count(distinct denial_id)                               as denial_count,
        count(distinct claim_id)                               as affected_claims,
        count(distinct patient_id)                             as affected_patients,

        -- Financials
        sum(denied_amount)                                      as total_denied_amount,
        avg(denied_amount)                                      as avg_denied_amount,
        sum(recovered_amount)                                   as total_recovered_amount,

        -- Recovery metrics
        sum(case when is_recovered then 1 else 0 end)          as recovered_count,
        sum(case when is_open      then 1 else 0 end)          as open_count,

        round(
            sum(case when is_recovered then 1 else 0 end)
            / nullif(count(distinct denial_id), 0) * 100, 2
        )                                                       as recovery_rate_pct,

        round(
            sum(recovered_amount)
            / nullif(sum(denied_amount), 0) * 100, 2
        )                                                       as recovery_dollar_rate_pct,

        avg(days_to_resolve)                                    as avg_days_to_resolve

    from joined
    group by 1,2,3,4,5,6,7
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'denial_month', 'payer_id', 'denial_reason_code'
        ]) }}                                   as denial_summary_id,

        *,

        -- Running total denied by payer (within lookback window)
        sum(total_denied_amount) over (
            partition by payer_id
            order by denial_month
            rows between unbounded preceding and current row
        ) as cumulative_denied_by_payer,

        current_timestamp                       as dbt_updated_at

    from aggregated
)

select * from final
