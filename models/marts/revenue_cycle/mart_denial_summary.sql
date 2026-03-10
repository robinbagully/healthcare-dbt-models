{{
  config(
    materialized='table',
    tags=['mart','rcm','denials']
  )
}}

/*
  mart_denial_summary.sql
  Denial trending by reason code, payer, and month.
  Grain: payer_id + reason_code + denial_month (unique)
  Consumers: Revenue integrity, billing team, ops leadership
*/

with denials as (
    select * from {{ ref('stg_denials') }}
    where denial_date >= current_date - {{ var('denial_lookback_days') }}
),

payers as (
    select payer_id, payer_name, payer_type
    from {{ source('raw', 'payers') }}
),

reason_codes as (
    select reason_code, description, category
    from {{ source('raw', 'denial_reason_codes') }}
),

claims as (
    select claim_id, billed_amount, service_date
    from {{ ref('stg_claims') }}
),

joined as (
    select
        d.denial_id,
        d.claim_id,
        d.patient_id,
        d.payer_id,
        p.payer_name,
        p.payer_type,
        d.reason_code,
        r.description                           as reason_description,
        r.category                              as denial_category,
        d.denial_date,
        d.appeal_date,
        d.resolved_date,
        d.appeal_status,
        d.denied_amount,
        d.recovered_amount,
        d.is_recovered,
        d.is_open,
        c.billed_amount,

        -- Time dimensions
        date_trunc('month', d.denial_date)::date as denial_month,

        -- Days to resolve
        case
            when d.resolved_date is not null
            then d.resolved_date - d.denial_date
        end                                     as days_to_resolve

    from denials d
    left join payers       p on d.payer_id    = p.payer_id
    left join reason_codes r on d.reason_code = r.reason_code
    left join claims       c on d.claim_id    = c.claim_id
),

aggregated as (
    select
        denial_month,
        payer_id,
        payer_name,
        payer_type,
        reason_code,
        reason_description,
        denial_category,

        count(distinct denial_id)                           as denial_count,
        count(distinct claim_id)                            as affected_claims,
        count(distinct patient_id)                          as affected_patients,

        sum(denied_amount)                                  as total_denied,
        avg(denied_amount)                                  as avg_denied,
        sum(recovered_amount)                               as total_recovered,

        sum(case when is_recovered then 1 else 0 end)       as recovered_count,
        sum(case when is_open      then 1 else 0 end)       as open_count,

        round(
            sum(case when is_recovered then 1 else 0 end)
            / nullif(count(*), 0) * 100, 1
        )                                                   as recovery_rate_pct,

        round(
            sum(recovered_amount)
            / nullif(sum(denied_amount), 0) * 100, 1
        )                                                   as recovery_dollar_pct,

        round(avg(days_to_resolve), 0)                      as avg_days_to_resolve

    from joined
    group by 1,2,3,4,5,6,7
)

select
    payer_id || '|' || reason_code || '|'
        || denial_month::text                               as denial_summary_id,
    *,
    current_timestamp                                       as dbt_updated_at
from aggregated
order by denial_month desc, total_denied desc
