{{
  config(
    materialized='table',
    tags=['mart','clinical','ops']
  )
}}

/*
  mart_referral_funnel.sql
  Referral conversion and timing analytics.
  Grain: one row per referral_id
  Consumers: Clinical ops, intake team, leadership
*/

with referrals as (
    select * from {{ ref('stg_referrals') }}
),

patients as (
    select
        patient_id,
        primary_payer_id,
        referral_source,
        state
    from {{ ref('stg_patients') }}
),

payers as (
    select payer_id, payer_name, payer_type
    from {{ source('raw', 'payers') }}
),

therapy as (
    select therapy_code, therapy_name, category as therapy_category
    from {{ source('raw', 'therapy_types') }}
),

joined as (
    select
        r.referral_id,
        r.patient_id,
        r.therapy_code,
        t.therapy_name,
        t.therapy_category,
        r.primary_diagnosis,
        r.referral_status,
        r.is_converted,
        r.referral_date,
        r.received_date,
        r.start_of_care_date,
        r.referral_to_start_days,
        r.referring_facility,

        -- Patient / payer context
        p.referral_source,
        p.state,
        py.payer_name,
        py.payer_type,

        -- Month dimensions
        date_trunc('month', r.referral_date)::date as referral_month

    from referrals r
    left join patients p on r.patient_id    = p.patient_id
    left join payers  py on p.primary_payer_id = py.payer_id
    left join therapy  t on r.therapy_code  = t.therapy_code
)

select
    referral_id,
    patient_id,
    therapy_code,
    therapy_name,
    therapy_category,
    primary_diagnosis,
    referral_status,
    is_converted,
    referral_date,
    received_date,
    start_of_care_date,
    referral_to_start_days,
    referring_facility,
    referral_source,
    state,
    payer_name,
    payer_type,
    referral_month,

    -- Timing buckets
    case
        when referral_to_start_days <= 2  then '0-2 days'
        when referral_to_start_days <= 5  then '3-5 days'
        when referral_to_start_days <= 10 then '6-10 days'
        else '10+ days'
    end                                         as time_to_start_bucket,

    current_timestamp                           as dbt_updated_at

from joined
order by referral_date desc
