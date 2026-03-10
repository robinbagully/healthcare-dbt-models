{{
  config(
    materialized='table',
    tags=['mart','clinical','ops']
  )
}}

/*
  mart_patient_census.sql
  Active patient census by therapy, payer, and month.
  Grain: order_id (one row per therapy episode)
  Consumers: Clinical ops, capacity planning, executive dashboard
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

patients as (
    select
        patient_id,
        full_name,
        date_of_birth,
        age_years,
        gender,
        state,
        primary_payer_id,
        is_medicare_age
    from {{ ref('stg_patients') }}
),

payers as (
    select payer_id, payer_name, payer_type
    from {{ source('raw', 'payers') }}
),

therapy as (
    select therapy_code, therapy_name, category as therapy_category, avg_duration_days
    from {{ source('raw', 'therapy_types') }}
),

joined as (
    select
        o.order_id,
        o.patient_id,
        p.full_name,
        p.age_years,
        p.gender,
        p.state,
        p.is_medicare_age,

        o.therapy_code,
        t.therapy_name,
        t.therapy_category,
        t.avg_duration_days         as expected_duration_days,
        o.episode_length_days       as actual_duration_days,
        o.drug_name,
        o.frequency,
        o.route,
        o.order_status,
        o.is_active,
        o.start_date,
        o.end_date,
        o.discharge_date,
        o.discharge_reason,

        py.payer_name,
        py.payer_type,

        date_trunc('month', o.start_date)::date as start_month

    from orders o
    left join patients p  on o.patient_id   = p.patient_id
    left join payers   py on p.primary_payer_id = py.payer_id
    left join therapy   t on o.therapy_code = t.therapy_code
)

select
    order_id,
    patient_id,
    full_name,
    age_years,
    gender,
    state,
    is_medicare_age,
    therapy_code,
    therapy_name,
    therapy_category,
    expected_duration_days,
    actual_duration_days,

    -- Variance to expected
    actual_duration_days - expected_duration_days as duration_variance_days,

    drug_name,
    frequency,
    route,
    order_status,
    is_active,
    start_date,
    end_date,
    discharge_date,
    discharge_reason,
    payer_name,
    payer_type,
    start_month,

    current_timestamp as dbt_updated_at

from joined
order by start_date desc
