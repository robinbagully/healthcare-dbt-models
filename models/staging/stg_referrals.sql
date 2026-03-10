{{
  config(materialized='view', tags=['staging','clinical'])
}}

/*
  stg_referrals.sql
  Grain: one row per referral_id (unique)
*/

with source as (
    select * from {{ source('raw', 'referrals') }}
),

cleaned as (
    select
        referral_id,
        patient_id,
        therapy_code,
        primary_diagnosis,
        referring_facility,
        upper(trim(referral_status))  as referral_status,

        cast(referral_date      as date) as referral_date,
        cast(received_date      as date) as received_date,
        cast(start_of_care_date as date) as start_of_care_date,

        -- Referral to start lag (days)
        case
            when start_of_care_date is not null
            then cast(start_of_care_date as date) - cast(referral_date as date)
        end as referral_to_start_days,

        -- Conversion flag
        referral_status = 'START_OF_CARE' as is_converted,

        cast(created_at as timestamp) as created_at

    from source
    where referral_id is not null
)

select * from cleaned