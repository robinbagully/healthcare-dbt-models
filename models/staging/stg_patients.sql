{{
  config(materialized='view', tags=['staging','clinical'])
}}

/*
  stg_patients.sql
  Grain: one row per patient_id (unique)
*/

with source as (
    select * from {{ source('raw', 'patients') }}
),

cleaned as (
    select
        patient_id,
        first_name,
        last_name,
        first_name || ' ' || last_name  as full_name,
        cast(date_of_birth as date)     as date_of_birth,
        upper(trim(gender))             as gender,
        upper(trim(state))              as state,
        zip_code,
        primary_payer_id,
        secondary_payer_id,
        member_id,
        upper(trim(referral_source))    as referral_source,
        referring_facility,
        upper(trim(patient_status))     as patient_status,

        -- Derived
        date_part('year', age(cast(date_of_birth as date))) as age_years,

        case
            when date_part('year', age(cast(date_of_birth as date))) >= 65
            then true else false
        end as is_medicare_age,

        cast(created_at as timestamp) as created_at

    from source
    where patient_id is not null
)

select * from cleaned