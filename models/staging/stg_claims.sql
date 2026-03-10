{{
  config(materialized='view', tags=['staging','billing'])
}}

/*
  stg_claims.sql
  Clean and standardize raw claims.
  Grain: one row per claim_id (unique)
*/

with source as (
    select * from {{ source('raw', 'claims') }}
),

cleaned as (
    select
        claim_id,
        order_id,
        patient_id,
        payer_id,
        secondary_payer_id,

        -- Dates
        cast(service_date     as date) as service_date,
        cast(service_end_date as date) as service_end_date,
        cast(billed_date      as date) as billed_date,

        -- Codes
        upper(trim(primary_diagnosis)) as primary_diagnosis,
        upper(trim(procedure_code))    as procedure_code,
        upper(trim(revenue_code))      as revenue_code,
        upper(trim(claim_status))      as claim_status,

        -- Financials
        coalesce(billed_amount,      0) as billed_amount,
        coalesce(allowed_amount,     0) as allowed_amount,
        coalesce(paid_amount,        0) as paid_amount,
        coalesce(adjustment_amount,  0) as adjustment_amount,
        coalesce(patient_responsibility, 0) as patient_responsibility,

        -- Derived
        coalesce(billed_amount, 0)
            - coalesce(paid_amount, 0)
            - coalesce(adjustment_amount, 0) as outstanding_balance,

        coalesce(denial_count, 0) as denial_count,

        cast(created_at as timestamp) as created_at

    from source
    where claim_id is not null
)

select * from cleaned