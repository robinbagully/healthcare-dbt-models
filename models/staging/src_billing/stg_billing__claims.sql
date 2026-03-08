{{
  config(
    materialized = 'view',
    tags = ['staging', 'billing']
  )
}}

/*
  stg_billing__claims.sql
  -----------------------
  Cleans and standardizes raw claims from the source billing system (CPR+).
  One row per claim. No business logic — just renaming, casting, and nulls.

  Source: {{ source('billing', 'raw_claims') }}
  Grain: claim_id (unique)
*/

with source as (
    select * from {{ source('billing', 'raw_claims') }}
),

cleaned as (
    select
        -- Keys
        cast(claim_id        as varchar)   as claim_id,
        cast(patient_id      as varchar)   as patient_id,
        cast(payer_id        as varchar)   as payer_id,
        cast(order_id        as varchar)   as order_id,

        -- Dates
        cast(service_date    as date)      as service_date,
        cast(billed_date     as date)      as billed_date,
        cast(paid_date       as date)      as paid_date,
        cast(posted_date     as date)      as posted_date,

        -- Financials
        cast(billed_amount   as numeric(12,2)) as billed_amount,
        cast(allowed_amount  as numeric(12,2)) as allowed_amount,
        cast(paid_amount     as numeric(12,2)) as paid_amount,
        cast(adjustment_amount as numeric(12,2)) as adjustment_amount,
        cast(patient_responsibility as numeric(12,2)) as patient_responsibility,

        -- Derived balance
        coalesce(cast(billed_amount as numeric(12,2)), 0)
            - coalesce(cast(paid_amount as numeric(12,2)), 0)
            - coalesce(cast(adjustment_amount as numeric(12,2)), 0)
            as outstanding_balance,

        -- Codes
        upper(trim(claim_status_code))     as claim_status_code,
        upper(trim(primary_icd10_code))    as primary_diagnosis_code,
        upper(trim(procedure_code))        as procedure_code,
        upper(trim(revenue_code))          as revenue_code,
        upper(trim(place_of_service_code)) as place_of_service_code,

        -- Payer info
        trim(payer_name)                   as payer_name,
        upper(trim(payer_type))            as payer_type,  -- COMMERCIAL, MEDICARE, MEDICAID, etc.
        trim(plan_name)                    as plan_name,

        -- Metadata
        cast(created_at as timestamp)      as created_at,
        cast(updated_at as timestamp)      as updated_at,
        current_timestamp                  as _loaded_at

    from source
    where claim_id is not null  -- exclude malformed rows
),

deduplicated as (
    -- Take the most recent record per claim_id
    select *,
        row_number() over (
            partition by claim_id
            order by updated_at desc
        ) as row_num
    from cleaned
)

select * exclude (row_num)
from deduplicated
where row_num = 1
