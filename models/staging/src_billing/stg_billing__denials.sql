{{
  config(
    materialized = 'view',
    tags = ['staging', 'billing']
  )
}}

/*
  stg_billing__denials.sql
  ------------------------
  Cleans raw denial records from the billing system.
  One row per denial event. A single claim may have multiple denials over time.

  Grain: denial_id (unique)
*/

with source as (
    select * from {{ source('billing', 'raw_denials') }}
),

cleaned as (
    select
        -- Keys
        cast(denial_id   as varchar) as denial_id,
        cast(claim_id    as varchar) as claim_id,
        cast(patient_id  as varchar) as patient_id,
        cast(payer_id    as varchar) as payer_id,

        -- Dates
        cast(denial_date  as date)   as denial_date,
        cast(appeal_date  as date)   as appeal_date,
        cast(resolved_date as date)  as resolved_date,

        -- Denial classification
        upper(trim(denial_reason_code))    as denial_reason_code,
        trim(denial_reason_description)    as denial_reason_description,
        upper(trim(denial_category))       as denial_category,
        -- Categories: CLINICAL, ADMINISTRATIVE, AUTHORIZATION, CODING, DUPLICATE

        -- Appeal tracking
        upper(trim(appeal_status))         as appeal_status,
        -- Statuses: NOT_APPEALED, PENDING, UPHELD, OVERTURNED
        cast(appeal_amount as numeric(12,2)) as appeal_amount,
        cast(recovered_amount as numeric(12,2)) as recovered_amount,

        -- Financials
        cast(denied_amount as numeric(12,2)) as denied_amount,

        -- Flags
        case
            when upper(trim(appeal_status)) = 'OVERTURNED' then true
            else false
        end as is_recovered,

        case
            when resolved_date is null
            and upper(trim(appeal_status)) not in ('UPHELD', 'OVERTURNED')
            then true
            else false
        end as is_open,

        -- Metadata
        cast(created_at as timestamp) as created_at,
        current_timestamp             as _loaded_at

    from source
    where denial_id is not null
)

select * from cleaned
