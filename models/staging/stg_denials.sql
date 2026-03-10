{{
  config(materialized='view', tags=['staging','billing'])
}}

/*
  stg_denials.sql
  Grain: one row per denial_id (unique)
*/

with source as (
    select * from {{ source('raw', 'denials') }}
),

cleaned as (
    select
        denial_id,
        claim_id,
        patient_id,
        payer_id,
        upper(trim(reason_code))    as reason_code,
        upper(trim(appeal_status))  as appeal_status,

        cast(denial_date    as date) as denial_date,
        cast(appeal_date    as date) as appeal_date,
        cast(resolved_date  as date) as resolved_date,

        coalesce(denied_amount,    0) as denied_amount,
        coalesce(recovered_amount, 0) as recovered_amount,

        coalesce(appeal_level, 1) as appeal_level,

        -- Derived flags
        appeal_status = 'OVERTURNED'                        as is_recovered,
        (resolved_date is null
            and appeal_status not in ('UPHELD','OVERTURNED')) as is_open,

        cast(created_at as timestamp) as created_at

    from source
    where denial_id is not null
)

select * from cleaned