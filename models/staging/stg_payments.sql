{{
  config(materialized='view', tags=['staging','billing'])
}}

/*
  stg_payments.sql
  Grain: one row per payment_id (unique)
*/

with source as (
    select * from {{ source('raw', 'payments') }}
),

cleaned as (
    select
        payment_id,
        claim_id,
        patient_id,
        payer_id,
        upper(trim(payment_type))       as payment_type,
        upper(trim(adjustment_reason))  as adjustment_reason,

        cast(payment_date as date)  as payment_date,
        cast(posted_date  as date)  as posted_date,
        cast(check_date   as date)  as check_date,

        coalesce(paid_amount,       0) as paid_amount,
        coalesce(adjustment_amount, 0) as adjustment_amount,

        check_number,
        era_number,

        cast(created_at as timestamp) as created_at

    from source
    where payment_id is not null
)

select * from cleaned