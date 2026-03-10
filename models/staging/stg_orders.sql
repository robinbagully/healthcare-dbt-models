{{
  config(materialized='view', tags=['staging','clinical'])
}}

/*
  stg_orders.sql
  Grain: one row per order_id (unique)
*/

with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        patient_id,
        referral_id,
        therapy_code,
        primary_diagnosis,
        drug_name,
        upper(trim(frequency))      as frequency,
        upper(trim(route))          as route,
        upper(trim(order_status))   as order_status,
        discharge_reason,

        cast(order_date     as date) as order_date,
        cast(start_date     as date) as start_date,
        cast(end_date       as date) as end_date,
        cast(discharge_date as date) as discharge_date,

        -- Episode length in days
        case
            when end_date is not null and start_date is not null
            then cast(end_date as date) - cast(start_date as date)
        end as episode_length_days,

        -- Active flag
        order_status = 'ACTIVE' as is_active,

        cast(created_at as timestamp) as created_at

    from source
    where order_id is not null
)

select * from cleaned