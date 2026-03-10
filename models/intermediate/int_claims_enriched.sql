{{
  config(materialized='ephemeral', tags=['intermediate','rcm'])
}}

/*
  int_claims_enriched.sql
  Joins claims with payer info, denial flags, and AR aging buckets.
  Feeds all revenue cycle mart models.
*/

with claims as (
    select * from {{ ref('stg_claims') }}
),

payers as (
    select
        payer_id,
        payer_name,
        payer_type
    from {{ source('raw', 'payers') }}
),

denial_flags as (
    select
        claim_id,
        count(*)                        as total_denials,
        sum(denied_amount)              as total_denied_amount,
        sum(recovered_amount)           as total_recovered_amount,
        max(denial_date)                as latest_denial_date,
        bool_or(is_open)                as has_open_denial,
        bool_or(is_recovered)           as has_recovery
    from {{ ref('stg_denials') }}
    group by 1
),

enriched as (
    select
        -- Keys
        c.claim_id,
        c.order_id,
        c.patient_id,
        c.payer_id,

        -- Payer
        p.payer_name,
        p.payer_type,

        -- Dates
        c.service_date,
        c.service_end_date,
        c.billed_date,

        -- Codes
        c.primary_diagnosis,
        c.procedure_code,
        c.claim_status,

        -- Financials
        c.billed_amount,
        c.allowed_amount,
        c.paid_amount,
        c.adjustment_amount,
        c.outstanding_balance,
        c.patient_responsibility,

        -- ── AR AGING ──────────────────────────────────────
        current_date - c.billed_date as days_outstanding,

        case
            when current_date - c.billed_date
                between 0 and {{ var('ar_bucket_1') }}
            then '0-30'
            when current_date - c.billed_date
                between {{ var('ar_bucket_1') + 1 }} and {{ var('ar_bucket_2') }}
            then '31-60'
            when current_date - c.billed_date
                between {{ var('ar_bucket_2') + 1 }} and {{ var('ar_bucket_3') }}
            then '61-90'
            else '90+'
        end as aging_bucket,

        case
            when current_date - c.billed_date
                between 0 and {{ var('ar_bucket_1') }}              then 1
            when current_date - c.billed_date
                between {{ var('ar_bucket_1') + 1 }}
                    and {{ var('ar_bucket_2') }}                     then 2
            when current_date - c.billed_date
                between {{ var('ar_bucket_2') + 1 }}
                    and {{ var('ar_bucket_3') }}                     then 3
            else 4
        end as aging_bucket_sort,

        -- ── DENIAL FLAGS ──────────────────────────────────
        coalesce(d.total_denials,        0)     as total_denials,
        coalesce(d.total_denied_amount,  0)     as total_denied_amount,
        coalesce(d.total_recovered_amount, 0)   as total_recovered_amount,
        coalesce(d.has_open_denial,      false) as has_open_denial,
        coalesce(d.has_recovery,         false) as has_recovery,
        d.latest_denial_date,
        d.claim_id is not null                  as has_been_denied,

        -- ── STATUS FLAGS ──────────────────────────────────
        c.claim_status = 'PAID'                 as is_paid,
        c.outstanding_balance > 0.01
            and c.claim_status not in
                ('PAID','WRITTEN_OFF','VOID')    as is_open_ar

    from claims c
    left join payers      p on c.payer_id  = p.payer_id
    left join denial_flags d on c.claim_id = d.claim_id
)

select * from enriched
