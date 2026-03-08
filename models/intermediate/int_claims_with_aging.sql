{{
  config(
    materialized = 'ephemeral',
    tags = ['intermediate', 'rcm']
  )
}}

/*
  int_claims_with_aging.sql
  -------------------------
  Enriches cleaned claims with:
  - Days outstanding calculation
  - AR aging bucket assignment
  - Payer category standardization
  - Denial flag join

  Feeds: mart_ar_aging, mart_cash_collections
*/

with claims as (
    select * from {{ ref('stg_billing__claims') }}
    where outstanding_balance > {{ var('ar_min_balance') }}
),

denials as (
    select
        claim_id,
        count(*) as total_denials,
        max(denial_date) as most_recent_denial_date,
        bool_or(is_open) as has_open_denial
    from {{ ref('stg_billing__denials') }}
    group by 1
),

patients as (
    select
        patient_id,
        patient_name,
        date_of_birth,
        primary_insurance_id,
        secondary_insurance_id
    from {{ ref('stg_clinical__patients') }}
),

enriched as (
    select
        -- Claim keys
        c.claim_id,
        c.patient_id,
        c.payer_id,
        c.order_id,

        -- Patient info
        p.patient_name,
        p.date_of_birth,

        -- Dates
        c.service_date,
        c.billed_date,
        c.paid_date,

        -- Financials
        c.billed_amount,
        c.allowed_amount,
        c.paid_amount,
        c.adjustment_amount,
        c.outstanding_balance,

        -- Payer
        c.payer_name,
        c.payer_type,
        c.plan_name,

        -- Codes
        c.claim_status_code,
        c.primary_diagnosis_code,
        c.procedure_code,

        -- ── AR AGING ──────────────────────────────────────────────
        -- Days from billed date to today (or paid date if closed)
        datediff(
            'day',
            c.billed_date,
            coalesce(c.paid_date, current_date)
        ) as days_outstanding,

        -- Aging bucket using project vars for flexibility
        case
            when datediff('day', c.billed_date, coalesce(c.paid_date, current_date))
                between 0 and {{ var('ar_bucket_1_max') }}
            then '0-30'
            when datediff('day', c.billed_date, coalesce(c.paid_date, current_date))
                between {{ var('ar_bucket_1_max') + 1 }} and {{ var('ar_bucket_2_max') }}
            then '31-60'
            when datediff('day', c.billed_date, coalesce(c.paid_date, current_date))
                between {{ var('ar_bucket_2_max') + 1 }} and {{ var('ar_bucket_3_max') }}
            then '61-90'
            else '90+'
        end as ar_aging_bucket,

        -- Numeric bucket for sorting
        case
            when datediff('day', c.billed_date, coalesce(c.paid_date, current_date))
                between 0 and {{ var('ar_bucket_1_max') }}
            then 1
            when datediff('day', c.billed_date, coalesce(c.paid_date, current_date))
                between {{ var('ar_bucket_1_max') + 1 }} and {{ var('ar_bucket_2_max') }}
            then 2
            when datediff('day', c.billed_date, coalesce(c.paid_date, current_date))
                between {{ var('ar_bucket_2_max') + 1 }} and {{ var('ar_bucket_3_max') }}
            then 3
            else 4
        end as ar_aging_bucket_order,

        -- ── DENIAL FLAGS ──────────────────────────────────────────
        coalesce(d.total_denials, 0)       as total_denials,
        coalesce(d.has_open_denial, false) as has_open_denial,
        d.most_recent_denial_date,

        case when d.claim_id is not null then true else false end as has_been_denied,

        -- ── STATUS FLAGS ──────────────────────────────────────────
        case
            when c.claim_status_code = 'PAID' then true else false
        end as is_paid,

        case
            when c.outstanding_balance > 0
            and c.claim_status_code not in ('PAID', 'WRITTEN_OFF', 'VOID')
            then true else false
        end as is_open_ar,

        -- Metadata
        c._loaded_at

    from claims c
    left join denials  d on c.claim_id   = d.claim_id
    left join patients p on c.patient_id = p.patient_id
)

select * from enriched
