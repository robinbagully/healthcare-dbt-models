{{
  config(
    materialized='table',
    tags=['mart','rcm','finance']
  )
}}

/*
  mart_cash_collections.sql
  Daily cash posting with trends and period comparisons.
  Grain: posted_date + payer_type (unique)
  Consumers: Finance, daily cash report, executive dashboard
*/

with payments as (
    select * from {{ ref('stg_payments') }}
    where payment_type = 'INSURANCE'
      and posted_date is not null
),

payers as (
    select payer_id, payer_name, payer_type
    from {{ source('raw', 'payers') }}
),

daily as (
    select
        p.posted_date,
        py.payer_type,

        sum(p.paid_amount)                      as cash_posted,
        sum(p.adjustment_amount)                as total_adjustments,
        count(distinct p.claim_id)              as claims_paid,
        count(distinct p.patient_id)            as patients_paid,
        count(distinct p.payer_id)              as payers_posting

    from payments p
    left join payers py on p.payer_id = py.payer_id
    group by 1, 2
),

with_trends as (
    select
        *,

        -- 7-day moving average
        round(avg(cash_posted) over (
            partition by payer_type
            order by posted_date
            rows between 6 preceding and current row
        ), 2)                                   as cash_7day_avg,

        -- Month-to-date cumulative
        sum(cash_posted) over (
            partition by
                date_trunc('month', posted_date),
                payer_type
            order by posted_date
            rows between unbounded preceding and current row
        )                                       as mtd_cash,

        -- Prior week same day
        lag(cash_posted, 7) over (
            partition by payer_type
            order by posted_date
        )                                       as cash_prior_week,

        -- WoW % change
        round(
            (cash_posted
                - lag(cash_posted, 7) over (
                    partition by payer_type order by posted_date))
            / nullif(lag(cash_posted, 7) over (
                partition by payer_type order by posted_date), 0)
            * 100, 1
        )                                       as wow_change_pct

    from daily
)

select
    posted_date || '|' || payer_type            as cash_id,
    posted_date,
    payer_type,
    round(cash_posted, 2)                       as cash_posted,
    round(total_adjustments, 2)                 as total_adjustments,
    claims_paid,
    patients_paid,
    cash_7day_avg,
    round(mtd_cash, 2)                          as mtd_cash,
    round(cash_prior_week, 2)                   as cash_prior_week,
    wow_change_pct,
    current_timestamp                           as dbt_updated_at

from with_trends
order by posted_date desc, payer_type
