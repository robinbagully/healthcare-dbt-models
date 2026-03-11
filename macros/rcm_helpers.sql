{% macro assert_ar_buckets_sum_to_total(model, bucket_col, total_col, tolerance=0.01) %}
/*
  assert_ar_buckets_sum_to_total
  ------------------------------
  Validates that the sum of all AR aging buckets equals the total AR balance.
  Allows for a small floating-point tolerance (default: $0.01).

  Usage in schema.yml:
    tests:
      - healthcare_rcm.assert_ar_buckets_sum_to_total:
          bucket_col: outstanding_balance
          total_col: payer_total_ar
*/

with bucketed as (
    select
        payer_id,
        sum({{ bucket_col }})    as sum_of_buckets,
        max({{ total_col }})     as reported_total,
        abs(
            sum({{ bucket_col }}) - max({{ total_col }})
        )                        as variance
    from {{ model }}
    group by payer_id
),

failures as (
    select *
    from bucketed
    where variance > {{ tolerance }}
)

select count(*) as failure_count
from failures

{% endmacro %}


{% macro clean_icd10(icd_code) %}
/*
  clean_icd10
  -----------
  Standardizes ICD-10 codes: uppercase, strip spaces, validate format.
  Returns null for invalid codes rather than propagating dirty data.

  Usage: {{ clean_icd10('primary_diagnosis_code') }}
*/
case
    when {{ icd_code }} is null then null
    when length(regexp_replace(upper(trim({{ icd_code }})), '[^A-Z0-9]', '')) < 3 then null
    else upper(trim({{ icd_code }}))
end
{% endmacro %}


{% macro get_ar_aging_bucket(billed_date_col, paid_date_col=None) %}
/*
  get_ar_aging_bucket
  -------------------
  Reusable macro to calculate AR aging bucket from billed date.
  Uses project vars for bucket thresholds — change in dbt_project.yml.

  Usage: {{ get_ar_aging_bucket('billed_date') }}
         {{ get_ar_aging_bucket('billed_date', 'paid_date') }}
*/
case
    when datediff('day', {{ billed_date_col }},
        {% if paid_date_col %}coalesce({{ paid_date_col }}, current_date)
        {% else %}current_date{% endif %}
    ) between 0 and {{ var('ar_bucket_1_max') }}
    then '0-{{ var("ar_bucket_1_max") }}'

    when datediff('day', {{ billed_date_col }},
        {% if paid_date_col %}coalesce({{ paid_date_col }}, current_date)
        {% else %}current_date{% endif %}
    ) between {{ var('ar_bucket_1_max') + 1 }} and {{ var('ar_bucket_2_max') }}
    then '{{ var("ar_bucket_1_max") + 1 }}-{{ var("ar_bucket_2_max") }}'

    when datediff('day', {{ billed_date_col }},
        {% if paid_date_col %}coalesce({{ paid_date_col }}, current_date)
        {% else %}current_date{% endif %}
    ) between {{ var('ar_bucket_2_max') + 1 }} and {{ var('ar_bucket_3_max') }}
    then '{{ var("ar_bucket_2_max") + 1 }}-{{ var("ar_bucket_3_max") }}'

    else '{{ var("ar_bucket_3_max") }}+'
end
{% endmacro %}
