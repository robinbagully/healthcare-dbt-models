# 🏥 Healthcare dbt Models

> A production-grade dbt project modeling Revenue Cycle Management (RCM) and clinical analytics for home infusion and specialty pharmacy data.

Built by [Robin Bagully](https://linkedin.com/in/robin-bagully) — Freelance Analytics Engineer specializing in healthcare data.

---

## What This Models

This project transforms raw healthcare operational data into a trusted analytics layer across three domains:

| Domain | Key Metrics |
|--------|-------------|
| **AR Aging** | Outstanding balances by bucket (0-30, 31-60, 61-90, 90+ days) |
| **Denial Management** | Denial rates by reason code, payer, and service line |
| **Cash Collections** | Daily/monthly cash posting, variance to expected |
| **Clinical Ops** | Patient census, referral-to-start, therapy days |

---

## Project Structure

```
healthcare-dbt-models/
├── models/
│   ├── staging/              # Raw source cleaning (1:1 with source tables)
│   │   ├── src_billing/      # Claims, charges, payments, adjustments
│   │   └── src_clinical/     # Patients, referrals, orders, therapies
│   ├── intermediate/         # Business logic, joins, calculations
│   └── marts/
│       ├── revenue_cycle/    # AR, denials, cash — for finance & ops
│       └── clinical/         # Census, referrals — for clinical teams
├── macros/                   # Reusable Jinja macros
├── tests/                    # Custom data tests
├── seeds/                    # Reference data (ICD codes, denial reasons)
├── analyses/                 # Ad-hoc SQL explorations
└── docs/                     # Model documentation
```

---

## Key Models

### `mart_ar_aging`
Tracks outstanding AR by payer and aging bucket. Primary input for weekly AR review and denial prioritization.

```sql
select
    payer_name,
    sum(case when days_outstanding between 0  and 30  then balance else 0 end) as bucket_0_30,
    sum(case when days_outstanding between 31 and 60  then balance else 0 end) as bucket_31_60,
    sum(case when days_outstanding between 61 and 90  then balance else 0 end) as bucket_61_90,
    sum(case when days_outstanding > 90              then balance else 0 end) as bucket_90_plus,
    sum(balance) as total_ar
from {{ ref('int_claims_with_aging') }}
group by 1
```

### `mart_denial_summary`
Aggregates denial activity by reason code, payer, and period. Feeds denial dashboards and appeal prioritization workflows.

### `mart_cash_collections`
Daily cash posting vs. expected collections. Variance flagging for revenue integrity teams.

---

## Stack

- **Transformation:** dbt Cloud (dbt Core compatible)
- **Warehouse:** Snowflake
- **Orchestration:** Airflow / dbt Cloud jobs
- **Source System:** CPR+ (home infusion EHR/billing)
- **BI Layer:** Looker Studio / Tableau

---

## Data Tests

Every mart model includes:
- `not_null` on all primary keys and critical metrics
- `unique` on grain columns
- `accepted_values` on status and category fields
- Custom `assert_ar_buckets_sum_to_total` macro test

---

## Running Locally

```bash
# Install dependencies
pip install dbt-snowflake

# Set up profile
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit with your Snowflake credentials

# Run full project
dbt deps
dbt seed
dbt run
dbt test

# Run specific layer
dbt run --select staging
dbt run --select marts.revenue_cycle
```

---

## Notes on Healthcare Data

All models in this repo use **synthetic or anonymized data**. No PHI/PII is present.
Source table schemas mirror real CPR+ structures — field names and grain are authentic,
values are generated. This is intentional: the modeling patterns are transferable,
the data is safe to share.

---

## About

I'm a freelance Analytics Engineer with 7+ years in healthcare data — dbt, Snowflake, RCM, and clinical ops.
Available for project work and retainer engagements.

📩 robin.bagully@icloud.com | [LinkedIn](https://linkedin.com/in/robin-bagully) | [Portfolio](https://robinbagully.com)
