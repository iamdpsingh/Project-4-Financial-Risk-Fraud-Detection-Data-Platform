# Data Model — Financial Risk & Fraud Detection Platform

## 1. Entity-Relationship Overview

The platform models a financial company's core operational data. Six primary entities are stored in PostgreSQL (OLTP) and replicated to BigQuery (OLAP) with a dimensional model transformation.

```text
           ┌──────────────┐
           │   Customer   │
           └──────┬───────┘
                  │ 1
                  │ has many
                  │ ∞
           ┌──────┴───────┐        ┌──────────────┐
           │   Account    │        │   Merchant   │
           └──────┬───────┘        └──────┬───────┘
                  │ 1                     │ 1
                  │                       │
                  │ ∞             ┌───────┘
                  └───────────────┤
                           ┌──────┴──────────────┐
                           │     Transaction     │◄──── Device (many:1)
                           └──────┬──────────────┘
                                  │ 1
                                  │ has many
                                  │ ∞
                           ┌──────┴──────────────┐
                           │ Transaction Event   │
                           └─────────────────────┘
```

---

## 2. Star Schema (BigQuery Core Layer)

The BigQuery `core` dataset implements a **star schema** for optimal analytical query performance.

```text
                        dim_date
                            │
                            │
dim_customer ──────── fact_transaction ──────── dim_merchant
                            │
                            │
                       dim_account
                            │
                            │
                        dim_device
```

### Why Star Schema?

| Approach | Pros | Cons |
|---|---|---|
| Star Schema (chosen) | Fast analytical queries; simple joins; BI tool friendly | Some data duplication |
| Snowflake Schema | Less duplication | More joins; slower queries; harder to use in Looker |
| Wide/Flat Table | Fastest reads | Enormous; hard to maintain; update anomalies |

For a BI + fraud analytics workload, the star schema is the correct choice. Looker Studio and BigQuery are both optimized for it.

---

## 3. Dimension Tables

### dim_customer
Implements **SCD Type 2** (slowly changing dimension) to preserve history when customer attributes change (e.g., risk_level, customer_segment).

| Column | Type | Description |
|---|---|---|
| `customer_key` | INT64 | Surrogate key (auto-increment) |
| `customer_id` | STRING | Business key (UUID from PostgreSQL) |
| `name` | STRING | Full name |
| `email` | STRING | Email address |
| `phone` | STRING | Phone number |
| `country` | STRING | ISO 3166-1 alpha-2 country code |
| `city` | STRING | City name |
| `customer_segment` | STRING | retail / premium / business / vip / student |
| `risk_level` | STRING | low / medium / high (changes over time → SCD2) |
| `valid_from` | TIMESTAMP | When this record version became active |
| `valid_to` | TIMESTAMP | When this record version expired (NULL = current) |
| `is_current` | BOOL | True if this is the active version |
| `ingestion_timestamp` | TIMESTAMP | When this record was loaded |

**SCD Type 2 example:**
```
customer_key | customer_id | risk_level | valid_from  | valid_to    | is_current
100          | cust-abc    | LOW        | 2024-01-01  | 2024-03-10  | false
101          | cust-abc    | MEDIUM     | 2024-03-10  | 2024-04-22  | false
102          | cust-abc    | HIGH       | 2024-04-22  | NULL        | true
```

---

### dim_account

| Column | Type | Description |
|---|---|---|
| `account_key` | INT64 | Surrogate key |
| `account_id` | STRING | Business key |
| `customer_id` | STRING | FK to customer |
| `account_type` | STRING | checking / savings / credit / loan / investment |
| `account_status` | STRING | active / inactive / suspended / closed |
| `currency` | STRING | Default currency for this account |
| `opened_at` | TIMESTAMP | When account was opened |
| `closed_at` | TIMESTAMP | When account was closed (NULL = still open) |
| `is_active` | BOOL | Derived: account_status = 'active' |

---

### dim_merchant
Also implements **SCD Type 2** (merchant risk category can be upgraded by the compliance team).

| Column | Type | Description |
|---|---|---|
| `merchant_key` | INT64 | Surrogate key |
| `merchant_id` | STRING | Business key |
| `merchant_name` | STRING | Display name |
| `merchant_category` | STRING | Category description |
| `mcc_code` | STRING | ISO 18245 Merchant Category Code |
| `country` | STRING | Country of operation |
| `city` | STRING | City |
| `risk_category` | STRING | low / medium / high (SCD2) |
| `valid_from` | TIMESTAMP | |
| `valid_to` | TIMESTAMP | |
| `is_current` | BOOL | |

---

### dim_device

| Column | Type | Description |
|---|---|---|
| `device_key` | INT64 | Surrogate key |
| `device_id` | STRING | Business key |
| `customer_id` | STRING | FK to customer |
| `device_type` | STRING | mobile / desktop / tablet / atm / pos_terminal |
| `os` | STRING | Operating system |
| `browser` | STRING | Browser (if web) |
| `ip_address` | STRING | Last known IP address |
| `first_seen_at` | TIMESTAMP | First time this device was used |
| `last_seen_at` | TIMESTAMP | Most recent usage |

---

### dim_date
Pre-generated date dimension for efficient time-based analysis.

| Column | Type | Description |
|---|---|---|
| `date_key` | INT64 | YYYYMMDD integer |
| `date` | DATE | The date |
| `year` | INT64 | Calendar year |
| `quarter` | INT64 | 1–4 |
| `month` | INT64 | 1–12 |
| `month_name` | STRING | January … December |
| `week_of_year` | INT64 | ISO week number |
| `day_of_week` | INT64 | 1 = Monday, 7 = Sunday |
| `day_name` | STRING | Monday … Sunday |
| `is_weekend` | BOOL | Saturday or Sunday |
| `is_holiday` | BOOL | Public holiday flag (country-specific) |

---

## 4. Fact Tables

### fact_transaction

The central fact table. Grain: **one row per transaction**.

| Column | Type | Description |
|---|---|---|
| `transaction_key` | INT64 | Surrogate key |
| `transaction_id` | STRING | Business key (UUID) |
| `customer_key` | INT64 | FK → dim_customer |
| `account_key` | INT64 | FK → dim_account |
| `merchant_key` | INT64 | FK → dim_merchant |
| `device_key` | INT64 | FK → dim_device |
| `date_key` | INT64 | FK → dim_date |
| `transaction_timestamp` | TIMESTAMP | When the transaction occurred |
| `transaction_date` | DATE | Partition column |
| `amount` | NUMERIC | Transaction amount in original currency |
| `currency` | STRING | Original currency code |
| `amount_usd` | NUMERIC | Normalized amount in USD |
| `transaction_type` | STRING | purchase / withdrawal / transfer / etc. |
| `country` | STRING | Where the transaction occurred |
| `city` | STRING | |
| `payment_method` | STRING | card / bank_transfer / wallet / etc. |
| `status` | STRING | completed / failed / reversed / flagged |
| `is_international` | BOOL | Customer country ≠ transaction country |
| `risk_score` | FLOAT64 | 0–100; populated by risk pipeline |
| `risk_level` | STRING | low / medium / high (derived from risk_score) |
| `ingestion_timestamp` | TIMESTAMP | When loaded to BQ |
| `pipeline_run_id` | STRING | For lineage tracking |

**Partitioned by:** `transaction_date` (DATE)
**Clustered by:** `customer_key`, `merchant_key`

---

### fact_transaction_event

Grain: **one row per event per transaction**.

| Column | Type | Description |
|---|---|---|
| `event_key` | INT64 | Surrogate key |
| `event_id` | STRING | Business key (UUID) |
| `transaction_key` | INT64 | FK → fact_transaction |
| `transaction_id` | STRING | Business key for joins |
| `event_type` | STRING | transaction_created / payment_failed / etc. |
| `event_timestamp` | TIMESTAMP | Event occurrence time |
| `event_date` | DATE | Partition column |
| `event_metadata` | JSON | Flexible payload |
| `ingestion_timestamp` | TIMESTAMP | |

**Partitioned by:** `event_date`
**Clustered by:** `event_type`

---

## 5. Analytics Mart Tables

### analytics.transaction_risk
One row per transaction with enriched risk signals.

| Column | Description |
|---|---|
| `transaction_id` | |
| `risk_score` | 0–100 composite |
| `risk_level` | low / medium / high |
| `signal_high_amount` | BOOL — amount > 3× customer average |
| `signal_new_device` | BOOL — device first seen < 24h ago |
| `signal_geo_anomaly` | BOOL — unusual country for this customer |
| `signal_velocity` | BOOL — >5 txns in last 5 min |
| `signal_repeated_failure` | BOOL — >2 failures in last hour |
| `signal_high_risk_merchant` | BOOL — merchant.risk_category = 'high' |
| `signal_off_hours` | BOOL — transaction between 00:00–05:00 local time |

### analytics.customer_risk
Aggregated customer-level risk profile.

### analytics.merchant_risk
Aggregated merchant-level risk and transaction volume.

### analytics.daily_fraud_summary
One row per date with KPIs for executive dashboards.

### analytics.customer_transaction_behavior
Rolling 30-day behavioral baseline per customer (used for anomaly detection).

---

## 6. Data Flow: Source → BigQuery

```text
PostgreSQL                 BigQuery raw          BigQuery staging         BigQuery core
─────────────              ───────────────       ──────────────────       ─────────────────
customers           ──→    raw.customers  ──→    staging.customers ──→   core.dim_customer
accounts            ──→    raw.accounts   ──→    staging.accounts  ──→   core.dim_account
merchants           ──→    raw.merchants  ──→    staging.merchants ──→   core.dim_merchant
devices             ──→    raw.devices    ──→    staging.devices   ──→   core.dim_device
transactions        ──→    raw.txns       ──→    staging.txns      ──→   core.fact_transaction
transaction_events  ──→    raw.events     ──→    staging.events    ──→   core.fact_txn_event

                                                                          ↓
                                                                    analytics.*
```
