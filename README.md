# Financial Risk & Fraud Detection Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Beam-2.55+-orange?style=for-the-badge&logo=apache" />
  <img src="https://img.shields.io/badge/Google%20BigQuery-4285F4?style=for-the-badge&logo=google-cloud" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=for-the-badge&logo=apache-airflow" />
  <img src="https://img.shields.io/badge/Terraform-1.7+-7B42BC?style=for-the-badge&logo=terraform" />
  <img src="https://img.shields.io/badge/Docker-24+-2496ED?style=for-the-badge&logo=docker" />
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" />
</p>

> A production-style financial data engineering platform built on Google Cloud Platform. It ingests batch and real-time transaction data, runs a risk scoring engine, enforces data quality at every layer, and delivers analytics through a governed BigQuery data warehouse — orchestrated with Airflow, provisioned with Terraform, and monitored with Cloud Monitoring.

---

## What this project is

Most fraud detection projects you find online are Jupyter notebooks that do `if amount > 10000: fraud = True`. This is not that.

This is a **data engineering platform** where fraud detection is the business problem. The goal is to build the kind of data infrastructure that a real financial company would use — ingestion, transformation, quality checks, orchestration, monitoring, and analytics — all wired together properly.

It processes two types of data:

- **Historical data** — customers, accounts, merchants, and past transactions stored in PostgreSQL, extracted and loaded into BigQuery through a batch pipeline
- **Live data** — transaction events arriving in real time through Pub/Sub, processed by a streaming Dataflow pipeline and scored for fraud risk within seconds

---

## Architecture

```
                          DATA SOURCES
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
        PostgreSQL          REST APIs         Event stream
      (historical data)  (enrichment)    (live transactions)
            │                  │                  │
            └──────────────────┼──────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                 BATCH               STREAMING
                    │                     │
                    ↓                     ↓
             Cloud Storage            Pub/Sub
             (raw bucket)      (transaction-events)
                    │                     │
                    ↓                     ↓
                Dataflow              Dataflow
             (batch pipeline)   (streaming pipeline)
                    │                     │
                    └──────────┬──────────┘
                               ↓
                          BigQuery
                               │
            ┌──────────────────┼──────────────────┐
            ↓                  ↓                  ↓
           raw             staging             core
       (raw data)       (validated)     (dims + facts)
                               │
                               ↓
                          analytics
                      (risk + KPI marts)
                               │
                        Looker Studio

  Airflow / Cloud Composer — orchestrates all batch jobs
  Terraform               — provisions all GCP resources
  GitHub Actions          — CI/CD: lint → test → deploy
  Cloud Monitoring        — infrastructure + data health alerts
```

---

## Technology stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Batch + stream processing | Apache Beam 2.55+ |
| Batch runner (local) | DirectRunner |
| Batch runner (GCP) | Google Dataflow |
| Orchestration | Apache Airflow 2.8+ (Astronomer CLI locally) |
| Data warehouse | Google BigQuery |
| Object storage | Google Cloud Storage |
| Messaging | Google Cloud Pub/Sub (emulator locally) |
| Serving API | FastAPI + Cloud Run |
| Infrastructure as Code | Terraform 1.7+ |
| Containers | Docker + Artifact Registry |
| CI/CD | GitHub Actions |
| Data quality | Great Expectations 0.18+ |
| Synthetic data | Faker + NumPy |
| Local database | PostgreSQL 16 (via Docker Compose) |
| ML fraud model | XGBoost → Vertex AI |
| Dashboard | Looker Studio |
| Monitoring | Cloud Monitoring + Cloud Logging |
| Secrets | Cloud Secret Manager |

---

## Repository structure and what each file does

```
.
├── README.md                         this file
├── LICENSE                           MIT license
├── .gitignore                        ignores venvs, secrets, generated data, terraform state
├── .env.example                      template for all environment variables — copy to .env
├── pyproject.toml                    Python dependencies for the whole project + tool config
├── docker-compose.yml                starts PostgreSQL 16 and the Pub/Sub emulator locally
│
├── docs/
│   ├── architecture.md               explains the full system design and why decisions were made
│   ├── data-model.md                 star schema design, SCD Type 2, all BigQuery table shapes
│   ├── data-dictionary.md            every field in every table: type, nullable, description, example
│   ├── deployment.md                 step-by-step: local setup and full GCP deployment
│   ├── monitoring.md                 what we monitor, alert thresholds, SLA definitions
│   └── cost-optimization.md          BigQuery partitioning experiments and cost reduction strategy
│
├── data/
│   ├── schemas/                      JSON Schema files that define the shape of each entity
│   ├── sample/                       small committed CSV samples (good for a quick look)
│   ├── raw/                          where generated data lands locally (gitignored — too large)
│   └── generators/
│       ├── config.py                 shared settings used by all generators (seeds, counts, etc.)
│       ├── generate_customers.py     creates realistic customer records with country distribution
│       ├── generate_accounts.py      creates accounts linked to customers
│       ├── generate_merchants.py     creates merchants across categories, some flagged high-risk
│       ├── generate_devices.py       creates devices per customer, needed for new-device fraud signal
│       ├── generate_transactions.py  the main generator — embeds realistic fraud patterns
│       ├── generate_events.py        creates lifecycle events for each transaction
│       ├── generate_streaming.py     continuously emits transaction events to Pub/Sub (or stdout)
│       └── generate_all.py           runs all generators in order and loads data into PostgreSQL
│
├── ingestion/
│   ├── batch/
│   │   ├── extract.py                reads from PostgreSQL using watermarks (incremental loads)
│   │   └── upload.py                 uploads extracted CSVs to GCS
│   └── api/
│       └── main.py                   FastAPI app deployed to Cloud Run — accepts transactions, publishes to Pub/Sub
│
├── pipelines/
│   ├── batch/
│   │   ├── pipeline.py               Apache Beam batch pipeline: read GCS → validate → transform → write BQ
│   │   ├── transforms.py             all Beam DoFn classes used by the batch pipeline
│   │   └── run_local.py              runs the batch pipeline locally with DirectRunner
│   └── streaming/
│       ├── pipeline.py               Apache Beam streaming pipeline: Pub/Sub → enrich → score → BQ
│       ├── transforms.py             Beam DoFns for parsing, enrichment, windowing, risk scoring
│       └── run_local.py              runs the streaming pipeline locally with DirectRunner
│
├── transformations/
│   ├── dimensions/
│   │   ├── dim_customer.sql          SQL that builds dim_customer with SCD Type 2 logic
│   │   ├── dim_account.sql
│   │   ├── dim_merchant.sql          also SCD Type 2
│   │   ├── dim_device.sql
│   │   └── dim_date.sql              pre-generates 10 years of date dimension rows
│   ├── facts/
│   │   ├── fact_transaction.sql      joins staging transactions to dimension surrogate keys
│   │   └── fact_transaction_event.sql
│   └── marts/
│       ├── transaction_risk.sql      one row per transaction with all 7 risk signals unpacked
│       ├── customer_risk.sql         aggregated risk profile per customer
│       ├── merchant_risk.sql         transaction volume and risk by merchant
│       ├── daily_fraud_summary.sql   one row per day — KPIs for the executive dashboard
│       └── customer_behavior.sql     30-day rolling baseline per customer for anomaly detection
│
├── data_quality/
│   ├── expectations/                 Great Expectations suite definitions for each layer
│   ├── checkpoints/                  GE checkpoint configs (what to run, where to save results)
│   └── run_checks.py                 entry point — runs the right suite for a given layer
│
├── fraud/
│   ├── rules/
│   │   └── rule_engine.py            evaluates each of the 7 fraud signals for a transaction
│   └── risk_scoring/
│       └── scorer.py                 combines signals into a 0–100 score with configurable weights
│
├── airflow/
│   └── dags/
│       ├── batch_ingestion_dag.py    daily DAG: extract → upload GCS → run Dataflow → BQ
│       ├── dimension_refresh_dag.py  runs SCD Type 2 merges for customer and merchant dims
│       ├── data_quality_dag.py       runs Great Expectations after each load
│       └── analytics_mart_dag.py     refreshes all analytics mart tables
│
├── infrastructure/
│   ├── postgres/
│   │   └── init.sql                  creates all tables, enums, indexes, and triggers in PostgreSQL
│   └── terraform/
│       ├── providers.tf              GCP provider config
│       ├── variables.tf              all input variables (project ID, region, etc.)
│       ├── outputs.tf                outputs useful values after apply (bucket name, BQ datasets, etc.)
│       ├── project.tf                creates the GCP project and enables APIs
│       ├── storage.tf                GCS bucket with folder structure and lifecycle policies
│       ├── bigquery.tf               BigQuery datasets and table schemas
│       ├── pubsub.tf                 Pub/Sub topics and subscriptions including dead-letter
│       ├── iam.tf                    all IAM bindings — each service account gets only what it needs
│       ├── service_accounts.tf       one service account per component
│       ├── cloud_run.tf              Cloud Run service definition
│       ├── artifact_registry.tf      Docker image repository
│       └── monitoring.tf             dashboards, alert policies, log-based metrics
│
├── cloud_run/
│   └── main.py                       FastAPI transaction ingestion service (deployed to Cloud Run)
│
├── docker/
│   └── ingestion-api/
│       └── Dockerfile                container image for the Cloud Run ingestion API
│
├── tests/
│   ├── conftest.py                   shared pytest fixtures used across all test files
│   ├── unit/                         tests for individual Python functions (no GCP needed)
│   ├── integration/                  tests that talk to Pub/Sub, BigQuery, etc.
│   └── data_quality/                 data-level tests: uniqueness, nulls, referential integrity
│
├── monitoring/
│   └── alert_policies/               Cloud Monitoring alert policy configs as JSON
│
└── .github/
    └── workflows/
        └── ci-cd.yml                 GitHub Actions: lint → type check → tests → docker → deploy
```

---

## Risk scoring engine

Each transaction is evaluated against seven signals. Each signal is independent and carries a weight. The final score is 0–100.

```
Transaction
     │
     ├── High amount          weight 25   amount > 3× customer's 30-day average
     ├── New device           weight 20   device was first seen less than 24 hours ago
     ├── Geo anomaly          weight 20   transaction country not in customer's usual countries
     ├── Velocity burst       weight 15   more than 5 transactions in the last 5 minutes
     ├── Repeated failures    weight 10   more than 2 payment failures in the last hour
     ├── High-risk merchant   weight  7   merchant is flagged as high-risk by compliance
     └── Off-hours            weight  3   transaction happened between midnight and 5am local time
                │
                ↓
         risk_score (0–100)

  0–30   LOW     no action
  31–70  MEDIUM  monitoring, soft alert
  71–100 HIGH    flagged for review
```

---

## Data model

The BigQuery `core` dataset uses a star schema:

```
                  dim_date
                      │
dim_customer ─── fact_transaction ─── dim_merchant
                      │
                 dim_account
                      │
                  dim_device
```

`dim_customer` and `dim_merchant` use **SCD Type 2** — when a customer's risk level changes, we don't overwrite the record. We close the old version and insert a new one. This preserves history so you can always ask "what was this customer's risk level at the time of this transaction?"

---

## Data quality

Every layer has automated checks:

| Layer | What gets checked |
|---|---|
| `raw` | Schema, file completeness |
| `staging` | Null checks, uniqueness, type validation, referential integrity |
| `core` | FK resolution against dims, amount ranges, timestamp sanity |
| `analytics` | Row count thresholds, freshness, score range (0–100) |

Bad records go to `staging.data_quality_errors` with the full payload, so nothing silently disappears.

---

## Quick start (local)

```bash
# 1. Clone the repo
git clone https://github.com/iamdpsingh/Project-4-Financial-Risk-Fraud-Detection-Data-Platform.git
cd Project-4-Financial-Risk-Fraud-Detection-Data-Platform

# 2. Create a Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 3. Set up your environment
cp .env.example .env
# open .env and set POSTGRES_PASSWORD and anything else you want to change

# 4. Start local services (PostgreSQL + Pub/Sub emulator)
docker compose up -d

# 5. Generate synthetic data and load it into PostgreSQL
python data/generators/generate_all.py

# 6. Run the batch pipeline locally
python pipelines/batch/run_local.py

# 7. Run data quality checks
python data_quality/run_checks.py

# 8. Run the test suite
pytest tests/ -v
```

Adminer (database UI) runs at http://localhost:8080 — use it to browse the PostgreSQL tables.

For the streaming simulation, open two more terminals:
```bash
# Terminal 2 — start the streaming pipeline
python pipelines/streaming/run_local.py

# Terminal 3 — emit transaction events at 10/second for 5 minutes
python data/generators/generate_streaming.py --rate 10 --duration 300
```

---

## GCP deployment

```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars
# fill in your billing account ID and org ID

terraform init
terraform apply
```

This creates the GCP project, enables all APIs, sets up service accounts with least-privilege IAM, creates the GCS bucket, BigQuery datasets, Pub/Sub topics, Artifact Registry, and monitoring dashboards. See `docs/deployment.md` for the full walkthrough.

---

## CI/CD

Every push to any branch runs: ruff lint → mypy type check → pytest unit tests → terraform validate → Docker build.

Merges to `main` additionally push the Docker image to Artifact Registry and deploy to Cloud Run using Workload Identity Federation (no service account keys stored in GitHub).

---

## Documentation

| Document | What it covers |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Full system design, component decisions, security model |
| [`docs/data-model.md`](docs/data-model.md) | Star schema, SCD Type 2, all table column definitions |
| [`docs/data-dictionary.md`](docs/data-dictionary.md) | Every field: type, nullable, description, example value |
| [`docs/deployment.md`](docs/deployment.md) | Local setup and GCP deployment, step by step |
| [`docs/monitoring.md`](docs/monitoring.md) | Alert thresholds, SLA definitions, runbooks |
| [`docs/cost-optimization.md`](docs/cost-optimization.md) | BigQuery optimization experiments with before/after numbers |

---

*Built by Dhruv Pratap Singh*
