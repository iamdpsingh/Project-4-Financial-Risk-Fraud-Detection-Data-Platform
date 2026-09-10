# 🚀 Financial Risk & Fraud Detection Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Beam-2.55+-orange?style=for-the-badge&logo=apache" />
  <img src="https://img.shields.io/badge/Google%20BigQuery-4285F4?style=for-the-badge&logo=google-cloud" />
  <img src="https://img.shields.io/badge/Google%20Pub/Sub-4285F4?style=for-the-badge&logo=google-cloud" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.10+-017CEE?style=for-the-badge&logo=apache-airflow" />
  <img src="https://img.shields.io/badge/Terraform-1.7+-7B42BC?style=for-the-badge&logo=terraform" />
  <img src="https://img.shields.io/badge/dbt-1.7+-FF694B?style=for-the-badge&logo=dbt" />
  <img src="https://img.shields.io/badge/Docker-24+-2496ED?style=for-the-badge&logo=docker" />
</p>

A production-grade, end-to-end financial data engineering platform designed to process massive datasets at scale. It generates **2.5 million (25 Lakhs) synthetic banking transactions** and 7.5 million lifecycle events, ingesting them in both **batch** (GCS → Dataflow → BigQuery) and **streaming** (Cloud Run → Pub/Sub → Dataflow) modes. 

Every single transaction is scored for fraud risk using a custom **rule engine**, modeled with **dbt**, and surfaced on a beautiful **Streamlit** dashboard. Everything is orchestrated with **Airflow**, provisioned natively on Google Cloud with **Terraform**, and validated by a GitHub Actions CI/CD pipeline.

---

## 📑 Table of Contents

- [1. System Architecture](#-system-architecture)
- [2. Repository Layout](#-repository-layout)
- [3. Documentation Index (docs/)](#-documentation-index-docs)
- [4. Execution Guide](#-execution-guide)
  - [Option 1: Local Development](#-option-1-local-development-directrunner)
  - [Option 2: Production on GCP](#-option-2-production-on-google-cloud-platform)
- [5. Running Tests, Lint & Type Checks](#-running-tests-lint--type-checks)
- [6. CI/CD](#-cicd)
- [7. Project Status](#-project-status--whats-scaffolded-vs-implemented)

---

## 🏗️ System Architecture

```mermaid
graph TD
    %% Data Sources
    subgraph Sources [Data Sources]
        PG[(PostgreSQL<br>Historical Data)]
        Stream[Live Transaction Stream]
    end

    %% Ingestion Layer
    subgraph Ingestion [Ingestion Layer]
        Extract[Python Extractor]
        API[Cloud Run Ingestion API]
        PS[Google Pub/Sub]
        Extract -->|Upload| GCS[Cloud Storage]
        Stream -->|POST| API
        API -->|Publish| PS
    end
    PG --> Extract

    %% Processing Layer
    subgraph Processing [Dataflow / Apache Beam]
        BatchPipe[Batch Pipeline<br>Validation & Parsing]
        StreamPipe[Streaming Pipeline<br>Risk Engine & Enrichment]
    end
    GCS --> BatchPipe
    PS --> StreamPipe
    StreamPipe -.->|Bad messages| DLQ[(transactions_dlq)]

    %% Storage & Modeling Layer
    subgraph Storage [BigQuery & dbt]
        Staging[(Staging Dataset)]
        Analytics[(Analytics Dataset<br>transaction_risk)]
        DBT[dbt: staging views → dims/facts → snapshots]
        Staging --> DBT
        Analytics --> DBT
        DBT --> Marts[(dim_customer, dim_merchant,<br>fct_transactions)]
    end
    BatchPipe --> Staging
    StreamPipe --> Analytics

    %% Serving
    Marts --> Dash[Streamlit Dashboard]
    Analytics --> Dash

    %% Orchestration
    Airflow((Apache Airflow)) -.->|Schedules| Extract
    Airflow -.->|Schedules| BatchPipe
    Airflow -.->|Monitors| StreamPipe
```

Read left to right: PostgreSQL feeds the **batch** path, a live event generator (or a real app)
feeds the **streaming** path through a Cloud Run ingestion API. Both paths land in BigQuery, dbt
turns the raw tables into a small dimensional model, and a Streamlit dashboard reads the result.
Airflow schedules/monitors the pipelines; Terraform provisions every GCP resource involved.

---

## 📂 Repository Layout

```text
.
├── .env.example                          # Template for every environment variable the project reads — copy to .env and fill in
├── .github/workflows/ci-cd.yml           # GitHub Actions: lint → test → terraform validate → docker build → deploy to Cloud Run
├── docker-compose.yml                    # Local PostgreSQL 16 + Pub/Sub emulator + Adminer (DB UI)
├── requirements.txt                      # Quick minimal dependency set (Beam, GCS, Pub/Sub, psycopg2, pandas, Faker, Airflow)
├── pyproject.toml                        # Full project definition — complete dependency set (incl. FastAPI, Great Expectations, Streamlit/Plotly, dev tooling) + ruff/mypy/pytest config
├── setup.py                               # Packaging file Apache Beam/Dataflow uses to ship this repo's code to worker VMs (--setup_file)
│
├── cloud_run/                            # FastAPI ingestion API — the entry point for the streaming path
│   ├── main.py                           # Defines the app: GET /health, POST /ingest/transaction (validates a transaction, publishes it to the Pub/Sub `transaction-events` topic, returns 202 Accepted)
│   ├── models.py                         # Pydantic `TransactionEvent` model used to validate incoming payloads
│   └── Dockerfile                        # Builds a slim container running this API with uvicorn on port 8080, deployed by the CI/CD pipeline to Cloud Run
│
├── dashboard/                            # Streamlit executive dashboard, reads live from BigQuery
│   ├── app.py                            # Page layout: today's metrics (transactions, fraud count/value), a risk-distribution pie chart, an hourly volume/fraud trend line, and a live table of recent high-risk transactions
│   └── queries.py                        # The parametrized SQL strings app.py runs against `<project>.<dataset>.transaction_risk`
│
├── data/
│   ├── generators/                       # Everything needed to create the synthetic dataset from scratch
│   │   ├── config.py                     # Shared config: record counts, fraud-injection rates, currencies, merchant categories/risk map, amount ranges
│   │   ├── generate_customers.py         # Customers (name, email, country, segment, DOB, ...)
│   │   ├── generate_accounts.py          # Accounts linked to customers (checking/savings/credit/loan/investment)
│   │   ├── generate_merchants.py         # Merchants with category + risk classification (crypto/gambling = high risk)
│   │   ├── generate_devices.py           # Devices (mobile/desktop/tablet/ATM/POS) linked to customers
│   │   ├── generate_transactions.py      # Transactions with deliberately injected fraud patterns (velocity bursts, new devices, geo anomalies, off-hours, ...)
│   │   ├── generate_events.py            # Transaction lifecycle log (created → attempted → completed/failed/flagged)
│   │   ├── generate_streaming.py         # Continuously emits transaction events to stdout, the local Pub/Sub emulator, or real GCP Pub/Sub — simulates a live feed
│   │   └── generate_all.py               # Master script: runs every generator above in order, bulk-loads the CSVs into PostgreSQL, and logs to console + logs/generate_all.log
│   ├── schemas/                          # JSON Schema definitions — one per entity, the formal field/type contract
│   │   └── customer.json, account.json, merchant.json, device.json, transaction.json, transaction_event.json
│   └── sample/                           # ~100-row CSV samples of each entity, committed to git so you can inspect the data shape without generating anything
│
├── data_quality/
│   └── run_checks.py                     # Validates generated/extracted transaction data with Great Expectations (ephemeral, in-memory context): not-null + uniqueness on transaction_id, not-null customer_id/timestamp, amount range, transaction_type enum check
│
├── dbt/
│   ├── dbt_project.yml                   # staging models → views, marts → tables
│   ├── models/
│   │   ├── staging/
│   │   │   ├── schema.yml                # Declares the `financial_risk_staging` and `financial_risk_analytics` BigQuery datasets as dbt sources
│   │   │   ├── stg_customers.sql         # Cleans/renames raw customer columns from the staging source
│   │   │   ├── stg_merchants.sql         # Cleans/renames raw merchant columns from the staging source
│   │   │   └── stg_transactions.sql      # Cleans/renames raw `transaction_risk` rows from the analytics source (the table the streaming pipeline writes into)
│   │   └── marts/
│   │       ├── dim_customer.sql          # Customer dimension — adds a computed `age` from date_of_birth
│   │       ├── dim_merchant.sql          # Merchant dimension — name/category/country/risk tier
│   │       └── fct_transactions.sql      # Incremental fact table (partitioned by day on transaction_timestamp) combining transaction data with the streaming risk score/level
│   └── snapshots/
│       └── customer_risk_snapshot.sql    # dbt snapshot that tracks how each customer's risk level/score changes over time (type: check, on streaming_risk_level/score)
│
├── docs/                                 # Written design docs — see the Documentation Index below for how these relate to the code
│   ├── architecture.md
│   ├── data-model.md
│   ├── data-dictionary.md
│   ├── deployment.md
│   ├── cost-optimization.md
│   └── monitoring.md
│
├── fraud/
│   ├── rules/rule_engine.py              # Evaluates 7 fraud signals per transaction: high amount vs. 30-day average, new device, geo anomaly, velocity burst, repeated failures, high-risk merchant, off-hours
│   └── risk_scoring/scorer.py            # Combines the boolean signals into a weighted 0–100 risk score → LOW (0–30) / MEDIUM (31–70) / HIGH (71–100)
│
├── infrastructure/
│   ├── postgres/init.sql                 # DDL for the local PostgreSQL OLTP schema (enum types + customers/accounts/merchants/devices/transactions/transaction_events). Auto-run by docker-compose on first start.
│   └── terraform/                        # IaC for the real GCP environment
│       ├── main.tf, variables.tf, outputs.tf  # Root module wiring the modules below together
│       └── modules/
│           ├── iam/                      # Service accounts & IAM role bindings (e.g. Dataflow worker SA)
│           ├── storage/                  # GCS buckets: raw / processed / quarantine / archive
│           ├── pubsub/                   # Pub/Sub topic + subscription + dead-letter topic
│           └── bigquery/                 # `staging`/`analytics` datasets, the day-partitioned `transaction_risk` table, and the `transactions_dlq` dead-letter table
│
├── ingestion/
│   ├── api/                              # Placeholder package — superseded by the now-implemented cloud_run/ FastAPI app
│   └── batch/
│       ├── extract.py                    # Incrementally extracts rows from PostgreSQL to CSV using an `updated_at`/timestamp high-water mark. Logs to console + logs/extract.log.
│       └── upload.py                     # Uploads extracted CSVs to the GCS raw bucket for the batch Dataflow pipeline
│
├── monitoring/
│   └── __init__.py                       # Placeholder — the monitoring strategy is documented in docs/monitoring.md but not yet wired up in code
│
├── orchestration/dags/
│   ├── batch_ingestion_dag.py            # Daily Airflow DAG: extract from Postgres → upload to GCS → trigger the batch Dataflow job and block until it completes
│   └── streaming_monitor_dag.py          # Airflow DAG that periodically checks whether the always-on streaming Dataflow job is still healthy
│
├── pipelines/
│   ├── batch/
│   │   ├── pipeline.py                   # Beam pipeline: reads raw CSVs (local or GCS), parses, validates, writes to BigQuery staging. Logs to console + logs/batch_pipeline.log.
│   │   ├── transforms.py                 # DoFns used by pipeline.py (ParseCSVLine, ValidateRecord, TransformForBigQuery)
│   │   ├── run_local.py                  # Runs pipeline.py with --runner=DirectRunner against local data
│   │   └── run_dataflow.py               # Runs pipeline.py with --runner=DataflowRunner against real GCP resources; blocks until the job is submitted (this is what the Airflow DAG calls)
│   └── streaming/
│       ├── pipeline.py                   # Beam pipeline: reads Pub/Sub, enriches, scores fraud risk, writes scored rows to `transaction_risk` and routes invalid/scoring-error messages to the `transactions_dlq` BigQuery table. Logs to console + logs/streaming_pipeline.log.
│       ├── transforms.py                 # DoFns used by pipeline.py (ParsePubSubMessage, EnrichTransaction, ScoreFraudRisk, FormatForBigQuery)
│       ├── run_local.py                  # Runs the streaming pipeline locally with DirectRunner
│       └── run_dataflow.py               # Runs the streaming pipeline on DataflowRunner against a real Pub/Sub subscription
│
├── tests/
│   ├── conftest.py                       # Shared pytest fixtures (sample_customer, sample_transaction, sample_merchant) + .env loading
│   ├── unit/test_fraud_engine.py         # Tests for fraud/rules/rule_engine.py and fraud/risk_scoring/scorer.py
│   ├── unit/test_generators.py           # Tests for the data generator scripts
│   ├── integration/                      # Reserved for pipeline-to-BigQuery integration tests (package marker only)
│   └── data_quality/                     # Reserved for tests around the Great Expectations checks (package marker only)
│
└── transformations/                      # Reserved namespace for a possible Python-side modeling layer mirroring dbt (dimensions/facts/marts); all modeling logic currently lives in dbt/
```

> 💡 **Log Files:** Several scripts (`generate_all.py`, `extract.py`, `pipelines/batch/pipeline.py`,
> `pipelines/streaming/pipeline.py`) write persistent logs to a `logs/` folder created next to
> wherever you run them from, in addition to printing to the console. `logs/` is not committed.

---

## 📚 Documentation Index (`docs/`)

The `docs/` folder is the platform's **design spec** — written ahead of and alongside the code to
capture the full target architecture (in the style of a Google Professional Data Engineer case
study). Some of it describes infrastructure that isn't provisioned yet (e.g. Cloud Composer,
Looker Studio, a `raw`/`core` BigQuery layering, SCD Type 2 dimensions, extra Airflow DAGs). Use
this table to see how each doc maps to what actually exists in the repo today:

| Doc | What it covers | How it maps to the current code |
|---|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Full Lambda-architecture breakdown: data sources, batch/streaming ingestion, the 4-layer `raw → staging → core → analytics` BigQuery model, orchestration DAGs, Cloud Run, security model, DR/retention | The **batch + streaming + Cloud Run + Pub/Sub + BigQuery** shape matches what's implemented. The 4-layer BigQuery split, `core` star-schema dataset, and the extra DAGs it lists (`dimension_refresh_dag`, `fact_load_dag`, `data_quality_dag`, `analytics_mart_dag`, `monitoring_dag`) are target-state — today there are two DAGs (`orchestration/dags/`) and two BigQuery datasets (`staging`, `analytics`) |
| [`docs/data-model.md`](docs/data-model.md) | Full star schema: SCD Type 2 `dim_customer`/`dim_merchant`, `dim_account`, `dim_device`, `dim_date`, `fact_transaction`, `fact_transaction_event`, plus several analytics marts | The current `dbt/models/marts/` implements a simpler slice of this: `dim_customer`, `dim_merchant` (no SCD2 yet), and `fct_transactions`. `customer_risk_snapshot.sql` in `dbt/snapshots/` is the first step toward the SCD2/history tracking this doc describes |
| [`docs/data-dictionary.md`](docs/data-dictionary.md) | Field-by-field reference for every entity, across PostgreSQL and each BigQuery layer | Matches the PostgreSQL schema (`infrastructure/postgres/init.sql`) and JSON Schemas (`data/schemas/`) closely; use it alongside those two when writing new transforms |
| [`docs/deployment.md`](docs/deployment.md) | End-to-end local + GCP deployment walkthrough, including Astronomer/Cloud Composer setup and a troubleshooting section | The **local setup steps** (docker compose, generate data, run pipelines, tests) match the [Execution Guide](#-execution-guide) below. Some GCP steps reference files/flags that don't exist yet in this repo (e.g. `terraform.tfvars.example`, `docker/ingestion-api/`, `generate_streaming_events.py`, `upload_to_gcs.py` — the real script is `ingestion/batch/upload.py`) — follow the **commands in this README** for what's actually runnable today |
| [`docs/cost-optimization.md`](docs/cost-optimization.md) | BigQuery pricing model and planned partitioning/clustering benchmarks | Cost model and reasoning are accurate; the benchmark numbers are placeholders pending a real GCP run |
| [`docs/monitoring.md`](docs/monitoring.md) | Full monitoring/alerting design: Cloud Monitoring metrics, SLAs, dashboards, runbooks | Explicitly marked in the doc itself as a design spec — the `monitoring/` package is still just a placeholder. The Streamlit `dashboard/` is a working step toward the "Data Quality / Fraud Dashboard" this doc describes |

---

## 🚀 Execution Guide

This platform runs entirely on your laptop using simulated services, or fully deployed to GCP.

### 💻 Option 1: Local Development (DirectRunner)

#### 1. Setup Environment
```bash
git clone https://github.com/iamdpsingh/Project-4-Financial-Risk-Fraud-Detection-Data-Platform.git
cd Project-4-Financial-Risk-Fraud-Detection-Data-Platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
```
**Why do we do this?** A virtual environment (`.venv`) isolates this project's dependencies from
your system Python. `requirements.txt` gets you the core runtime packages quickly;
`pip install -e .` additionally installs the repo itself in editable mode (via `pyproject.toml`),
so its internal packages (`fraud`, `pipelines`, `cloud_run`, ...) are importable everywhere, and
pulls in the fuller dependency set (FastAPI, Great Expectations, Streamlit/Plotly). Add
`pip install -e ".[dev]"` if you also want `pytest`/`ruff`/`mypy`.

Copy the environment template and adjust as needed:
```bash
cp .env.example .env
```

#### 2. Start Services & Generate Data
*Requires Docker Desktop running.*
```bash
# Start Postgres & Pub/Sub Emulator
docker compose up -d

# Schema is applied automatically on first start; re-apply manually if ever needed:
docker exec -i frp_postgres psql -U fraud_user -d financial_risk < infrastructure/postgres/init.sql

python data/generators/generate_all.py
```
**Why do we do this?** `docker compose up -d` starts a local PostgreSQL database (source of truth
for historical data) and a Pub/Sub emulator (message broker for live streams). `generate_all.py`
fills PostgreSQL with realistic, synthetic customers/accounts/merchants/devices/transactions/events
and logs its progress to `logs/generate_all.log`.

#### 3. Run Data Quality Checks (optional but recommended)
```bash
python data_quality/run_checks.py
```
**Why do we do this?** Runs the Great Expectations suite over the generated transactions —
uniqueness of `transaction_id`, non-null key fields, valid amount ranges, and valid
`transaction_type` values — catching bad data before it reaches the pipelines.

#### 4. Run Pipelines Locally
**Batch Pipeline:**
```bash
python ingestion/batch/extract.py --output data/raw
python pipelines/batch/pipeline.py --runner=DirectRunner --input_dir=data/raw --output_local
```
**Why do we do this?** `extract.py` simulates a nightly job pulling the latest records from
Postgres and saving them as CSVs. The batch `pipeline.py` then uses Apache Beam's `DirectRunner`
(a local execution engine) to parse the CSVs, validate the data types, and prepare it for
analytics — logging to `logs/batch_pipeline.log` along the way.

**Streaming Pipeline (2 terminal windows):**
```bash
# Terminal 1: Stream transactions to Pub/Sub
source .venv/bin/activate
python data/generators/generate_streaming.py --pubsub

# Terminal 2: Process the stream through Apache Beam
source .venv/bin/activate
python pipelines/streaming/pipeline.py --runner=DirectRunner
```
**Why do we do this?** Terminal 1 fires simulated live transactions into the Pub/Sub broker.
Terminal 2 runs the streaming pipeline, which consumes those messages, runs each one through
`fraud/rules/rule_engine.py` + `fraud/risk_scoring/scorer.py`, writes scored rows to the
`transaction_risk` table, and routes anything malformed or that fails scoring to the
`transactions_dlq` dead-letter table instead of dropping it silently.

#### 5. Run Tests
```bash
pytest tests/unit/ -v --cov=. --cov-report=term-missing
```

#### 6. (Optional) Run the Cloud Run API and Dashboard locally
```bash
# API (needs GOOGLE_APPLICATION_CREDENTIALS + a real/emulated Pub/Sub topic to publish to)
uvicorn cloud_run.main:app --reload --port 8080

# Dashboard (needs a real BigQuery project with a `transaction_risk` table to read from)
streamlit run dashboard/app.py
```

---

### ☁️ Option 2: Production on Google Cloud Platform

*(If you are skipping local execution entirely, these are the steps to deploy and run the platform in the cloud.)*

#### 1. Provision Infrastructure
```bash
gcloud auth application-default login
cd infrastructure/terraform
terraform init
terraform apply -var="project_id=your-gcp-project-id"
```
**Why do we do this?** Terraform (Infrastructure as Code) predictably creates the GCS buckets,
BigQuery datasets/tables (including `transaction_risk` and the `transactions_dlq` dead-letter
table), Pub/Sub topic/subscription/DLQ topic, and service accounts exactly as configured in code.
Deletion protection is off on these resources for clean `terraform destroy` — be mindful of that
outside of learning/demo use.

#### 2. Build and Deploy the Ingestion API
```bash
gcloud auth configure-docker <region>-docker.pkg.dev
docker build -t <image-tag> -f cloud_run/Dockerfile .
docker push <image-tag>
gcloud run deploy transaction-ingestion-api --image=<image-tag> --region=<region>
```
**Why do we do this?** `cloud_run/main.py` is the real front door for live transactions in
production: it validates each payload with the `TransactionEvent` Pydantic model
(`cloud_run/models.py`) and publishes it to Pub/Sub, returning `202 Accepted` immediately.

#### 3. Upload Historical Data and Run Dataflow Pipelines
```bash
python ingestion/batch/upload.py            # push extracted CSVs to the GCS raw bucket
python pipelines/batch/run_dataflow.py      # submit the batch job to Dataflow
python pipelines/streaming/run_dataflow.py  # submit the (long-running) streaming job to Dataflow
```
**Why do we do this?** The `run_dataflow.py` scripts run the exact same Beam pipeline code but on
`DataflowRunner`, letting Google Cloud spin up and autoscale worker VMs instead of your laptop.

#### 4. Model the Warehouse with dbt
```bash
pip install dbt-bigquery
cd dbt
dbt run --vars '{"project_id": "your-gcp-project-id"}'
dbt snapshot --vars '{"project_id": "your-gcp-project-id"}'
```
**Why do we do this?** `dbt run` builds the staging views and the `dim_customer`, `dim_merchant`,
`fct_transactions` tables described above. `dbt snapshot` captures `customer_risk_snapshot`, so you
can see how a customer's risk level changed over time rather than only its latest value.

#### 5. Airflow Orchestration
```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PROJECT_ROOT=$(pwd)
export VENV_PYTHON=$(pwd)/.venv/bin/python
airflow db init
airflow standalone
```
*Access the Airflow UI at `http://localhost:8080` (login printed in the terminal) to enable
`batch_ingestion_pipeline` and `streaming_monitor_dag`.*

**Why do we do this?** Airflow schedules the daily batch chain (extract → upload → Dataflow,
blocking until the job is submitted) and separately polls the health of the always-on streaming
Dataflow job — both defined in `orchestration/dags/`.

#### 6. Deploy the Dashboard
```bash
streamlit run dashboard/app.py
# or containerize it yourself and deploy to Cloud Run alongside the ingestion API
```

---

## 🧪 Running Tests, Lint & Type Checks

```bash
pytest tests/unit/ -v                       # unit tests (fraud engine, generators)
pytest --cov=. --cov-report=term-missing    # full suite with coverage (as run in CI)

ruff check .                                # lint
ruff format --check .                       # format check
mypy . --ignore-missing-imports             # type check
```

---

## 🔄 CI/CD

`.github/workflows/ci-cd.yml` runs on every push/PR to `main`/`develop`:

1. **Lint** — `ruff check`, `ruff format --check`, `mypy`
2. **Test** — `pytest tests/unit/` with coverage uploaded to Codecov
3. **Terraform validate** — `terraform fmt -check`, `terraform validate`
4. **Docker build** — builds the `cloud_run/` ingestion API image (build-only on PRs)
5. **Deploy** (main branch only) — authenticates to GCP via Workload Identity Federation, pushes
   the image to Artifact Registry, and deploys it to Cloud Run

---

## ✅ Project Status

**Fully Implemented and runnable today**, locally and/or against a real GCP project: synthetic data
generation, the PostgreSQL OLTP schema, batch extract/upload, both Beam pipelines (including the
streaming pipeline's dead-letter routing to BigQuery), the rule-based fraud engine, Great
Expectations data quality checks, the `cloud_run/` FastAPI ingestion API, the `dashboard/`
Streamlit app, dbt staging/mart models + a snapshot, Airflow DAGs, and the Terraform infrastructure.

**Documentation:** The `docs/` folder contains extensive design documents outlining a robust target architecture (Cloud Composer, Looker Studio, a 4-layer BigQuery model, SCD Type 2 dimensions). See the [Documentation Index](#-documentation-index-docs) above for how each document maps to the implemented code.