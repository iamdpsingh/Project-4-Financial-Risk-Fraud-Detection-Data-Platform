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

- [1. System Architecture & Animated Flow](#-system-architecture)
- [2. Real-Time Fraud Engine & Scoring Logic](#-real-time-fraud-engine--scoring-logic)
- [3. Data Matrix & Volume Scale](#-data-matrix--volume-scale)
- [4. Repository Layout](#-repository-layout)
- [5. Documentation Index (docs/)](#-documentation-index-docs)
- [6. The Ultimate Step-by-Step Guide](#-the-ultimate-step-by-step-guide-start-to-finish)
- [7. Execution Guide (Local & GCP)](#-execution-guide)
  - [Option 1: Local Development](#-option-1-local-development-directrunner)
  - [Option 2: Production on GCP](#-option-2-production-on-google-cloud-platform)
- [8. Troubleshooting & Common GCP Errors (12 Real Solutions)](#-troubleshooting--common-gcp-errors)
- [9. Running Tests, Lint & Type Checks](#-running-tests-lint--type-checks)
- [10. CI/CD](#-cicd)
- [11. Project Status](#-project-status)

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

### 🔄 How the System Works (Simple Explanation)

This project handles both historical data and live data at the same time:

1. **The Batch Path (Historical Data):**
   - We take 2.5 million old banking records from our database.
   - We save them into Google Cloud Storage as simple CSV files.
   - We use Google Cloud Dataflow to clean up this data and load it into Google BigQuery so we can analyze it later.
   - This happens automatically every day using a tool called Apache Airflow.

2. **The Real-Time Path (Live Streaming):**
   - Live transactions (like someone swiping a credit card right now) are sent to a fast web service (Cloud Run).
   - Google Cloud Pub/Sub acts as a waiting room so we don't get overwhelmed if millions of people swipe their cards at once.
   - A real-time engine (Dataflow) checks each transaction for fraud as it happens, gives it a score from 0-100, and saves the results to BigQuery.
   - Any broken or corrupted data is safely set aside so we don't lose anything.

3. **Data Analysis & Dashboard:**
   - We use a tool called `dbt` to organize the messy raw data into neat, easy-to-read tables.
   - Finally, a beautiful Streamlit dashboard shows us live charts, maps, and alerts about fraud.

---

## 🧠 How We Detect Fraud (The Rules)

Our real-time engine checks every single transaction against 7 simple rules. Each rule adds a certain amount of "risk points" to the transaction:

1. **Unusually High Amount (25% weight):** Is the transaction much larger than the person's normal everyday spending?
2. **New Device (20% weight):** Is this a brand new phone or computer that this customer has never used before?
3. **Impossible Travel (20% weight):** Did the person make a purchase in New York and then London just 10 minutes later?
4. **Too Many Transactions (15% weight):** Are they trying to buy things way too fast (like 5 purchases in 10 minutes)?
5. **Repeated Failures (10% weight):** Did their card get declined several times in a row before this purchase?
6. **Risky Merchants (10% weight):** Are they buying from a high-risk category (like offshore gambling or crypto)?
7. **Late Night Activity (5% weight):** Is the purchase happening in the middle of the night (between 2:00 AM and 5:00 AM)?

### 🧮 The Final Risk Score

We add up the points to get a final score from 0 to 100:

- 🟢 **LOW RISK (0 to 30):** Safe! The transaction is approved automatically.
- 🟡 **MEDIUM RISK (31 to 70):** Suspicious. We pause the transaction and ask the user to verify it (like sending them a text message code).
- 🔴 **HIGH RISK (71 to 100):** Dangerous! The transaction is blocked immediately and the fraud security team is alerted.

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
cp .env.example .env
```

#### 2. Start Services & Generate Data
*Requires Docker Desktop running.*
```bash
docker compose up -d
docker exec -i frp_postgres psql -U fraud_user -d financial_risk < infrastructure/postgres/init.sql
PYTHONPATH=. python data/generators/generate_all.py
```

#### 3. Run Data Quality Checks (Optional)
```bash
PYTHONPATH=. python data_quality/run_checks.py
```

#### 4. Run Pipelines Locally
**Batch Pipeline:**
```bash
PYTHONPATH=. python ingestion/batch/extract.py --output data/raw
PYTHONPATH=. python pipelines/batch/pipeline.py --runner=DirectRunner --input_dir=data/raw --output_local
```

**Streaming Pipeline (2 terminal windows):**
```bash
# Terminal 1: Stream transactions to Pub/Sub
source .venv/bin/activate
PYTHONPATH=. python data/generators/generate_streaming.py --pubsub

# Terminal 2: Process the stream through Apache Beam
source .venv/bin/activate
PYTHONPATH=. python pipelines/streaming/pipeline.py --runner=DirectRunner
```

#### 5. Run Tests
```bash
pytest tests/unit/ -v --cov=. --cov-report=term-missing
```

#### 6. (Optional) Run the Cloud Run API and Dashboard locally
```bash
uvicorn cloud_run.main:app --reload --port 8080
streamlit run dashboard/app.py
```

---

### ☁️ Option 2: Production on Google Cloud Platform

#### 1. Provision Infrastructure
```bash
gcloud auth application-default login
cd infrastructure/terraform
terraform init
terraform apply -var="project_id=your-gcp-project-id"
```

#### 2. Build and Deploy the Ingestion API (No local Docker needed)
```bash
# Cloud Build expects the Dockerfile in the root, so we temporarily copy it
cp cloud_run/Dockerfile .
gcloud builds submit --tag us-central1-docker.pkg.dev/your-gcp-project-id/fraud-repo/transaction-ingestion-api:latest .
rm Dockerfile

# Deploy the built image to Cloud Run
gcloud run deploy transaction-ingestion-api \
  --image=us-central1-docker.pkg.dev/your-gcp-project-id/fraud-repo/transaction-ingestion-api:latest \
  --region=us-central1 \
  --allow-unauthenticated
```

#### 3. Upload Historical Data and Run Dataflow Pipelines
```bash
PYTHONPATH=. python ingestion/batch/upload.py --source data/raw  # push generated CSVs to GCS
PYTHONPATH=. python pipelines/batch/run_dataflow.py              # submit the batch job
PYTHONPATH=. python pipelines/streaming/run_dataflow.py          # submit the streaming job
```

#### 4. Model the Warehouse with dbt
```bash
pip install dbt-bigquery
cd dbt
dbt run --vars '{"project_id": "your-gcp-project-id"}'
dbt snapshot --vars '{"project_id": "your-gcp-project-id"}'
```

#### 5. Airflow Orchestration
```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PROJECT_ROOT=$(pwd)
export VENV_PYTHON=$(pwd)/.venv/bin/python
airflow db migrate
airflow standalone
```

#### 6. Deploy the Dashboard
```bash
streamlit run dashboard/app.py
```

### 🔧 Troubleshooting & Common GCP Errors

If you encounter issues deploying to GCP, here are the common causes and solutions:

#### 1. Cloud Build Permission Denied Error
**Problem:** `PERMISSION_DENIED: The caller does not have permission...` or `Image not found` during `gcloud run deploy`.
**Cause:** The default Compute Engine service account used by Cloud Build lacks permissions to read from the Cloud Build GCS bucket or write to Artifact Registry.
**Solution:** Ensure the service account has `roles/storage.admin` and `roles/artifactregistry.writer` using IAM bindings.

#### 2. Cloud Run API Fails to Start
**Problem:** `The user-provided container failed to start and listen on the port defined provided by the PORT=8080 environment variable...`
**Cause:** `cloud_run/main.py` is trying to import from `utils/` but the Dockerfile only copies `cloud_run/`.
**Solution:** Update `main.py` to use Python's built-in `logging` module instead of the local `utils.logger`, or update the Dockerfile to `COPY utils/ /app/utils/`.

#### 3. Dataflow Job Startup / Missing Permissions
**Problem:** `Missing permissions dataflow.jobs.get... as Dataflow worker service account 645137491665-compute@developer.gserviceaccount.com`
**Cause:** The Dataflow pipeline is attempting to run under the default compute service account which lacks permissions, instead of the dedicated Terraform-created `dataflow-worker-sa`.
**Solution:** Pass `--service_account_email=dataflow-worker-sa@<your-project-id>.iam.gserviceaccount.com` when running `pipelines/batch/run_dataflow.py` and `pipelines/streaming/run_dataflow.py`.

#### 4. GCP Zone Resource Stockout
**Problem:** `ZONE_RESOURCE_POOL_EXHAUSTED: Instance creation failed: The zone 'us-central1-c' does not have enough resources available...`
**Cause:** Google Cloud's data center in the selected zone is temporarily full and cannot provision new VMs for your Dataflow job.
**Solution:** Tell Dataflow to use a different zone by passing `--worker_zone=us-central1-f` or `--worker_zone=us-central1-b` in the python script runner flags.

#### 5. `dbt` Profile Not Found
**Problem:** `dbt run` fails with `Profile 'financial_risk_platform' not found`.
**Cause:** dbt looks for `profiles.yml` in `~/.dbt/` by default, which may not exist.
**Solution:** Create a local `profiles.yml` in the `dbt/` directory and run `dbt run --profiles-dir .` to tell dbt to use the local configuration.

#### 6. BigQuery Dataset Naming Mismatches
**Problem:** `dbt` errors with `Not found: Dataset financial_risk_staging`.
**Cause:** Terraform provisions the datasets as `staging` and `analytics`, but the `dbt` source schemas might reference the old names `financial_risk_staging` and `financial_risk_analytics`.
**Solution:** Edit `dbt/models/staging/schema.yml` to change `dataset: financial_risk_staging` to `dataset: staging` and `dataset: financial_risk_analytics` to `dataset: analytics`.

#### 7. `dbt` SQL Column Name Mismatches
**Problem:** `Unrecognized name: timestamp` or `actual_is_fraud` during `dbt run`.
**Cause:** The SQL models in `dbt/models/staging/` or `dbt/models/marts/` are trying to select columns that don't match the exact names generated by the BigQuery schema from Dataflow (e.g., trying to rename `timestamp as transaction_timestamp` when the schema is already named `transaction_timestamp`).
**Solution:** Edit the `.sql` models to select the exact column names present in the BigQuery tables.

#### 8. Dataflow `ModuleNotFoundError`
**Problem:** `Dataflow pipeline failed. State: FAILED... ModuleNotFoundError: No module named 'utils'`.
**Cause:** Dataflow workers fail to unpickle Python dependencies if local root-level modules (like `utils`) are imported but not properly packaged or are shadowed by standard libraries on the Google Cloud worker.
**Solution:** Change local imports (like `from utils.logger import get_logger`) to standard built-in modules (`import logging`) in the files executed by Dataflow (e.g., `pipelines/batch/transforms.py`).

#### 9. Airflow Database Initialization Error
**Problem:** `airflow db command error: argument COMMAND: invalid choice: 'init'`.
**Cause:** In Airflow 2.7 and later, `airflow db init` has been deprecated and removed.
**Solution:** Use `airflow db migrate` instead.

#### 10. Dataflow BigQuery Sink: "400 No schema specified on job or table"
**Problem:** `google.api_core.exceptions.BadRequest: 400 No schema specified on job or table.` when Dataflow attempts to load batch records or errors into BigQuery.
**Cause:** Apache Beam's `beam.io.WriteToBigQuery` with `create_disposition=CREATE_IF_NEEDED` expects a table schema when creating tables dynamically. Without an explicit schema string, BigQuery rejects the load job.
**Solution:** Pass `additional_bq_parameters={'autodetect': True}` inside `WriteToBigQuery(...)` so BigQuery automatically infers column types from incoming JSON dictionaries.

#### 11. Orphaned / Zombie Dataflow Jobs Running on GCP
**Problem:** You press `Ctrl+C` in your terminal to cancel `run_dataflow.py`, but you notice compute costs increasing and worker VMs still active in the Google Cloud Console.
**Cause:** Terminating the local Python client script only disconnects the logging listener. The distributed pipeline graph has already been submitted to Google Cloud Dataflow and continues executing independently on GCP Compute Engine instances.
**Solution:** Run `gcloud dataflow jobs list --status=active --region=us-central1` to find the active Job ID, then cancel it explicitly via `gcloud dataflow jobs cancel <JOB_ID> --region=us-central1`.

#### 12. GCS Bucket Soft-Delete Policy Storage Costs
**Problem:** Warning: `Bucket gs://*-dataflow-temp used as temp_location has soft-delete policy enabled.`
**Cause:** New GCP buckets have a 7-day soft-delete retention policy enabled by default. Since Dataflow writes and deletes thousands of temporary shuffle shards every run, retained soft-deleted files can incur unexpected storage charges.
**Solution:** Set the soft-delete retention duration to 0 on temporary buckets using `gcloud storage buckets update gs://<your-bucket> --clear-soft-delete` or configure it in Terraform (`soft_delete_policy { retention_duration_seconds = 0 }`).

#### 13. Missing API Services (Cloud Scheduler / Data Pipelines)
**Problem:** Errors in GCP indicating "Enable service: cloudscheduler.googleapis.com" or "datapipelines.googleapis.com".
**Cause:** Some background orchestration or pipeline tasks require these APIs to be explicitly enabled in your GCP project.
**Solution:** Run the following commands to enable them:
```bash
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable datapipelines.googleapis.com
```

#### 14. Dashboard is Empty (Streaming Inserts Silently Dropped)
**Problem:** The Streamlit dashboard shows no metrics or data, and the BigQuery `transaction_risk` table remains empty even though the Dataflow streaming job is running and messages are publishing to Pub/Sub.
**Cause:** The streaming data generator produces extra diagnostic fields (e.g. `emitted_at`) that do not exist in the BigQuery schema. By default, Dataflow `WriteToBigQuery` using streaming inserts rejects rows with unknown columns.
**Solution:** Add `ignore_unknown_columns=True` to the `WriteToBigQuery` transform in `pipelines/streaming/pipeline.py` so BigQuery safely ignores the extra fields instead of dropping the entire transaction record.

---

## 🚀 The Ultimate Step-by-Step Guide (Start to Finish)

If you have never touched Google Cloud, Dataflow, or dbt before, this is your complete end-to-end roadmap. Follow these exact steps in order, and you will have a full enterprise data platform running locally and in the cloud:

### Step 1: Environment & Tooling Setup
Before building any platform, you need your local tools ready.
1. **Prepare Virtual Environment:** In your terminal, run `python3 -m venv .venv` and `source .venv/bin/activate` to create an isolated Python workspace. Install project dependencies with `pip install -r requirements.txt`.
2. **Setup Google Cloud Project:** Create a project in `console.cloud.google.com` (e.g. `financial-data-platform-XXXXXX`) and ensure billing is enabled.
3. **Authenticate Local CLI:** Install the Google Cloud SDK and run:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
   This generates local credentials so Terraform, Apache Beam, and Python scripts can securely interact with GCP APIs without hardcoding passwords.

### Step 2: Provision Cloud Infrastructure (Terraform)
Automate the provisioning of all cloud storage, analytical warehouses, and event buses.
4. **Initialize Infrastructure as Code:**
   ```bash
   cd infrastructure/terraform
   terraform init
   terraform apply -var="project_id=your-gcp-project-id"
   cd ../..
   ```
5. **What Just Happened:** Terraform created your raw Google Cloud Storage buckets, the Pub/Sub `transaction-events` topic, the BigQuery `staging` and `analytics` datasets, and generated a dedicated service account (`dataflow-worker-sa`) pre-configured with required IAM roles.

### Step 3: Generate Synthetic Banking Dataset
Generate realistic financial transaction history containing deliberate fraud anomalies.
6. **Generate Data & Upload to GCS:**
   ```bash
   PYTHONPATH=. python ingestion/batch/upload.py --source data/raw
   ```
7. **What Just Happened:** Synthesizes 2.5 million banking transactions across 100K customer profiles, 25K merchants, and 250K devices, embedding behavioral fraud signatures (velocity bursts, impossible travel, new device logins) and uploads partitioned CSV files directly to `gs://<bucket>-raw-data/`.

### Step 4: Run Distributed Batch Ingestion (Apache Beam on Dataflow)
Ingest the 2.5 million CSV records from GCS into Google BigQuery using parallel worker nodes.
8. **Submit Batch Pipeline:**
   ```bash
   PYTHONPATH=. python pipelines/batch/run_dataflow.py
   ```
9. **What Just Happened:** Submits an Apache Beam DAG to Google Cloud Dataflow. Dataflow dynamically provisions Compute Engine worker VMs to read GCS CSVs, validate field constraints against JSON schemas, route malformed rows to Dead-Letter sinks, and bulk-load clean records into BigQuery `staging` tables.
10. **Runtime Note:** *Batch execution takes approximately 6 to 10 minutes.* Let this complete before running dbt modeling in Step 6.

### Step 5: Start Real-Time Fraud Radar (Streaming Dataflow)
Deploy a low-latency event processing engine that scores live transactions in sub-second time.
11. **Submit Streaming Pipeline:**
    ```bash
    PYTHONPATH=. python pipelines/streaming/run_dataflow.py
    ```
12. **Start the Live Data Generator:** The streaming pipeline needs live data to process! Open a new terminal and run:
    ```bash
    source .venv/bin/activate
    PYTHONPATH=. python data/generators/generate_streaming.py --pubsub
    ```
13. **What Just Happened:** First, we deployed an always-on streaming pipeline to Dataflow. Second, we started a live simulator pumping 50 synthetic transactions per second into Pub/Sub. The Dataflow engine evaluates these transactions for fraud in real-time and writes the scored results directly to `analytics.transaction_risk` in BigQuery!

### Step 6: Transform & Model the Warehouse (dbt)
Transform raw staging tables into an optimized dimensional star schema for business intelligence.
14. **Execute dbt Models:**
    ```bash
    cd dbt
    dbt run --profiles-dir .
    dbt test --profiles-dir .
    cd ..
    ```
15. **What Just Happened:** dbt cleans, deduplicates, and structures raw data from `staging` into production-ready analytical models: `dim_customer` (enriched with age calculations), `dim_merchant` (categorized by risk tier), and the partitioned fact table `fct_transactions`.

### Step 7: Automate Daily Pipelines (Apache Airflow)
Automate recurring data workflows so the platform runs unattended.
16. **Start Orchestrator:**
    ```bash
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db migrate
    airflow standalone
    ```
17. **What Just Happened:** Airflow runs an orchestration server that triggers daily batch extractions from PostgreSQL, monitors Dataflow job execution states, and triggers downstream dbt runs upon successful data landing.

### Step 8: Launch Executive Risk Dashboard (Streamlit)
Provide compliance officers and fraud analysts with real-time operational visibility.
18. **Start the Dashboard:**
    ```bash
    streamlit run dashboard/app.py
    ```
19. **View the Live Data:** 
    - Open your browser to `http://localhost:8501`.
    - Wait about 30 seconds for the live data from Step 5 to reach BigQuery.
    - Click the **"🔄 Refresh Data"** button in the left sidebar (the dashboard caches data for 60 seconds).
20. **What Just Happened:** You launched an interactive web application! Because you started the live data stream in Step 5, you'll see the dashboard instantly light up with live volume metrics, risk distribution charts, and critical threat feeds!

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