<div align="center">
  
# 🛡️ Enterprise Financial Risk & Fraud Detection Platform
**A Production-Grade, Real-Time Data Engineering Architecture**

[![Python 3.12+](https://img.shields.io/badge/Python-3.12+-blue?style=for-the-badge&logo=python)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.55+-orange?style=for-the-badge&logo=apache)](https://beam.apache.org/)
[![Google BigQuery](https://img.shields.io/badge/Google%20BigQuery-4285F4?style=for-the-badge&logo=google-cloud)](https://cloud.google.com/bigquery)
[![Google Pub/Sub](https://img.shields.io/badge/Google%20Pub/Sub-4285F4?style=for-the-badge&logo=google-cloud)](https://cloud.google.com/pubsub)
<br>
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10+-017CEE?style=for-the-badge&logo=apache-airflow)](https://airflow.apache.org/)
[![Terraform 1.7+](https://img.shields.io/badge/Terraform-1.7+-7B42BC?style=for-the-badge&logo=terraform)](https://www.terraform.io/)
[![dbt 1.7+](https://img.shields.io/badge/dbt-1.7+-FF694B?style=for-the-badge&logo=dbt)](https://www.getdbt.com/)
[![Docker 24+](https://img.shields.io/badge/Docker-24+-2496ED?style=for-the-badge&logo=docker)](https://www.docker.com/)

An end-to-end cloud data platform capable of processing **millions of synthetic banking transactions** through both batch and sub-second streaming pipelines. Built natively on GCP with industry best practices for IaC, orchestration, and CI/CD.

Every single transaction is scored for fraud risk using a custom **rule engine**, modeled with **dbt**, and surfaced on a beautiful **Streamlit** dashboard. Everything is orchestrated with **Airflow**, provisioned natively on Google Cloud with **Terraform**, and validated by a GitHub Actions CI/CD pipeline.

</div>

---

## 📑 Table of Contents

- [1. System Architecture & Animated Flow](#-system-architecture)
- [2. Real-Time Fraud Engine & Scoring Logic](#-real-time-fraud-engine--scoring-logic)
- [3. Repository Layout](#-repository-layout)
- [4. Documentation Index (docs/)](#-documentation-index-docs)
- [5. Execution Guide (Local & GCP)](#-execution-guide)
  - [Option 1: Local Development](#-option-1-local-development-directrunner)
  - [Option 2: Production on GCP](#-option-2-production-on-google-cloud-platform)
- [6. Troubleshooting & Common GCP Errors (14 Real Solutions)](#-troubleshooting--common-gcp-errors)
- [7. Running Tests, Lint & Type Checks](#-running-tests-lint--type-checks)
- [8. CI/CD](#-cicd)

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

1. **The Batch Path (Historical Data):** We extract 2.5 million old banking records from PostgreSQL to Google Cloud Storage (GCS) and load them into BigQuery using Dataflow.
2. **The Real-Time Path (Live Streaming):** Live transactions are sent to Cloud Run, queued via Pub/Sub, and instantly analyzed by a Dataflow real-time engine which gives a risk score from 0-100.
3. **Data Analysis & Dashboard:** Raw data is structured using `dbt`, and the real-time Streamlit dashboard shows live fraud threats instantly.

---

## 🧠 Real-Time Fraud Engine & Scoring Logic

Our real-time engine checks every single transaction against 7 simple rules to generate a unified risk score (0-100):

| Signal | Weight | Detection Logic |
|---|---|---|
| **High Velocity** | 25% | Transaction is much larger than the person's normal everyday spending |
| **New Device** | 20% | Transaction originating from a brand new unlinked device |
| **Impossible Travel** | 20% | Purchase in NY and then London just 10 minutes later |
| **Velocity Burst** | 15% | High frequency of rapid-fire transactions within a 10-minute window |
| **Repeated Failures**| 10% | Card got declined several times in a row before this purchase |
| **Risky Merchant** | 10% | Buying from a high-risk category (e.g. offshore gambling, crypto) |
| **Off-Hours** | 5% | Activity detected during deep off-hours (2:00 AM - 5:00 AM) |

### 🧮 Final Risk Score Outcomes

- 🟢 **LOW RISK (0 to 30):** Safe! Approved automatically.
- 🟡 **MEDIUM RISK (31 to 70):** Suspicious. Paused for user verification (2FA/Step-up).
- 🔴 **HIGH RISK (71 to 100):** Dangerous! Blocked immediately and security team alerted.

---

## 📂 Repository Layout

```text
├── .github/workflows/ci-cd.yml      # GitHub Actions CI/CD Pipeline
├── cloud_run/                       # FastAPI high-throughput ingestion endpoint
├── dashboard/                       # Real-time Streamlit Command Center
├── data/                            # Synthetic data & fraud generators (PostgreSQL load, Pub/Sub emission)
├── dbt/                             # BigQuery dimensional modeling & snapshotting
├── docs/                            # Comprehensive architectural design specs
├── fraud/                           # Core risk scoring engine & behavioral heuristics
├── infrastructure/                  # Terraform IaC (GCS, Pub/Sub, BigQuery) & PostgreSQL Docker init
├── ingestion/                       # PostgreSQL extraction & GCS bulk uploading scripts
├── orchestration/dags/              # Apache Airflow DAGs for batch scheduling and monitoring
├── pipelines/                       # Apache Beam pipelines (Batch CSV & Streaming Pub/Sub)
└── tests/                           # Pytest unit tests & Great Expectations Data Quality assertions
```

---

## 📚 Documentation Index (`docs/`)

The `docs/` folder is the platform's **design spec**:
- **`architecture.md`**: Full Lambda-architecture breakdown, Dataflow/BigQuery sizing, DR/retention.
- **`data-model.md`**: Full star schema structure and analytics marts.
- **`data-dictionary.md`**: Field-by-field reference for every Postgres/BigQuery entity.
- **`deployment.md`**: Extensive local and GCP deployment guide.
- **`cost-optimization.md`**: BigQuery pricing and planned partitioning benchmarks.
- **`monitoring.md`**: SLA, dashboards, and Cloud Monitoring alert designs.

---

## 🚀 Execution Guide

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

#### 3. Run Pipelines Locally
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

#### 4. Launch the Dashboard
```bash
streamlit run dashboard/app.py
```

---

### ☁️ Option 2: Production on Google Cloud Platform

#### 1. Provision Infrastructure
```bash
gcloud auth application-default login
cd infrastructure/terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

#### 2. Build and Deploy the Ingestion API (Cloud Run)
```bash
cp cloud_run/Dockerfile .
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/fraud-repo/transaction-api:latest .
gcloud run deploy transaction-api --image=us-central1-docker.pkg.dev/YOUR_PROJECT_ID/fraud-repo/transaction-api:latest --region=us-central1 --allow-unauthenticated
rm Dockerfile
```

#### 3. Upload Historical Data and Run Dataflow Pipelines
```bash
PYTHONPATH=. python ingestion/batch/upload.py --source data/raw
PYTHONPATH=. python pipelines/batch/run_dataflow.py
PYTHONPATH=. python pipelines/streaming/run_dataflow.py
```

#### 4. Model the Warehouse with dbt
```bash
cd dbt
dbt run --vars '{"project_id": "YOUR_PROJECT_ID"}'
```

#### 5. Airflow Orchestration
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow standalone
```

---

## 🔧 Troubleshooting & Common GCP Errors

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

## 🛠️ Running Tests, Lint & Type Checks

```bash
pytest tests/unit/ -v                       # unit tests
pytest --cov=. --cov-report=term-missing    # test suite with coverage

ruff check .                                # linter
ruff format --check .                       # format checker
mypy . --ignore-missing-imports             # static type checker
```

---

## 🔄 CI/CD

`.github/workflows/ci-cd.yml` runs on every push/PR to `main`/`develop`:

1. **Lint** — `ruff check`, `ruff format --check`, `mypy`
2. **Test** — `pytest tests/unit/` with coverage uploaded to Codecov
3. **Terraform Validate** — `terraform fmt -check`, `terraform validate`
4. **Docker Build** — builds the `cloud_run/` ingestion API image
5. **Deploy** (main branch only) — authenticates to GCP via Workload Identity Federation, pushes to Artifact Registry, and deploys to Cloud Run

<div align="center">
  <br>
  <i>Architected for scale. Designed for security. Built for production.</i>
</div>