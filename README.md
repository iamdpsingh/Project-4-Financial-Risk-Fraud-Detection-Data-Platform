# Financial Risk & Fraud Detection Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Beam-2.55+-orange?style=for-the-badge&logo=apache" />
  <img src="https://img.shields.io/badge/Google%20BigQuery-4285F4?style=for-the-badge&logo=google-cloud" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.10+-017CEE?style=for-the-badge&logo=apache-airflow" />
  <img src="https://img.shields.io/badge/Terraform-1.7+-7B42BC?style=for-the-badge&logo=terraform" />
  <img src="https://img.shields.io/badge/dbt-1.7+-FF694B?style=for-the-badge&logo=dbt" />
  <img src="https://img.shields.io/badge/Docker-24+-2496ED?style=for-the-badge&logo=docker" />
</p>

> A production-grade financial data engineering platform. It ingests batch and real-time transaction data, runs a risk scoring engine, enforces data quality at every layer, and models data into a BigQuery data warehouse. It is orchestrated with Airflow, transformed using dbt, provisioned with Terraform, and secured with GitHub Actions.

---

## 🏗️ Architecture

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
                               ↓
                        dbt (Data Build Tool)
                               │
            ┌──────────────────┼──────────────────┐
            ↓                  ↓                  ↓
       Staging Views     Dimensions/Facts   Analytics Marts
```

---

## 🚀 How to Run Locally

You can run this entire platform on your local machine using Python `.venv` and Docker Compose. 

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/iamdpsingh/Project-4-Financial-Risk-Fraud-Detection-Data-Platform.git
cd Project-4-Financial-Risk-Fraud-Detection-Data-Platform

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install all requirements
pip install -r requirements.txt
pip install -e .  # Installs the local package
```

### 2. Start Local Infrastructure

> **Note:** You must have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running to use the local database and Pub/Sub emulator.

Start the local PostgreSQL database and Pub/Sub emulator:
```bash
docker-compose up -d
```

Initialize the database schema:
```bash
docker exec -i postgres_db psql -U admin -d financial_risk < infrastructure/postgres/init.sql
```

### 3. Generate Data

Generate the historical synthetic data into the database:
```bash
python data/generators/generate_all.py
```

### 4. Run Batch Pipeline (DirectRunner)

Extract data from Postgres to local staging:
```bash
python ingestion/batch/extract.py --output data/raw
```

Process data through Apache Beam (Local):
```bash
python pipelines/batch/pipeline.py \
  --runner=DirectRunner \
  --input_dir=data/raw \
  --output_local
```

### 5. Run Streaming Pipeline (DirectRunner)

Start the streaming generator in one terminal window:
```bash
source .venv/bin/activate
python data/generators/generate_streaming.py --pubsub
```

In another terminal, start the streaming processor:
```bash
source .venv/bin/activate
python pipelines/streaming/pipeline.py \
  --runner=DirectRunner
```

### 6. Airflow Orchestration

Initialize the Airflow database and start the standalone server:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow standalone
```
Log in at `http://localhost:8080` (credentials will be printed in the terminal). Enable the `batch_ingestion_pipeline` DAG.

---

## 📂 Repository Structure

```text
.
├── .github/workflows/         # CI/CD pipelines (Lint, Test, Deploy)
├── airflow/                   # Airflow runtime directory (ignored)
├── data/
│   ├── schemas/               # JSON Schema definitions for all entities
│   └── generators/            # Synthetic data generation scripts
├── data_quality/              # Great Expectations validation suites
├── dbt/                       # Data Build Tool SQL models (staging, marts)
├── docs/                      # Extensive architecture and cost documentation
├── fraud/
│   ├── rules/                 # Fraud detection signal logic (e.g. velocity, location)
│   └── risk_scoring/          # Scoring engine weighting the signals
├── infrastructure/
│   ├── postgres/              # Local DB initialization scripts
│   └── terraform/             # IaC to provision GCP resources (Buckets, BQ, IAM)
├── ingestion/                 # Scripts to extract data from DB and upload
├── orchestration/dags/        # Airflow DAGs for batch and streaming pipelines
├── pipelines/                 # Apache Beam pipelines
│   ├── batch/                 # Historical data loading and validation
│   └── streaming/             # Real-time transaction scoring via Pub/Sub
├── tests/                     # Unit and integration test suites
├── requirements.txt           # Python dependencies
└── setup.py                   # Packaging script for GCP Dataflow workers
```
