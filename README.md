# 🏦 Financial Risk & Fraud Detection Data Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Beam-2.55+-orange?style=for-the-badge&logo=apache" />
  <img src="https://img.shields.io/badge/Google%20BigQuery-4285F4?style=for-the-badge&logo=google-cloud" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=for-the-badge&logo=apache-airflow" />
  <img src="https://img.shields.io/badge/Terraform-1.7+-7B42BC?style=for-the-badge&logo=terraform" />
  <img src="https://img.shields.io/badge/Docker-24+-2496ED?style=for-the-badge&logo=docker" />
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" />
</p>

> **A production-oriented financial data engineering platform** built on Google Cloud Platform. Processes batch and real-time transaction data, performs risk scoring, enforces data quality, and delivers analytics through a governed BigQuery data warehouse — orchestrated with Airflow, provisioned with Terraform, and monitored with Cloud Monitoring.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Quick Start (Local)](#quick-start-local)
- [Data Model](#data-model)
- [Risk Scoring Engine](#risk-scoring-engine)
- [Streaming Pipeline](#streaming-pipeline)
- [Data Quality Framework](#data-quality-framework)
- [GCP Deployment](#gcp-deployment)
- [CI/CD](#cicd)
- [Monitoring](#monitoring)
- [BigQuery Optimization](#bigquery-optimization)
- [Documentation](#documentation)
- [Build Phases](#build-phases)

---

## Overview

This platform simulates a **mid-to-large financial company** processing millions of transactions per day. It is designed as a realistic, production-grade data engineering system — not a notebook experiment.

### Business Context

- **10,000+ customers** across 15+ countries
- **500,000+ transactions/day** across multiple payment methods
- **Near-real-time fraud detection** with a composite risk scoring engine
- **Historical analytics** delivered through a governed BigQuery data warehouse
- **End-to-end data lineage** and quality tracking at every pipeline stage

### What makes this project stand out

| Capability | Implementation |
|---|---|
| Hybrid batch + streaming | Lambda architecture: Dataflow batch + Dataflow streaming |
| Real-time fraud signals | 7-signal weighted risk engine scoring transactions in < 30 seconds |
| Historical dimension management | SCD Type 2 for customer and merchant dimension tables |
| Data quality enforcement | Great Expectations checkpoints at every layer with error tracking |
| Late-arriving event handling | Watermarks + allowed lateness in Apache Beam |
| Infrastructure as Code | Full Terraform provisioning of all GCP resources |
| Cost optimization | Partitioned + clustered BigQuery tables; incremental loading |
| Operational observability | Cloud Monitoring dashboards, custom metrics, alerting policies |

---

## Architecture

```
                            ┌────────────────────────────────────────────────┐
                            │                  DATA SOURCES                  │
                            └────────────────────────────────────────────────┘
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    │                         │                         │
              PostgreSQL                  REST APIs             Transaction
           (historical data)          (external enrichment)    event stream
                    │                         │                         │
                    └─────────────────────────┼─────────────────────────┘
                                              │
                                   ┌──────────┴──────────┐
                                   │                     │
                                BATCH               STREAMING
                                   │                     │
                                   ↓                     ↓
                            Cloud Storage            Cloud Pub/Sub
                            (raw bucket)        (transaction-events)
                                   │                     │
                                   ↓                     ↓
                             Dataflow               Dataflow
                           (Batch Pipeline)     (Streaming Pipeline)
                                   │                     │
                                   └──────────┬──────────┘
                                              ↓
                                         BigQuery
                                              │
                          ┌───────────────────┼────────────────────┐
                          ↓                   ↓                    ↓
                        raw               staging               core
                    (landing zone)    (validated+typed)   (dim_* + fact_*)
                          │                   │                    │
                          └───────────────────┼────────────────────┘
                                              ↓
                                         analytics
                                    (risk marts + KPIs)
                                              │
                                       Looker Studio
                                              │
                                       Business Users

  ─────────────────────────────────────────────────────────────────────────
  Orchestration  │  Cloud Composer (Airflow 2.8+)  — all batch DAGs
  IaC            │  Terraform 1.7+                 — all GCP resources
  CI/CD          │  GitHub Actions                 — lint → test → deploy
  Observability  │  Cloud Monitoring + Cloud Logging
  Secrets        │  Cloud Secret Manager
  ML (Phase 13)  │  XGBoost fraud model → Vertex AI / Cloud Run
  ─────────────────────────────────────────────────────────────────────────
```

---

## Key Features

### 🔄 Batch Pipeline
- Incremental extraction from PostgreSQL using watermarks
- Dataflow batch jobs for scalable transformation
- BigQuery layered architecture: `raw → staging → core → analytics`
- SCD Type 2 merge for slowly changing dimensions

### ⚡ Streaming Pipeline
- Cloud Run API → Pub/Sub → Dataflow → BigQuery
- 5-minute fixed windows for velocity feature calculation
- Watermarks and allowed lateness for out-of-order event handling
- Dead-letter queue for unprocessable messages

### 🚨 Risk Scoring Engine
Seven weighted signals produce a composite score 0–100:

| Signal | Weight | Trigger |
|---|---|---|
| High amount | 25 | > 3× customer 30-day average |
| New device | 20 | Device age < 24 hours |
| Geo anomaly | 20 | Country not in customer's top-3 |
| Velocity burst | 15 | > 5 txns in 5 minutes |
| Repeated failures | 10 | > 2 failures in 1 hour |
| High-risk merchant | 7 | Merchant risk_category = 'high' |
| Off-hours | 3 | Transaction at 00:00–05:00 local time |

```
Score 0–30   → LOW risk    (no action)
Score 31–70  → MEDIUM risk (monitoring)
Score 71–100 → HIGH risk   (flag for review)
```

### ✅ Data Quality Framework
- **Completeness**: No nulls in primary key / critical columns
- **Uniqueness**: No duplicate `transaction_id`
- **Referential integrity**: All FKs resolve
- **Validity**: Amount > 0, timestamp ≤ current time, valid currency codes
- **Freshness**: Latest record within expected ingestion window
- **Volume anomaly**: Alert if daily volume deviates > 20% from baseline
- All errors written to `staging.data_quality_errors` for investigation

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Language | Python 3.11 | All pipeline and application code |
| Batch/Stream Processing | Apache Beam 2.55+ | Unified batch + stream programming model |
| Batch Runner | Dataflow (GCP) / DirectRunner (local) | Scalable distributed processing |
| Orchestration | Apache Airflow 2.8+ (Astronomer CLI) | DAG-based workflow management |
| Data Warehouse | Google BigQuery | Analytical storage and query |
| Object Storage | Google Cloud Storage | Raw data lake + staging area |
| Messaging | Google Cloud Pub/Sub (+ emulator locally) | Event streaming |
| Serving API | FastAPI + Cloud Run | Transaction ingestion endpoint |
| IaC | Terraform 1.7+ | GCP resource provisioning |
| Containers | Docker + Artifact Registry | Reproducible deployments |
| CI/CD | GitHub Actions | Automated lint, test, build, deploy |
| Data Quality | Great Expectations 0.18+ | Automated data validation |
| Data Generation | Faker + NumPy | Realistic synthetic data |
| Local Database | PostgreSQL 16 (Docker) | OLTP source simulation |
| ML (Phase 13) | XGBoost + Vertex AI | Fraud classification model |
| Dashboard | Looker Studio | Executive and operational dashboards |
| Monitoring | Cloud Monitoring + Cloud Logging | Infra + data health |
| Secrets | Cloud Secret Manager | Zero plaintext credentials |
| Linting | Ruff | Python linting + formatting |
| Type checking | Mypy | Static type analysis |
| Testing | pytest + apache-beam[test] | Unit, integration, DQ tests |

---

## Repository Structure

```
financial-risk-fraud-platform/
│
├── README.md                        ← you are here
├── LICENSE
├── .gitignore
├── .env.example                     ← copy to .env and configure
├── pyproject.toml                   ← all Python deps + tool config
├── docker-compose.yml               ← PostgreSQL + Pub/Sub emulator
│
├── docs/
│   ├── architecture.md              ← full architecture narrative
│   ├── data-model.md                ← star schema + SCD2 design
│   ├── data-dictionary.md           ← every field defined
│   ├── deployment.md                ← local + GCP deployment guide
│   ├── monitoring.md                ← observability design
│   ├── cost-optimization.md         ← BQ optimization experiments
│   ├── resume-bullets.md            ← (Phase 12)
│   └── interview-prep.md            ← (Phase 12)
│
├── data/
│   ├── schemas/                     ← JSON Schema for all entities
│   ├── sample/                      ← small CSV samples (committed)
│   ├── raw/                         ← generated data (gitignored)
│   └── generators/                  ← synthetic data generators
│       ├── generate_customers.py
│       ├── generate_accounts.py
│       ├── generate_merchants.py
│       ├── generate_devices.py
│       ├── generate_transactions.py
│       ├── generate_transaction_events.py
│       ├── generate_streaming_events.py
│       └── generate_all.py
│
├── ingestion/
│   ├── batch/                       ← PostgreSQL → GCS extraction
│   └── api/                         ← Cloud Run HTTP ingestion service
│
├── pipelines/
│   ├── batch/                       ← Beam batch pipeline (Dataflow)
│   └── streaming/                   ← Beam streaming pipeline (Dataflow)
│
├── transformations/
│   ├── dimensions/                  ← dim_* transformation SQL + Python
│   ├── facts/                       ← fact_* transformation
│   └── marts/                       ← analytics mart queries
│
├── data_quality/                    ← Great Expectations suites + checks
│
├── fraud/
│   ├── rules/                       ← deterministic rule engine
│   └── risk_scoring/                ← composite risk score calculator
│
├── airflow/
│   └── dags/                        ← all Airflow DAGs
│
├── infrastructure/
│   ├── postgres/                    ← PostgreSQL schema (local)
│   └── terraform/                   ← all Terraform configs (GCP)
│
├── cloud_run/                       ← FastAPI ingestion API
│
├── docker/                          ← Dockerfiles
│
├── tests/
│   ├── unit/                        ← pytest unit tests
│   ├── integration/                 ← integration tests (Pub/Sub, BQ)
│   └── data_quality/               ← DQ test suite
│
├── monitoring/                      ← Cloud Monitoring configs
│
└── .github/
    └── workflows/                   ← GitHub Actions CI/CD pipelines
```

---

## Quick Start (Local)

### Prerequisites
- Python 3.11+, Docker Desktop, git

### Setup
```bash
# 1. Clone
git clone https://github.com/iamdpsingh/Project-4-Financial-Risk-Fraud-Detection-Data-Platform.git
cd Project-4-Financial-Risk-Fraud-Detection-Data-Platform

# 2. Virtual environment
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 3. Configure
cp .env.example .env   # edit with your values

# 4. Start local services
docker compose up -d

# 5. Generate synthetic data
python data/generators/generate_all.py

# 6. Run local batch pipeline
python pipelines/batch/run_local.py

# 7. Run data quality checks
python data_quality/run_checks.py

# 8. Run tests
pytest tests/ -v
```

See [docs/deployment.md](docs/deployment.md) for full instructions.

---

## Data Model

The platform uses a **star schema** in BigQuery for analytical queries:

```
                   dim_date
                       │
                       │
dim_customer ─── fact_transaction ─── dim_merchant
                       │
                       │
                  dim_account
                       │
                  dim_device
```

**SCD Type 2** is implemented for `dim_customer` and `dim_merchant` to preserve historical attribute changes (e.g., customer risk_level promotions).

→ See [docs/data-model.md](docs/data-model.md) for full schema details.
→ See [docs/data-dictionary.md](docs/data-dictionary.md) for field-level definitions.

---

## Risk Scoring Engine

```
Transaction
     │
     ├── signal_high_amount        (weight: 25)
     ├── signal_new_device         (weight: 20)
     ├── signal_geo_anomaly        (weight: 20)
     ├── signal_velocity_burst     (weight: 15)
     ├── signal_repeated_failure   (weight: 10)
     ├── signal_high_risk_merchant (weight:  7)
     └── signal_off_hours          (weight:  3)
                │
                ↓
          Risk Score (0–100)
                │
       ┌────────┼──────────┐
       ↓        ↓          ↓
      LOW    MEDIUM      HIGH
    (0–30)  (31–70)   (71–100)
```

→ See [fraud/risk_scoring/](fraud/risk_scoring/) for implementation.

---

## Streaming Pipeline

```
Application / Generator
         ↓
   Cloud Run API (POST /transactions)
         ↓
    Cloud Pub/Sub
    (transaction-events topic)
         ↓
      Dataflow
    (Streaming Pipeline)
         │
         ├── Parse + Validate
         ├── Enrich (side inputs from BQ dims)
         ├── 5-min fixed windows (velocity features)
         ├── Watermark + 10-min allowed lateness
         ├── Risk score calculation
         └── Dead-letter routing
         │
         ↓
      BigQuery
  (fact_transaction + fact_transaction_event)
```

**Beam concepts demonstrated:**
- Fixed time windows
- Event time vs. processing time
- Watermarks and allowed lateness
- Side inputs for dimension enrichment
- Dead-letter pattern

---

## Data Quality Framework

Every pipeline stage runs Great Expectations checkpoints:

| Layer | Checks |
|---|---|
| `raw` | Schema validation, file completeness |
| `staging` | Null checks, type checks, uniqueness, referential integrity |
| `core` | Referential integrity to dims, amount ranges, timestamp sanity |
| `analytics` | Row count thresholds, freshness, score range validity |

Failed records are written to `staging.data_quality_errors` with full payload for investigation.

---

## GCP Deployment

```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars  # fill in billing account, org ID
terraform init && terraform apply
```

Terraform provisions: GCP project, APIs, service accounts, IAM, GCS, BigQuery, Pub/Sub, Artifact Registry, Cloud Monitoring.

→ See [docs/deployment.md](docs/deployment.md) for full guide.

---

## CI/CD

Every push triggers:

```
git push
    │
    ├── ruff lint
    ├── mypy type check
    ├── pytest (unit tests)
    ├── Docker image build
    ├── Push to Artifact Registry
    ├── terraform plan (validation)
    └── Cloud Run deploy (main branch only)
```

---

## Monitoring

- **Cloud Monitoring dashboards** for Dataflow, Pub/Sub, Cloud Run, Composer
- **Custom data metrics**: null rate, duplicate rate, freshness lag, volume deviation
- **Alerting policies**: Pub/Sub backlog > 10k, DQ errors > 100, pipeline failure
- **Log-based metrics** for pipeline errors and dead-letter messages

→ See [docs/monitoring.md](docs/monitoring.md).

---

## BigQuery Optimization

Key optimizations with measured results (Phase 11):

| Optimization | Impact |
|---|---|
| Partition `fact_transaction` by `transaction_date` | ~99% reduction in bytes scanned for date-filtered queries |
| Cluster by `customer_key`, `merchant_key` | Additional 60–80% reduction for customer/merchant filters |
| Incremental MERGE vs full reload | ~95% reduction in DML cost |
| Materialized views for daily aggregations | Eliminates repeated full-table aggregation |

→ See [docs/cost-optimization.md](docs/cost-optimization.md) for benchmarks.

---

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | Full technical architecture |
| [Data Model](docs/data-model.md) | Star schema, SCD2, table definitions |
| [Data Dictionary](docs/data-dictionary.md) | Every field defined |
| [Deployment Guide](docs/deployment.md) | Local + GCP step-by-step |
| [Monitoring](docs/monitoring.md) | Observability design |
| [Cost Optimization](docs/cost-optimization.md) | BQ optimization experiments |

---

## Build Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Business & system design, docs, repo scaffold | ✅ Complete |
| 2 | Data design + synthetic data generation | 🔄 In Progress |
| 3 | Local engineering (PostgreSQL, Beam DirectRunner, DQ) | ⏳ Planned |
| 4 | GCP foundation (Terraform) | ⏳ Planned |
| 5 | Batch platform (GCS → Dataflow → BigQuery) | ⏳ Planned |
| 6 | Streaming pipeline (Pub/Sub → Dataflow → BigQuery) | ⏳ Planned |
| 7 | Orchestration (Cloud Composer / Airflow) | ⏳ Planned |
| 8 | Fraud analytics (risk scoring, analytics marts) | ⏳ Planned |
| 9 | Production engineering (Docker, Cloud Run, CI/CD) | ⏳ Planned |
| 10 | Observability (Monitoring, alerts, runbooks) | ⏳ Planned |
| 11 | BigQuery optimization (partitioning, clustering, cost) | ⏳ Planned |
| 12 | Presentation (Looker Studio, README, interview prep) | ⏳ Planned |
| 13 | ML fraud model (XGBoost → Vertex AI / Cloud Run) | ⏳ Planned |

---

## Author

**Dhruv Pratap Singh** — Data Engineer

---

*Built with ❤️ as a production-grade portfolio project demonstrating end-to-end GCP data engineering.*
