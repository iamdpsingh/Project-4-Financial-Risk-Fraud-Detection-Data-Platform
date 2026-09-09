# Deployment Guide — Financial Risk & Fraud Detection Platform

> This guide covers local development setup, followed by full GCP deployment. Follow sections in order.

---

## Prerequisites

### Required tools
| Tool | Version | Install |
|---|---|---|
| Python | 3.12+ | Pre-installed on macOS or `brew install python@3.12` |
| Docker Desktop | Latest | docker.com/products/docker-desktop |
| gcloud CLI | Latest | `brew install google-cloud-sdk` |
| Terraform | 1.7+ | `brew install terraform` |
| Astronomer CLI | Latest | `brew install astro` |
| git | Any | Pre-installed on macOS |

---

## Part 1 — Local Development Setup

### Step 1: Clone the repository

```bash
git clone https://github.com/iamdpsingh/Project-4-Financial-Risk-Fraud-Detection-Data-Platform.git
cd Project-4-Financial-Risk-Fraud-Detection-Data-Platform
```

### Step 2: Create Python virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate          # macOS/Linux
# .venv\Scripts\activate           # Windows

pip install --upgrade pip
pip install -e ".[dev]"
```

### Step 3: Configure environment

```bash
cp .env.example .env
# Edit .env with your local values (PostgreSQL password, etc.)
```

### Step 4: Start local services

```bash
docker compose up -d
# Starts: PostgreSQL (5432), Pub/Sub emulator (8085), Adminer UI (8080)

# Verify services
docker compose ps
docker compose logs postgres
```

Access Adminer DB admin at: http://localhost:8080
- System: PostgreSQL
- Server: postgres
- Username: fraud_user
- Password: (from your .env)
- Database: financial_risk

### Step 5: Generate synthetic data

```bash
python data/generators/generate_all.py \
  --customers 10000 \
  --merchants 2000 \
  --transactions 500000

# Output: data/raw/<entity>/*.csv
# Also loads into PostgreSQL automatically
```

### Step 6: Run local batch pipeline

```bash
python pipelines/batch/run_local.py \
  --date $(date +%Y-%m-%d) \
  --runner DirectRunner

# Output: data/processed/*.parquet
```

### Step 7: Run data quality checks

```bash
python data_quality/run_checks.py \
  --suite all \
  --environment local

# Opens Great Expectations Data Docs at http://localhost:8888
```

### Step 8: Run tests

```bash
pytest tests/ -v --cov=. --cov-report=html
# Open htmlcov/index.html for coverage report
```

### Step 9: (Optional) Run local streaming simulation

```bash
# Terminal 1 — Pub/Sub emulator should already be running via docker compose

# Terminal 2 — Start the Dataflow streaming pipeline locally
python pipelines/streaming/run_local.py \
  --project local-project \
  --runner DirectRunner

# Terminal 3 — Start the event generator
python data/generators/generate_streaming_events.py \
  --rate 10 \         # 10 transactions/second
  --duration 300      # run for 5 minutes
```

---

## Part 2 — GCP Deployment

> ⚠️ Complete Part 1 (local) before attempting GCP deployment.

### Step 1: Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
```

### Step 2: Provision infrastructure with Terraform

```bash
cd infrastructure/terraform

# Review and set variables
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your billing account, org ID, etc.

terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

This creates:
- GCP project (`financial-risk-platform-<random-suffix>`)
- All GCP APIs enabled
- Service accounts with least-privilege IAM
- GCS bucket with folder structure + lifecycle policies
- BigQuery datasets (raw, staging, core, analytics)
- Pub/Sub topics and subscriptions
- Secret Manager secrets
- Artifact Registry repository
- Cloud Monitoring dashboards

### Step 3: Configure Airflow (Astronomer CLI)

```bash
cd airflow

astro dev init          # first time only
astro dev start         # starts local Airflow at http://localhost:8888

# For Cloud Composer deployment to GCP:
# See docs/cloud-composer-setup.md
```

### Step 4: Build and push Docker images

```bash
# Authenticate to Artifact Registry
gcloud auth configure-docker ${ARTIFACT_REGISTRY_REGION}-docker.pkg.dev

# Build and push ingestion API
docker build -t ${DOCKER_IMAGE_INGESTION_API}:latest ./docker/ingestion-api/
docker push ${DOCKER_IMAGE_INGESTION_API}:latest
```

### Step 5: Deploy Cloud Run

```bash
gcloud run deploy ${CLOUD_RUN_SERVICE_NAME} \
  --image=${DOCKER_IMAGE_INGESTION_API}:latest \
  --region=${CLOUD_RUN_REGION} \
  --service-account=cloudrun-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com \
  --no-allow-unauthenticated \
  --set-secrets="PUBSUB_TOPIC=PUBSUB_TOPIC_TRANSACTIONS:latest"
```

### Step 6: Upload historical data to GCS

```bash
python ingestion/batch/upload_to_gcs.py \
  --bucket=${GCS_BUCKET_NAME} \
  --date-range 2024-01-01:2024-12-31
```

### Step 7: Trigger Dataflow batch pipeline

```bash
python pipelines/batch/run_dataflow.py \
  --project=${GCP_PROJECT_ID} \
  --region=${GCP_REGION} \
  --date 2024-12-31
```

### Step 8: Trigger Dataflow streaming pipeline

```bash
python pipelines/streaming/run_dataflow.py \
  --project=${GCP_PROJECT_ID} \
  --region=${GCP_REGION} \
  --subscription=projects/${GCP_PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION_DATAFLOW}
```

---

## Part 3 — CI/CD (GitHub Actions)

Every push to `main` triggers:
1. Ruff lint check
2. Mypy type check
3. Pytest unit tests
4. Docker image build
5. Push to Artifact Registry (tagged with commit SHA)
6. `terraform plan` validation
7. Cloud Run deployment (on merge to `main` only)

See `.github/workflows/` for pipeline definitions.

---

## Environment Variables Reference

See [`.env.example`](../.env.example) for the full list of required environment variables.

---

## Troubleshooting

### PostgreSQL connection refused
```bash
docker compose ps         # check if postgres container is healthy
docker compose logs postgres
```

### Pub/Sub emulator not receiving messages
```bash
# Ensure PUBSUB_EMULATOR_HOST is set in your shell
export PUBSUB_EMULATOR_HOST=localhost:8085
echo $PUBSUB_EMULATOR_HOST
```

### Beam pipeline fails locally
```bash
# Check Python path and that you're in the venv
which python   # should point to .venv/bin/python
pip list | grep apache-beam
```

### Terraform errors
```bash
terraform validate        # check syntax
terraform plan            # preview changes
gcloud projects list      # verify project exists
```
