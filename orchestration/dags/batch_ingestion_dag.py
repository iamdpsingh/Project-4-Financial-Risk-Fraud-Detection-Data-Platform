"""
Batch Ingestion DAG.

This DAG runs daily to:
1. Extract incremental data from PostgreSQL (locally or Cloud SQL)
2. Upload the extracted CSVs to Google Cloud Storage
3. Trigger a Dataflow batch job to parse, validate, and load the data into BigQuery
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
# For this project, we'll trigger Dataflow via a bash script to reuse our python run_dataflow.py

# Project root relative to this DAG file (assuming airflow runs from repo root or similar)
# In production Composer, we'd package our scripts properly or use Dataflow operators.
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow/dags/repo")
VENV_PYTHON = os.environ.get("VENV_PYTHON", "python")

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "batch_ingestion_pipeline",
    default_args=default_args,
    description="Extracts data from Postgres and loads to BigQuery via Dataflow",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["financial_risk", "batch"],
) as dag:

    # 1. Extract from PostgreSQL
    extract_task = BashOperator(
        task_id="extract_from_postgres",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ingestion/batch/extract.py --output data/extracted"
    )

    # 2. Upload to GCS
    upload_task = BashOperator(
        task_id="upload_to_gcs",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} ingestion/batch/upload.py --source data/extracted"
    )

    # 3. Run Dataflow Batch Job
    # We use our custom runner script which configures the pipeline options
    run_dataflow_task = BashOperator(
        task_id="run_dataflow_job",
        bash_command=f"cd {PROJECT_ROOT} && {VENV_PYTHON} pipelines/batch/run_dataflow.py"
    )

    extract_task >> upload_task >> run_dataflow_task
