"""
Streaming Monitor DAG.

Unlike the batch pipeline, the streaming pipeline runs continuously on Dataflow.
This DAG doesn't trigger the pipeline; instead, it checks the health of the 
Dataflow job periodically to ensure it hasn't crashed.
"""

import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG


def check_streaming_health():
    """
    Checks if the streaming Dataflow job is running.
    In a real implementation, this would use the Google Cloud Dataflow API
    to verify the job state is RUNNING.
    """
    logging.info("Checking health of the streaming Dataflow job...")
    # Mock implementation
    logging.info("Streaming job is healthy.")
    return True

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "streaming_monitor",
    default_args=default_args,
    description="Monitors the continuous streaming Dataflow job",
    schedule=timedelta(hours=1), # Run health check every hour
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["financial_risk", "streaming"],
) as dag:

    monitor_task = PythonOperator(
        task_id="check_dataflow_status",
        python_callable=check_streaming_health
    )
