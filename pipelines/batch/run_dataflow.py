"""
Runner script for the Batch Dataflow pipeline.

Executes the pipeline on Google Cloud Dataflow instead of the local DirectRunner.
"""

import logging
import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(name)s]  %(message)s")
log = logging.getLogger("batch_run_dataflow")

def run():
    repo_root = Path(__file__).parent.parent.parent
    load_dotenv(repo_root / ".env")
    
    # Read environment variables set by Terraform or manually
    project_id = os.getenv("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
    region = os.getenv("GCP_REGION", "us-central1")
    bucket_prefix = os.getenv("GCS_BUCKET_PREFIX", "fr-data-platform-999")
    
    input_dir = f"gs://{bucket_prefix}-raw-data"
    temp_location = f"gs://{bucket_prefix}-dataflow-temp/batch/temp"
    staging_location = f"gs://{bucket_prefix}-dataflow-temp/batch/staging"
    
    pipeline_script = repo_root / "pipelines" / "batch" / "pipeline.py"
    
    cmd = [
        sys.executable, str(pipeline_script),
        "--runner=DataflowRunner",
        f"--project={project_id}",
        f"--region={region}",
        f"--temp_location={temp_location}",
        f"--staging_location={staging_location}",
        "--setup_file=./setup.py",
        f"--input_dir={input_dir}",
        "--dataset=staging",
        "--job_name=batch-ingestion-pipeline"
    ]
    
    log.info("Submitting batch pipeline to Google Cloud Dataflow...")
    log.info(f"Command: {' '.join(cmd)}")
    
    if project_id == "YOUR_GCP_PROJECT_ID":
        log.warning("GCP_PROJECT_ID not set. Mocking submission. To actually submit, configure your .env file.")
        return
        
    try:
        # We don't block and wait for Dataflow to finish synchronously unless we want to hold the Airflow worker
        # Usually, Dataflow jobs are submitted asynchronously and Airflow tracks them via Dataflow sensors,
        # but for this script we will block until the job is submitted.
        subprocess.run(cmd, check=True)
        log.info("Dataflow batch job submitted successfully.")
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to submit Dataflow job: {e}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    run()
