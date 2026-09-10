"""
Runner script for the Streaming Dataflow pipeline.

Executes the pipeline on Google Cloud Dataflow for continuous streaming processing.
"""

import logging
import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(name)s]  %(message)s")
log = logging.getLogger("streaming_run_dataflow")

def run():
    repo_root = Path(__file__).parent.parent.parent
    load_dotenv(repo_root / ".env")
    
    project_id = os.getenv("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
    region = os.getenv("GCP_REGION", "us-central1")
    bucket_prefix = os.getenv("GCS_BUCKET_PREFIX", "fr-data-platform-999")
    
    temp_location = f"gs://{bucket_prefix}-dataflow-temp/streaming/temp"
    staging_location = f"gs://{bucket_prefix}-dataflow-temp/streaming/staging"
    input_subscription = f"projects/{project_id}/subscriptions/transaction-events-sub"
    
    pipeline_script = repo_root / "pipelines" / "streaming" / "pipeline.py"
    
    cmd = [
        sys.executable, str(pipeline_script),
        "--runner=DataflowRunner",
        f"--project={project_id}",
        f"--region={region}",
        f"--temp_location={temp_location}",
        f"--staging_location={staging_location}",
        "--setup_file=./setup.py",
        f"--input_subscription={input_subscription}",
        "--dataset=analytics",
        "--job_name=streaming-risk-engine"
    ]
    
    log.info("Submitting streaming pipeline to Google Cloud Dataflow...")
    log.info(f"Command: {' '.join(cmd)}")
    
    if project_id == "YOUR_GCP_PROJECT_ID":
        log.warning("GCP_PROJECT_ID not set. Mocking submission.")
        return
        
    try:
        # Since this is a streaming pipeline, it runs continuously.
        # We launch it asynchronously. The command will return once the job is successfully queued.
        subprocess.run(cmd, check=True)
        log.info("Dataflow streaming job submitted successfully.")
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to submit streaming Dataflow job: {e}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    run()
