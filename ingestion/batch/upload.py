"""
GCS Upload script for batch ingestion.

This script takes the extracted CSV files from PostgreSQL and uploads them
to Google Cloud Storage (GCS) where they act as the source for the Dataflow
batch pipeline.
"""

from utils.logger import get_logger
import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

log = get_logger("batch_upload")

# Make sure we load the env variables
load_dotenv(Path(__file__).parent.parent.parent / ".env")

try:
    from google.cloud import storage
except ImportError:
    log.warning("google-cloud-storage is not installed. Run: pip install google-cloud-storage")
    storage = None

def upload_to_gcs(bucket_name: str, source_folder: Path, destination_prefix: str = "raw/"):
    """
    Upload all CSV files in the source_folder to the specified GCS bucket.
    """
    if not storage:
        log.error("google-cloud-storage package missing. Cannot upload to GCS.")
        sys.exit(1)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
    except Exception as e:
        log.error("Failed to initialize GCS client: %s", e)
        log.error("Are your GCP credentials configured? Set GOOGLE_APPLICATION_CREDENTIALS or run 'gcloud auth application-default login'")
        sys.exit(1)
        
    if not source_folder.exists() or not source_folder.is_dir():
        log.error("Source folder %s does not exist or is not a directory.", source_folder)
        sys.exit(1)

    csv_files = list(source_folder.glob("*.csv"))
    if not csv_files:
        log.warning("No CSV files found in %s", source_folder)
        return

    for file_path in csv_files:
        # GCS path: raw/customers.csv
        blob_name = f"{destination_prefix}{file_path.name}"
        blob = bucket.blob(blob_name)
        
        log.info("Uploading %s to gs://%s/%s ...", file_path.name, bucket_name, blob_name)
        try:
            blob.upload_from_filename(str(file_path))
            log.info("  ✓ Upload complete")
        except Exception as e:
            log.error("  ✗ Upload failed: %s", e)

def main(args: argparse.Namespace):
    bucket_name = args.bucket or os.getenv("GCS_RAW_BUCKET")
    if not bucket_name:
        log.error("GCS bucket name must be provided via --bucket or GCS_RAW_BUCKET environment variable.")
        sys.exit(1)
        
    log.info("Starting GCS upload to bucket: %s", bucket_name)
    upload_to_gcs(bucket_name, args.source, args.prefix)
    log.info("GCS upload process finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload extracted data to Google Cloud Storage")
    parser.add_argument("--source", type=Path, default=Path("data/extracted"), help="Directory containing CSVs to upload")
    parser.add_argument("--bucket", type=str, help="GCS Bucket name")
    parser.add_argument("--prefix", type=str, default="raw/", help="Prefix (folder) in GCS")
    
    args = parser.parse_args()
    main(args)
