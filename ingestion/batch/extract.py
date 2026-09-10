"""
PostgreSQL extraction script for batch ingestion.

This script extracts data from PostgreSQL tables and saves it as CSV files.
It supports incremental extraction using a high-water mark approach on the
`updated_at` column, ensuring we only pull new or modified records.
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(name)s]  %(message)s")
log = logging.getLogger("batch_extract")

# Make sure we load the env variables for Postgres credentials
load_dotenv(Path(__file__).parent.parent.parent / ".env")

TABLES = [
    "customers",
    "accounts",
    "merchants",
    "devices",
    "transactions",
    "transaction_events"
]

def get_db_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "financial_risk"),
            user=os.getenv("POSTGRES_USER", "fraud_user"),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )
    except Exception as e:
        log.error("Could not connect to PostgreSQL: %s", e)
        sys.exit(1)

def extract_table(conn, table: str, output_dir: Path, last_updated_at: str = None):
    """
    Extract a table to CSV. If last_updated_at is provided, only extracts
    records where updated_at (or created_at for immutable tables) > last_updated_at.
    """
    cur = conn.cursor()
    
    # Determine the watermark column
    # For transactions and transaction_events, we use transaction_timestamp / event_timestamp or created_at
    # The others have updated_at.
    watermark_col = "updated_at"
    if table == "transactions":
        watermark_col = "transaction_timestamp"
    elif table == "transaction_events":
        watermark_col = "event_timestamp"

    query = f"SELECT * FROM {table}"
    if last_updated_at:
        query += f" WHERE {watermark_col} > '{last_updated_at}'"
    
    log.info("Extracting %s with query: %s", table, query)
    
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    
    output_path = output_dir / f"{table}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        
        count = 0
        # Use fetchmany to avoid loading entire table into RAM
        while True:
            rows = cur.fetchmany(10000)
            if not rows:
                break
            writer.writerows(rows)
            count += len(rows)
            
    log.info("Extracted %d rows to %s", count, output_path)
    cur.close()

def main(args: argparse.Namespace):
    output_dir = args.output
    last_updated_at = args.since
    
    log.info("Starting batch extraction...")
    conn = get_db_connection()
    
    for table in TABLES:
        extract_table(conn, table, output_dir, last_updated_at)
        
    conn.close()
    log.info("Batch extraction complete. Files saved to: %s", output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from PostgreSQL to CSV")
    parser.add_argument("--output", type=Path, default=Path("data/extracted"), help="Directory to save CSVs")
    parser.add_argument("--since", type=str, help="Extract records updated after this ISO timestamp (e.g. 2024-01-01T00:00:00)")
    
    args = parser.parse_args()
    main(args)
