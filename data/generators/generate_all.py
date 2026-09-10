"""
Master data generation script.

Runs all generators in dependency order:
    customers → accounts → merchants → devices → transactions → events

Then optionally loads everything into the local PostgreSQL database.

This is the single command you run when setting up the project from scratch.
After this finishes, PostgreSQL will be fully populated and all CSVs will
be sitting in data/raw/ ready for the batch pipeline.

Usage:
    python data/generators/generate_all.py
    python data/generators/generate_all.py --skip-postgres
    python data/generators/generate_all.py --customers 1000 --transactions 50000
"""

from utils.logger import get_logger
import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Make sure the generators directory is in the path so we can import config.
sys.path.insert(0, str(Path(__file__).parent))

import generate_accounts
import generate_customers
import generate_devices
import generate_events
import generate_merchants
import generate_transactions
from config import (
    NUM_ACCOUNTS,
    NUM_CUSTOMERS,
    NUM_DEVICES,
    NUM_MERCHANTS,
    NUM_TRANSACTIONS,
    OUTPUT_DIR,
    SAMPLE_DIR,
)

# Configure logging to write to both console and a file in the logs/ directory
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

log = get_logger("generate_all")


def ensure_directories(output_dir: Path) -> None:
    """Create all the output directories upfront so generators don't have to worry about it."""
    for subdir in ["customers", "accounts", "merchants", "devices", "transactions", "events"]:
        (output_dir / subdir).mkdir(parents=True, exist_ok=True)
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Output directories ready at %s", output_dir)


def load_into_postgres(output_dir: Path) -> None:
    """
    Load all generated CSVs into PostgreSQL using psycopg2 COPY.

    COPY is the fastest way to bulk-load data into PostgreSQL — much faster
    than running individual INSERT statements for hundreds of thousands of rows.
    We load in dependency order so foreign key constraints are satisfied.
    """
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname": os.getenv("POSTGRES_DB", "financial_risk"),
        "user": os.getenv("POSTGRES_USER", "fraud_user"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }

    log.info("Connecting to PostgreSQL at %s:%s", conn_params["host"], conn_params["port"])

    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as e:
        log.error("Could not connect to PostgreSQL: %s", e)
        log.error("Is the database running? Try: docker compose up -d")
        return

    # Load order matters — child tables must come after parent tables.
    load_plan = [
        ("customers",           "customers/customers.csv",            "customer_id,name,email,phone,country,city,customer_segment,date_of_birth,created_at,updated_at"),
        ("accounts",            "accounts/accounts.csv",              "account_id,customer_id,account_type,account_status,currency,opened_at,closed_at,created_at,updated_at"),
        ("merchants",           "merchants/merchants.csv",            "merchant_id,merchant_name,merchant_category,mcc_code,country,city,risk_category,created_at,updated_at"),
        ("devices",             "devices/devices.csv",                "device_id,customer_id,device_type,os,browser,ip_address,user_agent,first_seen_at,last_seen_at,created_at"),
        ("transactions",        "transactions/transactions.csv",      "transaction_id,customer_id,account_id,merchant_id,device_id,transaction_timestamp,amount,currency,amount_usd,transaction_type,country,city,payment_method,status,ip_address,is_international"),
        ("transaction_events",  "events/transaction_events.csv",      "event_id,transaction_id,event_type,event_timestamp,event_metadata,created_at"),
    ]

    for table, csv_rel_path, columns in load_plan:
        csv_path = output_dir / csv_rel_path
        if not csv_path.exists():
            log.warning("Skipping %s — CSV not found at %s", table, csv_path)
            continue

        log.info("Loading %s...", table)
        start = time.time()

        try:
            with open(csv_path, encoding="utf-8") as f:
                # Skip the header row since we specify columns explicitly.
                next(f)
                cur.copy_expert(
                    f"COPY {table} ({columns}) FROM STDIN WITH CSV NULL ''",
                    f,
                )
            conn.commit()
            elapsed = time.time() - start
            log.info("  ✓ %s loaded in %.1fs", table, elapsed)
        except Exception as e:
            conn.rollback()
            log.error("  ✗ Failed to load %s: %s", table, e)

    cur.close()
    conn.close()
    log.info("PostgreSQL load complete.")


def main(args: argparse.Namespace) -> None:
    start_total = time.time()
    output_dir = args.output

    ensure_directories(output_dir)

    steps = [
        ("Customers",           lambda: generate_customers.main(args.customers, output_dir)),
        ("Accounts",            lambda: generate_accounts.main(args.accounts, output_dir)),
        ("Merchants",           lambda: generate_merchants.main(args.merchants, output_dir)),
        ("Devices",             lambda: generate_devices.main(args.devices, output_dir)),
        ("Transactions",        lambda: generate_transactions.main(args.transactions, output_dir)),
        ("Transaction events",  lambda: generate_events.main(output_dir)),
    ]

    for name, fn in steps:
        log.info("─── %s ───", name)
        step_start = time.time()
        fn()
        log.info("  Done in %.1fs", time.time() - step_start)

    if not args.skip_postgres:
        log.info("─── Loading into PostgreSQL ───")
        load_into_postgres(output_dir)
    else:
        log.info("Skipping PostgreSQL load (--skip-postgres set).")

    total = time.time() - start_total
    log.info("All done! Total time: %.1fs", total)
    log.info("CSVs are at: %s", output_dir)
    log.info("Samples are at: %s", SAMPLE_DIR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate all synthetic data and optionally load it into PostgreSQL"
    )
    parser.add_argument("--customers",    type=int, default=NUM_CUSTOMERS)
    parser.add_argument("--accounts",     type=int, default=NUM_ACCOUNTS)
    parser.add_argument("--merchants",    type=int, default=NUM_MERCHANTS)
    parser.add_argument("--devices",      type=int, default=NUM_DEVICES)
    parser.add_argument("--transactions", type=int, default=NUM_TRANSACTIONS)
    parser.add_argument("--output",       type=Path, default=OUTPUT_DIR)
    parser.add_argument("--skip-postgres", action="store_true",
                        help="Skip loading into PostgreSQL (just generate CSVs)")
    args = parser.parse_args()
    main(args)
