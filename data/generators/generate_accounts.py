"""
Account data generator.

Each customer gets between 1 and 3 accounts. The mix leans toward
checking and savings accounts, with credit and loan accounts being
less common. A small percentage of accounts are suspended or closed,
which is realistic for a real customer base.

Run directly:
    python data/generators/generate_accounts.py
    python data/generators/generate_accounts.py --count 20000

Note: this script needs the customer CSV to already exist, because
accounts are linked to customers by customer_id.
"""

from utils.logger import get_logger
import argparse
import csv
import logging
import random
from typing import Any
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from config import (
    NUM_ACCOUNTS,
    OUTPUT_DIR,
    RANDOM_SEED,
    SAMPLE_DIR,
    SAMPLE_SIZE,
)

log = get_logger(__name__)

ACCOUNT_TYPES = ["checking", "savings", "credit", "loan", "investment"]
ACCOUNT_TYPE_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]

ACCOUNT_STATUSES = ["active", "inactive", "suspended", "closed"]
ACCOUNT_STATUS_WEIGHTS = [0.80, 0.10, 0.05, 0.05]


def load_customer_ids(customers_csv: Path) -> list[str]:
    """Read customer IDs from the previously generated customer CSV."""
    if not customers_csv.exists():
        raise FileNotFoundError(
            f"Customer data not found at {customers_csv}. "
            "Run generate_customers.py first."
        )
    with open(customers_csv, encoding="utf-8") as f:
        return [row["customer_id"] for row in csv.DictReader(f)]


def pick_currency_for_account(rng: random.Random) -> str:
    """
    Most accounts are in USD, but we include other currencies to reflect
    a global user base. The distribution is skewed heavily toward USD
    because that's how most international financial apps work.
    """
    return rng.choices(
        ["USD", "EUR", "GBP", "INR", "SGD", "AED", "CAD", "AUD"],
        weights=[0.50, 0.15, 0.10, 0.10, 0.05, 0.05, 0.03, 0.02],
        k=1,
    )[0]


def generate_accounts(
    customer_ids: list[str],
    target_count: int,
    seed: int,
) -> list[dict]:
    """
    Generate accounts linked to the provided customer IDs.

    We assign each customer a random number of accounts (1-3), then
    trim or pad the list to hit the target count. The distribution of
    account types and statuses mirrors what you'd expect from a real
    financial institution.
    """
    rng = random.Random(seed)
    accounts: list[dict[str, Any]] = []
    base_time = datetime(2018, 1, 1, tzinfo=UTC)

    # Shuffle customer IDs so the assignment of "how many accounts" is random.
    shuffled_customers = customer_ids.copy()
    rng.shuffle(shuffled_customers)

    for customer_id in shuffled_customers:
        if len(accounts) >= target_count:
            break

        # Give each customer 1-3 accounts.
        num_accounts = rng.choices([1, 2, 3], weights=[0.50, 0.35, 0.15], k=1)[0]

        for _ in range(num_accounts):
            if len(accounts) >= target_count:
                break

            account_type = rng.choices(ACCOUNT_TYPES, weights=ACCOUNT_TYPE_WEIGHTS, k=1)[0]
            status = rng.choices(ACCOUNT_STATUSES, weights=ACCOUNT_STATUS_WEIGHTS, k=1)[0]

            opened_at = base_time + timedelta(days=rng.randint(0, 5 * 365))

            # Closed accounts have a closed_at date after they were opened.
            closed_at = None
            if status == "closed":
                days_open = rng.randint(30, 3 * 365)
                closed_at = (opened_at + timedelta(days=days_open)).isoformat()

            accounts.append({
                "account_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "account_type": account_type,
                "account_status": status,
                "currency": pick_currency_for_account(rng),
                "opened_at": opened_at.isoformat(),
                "closed_at": closed_at or "",
                "created_at": opened_at.isoformat(),
                "updated_at": opened_at.isoformat(),
            })

    log.info("Generated %d accounts", len(accounts))
    return accounts


def save_to_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info("Saved %d records → %s", len(records), path)


def main(count: int, output_dir: Path) -> list[dict]:
    customers_csv = output_dir / "customers" / "customers.csv"
    customer_ids = load_customer_ids(customers_csv)
    log.info("Loaded %d customer IDs", len(customer_ids))

    accounts = generate_accounts(customer_ids, count, RANDOM_SEED)
    save_to_csv(accounts, output_dir / "accounts" / "accounts.csv")
    save_to_csv(accounts[:SAMPLE_SIZE], SAMPLE_DIR / "accounts_sample.csv")
    return accounts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic account data")
    parser.add_argument("--count", type=int, default=NUM_ACCOUNTS)
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    main(count=args.count, output_dir=args.output)
