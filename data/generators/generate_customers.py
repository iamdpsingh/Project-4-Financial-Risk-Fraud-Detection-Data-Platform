"""
Customer data generator.

Produces realistic customer records and saves them to a CSV file.
Customers are distributed across countries according to the weights
in config.py, so you get a mix that looks like a real global app —
most users from the US and India, smaller numbers from Germany, the UK, etc.

Run directly:
    python data/generators/generate_customers.py
    python data/generators/generate_customers.py --count 5000 --output custom_dir/
"""

import argparse
import csv
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
from config import (
    CUSTOMER_COUNTRIES,
    NUM_CUSTOMERS,
    OUTPUT_DIR,
    RANDOM_SEED,
    SAMPLE_DIR,
    SAMPLE_SIZE,
)
from faker import Faker

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def seeded_uuid(rng: random.Random) -> str:
    """Generate a UUID using the seeded RNG so outputs are reproducible."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def pick_country(rng: random.Random) -> str:
    """Pick a country code using the configured probability weights."""
    countries = list(CUSTOMER_COUNTRIES.keys())
    weights = list(CUSTOMER_COUNTRIES.values())
    return rng.choices(countries, weights=weights, k=1)[0]


def pick_segment(rng: random.Random) -> str:
    """
    Assign a customer segment. Most people are retail, a smaller slice
    are premium or business, and VIP is rare.
    """
    return rng.choices(
        ["retail", "premium", "business", "vip", "student"],
        weights=[0.55, 0.20, 0.15, 0.05, 0.05],
        k=1,
    )[0]


def random_dob(rng: random.Random) -> str:
    """Generate a date of birth for someone between 18 and 75 years old."""
    today = datetime.now(UTC).date()
    days_offset = rng.randint(18 * 365, 75 * 365)
    dob = today - timedelta(days=days_offset)
    return dob.isoformat()


def generate_customers(count: int, seed: int) -> list[dict]:
    """
    Generate `count` customer records.

    Each customer gets a stable UUID, a fake name and email from Faker,
    a country drawn from the weighted distribution, and a random segment.
    We use a seeded RNG so the same seed always produces the same dataset —
    useful for reproducibility when running pipelines against local test data.
    """
    rng = random.Random(seed)
    np.random.seed(seed)

    # Faker supports locale-specific names, but we keep it as en_US here
    # because we handle the country distribution ourselves.
    fake = Faker()
    Faker.seed(seed)

    customers = []
    base_time = datetime(2020, 1, 1, tzinfo=UTC)

    for i in range(count):
        country = pick_country(rng)
        created_delta = timedelta(days=rng.randint(0, 4 * 365))
        created_at = base_time + created_delta

        # updated_at is either the same as created_at or sometime after
        updated_delta = timedelta(days=rng.randint(0, (datetime.now(UTC) - created_at).days or 1))
        updated_at = created_at + updated_delta

        customers.append({
            "customer_id": seeded_uuid(rng),
            "name": fake.name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number()[:30],
            "country": country,
            "city": fake.city()[:100],
            "customer_segment": pick_segment(rng),
            "date_of_birth": random_dob(rng),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        })

        if (i + 1) % 1000 == 0:
            log.info("Generated %d / %d customers", i + 1, count)

    return customers


def save_to_csv(records: list[dict], path: Path) -> None:
    """Write records to a CSV file, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info("Saved %d records → %s", len(records), path)


def main(count: int, output_dir: Path) -> list[dict]:
    log.info("Generating %d customers (seed=%d)", count, RANDOM_SEED)
    customers = generate_customers(count, RANDOM_SEED)

    # Full dataset
    save_to_csv(customers, output_dir / "customers" / "customers.csv")

    # Small sample for the repo (committed to git so reviewers can see the shape)
    save_to_csv(customers[:SAMPLE_SIZE], SAMPLE_DIR / "customers_sample.csv")

    return customers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic customer data")
    parser.add_argument("--count", type=int, default=NUM_CUSTOMERS, help="Number of customers to generate")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    main(count=args.count, output_dir=args.output)
