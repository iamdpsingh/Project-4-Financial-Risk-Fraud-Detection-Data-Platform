"""
Streaming event generator.

This script continuously emits transaction events to either stdout or a
Pub/Sub topic. It's used when you want to run the streaming pipeline locally
and watch transactions flow through in real time.

You can control the emission rate (transactions per second) and how long
it runs. The events are slightly randomised so you see a realistic mix of
normal and suspicious transactions, not a perfectly uniform stream.

Usage:
    # Print to stdout (useful for debugging)
    python data/generators/generate_streaming.py --rate 5 --duration 60

    # Publish to local Pub/Sub emulator
    python data/generators/generate_streaming.py --rate 10 --duration 300 --pubsub

    # Publish to real GCP Pub/Sub
    python data/generators/generate_streaming.py --rate 10 --project my-project --pubsub
"""

from utils.logger import get_logger
import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from config import OUTPUT_DIR, RANDOM_SEED

log = get_logger(__name__)


def load_reference_data(output_dir: Path) -> tuple[list, list, list, list]:
    """Load the pre-generated reference data so streaming events reference real IDs."""
    import csv

    def load_csv(path: Path) -> list[dict]:
        if not path.exists():
            log.error("Missing: %s — run generate_all.py first", path)
            sys.exit(1)
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))

    customers = load_csv(output_dir / "customers" / "customers.csv")
    accounts = load_csv(output_dir / "accounts" / "accounts.csv")
    merchants = load_csv(output_dir / "merchants" / "merchants.csv")
    devices = load_csv(output_dir / "devices" / "devices.csv")

    log.info(
        "Reference data loaded: %d customers, %d accounts, %d merchants, %d devices",
        len(customers), len(accounts), len(merchants), len(devices)
    )
    return customers, accounts, merchants, devices


def build_event(
    customers: list[dict],
    accounts: list[dict],
    merchants: list[dict],
    devices: list[dict],
    rng: random.Random,
) -> dict:
    """
    Build a single streaming transaction event.

    The event format matches what the Dataflow streaming pipeline expects.
    We include enough fields so the pipeline can validate, enrich, and score
    it without needing to join against the full historical dataset immediately.
    """
    customer = rng.choice(customers)
    merchant = rng.choice(merchants)

    # Pick an account and device that belong to this customer if we can.
    customer_accounts = [a for a in accounts if a["customer_id"] == customer["customer_id"]]
    customer_devices = [d for d in devices if d["customer_id"] == customer["customer_id"]]

    account = rng.choice(customer_accounts) if customer_accounts else rng.choice(accounts)
    device = rng.choice(customer_devices) if customer_devices else rng.choice(devices)

    # A small fraction of streaming events will be obviously suspicious.
    # 2% chance of a geo anomaly in live traffic.
    is_geo_anomaly = rng.random() < 0.02
    txn_country = rng.choice(["RU", "CN", "NG"]) if is_geo_anomaly else customer["country"]

    amount = round(rng.uniform(5, 2000), 2)
    now = datetime.now(UTC)

    return {
        "event_id": str(uuid.uuid4()),
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "account_id": account["account_id"],
        "merchant_id": merchant["merchant_id"],
        "device_id": device["device_id"],
        "merchant_category": merchant["merchant_category"],
        "merchant_risk_category": merchant["risk_category"],
        "transaction_timestamp": now.isoformat(),
        "amount": amount,
        "currency": "USD",
        "amount_usd": amount,
        "transaction_type": rng.choice(["purchase", "transfer", "payment"]),
        "country": txn_country,
        "city": customer["city"],
        "payment_method": rng.choice(["card", "wallet", "upi"]),
        "customer_country": customer["country"],
        "is_international": str(txn_country != customer["country"]).lower(),
        "emitted_at": now.isoformat(),
    }


def emit_to_stdout(event: dict) -> None:
    """Print the event as a JSON line — useful for local debugging."""
    print(json.dumps(event))


def emit_to_pubsub(event: dict, publisher, topic_path: str) -> None:
    """Publish the event to Pub/Sub. The message data is JSON-encoded bytes."""
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    future.result()  # wait for the publish to complete


def main(
    rate: float,
    duration: int,
    use_pubsub: bool,
    project: str,
    topic: str,
    output_dir: Path,
    seed: int,
) -> None:
    rng = random.Random(seed)
    customers, accounts, merchants, devices = load_reference_data(output_dir)

    publisher = None
    topic_path: str | None = None

    if use_pubsub:
        from google.cloud import pubsub_v1  # type: ignore
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project, topic)
        log.info("Publishing to Pub/Sub topic: %s", topic_path)
    else:
        log.info("Printing events to stdout (--pubsub flag not set)")

    interval = 1.0 / rate  # seconds between events
    end_time = time.time() + duration
    emitted = 0

    log.info("Emitting at %.1f events/second for %d seconds...", rate, duration)

    while time.time() < end_time:
        event = build_event(customers, accounts, merchants, devices, rng)
        if use_pubsub and publisher and topic_path:
            emit_to_pubsub(event, publisher, topic_path)
        else:
            emit_to_stdout(event)

        emitted += 1
        if emitted % 100 == 0:
            log.info("  Emitted %d events", emitted)

        time.sleep(interval)

    log.info("Done. Emitted %d events total.", emitted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream synthetic transaction events to Pub/Sub or stdout")
    parser.add_argument("--rate", type=float, default=5.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=300, help="How long to run (seconds)")
    parser.add_argument("--pubsub", action="store_true", help="Publish to Pub/Sub instead of stdout")
    parser.add_argument("--project", type=str, default=os.getenv("GCP_PROJECT_ID", "local-project"))
    parser.add_argument("--topic", type=str, default=os.getenv("PUBSUB_TOPIC_TRANSACTIONS", "transaction-events"))
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR, help="Directory containing reference data CSVs")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    args = parser.parse_args()

    main(
        rate=args.rate,
        duration=args.duration,
        use_pubsub=args.pubsub,
        project=args.project,
        topic=args.topic,
        output_dir=args.output,
        seed=args.seed,
    )
