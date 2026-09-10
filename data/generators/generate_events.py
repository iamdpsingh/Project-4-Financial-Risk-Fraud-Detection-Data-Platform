"""
Transaction event generator.

Every transaction goes through a lifecycle. When a customer initiates a purchase,
a 'transaction_created' event fires. Then a 'payment_attempted' event. Then either
'payment_completed' or 'payment_failed'. If something fails and gets retried a few
times, you'll see multiple failure events before a success. Reversed transactions
get their own event too.

These events come in roughly chronological order, but not perfectly — a failure
event might arrive a few seconds before the success event in a streaming system.
That's intentional, and it lets us demonstrate watermarks and allowed lateness
in the Dataflow streaming pipeline later.

For fraud detection, the event stream is how we count repeated failures.
If we see 'payment_failed' more than twice in an hour for the same customer,
that's a signal.

Run directly:
    python data/generators/generate_events.py
"""

import argparse
import csv
import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from config import OUTPUT_DIR, RANDOM_SEED, SAMPLE_DIR, SAMPLE_SIZE

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

# Possible events in a transaction's lifecycle and how many seconds after
# the transaction_timestamp each event typically fires.
LIFECYCLE = {
    "completed": [
        ("transaction_created", 0),
        ("payment_attempted", 2),
        ("payment_completed", 5),
    ],
    "failed": [
        ("transaction_created", 0),
        ("payment_attempted", 2),
        ("payment_failed", 4),
    ],
    "pending": [
        ("transaction_created", 0),
        ("payment_attempted", 2),
    ],
    "reversed": [
        ("transaction_created", 0),
        ("payment_attempted", 2),
        ("payment_completed", 5),
        ("transaction_reversed", 3600),  # reversed an hour later
    ],
    "flagged": [
        ("transaction_created", 0),
        ("payment_attempted", 2),
        ("payment_completed", 5),
        ("fraud_flagged", 30),
        ("review_initiated", 120),
    ],
}


def load_transactions(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Transaction data not found at {path}. Run generate_transactions.py first.")
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def generate_events_for_transaction(
    txn: dict,
    rng: random.Random,
    late_arrival_fraction: float = 0.05,
) -> list[dict]:
    """
    Generate the lifecycle events for a single transaction.

    The late_arrival_fraction controls how many events will arrive
    slightly out of order — this is a normal occurrence in distributed
    systems and we want the pipeline to handle it correctly.
    """
    status = txn["status"]
    lifecycle = LIFECYCLE.get(status, LIFECYCLE["completed"])
    txn_time = datetime.fromisoformat(txn["transaction_timestamp"].replace("Z", "+00:00"))

    events = []
    for event_type, base_seconds in lifecycle:
        # Add a little jitter to the timing so events aren't perfectly spaced.
        jitter = rng.randint(-1, 3)
        event_time = txn_time + timedelta(seconds=base_seconds + jitter)

        # Simulate late-arriving events: a small fraction arrive a bit earlier
        # than they should, which creates out-of-order scenarios in the stream.
        if rng.random() < late_arrival_fraction:
            event_time -= timedelta(seconds=rng.randint(1, 30))

        events.append({
            "event_id": str(uuid.uuid4()),
            "transaction_id": txn["transaction_id"],
            "event_type": event_type,
            "event_timestamp": event_time.isoformat(),
            "event_metadata": "",  # populated later for specific event types
            "created_at": event_time.isoformat(),
        })

    # For failed transactions, there's a chance of a retry that also fails,
    # which gives us the 'repeated failures' fraud signal.
    if status == "failed" and rng.random() < 0.40:
        retry_time = txn_time + timedelta(seconds=rng.randint(30, 120))
        events.append({
            "event_id": str(uuid.uuid4()),
            "transaction_id": txn["transaction_id"],
            "event_type": "payment_failed",
            "event_timestamp": retry_time.isoformat(),
            "event_metadata": '{"reason": "retry_failed"}',
            "created_at": retry_time.isoformat(),
        })

    return events


def generate_events(seed: int, output_dir: Path) -> list[dict]:
    rng = random.Random(seed)
    transactions = load_transactions(output_dir / "transactions" / "transactions.csv")
    log.info("Loaded %d transactions, generating events...", len(transactions))

    all_events = []
    for i, txn in enumerate(transactions):
        all_events.extend(generate_events_for_transaction(txn, rng))
        if (i + 1) % 50_000 == 0:
            log.info("  Processed %d / %d transactions", i + 1, len(transactions))

    # Sort by event_timestamp so the CSV is in approximate time order.
    all_events.sort(key=lambda e: e["event_timestamp"])
    log.info("Total events generated: %d", len(all_events))
    return all_events


def save_to_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info("Saved %d records → %s", len(records), path)


def main(output_dir: Path) -> list[dict]:
    events = generate_events(RANDOM_SEED, output_dir)
    save_to_csv(events, output_dir / "events" / "transaction_events.csv")
    save_to_csv(events[:SAMPLE_SIZE], SAMPLE_DIR / "events_sample.csv")
    return events


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate transaction lifecycle events")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    main(output_dir=args.output)
