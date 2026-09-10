"""
Device data generator.

Each customer has between 1 and 4 devices. Most people use a mobile phone
as their primary transaction device, with some using a desktop browser and
a smaller number using tablets or POS terminals.

Devices are important for the fraud detection side of things. The
'new device' fraud signal fires when a transaction comes from a device
that was first seen less than 24 hours ago. To support that, each device
has a first_seen_at timestamp, and some are intentionally set to very
recent times to simulate new device scenarios.

Run directly:
    python data/generators/generate_devices.py
"""

import argparse
import csv
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address
from pathlib import Path

from config import NUM_DEVICES, OUTPUT_DIR, RANDOM_SEED, SAMPLE_DIR, SAMPLE_SIZE
from faker import Faker

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

DEVICE_TYPES = ["mobile", "desktop", "tablet", "pos_terminal"]
DEVICE_TYPE_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

# OS options per device type — keeping it realistic.
OS_BY_DEVICE = {
    "mobile": ["Android 13", "Android 14", "iOS 16", "iOS 17", "iOS 17.4"],
    "desktop": ["Windows 11", "Windows 10", "macOS 14", "macOS 13", "Ubuntu 22.04"],
    "tablet": ["iPadOS 17", "iPadOS 16", "Android 13"],
    "pos_terminal": ["Android 9", "POS OS 3.2", "Linux Embedded"],
}

# Browsers are only relevant for mobile and desktop web sessions.
BROWSERS = [
    "Chrome/123.0", "Chrome/122.0", "Safari/17.4", "Safari/17.3",
    "Firefox/125.0", "Edge/123.0",
]


def random_ip(rng: random.Random) -> str:
    """
    Generate a random public-range IPv4 address.
    We avoid private ranges (10.x, 192.168.x, 172.16.x) because
    those would never appear in real transaction logs.
    """
    while True:
        ip = IPv4Address(rng.randint(0, 2**32 - 1))
        if not ip.is_private and not ip.is_loopback and not ip.is_multicast:
            return str(ip)


def generate_devices(
    customer_ids: list[str],
    target_count: int,
    seed: int,
    # Fraction of devices that will be "new" (first_seen within last 24h).
    # These trigger the new-device fraud signal in the risk scorer.
    new_device_fraction: float = 0.03,
) -> list[dict]:
    """
    Generate devices linked to customers.

    Most customers get 1-2 devices. The new_device_fraction controls how many
    devices were created very recently (to simulate the fraud pattern where
    someone uses a brand new device for a high-value transaction).
    """
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    devices = []
    now = datetime.now(UTC)
    base_time = datetime(2019, 1, 1, tzinfo=UTC)

    shuffled = customer_ids.copy()
    rng.shuffle(shuffled)

    for customer_id in shuffled:
        if len(devices) >= target_count:
            break

        num_devices = rng.choices([1, 2, 3, 4], weights=[0.50, 0.30, 0.15, 0.05], k=1)[0]

        for _ in range(num_devices):
            if len(devices) >= target_count:
                break

            device_type = rng.choices(DEVICE_TYPES, weights=DEVICE_TYPE_WEIGHTS, k=1)[0]
            os = rng.choice(OS_BY_DEVICE[device_type])

            # Browser only makes sense for web-based device types.
            browser = None
            if device_type in ("mobile", "desktop"):
                browser = rng.choice(BROWSERS) if rng.random() < 0.7 else None

            # A small fraction of devices are brand new — first seen in the last 24 hours.
            if rng.random() < new_device_fraction:
                first_seen = now - timedelta(hours=rng.uniform(0, 23))
            else:
                first_seen = base_time + timedelta(days=rng.randint(0, 5 * 365))

            # last_seen is somewhere between first_seen and now.
            max_days = max((now - first_seen).days, 1)
            last_seen = first_seen + timedelta(days=rng.randint(0, max_days))

            devices.append({
                "device_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "device_type": device_type,
                "os": os,
                "browser": browser or "",
                "ip_address": random_ip(rng),
                "user_agent": fake.user_agent()[:500] if device_type != "pos_terminal" else "",
                "first_seen_at": first_seen.isoformat(),
                "last_seen_at": last_seen.isoformat(),
                "created_at": first_seen.isoformat(),
            })

    log.info("Generated %d devices", len(devices))
    return devices


def load_customer_ids(customers_csv: Path) -> list[str]:
    if not customers_csv.exists():
        raise FileNotFoundError(f"Customer data not found at {customers_csv}. Run generate_customers.py first.")
    with open(customers_csv, encoding="utf-8") as f:
        return [row["customer_id"] for row in csv.DictReader(f)]


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

    devices = generate_devices(customer_ids, count, RANDOM_SEED)
    save_to_csv(devices, output_dir / "devices" / "devices.csv")
    save_to_csv(devices[:SAMPLE_SIZE], SAMPLE_DIR / "devices_sample.csv")
    return devices


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic device data")
    parser.add_argument("--count", type=int, default=NUM_DEVICES)
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    main(count=args.count, output_dir=args.output)
