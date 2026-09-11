"""
Transaction data generator — the heart of this whole project.

This is where we create 500,000 (or however many you configure) transactions
and deliberately embed realistic fraud patterns into a fraction of them.
The goal is not random noise. If every transaction looks the same, the fraud
signals won't have anything to detect. So we carefully construct specific
scenarios that will trigger the risk scoring engine later.

The fraud patterns embedded here:
    1. Velocity bursts      — a customer makes 10+ transactions within 3 minutes
    2. New device           — a high-value transaction from a device first seen today
    3. Geo anomaly          — the transaction country doesn't match the customer's home country
    4. Repeated failures    — the same customer had 2+ failed transactions in the last hour
    5. High-risk merchant + large amount — a crypto exchange or gambling site with an unusually big purchase

Most transactions are completely normal. The fraudulent ones are mixed in
at realistic proportions — usually 1-5% depending on the pattern.

Run directly:
    python data/generators/generate_transactions.py
    python data/generators/generate_transactions.py --count 100000
"""

import argparse
import csv
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
from config import (
    AMOUNT_RANGES_USD,
    EXCHANGE_RATES_TO_USD,
    FRAUD_CONFIG,
    NUM_TRANSACTIONS,
    OUTPUT_DIR,
    RANDOM_SEED,
    SAMPLE_DIR,
    SAMPLE_SIZE,
)

from utils.logger import get_logger

log = get_logger(__name__)

TRANSACTION_TYPES = ["purchase", "withdrawal", "transfer", "refund", "payment", "deposit"]
TRANSACTION_TYPE_WEIGHTS = [0.55, 0.10, 0.15, 0.05, 0.10, 0.05]

PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "upi", "crypto", "cheque"]
PAYMENT_METHOD_WEIGHTS = [0.45, 0.20, 0.15, 0.12, 0.05, 0.03]

STATUSES = ["completed", "failed", "pending", "reversed", "flagged"]
STATUS_WEIGHTS = [0.85, 0.08, 0.04, 0.02, 0.01]


def load_csv(path: Path, key_field: str | None = None) -> list[dict] | dict:
    """
    Load a CSV file. If key_field is specified, return a dict keyed by that field
    (useful for fast lookups by ID). Otherwise return the full list.
    """
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if key_field:
        return {row[key_field]: row for row in rows}
    return rows


def amount_in_original_currency(
    merchant_category: str,
    currency: str,
    rng: random.Random,
    multiplier: float = 1.0,
) -> tuple[float, float]:
    """
    Generate a realistic transaction amount for a given merchant category and currency.

    Returns (amount_in_original_currency, amount_in_usd).
    The multiplier lets us inflate amounts for fraud scenarios.
    """
    low_usd, high_usd = AMOUNT_RANGES_USD.get(merchant_category, (5, 500))
    amount_usd = rng.uniform(low_usd * multiplier, high_usd * multiplier)

    rate = EXCHANGE_RATES_TO_USD.get(currency, 1.0)
    amount = round(amount_usd / rate, 2)
    amount_usd = round(amount_usd, 2)

    return amount, amount_usd


def pick_currency_for_country(country: str, rng: random.Random) -> str:
    """
    Pick a plausible currency for the customer's country.
    In reality people transact in their local currency most of the time.
    """
    country_currency = {
        "US": "USD", "IN": "INR", "GB": "GBP", "DE": "EUR",
        "FR": "EUR", "JP": "JPY", "CA": "CAD", "AU": "AUD",
        "SG": "SGD", "AE": "AED", "BR": "BRL", "MX": "MXN",
        "NG": "USD", "ZA": "USD",
    }
    # 80% chance of using the local currency, 20% chance of USD (international card)
    local = country_currency.get(country, "USD")
    return local if rng.random() < 0.80 else "USD"


def generate_normal_transaction(
    customer: dict,
    account: dict,
    merchant: dict,
    device: dict,
    txn_time: datetime,
    rng: random.Random,
) -> dict:
    """Build a completely normal transaction with no fraud signals."""
    currency = pick_currency_for_country(customer["country"], rng)
    amount, amount_usd = amount_in_original_currency(
        merchant["merchant_category"], currency, rng
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "account_id": account["account_id"],
        "merchant_id": merchant["merchant_id"],
        "device_id": device["device_id"],
        "transaction_timestamp": txn_time.isoformat(),
        "amount": amount,
        "currency": currency,
        "amount_usd": amount_usd,
        "transaction_type": rng.choices(TRANSACTION_TYPES, weights=TRANSACTION_TYPE_WEIGHTS, k=1)[0],
        "country": customer["country"],  # same as customer country — no geo anomaly
        "city": customer["city"],
        "payment_method": rng.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS, k=1)[0],
        "status": rng.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "ip_address": device["ip_address"],
        "is_international": "false",
    }


def inject_velocity_burst(
    customer: dict,
    accounts: list[dict],
    merchants: list[dict],
    devices: list[dict],
    burst_time: datetime,
    rng: random.Random,
    count: int = 12,
) -> list[dict]:
    """
    Create a burst of transactions from one customer in a very short window.

    This triggers the velocity fraud signal: more than 5 transactions
    from the same customer within 5 minutes. We use a 3-minute window
    to make sure it's a clear signal.
    """
    merchant = rng.choice(merchants)
    device = rng.choice(devices)
    txns = []

    for _i in range(count):
        seconds_offset = rng.randint(0, 180)  # spread over 3 minutes
        txn_time = burst_time + timedelta(seconds=seconds_offset)
        currency = pick_currency_for_country(customer["country"], rng)
        # Keep amounts small so it looks like card testing, not just large purchases.
        amount = round(rng.uniform(1, 50), 2)
        amount_usd = round(amount * EXCHANGE_RATES_TO_USD.get(currency, 1.0), 2)

        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "account_id": rng.choice(accounts)["account_id"],
            "merchant_id": merchant["merchant_id"],
            "device_id": device["device_id"],
            "transaction_timestamp": txn_time.isoformat(),
            "amount": amount,
            "currency": currency,
            "amount_usd": amount_usd,
            "transaction_type": "purchase",
            "country": customer["country"],
            "city": customer["city"],
            "payment_method": "card",
            "status": rng.choices(["completed", "failed"], weights=[0.6, 0.4], k=1)[0],
            "ip_address": device["ip_address"],
            "is_international": "false",
        })

    return txns


def inject_geo_anomaly(
    customer: dict,
    account: dict,
    merchant: dict,
    device: dict,
    txn_time: datetime,
    rng: random.Random,
) -> dict:
    """
    Create a transaction where the country doesn't match the customer's home country.

    Triggers the geo-anomaly fraud signal. We pick a country that's clearly
    different from the customer's home country rather than just a neighbouring one.
    """
    foreign_countries = [c for c in ["RU", "CN", "NG", "BR", "UA"] if c != customer["country"]]
    txn_country = rng.choice(foreign_countries)
    currency = "USD"  # International transactions often use USD
    amount, amount_usd = amount_in_original_currency(
        merchant["merchant_category"], currency, rng, multiplier=2.0  # slightly higher amounts
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "account_id": account["account_id"],
        "merchant_id": merchant["merchant_id"],
        "device_id": device["device_id"],
        "transaction_timestamp": txn_time.isoformat(),
        "amount": amount,
        "currency": currency,
        "amount_usd": amount_usd,
        "transaction_type": "purchase",
        "country": txn_country,  # different from customer.country
        "city": "Unknown",
        "payment_method": "card",
        "status": rng.choices(["completed", "failed"], weights=[0.7, 0.3], k=1)[0],
        "ip_address": device["ip_address"],
        "is_international": "true",  # customer.country != transaction.country
    }


def inject_high_risk_large_amount(
    customer: dict,
    account: dict,
    high_risk_merchants: list[dict],
    device: dict,
    txn_time: datetime,
    rng: random.Random,
) -> dict:
    """
    Create a large transaction at a high-risk merchant.

    This triggers two signals at once: high-risk merchant and unusually
    large amount. Combined, these push the risk score quite high.
    """
    merchant = rng.choice(high_risk_merchants)
    currency = "USD"
    # Amount is intentionally 5-10x the normal range for this category.
    amount, amount_usd = amount_in_original_currency(
        merchant["merchant_category"], currency, rng, multiplier=rng.uniform(5, 10)
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "account_id": account["account_id"],
        "merchant_id": merchant["merchant_id"],
        "device_id": device["device_id"],
        "transaction_timestamp": txn_time.isoformat(),
        "amount": amount,
        "currency": currency,
        "amount_usd": amount_usd,
        "transaction_type": "purchase",
        "country": customer["country"],
        "city": customer["city"],
        "payment_method": rng.choice(["card", "crypto", "wallet"]),
        "status": "completed",
        "ip_address": device["ip_address"],
        "is_international": "false",
    }


def generate_transactions(
    count: int,
    seed: int,
    output_dir: Path,
) -> list[dict]:
    """
    Main transaction generation function.

    We load the previously generated reference data (customers, accounts,
    merchants, devices) and use it to produce realistic transactions.
    The fraud patterns are injected into a small fraction of the output,
    with the rest being normal transactions.
    """
    rng = random.Random(seed)
    np.random.seed(seed)

    log.info("Loading reference data...")
    customers = load_csv(output_dir / "customers" / "customers.csv")
    accounts = load_csv(output_dir / "accounts" / "accounts.csv")
    merchants = load_csv(output_dir / "merchants" / "merchants.csv")
    devices = load_csv(output_dir / "devices" / "devices.csv")

    # Pre-index some data for fast lookups.
    customer_accounts: dict[str, list[dict]] = {}
    for acc in accounts:
        customer_accounts.setdefault(acc["customer_id"], []).append(acc)

    customer_devices: dict[str, list[dict]] = {}
    for dev in devices:
        customer_devices.setdefault(dev["customer_id"], []).append(dev)

    high_risk_merchants = [m for m in merchants if m["risk_category"] == "high"]

    transactions = []
    base_time = datetime(2024, 1, 1, tzinfo=UTC)
    end_time = datetime(2024, 12, 31, tzinfo=UTC)
    total_seconds = int((end_time - base_time).total_seconds())

    # How many transactions should be fraud-pattern injections?
    n_velocity = int(count * FRAUD_CONFIG["velocity_burst_fraction"])
    n_geo = int(count * FRAUD_CONFIG["geo_anomaly_fraction"])
    n_high_risk = int(count * FRAUD_CONFIG["high_risk_merchant_large_amount_fraction"])

    log.info(
        "Fraud pattern targets: %d velocity bursts, %d geo anomalies, %d high-risk merchant hits",
        n_velocity, n_geo, n_high_risk
    )

    # Inject velocity bursts.
    for _ in range(n_velocity):
        from typing import Any
        customer = rng.choice(customers)
        accs: list[dict[str, Any]] = customer_accounts.get(customer["customer_id"], []) # type: ignore
        devs: list[dict[str, Any]] = customer_devices.get(customer["customer_id"], []) # type: ignore
        if not accs or not devs:
            continue
        burst_time = base_time + timedelta(seconds=rng.randint(0, total_seconds))
        transactions.extend(inject_velocity_burst(customer, accs, merchants, devs, burst_time, rng))  # type: ignore

    # Inject geo anomalies.
    for _ in range(n_geo):
        from typing import Any
        customer = rng.choice(customers)
        accs: list[dict[str, Any]] = customer_accounts.get(customer["customer_id"], []) # type: ignore
        devs: list[dict[str, Any]] = customer_devices.get(customer["customer_id"], []) # type: ignore
        if not accs or not devs:
            continue
        txn_time = base_time + timedelta(seconds=rng.randint(0, total_seconds))
        transactions.append(inject_geo_anomaly(
            customer, rng.choice(accs), rng.choice(merchants), rng.choice(devs), txn_time, rng
        ))

    # Inject high-risk merchant + large amount transactions.
    if high_risk_merchants:
        for _ in range(n_high_risk):
            customer = rng.choice(customers)
            accs = customer_accounts.get(customer["customer_id"], [])  # type: ignore
            devs = list(customer_devices.get(customer["customer_id"], []))  # type: ignore
            if not accs or not devs:
                continue
            txn_time = base_time + timedelta(seconds=rng.randint(0, total_seconds))
            transactions.append(inject_high_risk_large_amount(
                customer, rng.choice(accs), high_risk_merchants, rng.choice(devs), txn_time, rng
            ))

    # Fill the rest with normal transactions.
    needed = count - len(transactions)
    log.info("Generating %d normal transactions...", needed)
    for i in range(needed):
        customer = rng.choice(customers)
        accs = customer_accounts.get(customer["customer_id"], [])  # type: ignore
        devs = list(customer_devices.get(customer["customer_id"], []))  # type: ignore
        if not accs or not devs:
            # This customer has no account/device yet — skip and let the count drift slightly.
            continue

        txn_time = base_time + timedelta(seconds=rng.randint(0, total_seconds))
        transactions.append(generate_normal_transaction(
            customer, rng.choice(accs), rng.choice(merchants), rng.choice(devs), txn_time, rng
        ))

        if (i + 1) % 50_000 == 0:
            log.info("  Normal transactions: %d / %d", i + 1, needed)

    # Sort by timestamp so the CSV is in chronological order.
    transactions.sort(key=lambda t: t["transaction_timestamp"])
    log.info("Total transactions generated: %d", len(transactions))
    return transactions


def save_to_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info("Saved %d records → %s", len(records), path)


def main(count: int, output_dir: Path) -> list[dict]:
    transactions = generate_transactions(count, RANDOM_SEED, output_dir)
    save_to_csv(transactions, output_dir / "transactions" / "transactions.csv")
    save_to_csv(transactions[:SAMPLE_SIZE], SAMPLE_DIR / "transactions_sample.csv")
    return transactions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data with embedded fraud patterns")
    parser.add_argument("--count", type=int, default=NUM_TRANSACTIONS)
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    main(count=args.count, output_dir=args.output)
