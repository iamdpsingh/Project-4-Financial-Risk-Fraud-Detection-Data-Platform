"""
Shared configuration for all data generators.

This is the single place to change how much data gets generated and
what parameters control the synthetic data. All other generator scripts
import from here rather than hardcoding their own values.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env", override=False)

# How many records each generator will produce by default.
# These can be overridden by environment variables or CLI flags.
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 10_000))
NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS", 15_000))
NUM_MERCHANTS = int(os.getenv("NUM_MERCHANTS", 2_000))
NUM_DEVICES = int(os.getenv("NUM_DEVICES", 20_000))
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", 500_000))
NUM_EVENTS = int(os.getenv("NUM_EVENTS", 1_500_000))

# Random seed keeps results reproducible across runs.
RANDOM_SEED = int(os.getenv("RANDOM_SEED", 42))

# Where generated CSV files are saved locally.
OUTPUT_DIR = Path(__file__).parent.parent / "raw"

# Where small sample files (committed to git) are saved.
SAMPLE_DIR = Path(__file__).parent.parent / "sample"

# How many rows go into the sample files.
SAMPLE_SIZE = 100

# Country distribution for customers. Weighted so most customers
# are from a handful of countries, which is realistic for a financial app.
CUSTOMER_COUNTRIES = {
    "US": 0.25,
    "IN": 0.20,
    "GB": 0.10,
    "DE": 0.08,
    "CA": 0.07,
    "AU": 0.06,
    "SG": 0.05,
    "AE": 0.04,
    "BR": 0.04,
    "FR": 0.04,
    "JP": 0.03,
    "NG": 0.02,
    "ZA": 0.01,
    "MX": 0.01,
}

# Merchant categories and their share of total merchants.
# High-risk ones are flagged explicitly below.
MERCHANT_CATEGORIES = {
    "Online Retail": 0.20,
    "Grocery & Supermarket": 0.12,
    "Restaurant & Food Delivery": 0.10,
    "Travel & Hotels": 0.08,
    "Electronics": 0.08,
    "Fuel & Gas Station": 0.07,
    "Healthcare & Pharmacy": 0.06,
    "Subscription Services": 0.06,
    "Entertainment & Streaming": 0.05,
    "Money Transfer": 0.04,          # elevated risk
    "Cryptocurrency Exchange": 0.03, # high risk
    "Online Gambling": 0.03,         # high risk
    "Luxury Goods": 0.03,            # elevated risk
    "Gift Cards": 0.03,              # high risk
    "ATM & Cash Advance": 0.02,
}

# Which categories are automatically assigned a risk level.
# Everything else defaults to 'low'.
MERCHANT_RISK_MAP = {
    "Cryptocurrency Exchange": "high",
    "Online Gambling": "high",
    "Gift Cards": "high",
    "Money Transfer": "medium",
    "Luxury Goods": "medium",
    "ATM & Cash Advance": "medium",
    "Travel & Hotels": "low",
}

# Fraud pattern config — controls how many transactions will exhibit
# each type of suspicious behaviour. These are used by generate_transactions.py
# to inject realistic fraud signals into the dataset.
FRAUD_CONFIG = {
    # Fraction of transactions that are part of a velocity burst
    # (10+ transactions from the same customer within 3 minutes).
    "velocity_burst_fraction": 0.005,

    # Fraction of transactions involving a device that was first seen
    # within the last 24 hours.
    "new_device_fraction": 0.02,

    # Fraction where the transaction country doesn't match the customer's
    # home country (geo anomaly).
    "geo_anomaly_fraction": 0.015,

    # Fraction of transactions that are preceded by 2+ payment failures.
    "repeated_failure_fraction": 0.01,

    # Fraction of high-risk merchant transactions with unusually large amounts.
    "high_risk_merchant_large_amount_fraction": 0.008,

    # Fraction of transactions that happen between midnight and 5am local time.
    "off_hours_fraction": 0.03,
}

# The valid ISO 4217 currency codes the platform supports.
SUPPORTED_CURRENCIES = [
    "USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD",
    "SGD", "CHF", "CNY", "HKD", "BRL", "MXN", "AED", "SAR",
]

# Rough exchange rates to USD (approximate, for normalising amount_usd).
# These don't need to be real-time — they're just for generating plausible data.
EXCHANGE_RATES_TO_USD = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "INR": 0.012,
    "JPY": 0.0067, "CAD": 0.74, "AUD": 0.65, "SGD": 0.74,
    "CHF": 1.11, "CNY": 0.138, "HKD": 0.128, "BRL": 0.20,
    "MXN": 0.058, "AED": 0.272, "SAR": 0.267,
}

# Typical transaction amount ranges by merchant category (in USD equivalent).
# Used to generate realistic amounts rather than pure random noise.
AMOUNT_RANGES_USD = {
    "Online Retail": (5, 500),
    "Grocery & Supermarket": (10, 200),
    "Restaurant & Food Delivery": (5, 100),
    "Travel & Hotels": (50, 3000),
    "Electronics": (50, 2000),
    "Fuel & Gas Station": (20, 150),
    "Healthcare & Pharmacy": (5, 500),
    "Subscription Services": (5, 50),
    "Entertainment & Streaming": (5, 30),
    "Money Transfer": (100, 5000),
    "Cryptocurrency Exchange": (50, 10000),
    "Online Gambling": (10, 2000),
    "Luxury Goods": (200, 20000),
    "Gift Cards": (25, 500),
    "ATM & Cash Advance": (20, 1000),
}
