"""
Unit tests for the data generators.

These tests don't need PostgreSQL or any external services — they just
verify that the generator functions produce valid, correctly shaped data.
We check the things that matter: required fields are present, values are
within valid ranges, and the fraud patterns are actually being injected.
"""

import csv
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Add the generators folder to the path so we can import the modules directly.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "data" / "generators"))

import generate_customers
import generate_accounts
import generate_merchants
import generate_devices
import generate_transactions
from config import CUSTOMER_COUNTRIES, MERCHANT_CATEGORIES, SUPPORTED_CURRENCIES


# ── Customer tests ─────────────────────────────────────────────────────────────

class TestGenerateCustomers:

    def test_correct_count(self):
        customers = generate_customers.generate_customers(100, seed=42)
        assert len(customers) == 100

    def test_required_fields_present(self):
        customers = generate_customers.generate_customers(10, seed=42)
        required = {"customer_id", "name", "email", "country", "city",
                    "customer_segment", "created_at", "updated_at"}
        for c in customers:
            assert required.issubset(c.keys()), f"Missing fields in customer: {c}"

    def test_country_is_valid(self):
        customers = generate_customers.generate_customers(200, seed=42)
        valid_countries = set(CUSTOMER_COUNTRIES.keys())
        for c in customers:
            assert c["country"] in valid_countries, f"Invalid country: {c['country']}"

    def test_segment_is_valid(self):
        customers = generate_customers.generate_customers(200, seed=42)
        valid_segments = {"retail", "premium", "business", "vip", "student"}
        for c in customers:
            assert c["customer_segment"] in valid_segments

    def test_emails_are_unique(self):
        customers = generate_customers.generate_customers(500, seed=42)
        emails = [c["email"] for c in customers]
        assert len(emails) == len(set(emails)), "Duplicate emails found"

    def test_customer_ids_are_unique(self):
        customers = generate_customers.generate_customers(500, seed=42)
        ids = [c["customer_id"] for c in customers]
        assert len(ids) == len(set(ids))

    def test_reproducible_with_same_seed(self):
        # Same seed should always produce the same first customer.
        batch1 = generate_customers.generate_customers(10, seed=99)
        batch2 = generate_customers.generate_customers(10, seed=99)
        assert batch1[0]["customer_id"] == batch2[0]["customer_id"]

    def test_different_seeds_produce_different_data(self):
        batch1 = generate_customers.generate_customers(10, seed=1)
        batch2 = generate_customers.generate_customers(10, seed=2)
        assert batch1[0]["customer_id"] != batch2[0]["customer_id"]

    def test_updated_at_not_before_created_at(self):
        customers = generate_customers.generate_customers(100, seed=42)
        for c in customers:
            created = datetime.fromisoformat(c["created_at"])
            updated = datetime.fromisoformat(c["updated_at"])
            assert updated >= created, f"updated_at is before created_at for {c['customer_id']}"


# ── Merchant tests ─────────────────────────────────────────────────────────────

class TestGenerateMerchants:

    def test_correct_count(self):
        merchants = generate_merchants.generate_merchants(50, seed=42)
        assert len(merchants) == 50

    def test_risk_category_matches_category(self):
        """
        High-risk categories must always have risk_category='high'.
        This is important for the fraud signals to work correctly.
        """
        from config import MERCHANT_RISK_MAP
        merchants = generate_merchants.generate_merchants(200, seed=42)
        for m in merchants:
            expected_risk = MERCHANT_RISK_MAP.get(m["merchant_category"], "low")
            assert m["risk_category"] == expected_risk, (
                f"Merchant category '{m['merchant_category']}' has risk '{m['risk_category']}' "
                f"but expected '{expected_risk}'"
            )

    def test_mcc_code_is_4_digits(self):
        merchants = generate_merchants.generate_merchants(50, seed=42)
        for m in merchants:
            if m["mcc_code"]:
                assert len(m["mcc_code"]) == 4, f"Invalid MCC: {m['mcc_code']}"
                assert m["mcc_code"].isdigit(), f"MCC not numeric: {m['mcc_code']}"

    def test_high_risk_merchants_exist(self):
        """We need high-risk merchants in the dataset for the fraud signal to fire."""
        merchants = generate_merchants.generate_merchants(200, seed=42)
        high_risk = [m for m in merchants if m["risk_category"] == "high"]
        assert len(high_risk) > 0, "No high-risk merchants generated"


# ── Device tests ───────────────────────────────────────────────────────────────

class TestGenerateDevices:

    def _make_customer_ids(self, n: int) -> list[str]:
        import uuid
        return [str(uuid.uuid4()) for _ in range(n)]

    def test_device_count_roughly_matches_target(self):
        ids = self._make_customer_ids(100)
        devices = generate_devices.generate_devices(ids, target_count=150, seed=42)
        # We might end up slightly under if customers run out.
        assert 100 <= len(devices) <= 160

    def test_device_type_is_valid(self):
        ids = self._make_customer_ids(50)
        devices = generate_devices.generate_devices(ids, target_count=80, seed=42)
        valid_types = {"mobile", "desktop", "tablet", "pos_terminal"}
        for d in devices:
            assert d["device_type"] in valid_types

    def test_ip_address_looks_valid(self):
        """IPs shouldn't be private or loopback ranges."""
        import ipaddress
        ids = self._make_customer_ids(50)
        devices = generate_devices.generate_devices(ids, target_count=80, seed=42)
        for d in devices:
            if d["ip_address"]:
                ip = ipaddress.IPv4Address(d["ip_address"])
                assert not ip.is_private, f"Private IP found: {d['ip_address']}"
                assert not ip.is_loopback, f"Loopback IP found: {d['ip_address']}"

    def test_new_devices_exist_when_fraction_nonzero(self):
        """
        With new_device_fraction > 0, some devices should have been first seen
        within the last 24 hours. These are the ones that trigger the fraud signal.
        """
        ids = self._make_customer_ids(200)
        devices = generate_devices.generate_devices(
            ids, target_count=300, seed=42, new_device_fraction=0.10
        )
        now = datetime.now(timezone.utc)
        new_devices = [
            d for d in devices
            if (now - datetime.fromisoformat(d["first_seen_at"])).total_seconds() < 86400
        ]
        assert len(new_devices) > 0, "Expected some new devices but found none"

    def test_last_seen_not_before_first_seen(self):
        ids = self._make_customer_ids(50)
        devices = generate_devices.generate_devices(ids, target_count=80, seed=42)
        for d in devices:
            first = datetime.fromisoformat(d["first_seen_at"])
            last = datetime.fromisoformat(d["last_seen_at"])
            assert last >= first, f"last_seen before first_seen for device {d['device_id']}"


# ── Transaction tests ──────────────────────────────────────────────────────────

class TestTransactionHelpers:

    def test_amount_in_original_currency_positive(self):
        rng = random.Random(42)
        for currency in ["USD", "INR", "JPY", "EUR"]:
            amount, amount_usd = generate_transactions.amount_in_original_currency(
                "Online Retail", currency, rng
            )
            assert amount > 0, f"Amount must be positive, got {amount}"
            assert amount_usd > 0

    def test_geo_anomaly_uses_different_country(self):
        rng = random.Random(42)
        customer = {"customer_id": "c1", "country": "US", "city": "NYC", "customer_segment": "retail"}
        account = {"account_id": "a1"}
        merchant = {"merchant_id": "m1", "merchant_category": "Online Retail"}
        device = {"device_id": "d1", "ip_address": "1.2.3.4"}
        txn_time = datetime(2024, 6, 1, tzinfo=timezone.utc)

        txn = generate_transactions.inject_geo_anomaly(customer, account, merchant, device, txn_time, rng)

        assert txn["country"] != customer["country"], "Geo anomaly should use a different country"
        assert txn["is_international"] == "true"

    def test_velocity_burst_produces_multiple_transactions(self):
        rng = random.Random(42)
        customer = {"customer_id": "c1", "country": "US", "city": "NYC", "customer_segment": "retail"}
        accounts = [{"account_id": f"a{i}"} for i in range(3)]
        merchants = [{"merchant_id": "m1", "merchant_category": "Online Retail", "risk_category": "low"}]
        devices = [{"device_id": "d1", "ip_address": "1.2.3.4"}]
        burst_time = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)

        txns = generate_transactions.inject_velocity_burst(
            customer, accounts, merchants, devices, burst_time, rng, count=10
        )

        assert len(txns) == 10
        # All from the same customer.
        for t in txns:
            assert t["customer_id"] == "c1"

    def test_velocity_burst_within_3_minutes(self):
        rng = random.Random(42)
        customer = {"customer_id": "c1", "country": "US", "city": "NYC"}
        accounts = [{"account_id": "a1"}]
        merchants = [{"merchant_id": "m1", "merchant_category": "Online Retail", "risk_category": "low"}]
        devices = [{"device_id": "d1", "ip_address": "1.2.3.4"}]
        burst_time = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)

        txns = generate_transactions.inject_velocity_burst(
            customer, accounts, merchants, devices, burst_time, rng, count=12
        )

        timestamps = [datetime.fromisoformat(t["transaction_timestamp"]) for t in txns]
        earliest = min(timestamps)
        latest = max(timestamps)
        spread_seconds = (latest - earliest).total_seconds()

        assert spread_seconds <= 180, f"Burst spread too wide: {spread_seconds}s (expected ≤ 180s)"

    def test_pick_currency_for_country(self):
        rng = random.Random(42)
        # Indian customers should mostly get INR.
        inr_count = sum(
            1 for _ in range(100)
            if generate_transactions.pick_currency_for_country("IN", rng) == "INR"
        )
        assert inr_count > 70, f"Expected mostly INR for IN, got {inr_count}/100"
