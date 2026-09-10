"""
pytest configuration and shared fixtures for the Financial Risk & Fraud Detection Platform.
"""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

# ── Load environment variables ────────────────────────────────────────────────
# Use .env.test if it exists, otherwise fall back to .env.example defaults
env_file = Path(__file__).parent / ".env.test"
if not env_file.exists():
    env_file = Path(__file__).parent / ".env.example"
load_dotenv(env_file, override=False)


# ── Shared constants ──────────────────────────────────────────────────────────
TEST_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
TEST_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "raw")
TEST_DATASET_STAGING = os.getenv("BQ_DATASET_STAGING", "staging")
TEST_DATASET_CORE = os.getenv("BQ_DATASET_CORE", "core")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def sample_customer() -> dict:
    """A minimal valid customer record for testing."""
    return {
        "customer_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "name": "Test Customer",
        "email": "test@example.com",
        "phone": "+1-555-0100",
        "country": "US",
        "city": "New York",
        "customer_segment": "retail",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }


@pytest.fixture(scope="session")
def sample_transaction() -> dict:
    """A minimal valid transaction record for testing."""
    return {
        "transaction_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
        "customer_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "account_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "merchant_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
        "device_id": "c3d4e5f6-a7b8-9012-cdef-123456789012",
        "transaction_timestamp": "2024-06-15T14:32:07Z",
        "amount": "150.00",
        "currency": "USD",
        "transaction_type": "purchase",
        "country": "US",
        "city": "New York",
        "payment_method": "card",
        "status": "completed",
        "is_international": False,
    }


@pytest.fixture(scope="session")
def sample_merchant() -> dict:
    """A minimal valid merchant record for testing."""
    return {
        "merchant_id": "d4e5f6a7-b8c9-0123-defa-234567890123",
        "merchant_name": "Test Merchant",
        "merchant_category": "Online Retail",
        "mcc_code": "5411",
        "country": "US",
        "city": "Seattle",
        "risk_category": "low",
    }
