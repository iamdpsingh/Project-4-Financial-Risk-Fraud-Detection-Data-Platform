"""
Rule Engine for evaluating fraud signals on a transaction.

This engine evaluates 7 distinct signals. In a production system, these might
rely on real-time lookups (e.g., Redis for velocity) and ML models.
For this simulation, we use the data encoded in the transaction event itself
and simple state abstractions.
"""

from typing import Dict, Any

def evaluate_high_amount(transaction: Dict[str, Any], customer_avg_30d: float) -> bool:
    """True if amount > 3x customer's 30-day average."""
    amount_usd = transaction.get("amount_usd", 0)
    # If we don't have a baseline, we use a sensible default check
    if customer_avg_30d <= 0:
        return amount_usd > 1000
    return amount_usd > (3 * customer_avg_30d)

def evaluate_new_device(transaction: Dict[str, Any]) -> bool:
    """
    True if the device was first seen less than 24 hours ago.
    In the streaming payload from our generator, we might not have the device
    first_seen date directly unless enriched.
    """
    # For simulation, we assume this info is enriched into the event,
    # or we mock it based on a flag.
    return transaction.get("is_new_device", False)

def evaluate_geo_anomaly(transaction: Dict[str, Any]) -> bool:
    """True if transaction country does not match customer's home country."""
    # Our generator provides 'is_international' as a string "true"/"false"
    return str(transaction.get("is_international", "false")).lower() == "true"

def evaluate_velocity_burst(transaction: Dict[str, Any], txn_count_5m: int) -> bool:
    """True if more than 5 transactions in the last 5 minutes."""
    return txn_count_5m > 5

def evaluate_repeated_failures(transaction: Dict[str, Any], failure_count_1h: int) -> bool:
    """True if more than 2 payment failures in the last hour."""
    return failure_count_1h > 2

def evaluate_high_risk_merchant(transaction: Dict[str, Any]) -> bool:
    """True if merchant is flagged as high-risk by compliance."""
    return transaction.get("merchant_risk_category", "low").lower() == "high"

def evaluate_off_hours(transaction: Dict[str, Any]) -> bool:
    """True if transaction happened between midnight and 5am local time."""
    # Simplified: We just check the UTC timestamp for demo purposes.
    # A real implementation would parse the timestamp and convert to local TZ.
    from datetime import datetime
    txn_time_str = transaction.get("transaction_timestamp")
    if not txn_time_str:
        return False
    try:
        dt = datetime.fromisoformat(txn_time_str.replace("Z", "+00:00"))
        return 0 <= dt.hour < 5
    except Exception:
        return False

def evaluate_all_rules(
    transaction: Dict[str, Any],
    customer_avg_30d: float = 0.0,
    txn_count_5m: int = 0,
    failure_count_1h: int = 0
) -> Dict[str, bool]:
    """Evaluates all fraud signals and returns a dictionary of the results."""
    return {
        "high_amount": evaluate_high_amount(transaction, customer_avg_30d),
        "new_device": evaluate_new_device(transaction),
        "geo_anomaly": evaluate_geo_anomaly(transaction),
        "velocity_burst": evaluate_velocity_burst(transaction, txn_count_5m),
        "repeated_failures": evaluate_repeated_failures(transaction, failure_count_1h),
        "high_risk_merchant": evaluate_high_risk_merchant(transaction),
        "off_hours": evaluate_off_hours(transaction)
    }
