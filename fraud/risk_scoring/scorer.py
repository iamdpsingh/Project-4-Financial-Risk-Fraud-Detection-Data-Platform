"""
Risk Scorer for financial transactions.

Combines the results from the rule engine into a final 0-100 risk score
based on predefined weights.
"""

from typing import Any

# Weights for each signal
WEIGHTS = {
    "high_amount": 25,
    "new_device": 20,
    "geo_anomaly": 20,
    "velocity_burst": 15,
    "repeated_failures": 10,
    "high_risk_merchant": 7,
    "off_hours": 3
}

def calculate_risk_score(signals: dict[str, bool]) -> tuple[int, str]:
    """
    Calculates the final risk score based on triggered signals.
    Returns (score, risk_level).
    
    0-30: LOW
    31-70: MEDIUM
    71-100: HIGH
    """
    score = 0
    
    for signal, is_triggered in signals.items():
        if is_triggered:
            score += WEIGHTS.get(signal, 0)
            
    # Cap score at 100
    score = min(score, 100)
    
    if score <= 30:
        risk_level = "LOW"
    elif score <= 70:
        risk_level = "MEDIUM"
    else:
        risk_level = "HIGH"
        
    return score, risk_level

def score_transaction(transaction: dict[str, Any], signals: dict[str, bool]) -> dict[str, Any]:
    """
    Takes a transaction and its evaluated signals, and returns an enriched
    transaction dictionary including the risk score and level.
    """
    score, risk_level = calculate_risk_score(signals)
    
    enriched = transaction.copy()
    enriched["risk_score"] = score
    enriched["risk_level"] = risk_level
    enriched["signals_triggered"] = [k for k, v in signals.items() if v]
    
    return enriched
