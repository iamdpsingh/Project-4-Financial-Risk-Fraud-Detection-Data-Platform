"""
Unit tests for the Beam pipeline DoFns and Fraud Rules.
"""

import unittest

from fraud.risk_scoring.scorer import calculate_risk_score
from fraud.rules.rule_engine import evaluate_all_rules, evaluate_high_amount, evaluate_new_device


class TestFraudRules(unittest.TestCase):

    def test_high_amount_rule(self):
        # amount_usd > 3 * customer_avg_30d
        txn = {"amount_usd": 400.0}
        self.assertTrue(evaluate_high_amount(txn, customer_avg_30d=100.0))
        
        txn2 = {"amount_usd": 200.0}
        self.assertFalse(evaluate_high_amount(txn2, customer_avg_30d=100.0))
        
        # fallback rule when avg is 0
        txn3 = {"amount_usd": 1500.0}
        self.assertTrue(evaluate_high_amount(txn3, customer_avg_30d=0.0))

    def test_new_device_rule(self):
        self.assertTrue(evaluate_new_device({"is_new_device": True}))
        self.assertFalse(evaluate_new_device({"is_new_device": False}))
        self.assertFalse(evaluate_new_device({}))

    def test_rule_engine_all(self):
        txn = {
            "amount_usd": 5000,
            "is_international": "true",
            "merchant_risk_category": "high",
            "is_new_device": True
        }
        signals = evaluate_all_rules(txn, customer_avg_30d=100.0, txn_count_5m=1, failure_count_1h=0)
        
        self.assertTrue(signals["high_amount"])
        self.assertTrue(signals["geo_anomaly"])
        self.assertTrue(signals["high_risk_merchant"])
        self.assertTrue(signals["new_device"])
        self.assertFalse(signals["velocity_burst"])
        self.assertFalse(signals["repeated_failures"])
        
    def test_risk_scorer(self):
        signals = {
            "high_amount": True,          # 25
            "geo_anomaly": True,          # 20
            "high_risk_merchant": True,   # 7
            "new_device": False,
            "velocity_burst": False,
            "repeated_failures": False,
            "off_hours": False
        }
        score, level = calculate_risk_score(signals)
        self.assertEqual(score, 52)
        self.assertEqual(level, "MEDIUM")
        
        signals["new_device"] = True      # +20
        score, level = calculate_risk_score(signals)
        self.assertEqual(score, 72)
        self.assertEqual(level, "HIGH")

if __name__ == "__main__":
    unittest.main()
