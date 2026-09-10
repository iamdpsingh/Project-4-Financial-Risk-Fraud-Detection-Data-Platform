"""
Beam DoFn classes for the streaming pipeline.
Handles parsing Pub/Sub messages, evaluating fraud rules, and scoring risk.
"""

from utils.logger import get_logger
import json
import logging
from typing import Any

import apache_beam as beam

# We add the fraud module to the path when running locally, or it gets packaged
# in the actual Dataflow job. For simplicity in the DoFn, we import it inside.
log = get_logger("streaming_pipeline")

class ParsePubSubMessage(beam.DoFn):
    """Parses a JSON string from Pub/Sub into a Python dictionary."""
    def process(self, element: bytes):
        try:
            # PubSub messages are bytes
            message_str = element.decode('utf-8')
            record = json.loads(message_str)
            yield record
        except Exception as e:
            log.error(f"Failed to parse Pub/Sub message: {e}")
            yield beam.pvalue.TaggedOutput("invalid", {"element": str(element), "error": str(e)})

class EnrichTransaction(beam.DoFn):
    """
    Enriches the transaction with data needed for risk scoring.
    In production, this might query a fast key-value store (like Redis or Bigtable)
    to get the customer's 30-day average, recent transaction count, etc.
    For this simulation, we'll mock some of these values if they aren't present.
    """
    def process(self, transaction: dict[str, Any]):
        # Mocking the enrichment for Phase 3 local execution
        # In Phase 13, this connects to a real feature store.
        enriched = transaction.copy()
        
        # We need these for the rule engine:
        # customer_avg_30d, txn_count_5m, failure_count_1h, is_new_device
        
        enriched["customer_avg_30d"] = enriched.get("amount_usd", 100.0) / 2.0  # Mock
        enriched["txn_count_5m"] = 1  # Mock (unless we implement Beam stateful processing)
        enriched["failure_count_1h"] = 0  # Mock
        
        # The generator passes is_international, but we can check device
        # For simulation, assume device is not new unless specified
        enriched["is_new_device"] = enriched.get("is_new_device", False)
        
        yield enriched

class ScoreFraudRisk(beam.DoFn):
    """Evaluates fraud rules and calculates the risk score."""
    def setup(self):
        # Import here so it works on Dataflow workers without complex packaging in Phase 3
        import sys
        from pathlib import Path
        
        # Ensure the fraud module is accessible
        repo_root = Path(__file__).parent.parent.parent
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
            
    def process(self, transaction: dict[str, Any]):
        try:
            from fraud.risk_scoring.scorer import score_transaction
            from fraud.rules.rule_engine import evaluate_all_rules
            
            # 1. Evaluate rules
            signals = evaluate_all_rules(
                transaction=transaction,
                customer_avg_30d=transaction.get("customer_avg_30d", 0.0),
                txn_count_5m=transaction.get("txn_count_5m", 0),
                failure_count_1h=transaction.get("failure_count_1h", 0)
            )
            
            # 2. Score transaction
            scored_txn = score_transaction(transaction, signals)
            
            # Remove the mock enrichment fields before sending to BigQuery
            scored_txn.pop("customer_avg_30d", None)
            scored_txn.pop("txn_count_5m", None)
            scored_txn.pop("failure_count_1h", None)
            scored_txn.pop("is_new_device", None)
            
            # Format signals as a comma-separated string or leave as list if BQ schema supports REPEATED
            scored_txn["signals_triggered"] = ",".join(scored_txn["signals_triggered"])
            
            yield scored_txn
            
        except Exception as e:
            log.error(f"Failed to score transaction {transaction.get('transaction_id')}: {e}")
            yield beam.pvalue.TaggedOutput("errors", {"transaction": transaction, "error": str(e)})

class FormatForBigQuery(beam.DoFn):
    """Prepares the final scored transaction for BigQuery insertion."""
    def process(self, scored_txn: dict[str, Any]):
        # Depending on the schema, ensure timestamps are correct
        # and types match.
        yield scored_txn
