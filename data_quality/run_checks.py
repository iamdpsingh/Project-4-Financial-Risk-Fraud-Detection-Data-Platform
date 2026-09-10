"""
Data Quality runner script using Great Expectations.

In a real setup, this would use a full GE context directory. For this local simulation,
we use the ephemeral context to validate pandas DataFrames loaded from our CSV outputs.
"""

import argparse
import logging
from pathlib import Path

import great_expectations as gx
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s  [%(name)s]  %(message)s")
log = logging.getLogger("data_quality")

def check_transactions(data_dir: Path):
    """Basic data quality checks for transactions using pandas + Great Expectations."""
    txn_file = data_dir / "transactions/transactions.csv"
    if not txn_file.exists():
        # Fallback to sample for testing
        txn_file = data_dir.parent / "sample/transactions_sample.csv"
        if not txn_file.exists():
            log.error("Transactions file not found")
            return
            
    log.info(f"Loading transactions from {txn_file}")
    df = pd.read_csv(txn_file)
    
    # Initialize an Ephemeral DataContext
    context = gx.get_context(mode="ephemeral")
    
    # Add data to the context
    data_source = context.data_sources.add_pandas("local_pandas")
    data_asset = data_source.add_dataframe_asset(name="transactions")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")
    
    # Create an expectation suite
    suite = context.suites.add(gx.ExpectationSuite(name="transactions_suite"))
    
    # Add some basic expectations
    # Add comprehensive expectations for Phase 11
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01, max_value=1000000.0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="transaction_type", 
            value_set=["online", "pos", "atm", "wire"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_timestamp")
    )
    
    # Validate
    batch_parameters = {"dataframe": df}
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="transactions_validation",
            data=batch_definition,
            suite=suite
        )
    )
    
    log.info("Running validation on transactions...")
    results = validation_definition.run(batch_parameters=batch_parameters)
    
    if results.success:
        log.info("✅ Transactions data quality checks PASSED")
    else:
        log.error("❌ Transactions data quality checks FAILED")
        for res in results.results:
            if not res.success:
                log.error(f"  Failed expectation: {res.expectation_config.type}")
                log.error(f"  Result details: {res.result}")

def main():
    parser = argparse.ArgumentParser(description="Run Data Quality checks")
    parser.add_argument("--data_dir", type=Path, default=Path("data/raw"), help="Data directory")
    args = parser.parse_args()
    
    log.info("Starting Data Quality checks...")
    check_transactions(args.data_dir)
    log.info("Finished Data Quality checks.")

if __name__ == "__main__":
    main()
