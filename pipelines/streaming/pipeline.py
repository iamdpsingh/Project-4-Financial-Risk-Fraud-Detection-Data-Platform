"""
Apache Beam streaming pipeline for real-time fraud detection.

Reads transaction events from Pub/Sub, enriches them with customer/merchant
context, scores them for fraud risk using the Rule Engine, and writes the
results to BigQuery.
"""

import logging
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from dotenv import load_dotenv
from transforms import EnrichTransaction, FormatForBigQuery, ParsePubSubMessage, ScoreFraudRisk

load_dotenv()

# Configure logging to write to both console and a file in the logs/ directory
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "streaming_pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("streaming_pipeline")

class StreamingPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            help="Pub/Sub subscription to read from (e.g. projects/my-project/subscriptions/my-sub)"
        )
        parser.add_argument(
            "--output_topic",
            help="Pub/Sub topic to write scored transactions to (optional)"
        )
        parser.add_argument(
            "--dataset",
            default="analytics",
            help="BigQuery dataset to write to"
        )
        parser.add_argument(
            "--project_id",
            help="GCP Project ID"
        )
        parser.add_argument(
            "--output_local",
            action="store_true",
            help="If set, writes to local JSON instead of BigQuery (useful for local dev)"
        )

def run_pipeline(argv=None):
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    # Enable streaming
    pipeline_options.view_as(StandardOptions).streaming = True
    
    custom_options = pipeline_options.view_as(StreamingPipelineOptions)
    input_subscription = custom_options.input_subscription
    output_local = custom_options.output_local
    dataset = custom_options.dataset
    project = custom_options.project_id

    with beam.Pipeline(options=pipeline_options) as p:
        
        if input_subscription:
            messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        else:
            # If no subscription is provided, we can read from a mock unbounded source or local file for testing
            log.warning("No Pub/Sub subscription provided. Reading from mock data for local testing.")
            # For local testing without pubsub, we'll just read the generated transactions CSV
            # However, textio isn't streaming. We'd use a custom generator or file watcher.
            # In Phase 3, we usually just want to see it work.
            messages = (
                p 
                | "CreateMockStream" >> beam.Create([
                    b'{"transaction_id": "1", "amount_usd": 5000, "is_international": "true", "merchant_risk_category": "high"}',
                    b'{"transaction_id": "2", "amount_usd": 15, "is_international": "false", "merchant_risk_category": "low"}'
                ])
            )
            
        # 1. Parse JSON
        parsed = messages | "ParseJSON" >> beam.ParDo(ParsePubSubMessage()).with_outputs("invalid", main="valid")
        valid_records = parsed.valid
        invalid_records = parsed.invalid
        
        # 2. Enrich
        enriched = valid_records | "Enrich" >> beam.ParDo(EnrichTransaction())
        
        # 3. Score Risk
        scored = enriched | "ScoreRisk" >> beam.ParDo(ScoreFraudRisk()).with_outputs("errors", main="scored")
        successfully_scored = scored.scored
        scoring_errors = scored.errors
        
        # 4. Format for BigQuery
        final_records = successfully_scored | "FormatForBQ" >> beam.ParDo(FormatForBigQuery())
        
        # 5. Output
        if output_local:
            # When streaming locally to text files, Beam needs windowing, 
            # but for debugging we can just print it.
            final_records | "PrintToConsole" >> beam.Map(lambda x: log.info(f"SCORED TXN: {x}"))
            invalid_records | "LogInvalid" >> beam.Map(lambda x: log.error(f"INVALID MESSAGE: {x}"))
            scoring_errors | "LogScoringError" >> beam.Map(lambda x: log.error(f"SCORING ERROR: {x}"))
        else:
            table_spec = f"{project}:{dataset}.transaction_risk"
            dlq_spec = f"{project}:{dataset}.transactions_dlq"
            
            final_records | "WriteToBQ" >> beam.io.WriteToBigQuery(
                table=table_spec,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
            
            # Format DLQ records
            def format_dlq(record, error_type):
                import json
                from datetime import datetime
                return {
                    "raw_record": json.dumps(record) if isinstance(record, dict) else str(record),
                    "error_type": error_type,
                    "timestamp": datetime.utcnow().isoformat()
                }

            # Write DLQ items to BigQuery
            (
                invalid_records 
                | "FormatInvalidDLQ" >> beam.Map(lambda x: format_dlq(x, "JSON_PARSE_ERROR"))
                | "WriteInvalidToDLQ" >> beam.io.WriteToBigQuery(
                    table=dlq_spec,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
            
            (
                scoring_errors 
                | "FormatScoringDLQ" >> beam.Map(lambda x: format_dlq(x, "SCORING_ERROR"))
                | "WriteScoringToDLQ" >> beam.io.WriteToBigQuery(
                    table=dlq_spec,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
