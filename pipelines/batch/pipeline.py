"""
Apache Beam batch pipeline for financial data ingestion.

Reads raw CSV files from GCS (or local), validates them, applies basic
transformations, and loads the data into BigQuery staging tables.
"""

import logging
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from transforms import ParseCSVLine, ValidateRecord, TransformForBigQuery

log = logging.getLogger("batch_pipeline")

# Headers for parsing the CSV files
HEADERS = {
    "customers": ["customer_id","name","email","phone","country","city","customer_segment","date_of_birth","created_at","updated_at"],
    "accounts": ["account_id","customer_id","account_type","account_status","currency","opened_at","closed_at","created_at","updated_at"],
    "merchants": ["merchant_id","merchant_name","merchant_category","mcc_code","country","city","risk_category","created_at","updated_at"],
    "devices": ["device_id","customer_id","device_type","os","browser","ip_address","user_agent","first_seen_at","last_seen_at","created_at"],
    "transactions": ["transaction_id","customer_id","account_id","merchant_id","device_id","transaction_timestamp","amount","currency","amount_usd","transaction_type","country","city","payment_method","status","ip_address","is_international"],
    "transaction_events": ["event_id","transaction_id","event_type","event_timestamp","event_metadata","created_at"]
}

class IngestionPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_dir",
            required=True,
            help="Path to the directory containing input CSVs (e.g. gs://my-bucket/raw/ or data/extracted/)"
        )
        parser.add_argument(
            "--dataset",
            default="staging",
            help="BigQuery dataset to write to"
        )
        parser.add_argument(
            "--project_id",
            help="GCP Project ID"
        )
        parser.add_argument(
            "--output_local",
            action="store_true",
            help="If set, writes to local JSON instead of BigQuery (useful for local dev without GCP)"
        )

def run_pipeline(argv=None):
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    custom_options = pipeline_options.view_as(IngestionPipelineOptions)
    input_dir = custom_options.input_dir.rstrip("/")
    dataset = custom_options.dataset
    project = custom_options.project_id
    output_local = custom_options.output_local

    with beam.Pipeline(options=pipeline_options) as p:
        for table, headers in HEADERS.items():
            folder = "events" if table == "transaction_events" else table
            input_pattern = f"{input_dir}/{folder}/*.csv"
            
            # 1. Read CSV lines
            lines = p | f"Read_{table}" >> beam.io.ReadFromText(input_pattern)
            
            # 2. Parse CSV
            parsed = lines | f"Parse_{table}" >> beam.ParDo(ParseCSVLine(headers))
            
            # 3. Validate
            validated = parsed | f"Validate_{table}" >> beam.ParDo(ValidateRecord(table)).with_outputs("valid", "invalid")
            
            valid_records = validated.valid
            invalid_records = validated.invalid
            
            # 4. Transform for BigQuery
            transformed = valid_records | f"Transform_{table}" >> beam.ParDo(TransformForBigQuery())
            
            # 5. Write valid records
            if output_local:
                output_path = f"data/staged/{table}.json"
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                transformed | f"WriteLocal_{table}" >> beam.io.WriteToText(
                    file_path_prefix=f"data/staged/{table}",
                    file_name_suffix=".jsonl",
                    shard_name_template="",
                    num_shards=1
                )
            else:
                table_spec = f"{project}:{dataset}.{table}"
                transformed | f"WriteBQ_{table}" >> beam.io.WriteToBigQuery(
                    table=table_spec,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            
            # 6. Handle invalid records (dead letter queue)
            if output_local:
                invalid_records | f"WriteInvalidLocal_{table}" >> beam.io.WriteToText(
                    file_path_prefix=f"data/dlq/{table}_errors",
                    file_name_suffix=".jsonl",
                    shard_name_template="",
                    num_shards=1
                )
            else:
                error_table_spec = f"{project}:{dataset}.data_quality_errors"
                invalid_records | f"WriteErrorsBQ_{table}" >> beam.io.WriteToBigQuery(
                    table=error_table_spec,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
