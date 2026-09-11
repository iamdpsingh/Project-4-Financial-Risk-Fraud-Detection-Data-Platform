"""
Apache Beam DoFn classes and transformations for the batch pipeline.
"""

import logging
import typing
from typing import Any

import apache_beam as beam

log = logging.getLogger("batch_pipeline")

class ParseCSVLine(beam.DoFn):
    """Parses a CSV string into a dictionary given a list of headers."""
    def __init__(self, headers: list[str]):
        self.headers = headers

    def process(self, element: str) -> "typing.Iterator[dict[str, Any]]":
        import csv
        from io import StringIO
        
        # Skip header lines
        if element.startswith(self.headers[0]):
            return

        try:
            reader = csv.reader(StringIO(element))
            row = next(reader)
            
            # Map row to headers
            record: dict[str, Any] = dict(zip(self.headers, row, strict=False))
            
            # Clean up empty strings to None/null for BigQuery
            for k, v in record.items():
                if v == "":
                    record[k] = None
                    
            yield record
        except Exception as e:
            log.error(f"Error parsing CSV line: {element} | Error: {e}")


class ValidateRecord(beam.DoFn):
    """
    Validates a parsed record.
    In a real system, this might call Great Expectations or use a schema.
    For this batch job, we just do basic type casting and null checks.
    """
    def __init__(self, table_name: str):
        self.table_name = table_name
        
    def process(self, record: dict[str, Any]):
        try:
            # Example basic validation/casting
            if self.table_name == "transactions":
                if record.get("amount"):
                    record["amount"] = float(record["amount"])
                if record.get("amount_usd"):
                    record["amount_usd"] = float(record["amount_usd"])
            
            # Additional validation logic would go here
            # For now, just pass it through
            yield beam.pvalue.TaggedOutput("valid", record)
            
        except Exception as e:
            log.error(f"Validation failed for record: {record} | Error: {e}")
            yield beam.pvalue.TaggedOutput("invalid", {"record": str(record), "error": str(e)})


class TransformForBigQuery(beam.DoFn):
    """
    Transforms the validated records into the exact format expected by BigQuery.
    Handles any necessary datatype conversions, timestamp formatting, etc.
    """
    def process(self, record: dict[str, Any]):
        # The schema definition in BigQuery will handle basic JSON-to-SQL types.
        # Ensure we don't pass complex objects if they aren't expected.
        # This is essentially a pass-through in this simplified setup.
        yield record
