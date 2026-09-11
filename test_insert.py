import datetime
import uuid

from google.cloud import bigquery

client = bigquery.Client()
table_id = "financial-data-platform-508216.analytics.transaction_risk"

now = datetime.datetime.now(datetime.UTC)
record = {
    "event_id": str(uuid.uuid4()),
    "transaction_id": str(uuid.uuid4()),
    "customer_id": "CUST-001",
    "account_id": "ACC-001",
    "merchant_id": "MERCH-001",
    "device_id": "DEV-001",
    "merchant_category": "retail",
    "merchant_risk_category": "low",
    "transaction_timestamp": now.isoformat(),
    "amount": 150.0,
    "currency": "USD",
    "amount_usd": 150.0,
    "transaction_type": "purchase",
    "country": "US",
    "city": "New York",
    "payment_method": "card",
    "status": "completed",
    "ip_address": "192.168.1.1",
    "customer_country": "US",
    "is_international": "false",
    "emitted_at": now.isoformat(),
    "risk_score": 5,
    "risk_level": "LOW",
    "signals_triggered": ""
}

# Emulate what Dataflow ignore_unknown_columns does
schema = client.get_table(table_id).schema
schema_names = [f.name for f in schema]
clean_record = {k: v for k, v in record.items() if k in schema_names}

errors = client.insert_rows_json(table_id, [clean_record])
print("Errors:", errors)
