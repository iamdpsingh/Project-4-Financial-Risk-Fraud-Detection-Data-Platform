# Staging Dataset (for raw/batch data)
resource "google_bigquery_dataset" "staging" {
  dataset_id                 = var.staging_dataset
  friendly_name              = "Staging Dataset"
  description                = "Dataset for batch ingested raw data"
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = true # For learning/demo purposes
}

# Analytics Dataset (for clean, enriched, and scored data)
resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.analytics_dataset
  friendly_name              = "Analytics Dataset"
  description                = "Dataset for enriched and risk-scored transaction data"
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = true
}

# Example Table: Transaction Risk (Streaming Output)
resource "google_bigquery_table" "transaction_risk" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "transaction_risk"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "transaction_timestamp"
  }

  schema = <<EOF
[
  {
    "name": "transaction_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "customer_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "account_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "merchant_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "device_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "transaction_timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "amount",
    "type": "FLOAT64",
    "mode": "REQUIRED"
  },
  {
    "name": "currency",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "amount_usd",
    "type": "FLOAT64",
    "mode": "NULLABLE"
  },
  {
    "name": "transaction_type",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "country",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "city",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payment_method",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "ip_address",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "is_international",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "risk_score",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "risk_level",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "signals_triggered",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}
