output "raw_data_bucket" {
  value       = module.storage.raw_bucket_name
  description = "The GCS bucket for raw extracted data"
}

output "dataflow_staging_bucket" {
  value       = module.storage.dataflow_bucket_name
  description = "The GCS bucket for Dataflow temp and staging files"
}

output "pubsub_topic" {
  value       = module.pubsub.topic_name
  description = "The Pub/Sub topic for streaming transaction events"
}

output "dataflow_service_account" {
  value       = module.iam.dataflow_sa_email
  description = "The Service Account email used by Dataflow jobs"
}

output "bigquery_staging_dataset" {
  value       = module.bigquery.staging_dataset_id
  description = "The BigQuery dataset for staging batch data"
}

output "bigquery_analytics_dataset" {
  value       = module.bigquery.analytics_dataset_id
  description = "The BigQuery dataset for analytics and reporting"
}
