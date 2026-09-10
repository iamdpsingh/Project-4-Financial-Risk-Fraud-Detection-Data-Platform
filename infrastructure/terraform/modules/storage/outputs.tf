output "raw_bucket_name" {
  value = google_storage_bucket.raw_data.name
}

output "dataflow_bucket_name" {
  value = google_storage_bucket.dataflow_temp.name
}
