output "dataflow_sa_email" {
  value = google_service_account.dataflow_worker.email
}

output "airflow_sa_email" {
  value = google_service_account.airflow_worker.email
}
