# Dataflow Worker Service Account
resource "google_service_account" "dataflow_worker" {
  account_id   = "dataflow-worker-sa"
  display_name = "Dataflow Worker Service Account"
  project      = var.project_id
}

# Grant Dataflow worker permissions
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Grant BigQuery Data Editor to Dataflow
resource "google_project_iam_member" "dataflow_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Grant Pub/Sub Subscriber to Dataflow
resource "google_project_iam_member" "dataflow_pubsub_sub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Grant Pub/Sub Viewer to Dataflow
resource "google_project_iam_member" "dataflow_pubsub_viewer" {
  project = var.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Storage Admin for Dataflow (needs to write to staging/temp buckets)
resource "google_project_iam_member" "dataflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# -------------------------------------------------------------------------
# Airflow (Composer) Worker Service Account
# -------------------------------------------------------------------------
resource "google_service_account" "airflow_worker" {
  account_id   = "airflow-worker-sa"
  display_name = "Airflow Worker Service Account"
  project      = var.project_id
}

# Airflow needs to trigger Dataflow jobs
resource "google_project_iam_member" "airflow_dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.airflow_worker.email}"
}

# Airflow needs to act as the Dataflow worker SA
resource "google_service_account_iam_member" "airflow_act_as_dataflow" {
  service_account_id = google_service_account.dataflow_worker.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.airflow_worker.email}"
}
