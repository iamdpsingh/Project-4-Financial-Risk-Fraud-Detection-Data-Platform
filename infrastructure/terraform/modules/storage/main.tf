# GCS Bucket for raw batch data
resource "google_storage_bucket" "raw_data" {
  name                        = "${var.bucket_prefix}-raw-data"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true # Set to true for learning/demo purposes

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# GCS Bucket for Dataflow staging and temp files
resource "google_storage_bucket" "dataflow_temp" {
  name                        = "${var.bucket_prefix}-dataflow-temp"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 7 # Auto-delete temp files after 7 days
    }
    action {
      type = "Delete"
    }
  }
}
