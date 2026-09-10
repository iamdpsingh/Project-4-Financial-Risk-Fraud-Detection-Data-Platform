terraform {
  required_version = ">= 1.7.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.15.0"
    }
  }

  # Uncomment and configure this block when you're ready to use GCS for remote state
  # backend "gcs" {
  #   bucket = "YOUR_STATE_BUCKET_NAME"
  #   prefix = "terraform/state"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
}

module "storage" {
  source        = "./modules/storage"
  project_id    = var.project_id
  region        = var.region
  bucket_prefix = var.bucket_prefix
}

module "pubsub" {
  source     = "./modules/pubsub"
  project_id = var.project_id
  topic_name = "transaction-events"
}

module "bigquery" {
  source            = "./modules/bigquery"
  project_id        = var.project_id
  region            = var.region
  staging_dataset   = "staging"
  analytics_dataset = "analytics"
}
