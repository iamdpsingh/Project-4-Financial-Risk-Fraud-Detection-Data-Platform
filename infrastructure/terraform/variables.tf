variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources into"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone to deploy resources into"
  type        = string
  default     = "us-central1-a"
}

variable "bucket_prefix" {
  description = "Prefix for GCS buckets to ensure global uniqueness"
  type        = string
  default     = "fr-data-platform"
}
