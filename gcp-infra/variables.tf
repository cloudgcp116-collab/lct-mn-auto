variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Default region for GCP resources"
  type        = string
  default     = "us-central1"
}

# GCS Variables
variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "storage_class" {
  description = "Storage class of the bucket"
  type        = string
  default     = "STANDARD"
}

variable "gcs_location" {
  description = "Location for the GCS bucket"
  type        = string
  default     = "US"
}

# BigQuery Variables
variable "dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
}

variable "dataset_location" {
  description = "Location of BigQuery dataset"
  type        = string
  default     = "US"
}

variable "table_name" {
  description = "BigQuery table name"
  type        = string
}

variable "table_schema" {
  description = "Schema for BigQuery table (JSON string)"
  type        = string
}
