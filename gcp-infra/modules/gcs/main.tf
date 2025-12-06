resource "google_storage_bucket" "gcs_bucket" {
  name          = var.bucket_name
  location      = var.location
  storage_class = var.storage_class
  force_destroy = true
}
