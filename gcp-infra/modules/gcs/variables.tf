variable "bucket_name" {
  type        = string
  description = "GCS Bucket Name"
}

variable "location" {
  type        = string
  description = "Bucket location"
}

variable "storage_class" {
  type        = string
  description = "Bucket storage class"
}
