terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "gcs" {
  source        = "./modules/gcs"
  bucket_name   = var.bucket_name
  storage_class = var.storage_class
  location      = var.gcs_location
}

module "bigquery" {
  source             = "./modules/bigquery"
  dataset_id         = var.dataset_id
  location           = var.dataset_location 
  table_name         = var.table_name
  table_schema       = var.table_schema
}


module "composer_envs" {
source = "./modules/composer_envs"
composer_env_name = var.composer_env_name
composer_region   = var.composer_region
composer_sa       = var.composer_sa
composer_image    = var.composer_image
}

