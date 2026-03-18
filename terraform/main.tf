terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  # On utilise la variable définie dans variables.tf
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "data-lake-bucket" {
  # Le nom du bucket vient de la variable
  name     = var.gcs_bucket_name
  location = var.location

  # Paramètres recommandés par le cours
  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // jours
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
}