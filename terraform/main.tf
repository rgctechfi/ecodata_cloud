terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "services" {
  for_each = toset([
    "iam.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
  ])

  service            = each.key
  disable_on_destroy = false
}

# Service account for ingestion
resource "google_service_account" "sa_ecodata" {
  account_id   = "bruin-ingestor"
  display_name = "Compte pour l'ingestion et Bruin"

  depends_on = [google_project_service.services]
}

# IAM roles for the service account
resource "google_project_iam_member" "sa_storage_object_admin" {
  project = var.project
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.sa_ecodata.email}"
}

resource "google_project_iam_member" "sa_bigquery_data_editor" {
  project = var.project
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.sa_ecodata.email}"
}

# GCS buckets (bronze + silver)
resource "google_storage_bucket" "bronze" {
  name     = var.gcs_bucket_bronze_name
  location = var.location

  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    layer = "bronze"
  }

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  depends_on = [google_project_service.services]
}

resource "google_storage_bucket" "silver" {
  name     = var.gcs_bucket_silver_name
  location = var.location

  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true
  force_destroy               = true

  labels = {
    layer = "silver"
  }

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }

  depends_on = [google_project_service.services]
}

# BigQuery dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location

  depends_on = [google_project_service.services]
}
