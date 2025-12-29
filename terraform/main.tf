terraform {
  cloud {
    organization = "svet-g-org"
    workspaces {
      name = "payment-processing-pipeline-svet-g"
    }
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = "payment-processing-pipeline"
  region  = "europe-west2"
}

############################
# Storage Bucket
############################

resource "google_storage_bucket" "pipeline_bucket" {
  name          = "payment-processing-pipeline-svet-g"
  location      = "EU"
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  retention_policy {
    retention_period = 604800  # 7 days
    is_locked        = false
  }
}

############################
# Service Account
############################

resource "google_service_account" "pipeline_sa" {
  account_id   = "payment-pipeline-sa"
  display_name = "Payment Processing Pipeline Service Account"
}

############################
# Least-Privilege IAM
############################

resource "google_storage_bucket_iam_member" "pipeline_bucket_access" {
  bucket = google_storage_bucket.pipeline_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

############################
# Service Account Key
############################

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
  keepers = {
    last_rotated = timestamp()
  }
}

############################
# Secret Manager Storage
############################

resource "google_secret_manager_secret" "pipeline_sa_secret" {
  secret_id = "payment-pipeline-sa-key"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "pipeline_sa_secret_version" {
  secret      = google_secret_manager_secret.pipeline_sa_secret.id
  secret_data = google_service_account_key.pipeline_sa_key.private_key
}

############################
# Allow pipeline to read the secret
############################

resource "google_secret_manager_secret_iam_member" "pipeline_secret_access" {
  secret_id = google_secret_manager_secret.pipeline_sa_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_sa.email}"
}