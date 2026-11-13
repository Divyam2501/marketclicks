terraform {
  required_version = ">= 1.9"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.10"
    }
  }

  # State lives in GCS: survives losing the laptop, versioned for rollback,
  # and locked so two applies can't write at once. The bucket is created once
  # with gcloud (see README in this folder), since Terraform needs it to start.
  backend "gcs" {
    bucket = "market-data-stream-tfstate"
    prefix = "gke"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
