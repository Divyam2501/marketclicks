# GKE Standard, zonal, one Spot node. See docs/deploy-design.md §4.1.

# Dedicated, minimal service account for the node, instead of the default
# compute service account (which has broad Editor access on the project).
resource "google_service_account" "gke_nodes" {
  account_id   = "gke-nodes"
  display_name = "GKE node service account (tickstore)"
}

resource "google_project_iam_member" "gke_nodes_default" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount" # logging + monitoring writes
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_project_iam_member" "gke_nodes_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.reader" # pull our image
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_container_cluster" "main" {
  name     = var.cluster_name
  location = var.zone # a zone, not a region: zonal cluster

  # Off on purpose: learning project, destroyed and recreated to save cost.
  deletion_protection = false

  # Create the cluster with no default node pool (supported on 1.36.4-gke.1469000+;
  # REGULAR channel default is newer). The only nodes are the pool below, so
  # node settings live in one place.
  initial_node_count = 0

  release_channel {
    channel = "REGULAR"
  }

  # Saves memory on the e2-small node; metrics go to Grafana Cloud later.
  monitoring_config {
    managed_prometheus {
      enabled = false
    }
  }
}

resource "google_container_node_pool" "spot" {
  name       = "spot-pool"
  cluster    = google_container_cluster.main.id
  location   = var.zone
  node_count = 1

  # Terraform owns that the pool exists; how many nodes run right now is set by
  # gcloud resize (0 outside market hours, 1 while streaming).
  lifecycle {
    ignore_changes = [node_count]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = var.machine_type
    spot            = true
    disk_size_gb    = 30
    disk_type       = "pd-standard"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"] # access governed by IAM roles above
  }
}
