resource "google_artifact_registry_repository" "images" {
  repository_id = "tickstore"
  location      = var.region
  format        = "DOCKER"
  description   = "tickstore container images"
}
