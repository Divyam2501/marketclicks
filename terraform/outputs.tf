output "get_credentials_command" {
  description = "Wire kubectl to the cluster"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.main.name} --zone ${var.zone} --project ${var.project_id}"
}

output "image_repository" {
  description = "Prefix for docker tag/push"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}
