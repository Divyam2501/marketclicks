variable "project_id" {
  description = "GCP project that holds the cluster and registry"
  type        = string
  default     = "market-data-stream"
}

variable "region" {
  description = "Region for regional resources (Artifact Registry); matches Confluent's us-east1"
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "Zone for the zonal GKE cluster"
  type        = string
  default     = "us-east1-b"
}

variable "cluster_name" {
  type    = string
  default = "tickstore"
}

variable "machine_type" {
  description = "e2-small for the ingest pod alone; e2-standard-4 once Flink/Spark joins"
  type        = string
  default     = "e2-small"
}
