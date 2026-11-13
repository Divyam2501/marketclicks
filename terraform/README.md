# terraform/

GKE cluster (zonal, one Spot node), node service account, Artifact Registry.
Design: `docs/deploy-design.md`.

One-time state bucket (Terraform can't create the bucket that holds its own state):

```bash
gcloud storage buckets create gs://market-data-stream-tfstate \
  --location=us-east1 --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://market-data-stream-tfstate --versioning
```

Then:

```bash
terraform init
terraform plan
terraform apply
$(terraform output -raw get_credentials_command)
```

Tear down when not needed (deletion protection is off): `terraform destroy`.
