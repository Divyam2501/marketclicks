# Deploy Design — GKE for alpaca_feed

**Module:** `src/tickstore/ingest/alpaca_feed.py` deployed as a long-running pod on GKE
**Status:** Designed Nov 9–10, 2026; **deployed and verified Nov 12, 2026**
**Author:** Divyam Kaushik
**Related:** `docs/ingestion-design.md` (reconnect semantics), `docs/producer-design.md` (Kafka producer)

---

## 1. Purpose

*(GKE deployment provides the infinitely running streaming python processes a place to run, and be managed by Kubernetes directly. POD processes would rebuild on failures and allow for visible health check on the process, at the same time the cluster itself can be resized to bigger nodes as trading volume increases with added symbols (the ingest pod stays at one replica, see §4.4), and GKE also supports development across multiple developers sharing the same shared OS/dependencies build through the dockerfile, so development is in sync)*

---

## 2. Scope

### In scope
- Single-node zonal GKE cluster (cheapest tier) in us-east1-b for Confluent-proximity
- Spot node pool (cost control), scaled to zero outside market hours (§4.9)
- One Deployment running `alpaca_feed` with replicas=1
- Kubernetes Secret holding ALPACA + CONFLUENT credentials
- Plain-text stdout logs auto-captured by Cloud Logging
- Infrastructure defined as Terraform, state in GCS (§4.8)

### Out of scope (picked up later)
- Horizontal autoscaling (irrelevant with replicas=1, and single-producer semantics matter for ordering)
- Prometheus metrics endpoint + Grafana dashboards
- Structured JSON logs
- Health / readiness probes (§4.7)
- Multi-region / multi-zone HA (overkill for a solo project)
- Service mesh / Istio (not needed)

---

## 3. Failure modes considered

| Mode | Example | How we respond |
|---|---|---|
| Pod crashes | Python unhandled exception | Deployment restarts the pod on the same node; image is cached, so restart takes seconds. Process exits 1 |
| Auth failure exit | Bad Alpaca key, or 406 connection limit, twice in a row | Queued messages are flushed, then the process exits 1. Pod crash-loops visibly; `kubectl describe pod` shows the exit code |
| Node preemption | GCP reclaims the Spot node | WebSocket always drops. Pod is Pending until a new node is created and pulls the image (cold start, §4.9; not yet measured). Alpaca has no replay, so trades in the gap are lost; fill with `alpaca_backfill`. Overlapping backfill creates duplicates, since idempotence only covers one producer session; downstream must dedup. Queued messages are flushed only if shutdown fits the ~15s preemption window (app budget: 13s) |
| Routine pod termination | Every deploy (`Recreate` stops the old pod first), `kubectl delete`, GKE node auto-upgrade / maintenance drain, scale-to-zero | Kubernetes sends SIGTERM, then SIGKILL after `terminationGracePeriodSeconds` (20s). Happens on on-demand nodes too, and far more often than preemption. The process handles SIGTERM (Python as PID 1 ignores it by default) and exits 0. **Verified Nov 12** for `rollout restart` and for the node drain during scale-down |
| Deploy overlap | Rollout starts the new pod before the old one stops | Alpaca allows one stream connection, so the second gets error 406; prevented by the `Recreate` strategy (§4.4). Verified Nov 12: new pod subscribed 2s after the old one exited, no 406 |
| Node scaled to zero (by design) | Outside market hours | Pod sits Pending, no ingest; no trades are lost because the market is closed. Starts automatically when a node is added (§4.9) |
| Not enough memory on node | e2-small has ~300 MiB left after GKE system pods (§4.1) | Pod stays Pending (unschedulable) or is OOM-killed above its 256Mi limit. Node must be upgraded before any other workload joins |
| Container config rejected | `runAsNonRoot` with a non-numeric image `USER` | Pod stuck in `CreateContainerConfigError` (hit Nov 12). Prevented by a numeric UID in both the Dockerfile and the manifest (§4.3) |
| Image pull fail | Typo in tag, registry unreachable, node SA missing registry role | Pod `ImagePullBackOff`; surfaces clearly |
| Secret missing / stale | K8s Secret not created, env vars missing | Python startup fails with KeyError; pod crash-loops |
| Cluster-wide failure | GKE control plane outage (rare) | Everything stops until GCP restores; manual investigation |

---

## 4. Design decisions

Each decision has a **What** (committed) and a **Why** (to be written in own voice).

### 4.1 Cluster type and size

**What:** GKE Standard (not Autopilot), zonal cluster `tickstore` in us-east1-b on the REGULAR release channel (1.36.4 at creation), with a single node pool `spot-pool` of 1 Spot `e2-small` node (2 burstable vCPU, 2 GB RAM, 30 GB standard disk). The cluster is created with no default node pool (`initial_node_count = 0`), so all node settings live in `spot-pool`. Zonal, not regional: a regional cluster places the node count in each of three zones, and GKE's free-tier management credit covers only zonal clusters. Spot, not preemptible: both can be reclaimed by GCP at any time, but preemptible VMs are also always stopped after 24 hours, which would force a daily feed gap; Spot has no such cap. Spot does not avoid preemption; avoiding it entirely requires on-demand nodes at full price, and we accept occasional preemption for the cost saving (see §3). GKE Managed Prometheus is turned off to save node memory.

Measured on the node (Nov 12): allocatable 940m CPU / 1.33 GiB memory; GKE system pods request 521m CPU / 1.02 GiB (77% of memory), leaving about 300 MiB for workloads; actual node memory use 88%. The ingest pod uses ~31 MiB idle. The node is upgraded to `e2-standard-4` (4 vCPU, 16 GB) before a stream processor (Flink or Spark, undecided) joins the cluster.

**Why:** *(GKE standard is a better choice for a single developer project, as autopilot allows for managed services but with a premium cost as well as minimum resource pod allocation, which can be costly, and is not needed for the scope of this project. The single node pool with 1 preemptible small node is an option but SPOT VM is the most cost-effective option. With GKE self-healing and our inherent Alpaca connect retry loop, even if Google takes the node away, the process will not fail, but start back within a few minutes, which is a real trade off. So we should use SPOT VMs, as compared to preemptile, it does not have a daily shutdown. e2-small is enough for the single ingest pod today, but Flink's JobManager and TaskManager need far more memory, so the node moves to e2-standard-4 when Flink is deployed)*

---

### 4.2 Secret management

**What:** Kubernetes Secret `alpaca-feed-env`, created directly from the local `.env` with `kubectl create secret generic --from-env-file`, so no secret file ever exists on disk or in git. It holds only the five keys the feed uses (ALPACA_API_KEY, ALPACA_API_SECRET, CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET); the deprecated Polygon key is filtered out. Injected into the pod as environment variables (`envFrom`). `k8s/secret.yaml.example` documents the expected keys only. Rotation is delete + recreate + `kubectl rollout restart`; `kubectl apply` is not used for this Secret because it merges and never removes keys it wasn't tracking (hit Nov 12). Not using GCP Secret Manager yet.

**Why:** *(For this Solo project K8 secrets is enough as it saves us the overhead of configuring google IAMs and auth loops on every POD connect. Using K8 secrets does expose the decrypted env API keys to the POD, but for a Solo developer, it is only exposed to me and it is already safe. The better way would be to do it in production pulling secret from google secret manager)*

---

### 4.3 Image build pipeline

**What:** Local `docker build --platform linux/amd64` (Mac is arm64, GKE nodes are x86), push to Artifact Registry `us-east1-docker.pkg.dev/market-data-stream/tickstore` via `docker push`. No CI-driven builds yet. Two-stage Dockerfile: a builder stage with uv runs `uv sync --locked --no-dev --no-editable` into `/app/.venv` (dependencies in their own layer, so code changes don't reinstall them); the runtime stage is `python:3.13-slim` plus the venv only, with no uv and no source tree (~50 MB). Runs as a non-root user with a fixed numeric UID/GID 999 (`USER 999:999`), because Kubernetes `runAsNonRoot` can only verify a numeric user. Exec-form `CMD` so the venv's python is PID 1 and receives SIGTERM directly. `.dockerignore` keeps `.env`, the local macOS venv and local notes out of the build context. Images are tagged with the git short SHA of the code inside (e.g. `alpaca-feed:1fea306`), never `:latest`, so the running code is identifiable and the node reuses its cached image on restarts.

**Why:** *(During the early iteration phase, a manual Docker workflow is preferred over a CI pipeline to maximize developer iteration speed and avoid the latency of forcing a GitHub push for every microscopic image test. Manual builds allow you to compile code changes locally, quickly bump version tags, and push directly to Google Artifact Registry for immediate GKE deployment without overhead. However, introducing CI/CD complexity becomes highly valuable once the image requires daily rebuilds or a second developer joins the team, shifting the system from experimental debugging to a stable, automated, and audited release path.)*

*(For Docker, it is in two stages: docker build, which has uv, but uv is not required during run-time, as the docker builder builds the .venv with the uv-locked packages. uv's job ends at build time, and once that is done, the new PODs only pull the latest image. The build only needs to be done at the time of code changes. Image re-pull to a POD depends on HOW the POD crashed: if the POD crashed due to an intermittent issue, it restarts on the same node, where the image is already cached, so no pull is needed and it starts quicker. However, if it was a preemption, the node itself was replaced by a new VM that does not have the image, so it needs to pull from Artifact Registry, and a smaller image means a shorter feed gap.)*

---

### 4.4 Deployment vs single Pod

**What:** Kubernetes `Deployment` `alpaca-feed` with `replicas: 1`, `restartPolicy: Always` (via the Deployment controller), and `strategy: Recreate` so the old pod's Alpaca connection closes before the new pod opens one. Not a bare Pod. `terminationGracePeriodSeconds: 20` covers the app's 13s shutdown budget with margin (Spot preemption has its own fixed window). Resources: request 100m CPU / 128Mi memory (fits the ~300 MiB headroom on e2-small), memory limit 256Mi, no CPU limit. Security context: `runAsNonRoot` with `runAsUser`/`runAsGroup` 999 (must match the Dockerfile), read-only root filesystem, no privilege escalation, all Linux capabilities dropped.

**Why:** *(Deployment is correct even with 1 replicas as deployment allows us to take advantage of the self-healing capability of GKE. We do not do replicas >1 as in our use case, if replicas>1, GKE would open multiple connections with Alpaca, and Alpaca allows only one stream connection on our plan, so the second is rejected with error 406 (connection limit exceeded). The same applies during deploys, which is why the strategy is Recreate rather than the default RollingUpdate. Even if the replicas work successfully, there would be message duplicates. A better way to do replicas > 1 would be to divide based on symbols.)*

---

### 4.5 Logging

**What:** Python logging to stdout (already configured). GKE's default Cloud Logging agent captures stdout and routes it to Google Cloud Logging; verified Nov 12 (shutdown logs from a drained pod were readable with `gcloud logging read` after the pod was gone). Container timestamps are UTC. No structured JSON logs yet.

**Why:** *(Relying on the default Google Cloud Logging integration for Day 5 is the ideal choice because GKE automatically captures standard container stdout streams out of the box without requiring extra agent configurations, easily staying within Cloud Logging's generous free tier (first 50 GiB/month) for a 10-symbol feed. Unlike raw terminal stdout tails that scroll endlessly and vanish when a Pod restarts, Cloud Logging indexes your log entries for 30 days. For Day 6, the seamless upgrade path is switching your Python logging formatter to output structured JSON payloads (using a library like python-json-logger), which Cloud Logging automatically parses so you can query specific fields like jsonPayload.symbol="NVDA" or jsonPayload.latency_ms > 50 directly in the console.)*

---

### 4.6 Networking

**What:** Default VPC, no custom network policies. Pod can reach Confluent Cloud (public internet) and Alpaca (public internet) via default egress. No ingress required — this is a producer, not a server.

**Why:** *(This standalone project does not need any Ingress, as this is strictly a producer of market ticks sent over to confluent. As for VPC, setting it up is very complex with multiple DNS configs in place, and also costs extra money to give that backdoor for data movement avoiding the public internet. Traffic is still encrypted over the public internet: our Kafka producer connects to Confluent with SASL_SSL, and the Alpaca stream uses wss (TLS). Confluent Basic clusters also only offer public endpoints; private networking would need a Dedicated cluster. If we later want to add in a Prometheus metrics endpoint, we would ask GKE to include an internal cluster service mapping to allow to look into the inside-the-cluster monitoring scrapes without exposing the application to the public.)*

---

### 4.7 Health probes

**What:** **None yet.** Deferred until after the first weeks of running on GKE.

**Why:** *(Readiness and liveness probes are skipped today because the application is under active manual observation during initial deployment, and setting up premature probes risks triggering false-positive Pod restarts while tuning connection backoffs. For a streaming producer, a meaningful liveness probe must go beyond checking if the Python process is simply alive—it needs to verify that the WebSocket connection is alive (ping/pong) and the Kafka producer is successfully flushing batches to Confluent Cloud without stalling. It must not check that ticks are arriving: there are no trades outside market hours, so a tick-based probe would restart the Pod every night. By Day 7, the production implementation will expose a lightweight internal HTTP health endpoint (or update a local heartbeat timestamp file) that confirms connection and producer health, allowing GKE to automatically kill and restart the Pod if the pipeline ever silently deadlocks. Trade-flow staleness becomes a market-hours-aware alert instead of a probe.)*

---

### 4.8 Infrastructure as code (Terraform)

**What:** Terraform with the `google` provider pinned `~> 8.10` (8.11.0 in `.terraform.lock.hcl`, which is committed). Creates six resources: the cluster, the node pool, Artifact Registry, and a dedicated node service account `gke-nodes` with two IAM roles. State lives in the GCS bucket `gs://market-data-stream-tfstate` (us-east1, uniform bucket-level access, public access prevention enforced, object versioning on); the GCS backend also provides state locking. The bucket is created once with `gcloud`, because Terraform cannot create the bucket that holds its own state. Cluster `deletion_protection = false`, so `terraform destroy` works. The node runs as `gke-nodes` (roles `container.defaultNodeServiceAccount` for logs/metrics and `artifactregistry.reader` to pull images) instead of the project's default compute service account, which has broad Editor access. The node pool ignores `node_count` changes (`lifecycle { ignore_changes = [node_count] }`) so scaling with `gcloud` (§4.9) doesn't show up as drift. Terraform creates infrastructure only; what runs inside the cluster is deployed with `kubectl` and `k8s/`. First apply took 4m42s (cluster 3m18s, node pool 1m23s).

**Why:** *(State file: to be written.)*

*(Deletion protection: For this project I would keep it off, as we don't have any downstream users and this is in terms of testing and learning, so we can save cost. For downstream users, if someone accidentally does a terraform destroy, we don't want the node or cluster to get destroyed immediately as a safety in production.)*

---

### 4.9 Cost: scale to zero outside market hours

**What:** The node pool runs 1 node while streaming and is scaled to 0 nodes outside market hours with `gcloud container clusters resize tickstore --node-pool spot-pool --num-nodes 0|1 --zone us-east1-b`. At 0 nodes the VM and its boot disk are gone; the Deployment's pod waits in Pending and starts automatically when a node is added, with no manifest change. Remaining idle cost is the cluster management fee (believed to be covered by GKE's free-tier credit for one zonal cluster; not yet verified against the pricing page) plus small Artifact Registry and GCS storage. Scale-down took 165s (Nov 12), and the drained pod shut down gracefully (exit 0). Cold start (0 → 1 node → pod Running) is the same sequence as preemption recovery and will be measured at the next scale-up. Manual for now; later automated on a market-hours schedule.

**Why:** *(to be written)*

---

## 5. What a deploy looks like (operational flow)

```bash
# One-time setup
gcloud storage buckets create gs://market-data-stream-tfstate --location=us-east1 \
  --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://market-data-stream-tfstate --versioning
cd terraform && terraform init && terraform plan && terraform apply
$(terraform output -raw get_credentials_command)    # wires kubectl to the cluster
kubectl create secret generic alpaca-feed-env --from-env-file=.env

# Per deploy (tag = git short SHA of committed code)
TAG=$(git rev-parse --short HEAD)
REPO=$(terraform -chdir=terraform output -raw image_repository)
docker build --platform linux/amd64 -t $REPO/alpaca-feed:$TAG .
docker push $REPO/alpaca-feed:$TAG
# update the image tag in k8s/deployment.yaml, then:
kubectl apply -f k8s/deployment.yaml              # Recreate: old pod stops, new tag starts
kubectl rollout status deployment/alpaca-feed
kubectl logs -f deployment/alpaca-feed            # tail live

# Daily
gcloud container clusters resize tickstore --node-pool spot-pool --num-nodes 1 --zone us-east1-b  # before open
gcloud container clusters resize tickstore --node-pool spot-pool --num-nodes 0 --zone us-east1-b  # after close
```

---

## 6. Future work (explicitly deferred)

- Measure cold start (0 → 1 node → pod Running); record it in §3 and §4.9
- Automate scale up/down on a market-hours schedule
- Prometheus metrics endpoint + Grafana
- Structured JSON logging for Cloud Logging queries
- Readiness + liveness probes (§4.7)
- GCP Secret Manager for credential rotation
- Workload Identity (instead of embedded API keys) for GCP service auth
- CI-driven image builds (§4.3)
- Private VPC peering to Confluent Cloud (if ever production-serious)
- Horizontal scaling with per-symbol partition assignment (if the single-WebSocket model ever becomes insufficient)

---

## 7. Open questions I'd flag in a real design review

*(what if the WebSocket connection drops across a node preemption (The WebSocket connection always drops during a node preemption. Compared to a pod crash, recovery takes minutes because a new node has to be created and pull the image, the graceful shutdown must fit inside the ~15s SIGTERM window, and the trade gap has to be backfilled, see §3)? What are we not metrics-ing today that we'd want? Could this architecture scale to 100 symbols without redesign?)*

---

## 8. Files

- `terraform/versions.tf` — Terraform + provider versions, GCS backend
- `terraform/main.tf` — GKE cluster, Spot node pool, node service account + IAM
- `terraform/artifact_registry.tf` — image registry
- `terraform/variables.tf` — project ID, region, zone, cluster name, machine type
- `terraform/outputs.tf` — get-credentials command, image repository prefix
- `terraform/.terraform.lock.hcl` — pinned provider version
- `terraform/README.md` — one-time bucket setup + commands
- `Dockerfile` — two-stage build for alpaca_feed
- `.dockerignore` — keeps secrets, local venv and notes out of the build context
- `k8s/deployment.yaml` — the Deployment manifest
- `k8s/secret.yaml.example` — documents the Secret's keys (the real Secret is created from `.env`, never a file)
