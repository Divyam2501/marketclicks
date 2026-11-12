# Deploy Design — GKE for alpaca_feed

**Module:** `src/tickstore/ingest/alpaca_feed.py` deployed as a long-running pod on GKE
**Status:** Day 5 design (Nov 9, 2026), revised Nov 10
**Author:** Divyam Kaushik
**Related:** `docs/ingestion-design.md` (reconnect semantics), `docs/producer-design.md` (Kafka producer)

---

## 1. Purpose

*(GKE deployment provides the infinitely running streaming python processes a place to run, and be managed by Kubernetes directly. POD processes would rebuild on failures and allow for visible health check on the process, at the same time the cluster itself can be resized to bigger nodes as trading volume increases with added symbols (the ingest pod stays at one replica, see §4.4), and GKE also supports development across multiple developers sharing the same shared OS/dependencies build through the dockerfile, so development is in sync)*

---

## 2. Scope

### In scope (Day 5)
- Single-node zonal GKE cluster (cheapest tier) in us-east1-b for Confluent-proximity
- Spot node pool (cost control)
- One Deployment running `alpaca_feed` with replicas=1
- Kubernetes Secret holding ALPACA + CONFLUENT credentials
- Plain-text stdout logs auto-captured by Cloud Logging (structured JSON follows on Day 6, see §4.5)
- Infrastructure defined as Terraform

### Out of scope (today — picked up later)
- Horizontal autoscaling (irrelevant with replicas=1, and single-producer semantics matter for ordering)
- Prometheus metrics endpoint (Day 6)
- Grafana dashboards (Day 6)
- Multi-region / multi-zone HA (overkill for a solo project)
- Service mesh / Istio (not needed)
- Health / readiness probes (nice-to-have, deferred to Day 7 polish)

---

## 3. Failure modes considered

| Mode | Example | How we respond |
|---|---|---|
| Pod crashes | Python unhandled exception | Deployment restarts the pod automatically |
| Auth failure exit | Bad Alpaca key, process exits non-zero | Pod crash-loops visibly; `kubectl describe pod` surfaces it |
| Node preemption | GCP reclaims the Spot node | WebSocket always drops. Pod is Pending until a new node is created and pulls the image (minutes on a one-node cluster). Alpaca has no replay, so trades in the gap are lost; fill with `alpaca_backfill`. Overlapping backfill creates duplicates, since idempotence only covers one producer session; downstream must dedup. Queued messages are flushed only if SIGTERM is handled within the ~15s grace period |
| Routine pod termination | Every deploy (`Recreate` stops the old pod first), `kubectl delete`, GKE node auto-upgrade / maintenance drain | Kubernetes sends SIGTERM, then SIGKILL after `terminationGracePeriodSeconds`. Happens on on-demand nodes too, and far more often than preemption. The process must handle SIGTERM (Python as PID 1 ignores it by default) so the graceful shutdown runs |
| Deploy overlap | Rollout starts the new pod before the old one stops | Alpaca allows one stream connection, so the second gets error 406; prevented by the `Recreate` strategy (§4.4) |
| Image pull fail | Typo in tag, registry unreachable | Pod `ImagePullBackOff`; surfaces clearly |
| Secret missing / stale | K8s Secret not synced, env vars missing | Python startup fails with KeyError; pod crash-loops |
| Cluster-wide failure | GKE control plane outage (rare) | Everything stops until GCP restores; manual investigation |

---

## 4. Design decisions

Each decision has a **What** (committed) and a **Why** (to be written in own voice).

### 4.1 Cluster type and size

**What:** GKE Standard (not Autopilot), zonal cluster in us-east1-b, single node pool with 1 Spot `e2-small` node (2 burstable vCPU, 2 GB RAM). Zonal, not regional: a regional cluster places the node count in each of three zones, and GKE's free-tier management credit covers only zonal clusters. Spot, not preemptible: both can be reclaimed by GCP at any time, but preemptible VMs are also always stopped after 24 hours, which would force a daily feed gap; Spot has no such cap. Spot does not avoid preemption; avoiding it entirely requires on-demand nodes at full price, and we accept occasional preemption for the cost saving (see §3). Node is upgraded to `e2-standard-4` (4 vCPU, 16 GB) when Flink joins the cluster (~Nov 17).

**Why:** *(GKE standard is a better choice for a single developer project, as autopilot allows for managed services but with a premium cost as well as minimum resource pod allocation, which can be costly, and is not needed for the scope of this project. The single node pool with 1 preemptible small node is an option but SPOT VM is the most cost-effective option. With GKE self-healing and our inherent Alpaca connect retry loop, even if Google takes the node away, the process will not fail, but start back within a few minutes, which is a real trade off. So we should use SPOT VMs, as compared to preemptile, it does not have a daily shutdown. e2-small is enough for the single ingest pod today, but Flink's JobManager and TaskManager need far more memory, so the node moves to e2-standard-4 when Flink is deployed)*

---

### 4.2 Secret management

**What:** Kubernetes Secret resource, created once via `kubectl` from values in local `.env`, mounted into the pod as environment variables. Not using GCP Secret Manager for Day 5.

**Why:** *(For this Solo project K8 secrets is enough as it saves us the overhead of configuring google IAMs and auth loops on every POD connect. Using K8 secrets does expose the decrypted env API keys to the POD, but for a Solo developer, it is only exposed to me and it is already safe. The better way would be to do it in production pulling secret from google secret manager)*

---

### 4.3 Image build pipeline

**What:** Local `docker build` on your machine, push to GCP Artifact Registry via `docker push`. No CI-driven builds yet.

**Why:** *(During the early iteration phase, a manual Docker workflow is preferred over a CI pipeline to maximize developer iteration speed and avoid the latency of forcing a GitHub push for every microscopic image test. Manual builds allow you to compile code changes locally, quickly bump version tags, and push directly to Google Artifact Registry for immediate GKE deployment without overhead. However, introducing CI/CD complexity becomes highly valuable once the image requires daily rebuilds or a second developer joins the team, shifting the system from experimental debugging to a stable, automated, and audited release path.)*

---

### 4.4 Deployment vs single Pod

**What:** Kubernetes `Deployment` with `replicas: 1`, `restartPolicy: Always` (via the Deployment controller), and `strategy: Recreate` so the old pod's Alpaca connection closes before the new pod opens one. Not a bare Pod.

**Why:** *(Deployment is correct even with 1 replicas as deployment allows us to take advantage of the self-healing capability of GKE. We do not do replicas >1 as in our use case, if replicas>1, GKE would open multiple connections with Alpaca, and Alpaca allows only one stream connection on our plan, so the second is rejected with error 406 (connection limit exceeded). The same applies during deploys, which is why the strategy is Recreate rather than the default RollingUpdate. Even if the replicas work successfully, there would be message duplicates. A better way to do replicas > 1 would be to divide based on symbols.)*

---

### 4.5 Logging

**What:** Python logging to stdout (already configured). GKE's default Cloud Logging agent (fluentbit) captures stdout → routes to Google Cloud Logging. No structured JSON logs yet.

**Why:** *(Relying on the default Google Cloud Logging integration for Day 5 is the ideal choice because GKE automatically captures standard container stdout streams out of the box without requiring extra agent configurations, easily staying within Cloud Logging's generous free tier (first 50 GiB/month) for a 10-symbol feed. Unlike raw terminal stdout tails that scroll endlessly and vanish when a Pod restarts, Cloud Logging indexes your log entries for 30 days. For Day 6, the seamless upgrade path is switching your Python logging formatter to output structured JSON payloads (using a library like python-json-logger), which Cloud Logging automatically parses so you can query specific fields like jsonPayload.symbol="NVDA" or jsonPayload.latency_ms > 50 directly in the console.)*

---

### 4.6 Networking

**What:** Default VPC, no custom network policies. Pod can reach Confluent Cloud (public internet) and Alpaca (public internet) via default egress. No ingress required — this is a producer, not a server.

**Why:** *(This standalone project does not need any Ingress, as this is strictly a producer of market ticks sent over to confluent. As for VPC, setting it up is very complex with multiple DNS configs in place, and also costs extra money to give that backdoor for data movement avoiding the public internet. Traffic is still encrypted over the public internet: our Kafka producer connects to Confluent with SASL_SSL, and the Alpaca stream uses wss (TLS). Confluent Basic clusters also only offer public endpoints; private networking would need a Dedicated cluster. If we later want to add in a Prometheus metrics endpoint, we would ask GKE to include an internal cluster service mapping to allow to look into the inside-the-cluster monitoring scrapes without exposing the application to the public.)*

---

### 4.7 Health probes

**What:** **None for Day 5.** Deferred to Day 7.

**Why:** *(Readiness and liveness probes are skipped today because the application is under active manual observation during initial deployment, and setting up premature probes risks triggering false-positive Pod restarts while tuning connection backoffs. For a streaming producer, a meaningful liveness probe must go beyond checking if the Python process is simply alive—it needs to verify that the WebSocket connection is alive (ping/pong) and the Kafka producer is successfully flushing batches to Confluent Cloud without stalling. It must not check that ticks are arriving: there are no trades outside market hours, so a tick-based probe would restart the Pod every night. By Day 7, the production implementation will expose a lightweight internal HTTP health endpoint (or update a local heartbeat timestamp file) that confirms connection and producer health, allowing GKE to automatically kill and restart the Pod if the pipeline ever silently deadlocks. Trade-flow staleness becomes a market-hours-aware alert instead of a probe.)*

---

## 5. What a deploy looks like (operational flow)

```
# One-time setup
terraform init
terraform apply                                  # creates GKE cluster + registry
gcloud container clusters get-credentials <name> # wires kubectl to the cluster

# Per deploy
docker build --platform linux/amd64 -t <image> .  # Mac is arm64, GKE nodes are x86
docker push <registry>/<image>:<tag>
kubectl apply -f k8s/secret.yaml                 # if creds changed
kubectl apply -f k8s/deployment.yaml             # Recreate: old pod stops, new tag starts
kubectl logs -f deployment/alpaca-feed           # tail live
```

---

## 6. Future work (explicitly deferred)

- Prometheus metrics endpoint + Grafana (Day 6)
- Structured JSON logging for Cloud Logging query joy (Day 6)
- Readiness + liveness probes (Day 7 polish)
- GCP Secret Manager for credential rotation (Week 2)
- Workload Identity (instead of embedded API keys) for GCP service auth (Week 2+)
- Private VPC peering to Confluent Cloud (if ever production-serious)
- Horizontal scaling with per-symbol partition assignment (if the single-WebSocket model ever becomes insufficient)

---

## 7. Open questions I'd flag in a real design review

*(what if the WebSocket connection drops across a node preemption (The WebSocket connection always drops during a node preemption. Compared to a pod crash, recovery takes minutes because a new node has to be created and pull the image, the graceful shutdown must fit inside the ~15s SIGTERM window, and the trade gap has to be backfilled, see §3)? What are we not metrics-ing today that we'd want? Could this architecture scale to 100 symbols without redesign?)*

---

## 8. Files this adds to the repo

- `terraform/main.tf` — GKE cluster + node pool
- `terraform/artifact_registry.tf` — image registry
- `terraform/variables.tf` — project ID, region, cluster name
- `terraform/outputs.tf` — kubeconfig commands
- `Dockerfile` — multi-stage build for alpaca_feed
- `.dockerignore` — exclude venv, docs, etc.
- `k8s/secret.yaml.example` — template for the Secret (real secret.yaml is gitignored)
- `k8s/deployment.yaml` — the Deployment manifest
