# Deploy Design — GKE for alpaca_feed

**Module:** `src/tickstore/ingest/alpaca_feed.py` deployed as a long-running pod on GKE
**Status:** Day 5 design (Nov 9, 2026)
**Author:** Divyam Kaushik
**Related:** `docs/ingestion-design.md` (reconnect semantics), `docs/producer-design.md` (Kafka producer)

---

## 1. Purpose

*(GKE deployment provides the infinitely running streaming python processes a place to run, and be managed by Kubernetes directly. POD processes would rebuild on failures and allow for visible health check on the process, at the same time GKE will handle the traffic and scale easily once trading volume increase with added symbols, and also supports development across multiple developers sharing the same shared OS/dependencies build through the dockerfile, so development is in sync)*

---

## 2. Scope

### In scope (Day 5)
- Single-node GKE cluster (cheapest tier) in us-east1 for Confluent-proximity
- Preemptible / spot node pool (cost control)
- One Deployment running `alpaca_feed` with replicas=1
- Kubernetes Secret holding POLYGON/ALPACA + CONFLUENT credentials
- Structured stdout logs auto-captured by Cloud Logging
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
| Node preemption | GCP reclaims the spot node | New node spun up; pod rescheduled; some messages in-flight may be lost (acceptable; Kafka idempotent producer limits dupes) |
| Image pull fail | Typo in tag, registry unreachable | Pod `ImagePullBackOff`; surfaces clearly |
| Secret missing / stale | K8s Secret not synced, env vars missing | Python startup fails with KeyError; pod crash-loops |
| Cluster-wide failure | GKE control plane outage (rare) | Everything stops until GCP restores; manual investigation |

---

## 4. Design decisions

Each decision has a **What** (committed) and a **Why** (to be written in own voice).

### 4.1 Cluster type and size

**What:** GKE Standard (not Autopilot) in us-east1, single node pool with 1 preemptible `e2-small` node (2 vCPU, 2 GB RAM).

**Why:** *(fill in — 2-4 sentences: why Standard over Autopilot, why single node, why preemptible, why e2-small. Think about: cost at idle vs. actual compute need for 10-symbol ingest, who benefits from Autopilot vs manual node config, what's lost by going cheap)*

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

**What:** Kubernetes `Deployment` with `replicas: 1` and `restartPolicy: Always` (via the Deployment controller). Not a bare Pod.

**Why:** *(Deployment is correct even with 1 replicas as deployment allows us to take advantage of the self-healing capability of GKE. We do not do replicas >1 as in our use case, if replicas>1, GKE would open multiple connections with POlygon and our account can get blocked. Even if the replicas work successfully, there would be message duplicates. A better way to do replicas > 1 would be to divide based on symbols.)*

---

### 4.5 Logging

**What:** Python logging to stdout (already configured). GKE's default Cloud Logging agent (fluentbit) captures stdout → routes to Google Cloud Logging. No structured JSON logs yet.

**Why:** *(fill in — 2-4 sentences: why rely on Cloud Logging default for Day 5, what the upgrade path to structured JSON logs looks like (Day 6), what queries Cloud Logging enables that stdout tail does not. Mention cost: Cloud Logging has a free tier that will easily cover this project)*

---

### 4.6 Networking

**What:** Default VPC, no custom network policies. Pod can reach Confluent Cloud (public internet) and Alpaca (public internet) via default egress. No ingress required — this is a producer, not a server.

**Why:** *(fill in — 2-4 sentences: why no VPC peering to Confluent (private link available but not worth the complexity at this stage), why no ingress / Service / LoadBalancer, what changes if we later want to expose a metrics endpoint for Prometheus)*

---

### 4.7 Health probes

**What:** **None for Day 5.** Deferred to Day 7.

**Why:** *(fill in — 2-4 sentences: why skip readiness/liveness probes today, what signal we'd want a probe to check (process alive? WebSocket connected? producing to Kafka?), what the Day 7 version would look like)*

---

## 5. What a deploy looks like (operational flow)

```
# One-time setup
terraform init
terraform apply                                  # creates GKE cluster + registry
gcloud container clusters get-credentials <name> # wires kubectl to the cluster

# Per deploy
docker build -t <image> .
docker push <registry>/<image>:<tag>
kubectl apply -f k8s/secret.yaml                 # if creds changed
kubectl apply -f k8s/deployment.yaml             # rolling restart to new tag
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

*(fill in — 2-3 questions you'd want pushed back on. Think about: what if the WebSocket connection drops across a node preemption? What are we not metrics-ing today that we'd want? Could this architecture scale to 100 symbols without redesign?)*

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
