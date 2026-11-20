# tickstore: streaming market data platform

Real-time US equity trades streamed from a WebSocket into Apache Kafka,
processed with Apache Flink using event time, and deployed on Google
Kubernetes Engine with Terraform. A self-directed project focused on
streaming correctness: delivery semantics, watermarks, graceful shutdown,
and replay-based validation.

![CI](https://github.com/Divyam2501/marketclicks/actions/workflows/ci.yml/badge.svg)

![tickstore architecture: Alpaca WebSocket to an ingest service on GKE, Confluent Kafka, a PyFlink event-time job and a metrics topic, with measured results](docs/images/architecture.png)

## Architecture

```
  Alpaca Markets WebSocket (IEX real-time trades, 10 US symbols)
                  │
                  ▼   alpaca_feed: async reader → bounded queue → producer
  ┌─────────────────────────────────────────────┐
  │ GKE Standard, one Spot node (Terraform)     │
  │ Deployment, replicas 1, Recreate strategy   │
  └─────────────────────────────────────────────┘
                  │   idempotent producer, acks=all, key = symbol
                  ▼
  Confluent Cloud Kafka   trades.raw (3 partitions)
                  │
                  ▼   PyFlink job: event time, watermarks, 1-min windows
  Confluent Cloud Kafka   metrics.trade_count_1m
                  │
                  ▼   (planned) BigQuery sink, anomaly detectors, alerts
```

## Measured results

| What | Result |
|---|---|
| Full trading day on GKE (Nov 13, 2026) | 242,962 trades ingested, 0 pod restarts |
| Latency, exchange trade time → producer send (1,849-trade sample at the open) | p50 21 ms, p95 45 ms, max 127 ms |
| Flink job validated by replay | Full day replayed in ~50 s; all 3,788 per-symbol 1-minute windows match an independent count exactly |
| Flink job live (Nov 18, 2026, 10:32–16:05 ET) | 7,067 windows, no missing minutes, 0 duplicates; windows typically land ~5 s after the minute ends |
| Recovery from a crash mid-session | Restored from checkpoint: next 580 windows (23,656 trades) match an independent count exactly, no partial or duplicate windows |
| Graceful shutdown in cluster | SIGTERM on deploy and node drain → queue drained, producer flushed, exit 0 |
| Cold start from zero nodes | Node ready 69 s, pod running 97 s, streaming 115 s |

Scale is honest: 10 symbols from the IEX feed, roughly 8–23 trades per second.
The point of the project is correctness under failure, not volume.

## Engineering highlights

- **A replay bug that loses 80% of the data with exit code 0.** PyFlink
  generates watermarks after the Kafka source when the timestamp logic is
  Python, not per partition. Replaying a day at parallelism 1 dropped
  195,309 of 242,962 trades as "late": the watermark followed the partition
  read first. Running one source subtask per partition fixes it (0 late).
  Found by reading the PyFlink source, confirmed by experiment.
  ([flink-design §4.4, §5](docs/flink-design.md))
- **Checkpoint restore, tested on a real failure.** The live job ran on a
  laptop that went to sleep mid-session; the Flink runtime died and stayed
  stuck restarting. Restoring from the last retained checkpoint brings back
  window state and Kafka offsets from the same instant, so output resumed
  with no gaps or duplicates. Also the case for moving Flink to GKE: a laptop
  is not a runtime.
- **Latency follows the data, not the clock.** A window closes when the
  watermark passes its end, and the watermark moves only when trades arrive.
  Windows usually land ~5 s after the minute; when the partition carrying
  quieter symbols went silent, it took 10–12 s.
- **Event time, not processing time.** Windows use the exchange timestamp, so
  a replay produces the same counts as the live run. Idleness handling keeps
  an always-empty partition from stalling the watermark.
- **Shutdown within GKE Spot's ~15 s preemption window.** One 13 s deadline:
  stop reading, drain the queue (capped at 3 s), flush Kafka with the rest.
  Python runs as PID 1 in exec form so SIGTERM actually reaches it.
  ([deploy-design](docs/deploy-design.md))
- **Partitioner finding.** librdkafka's default partitioner (CRC32) puts the
  10 symbols on only 2 of 3 partitions; Java clients use murmur2 and would
  place them differently. Documented as a cross-client hazard.
- **Reconnect taxonomy.** Network errors retry forever with exponential
  backoff (1 s → 60 s) and jitter; auth errors fail fast so Kubernetes
  crash-loops surface them.
- **Cost.** One Spot e2-small node, scaled to zero outside market hours;
  infrastructure fully in Terraform with remote, locked state in GCS.

## Status

| Component | Status |
|---|---|
| WebSocket ingest, idempotent Kafka producer, reconnect, graceful shutdown | ✅ Shipped |
| REST backfill for historical windows | ✅ Shipped |
| Container image + GKE deployment via Terraform, scale to zero | ✅ Shipped |
| Flink job 1: per-symbol trade counts in 1-minute event-time windows | ✅ Shipped, validated by replay and a full live session |
| Per-window data-quality counts (malformed, late, extended-hours) | 🟡 In progress |
| Flink on GKE (checkpoints in GCS, Workload Identity) | ⏳ Planned |
| VWAP and feed-gap jobs, BigQuery sink, anomaly detectors + alerts | ⏳ Planned |
| Prometheus metrics + Grafana dashboards | ⏳ Planned |

## Design docs

Each doc lists failure modes, then a decision with a *What* and a *Why*.

- [`docs/ingestion-design.md`](docs/ingestion-design.md): reconnect strategy,
  backoff and jitter, failure-mode taxonomy.
- [`docs/producer-design.md`](docs/producer-design.md): idempotent producer
  config, partitioning, bounded-queue backpressure, shutdown ordering.
- [`docs/deploy-design.md`](docs/deploy-design.md): GKE on Spot, Terraform,
  secrets, SIGTERM handling, identity model, scale to zero, measured cold start.
- [`docs/flink-design.md`](docs/flink-design.md): event time, watermarks,
  windows, late data, checkpoints, delivery guarantees, replay validation.

## Repository layout

```
src/tickstore/ingest/   WebSocket feed (alpaca_feed) and REST backfill
tests/                  ingest tests (pytest, run in CI)
flink/                  separate uv project (Python 3.12, PyFlink 2.2.1)
  jobs/                 trade_count_1m.py, hello_kafka.py
  tests/                job unit tests
terraform/              GKE cluster, Spot node pool, Artifact Registry
k8s/                    Deployment manifest, Secret template
Dockerfile              two-stage build: uv builds the venv, slim runtime
docs/                   design docs
```

## Quick start

Requires [`uv`](https://docs.astral.sh/uv/), an Alpaca Markets paper-trading
account (free market data), and a Confluent Cloud Kafka cluster.

```bash
git clone https://github.com/Divyam2501/marketclicks.git
cd marketclicks
uv sync                      # ingest: Python 3.11–3.13

cp .env.example .env         # Alpaca + Confluent credentials

# Live ingest (US market hours)
uv run python -m tickstore.ingest.alpaca_feed

# Or backfill the last 30 minutes (any time)
uv run python -m tickstore.ingest.alpaca_backfill --minutes 30
```

Flink job (needs Java 17 and the Kafka connector JAR; see
[flink-design §4.1](docs/flink-design.md)):

```bash
cd flink && uv sync          # Python 3.12
# Replay a past window to stdout
uv run python jobs/trade_count_1m.py \
  --from 2026-11-13T14:44:00+00:00 --until 2026-11-13T21:55:00+00:00 --sink print
# Live, writing to metrics.trade_count_1m
uv run python jobs/trade_count_1m.py --from latest --sink kafka
# Resume after a crash from a retained checkpoint
uv run python jobs/trade_count_1m.py --sink kafka --restore checkpoints/<job-id>/chk-<n>
```

Deploying to GKE: [`terraform/README.md`](terraform/README.md) and
[deploy-design §5](docs/deploy-design.md).

## Tech stack

Python (asyncio), Apache Kafka (Confluent Cloud, `confluent-kafka`), Apache
Flink (PyFlink 2.2), Docker, Kubernetes (GKE), Terraform, Google Cloud
(Artifact Registry, Cloud Logging, GCS). `uv` for dependencies; GitHub
Actions CI with ruff, mypy and pytest.

## License

MIT
