# tickstore — Streaming Market Data Platform

Real-time US equity market data ingestion into Apache Kafka with Apache Flink
stream processing, deployed on Google Kubernetes Engine. A self-directed project
exploring streaming system design, delivery semantics, and cloud-native
observability.

![CI](https://github.com/Divyam2501/marketclicks/actions/workflows/ci.yml/badge.svg)

## Architecture

```
          Alpaca Markets WebSocket  (IEX real-time trades)
                      │
                      ▼  async reader coroutine
             bounded asyncio.Queue   ← backpressure boundary
                      │
                      ▼  producer coroutine
          Confluent Cloud Kafka  (topic: trades.raw, symbol-keyed, 3 partitions)
                      │
                      ▼
          PyFlink stream jobs    (keyed state, event-time windows)
                      │
                      ▼
          Prometheus  →  Grafana  (ingestion lag, reconnect count, produce latency)
```

### Design principles

- **Decoupled ingest and produce.** WebSocket reader and Kafka producer run as
  separate coroutines sharing a bounded `asyncio.Queue`. Kafka backpressure is
  absorbed by the queue first, then propagates to the WebSocket layer only if
  the queue fills — Polygon's slow-consumer disconnect is a last resort, not a
  common case.
- **Effectively-once producer semantics.** Idempotent producer config
  (`enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection ≤ 5`)
  prevents in-session duplicates on retry; Kafka's guarantee ends at Kafka, so
  anything sinking out of Kafka would need its own dedup.
- **Resilient reconnect.** Exponential backoff (1s → 60s cap) with ±0.5s jitter
  for network errors; infinite retries acceptable because stream recovery is
  the goal. Auth errors fail-fast after one retry so Kubernetes crash-loops
  surface them visibly rather than silently retrying forever.
- **Symbol-keyed partitioning.** Preserves per-symbol trade ordering for
  downstream consumers. Accepts hot-key risk on dominant symbols (AAPL on IEX)
  as a trade-off for order guarantees.

Full design rationale in [`docs/`](docs/).

## Status

| Component | Status |
|---|---|
| WebSocket ingest (Alpaca IEX, 10 US equity symbols) | ✅ Shipped |
| Idempotent Kafka producer with symbol-keyed partitioning | ✅ Shipped |
| Exponential-backoff reconnect + auth-vs-network error differentiation | ✅ Shipped |
| REST backfill (historical window → Kafka) for pipeline validation | ✅ Shipped |
| GKE deployment via Terraform | 🟡 In progress |
| Prometheus metrics + Grafana dashboards | 🟡 In progress |
| PyFlink stream processing (rolling trade count, 1-min VWAP, feed-gap detection) | 🟡 In progress |

## Design docs

- [`docs/ingestion-design.md`](docs/ingestion-design.md) — Reconnect strategy,
  backoff + jitter rationale, failure-mode taxonomy, server-side state
  ownership argument for client-driven resubscribe.
- [`docs/producer-design.md`](docs/producer-design.md) — Kafka producer config,
  partitioning strategy with hot-key analysis, bounded-queue backpressure
  design, graceful shutdown semantics.
- [`docs/deploy-design.md`](docs/deploy-design.md) — GKE infrastructure design
  (in progress).

## Quick start

Requires Python 3.11+, [`uv`](https://docs.astral.sh/uv/), an Alpaca Markets
paper-trading account, and a Confluent Cloud Kafka cluster.

```bash
git clone https://github.com/Divyam2501/tickstore.git
cd tickstore
uv sync

cp .env.example .env
# Edit .env with:
#   ALPACA_API_KEY, ALPACA_API_SECRET
#   CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET

# Live ingest (during US market hours)
uv run python -m tickstore.ingest.alpaca_feed

# Or backfill from the last 30 minutes of historical data (works any time)
uv run python -m tickstore.ingest.alpaca_backfill --minutes 30
```

## Tech stack

Python (asyncio), Apache Kafka (Confluent Cloud via `confluent-kafka-python`),
Apache Flink (PyFlink), Google Kubernetes Engine (GKE), Terraform, Prometheus,
Grafana. Dependency management via `uv` + `pyproject.toml`. CI via GitHub
Actions (ruff, mypy, pytest).

## License

MIT
