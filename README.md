# tickstore

Real-time market data streaming and anomaly detection platform.

![CI](https://github.com/Divyam2501/marketclicks/actions/workflows/ci.yml/badge.svg)

## What this is

Ingests real-time trades and quotes from Polygon.io into Kafka, computes rolling market metrics (VWAP, bid-ask spread, volume z-score) with Flink, and surfaces 5 market data quality anomalies (spread blowouts, quote crosses, feed gaps, stale ticks, volume spikes) via Slack alerts. Deployed on GKE with Prometheus and Grafana observability.

## Architecture

TODO: Mermaid diagram.

## Quickstart

TODO once Week 1 scaffold is stable.

## Design docs

- `docs/ingestion-design.md` (Week 2)
- `docs/metrics-design.md` (Week 4)
- `docs/anomaly-design.md` (Week 5)

## License

MIT
