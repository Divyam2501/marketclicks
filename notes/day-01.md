# Day 1 — Nov 5, 2026

## Setup done

- [ ] Polygon.io Stocks Starter — websocket access confirmed
- [ ] Confluent Cloud account + cluster created + topic `trades.raw` created
- [ ] GCP project + billing + $50/mo budget alert
- [ ] Repo scaffolded (uv, CI, folder structure)
- [ ] First commit pushed
- [ ] CI green on GitHub Actions

## Decisions made tonight

### Why Confluent Cloud over Redpanda
(Fill in your reasoning — what you weighed and why Confluent won)

### Why uv over poetry
(Fill in — speed? modern defaults? CI time?)

### Why Python 3.11
(What the plan called for and why)

## Reading done

- [ ] Polygon.io WebSocket API overview
- [ ] Polygon trade (T) and quote (Q) message schemas
- [ ] One article on exactly-once vs at-least-once stream processing

## Gotchas hit

(Any sign-up friction, auth issues, unexpected setup steps)

## Tomorrow (Nov 6)

Python asyncio WebSocket client. Subscribe to trades for 10 symbols:
SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, GOOGL, META, JPM.
Log messages to stdout. No Kafka yet.
