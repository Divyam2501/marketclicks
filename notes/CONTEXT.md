# Project Context — tickstore

Living state of the project. Updated at the end of each work session and after any significant decision. Read this first in a new chat.

Source of plan: `Tickstore Learning & Build Plan (Nov 5 to Dec 31, 2026).md`
Historical plans for reference only: `project-bible.md`, `learning-plan.md`

---

## Snapshot

- **What:** Real-time market data streaming + anomaly detection platform
- **Timeline:** Nov 5 – Dec 30, 2026 (8 weeks)
- **Current day:** Day 2, Week 1 (**Fri Nov 6, 2026**). Plan's weekday labels are off by one (plan says "Thu Nov 6"); dates are authoritative, weekday labels in plan are ignored.
- **Target end state:** Shipped Kafka + PyFlink pipeline on GKE, 5 anomaly detectors live, blog + video, 25-30 applications in flight by Jan 1
- **Repo:** `https://github.com/Divyam2501/marketclicks` (named marketclicks, contents are tickstore)

---

## Infrastructure provisioned

| Resource | Status | Notes |
|---|---|---|
| Polygon.io Stocks Starter | ✅ Active, $29/mo | Websocket access confirmed. Delayed feed. |
| Confluent Cloud cluster `cluster_market_clicks` | ✅ Basic tier, GCP us-east1 | 3 partitions on `trades.raw` |
| Confluent topic `trades.raw` | ✅ 3 partitions | |
| Confluent service account `sa-market-clicks-process` | ✅ | CloudClusterAdmin role on cluster |
| Confluent API key for service account | ✅ | Credentials in `.env` |
| GCP project (personal, new) | ✅ | Billing enabled, $50/mo budget alert, email-only (no Pub/Sub) |
| Grafana Cloud | 🔲 Needed by Mon Nov 10 | |
| gcloud CLI | 🔲 Needed by Sun Nov 9 | |
| Terraform | 🔲 Needed by Sun Nov 9 | |
| uv | ✅ Installed, lock file committed | |

---

## Key decisions (dated, with why)

| Date | Decision | Why |
|---|---|---|
| Nov 5 | Kafka vendor: **Confluent Cloud** over Redpanda | Ecosystem depth, interview relevance, Confluent itself is a target firm; $400 trial covers 8 weeks |
| Nov 5 | Cluster tier: **Basic** | Standard is ~$540/mo baseline before usage; Basic is pay-only and covered by trial credit |
| Nov 5 | Dependency manager: **uv** over Poetry | Faster CI, modern defaults, Astral ecosystem |
| Nov 5 | Python version: **3.11** per plan | Managed via uv (`uv python install 3.11`) |
| Nov 5 | Auth: **service account** `sa-market-clicks-process` with CloudClusterAdmin | Industry standard; CloudClusterAdmin over-scoped because Basic tier has no topic-level RBAC — would use raw ACLs via CLI if hardening is needed later |
| Nov 5 | Partitions: **3** on `trades.raw` | Cost (Confluent bills per partition-hour); scale when parallelism need arises, not sooner |
| Nov 5 | GCP budget alerts: **email-only, no Pub/Sub** | Pub/Sub integration is scope creep at $50/mo spend |
| Nov 6 | Day-2 client structure: **Option A — polygon-api-client async interface** | Fast to productivity; keeps learning on asyncio patterns, not reimplementing Polygon framing |
| Nov 6 | mypy override: ignore missing stubs for `polygon.*` | Polygon SDK ships no py.typed marker; strict mypy would fail CI. Over-ride is scoped to polygon only. |
| Nov 6 | Deleted root `feed_handler.py` | Replaced by `src/tickstore/ingest/polygon_feed.py` in proper package structure |

---

## Code state

| File | Purpose | Status |
|---|---|---|
| `src/tickstore/__init__.py` | Package root | Empty |
| `src/tickstore/ingest/__init__.py` | Ingest subpackage | Empty |
| `src/tickstore/ingest/polygon_feed.py` | Day-2 asyncio WebSocket client, 10 symbols → stdout | ✅ Written, checks green, not yet live-tested against market |
| `pyproject.toml` | Project + tool config | uv, hatchling, ruff, mypy, pytest |
| `uv.lock` | Pinned deps | Committed |
| `.github/workflows/ci.yml` | CI: ruff + mypy + pytest on push/PR | Green on main |
| `tests/test_sanity.py` | One trivial import test | Passes |
| `.env` | Local secrets (POLYGON + CONFLUENT creds) | Gitignored |
| `.env.example` | Template | Committed |
| `notes/day-01.md` | Day-1 learning gate | Written; needs ~10-min revision pass to fix Kafka misconceptions |
| `LICENSE` | MIT | |
| `README.md` | Project front-door skeleton | CI badge wired |

---

## Progress against plan

### Week 1 (Nov 5-11) — Foundation & Ingestion Scaffold

Correct weekday mapping: Nov 5 = Thu, Nov 6 = Fri, Nov 7 = Sat, Nov 8 = Sun, Nov 9 = Mon, Nov 10 = Tue, Nov 11 = Wed.

- [x] Day 1 — **Thu Nov 5** — Setup (Polygon, Confluent, GCP, repo scaffold, CI, first commit)
- [x] Day 2 — **Fri Nov 6** — Python asyncio WebSocket client, 10 symbols, stdout only (code done, live-test pending next market open)
- [ ] Day 3 — **Sat Nov 7** — Reconnect logic with exponential backoff + resubscribe + distinguish auth failures from network drops
- [ ] Day 4 — **Sun Nov 8** — Kafka producer to Confluent `trades.raw`, partitioned by symbol, measure end-to-end throughput
- [ ] Day 5 — **Mon Nov 9** — GKE via Terraform (single-node preemptible), deploy ingestion pod, verify Kafka from pod
- [ ] Day 6 — **Tue Nov 10** — Structured logging + Prometheus metrics (messages_ingested_total, reconnect_count, bytes_processed_total) + Grafana Cloud free tier
- [ ] Day 7 — **Wed Nov 11** — Add quotes `Q` subscription + topic `quotes.raw`, write `notes/understanding-week1.md`

---

## Loose ends from Day 1

- [ ] Revise `notes/day-01.md`: fix Kafka misconceptions (routing, bootstrap, partitions, authn vs authz), add missing context (Polygon rationale, $400 credit, CloudClusterAdmin trade-off, partition cost math)
- [ ] Read one article on exactly-once vs at-least-once semantics (needed for Sat Nov 8 producer config decisions)

---

## Today (Day 2, Thu Nov 6) — focus

Build an asyncio WebSocket client that subscribes to trades for 10 symbols: SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, GOOGL, META, JPM. Log each trade message to stdout. No Kafka yet (that's Saturday).

Implementation structure (Option A chosen):
- New module: `src/tickstore/ingest/polygon_feed.py`
- Uses `polygon-api-client`'s async interface (not the blocking `.run()`)
- Entry point: `python -m tickstore.ingest.polygon_feed` or equivalent
- Logs messages to stdout with structured format (symbol, price, size, timestamp)
- Keep file small — reconnect / backoff is tomorrow's work, not today's

---

## Open questions / blockers

- None blocking as of Nov 6 morning
- Will need to make a Grafana Cloud account signup decision by Mon Nov 10

---

## Reference map

- **Plan:** `Tickstore Learning & Build Plan (Nov 5 to Dec 31, 2026).md`
- **Learning methodology:** section "Learning Framework" in the plan
- **Resume rollout schedule:** section "Resume Rollout Milestones" in the plan
- **Application targets + weekly cadence:** section "Parallel Application Strategy" in the plan
- **Interview defense reading list:** section "Interview Defense Reading List" in the plan
- **Repo:** `https://github.com/Divyam2501/marketclicks`
- **CI:** `https://github.com/Divyam2501/marketclicks/actions`
- **Secrets (never committed):** `.env` (POLYGON_API_KEY, CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET)

---

## Update protocol

I will update this file:
- At the end of each work session
- After any significant decision (add a row to Key decisions)
- After provisioning / tearing down any infrastructure (update Infrastructure table)
- After each new file / structural change (update Code state)
- When ticking off a day in the plan (update Progress)

If in doubt during a session, remind me "update CONTEXT.md."
