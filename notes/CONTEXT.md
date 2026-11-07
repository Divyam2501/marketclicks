# Project Context — tickstore

Living state of the project. **Read this first in any new session — it is the authoritative source of truth for where we are and what's next.** Updated at the end of each session and after any significant decision.

Source of plan: `Tickstore Learning & Build Plan (Nov 5 to Dec 31, 2026).md`
Historical plans for reference only: `project-bible.md`, `learning-plan.md`

---

## Snapshot

- **What:** Real-time market data streaming + anomaly detection platform
- **Timeline:** Nov 5 – Dec 30, 2026 (8 weeks)
- **Current day:** **Day 3, Week 1 (Sat Nov 7, 2026)**
- **Calendar note:** Plan doc's weekday labels are off by one (plan says "Fri Nov 7"); real calendar is Sat. Dates are authoritative.
- **Target end state:** Shipped Kafka + PyFlink pipeline on GKE, 5 anomaly detectors live, blog + video, 25-30 applications in flight by Jan 1
- **Repo:** `https://github.com/Divyam2501/marketclicks` (named marketclicks; project inside is tickstore)

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
| Grafana Cloud | 🔲 Needed by Tue Nov 10 | |
| gcloud CLI | 🔲 Needed by Mon Nov 9 | |
| Terraform | 🔲 Needed by Mon Nov 9 | |
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
| Nov 6 | mypy override: ignore missing stubs for `polygon.*` | Polygon SDK ships no py.typed marker; strict mypy would fail CI. Override scoped to polygon only. |
| Nov 6 | Deleted root `feed_handler.py` | Replaced by `src/tickstore/ingest/polygon_feed.py` in proper package structure |

---

## Code state

| File | Purpose | Status |
|---|---|---|
| `src/tickstore/__init__.py` | Package root | Empty |
| `src/tickstore/ingest/__init__.py` | Ingest subpackage | Empty |
| `src/tickstore/ingest/polygon_feed.py` | asyncio WebSocket client, 10 symbols → stdout | ✅ Shipped Day 2, checks green, not yet live-tested against market (Mon Nov 9) |
| `pyproject.toml` | Project + tool config | uv, hatchling, ruff, mypy, pytest. mypy override for polygon.* |
| `uv.lock` | Pinned deps | Committed |
| `.github/workflows/ci.yml` | CI: ruff + mypy + pytest on push/PR | Green on main |
| `tests/test_sanity.py` | Import tests for tickstore + tickstore.ingest.polygon_feed | Passes (2 tests) |
| `.env` | Local secrets | Gitignored. Contains POLYGON_API_KEY, CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET |
| `.env.example` | Template | Committed |
| `notes/day-01.md` | Day-1 learning gate | ⚠️ Written but has Kafka misconceptions that still need correcting (see Loose ends) |
| `LICENSE` | MIT | |
| `README.md` | Project front-door skeleton | CI badge wired |
| `docs/` | Design docs directory | Empty (`/docs/ingestion-design.md` to be written Day 3) |
| `terraform/` | Terraform configs | Empty (used starting Day 5) |

Repo-level git state: on `main`, clean, all pushed to origin. Recent commits: `f683dfc` (CONTEXT calendar fix), `f8186d0` (Day 2 asyncio client), `722e36b` (ruff format), `69f2cd9` (uv.lock), `6fcb85c` (scaffold).

---

## Progress against plan

### Week 1 (Nov 5-11) — Foundation & Ingestion Scaffold

Correct weekday mapping: Nov 5 = Thu, Nov 6 = Fri, Nov 7 = Sat, Nov 8 = Sun, Nov 9 = Mon, Nov 10 = Tue, Nov 11 = Wed.

- [x] Day 1 — **Thu Nov 5** — Setup (Polygon, Confluent, GCP, repo scaffold, CI, first commit)
- [x] Day 2 — **Fri Nov 6** — Python asyncio WebSocket client, 10 symbols, stdout only (code done; live-test deferred to Mon)
- [ ] Day 3 — **Sat Nov 7** — Reconnect logic with exponential backoff + resubscribe + distinguish auth failures from network drops (**TODAY**)
- [ ] Day 4 — **Sun Nov 8** — Kafka producer to Confluent `trades.raw`, partitioned by symbol, measure end-to-end throughput
- [ ] Day 5 — **Mon Nov 9** — GKE via Terraform (single-node preemptible), deploy ingestion pod, verify Kafka from pod. Also: first live market test of polygon_feed at open.
- [ ] Day 6 — **Tue Nov 10** — Structured logging + Prometheus metrics (messages_ingested_total, reconnect_count, bytes_processed_total) + Grafana Cloud free tier
- [ ] Day 7 — **Wed Nov 11** — Add quotes `Q` subscription + topic `quotes.raw`, write `notes/understanding-week1.md`

---

## Today — Day 3, Sat Nov 7 — focus

**Goal in one line:** `polygon_feed` survives arbitrary network drops and resumes without manual intervention, but doesn't infinite-loop on a bad API key.

### Step-by-step order

1. **Fix `notes/day-01.md`** (15 min, NON-NEGOTIABLE). Correct the Kafka misconceptions — see Loose ends section. This is committed public content with errors in your voice.
2. **Design the reconnect logic on paper first** — write `/docs/ingestion-design.md` BEFORE any code. Answer the 4 design questions below.
3. **Type the reconnect loop BY HAND** in `src/tickstore/ingest/polygon_feed.py`. This is interview-defense critical; Claude reviews, user types.
4. **Test by killing the connection manually**: disable wifi for 10s mid-run, verify recovery with backoff; kill the Polygon-side subscription, verify re-auth.
5. **Commit + push.** Update CONTEXT.md.

### 4 design questions to answer in `/docs/ingestion-design.md` BEFORE coding

1. **Max backoff cap?** Trade-off: short cap = hammer a down service; long cap = slow to recover from brief outages. Common answer: 60s.
2. **Infinite retries, or give up after N?** Infinite keeps process alive forever. Finite requires external restart (K8s will do this Day 5).
3. **Should auth errors retry at all?** Zero is safe answer. But consider: momentary Polygon-side auth glitch vs. permanent misconfigured key.
4. **Treat missed WebSocket heartbeats as disconnects?** How aggressive is the dead-connection detection?

### What you do NOT do today

- No Kafka producer (that's tomorrow Sun Nov 8)
- No GKE deploy (Mon Nov 9)
- No metrics / Prometheus (Tue Nov 10)
- No quotes `Q` subscription (Wed Nov 11)
- No Flink (Week 3)

### Market status today

Saturday — US market closed. No live Polygon data flowing. Test reconnect logic artificially (kill connection locally). First live market test = Mon Nov 9 at 9:30 AM ET.

---

## Loose ends (still open)

- [ ] **Revise `notes/day-01.md`** (carryover from Day 1): fix Kafka misconceptions (cluster doesn't route, bootstrap is initial-handshake not lookup, partitions serve parallelism AND availability, authn ≠ authz) + add missing context (Polygon rationale over alternatives, $400 Confluent trial credit, CloudClusterAdmin over-scoped on purpose, 3-partition cost math, Kafka's exactly-once boundary ends at Kafka)

---

## Concepts internalized (do not re-explain in new sessions)

The user has already covered these topics in prior sessions. Do not re-teach unless explicitly asked.

- **WebSocket vs REST**: WebSocket is push (server streams to client), REST is pull (client polls)
- **Async/await mechanics**: single thread, event loop, `await` yields control, handler + socket read are serial in our loop, OS buffer absorbs between-batch arrivals
- **OS-level backpressure**: kernel TCP buffer queues incoming bytes while handler runs; if handler slow, buffer fills, Kafka/Polygon disconnects us
- **Three-layer model**: sender (Polygon) keeps sending → OS kernel buffers → Python drains at its own pace
- **Why async with one connection**: future-proofs for multi-coroutine code (reader + producer sharing one thread)
- **Kafka architecture**: cluster = logical grouping of brokers; topic = logical, split into partitions; partition has leader + replica brokers; producer computes partition routing (not the cluster); bootstrap server = initial handshake point only, not a per-request lookup; partitions serve parallelism AND availability
- **Service accounts vs user accounts**: direct credentials (service account IS the identity used), not GCP-style impersonation; service accounts for apps, user accounts for humans
- **RBAC vs ACLs**: RBAC = role bindings (CloudClusterAdmin etc.); ACLs = raw Kafka permissions; Basic Confluent tier only has cluster-level RBAC, no topic-level; ACLs via CLI still work on Basic
- **effectively-once vs exactly-once**: at-least-once + idempotent consumer = effectively-once; Kafka's exactly-once = idempotent producer + transactions; **Kafka's exactly-once guarantee ends AT Kafka** — any external sink (BigQuery, Slack, API) needs its own idempotency
- **uv / pyproject.toml / uv.lock / CI**: pyproject is declarative project config; uv.lock pins exact versions for reproducibility; CI (GitHub Actions) uses uv to materialize an identical venv on its VM; venv doesn't travel, the lock does

---

## Interview-defense readiness

### Can defend crisply now
- Why Confluent over Redpanda, why Basic tier
- Service account + RBAC trade-offs (and why CloudClusterAdmin is pragmatic over-scoping on Basic)
- WebSocket vs REST, async/await semantics, event loop blocking
- Kafka basic architecture (brokers, partitions, consumer groups)
- effectively-once vs exactly-once vocabulary, Kafka's exactly-once boundary
- uv + pyproject.toml + lock file reproducibility story

### Needs tomorrow's work to defend
- Reconnect/backoff design trade-offs (Day 3 — today)
- Producer config choices (idempotence, acks, batching) (Day 4 — Sun)

### Needs Week 2+ work to defend
- Flink state primitives, checkpointing, watermarks
- Stream-stream joins (for spread calc)
- Anomaly detector threshold design, precision/recall validation

---

## Pacing concerns

1. **Weekend hours are the Week 1 risk.** Days 3 + 4 (Sat + Sun) are both heavy (reconnect + Kafka producer). If life compresses either, Week 1 slips a full day and Monday starts behind.
2. **No live market validation until Mon Nov 9.** 3 days of "works locally" without market-shaped messages. Mitigation: run polygon_feed Monday at 9:30 AM ET sharp for 15-20 min to observe real message shapes BEFORE starting GKE work.
3. **Concept explanations are taking real cycles.** Async took 3 rounds to click. Not a failure, but if Flink concepts in Week 3 compound the same way, Week 3 compresses. Watch pace.

---

## Open questions / blockers

- None blocking as of Sat Nov 7 morning
- User to decide the 4 reconnect design questions above during Day 3 design phase
- Grafana Cloud signup needs to happen by Mon Nov 10 (preferably earlier)

---

## Reference map

- **Plan:** `Tickstore Learning & Build Plan (Nov 5 to Dec 31, 2026).md`
- **Learning methodology:** section "Learning Framework" in the plan
- **AI usage rules:** section "Learning Framework" → sub-sections on Claude usage
- **Resume rollout schedule:** section "Resume Rollout Milestones" in the plan
- **Application targets + weekly cadence:** section "Parallel Application Strategy" in the plan
- **Interview defense reading list:** section "Interview Defense Reading List" in the plan
- **Risk and contingency:** section "Risk & Contingency" in the plan
- **Repo:** `https://github.com/Divyam2501/marketclicks`
- **CI:** `https://github.com/Divyam2501/marketclicks/actions`
- **Secrets (never committed):** `.env` (POLYGON_API_KEY, CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET)
- **Confluent cluster UI:** Accounts and access → `cluster_market_clicks` on default environment

---

## Git identity (per-repo)

- `user.name` = `Divyam Kaushik`
- `user.email` = `divyamk.iitd@gmail.com`
- Not set globally; only this repo. For commits to link to GitHub contribution graph, email must be verified on the GitHub account.

---

## Working style preferences (from prior sessions)

- Direct, honest, non-sycophantic feedback — no cheerleading
- Correct misconceptions explicitly, don't paper over
- Start simple, layer in complexity (user flagged this when first async explanation was too dense)
- Design-before-code discipline per plan
- Line-by-line code review required for AI-drafted code
- Weekly understanding notes in own voice; corrections flagged explicitly
- User types failure-mode code by hand (reconnect, Kafka producer config) — interview-defense material

---

## Update protocol

CONTEXT.md is updated:
- At the end of each work session
- After any significant decision (add a row to Key decisions)
- After provisioning / tearing down any infrastructure (update Infrastructure table)
- After each new file / structural change (update Code state)
- When ticking off a day in the plan (update Progress)
- When user explicitly asks "update CONTEXT.md"

If starting a new chat, Claude should re-read this file in full before answering anything project-related.
