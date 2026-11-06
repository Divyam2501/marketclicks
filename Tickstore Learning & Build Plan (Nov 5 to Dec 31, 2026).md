# Tickstore Learning & Build Plan (Nov 5 to Dec 31, 2026)

Nov 5, 2026 · @Div

**Target end state:** Shipped Kafka + PyFlink streaming market data platform with 5 anomaly detectors, deployed on GKE, Grafana dashboards, Slack alerting, published blog post, resume-ready by Jan 1. 20-30 applications in flight. First screens landing.

---

## Overview & Success Criteria

## Overview & Success Criteria

Ship a credible streaming market data platform by Dec 30, 2026, with 25-30 applications already in flight by Jan 1, 2027.

**Definition of done by Jan 1:**

- Tickstore deployed on GKE with 4+ weeks continuous uptime at resume publication
- 5 anomaly detectors live and validated against historical sessions
- Public GitHub repo with clean code, README with architecture diagram, tests on critical paths
- One or two blog posts explaining design decisions and tradeoffs
- 3-5 minute video walkthrough linked in README
- Resume Projects section updated with 4 strong bullets plus GitHub and blog links
- 25-30 applications submitted across quant multi-managers, big tech streaming teams, market makers, and fintech infra
- First recruiter screens landing; interview prep underway

**Why 8 weeks is the right horizon:**

- Enough calendar time to ship credibly at 25 hr/week pace with AI acceleration
- Aligns with industry hiring cycles: bonus-season attrition opens seats Jan to March
- Tight enough to prevent scope creep, long enough to actually finish
- By Feb 2027 you interview with completed project in hand, not half-built claims

**Explicitly out of scope (to protect the timeline):**

- Custom columnar storage engine
- Query engine design
- Benchmarking against ClickHouse or QuestDB (optional stretch if ahead)
- L2 order book depth (trades and top-of-book quotes only)
- Options or futures (US equities only)
- Multi-exchange (Polygon aggregated feed only)
- Historical data backfill from paid vendors

**Non-negotiables:**

- Deploy to GKE from Day 1; uptime claim compounds over weeks
- Public GitHub repo from Day 1
- Real Polygon data from Week 1; never simulated
- Learning gate: before claiming you built something, you can explain its mechanics in 3 sentences without looking at docs

---

## Learning Framework

## Learning Framework

Understanding-before-shipping is the rule. A project you can't defend in interviews is worse than no project.

**Daily time split (2-3 hr weekdays, 4-6 hr weekends):**

- 65% implementation (Claude-accelerated where appropriate)
- 25% reading official docs on what you just built or are about to build
- 10% writing down design decisions and tradeoffs in your repo

**AI acceleration rules (strict):**

- Claude writes code; you write design docs first
- Before Claude writes a module, hand-write in notes.md what it should do, why, and what alternatives exist
- Read every line Claude generates; if you can't explain a line, delete and rewrite
- For anomaly detectors: you design the logic and pick thresholds, Claude implements the plumbing
- For streaming semantics (watermarks, state backends, checkpointing): read docs first, decide yourself, then have Claude implement

**Weekly understanding gate:**

Every Sunday evening, write a 200-word "what I understand now" note in the repo under `/notes/understanding-weekN.md`. If you can't write it from memory, that week's work does not count as shipped.

**Where to use Claude heavily:**

- Terraform for GKE
- Dockerfiles, Docker Compose, Kubernetes manifests
- GitHub Actions CI config
- PyFlink boilerplate (once the pattern is set)
- Grafana dashboard JSON
- Test scaffolds and fixtures
- README structure, architecture diagram source (Mermaid or DOT)
- Blog post first drafts (then heavily rewrite in your voice)

**Where to NOT use Claude:**

- Framework choices (you must own the Spark vs Flink vs Kafka Streams defense)
- Threshold tuning for detectors (domain judgment, not code generation)
- Final blog post text (your voice, not AI prose)
- Any design decision with interview-defense value

---

## Day 1 Setup (Today, Nov 5)

## Day 1 Setup (Today, Nov 5)

Two to three hours tonight. Everything in place so you build ingestion tomorrow.

**Accounts to create:**

- Polygon.io Stocks Starter signup ($29/mo), confirm WebSocket access enabled
- Confluent Cloud free tier OR Redpanda Cloud free tier (pick one for managed Kafka)
- GitHub repo `tickstore`, public, MIT license
- New personal GCP project (separate from any CVS access); enable billing with a $50/mo budget alert

**Repo scaffolding:**

- Python 3.11 with `uv` or `poetry` for dependencies
- `.github/workflows/ci.yml` with pytest, ruff, mypy
- Folder structure: `src/tickstore/`, `tests/`, `terraform/`, `notes/`, `docs/`
- `.env.example` committed, real `.env` gitignored
- README skeleton: title, one-line description, CI badge, placeholder for architecture diagram

**Tonight's deliverable (minimum):**

- Repo is live on GitHub
- All credentials saved locally in `.env`
- First commit pushed
- CI passing on empty scaffolding
- `notes/day-01.md` written

**Reading before you code (30 min):**

- Polygon.io docs: WebSocket API overview, trade (`T`) and quote (`Q`) message schemas
- One article on exactly-once vs at-least-once stream processing semantics

**End-of-day note (`notes/day-01.md`):**

- Why you picked Confluent Cloud vs Redpanda vs self-host Kafka
- Any gotchas with Polygon signup or API key setup
- What you'll start tomorrow morning

---

## Week 1 (Nov 5 to Nov 11): Foundation & Ingestion Scaffold

## Week 1 (Nov 5 to Nov 11): Foundation & Ingestion Scaffold

Goal: Polygon WebSocket client flowing trades and quotes into Kafka, deployed to GKE, by Sunday night.

**Daily plan:**

- **Wed Nov 5 (tonight):** Day 1 setup checklist above
- **Thu Nov 6:** Python asyncio WebSocket client. Subscribe to trades for 10 symbols (SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, GOOGL, META, JPM). Log messages to stdout. No Kafka yet.
- **Fri Nov 7:** Reconnect logic with exponential backoff and resubscribe. Handle Polygon auth failures separately from network drops.
- **Sat Nov 8:** Kafka producer sending to Confluent Cloud topic `trades.raw`, partitioned by symbol. Measure throughput end-to-end.
- **Sun Nov 9:** GKE cluster via Terraform (single-node, preemptible, cheapest tier). Deploy ingestion client as one pod. Confirm messages land in Kafka from the pod.
- **Mon Nov 10:** Structured logging + Prometheus metrics (`messages_ingested_total`, `reconnect_count`, `bytes_processed_total`). Grafana Cloud free tier or in-cluster Prometheus.
- **Tue Nov 11:** Add quote subscription (`Q`) alongside trades, topic `quotes.raw`. Write `notes/understanding-week1.md`.

**Learning track (Kafka fundamentals):**

- Kafka docs: Producer API, delivery semantics, keys and partitions, idempotent producer
- One blog or video on exactly-once semantics
- Polygon rate limits and message format docs (read twice)
- End-of-week understanding: topics, partitions, offsets, consumer groups, replication factor, idempotent producer settings

**Shippable by Sunday Nov 11 night:**

- WebSocket client to Kafka topic, running on GKE
- Trades and top-of-book quotes flowing for 10 symbols
- Prometheus metrics exposed, basic Grafana dashboard
- GitHub repo has real working code, CI green

**Resume status this week:** Not ready yet. Wait for Week 2 hardening before adding anything to the resume.

---

## Week 2 (Nov 12 to Nov 18): Ingestion Live & Hardened

## Week 2 (Nov 12 to Nov 18): Ingestion Live & Hardened

Goal: resume-ready ingestion by Tuesday night. BigQuery storage live, dashboards legit, Resume Update #1 submitted.

**Daily plan:**

- **Wed Nov 12:** BigQuery sink from Kafka via a simple Python consumer that batches inserts. Table partitioned by date, clustered by symbol.
- **Thu Nov 13:** Expand symbol list to 50-100 most-liquid US equities. Confirm throughput under load.
- **Fri Nov 14:** Message schema validation with Pydantic. Dead-letter topic for malformed messages. Alert on DLQ growth.
- **Sat Nov 15:** Grafana dashboard expanded: ingestion rate per symbol, p95/p99 latency, reconnect events, DLQ rate, BigQuery lag.
- **Sun Nov 16:** End-to-end chaos test. Kill the pod mid-stream and verify messages resume cleanly after restart.
- **Mon Nov 17:** Documentation sprint. Architecture diagram (Mermaid), README update, first design doc at `/docs/ingestion-design.md`.
- **Tue Nov 18:** Resume Update #1 goes in. Write `notes/understanding-week2.md`.

**Learning track:**

- GCP streaming patterns: Pub/Sub vs Kafka, BigQuery streaming inserts vs batch load
- WebSocket gotchas: heartbeats, half-closed connections, clock drift
- Prometheus and Grafana fundamentals (if unfamiliar)

**Shippable by Tue Nov 18 night:**

- 7+ days continuous uptime
- 50-100 symbols, trades and top-of-book quotes
- BigQuery persistence working
- Grafana dashboards legit
- DLQ pattern in place
- Architecture diagram in README

**Resume Update #1 (add to Projects section of resume):**

```
Tickstore - Real-time Market Data Platform [in progress]
Python, Kafka, GCP (GKE, BigQuery) | github.com/kaushikdivyam/tickstore

- Built fault-tolerant ingestion of real-time trade and quote feeds from Polygon.io
  WebSocket, consuming ~[X]K events/sec across 50+ US equity symbols with automatic
  reconnection, schema validation, and dead-letter handling; deployed on GKE with
  10+ days continuous uptime
```

Submit this version to the next 5-8 roles immediately.

---

## Week 3 (Nov 19 to Nov 25): Flink Fundamentals & First Stream Job

## Week 3 (Nov 19 to Nov 25): Flink Fundamentals & First Stream Job

Goal: PyFlink running on GKE, consuming from Kafka, producing to Kafka, with your first real windowed aggregation flowing. Deep learning on Flink internals is non-negotiable this week.

**Daily plan:**

- **Wed Nov 19:** PyFlink installed locally. Walk through official PyFlink tutorial end-to-end. Do the exercises.
- **Thu Nov 20:** Scaffold Flink job reading from `trades.raw` and writing to `trades.processed` with a minimal passthrough transform. Confirm end-to-end locally.
- **Fri Nov 21:** Deploy Flink to GKE using the Flink Kubernetes Operator. Checkpoint storage on GCS. JobManager plus one TaskManager minimum.
- **Sat Nov 22:** First real windowed aggregation: 1-minute tumbling window trade count per symbol. Emit to Kafka topic `metrics.trade_count_1min`.
- **Sun Nov 23:** Watermarking and event-time vs processing-time. Experiment deliberately with late events to understand behavior.
- **Mon Nov 24:** State backend deep-dive. Switch from heap to RocksDB. Measure state size growth.
- **Tue Nov 25:** Weekly understanding note (lighter day, Thanksgiving Eve). Verify everything clean before the holiday break.

**Learning track (critical, non-negotiable):**

- Flink docs: DataStream API, event time, watermarks, state primitives, checkpointing
- One read-through of Flink's "stateful stream processing" concepts page
- Full PyFlink tutorial
- One Flink Forward talk on watermarking (YouTube, pick any recent one)

**Shippable by Tue Nov 25:**

- PyFlink job on GKE, reading from Kafka, writing to Kafka
- First windowed aggregation flowing
- Checkpoints landing in GCS
- RocksDB state backend configured

**Holiday note:** Thanksgiving week starts Thursday. Expect reduced evening hours Wed Nov 25 through Sun Nov 29. Front-load the important Flink work early in the week.

**Resume status:** No update this week. Hold until real metrics (VWAP, spread) land in Week 4.

---

## Week 4 (Nov 26 to Dec 2): Metrics Computation

## Week 4 (Nov 26 to Dec 2): Metrics Computation

Goal: three real market metrics flowing (VWAP, bid-ask spread, rolling volume z-score). End-to-end latency instrumented. Resume Update #2 goes out.

**Daily plan:**

- **Wed Nov 26:** Lighter day. Write design doc for the 3 metrics: what you're computing, over what windows, with what state. Save to `/docs/metrics-design.md`.
- **Thu Nov 27:** Thanksgiving. Off.
- **Fri Nov 28:** Implement VWAP calculation. 1-min and 5-min tumbling windows per symbol.
- **Sat Nov 29:** Implement bid-ask spread (requires joining trades and quotes streams). Watch for quote staleness.
- **Sun Nov 30:** Implement rolling 5-min volume z-score per symbol. Measure state size impact.
- **Mon Dec 1:** End-to-end latency instrumentation. Tag messages with ingestion timestamp. Measure WebSocket-to-metric-output p50/p95/p99.
- **Tue Dec 2:** Resume Update #2. Weekly understanding note.

**Learning track:**

- Window semantics deep-dive: tumbling, sliding, session windows. Decide when each is appropriate.
- Checkpointing modes: at-least-once vs exactly-once. State recovery semantics.
- Stream-stream joins (your spread calc requires this)
- Market microstructure basics: what VWAP is and why, why spreads matter, what "liquid" means in practice

**Shippable by Tue Dec 2:**

- 3 metrics flowing continuously
- Latency instrumented end-to-end
- 2-3 weeks continuous uptime on ingestion side

**Resume Update #2 (replace earlier single bullet):**

```
Tickstore - Real-time Market Data Platform
Python, Kafka, Flink, GCP (GKE, BigQuery) | github.com/kaushikdivyam/tickstore

- Built fault-tolerant ingestion of real-time trade and quote feeds from Polygon.io
  WebSocket, ~[X]K events/sec across 100 US equity symbols with automatic reconnection,
  schema validation, and dead-letter handling; sustained [Y] days continuous uptime on GKE
- Implemented stateful stream processing on Flink with RocksDB state backend and GCS
  checkpointing, computing rolling windowed aggregations (1-min VWAP, bid-ask spread,
  5-min volume z-score) with p99 end-to-end latency of [Z]ms from WebSocket to metric output
```

Submit to next 5-8 roles. Re-engage any Round 1 recruiters who went cold with the updated resume.

---

## Week 5 (Dec 3 to Dec 9): Anomaly Detection Core

## Week 5 (Dec 3 to Dec 9): Anomaly Detection Core

Goal: first 3 anomaly detectors live, Slack alerting end-to-end, thresholds tuned against a real trading day.

**Daily plan:**

- **Wed Dec 3:** Design doc for all 5 detectors at `/docs/anomaly-design.md`. For each: definition, inputs, state needed, threshold heuristic, false-positive concerns.
- **Thu Dec 4:** Detector #1 - Spread Blowout. Flag when bid-ask spread exceeds N standard deviations above rolling mean. Tune N against 1 trading day.
- **Fri Dec 5:** Detector #2 - Quote Cross. Flag when bid > ask (shouldn't happen normally, pure data quality signal).
- **Sat Dec 6:** Detector #3 - Feed Gap. Flag when no ticks for symbol X for Y seconds during active hours (9:30-16:00 ET, excluding halts).
- **Sun Dec 7:** Slack webhook integration. Alert formatting (symbol, detector, timestamp, values). Dedup within 30-second window.
- **Mon Dec 8:** Run the system for a full trading day. Hand-review alerts for false positives. Adjust thresholds.
- **Tue Dec 9:** Weekly understanding note. Cleanup.

**Learning track:**

- Anomaly detection patterns: statistical vs rule-based vs ML-based. When each fits.
- Market microstructure: what causes spread blowouts (news, low liquidity), what feed gaps mean, why quote crosses indicate vendor issues
- Alert fatigue prevention: dedup windows, severity levels, escalation ladders

**Shippable by Tue Dec 9:**

- 3 detectors live
- Alerts flowing to Slack
- 1 full trading day of threshold tuning captured in notes
- Threshold decisions documented with rationale

**Resume status:** Hold update until Week 6 when 2 more detectors land and historical validation is done.

---

## Week 6 (Dec 10 to Dec 16): Expanded Detectors & Validation

## Week 6 (Dec 10 to Dec 16): Expanded Detectors & Validation

Goal: 2 more detectors live, historical replay validation complete, Resume Update #3 out.

**Daily plan:**

- **Wed Dec 10:** Detector #4 - Stale Tick. Flag same price for > N seconds during active hours, configurable per symbol by liquidity tier.
- **Thu Dec 11:** Detector #5 - Volume Spike. Flag when 1-min volume > N std dev above rolling 20-min mean.
- **Fri Dec 12:** Historical replay harness. Code that reads trade/quote data from a known historical date and feeds it through the detector pipeline at real speed.
- **Sat Dec 13:** Run historical replay against 3-5 known event days (pick days with flash moves, earnings, halts). Record all alerts fired. Hand-label truth.
- **Sun Dec 14:** Compute precision/recall per detector. Write up in `/docs/detector-validation.md`.
- **Mon Dec 15:** Tune thresholds from validation data. Re-run. Target under 10% false positive rate per detector.
- **Tue Dec 16:** Resume Update #3. Weekly understanding note.

**Learning track:**

- Precision/recall/F1 for anomaly detection
- Historical replay testing patterns in streaming systems
- Backtesting pitfalls: lookahead bias, survivorship bias, data snooping

**Shippable by Tue Dec 16:**

- 5 detectors live and tuned
- Historical validation with precision/recall numbers documented
- Validation harness re-runnable

**Resume Update #3 (add third bullet):**

```
- Designed real-time anomaly detection for 5 market data quality issues (spread
  blowouts, quote crosses, feed gaps, stale ticks, volume spikes); validated against
  historical trading sessions with [X]% precision at [Y]% recall
```

Apply to next 5-8 roles. Follow up with any recruiter who went quiet with the upgraded resume.

---

## Week 7 (Dec 17 to Dec 23): Polish, Monitoring, Hardening

## Week 7 (Dec 17 to Dec 23): Polish, Monitoring, Hardening

Goal: production-grade polish. State recovery tested. Dashboards clean. Last week before Christmas.

**Daily plan:**

- **Wed Dec 17:** Grafana dashboard polish. 3 panels (ingestion health, metrics output, anomaly activity). Each panel: title, description, clear axes, no clutter.
- **Thu Dec 18:** Prometheus alerting rules: DLQ growth, checkpoint failures, consumer lag, Flink task failures.
- **Fri Dec 19:** State recovery test. Kill the Flink job mid-stream. Verify clean restart from checkpoint. Document behavior.
- **Sat Dec 20:** Load test. Scale from 100 to 500 symbols. Measure latency and state size impact. Document breaking points.
- **Sun Dec 21:** Code cleanup. Type hints throughout. Fix TODOs. Push detector test coverage past 80%.
- **Mon Dec 22:** README overhaul. Architecture diagram embedded. Quickstart section. Design-decisions section linking to `/docs/`.
- **Tue Dec 23:** Weekly understanding note. Pre-holiday light cleanup.

**Learning track:**

- Observability patterns: signals, SLIs, SLOs
- Flink operator and job failure recovery semantics
- Testing patterns for streaming systems

**Shippable by Tue Dec 23:**

- Production-grade monitoring with alert rules
- State recovery validated and documented
- 500-symbol scale tested with numbers in docs
- README is interview-ready (anyone can clone and run)
- 80%+ test coverage on critical paths

**Holiday note:** Christmas Eve is Thursday. Expect lower productivity Dec 24-25. Don't count on them.

**Resume status:** No update this week. Wait for Week 8 blog + video for the complete section.

---

## Week 8 (Dec 24 to Dec 30): Writeup, Blog, Final Push

## Week 8 (Dec 24 to Dec 30): Writeup, Blog, Final Push

Goal: ship blog, video walkthrough, final resume section. Submit the final wave of applications before Jan 1.

**Daily plan:**

- **Wed Dec 24:** Christmas Eve, lighter day. Draft blog outline: Problem, Architecture, 2-3 Design Decisions with rationale, Lessons Learned, What's Next.
- **Thu Dec 25:** Christmas. Off.
- **Fri Dec 26:** Blog post draft #1. Target 2000-2500 words. Your voice, not Claude's. Include 2-3 diagrams.
- **Sat Dec 27:** Blog post draft #2 after sleeping on it. Tighten. Add technical depth. Publish on Medium, dev.to, Substack, or personal site.
- **Sun Dec 28:** Video walkthrough. 3-5 min screen record. Architecture explanation + live demo of a detected anomaly. OBS or Loom.
- **Mon Dec 29:** Final README polish. Blog and video links embedded. Clean open issues. Pin the repo on GitHub profile. Update LinkedIn.
- **Tue Dec 30:** Resume Update #4 final. Submit last wave of 5-8 applications. Year-end wrap note.

**Learning track:**

- How to write a credible engineering blog post (read 2-3 strong examples for voice)
- What recruiters actually scan for when they click a project GitHub repo

**Shippable by Tue Dec 30:**

- Published blog post with public link
- 3-5 min video walkthrough in README
- Pinned repo on GitHub profile
- LinkedIn updated
- Resume Projects section complete (all 4 bullets with real numbers)
- 25-30 total applications submitted across the 8 weeks

**Resume Update #4 (final, complete section):**

```
Tickstore - Real-time Market Data Platform & Streaming Anomaly Detection
Python, Kafka, Flink, GCP (GKE, BigQuery), Prometheus/Grafana
github.com/kaushikdivyam/tickstore | [blog link]

- Built fault-tolerant ingestion of real-time trade and quote feeds from Polygon.io
  WebSocket, consuming ~[X]K events/sec across 500+ US equity symbols with automatic
  reconnection, schema validation, and dead-letter handling; sustained [Y]+ days
  continuous uptime on GKE
- Implemented stateful stream processing on Flink with RocksDB state backend and
  GCS checkpointing, computing rolling windowed aggregations (1-min VWAP, bid-ask
  spread, volume z-score) with p99 end-to-end latency of [Z]ms from WebSocket to sink
- Designed real-time anomaly detection for 5 market data quality issues (spread
  blowouts, quote crosses, feed gaps, stale ticks, volume spikes); validated against
  historical sessions with [P]% precision at [R]% recall
- Instrumented with Prometheus/Grafana monitoring and Slack alerting; documented
  architecture, design decisions, and tradeoffs in accompanying blog post [link]
```

**By Jan 1, 2027:** shipped project, published writeup, 25-30 applications in flight, interview prep ongoing, first onsites potentially scheduled.

---

## Resume Rollout Milestones

## Resume Rollout Milestones

Four staged updates, not one big-bang at the end. Each stage lets you keep applying with a more credible resume than the last.

| Week | Milestone | Resume Bullets | Application Action |
| --- | --- | --- | --- |
| 2 | Ingestion hardened | 1 bullet (ingestion) | Apply to 5-8 roles |
| 4 | Metrics flowing + latency measured | 2 bullets (+ stream processing) | Apply to 5-8 more; re-engage Round 1 recruiters |
| 6 | 5 detectors validated | 3 bullets (+ anomaly detection) | Apply to 5-8 more; follow up with quiet recruiters |
| 8 | Blog + video shipped | 4 bullets (complete + links) | Final 5-8 applications; LinkedIn update |

**How to re-engage a recruiter who went quiet:**

Short email, one paragraph: "Wanted to share an update since we last spoke - shipped the next phase of a project I mentioned \[brief description with new numbers\], updated resume attached. Happy to reconnect if timing works better now." Don't apologize for the follow-up. Don't over-explain. Numbers do the talking.

**How to decide whether to re-apply to a firm that already rejected:**

- Within 3 months of rejection: don't re-apply to same req, but a different req at the same firm is fine
- 3-6 months after rejection: can re-apply if your resume meaningfully changed (yours will)
- With a new referral: reset the clock, apply immediately regardless of prior rejection

**Each stage's bullet text lives in its week section above.** Copy-paste verbatim at each milestone, filling the `[X]`, `[Y]`, `[Z]` placeholders with the real numbers from your Grafana dashboard.

---

## Parallel Application Strategy

## Parallel Application Strategy

Total: 25-30 applications across 8 weeks. Portfolio distribution matters more than volume.

**Portfolio distribution:**

| Tier | Share | Target Firms |
| --- | --- | --- |
| Big tech streaming | 30% (\~8) | Databricks, Snowflake, Datadog, Confluent, Netflix, Google Dataflow team, Meta Data Platform |
| Quant multi-manager DE | 25% (\~7) | Millennium, Point72 (incl. KEPL), Citadel DSG, Balyasny, Walleye, GSA, Qube |
| Fintech infra | 20% (\~6) | Stripe, Plaid, Ramp, Mercury, Robinhood, Coinbase |
| Market makers | 15% (\~4) | Virtu, IMC, Optiver, Flow Traders, SIG, Akuna |
| Big tech data (non-streaming) | 10% (\~3) | AWS (Kinesis/MSK/Glue), Azure (Event Hubs), Apple data, LinkedIn |

**Weekly pacing:**

| Week | Apps | Resume Version | Where to Focus |
| --- | --- | --- | --- |
| 1 | 5-8 | Current (no project) | Big tech + market makers + fintech (least finance-pedigree sensitive) |
| 2 | 5-8 | Update #1 (ingestion) | Add quant multi-managers |
| 3-4 | 5-8 | Update #2 (stream processing) | All tiers; chase referrals hard |
| 5-6 | 5-8 | Update #3 (anomaly detection) | Re-engage silent apps + follow-up emails |
| 7-8 | 5-8 | Update #4 (complete) | Final cold apps + push on hot leads |

**Tuesday night application ritual (90 min):**

- Pick 5-8 reqs from your running tracker spreadsheet (columns: Firm, Role, Posted, Status, Referral?, Notes)
- Submit via each company's own portal, not LinkedIn Easy Apply
- Reach out to 1-2 potential referrers (Columbia OR + IIT Delhi alumni at target firms)
- Log all submissions with timestamp + expected follow-up date

**Referral hunt priorities:**

- Columbia OR alumni at Citadel, Two Sigma, DE Shaw, Jane Street, Cubist, Millennium
- IIT Delhi alumni at any big tech + quant shop (strong network)
- Your NYC tech uncle: one ask covering his whole network, specific ask: "anyone at Citadel, Point72, Millennium, Databricks, Datadog, or Stripe?"
- Surbhi at Goldman: one ask about her network's hedge fund crossover
- Target: 5-8 cold LinkedIn messages per week with specific ask

**Rules to follow:**

- Don't customize cover letters per role; use a short 3-sentence intro
- Never LinkedIn Easy Apply if a company portal exists
- Don't re-apply to the same req within 3 months; different req at same firm is fine
- 5-8 well-researched apps per week beats 30 lazy ones
- Follow-up emails produce \~30% of your actual interviews; never skip them
- Track every app in your spreadsheet or it becomes chaos by Week 4

---

## Interview Defense Reading List

## Interview Defense Reading List

Everything you claim to have built, you must defend when asked in an interview. For each technology or pattern in the project, here's what you need to understand well enough to speak to under pressure.

**Kafka (foundational):**

- Topics, partitions, offsets, consumer groups
- Replication factor and durability guarantees
- Idempotent producer vs transactions vs exactly-once semantics
- Rebalancing and consumer group coordination
- Backpressure signals

**Flink (critical - biggest interview risk if shallow):**

- DataStream API and operator composition
- Event time vs processing time vs ingestion time
- Watermarks: generation, propagation, late event handling
- Window types (tumbling, sliding, session) and when each fits
- State primitives: ValueState, ListState, MapState; keyed vs operator state
- State backends: heap vs RocksDB and the tradeoffs
- Checkpointing: aligned vs unaligned, exactly-once semantics, incremental checkpoints
- Failure recovery: what happens when a TaskManager dies mid-window
- Backpressure and what it signals

**Streaming patterns (framework-agnostic):**

- Stream-stream joins (interval joins, windowed joins)
- Enrichment via broadcast state
- Session vs tumbling window decisions
- At-least-once vs exactly-once vs effectively-once semantics
- Outer joins and watermark implications

**GCP / Kubernetes:**

- GKE primitives: pods, deployments, services, node pools
- GKE autoscaling (cluster + horizontal pod)
- Flink Kubernetes Operator semantics (how it manages JobManager + TaskManagers)
- BigQuery streaming inserts vs batch loads; cost implications
- GCS consistency model for Flink checkpoints

**Market data domain (minimum):**

- Trades vs quotes; top-of-book vs L2 depth
- Exchange-reported timestamps vs vendor-received timestamps
- Market hours and session structure (pre-market, regular, after-hours)
- Why spreads widen (news, open/close, low liquidity, halts)
- Corporate actions (splits, dividends) exist, even if you don't handle them
- Common data quality issues: gaps, bad prints, crossed quotes, stale ticks

**Anomaly detection:**

- Rolling statistics (mean, variance) with EMA vs simple moving windows
- Z-score assumptions (normality, stationarity) and where they break
- Threshold tuning and precision/recall tradeoffs
- Why rule-based detection scales better than ML for data quality signals

**Weekly reading cadence:**

- Pick 1-2 topics per week, 30-45 min reading official docs on each
- Write a 100-word "I now understand" entry in `notes/`
- By end of Week 8: comfortable defending every topic above when probed

---

## Risk & Contingency

## Risk & Contingency

The plan is aggressive. Things will slip. Here's how to handle each scenario without derailing the whole thing.

**If 1 week behind by end of Week 4:**

- Cut: 5 detectors to 3. Keep spread blowout, quote cross, feed gap. Drop stale tick and volume spike.
- Keep: all core infra (Flink, checkpointing, deployment, monitoring)
- Resume still reads strong

**If 2 weeks behind by end of Week 6:**

- Cut: historical replay validation harness. Replace with "validated against N live alerts during active trading sessions."
- Cut: video walkthrough. Keep the blog post (carries more recruiter weight).
- Scope final push to ingestion + processing + 3 detectors, skip the anomaly validation bullet

**If CVS has a crunch week:**

- Default: skip that week's project work entirely; don't try to half-do both
- Rebuild timeline by pushing each subsequent week back
- Catchup: use the following weekend for 10+ hour days
- Never sacrifice sleep; 5 hrs sleep for 2 weeks costs you more than the week you saved

**If a technical wall eats 3+ days (Flink state bug, Polygon reconnect loop, etc.):**

- Rule: if stuck 6 hours, change strategy
- Options: simplify (drop RocksDB for heap state), workaround + document as known limitation, or cut that feature
- Don't disappear into debugging for a week. Scope matters more than completeness.

**If an offer lands mid-project (Week 4-6):**

- Finish the project anyway; it stays valuable for future moves
- Negotiate hard using parallel apps as leverage
- Don't accept first offer unless it's overwhelming

**If nothing responds after 20 applications:**

- Audit resume: is it reaching the HM or ATS-rejected? Ask a recruiter friend to review.
- Audit bullets: still too healthcare-vague? Rewrite more aggressively.
- Audit seniority: applying too senior (staff level)? Drop to mid-level only.
- Audit firm selection: expand to adjacent tiers you ignored.

**Fallback resume bullets if the project is only 60% done by Jan 1:**

```
Tickstore - Market Data Streaming Pipeline [personal project]
Python, Kafka, Flink, GCP | github.com/kaushikdivyam/tickstore

- Built real-time ingestion of US equity trades and quotes from Polygon.io WebSocket
  into Kafka, deployed on GKE with automatic reconnection and schema validation
- Implemented Flink-based stream processing for rolling market metrics (VWAP, spread)
  with stateful windowing and GCS checkpointing
```

Two honest bullets with a working link beat four aspirational ones with vapor. Ship what you ship.
