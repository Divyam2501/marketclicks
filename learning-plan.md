# Learning + Implementation Plan: tickstore

Companion to `project-bible.md`. The bible is the *what* and *when*. This document is the *what to learn and build inside each block*.

**Owner:** Divyam Kaushik
**Window:** Oct 13, 2026 - Sep 30, 2027
**Budget assumption:** 12-15 hrs/week average, same as the bible.

---

## How to use this doc

- Each phase has: scope goal, implementation checklist, reading list with hour estimates, and a "learning deliverable" that proves you absorbed the reading.
- Reading is anchored to the block where it unlocks implementation. Not front-loaded.
- Treat the "learning deliverable" as non-skippable. If you build without it, you've over-leveraged AI (see bible AI rules).
- DDIA is the technical spine. FoDE is kept as a vocabulary-polish reference for interview week only.

---

## Scope locks (do these in Block 1, before any Phase 1 code)

These close the ambiguities I flagged in the architect review. Each is a short written artifact that lives in the repo.

1. **Problem spec (`docs/00-problem-spec.md`)** — 2 pages:
   - Target query shapes (top 3-4, specific)
   - Durability targets (RPO/RTO after kill -9)
   - Scale ceiling (single node, N symbols, M days)
   - Explicit non-goals (distributed, SQL, UDFs, schema evolution if cut)
2. **Polygon free-tier reality check** — symbol count, message rate, websocket limits. Decide on symbol set now (e.g. SPY + 10 liquid tech names).
3. **Phase 3 language commit** — Go unless specific Rust pull. Pull forward from Block 6 to Block 1.
4. **Benchmark scope** — pin ClickHouse as primary comparison. Treat QuestDB as optional stretch, not baseline.

---

## Resource stack

**Buy / download now:**
- DDIA (Kleppmann) — the spine
- Harris "Trading and Exchanges" — market data vocabulary
- Alex Xu "System Design Interview" vol 1 and 2 — for interview prep ramp
- Brendan Gregg "Systems Performance" — chapters 2 and 5 only, read before Phase 4 benchmarks
- FoDE (Reis/Housley) — you already have it. Reference only. Skim ch 2, 3, 11 in July 2027 before behavioral loops.

**Free / online:**
- Polygon.io docs
- Kafka Design docs + Jay Kreps "The Log"
- Parquet format spec
- Dremel paper (Google, 2010)
- ClickHouse MergeTree architecture posts
- DuckDB execution docs
- Confluent "Exactly-Once Semantics" blog series
- Donne Martin system-design-primer GitHub repo
- Writing voice models: Dan Luu, Marc Brooker, Jamie Brandon (5 posts across these before Post 1)

---

## Phase 1: Ingestion (Blocks 1-3, Oct 13 - Nov 23)

**Goal:** Python feed handler writes Polygon ticks to partitioned Parquet. Full trading day with no crashes.

### Implementation checklist

- [ ] Scope-lock artifacts above (Block 1)
- [ ] Polygon websocket client, AAPL only (Block 1)
- [ ] Reconnection / backoff logic
- [ ] Expand to pinned symbol set (Block 2)
- [ ] Parquet writer: partition by symbol and hour
- [ ] Out-of-order and duplicate handling
- [ ] Market-close / after-hours handling
- [ ] Full trading day run, no crashes
- [ ] Tag `v0.1-phase1` (Block 3)
- [ ] Publish Post 1 (Block 3)

### Reading (hours)

- DDIA Ch 1 "Reliable, Scalable, Maintainable" (2-3 hrs) — Block 1
- DDIA Ch 4 "Encoding and Evolution" first pass (3-4 hrs) — Blocks 2-3, read alongside Parquet work
- Harris chapters 1-6 and 23 (10-12 hrs) — Blocks 1-2
- Polygon.io docs cover to cover (3-4 hrs) — Block 1

### Skills to learn if rusty

- Python `asyncio`: event loop, `async`/`await`, `gather`, cancellation
- `websockets` library
- `pyarrow`: dataset partitioning, row groups, schemas
- `pydantic` or `dataclasses` for tick schema
- Docker + docker-compose
- Git tagging discipline

### Learning deliverable

- One-page `docs/01-ingestion-notes.md`: why Parquet, why partition by symbol+hour, what reconnection semantics you chose, what you'd change at 10x scale.

---

## Phase 2: Streaming (Blocks 4-8, Nov 24 - Feb 1)

**Goal:** Feed handler + Kafka + persister with documented recovery semantics. OHLCV aggregator on top.

### Implementation checklist

- [ ] Kafka local via Docker (Block 4)
- [ ] Trivial producer/consumer pair (Block 4)
- [ ] Refactor Phase 1 into feed handler + persister across Kafka (Block 5)
- [ ] Offset management
- [ ] Consumer group rebalancing handling
- [ ] Dedup on persister via source sequence numbers (Block 6)
- [ ] Design doc `docs/02-delivery-semantics.md` — pin exactly which boundary you claim semantics across (Block 6)
- [ ] Deliberate failure tests + documented recovery (Block 7)
- [ ] OHLCV 1-second tumbling window consumer (Block 8)
- [ ] Late-arrival handling
- [ ] Tag `v0.2-phase2` (Block 8)
- [ ] Publish Post 2 (Block 8)

### Reading (hours)

- Kafka Design docs cover to cover (3-4 hrs) — Block 4
- Jay Kreps "The Log" (2 hrs) — Block 4
- DDIA Ch 11 "Stream Processing" (4-5 hrs) — Block 4
- DDIA Ch 8 "The Trouble with Distributed Systems" (3-4 hrs) — Block 6, before failure testing
- DDIA Ch 7 "Transactions" first ~60% (isolation levels, serializability) (3-4 hrs) — Block 6
- Confluent "Exactly-Once Semantics" blog series (2 hrs) — Block 7

### Skills to learn

- Kafka internals: topics, partitions, offsets, consumer groups, rebalancing, log compaction, ISR, idempotent + transactional producers
- Event-time vs processing-time, watermarks, late data
- Protobuf (pick over Avro — more common at target companies)
- Why "exactly-once" is really "effectively-once" and where the boundary sits

### Learning deliverable

- `docs/02-delivery-semantics.md` — this is the backbone of Post 2. Non-skippable. If you can't write it, you don't understand your own system.

---

## Phase 3: Storage (Blocks 9-11, Feb 2 - Mar 15)

**Goal:** Custom columnar on-disk format in Go (or Rust). Writer + reader + encoding + compression. Benchmarks vs Parquet.

### Implementation checklist

- [ ] Design doc `docs/03-storage-format.md` on paper first (Block 9)
- [ ] Columnar layout decision (row groups, page sizes)
- [ ] Encoding per column type: dictionary for symbols, delta or delta-of-delta for timestamps, bit-packing where applicable
- [ ] Index strategy: zone maps per block, optional bloom filters
- [ ] File size strategy
- [ ] Writer implementation (Block 10)
- [ ] Reader implementation (Block 10)
- [ ] Reader throughput benchmark vs Parquet (Block 10)
- [ ] zstd compression layer (Block 11)
- [ ] Compression ratio measurement vs Parquet+Snappy (Block 11)
- [ ] Tag `v0.3-storage` (Block 11)
- [ ] Publish Post 3 (Block 11)

### Reading (hours) — heaviest block

- DDIA Ch 3 "Storage and Retrieval" (5-7 hrs, read twice) — Block 9
- DDIA Ch 4 re-read with storage lens (2 hrs) — Block 9
- Parquet format spec (3-4 hrs) — Block 9
- Dremel paper (2-3 hrs) — Block 9
- ClickHouse MergeTree architecture posts (2 hrs) — Block 9
- One foundational columnar paper: C-Store (Stonebraker) OR MonetDB/X100 (Boncz) (2-3 hrs) — Block 9
- DDIA Ch 2 "Data Models" (3-4 hrs) — optional, Block 10 if time

### Skills to learn

- Go deeply enough for byte-level work: `encoding/binary`, slices as buffers, allocation patterns, benchmarks via `testing.B`. If Rust: `bytes` crate, ownership for buffer management.
- Encoding techniques: dictionary, delta, delta-of-delta, RLE, bit-packing, frame-of-reference
- Compression trade-offs: zstd vs lz4 vs snappy, speed vs ratio
- Index strategies: zone maps, bloom filters, primary key sorting (MergeTree-style)

### Learning deliverable

- `docs/03-storage-format.md` — complete byte-level spec. If an interviewer asks "walk me through your file layout," this doc is your answer.
- A 150-word plain-English summary of DDIA Ch 3 written from memory.

---

## Phase 4: Query + Benchmarks (Blocks 13-15, Mar 30 - May 10)

**Goal:** gRPC/HTTP service wrapping storage. ClickHouse loaded as comparison. Reproducible benchmark harness. Flagship post.

### Implementation checklist

- [ ] gRPC or HTTP service around storage layer (Block 13)
- [ ] Time-range scan query interface
- [ ] Predicate pushdown implementation
- [ ] ClickHouse installed, same data loaded (Block 13)
- [ ] Benchmark harness: point lookups, range scans, aggregations (Block 14)
- [ ] p50/p95/p99 latency + throughput measurement
- [ ] Warmup, repetition, variance handling documented
- [ ] (Optional) QuestDB as third backend
- [ ] Methodology doc `docs/04-benchmark-methodology.md` (Block 14)
- [ ] Charts + reproducibility notes (Block 15)
- [ ] Publish Post 4 — the flagship (Block 15)
- [ ] Tag `v0.4-benchmarks` (Block 15)

### Reading (hours)

- DDIA Ch 10 "Batch Processing" (3-4 hrs) — Block 13, read before harness design
- DDIA Ch 3 targeted re-read on columnar execution (1-2 hrs) — Block 15, before Post 4
- ClickHouse execution docs (2 hrs) — Block 13
- DuckDB execution docs (2 hrs) — Block 13
- Brendan Gregg "Systems Performance" Ch 2 (methodology) + Ch 5 (applications) (4-5 hrs) — Block 13
- ClickBench methodology notes (1 hr) — Block 14
- Gil Tene's "How NOT to Measure Latency" talk (1 hr) — Block 14, coordinated-omission is table stakes for a credible benchmark post

### Skills to learn

- Query execution concepts: predicate pushdown, projection pushdown, vectorized execution, late materialization
- Protobuf service definitions, gRPC streaming vs unary
- Operating ClickHouse: schema design, MergeTree partitioning, loading
- Benchmark statistics: percentiles, distribution reporting, coordinated omission, warmup, cache-warm vs cache-cold runs

### Learning deliverable

- `docs/04-benchmark-methodology.md` — the single most important document in the repo for interview defensibility. A storage engineer at a target company will tear this apart first.

---

## Phase 5: Optional (Blocks 16-18, May 11 - Jun 21)

**Default: skip unless on track and interview prep hours are secure.** If you proceed, pick one:

- **Option A:** OCaml rewrite (only if Jane Street is specifically still a target) — adds Real World OCaml, 2-3 weeks evening study
- **Option B:** Streaming queries (continuous queries over the live feed) — adds DDIA Ch 11 revisit, materialized view literature
- **Option C:** Replay mode (deterministic replay of historical ticks) — lightest option

Default recommendation (per bible): Option B or C.

### Reading if B or C

- DDIA Ch 11 re-read with queries lens (2 hrs)
- Materialize blog posts on incremental view maintenance (2 hrs)

---

## Interview prep track (parallel, from Block 7)

### Leetcode ramp (per bible)

- Block 7 (Jan): 1 easy/day on weekdays
- Block 12 (Mar): 1 medium/day
- Block 19 (Jun): 2 mediums/day
- Patterns: arrays/strings, hash maps, two pointers, sliding window, heaps, graphs (BFS/DFS), basic DP, binary search
- No hards needed for DE roles

### System design reading (Blocks 19-22)

- DDIA Ch 5 "Replication" (4-5 hrs) — Block 19
- DDIA Ch 6 "Partitioning" (3-4 hrs) — Block 19
- DDIA Ch 7 "Transactions" finish back half (3 hrs) — Block 20
- DDIA Ch 9 "Consistency and Consensus" (5-7 hrs) — Block 20, the hardest chapter
- DDIA Ch 12 "The Future of Data Systems" (2-3 hrs) — Block 22, light capstone read
- Alex Xu vol 1 and 2 (20-30 hrs across Blocks 19-21)
- Donne Martin system-design-primer (5-10 hrs ongoing reference)
- FoDE Ch 2, 3, 11 for vocabulary polish (6 hrs) — one weekend in July 2027

### DE-specific system design problems to practice

- Design a metrics pipeline
- Design a log aggregation system
- Design a feature store
- Design a real-time recommendation system

### Project deep-dive prep

- One-pager per phase in `docs/` — covers key design decisions and alternatives considered (you've been building these all year)
- 90-second verbal pitch (Block 12)
- 10-minute whiteboard version (Block 19)
- 30-minute deep-dive version (Block 20)

### Behavioral

- 6-8 STAR stories pre-written: conflict, failure, leadership, ambiguity, scope cut, prioritization
- Draw from CVS experience, not just the project

---

## Writing track (parallel, from Block 3)

### Craft reading (Block 3, before Post 1)

- 5 posts across Dan Luu, Marc Brooker, Jamie Brandon — pick ones you like, don't skim
- Optional: Zinsser "On Writing Well" for craft improvement

### Habits

- One-line "what surprised me this week" log — fuel for future posts
- Explain-back test every week: pick one piece of code from the week, whiteboard it to yourself
- Every 2-4 weeks: private note explaining what you built and why. Painful = over-leveraged AI. Fix that section.

### Post schedule (from bible)

- Post 1: Block 3 (Nov 10-23, 2026)
- Post A (CVS-derived): Nov-Dec 2026
- Post 2: Block 8 (Jan 19 - Feb 1, 2027)
- Post B (CVS-derived): Jan-Feb 2027
- Post 3: Block 11 (Mar 2-15, 2027)
- Post 4 (flagship): Block 15 (Apr 27 - May 10, 2027)
- Post 5: Block 18 (Jun 8-21, 2027)
- Post 6 (capstone): Block 25 (Sep 14-30, 2027)

---

## Minimum viable reading if time-constrained

In priority order. If you execute just this list, the project is interview-defensible.

1. DDIA Ch 1 — before Phase 1
2. DDIA Ch 4 — before Phase 1 end
3. DDIA Ch 11 — before Phase 2
4. DDIA Ch 8 — before Phase 2 failure testing
5. DDIA Ch 3 — before Phase 3 (read twice)
6. DDIA Ch 10 — before Phase 4
7. DDIA Ch 5, 6, 7, 9 — before interview season
8. Harris Ch 1-6 — before Phase 1
9. Kafka Design docs + "The Log" — before Phase 2
10. Parquet spec + Dremel paper — before Phase 3
11. ClickHouse + DuckDB architecture posts — before Phase 4
12. Brendan Gregg Systems Performance Ch 2 and 5 — before Phase 4
13. Alex Xu vol 1 + Donne Martin primer — Blocks 19-20

Everything else is bonus.

---

## Drop list if time-constrained

In priority order (first = cut first):

1. OCaml / Real World OCaml — only if Jane Street still a target
2. OSTEP — nice-to-have, not required for DE interviews
3. Petrov "Database Internals" — overlaps DDIA
4. Cartea/Jaimungal/Penalva — vocabulary only, skim
5. FoDE full read — reference only, don't read front-to-back
6. QuestDB as benchmark target — stick with ClickHouse alone
7. Phase 5 — treat as optional, skip if on edge

---

## Weekly discipline (the three habits)

1. **One defensibility pass per week.** Pick one piece of code from the week, whiteboard-explain to yourself. If you can't, rework or discard.
2. **One 150-word summary per DDIA chapter.** Written from memory. If painful, re-read.
3. **One "what surprised me" line per week.** Fuel for posts.

These three habits are the compounding assets. If a block runs over, cut implementation scope before cutting these.
