# Project Bible: Market Data Platform
## 11-Month Plan to Top-of-Field DE Visibility

**Owner:** Divyam Kaushik
**Start:** October 13, 2026
**Target completion:** September 30, 2027
**Primary goal:** Clearly ahead of the lower and middle layer of DEs at 1-3 years of experience, and visibly identifiable in the field.
**Secondary goal:** Land at a top-tier DE/platform role (big tech data infra, quant-adjacent platform, or top fintech) during active applications in July-September 2027.

---

## Non-negotiable Principles

1. **Depth over breadth.** One project done deeply beats three projects done shallowly. Writing quality over quantity.
2. **Design before code.** Every substantive design decision must be yours. See AI usage rules.
3. **Interview defensibility test.** Every piece of code in this project must be defensible under interview scrutiny. If you can't walk through it on a whiteboard, rework it or cut it.
4. **Ship imperfect over polish perfect.** The repo that exists beats the repo that was going to be more unique. First commit matters more than final commit.
5. **Finish is the moat.** Most engineers start projects like this. Few finish. Finishing is the differentiator.
6. **Writing is non-negotiable.** Code without writing is low-visibility. Writing is the compounding asset.

---

## Project Overview

**What:** A real-time market data platform that ingests tick data from a live feed, persists it in a custom columnar format, serves low-latency queries, and publishes benchmarks against ClickHouse and QuestDB.

**Why:** Hits streaming, storage internals, query performance, and systems reasoning in one coherent system. Quant-adjacent without being quant. Produces 4-5 blog posts worth of material. Signals depth that composition-layer cloud DE work does not.

**Name:** `tickstore` (or similar — boring and precise, not clever).

**Repo:** Public GitHub, created Day 1 with a one-paragraph README.

**Tech stack:**
- Phase 1: Python for ingestion
- Phase 2: Kafka or Redpanda, Python or Go for consumers
- Phase 3: Rust or Go for storage layer (pick before Phase 3, learn basics in evenings during Phase 2)
- Phase 4: gRPC or HTTP for query service, benchmark harness in Python
- Phase 5: Optional, skip unless on track

---

## AI Usage Rules (The Core Discipline)

### Use AI aggressively for:
- **Phase 1 plumbing:** websocket client, Parquet writer glue, reconnection logic, config, Docker setup, CLI scaffolding
- **Build tooling:** Makefiles, Dockerfiles, CI setup, benchmark harness skeleton
- **Boilerplate wrappers:** gRPC/HTTP service wrappers around your storage layer
- **Tests:** writing comprehensive test suites (but read every test)
- **Documentation first drafts:** READMEs, API docs, design doc skeletons (heavy edit after)
- **Debugging:** "here's an error, here's the context, what's likely wrong" — smart rubber duck

### Do NOT use AI for (interview-defensibility critical):
- **Phase 2 streaming semantics:** exactly-once reasoning, offset management, deduplication logic, failure recovery design
- **Phase 3 storage format:** every byte-level decision (encoding, compression, index layout, file format)
- **Phase 4 query layer design:** predicate pushdown, projection handling, result merging
- **Benchmark methodology:** how you measure, what workloads you pick, how you report
- **Blog posts:** write first draft entirely yourself; AI only for editorial polish (clarity, grammar, cuts)

### Workflow discipline:
1. **Design-first, implement-second.** Spend time on design on paper. Only then use AI to generate implementation code.
2. **Explain-back test.** After AI generates non-trivial code, close the window and explain it out loud. If you can't, rework or discard.
3. **LLM as teacher, not author.** "Explain X" is good. "Build X for me" is dangerous for learning-critical pieces.
4. **Review mode > generation mode.** "Here's my design, what am I missing?" beats "design this for me."
5. **Every 2-4 weeks:** write a private note explaining what you built and why. If painful, you've over-leveraged AI. Fix that section.

### Target split:
- 30-40% of code: AI-generated plumbing (commodity work)
- 10-20%: AI-assisted (reviewed, understood, edited)
- 40-60%: your own thinking (design-heavy parts)

### Blog writing split:
- 100% your own thinking and voice
- 10-20% AI for editorial polish only
- Never let AI write the opinions or technical takes

---

## Front-loaded Reading (Weeks 1-4, ~25-30 hours total)

Non-negotiable. Without these, you'll build the wrong thing in later phases.

1. **Market data basics (15 hours):** Polygon.io docs cover to cover + Harris "Trading and Exchanges" chapters 1-6 and 23. Covers tick vs quote, L1 vs L2, NBBO, trade conditions, consolidated vs direct feeds.

2. **Kafka mental model (5 hours):** Kafka docs "Design" section + Jay Kreps' "The Log" article. Covers topics, partitions, offsets, consumer groups, ordering guarantees.

3. **Columnar storage concepts (4 hours):** Dremel paper + Parquet format docs (skim) + one ClickHouse "why it's fast" blog post. Covers columnar vs row, dictionary encoding, delta encoding, vectorized execution.

4. **Rust or Go basics (one weekend):** If you don't know either, pick Go (faster to productivity). Non-trivial program in it before Phase 3.

## In-Phase Reading (do during phases as needed)

- **Phase 2:** DDIA chapter 11 (streaming).
- **Phase 3:** DDIA chapters 3 and 4 (storage, encoding), Parquet spec, ClickHouse MergeTree posts, C-Store or Monetdb/X100 paper.
- **Phase 4:** ClickHouse execution docs, DuckDB execution docs, benchmark methodology (Brendan Gregg short section, Percona/ClickBench posts).
- **Phase 5A (if OCaml):** Real World OCaml book, 2-3 weeks of evening study.

## Over-the-year Reading (ambient)

- **DDIA cover to cover.** Chapters 5-9 are critical for system design interviews.
- **Alex Petrov "Database Internals"** after DDIA, if time.
- **OSTEP** for OS fundamentals if you don't have them.
- **Market microstructure** beyond Harris: skim Cartea/Jaimungal/Penalva for vocabulary.
- **Writing voice models:** Dan Luu (danluu.com), Marc Brooker (brooker.co.za), Jamie Brandon (scattered-thoughts.net). Read 5 posts across these before writing your own first post.

---

## Biweekly Timeline

Target: 12-15 hours/week average, so 24-30 hours per biweekly block. Hour counts are averages, not minimums. Aim for the biweekly total. If you hit 70% of the plan, you're on track.

### Block 1: Oct 13 - Oct 26 — Foundation & first commit
- Sign up for Polygon.io free tier
- Read Polygon docs end to end
- Read Harris chapters 1-6
- Create GitHub repo with one-paragraph README
- First commit: Python script connecting to Polygon websocket, printing AAPL ticks, handling reconnection
- Commit at least 5 days out of 14
- **Hours:** 20-25 (lighter — Q4 at work)

### Block 2: Oct 27 - Nov 9 — Phase 1 proper
- Expand to 20-50 symbols
- Write ticks to Parquet, partitioned by symbol and hour
- Handle websocket reconnections, market close, after-hours, out-of-order ticks, duplicates
- Full trading day run with no crashes
- **Hours:** 25-30

### Block 3: Nov 10 - Nov 23 — Phase 1 ships + first post
- Finalize Phase 1 readme, tag `v0.1-phase1`
- Set up personal domain (yourname.com)
- Pick Hugo or Astro for static site
- **Write Post 1:** "What I learned ingesting a day of real market data" (1200 words)
- Publish by end of block
- Thanksgiving falls in this block — plan for one week off
- **Hours:** 25-30 (writing budget: 6-8 hours)

### Block 4: Nov 24 - Dec 7 — Kafka setup & theory
- Read Kafka Design docs cover to cover
- Read Jay Kreps "The Log"
- Start DDIA chapter 11
- Set up Kafka locally in Docker
- Write trivial producer/consumer pair
- **Hours:** 20-25

### Block 5: Dec 8 - Dec 21 — Phase 2 start (split services)
- Refactor Phase 1 into feed handler + persister with Kafka in between
- Proper offset management
- Handle consumer group rebalancing
- First wave of weird streaming bugs
- **DO THIS YOURSELF — no AI generation of design decisions**
- **Hours:** 25-30

### Block 6: Dec 22 - Jan 4 — Holiday block (light)
- Target: implement dedup logic on persister side using source sequence numbers
- Document exactly-once reasoning in design doc in repo
- Pick language for Phase 3 (Rust or Go)
- Evening tutorial time if new to the language
- **Hours:** 15-20 (rest block)

### Block 7: Jan 5 - Jan 18 — Phase 2 hardening + Post 2 drafting
- Deliberately break pipeline, fix it, document failure modes
- Survive arbitrary kills with documented recovery semantics
- Begin drafting **Post 2:** "Exactly-once is a lie, and here's what it actually means for market data"
- **Start Leetcode: 1 easy problem per weekday**
- **Hours:** 25-30

### Block 8: Jan 19 - Feb 1 — Windowed aggregation + Post 2 ships
- Add OHLCV bars consumer with 1-second tumbling windows (implement yourself, not Flink)
- Handle late-arriving ticks
- **Publish Post 2.** Submit to HN Tuesday morning if good enough
- Tag `v0.2-phase2`
- **Hours:** 25-30

### Block 9: Feb 2 - Feb 15 — Phase 3 start (storage design)
- Read DDIA chapters 3 and 4
- Read Parquet format spec
- **Design your own on-disk format on paper first.** Write design doc in repo before any code.
- Decide: columnar layout, encoding per column type, index strategy, file size strategy
- Start implementing writer
- **This is the depth signal — ZERO AI design generation**
- **Hours:** 30+ (heaviest block)

### Block 10: Feb 16 - Mar 1 — Storage writer and reader
- Writer and reader both working for a single file
- Benchmark reader throughput vs Parquet
- Draft **Post 3:** storage format teardown
- **Hours:** 30+

### Block 11: Mar 2 - Mar 15 — Compression, encoding + Post 3 ships
- Dictionary encoding for symbols, delta encoding for timestamps, zstd compression
- Measure compression ratios vs Parquet+Snappy
- **Publish Post 3:** "I built a columnar storage format from scratch. Here's what Parquet gets right."
- Submit to HN
- Tag `v0.3-storage`
- **Hours:** 25-30

### Block 12: Mar 16 - Mar 29 — Resume rewrite + Goldman outreach
- **Pause major project work** for one block
- Full resume rewrite: lead with platform story, drop Aetna internship, add Projects section
- Update LinkedIn
- Draft 90-second verbal pitch for the project, practice until natural
- Reach out to Surbhi about Goldman DE
- **Leetcode ramps: 1 medium per day minimum**
- **Hours:** 25-30

### Block 13: Mar 30 - Apr 12 — Phase 4 start (query service)
- Wrap storage layer in gRPC or HTTP service
- Query interface for time-range scans across symbols
- Implement predicate pushdown yourself
- Set up ClickHouse as second backend, load same data
- **Hours:** 25-30
- Start applying to Tier 3 roles

### Block 14: Apr 13 - Apr 26 — Benchmark harness
- Reproducible benchmark harness
- Workload types: point lookups, range scans, aggregations
- Measure p50, p95, p99 latency and throughput
- Set up QuestDB as third backend
- Document methodology carefully
- **Hours:** 25-30

### Block 15: Apr 27 - May 10 — Benchmark results + Post 4 (flagship)
- Finalize benchmarks
- **Publish Post 4:** "My tick storage vs ClickHouse vs QuestDB: an honest benchmark" (2500 words, charts, reproducible methodology)
- Submit to HN (Tuesday/Wednesday morning Eastern)
- Tag `v0.4-benchmarks`
- **Even if you stop here, you have a credible portfolio**
- **Hours:** 30+

### Block 16: May 11 - May 24 — Phase 5 decision + start
- Decide: Option A (OCaml rewrite, Jane Street-specific), Option B (streaming queries), Option C (replay mode)
- **Default recommendation: Option B or C.** Skip A unless on track and Jane Street is specifically still a target.
- Honest check: if project is enough and interview prep needs hours, skip Phase 5
- **Hours:** 25-30

### Block 17: May 25 - Jun 7 — Phase 5 build
- Core implementation of chosen option
- Watch for burnout — month 8
- **Hours:** 25-30

### Block 18: Jun 8 - Jun 21 — Phase 5 completion + Post 5
- Finish Phase 5
- **Publish Post 5:** phase 5 writeup
- Submit to HN
- Tag `v0.5-final`
- Project functionally complete
- **Hours:** 25-30

### Block 19: Jun 22 - Jul 5 — Documentation + interview prep ramps
- Rewrite main README as project front door: architecture diagram, how to run, link to each writeup
- Make sure every blog post is cross-linked
- **Leetcode: 2 mediums per day**
- System design: one problem per weekend (DDIA exercises or Alex Xu book)
- **Hours:** 25-30 (shift toward interview prep)

### Block 20: Jul 6 - Jul 19 — Application push, Tier 2
- 15-20 Tier 2 applications: Two Sigma, Citadel Securities, Optiver, IMC, HRT, Jump, DRW
- 10-15 big tech data infra: Meta, Google, Databricks, Snowflake, Stripe, Anthropic
- Use referrals wherever possible
- Continue 2 Leetcode mediums per day
- **Hours:** 25-30

### Block 21: Jul 20 - Aug 2 — Interview season peak
- Mock interviews (interviewing.io)
- System design focus
- For Jane Street loops: OCaml refresh (if Option A)
- For big tech: standard behavioral prep
- For quant-adjacent: project deep-dive prep
- **Hours:** 30+ (crunch)

### Block 22: Aug 3 - Aug 16 — Continued interviews
- First offers if on track
- Jane Street DE application window if that's still a target
- **Hours:** 25-30

### Block 23: Aug 17 - Aug 30 — Negotiation + decisions
- Negotiation prep: know numbers, know levers
- Compare offers transparently
- Don't accept first offer
- **Hours:** 15-20

### Block 24: Aug 31 - Sep 13 — Final decisions
- Decide and accept
- If no acceptable offers, decide: extend search or stay at CVS with stronger profile for 2028
- **Hours:** 10-15 (reset block)

### Block 25: Sep 14 - Sep 30 — Transition prep
- Notice if accepted
- Prepare for new role
- Reassess if no offer

---

## Blog Timeline Summary

### Early CVS-derived posts (parallel with project ramp)

**Post A: Nov-Dec 2026** — "Migrating 47 production Airflow DAGs to Composer 3: what nobody tells you"
- Universal audience, easy to sanitize
- Should be among your first serious writing attempts
- Builds writing habit before project writing starts
- Potentially HN-worthy

**Post B: Jan-Feb 2027** — "Building a self-service data platform in a HIPAA environment: the design constraints nobody talks about"
- Underserved topic, smaller but higher-value audience
- Harder to sanitize — write at pattern level, not implementation level

**Post C (optional): Q2 2027** — Incident response post on HEDIS pipeline incidents (sanitized, pattern-level)

**Post D (optional): Q3 2027** — ML deployment post from NOVA pipeline work

**Sanitization rules for all CVS-derived posts:**
- No internal system names
- No business metrics tied to CVS
- No specifics about Medicare member counts
- No architecture details revealing CVS's internal data model
- Pattern-level only, not implementation-level
- When in doubt, check CVS policy on external writing or ask manager
- Safest framing: "In my work on a healthcare data platform" rather than "At CVS Health"

### Project-derived posts

**Post 1: Block 3 (Nov 10-23, 2026)** — "What I learned ingesting a day of real market data" (1200 words)

**Post 2: Block 8 (Jan 19 - Feb 1, 2027)** — "Exactly-once is a lie, and here's what it actually means for market data" (1500-2000 words)

**Post 3: Block 11 (Mar 2-15, 2027)** — "I built a columnar storage format from scratch. Here's what Parquet gets right." (2000 words)

**Post 4: Block 15 (Apr 27 - May 10, 2027)** — "My tick storage vs ClickHouse vs QuestDB: an honest benchmark." (2500 words, charts) — **FLAGSHIP POST**

**Post 5: Block 18 (Jun 8-21, 2027)** — Phase 5 writeup

**Post 6: Block 25 (Sep 14-30, 2027)** — "What I learned building a market data platform in 2026-2027" — capstone, becomes first thing recruiters find

### Writing standards (non-negotiable)

- Published on your own domain, not Medium
- Length appropriate to depth; don't pad
- Take positions, don't hedge
- Specific, confident prose (model: Dan Luu, Marc Brooker, Jamie Brandon)
- Link to code and specific commits
- Invite correction in closing
- Submit flagship posts to HN Tuesday-Wednesday morning Eastern
- 100% your own writing — no AI generation of opinions or takes
- Edit for AI polish only (clarity, grammar, cuts)

---

## Visibility Infrastructure

### GitHub
- Public repo from Day 1
- README is the front door — most people reading it won't read code
- Tag each phase as a release
- Profile README highlighting the project, writing, current work

### Personal domain
- yourname.com or similar
- Hugo or Astro, don't over-engineer
- All blog posts live here
- Cross-link to GitHub

### LinkedIn
- Headline: specialization-focused ("Data Engineer building streaming systems")
- Featured section: pinned to best blog posts and GitHub project
- 1 longer-form post per month cross-posting a technical idea
- Not daily "I'm so proud" posts

### Twitter/X (starting Block 10, Feb 2027)
- Follow 50-100 engineers in the field
- Post thoughtfully 1-2x/week about what you're building
- Share blog posts when they publish
- 2 hours/week commitment

### Open source contribution thread (starting Block 7, Jan 2027)
- Pick DuckDB (great docs, welcoming community, aligned with your work)
- Read codebase, file one well-researched issue
- Take on small feature
- Target 3-5 merged PRs by September 2027
- 2 hours/week

---

## Reachable Target Set (September 2027)

### Primary targets (top of the field)
- Two Sigma platform
- Citadel Securities infra
- Databricks
- Snowflake
- Stripe data platform
- Anthropic / OpenAI data infra
- Meta data infra
- Google core data
- Confluent, Materialize, ClickHouse (the company)

### Strong fallbacks
- Goldman DE/Strats (Q1-Q2 2027 via Surbhi — do this independently of project timeline)
- Morgan Stanley DE, JPM Athena/Quartz
- Point72 engineering, Millennium engineering
- Bloomberg engineering, Bridgewater DE

### Floor
- Lateral within CVS to more interesting team
- Stay another year, apply from stronger position in 2028

### Stretch (do not anchor the plan on these)
- Jane Street DE (10-20% even with full execution; requires OCaml path and interview luck)
- HRT platform, Jump platform

---

## Honest Probability Bands

With full execution of the plan by September 2027:

- **At least one Tier 3 offer (Goldman DE etc.):** 70-85%
- **At least one top-tier offer (primary targets above):** 50-65%
- **Multiple offers to choose between:** 40-55%
- **Jane Street DE specifically:** 10-20%
- **Visibility: clearly ahead of middle layer, identifiable in field:** 60-75%

These are calibrated guesses, not forecasts. Market conditions, interview performance, and timing all affect the actual outcome. Treat as "possible / improbable / probable" buckets, not precise percentages.

---

## Failure Modes to Watch For

1. **Project becomes an excuse to not interview.** If month 6 arrives and project is 60% done but Goldman is interested, interview with Goldman. Don't delay career moves for the artifact.

2. **Writing track skipped.** The most likely failure. Code without writing is low-visibility. If posts slip, the whole visibility strategy collapses. Treat posts as part of each phase's deliverable.

3. **Over-leverage on AI.** You ship fast but can't defend the work in interviews. Catch this with the 2-4 week private note test.

4. **Interview prep underweighted.** Starting Leetcode in April for July interviews is too late. Start in November (easy mediums), ramp in January.

5. **Over-planning instead of executing.** Any further "let me think about the right project" is procrastination. First commit matters more than final one.

6. **Trying to be a hero in Q4 2026.** Work is crunchy now. The plan accounts for this. Don't force a 30-hour week in October-November and burn out before January.

7. **Burnout at month 8 (May-June 2027).** Phase 5 blocks are high-risk for this. Honest check-in with yourself. Cut scope if needed.

8. **Phase 5A (OCaml) attempted on top of everything else.** Likely compromises interview prep. Default to skipping unless on track and Jane Street specifically remains a target.

---

## The One Rule That Matters Most

For anything that could become an interview question, you design it yourself. For anything that couldn't, use whatever tools make you faster.

If you forget everything else in this document, remember that.

---

## Today's Starting Action

Create the GitHub repo. Push the first commit — even if it's just a README with the project name and intent. Starting is 80% of finishing.
