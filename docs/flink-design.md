# Flink Design — Job 1: 1-minute trade counts per symbol

**Module:** `flink/jobs/trade_count_1m.py` (new, separate `flink/` project)
**Status:** Design (Nov 17, 2026)
**Author:** Divyam Kaushik
**Related:** `docs/producer-design.md` (what writes `trades.raw`), `docs/deploy-design.md` (GKE; Flink on GKE is a later doc)

---

## 1. Purpose

*(This job will use Kafka consumers to read from the trades.raw topic and process counts symbol wise, and post to a separate new topic as metrics.)*

---

## 2. Scope

### In scope (Nov 17–20)
- Read `trades.raw` from Confluent with the Flink Kafka source
- Event time taken from the trade's own `timestamp` (exchange time), with watermarks
- Count trades per symbol in 1-minute tumbling event-time windows
- Write each window's count to a new topic `metrics.trade_count_1m`
- Route late trades to a side output and count them
- Run locally (embedded mini-cluster on the Mac) against both live data and a replay of a past day
- Check correctness by replaying a recorded day and comparing with a plain Python count of the same data

### Out of scope (picked up later)
- Running on GKE: Flink Kubernetes operator, GCS checkpoints, Workload Identity, bigger node (Thanksgiving week)
- Exactly-once output via a transactional sink (§4.8)
- Per-window data-quality counts: invalid, late and extended-hours `T` (Fri Nov 20, extends this job)
- VWAP, feed-gap detection, anomaly detectors, BigQuery sink (December)
- Schema Registry / schema validation (December)

---

## 3. Failure modes considered

| Mode | Example | How we respond |
|---|---|---|
| Idle partition stalls the watermark | Partition 2 of `trades.raw` gets no symbols (CRC32 partitioner), so its watermark never moves and no window ever fires | `with_idleness(30s)` on the watermark strategy: a partition with no data for 30s is left out of the minimum (§4.4) |
| Market close: watermark stops | No trades overnight, so the watermark doesn't pass 16:00 and the last minute's window stays open | Accepted. After-hours `T` trades move it forward until they stop too; the last windows fire when trading resumes, or on replay. Watched in the live run Nov 19 |
| Late trade | A trade arrives after the watermark has passed its minute | Sent to a side output and counted, not silently dropped (§4.6) |
| Malformed message | Invalid JSON, missing `symbol` or `timestamp` | Parse step drops it and counts it (log now, a metric on Fri). The job must not crash on one bad record |
| Job crash / restart (local) | Exception, laptop sleep, Ctrl-C | Restart from the last checkpoint: window state and Kafka offsets restored together, records after it replayed (§4.7). Windows emitted after that checkpoint can be written again: duplicates with identical values (§4.8) |
| Duplicates in the input | Backfill overlapping a live day (deploy-design §3) | Not handled in job 1: counts would include duplicates. Noted for the data-quality work (dedup on `trade_id`) |
| Confluent unreachable / auth fails | Network down, wrong key | Kafka client retries; the job fails if it can't recover, and the restart strategy retries a few times before giving up |
| Replay data expired | `trades.raw` retention is 1 week: Nov 13 data deleted ~Nov 20 | Replay validation must run by Thu Nov 19; stream a fresh day each week |

---

## 4. Design decisions

Each decision has a **What** (committed) and a **Why** (to be written in own voice).

### 4.1 Versions and project layout

**What:** Flink **2.2.1** with PyFlink (`apache-flink==2.2.1`), **Python 3.12**, Kafka connector `flink-sql-connector-kafka` **5.0.0-2.2** (the self-contained JAR), Java 17. Checked Nov 17: every PyFlink 2.x release ships wheels for Python 3.9–3.12 only, so it can't install into the ingest project's 3.13 venv. Flink 2.3 exists, but the newest Kafka connector supports 2.1 and 2.2 only. The job lives in its own `flink/` directory with its own `pyproject.toml` and `uv.lock` pinned to 3.12. The ingest project stays on 3.13 and its image doesn't gain PyFlink's dependencies (pandas, apache-beam, pyarrow, all tightly pinned).

```
flink/
├── pyproject.toml        # requires-python ==3.12.*, apache-flink==2.2.1
├── uv.lock
├── jars/                 # connector JAR (downloaded, not committed)
├── jobs/trade_count_1m.py
└── tests/
```

**Why:** *(Flink gets its separate side project with its own pyproject and uv.lock files so it is not clubbed with the same docker image as the ingestion app, and these can exist as separate pods on GKE. This way we dont need to downgrade our ingestion app to 3.12 and we dont face compatibility issues either. It also keeps the Docker container image for ingestion app unaffected in terms of size, and thus the failures there will only cause that POD to restart and not the Flink one and vice-versa)*

### 4.2 API: DataStream, not SQL

**What:** The DataStream API in Python. The watermark strategy, `key_by`, window, aggregation and late-data side output are all written explicitly. Flink SQL could express the count in a few lines, but it drops late rows silently, with no side output.

```
KafkaSource(trades.raw)
  → parse JSON → Trade(symbol, ts_ms, conditions)          # drop + count malformed
  → watermarks: bounded out-of-orderness 2s, idleness 30s
  → key_by(symbol)
  → TumblingEventTimeWindows(1 min)
  → side_output_late_data(LATE)
  → aggregate(count, attach window bounds)
  → KafkaSink(metrics.trade_count_1m)
```

**Why:** *(Flink SQL is easier to take care of and implement and avoids the Java to Python serialization overheads, however Datastream API in Python gives more control over the late data handling. Datastream allows us to explicitly define each and every part of our pipeline unlike Flink SQL where it's all internal JAVA based)*

### 4.3 Event time

**What:** Event time is the trade's `timestamp` field (Alpaca `t`, exchange time, RFC 3339 UTC with nanoseconds, e.g. `2026-11-13T14:30:00.123456789Z`), converted to epoch milliseconds. Not the Kafka record timestamp: that is the producer's CreateTime (send time), which is 21 ms later at p50 and up to 127 ms later in the Nov 13 measurement, and it would be wrong for replayed or backfilled data. Not processing time: a replay would put a whole day's trades into the few minutes the replay takes.

**Why:** *(We should use the trade's timestamp as the event time so each trade lands in the minute it actually happened, no matter when Flink processes it, and a replay gives the same counts as the live run. In replays, processing time is much faster as compared to the event time and all trades will be gone into 2-3 small buckets if the read in Flink takes 2-3 minutes)*

### 4.4 Watermarks

**What:** `WatermarkStrategy.for_bounded_out_of_orderness(2s).with_idleness(30s)` with a timestamp assigner reading the parsed `ts_ms`.
- **Out-of-orderness 2s** is a starting value. The hello-world on Wed Nov 18 measures how out of order `trades.raw` really is (per partition: how often and by how much a trade's time is behind the newest one seen), and the bound is set from that.
- **Idleness 30s** is needed because partition 2 is always empty (§3). In normal hours every active partition gets several trades a second, so 30s of silence means the partition is genuinely idle.
- **To verify on Wed:** whether PyFlink generates watermarks per Kafka partition inside the source (as Java does) when the timestamp assigner is a Python function, or only after the source. This changes how out-of-order the data looks to the watermark.

**Why:** *(This is to make sure one idle partition is not causing the watermark to pause. If the watermark pauses, no window ever fires and the job produces no output at all. Open windows would also keep piling up in state, though with incremental counts that is small)*

### 4.5 Window and aggregation

**What:** 1-minute tumbling event-time windows, aligned to the minute (UTC), keyed by `symbol`. Counting is incremental: an `AggregateFunction` keeps one number per symbol per open window, and a small window function attaches `window_start`/`window_end` when it fires. A minute with no trades for a symbol emits nothing: no row, not a zero. Job 1 counts every trade, including extended-hours `T` trades. Splitting by condition is Friday's work.

**Why:** *(Incremental counting keeps the load very low on memory)*

### 4.6 Late data

**What:** `allowed_lateness = 0`, with late trades sent to a side output. Job 1 logs the late count per window; Friday turns it into a data-quality metric. No window is re-emitted for late data.

**Why:** *(Late data sent to a side output to check for data quality and make further design decisions on how to handle late data)*

### 4.7 Checkpoints and starting position

**What:** Checkpointing enabled every 30s, stored on the local filesystem for now (GCS on GKE later). A checkpoint holds the open windows' counts and the Kafka offsets from the same moment. Consumer group `flink-trade-count-1m`. Starting position is a command-line choice:
- `--from latest` for live runs
- `--from <ISO time>` for replay: start at the first offset at or after that time

Offsets are tracked in checkpoints; committing them back to Kafka is only so the Confluent UI can show consumer lag.

**Why:** *(Checkpoints are enabled and stored so we can restart our Flink process from the last completed checkpoint, up to 30s before the failure; trades after it are processed again, which is why §4.8 can produce duplicate windows. Storing checkpoints in GCS (not on the GKE node's disk, which is lost with a preempted node) will prevent state loss even if servers fail)*

### 4.8 Output topic and delivery guarantee

**What:** New topic `metrics.trade_count_1m`, 1 partition (about 3,900 records per regular-hours day: 10 symbols × 390 minutes), 1 week retention, created once in Confluent. Record key = symbol; value is JSON:

```json
{"symbol": "AAPL", "window_start": "2026-11-13T14:30:00Z", "window_end": "2026-11-13T14:31:00Z", "trade_count": 412}
```

Kafka sink with **at-least-once** delivery. After a crash, windows emitted after the last checkpoint can be written a second time, with identical values, and `(symbol, window_start)` identifies a window uniquely, so a downstream sink can upsert or dedup on it. An exactly-once (transactional) sink is deferred to the GKE work, together with GCS checkpoints. It would make output visible only when a checkpoint completes, delaying each count by up to the checkpoint interval.

**Why:** *(When replayed, the repeated output windows have the same (symbol, window_start) and identical counts, so these can be deduped in the BQ sink, and at least once deliver + sink dedup acts like exactly once with much lesser configuration right now)*

### 4.9 Credentials and parallelism

**What:** Confluent bootstrap server and API key/secret are read from environment variables (the gitignored `.env`) and passed to the Kafka source and sink as SASL_SSL / PLAIN properties. They are never written into code or committed config. Parallelism 1 locally: about 15 trades a second needs no more. Parallelism and the extra source subtask per partition are revisited on GKE.

**Why:** *(Secrets are never stored in the codebase so they dont accidentally get committed. For safety purposes they are injected at run time, just like they would be in GKE)*

---

## 5. How we know it's correct

1. **Replay vs batch count:** replay one recorded day (Nov 13, before it expires ~Nov 20) through the job. Separately, count the same trades per symbol per minute with a plain Python script that reads the topic directly. The two must match for every window, apart from trades the job reports as late or malformed, which must account for the whole difference.
2. **Unit tests:** parsing (valid, malformed, nanosecond timestamps) and the count/window function, without Kafka.
3. **Live run (Thu Nov 19):** counts appear about a minute behind real time (window length + 2s bound). No windows go missing for partition 2's sake (idleness works). Watch what happens to the watermark after 16:00.

---

## 6. Future work (explicitly deferred)

- Exactly-once sink (Kafka transactions + `read_committed` consumers)
- Flink on GKE: Kubernetes operator, GCS checkpoints, Workload Identity, node upgrade
- Data-quality counts per window: malformed, late, extended-hours `T`, duplicate `trade_id`
- More jobs: VWAP, feed-gap detection
- Schema Registry for `trades.raw` and the metrics topics

---

## 7. Open questions I'd flag in a real design review

- **Separate Confluent service account for Flink?** The job could reuse the producer's key or get its own least-privilege account (read `trades.raw`, write `metrics.*`, its consumer group). Decide before running on GKE.
- **Partition skew:** `trades.raw` partition 2 is empty and hot keys shift daily. Idleness handles the watermark; whether to fix the producer's partitioning is still open.
- **Out-of-orderness bound:** set from the Nov 18 measurement, not a guess.

---

## 8. Files

- `flink/pyproject.toml`, `flink/uv.lock`: new project, Python 3.12
- `flink/jobs/trade_count_1m.py`: the job
- `flink/tests/`: parse and aggregation tests
- `flink/README.md`: setup (Java 17, connector JAR download, run commands)
- `.gitignore`: `flink/jars/`, local checkpoint directory
- No changes to `src/tickstore/` or the ingest image
