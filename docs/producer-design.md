# Producer Design — Kafka Producer for Polygon Trades

**Module:** `src/tickstore/ingest/polygon_feed.py` (producer integrated into existing ingestion module)
**Status:** Day 4 design (Nov 8, 2026)
**Author:** Divyam Kaushik
**Related:** `docs/ingestion-design.md` (Day 3 reconnect logic this builds on top of)

---

## 1. Purpose

*(The Kafka producer takes up the data packets/stock messages from Polygon and helps the application process those in an exactly once semantic (within a producer session) to Confluent cloud topic trades.raw. This guarantees exactly once from the Kafka producer to the confluent cloud, but does not guarantee outside of it, i.e., the data packed ingestion from websocket, as that is outside of Kafka)*

---

## 2. Scope

### In scope (Day 4)
- Produce every received Polygon trade message to Confluent topic `trades.raw`
- Idempotent producer config (dedup on the Kafka side)
- Partition key = symbol (preserves per-symbol ordering across consumers)
- Error handling that doesn't kill the WebSocket reader on producer failures
- Clean shutdown: flush pending messages before process exits

### Out of scope (today — picked up later)
- Schema validation / schema registry (Week 2+)
- Quotes `Q` production to `quotes.raw` (Day 7)
- Metrics on produce rate, latency, failure count (Day 6)
- Multi-topic routing (far future)
- Kafka transactions / exactly-once end-to-end (deferred; idempotent producer is sufficient for Day 4)

---

## 3. Failure modes considered

| Mode | Example | How we respond |
|---|---|---|
| Producer network error | Broker unreachable | Library's internal retry; keep WebSocket reader alive |
| Idempotent duplicate rejection | Broker sees stale sequence number | Library handles silently; our code logs if surfaced |
| Full buffer / backpressure | Kafka slower than WebSocket arrival rate | Bounded asyncio.Queue(maxsize=10000) blocks the reader on full; backpressure propagates to Polygon via the WebSocket layer (§4.6) |
| Producer permanently broken | Bad auth, topic deleted, wrong cluster | Log ERROR with context; keep WebSocket running (partial degradation beats total outage) |
| Shutdown mid-produce | Ctrl+C with messages in flight | Flush with timeout before exiting |
| Message serialization fail | Unexpected message type from Polygon | Log + skip; don't crash the pipeline |

---

## 4. Design decisions

Each decision has a **What** (committed) and a **Why** (to be written in own voice).

### 4.1 Kafka client library

**What:** `confluent-kafka-python` (librdkafka wrapper)

**Why:** *(Confluent-kafka is a C dominant thread wrapper in python, which helps in processing the message through Kafka. Both support exactly once semantics with atomic transactions completely. Confluence-kafka with C is inherently faster and optimized for higher throughput in comparison to python based aiokafka. In terms of support, confluent-kafka has complete documentation and support across.)*

---

### 4.2 Idempotent producer config

**What:** `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5` (default), retries left at library default.

**Why:** *(idempotence=true guarantees exactly once semantic within a single session. If the Kafka producer breaks down, then the idempotence is only guaranteed since for the new session. Acks=all takes care of complete data backup to the follower brokers for each partition and prevents possible data loss if the lead broker confirms message receipt but the copy to the follower broker failed following the lead broker dying out. max inflight requests = 5 is the upper limit at which Kafka can maintain ordering in memory for messages in the event of a failure or lost connection. This guarantees right ordering only up to 5 requests, and creates a queue/buffer if the receipts are not received from Confluent cloud)*

---

### 4.3 Batching strategy

**What:** Batched. Configure `linger.ms=5` (producer waits up to 5ms to accumulate a batch) and leave `batch.size` at a reasonable value (e.g., 16KB default) as a size-based ceiling that overrides linger under high load.

**Why:** *(For our use case of anomaly detection, batch approach is better as the main demand would anyway need for the Kafka consumers to look at batches of data to reveal anomalies along with rolling metrics, also broker encryption decryption overhead. The linger.ms of 5 ms should be okay to allow the producer to batch the partitioned data and send it over. Per message separate transfer takes a much higher toll on the system as that many more network connections need to be made and can cause backpressure. So along with linger.ms, we will also employ a batch.size so it can take care in case of high traffic and overrule the linger.ms waiting and overloading the data packet)*

---

### 4.4 Partition key

**What:** Symbol as the partition key. Produced as bytes (`symbol.encode("utf-8")`).

**Why:** *(This guarantees that ordering across a single symbol like Apple is coherent, as order matters in for a single symbol to correlate it to the trades and stock price. This costs as well as if in a given minute window, Apple trades much more in volume than any other, then all of the load is going to hit the apple partition as well as its associated Kafka consumer. This bottleneck here can cause confluent to slow down and create backpressure eventually on the websocket to Polygon, which can result in data loss if buffer fills up and we lose connection on server side)*

---

### 4.5 Handler flow structure

**What:** Split. WebSocket reader (`handle_messages`) pushes received trades onto an `asyncio.Queue`; a separate producer coroutine consumes from the queue and calls into confluent-kafka-python.

**Why:** *(We should split using a queue to separate the polygon process from the confluent flow, so that confluent network loss does not lead to network connection loss to Polygon through backpressure and queue memory overflow, as that results in data loss. The Queue is also a good shock absorber and cushion for increased traffic)*

---

### 4.6 Backpressure handling

**What:** Bounded `asyncio.Queue(maxsize=N)` with blocking semantics on full. **N to be decided — fill in your choice (recommendation: 10000 for ~100s of absorption at peak rates)**. When the queue is full, `await queue.put(msg)` blocks the WebSocket reader coroutine, naturally propagating backpressure up to Polygon via the WebSocket layer.

**Why:** *(We should pick option a with asyncio queue of depth 10000 and block websocket reader when full on top of the confluent-kafka internal queue. In case of confluent network slow down, the queue will build up on top of confluent-kafka internal queue until these both are out of memory. In which case, we will rely on Polygon's server side buffer queue on sending data when websocket is paused to let Kafka clear up the queue. This introduces some latency due to the websocket pause, and also risks losing connection to Polygon in comparison to a drop strategy, however drop strategy causes data loss, and for our use case of anomaly detection, every data point is important, and takes precedence over a 1 secod data delay. 10000 comes from 100 msgs/second for 10 symbols max, so give a buffer for 10 seconds)*

---

### 4.7 Shutdown semantics

**What:** On Ctrl+C or auth-failure exit, call `producer.flush(timeout=10)` before process terminates to ensure in-flight messages are acked.

**Why:** *(The flush protects the in flight requests to be completed to confluent, because if we terminate, those messages are lost as they left the websocket (Not in the buffer queue) and then also got lost if we kill Kafka producer immediately and then no way of knowing whether the messages got written to Confluent or not.)*

---

## 5. What a successful produce looks like in logs

Startup:

```
2026-11-08 09:30:00 INFO  tickstore.polygon_feed | producer initialized; bootstrap=pkc-xxx.us-east1.gcp.confluent.cloud:9092 topic=trades.raw
2026-11-08 09:30:00 INFO  tickstore.polygon_feed | queue initialized; maxsize=10000
2026-11-08 09:30:01 INFO  tickstore.polygon_feed | connecting to polygon; subscribed to 10 trade channels
```

Steady-state produces (DEBUG-level per-message, INFO summary every N seconds):

```
2026-11-08 09:30:05 DEBUG tickstore.polygon_feed | queued: symbol=AAPL price=189.42 size=100 ts=1762598205123
2026-11-08 09:30:05 DEBUG tickstore.polygon_feed | produced: symbol=AAPL partition=1 offset=147 batch_size=7
2026-11-08 09:30:10 INFO  tickstore.polygon_feed | last 5s: received=312 produced=312 queue_depth=4 errors=0
```

Producer error (library retries internally; we log on surfaced exception):

```
2026-11-08 09:45:12 WARNING tickstore.polygon_feed | produce error: KafkaError{code=MSG_SIZE_TOO_LARGE} symbol=AAPL; dropping message
2026-11-08 09:45:13 INFO  tickstore.polygon_feed | last 5s: received=298 produced=297 queue_depth=5 errors=1
```

Queue backpressure (reader blocks on `queue.put`; depth stays at max):

```
2026-11-08 10:02:33 WARNING tickstore.polygon_feed | queue full (10000); reader will block until drained
2026-11-08 10:02:35 INFO  tickstore.polygon_feed | queue drained to 8431; reader resumed
```

Graceful shutdown (Ctrl+C):

```
2026-11-08 16:00:00 INFO  tickstore.polygon_feed | shutdown signal received
2026-11-08 16:00:00 INFO  tickstore.polygon_feed | stopping WebSocket reader
2026-11-08 16:00:00 INFO  tickstore.polygon_feed | draining queue (depth=42) and flushing producer (timeout=10s)
2026-11-08 16:00:01 INFO  tickstore.polygon_feed | flushed 42 pending messages; exiting cleanly
```

---

## 6. Future work (explicitly deferred)

- Schema registry + Avro/Protobuf serialization (Week 2+)
- Producer metrics: `trades_produced_total`, `produce_latency_ms`, `produce_errors_total` (Day 6)
- Multi-topic routing (e.g., high-frequency symbols on a dedicated topic)
- Transactional producer for exactly-once across trades + quotes (Week 3+ if needed)
- Dead-letter topic for unserializable messages (Day 7 along with quotes)

---

## 7. Open questions I'd flag in a real design review

*(1. I optimized for correctness over latency, what if the use case cannot afford even a second of delay? 2. For the partitioning, there is no mechanism to handle high traffic single symbol case. Would I increase the partition limit for skewed cases? or is there some other alternative. )*

---

## 8. Changes to the existing module

Day 4 modifies `src/tickstore/ingest/polygon_feed.py`:

### New module-level constants
- `KAFKA_TOPIC = "trades.raw"`
- `QUEUE_MAX_SIZE = 10000`
- `PRODUCER_FLUSH_TIMEOUT_SEC = 10`

### New functions
- `make_producer() -> Producer` — reads CONFLUENT_* from env, builds a `confluent_kafka.Producer` with idempotent config (`enable.idempotence=true`, `acks=all`, `linger.ms=5`, SASL_SSL auth to Confluent Cloud).
- `serialize_trade(msg) -> bytes` — JSON-encodes the Polygon trade message for Kafka. (Will become Avro in Week 2+.)
- `delivery_callback(err, msg)` — called by librdkafka when a produce is acked or fails. Logs errors; logs success at DEBUG.
- `producer_task(queue, producer)` — consumes from the shared asyncio.Queue, calls `producer.produce(...)`, polls for delivery callbacks. Runs until cancelled.

### Modified functions
- `handle_messages(messages, queue)` — new `queue` parameter. Now `await queue.put(msg)` for each message instead of logging it (logging demoted to DEBUG).
- `run_with_reconnect(queue)` — takes the queue, passes a `handle_messages` bound to it into `client.connect(...)`.
- `main()` — now orchestrates:
  1. Create `asyncio.Queue(maxsize=QUEUE_MAX_SIZE)`
  2. Create Kafka producer
  3. Start `run_with_reconnect(queue)` and `producer_task(queue, producer)` via `asyncio.gather`
  4. On KeyboardInterrupt / exit: cancel reader, await queue drain, call `producer.flush(timeout=10)`, exit

### New imports
- `confluent_kafka.Producer`
- `json` (for trade serialization)

### Unchanged
- Reconnect/backoff logic from Day 3
- Symbol list
- Backoff/auth retry constants
