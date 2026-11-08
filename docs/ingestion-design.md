# Ingestion Design — Polygon WebSocket Client

**Module:** `src/tickstore/ingest/polygon_feed.py`
**Status:** Day 3 design (Nov 7, 2026)
**Author:** Divyam Kaushik

---

## 1. Purpose

polygon_feed.py receives real-time trade messages for 10 US equity symbols from Polygon's WebSocket, logs each message to stdout, and automatically recovers from network disruptions through exponential backoff and auth key loud failures.

---

## 2. Scope

### In scope (Day 3)
- Receive trades for 10 US equity symbols from Polygon WebSocket
- Log to stdout
- Survive arbitrary network drops without manual intervention
- Fail loudly (not silently) on misconfigured credentials
- Clean shutdown on SIGINT (Ctrl+C)

### Out of scope (today — picked up in later days)
- Kafka producer (Day 4, Sun Nov 8)
- Metrics / Prometheus counters on reconnects (Day 6, Tue Nov 10)
- Quotes `Q` subscription (Day 7, Wed Nov 11)
- Backpressure handling (Week 2+)
- Multi-feed orchestration (later)

---

## 3. Failure modes considered

| Mode | Example | How we respond |
|---|---|---|
| Network drop | TCP reset, broken pipe, OS-level connection reset | Exponential backoff + resubscribe |
| Transient auth failure | Momentary Polygon auth server hiccup | 1 retry with 30s wait, then fail |
| Permanent auth failure | Wrong API key, expired plan, revoked key | After 1 retry fails, log ERROR + exit process |
| Missed heartbeat | Pong not returned within library timeout | Treat as network drop; library handles |
| Graceful shutdown | User Ctrl+C | Clean exit, no retry loop swallowing the signal |
| Slow consumer | Our handler can't keep up with message rate | **Not handled today** — deferred to Week 2 |

---

## 4. Design decisions

Each decision has a **What** (committed) and a **Why** (to be written in own voice).

### 4.1 Backoff strategy

**What:** Exponential backoff starting at 1s, doubling each attempt, capped at 60s.
Pattern: `1s → 2s → 4s → 8s → 16s → 32s → 60s → 60s → 60s → ...`

**Why:** *(Exponential backoff is needed in order to maintain balance between checking the network connection failure or any transient failure multiple times immediately, as well as give enough time for the transient failure to resolve and get the connection back. The time to retry keeps on increasing until it is 60 seconds and after that. it remains at 60 seconds because anything beyond that is time/data lost possibly, so 60 seconds is the sweet spot to keep on checking in case the outage is long)*

---

### 4.2 Retry termination

**What:** Infinite retries for network/connection errors. Finite (1) retry for auth errors.

**Why:** *(For Network connections, infinite retries are okay because the network connection will come back at some time. These are mostly transient errors. The downside usually is silent failures in case the network never comes back but we keep track of that through metrics. However for Auth errors, it mostly has to do with keys expiring or a very less plausible situation where some data loss or service downtime caused the auth to fail. In any case, this is not a transient failure and requires manual intervention in most cases. At the same time re-trying infinitely can cause the source system to lock out our application or block the IP)*

---

### 4.3 Auth failure action

**What:** After 1 retry (30s wait between attempts), log ERROR with context and exit the process.

**Why:** *(The 1 retry, 30s wait between attempts and then log ERROR with context and exit process is necessary as multiple auth retries through exponential backoff can cause our service IP to be locked out from the source system. Also with auth failures, the retries have diminishing returns because its probably an API key or auth key failure which will be fixed after intervention on fixing the key. The ERROR and exit is necessary for the Kubernetes POD to shut down and fail loudly as well, i.e., by creating a crash loop as the pods keep on restarting)*

---

### 4.4 Heartbeat / dead-connection detection

**What:** Use polygon-api-client's built-in heartbeat (~20-30s pong timeout by default). No custom override.

**Why:** *(The websockets heartbeat check is what we will trust as we are using that infra to log into Polygon under the hood)*

---

### 4.5 Resubscribe on reconnect

**What:** Symbol list is a module-level constant (`SYMBOLS`). On every new WebSocket connection, re-send SUBSCRIBE commands for all 10 channels.

**Why:** *(The new socket is like a new connection every time, so we need to re-send the subscribe commands for the 10 channels or we will see no data. It is a total reset. The source system would need to maintain a user-subscription dictionary in order to provide no re-subscribing based on the Auth key, which creates a 2 layered check on the server side, the Auth key user lookup and the user subscriptions lookup. These lookups create overhead for the server and can cause the performance to drop down (Polygon has multiple servers thus the subscription lookup would have to be a shared table), client side state ownership is the cleanest to maintain as the client knows the intent the best)*

---

### 4.6 Jitter

**What:** Yes — add ±0.5s random jitter to each backoff interval.

**Why:** *(jitter adds randomness to backoff to avoid thundering herd when many clients reconnect simultaneously. All applications are solo clients in their point of view, however this is more about the cases where the server goes through downtime and then all the clients will hit the server with the same exponential backoff logics. The Randomness introduced removes the thundering issue, and the application must always take care of it to be a good client to the network, as well as maximize possibility of a successful connection)*

---

## 5. What a disconnect looks like in logs

Example trace we expect to see on a transient network drop:

```
2026-11-07 14:03:01 INFO  tickstore.polygon_feed | connecting to polygon; subscribed to 10 trade channels
2026-11-07 14:03:02 INFO  tickstore.polygon_feed | <trade msg>
...
2026-11-07 14:15:41 WARNING tickstore.polygon_feed | connection lost: ConnectionResetError; reconnecting in 1.0s
2026-11-07 14:15:42 INFO  tickstore.polygon_feed | connecting to polygon; subscribed to 10 trade channels
2026-11-07 14:15:43 INFO  tickstore.polygon_feed | <trade msg>
```

Example on bad credentials:

```
2026-11-07 14:03:01 INFO  tickstore.polygon_feed | connecting to polygon; subscribed to 10 trade channels
2026-11-07 14:03:02 ERROR tickstore.polygon_feed | auth failed: <polygon error>; retrying once in 30s
2026-11-07 14:03:32 ERROR tickstore.polygon_feed | auth failed on retry: <polygon error>; exiting
<process exits with non-zero status>
```

---

## 6. Future work (explicitly deferred)

- Metrics: `reconnect_count`, `time_to_reconnect_seconds`, `disconnect_reason` labels (Day 6)
- Alert rule on `reconnect_count` rising (Day 6)
- Backpressure handling if handler can't keep up with arrival rate (Week 2)
- Health endpoint for GKE liveness probe (Day 5)
- Separate reconnect strategies per feed type (quotes, options, etc. — far future)

---

## 7. Open questions I'd flag in a real design review

*(1. The number of re-tries on auth, should it be 1? Is it needed at all because that 1 re-try is 30 seconds missed and we lose 30 seconds of data, which can be extremely costly in fields like quant and finance. 2. Should we trust the library's heartbeat? Or should we find a way to detect the heartbeat in the websocket connection?)*
