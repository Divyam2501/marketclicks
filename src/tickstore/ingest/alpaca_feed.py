"""Alpaca Markets WebSocket client with auto-reconnect + Kafka producer.

Streams real-time trades for 10 US equity symbols from Alpaca's IEX feed
to Confluent topic `trades.raw`, partitioned by symbol. Same architecture
as the previous polygon_feed.py: split WebSocket reader + Kafka producer
coroutines sharing a bounded asyncio.Queue; reader blocks on queue-full
for natural backpressure.

Day 5 (Nov 9) pivot from Polygon: discovered Polygon Starter plan does
not actually deliver WebSocket data despite pricing-page claims. Alpaca's
free IEX tier delivers real trade messages (IEX exchange only, ~2-3% of
US volume but structurally full real-time trades).

Uses raw websockets library rather than the alpaca-py SDK — the protocol
is simple (auth frame, subscribe frame, JSON message stream) and the raw
approach gives us full control over the asyncio integration + reconnect.

Day 6 (Nov 11): SIGTERM handling for Kubernetes, deadline-based shutdown
budget, and fixes from the Nov 10 review (see docs/deploy-design.md §3).

Run: python -m tickstore.ingest.alpaca_feed
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
import warnings
from collections.abc import Callable
from typing import Any

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from confluent_kafka import Producer  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from websockets.asyncio.client import connect  # noqa: E402
from websockets.exceptions import ConnectionClosed, InvalidHandshake  # noqa: E402

SYMBOLS: list[str] = [
    "SPY",
    "QQQ",
    "AAPL",
    "MSFT",
    "NVDA",
    "TSLA",
    "AMZN",
    "GOOGL",
    "META",
    "JPM",
]

# Reconnect / backoff (Day 3)
BASE_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 60.0
AUTH_RETRY_WAIT_SEC = 30.0
MAX_AUTH_RETRIES = 1  # consecutive auth failures allowed before giving up
STABLE_SESSION_THRESHOLD_SEC = 60.0
SETUP_RECV_TIMEOUT_SEC = 10.0  # hello + auth + subscribe must complete within this

# Kafka producer (Day 4)
KAFKA_TOPIC = "trades.raw"
QUEUE_MAX_SIZE = 10000
BUFFER_FULL_RETRY_SEC = 0.05
PRODUCER_FLUSH_TIMEOUT_SEC = 10  # used by alpaca_backfill

# Shutdown (Day 6). Total must fit inside GKE Spot preemption's ~15s window.
# Drain only moves messages into librdkafka's buffer; flush is what makes them
# durable, so drain is capped and flush gets the rest of the budget.
SHUTDOWN_BUDGET_SEC = 13.0
DRAIN_MAX_SEC = 3.0

EXIT_OK = 0
EXIT_FAILURE = 1

# Alpaca WebSocket endpoint — free IEX tier
ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"

logger = logging.getLogger("tickstore.alpaca_feed")


class AuthError(Exception):
    """Raised when Alpaca rejects our API key/secret (or connection limit, 406)."""


def make_producer() -> Producer:
    """Build a Confluent Cloud producer with idempotent-write config."""
    config = {
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
    }
    producer = Producer(config)
    logger.info(
        "producer initialized; bootstrap=%s topic=%s",
        config["bootstrap.servers"],
        KAFKA_TOPIC,
    )
    return producer


def serialize_trade(msg: dict[str, Any]) -> bytes:
    """JSON-encode an Alpaca trade message.

    Alpaca trade schema (per docs):
      T: 't' (type)       S: symbol      p: price       s: size
      t: timestamp (ISO)  i: trade id    x: exchange    c: conditions list
      z: tape             r: received_at (optional)
    """
    payload = {
        "event_type": msg.get("T"),
        "symbol": msg.get("S"),
        "price": msg.get("p"),
        "size": msg.get("s"),
        "timestamp": msg.get("t"),
        "exchange": msg.get("x"),
        "conditions": msg.get("c"),
        "tape": msg.get("z"),
        "trade_id": msg.get("i"),
    }
    return json.dumps(payload).encode("utf-8")


def delivery_callback(err: Any, msg: Any) -> None:
    """Called by librdkafka from its C thread when a produce is acked or fails."""
    if err is not None:
        logger.error(
            "delivery failed: %s key=%s",
            err,
            msg.key().decode("utf-8") if msg.key() else None,
        )
    else:
        logger.debug(
            "delivered: topic=%s partition=%d offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


async def producer_task(queue: asyncio.Queue, producer: Producer) -> None:
    """Consume trade dicts from the queue and produce to Kafka.

    REVIEW: on BufferError we retry the same message until librdkafka accepts
    it, never dropping it. While retrying we don't take from the queue, so the
    queue fills and the reader blocks on put(): backpressure reaches the
    WebSocket. Waits use asyncio.sleep, never a blocking poll(timeout), so the
    reader and WebSocket pings keep running. During shutdown this loop is
    bounded by the drain deadline (the task is cancelled when it expires).
    """
    while True:
        msg = await queue.get()
        key = msg["S"].encode("utf-8")
        value = serialize_trade(msg)
        stalled = False
        while True:
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=value,
                    on_delivery=delivery_callback,
                )
                break
            except BufferError:
                if not stalled:
                    logger.warning("librdkafka buffer full; retrying until accepted")
                    stalled = True
                producer.poll(0)
                await asyncio.sleep(BUFFER_FULL_RETRY_SEC)
        if stalled:
            logger.info("librdkafka buffer accepting again")
        producer.poll(0)
        queue.task_done()


async def _connect_and_stream(queue: asyncio.Queue, on_authenticated: Callable[[], None]) -> None:
    """Open WS, auth, subscribe, stream trade messages into the queue.

    Raises AuthError on auth failure. Calls on_authenticated() once auth
    succeeds. Returns normally if the server closes the connection cleanly.
    Network errors propagate to run_with_reconnect.
    """
    api_key = os.environ["ALPACA_API_KEY"].strip()
    api_secret = os.environ["ALPACA_API_SECRET"].strip()

    async with connect(ALPACA_WS_URL) as ws:
        # Setup handshake gets a deadline so a stalled server can't hang us;
        # TimeoutError is handled as a network error by the caller.
        async with asyncio.timeout(SETUP_RECV_TIMEOUT_SEC):
            hello_raw = await ws.recv()
            logger.info("alpaca hello: %s", hello_raw)

            await ws.send(json.dumps({"action": "auth", "key": api_key, "secret": api_secret}))
            auth_raw = await ws.recv()
            auth_data = json.loads(auth_raw)
            if isinstance(auth_data, list) and auth_data:
                first = auth_data[0]
                if first.get("T") == "error":
                    raise AuthError(f"auth failed: code={first.get('code')} msg={first.get('msg')}")
            logger.info("alpaca auth ok: %s", auth_raw)
            on_authenticated()

            await ws.send(json.dumps({"action": "subscribe", "trades": SYMBOLS}))
            sub_raw = await ws.recv()
            logger.info("alpaca subscribed: %s", sub_raw)

        # Stream loop: receive messages, filter to trades, push to queue
        async for raw in ws:
            try:
                messages = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("skipping malformed frame: %.200r", raw)
                continue
            if not isinstance(messages, list):
                messages = [messages]
            for m in messages:
                event = m.get("T")
                if event == "t":
                    await queue.put(m)
                elif event == "error":
                    logger.error("alpaca error message: %s", m)
                elif event == "subscription":
                    logger.info("subscription update: %s", m)
                else:
                    logger.debug("other message type %r: %s", event, m)


async def run_with_reconnect(queue: asyncio.Queue) -> None:
    """Wrap _connect_and_stream with exponential backoff + auth retry.

    Reconnect semantics from docs/ingestion-design.md:
    - Infinite retries for network errors, exponential backoff 1s→60s, jitter
    - A clean server close is treated as a disconnect (same backoff)
    - Auth errors: wait 30s and retry; more than MAX_AUTH_RETRIES consecutive
      failures re-raises AuthError (caller exits non-zero)
    - Reset backoff on stable sessions (>60s)

    REVIEW: all retry paths go through this one loop (no nested retries), so
    any error after a successful auth retry is handled like any other error.
    The auth-failure counter resets only when a connection authenticates, so
    two failures in a row is fatal but two failures days apart is not.
    """
    loop = asyncio.get_running_loop()
    backoff = BASE_BACKOFF_SEC
    auth_failures = 0

    def on_authenticated() -> None:
        nonlocal auth_failures
        auth_failures = 0

    while True:
        session_start = loop.time()
        try:
            await _connect_and_stream(queue, on_authenticated)
            logger.warning("alpaca closed the connection cleanly; treating as disconnect")

        except AuthError as auth_error:
            auth_failures += 1
            if auth_failures > MAX_AUTH_RETRIES:
                logger.critical(
                    "authentication failed %d times in a row; giving up: %s",
                    auth_failures,
                    auth_error,
                )
                raise
            logger.error(
                "authentication error: %s; retrying in %ds (retry %d of %d)",
                auth_error,
                AUTH_RETRY_WAIT_SEC,
                auth_failures,
                MAX_AUTH_RETRIES,
            )
            await asyncio.sleep(AUTH_RETRY_WAIT_SEC)
            continue

        except (ConnectionError, OSError, TimeoutError, ConnectionClosed, InvalidHandshake) as e:
            logger.warning("network error: %r", e)

        session_duration = loop.time() - session_start
        if session_duration >= STABLE_SESSION_THRESHOLD_SEC:
            logger.info("stable session of %.1fs; resetting backoff", session_duration)
            backoff = BASE_BACKOFF_SEC
        else:
            logger.warning(
                "session of %.1fs below stable threshold of %.1fs; escalating backoff",
                session_duration,
                STABLE_SESSION_THRESHOLD_SEC,
            )
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)
        jitter = random.uniform(-0.5, 0.5)
        wait = max(backoff + jitter, 0.1)
        logger.info("reconnecting in %.2fs", wait)
        await asyncio.sleep(wait)


async def _stop_task(task: asyncio.Task) -> None:
    """Cancel a task and wait for it to finish, without raising its exception."""
    task.cancel()
    await asyncio.wait({task})


async def _shutdown(
    reader: asyncio.Task,
    prod: asyncio.Task,
    queue: asyncio.Queue,
    producer: Producer,
) -> None:
    """4-step graceful shutdown inside a single deadline.

    REVIEW: the order matters. The producer task must still be running during
    the drain (step 2), because it is the only consumer of the queue. Cancelling
    the gather/main task instead would kill both tasks first and the drain
    would time out with every queued message lost (Nov 10 finding).
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SHUTDOWN_BUDGET_SEC
    logger.info("shutdown starting; budget=%.0fs", SHUTDOWN_BUDGET_SEC)

    # 1. Stop reading new trades.
    await _stop_task(reader)

    # 2. Drain the queue into librdkafka while the producer task is alive.
    if not prod.done():
        drain_timeout = max(min(DRAIN_MAX_SEC, deadline - loop.time()), 0.0)
        logger.info("draining queue (depth=%d, timeout=%.1fs)", queue.qsize(), drain_timeout)
        try:
            await asyncio.wait_for(queue.join(), timeout=drain_timeout)
        except TimeoutError:
            logger.warning(
                "queue drain timed out; at least %d messages not produced", queue.qsize()
            )

    # 3. Stop the producer task.
    await _stop_task(prod)

    # 4. Flush librdkafka with the rest of the budget. Runs in a thread so the
    # event loop stays responsive (a second signal can still force-exit).
    flush_timeout = max(deadline - loop.time(), 0.0)
    logger.info("flushing producer (timeout=%.1fs)", flush_timeout)
    remaining = await loop.run_in_executor(None, producer.flush, flush_timeout)
    if remaining > 0:
        logger.warning("%d messages undelivered after flush", remaining)
    else:
        logger.info("all messages delivered")


async def _run() -> int:
    """Orchestrate reader + producer with graceful shutdown. Returns exit code."""
    load_dotenv()
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    logger.info("queue initialized; maxsize=%d", QUEUE_MAX_SIZE)

    # REVIEW: the handler only sets a flag. All shutdown ordering lives in
    # _shutdown. A signal arriving while a shutdown is already in progress
    # (second Ctrl-C locally) exits immediately with 128 + signal number.
    shutdown_requested = asyncio.Event()

    def on_signal(sig: signal.Signals) -> None:
        if shutdown_requested.is_set():
            logger.critical(
                "%s during shutdown; exiting immediately (%d messages still queued)",
                sig.name,
                queue.qsize(),
            )
            os._exit(128 + sig.value)
        logger.info("%s received; starting graceful shutdown", sig.name)
        shutdown_requested.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, on_signal, sig)

    producer = make_producer()

    reader = asyncio.create_task(run_with_reconnect(queue), name="reader")
    prod = asyncio.create_task(producer_task(queue, producer), name="producer")
    shutdown_wait = asyncio.create_task(shutdown_requested.wait(), name="shutdown-wait")

    exit_code = EXIT_OK
    try:
        done, _ = await asyncio.wait(
            {reader, prod, shutdown_wait}, return_when=asyncio.FIRST_COMPLETED
        )
        # Either a signal arrived (clean exit) or a worker task ended, which
        # should never happen on its own: it means a failure.
        for task in (reader, prod):
            if task not in done:
                continue
            exit_code = EXIT_FAILURE
            exc = None if task.cancelled() else task.exception()
            if exc is None:
                logger.error("%s task stopped unexpectedly", task.get_name())
            else:
                logger.error(
                    "%s task failed: %r",
                    task.get_name(),
                    exc,
                    exc_info=None if isinstance(exc, AuthError) else exc,
                )
    finally:
        shutdown_requested.set()  # any signal from here on force-exits
        await _stop_task(shutdown_wait)
        await _shutdown(reader, prod, queue, producer)

    logger.info("exiting with code %d", exit_code)
    return exit_code


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    try:
        exit_code = asyncio.run(_run())
    except KeyboardInterrupt:
        # Only reachable if Ctrl-C lands before the signal handlers are installed.
        logger.info("interrupted during startup")
        exit_code = 128 + signal.SIGINT.value
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
