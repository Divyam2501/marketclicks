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

Run: python -m tickstore.ingest.alpaca_feed
"""

import asyncio
import json
import logging
import os
import random
import warnings
from typing import Any

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from confluent_kafka import Producer  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from websockets.asyncio.client import connect  # noqa: E402
from websockets.exceptions import ConnectionClosed  # noqa: E402

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

# Reconnect / backoff (Day 3, unchanged)
BASE_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 60.0
AUTH_RETRY_WAIT_SEC = 30.0
MAX_AUTH_RETRIES = 1
STABLE_SESSION_THRESHOLD_SEC = 60.0

# Kafka producer (Day 4, unchanged)
KAFKA_TOPIC = "trades.raw"
QUEUE_MAX_SIZE = 10000
PRODUCER_FLUSH_TIMEOUT_SEC = 10
QUEUE_DRAIN_TIMEOUT_SEC = 10

# Alpaca WebSocket endpoint — free IEX tier
ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"

logger = logging.getLogger("tickstore.alpaca_feed")


class AuthError(Exception):
    """Raised when Alpaca rejects our API key/secret."""


def make_producer() -> Producer:
    """Build a Confluent Cloud producer with idempotent-write config.

    Unchanged from polygon_feed.
    """
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
    """Consume trade dicts from the queue and produce to Kafka."""
    while True:
        msg = await queue.get()
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=msg["S"].encode("utf-8"),
                value=serialize_trade(msg),
                on_delivery=delivery_callback,
            )
        except BufferError:
            logger.warning("librdkafka buffer full; polling to drain")
            producer.poll(0.1)
        producer.poll(0)
        queue.task_done()


async def _connect_and_stream(queue: asyncio.Queue) -> None:
    """Open WS, auth, subscribe, stream trade messages into the queue.

    Raises AuthError on auth failure (handled by run_with_reconnect as a
    non-transient issue). Other exceptions propagate to the outer loop.
    """
    api_key = os.environ["ALPACA_API_KEY"].strip()
    api_secret = os.environ["ALPACA_API_SECRET"].strip()

    async with connect(ALPACA_WS_URL) as ws:
        # Alpaca sends a "connected" hello on connect
        hello_raw = await ws.recv()
        logger.info("alpaca hello: %s", hello_raw)

        # Auth frame
        await ws.send(json.dumps({"action": "auth", "key": api_key, "secret": api_secret}))
        auth_raw = await ws.recv()
        auth_data = json.loads(auth_raw)
        if isinstance(auth_data, list) and auth_data:
            first = auth_data[0]
            if first.get("T") == "error":
                raise AuthError(f"auth failed: code={first.get('code')} msg={first.get('msg')}")
        logger.info("alpaca auth ok: %s", auth_raw)

        # Subscribe to trades for our symbols
        await ws.send(json.dumps({"action": "subscribe", "trades": SYMBOLS}))
        sub_raw = await ws.recv()
        logger.info("alpaca subscribed: %s", sub_raw)

        # Stream loop: receive messages, filter to trades, push to queue
        async for raw in ws:
            messages = json.loads(raw)
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

    Mirrors polygon_feed's reconnect semantics from docs/ingestion-design.md:
    - Infinite retries for network errors, exponential backoff 1s→60s, jitter
    - 1 retry for auth errors (30s wait), then exit non-zero
    - Reset backoff on stable sessions (>60s)
    """
    backoff = BASE_BACKOFF_SEC
    while True:
        session_start = asyncio.get_event_loop().time()
        try:
            await _connect_and_stream(queue)
            logger.info("connection closed cleanly; reconnecting")
            backoff = BASE_BACKOFF_SEC

        except AuthError as auth_error:
            logger.error("authentication error: %s", auth_error)
            for attempt in range(1, MAX_AUTH_RETRIES + 1):
                logger.info(
                    "retrying authentication in %ds (attempt %d of %d)",
                    AUTH_RETRY_WAIT_SEC,
                    attempt,
                    MAX_AUTH_RETRIES,
                )
                await asyncio.sleep(AUTH_RETRY_WAIT_SEC)
                try:
                    await _connect_and_stream(queue)
                    break
                except AuthError as retry_auth_error:
                    logger.error(
                        "retry %d failed with authentication error: %s",
                        attempt,
                        retry_auth_error,
                    )
            else:
                logger.critical("max authentication retries reached; exiting")
                break

        except (ConnectionError, OSError, ConnectionClosed) as network_error:
            logger.warning("network error: %s", network_error)
            session_duration = asyncio.get_event_loop().time() - session_start
            if session_duration >= STABLE_SESSION_THRESHOLD_SEC:
                logger.info(
                    "stable session of %.1fs; resetting backoff",
                    session_duration,
                )
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


async def _run() -> None:
    """Orchestrate reader + producer with graceful shutdown."""
    load_dotenv()
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    logger.info("queue initialized; maxsize=%d", QUEUE_MAX_SIZE)

    producer = make_producer()

    reader_task = asyncio.create_task(run_with_reconnect(queue), name="reader")
    prod_task = asyncio.create_task(producer_task(queue, producer), name="producer")

    try:
        await asyncio.gather(reader_task, prod_task)
    finally:
        logger.info("shutdown starting")

        if not reader_task.done():
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):
                pass

        depth = queue.qsize()
        logger.info("draining queue (depth=%d)", depth)
        try:
            await asyncio.wait_for(queue.join(), timeout=QUEUE_DRAIN_TIMEOUT_SEC)
        except TimeoutError:
            logger.warning(
                "queue drain timed out after %ds; %d messages not produced",
                QUEUE_DRAIN_TIMEOUT_SEC,
                queue.qsize(),
            )

        if not prod_task.done():
            prod_task.cancel()
            try:
                await prod_task
            except (asyncio.CancelledError, Exception):
                pass

        logger.info("flushing producer (timeout=%ds)", PRODUCER_FLUSH_TIMEOUT_SEC)
        remaining = producer.flush(PRODUCER_FLUSH_TIMEOUT_SEC)
        if remaining > 0:
            logger.warning("%d messages undelivered after flush", remaining)
        else:
            logger.info("all messages delivered; exiting cleanly")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("shutdown signal received")


if __name__ == "__main__":
    main()
