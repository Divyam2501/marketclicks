"""Polygon.io WebSocket client with auto-reconnect + Kafka producer.

Streams second-level OHLCV aggregate bars (AS.*) for 10 US equity symbols to
Confluent topic `trades.raw` (name retained from initial design; carries bar
data). Partitioned by symbol. WebSocket reader and Kafka producer run as
separate coroutines sharing a bounded asyncio.Queue; reader blocks on
queue-full for natural backpressure to Polygon.

Day 4 scope: ingest + reconnect + idempotent produce to Kafka.
Day 5 (Nov 9) pivot: switched from T.* (trades) to AS.* (second aggregates)
after discovering Polygon Stocks Starter plan doesn't include trade WebSocket.
Not yet included: schema registry (Week 2+), metrics (Day 6), quotes (Day 7+).

Run: python -m tickstore.ingest.polygon_feed
"""

import asyncio
import json
import logging
import os
import random
import warnings

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from confluent_kafka import Producer  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from polygon import WebSocketClient  # noqa: E402
from polygon.exceptions import AuthError  # noqa: E402
from polygon.websocket.models import Feed, Market, WebSocketMessage  # noqa: E402
from websockets import ConnectionClosed  # noqa: E402

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
MAX_AUTH_RETRIES = 1
STABLE_SESSION_THRESHOLD_SEC = 60.0

# Kafka producer (Day 4)
KAFKA_TOPIC = "trades.raw"
QUEUE_MAX_SIZE = 10000
PRODUCER_FLUSH_TIMEOUT_SEC = 10
QUEUE_DRAIN_TIMEOUT_SEC = 10

logger = logging.getLogger("tickstore.polygon_feed")


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


def serialize_bar(msg: WebSocketMessage) -> bytes:
    """JSON-encode a Polygon second-aggregate bar. Uses getattr for defensive access."""
    payload = {
        "event_type": getattr(msg, "event_type", None),
        "symbol": getattr(msg, "symbol", None),
        "open": getattr(msg, "open", None),
        "high": getattr(msg, "high", None),
        "low": getattr(msg, "low", None),
        "close": getattr(msg, "close", None),
        "volume": getattr(msg, "volume", None),
        "vwap": getattr(msg, "vwap", None),
        "start_timestamp": getattr(msg, "start_timestamp", None),
        "end_timestamp": getattr(msg, "end_timestamp", None),
    }
    return json.dumps(payload).encode("utf-8")


def delivery_callback(err, msg) -> None:
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
    """Consume trades from the queue and produce to Kafka. Runs until cancelled."""
    while True:
        msg = await queue.get()
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=msg.symbol.encode("utf-8"),
                value=serialize_bar(msg),
                on_delivery=delivery_callback,
            )
        except BufferError:
            # librdkafka's internal queue is full; give it time to drain
            logger.warning("librdkafka buffer full; polling to drain")
            producer.poll(0.1)
        # Serve any ready delivery callbacks (non-blocking)
        producer.poll(0)
        queue.task_done()


async def run_with_reconnect(queue: asyncio.Queue) -> None:
    """Connect to Polygon, stream trades into the queue, auto-reconnect on drops."""
    api_key = os.environ["POLYGON_API_KEY"]

    async def handle_messages(messages: list[WebSocketMessage]) -> None:
        for m in messages:
            event = getattr(m, "event_type", None)
            if event == "AS":
                await queue.put(m)
            else:
                logger.info("non-bar message: %s", m)

    backoff = BASE_BACKOFF_SEC
    while True:
        try:
            client = WebSocketClient(
                api_key=api_key,
                feed=Feed.Delayed,
                market=Market.Stocks,
            )
            channels = [f"AS.{sym}" for sym in SYMBOLS]
            client.subscribe(*channels)

            logger.info(
                "connecting to polygon; subscribed to %d second-bar channels: %s",
                len(channels),
                ", ".join(SYMBOLS),
            )
            start_time = asyncio.get_event_loop().time()
            await client.connect(handle_messages)
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
                    await client.connect(handle_messages)
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

            session_duration = asyncio.get_event_loop().time() - start_time
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

        # 1. Stop the reader so no new messages enter the queue
        if not reader_task.done():
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):
                pass

        # 2. Let producer_task drain whatever is left in the queue
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

        # 3. Cancel the producer coroutine
        if not prod_task.done():
            prod_task.cancel()
            try:
                await prod_task
            except (asyncio.CancelledError, Exception):
                pass

        # 4. Flush librdkafka's internal buffer to Kafka
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
