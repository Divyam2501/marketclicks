"""Polygon.io WebSocket client with auto-reconnect.

Streams trades for 10 US equity symbols to stdout. Survives network drops
with exponential backoff + jitter. Fails fast on persistent auth errors.

Day 3 scope: receive + log + reconnect.
Not yet included: Kafka producer (Day 4), metrics (Day 6), quotes (Day 7).

Run: python -m tickstore.ingest.polygon_feed
"""

import asyncio
import logging
import os
import random
import warnings

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

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

BASE_BACKOFF_SEC = 1.0
MAX_BACKOFF_SEC = 60.0
AUTH_RETRY_WAIT_SEC = 30.0
MAX_AUTH_RETRIES = 1
STABLE_SESSION_THRESHOLD_SEC = 60.0

logger = logging.getLogger("tickstore.polygon_feed")


async def handle_messages(messages: list[WebSocketMessage]) -> None:
    """Log each incoming message. Polygon batches messages; we iterate the batch."""
    for msg in messages:
        logger.info("%s", msg)


async def run_with_reconnect() -> None:
    load_dotenv()
    api_key = os.environ["POLYGON_API_KEY"]

    backoff = BASE_BACKOFF_SEC
    while True:
        try:
            client = WebSocketClient(
                api_key=api_key,
                feed=Feed.Delayed,
                market=Market.Stocks,
            )
            channels = [f"T.{sym}" for sym in SYMBOLS]
            client.subscribe(*channels)

            logger.info(
                "connecting to polygon; subscribed to %d trade channels: %s",
                len(channels),
                ", ".join(SYMBOLS),
            )
            start_time = asyncio.get_event_loop().time()
            await client.connect(handle_messages)
            # Clean disconnect (no exception)
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
                    break  # retry succeeded; exit the inner loop
                except AuthError as retry_auth_error:
                    logger.error(
                        "retry %d failed with authentication error: %s",
                        attempt,
                        retry_auth_error,
                    )
            else:
                logger.critical("max authentication retries reached; exiting")
                break  # exit the outer while loop

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
            wait = max(backoff + jitter, 0.1)  # clamp to non-negative
            logger.info("reconnecting in %.2fs", wait)
            await asyncio.sleep(wait)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    asyncio.run(run_with_reconnect())


if __name__ == "__main__":
    main()
