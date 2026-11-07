"""Polygon.io WebSocket client streaming trades for 10 symbols to stdout.

Day 2 scope: receive + log only.
Explicitly NOT done today:
  - No Kafka producer (Sat Nov 8)
  - No reconnect/backoff (Fri Nov 7)
  - No schema validation, no DLQ, no metrics

Run: python -m tickstore.ingest.polygon_feed
"""

import asyncio
import logging
import os
import warnings

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from dotenv import load_dotenv  # noqa: E402
from polygon import WebSocketClient  # noqa: E402
from polygon.websocket.models import Feed, Market, WebSocketMessage  # noqa: E402

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

logger = logging.getLogger("tickstore.polygon_feed")


async def handle_messages(messages: list[WebSocketMessage]) -> None:
    """Log each incoming message. Polygon batches messages; we iterate the batch."""
    for msg in messages:
        logger.info("%s", msg)


async def run() -> None:
    load_dotenv()
    api_key = os.environ["POLYGON_API_KEY"]

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
    await client.connect(handle_messages)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
