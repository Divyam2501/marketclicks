"""Alpaca REST backfill: pull historical trades and produce to Kafka.

One-shot tool that validates the Kafka producer leg of the pipeline
without needing live market hours. Fetches the last N minutes of
trades for SYMBOLS via Alpaca's REST API and produces each to
`trades.raw`, using the same producer config as alpaca_feed.py.

Alpaca free-tier REST data has a ~15-minute delay, so the time window
is shifted back 15 minutes to stay inside what our key is entitled to.

Usage:
  uv run python -m tickstore.ingest.alpaca_backfill
  uv run python -m tickstore.ingest.alpaca_backfill --minutes 60
"""

import argparse
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv

from tickstore.ingest.alpaca_feed import (
    KAFKA_TOPIC,
    PRODUCER_FLUSH_TIMEOUT_SEC,
    SYMBOLS,
    delivery_callback,
    make_producer,
    serialize_trade,
)

ALPACA_TRADES_URL = "https://data.alpaca.markets/v2/stocks/trades"
ALPACA_FREE_TIER_DELAY_MIN = 15

logger = logging.getLogger("tickstore.alpaca_backfill")


def _fetch_page(headers: dict[str, str], params: dict[str, Any]) -> dict[str, Any]:
    """Synchronous one-page fetch from Alpaca REST."""
    url = f"{ALPACA_TRADES_URL}?{urlencode(params)}"
    req = Request(url, headers=headers)
    with urlopen(req, timeout=30) as resp:
        data: dict[str, Any] = json.loads(resp.read())
        return data


def fetch_trades(minutes: int) -> list[dict[str, Any]]:
    """Fetch historical trades across all SYMBOLS for the last `minutes` of market data.

    Shifts the time window back by Alpaca's ~15-minute free-tier delay so we stay
    inside what IEX free entitlement actually serves. Returns a flat list of trade
    dicts with symbol injected and type marker set so downstream serialization
    matches the WebSocket trade schema.
    """
    key = os.environ["ALPACA_API_KEY"].strip()
    secret = os.environ["ALPACA_API_SECRET"].strip()

    end_time = datetime.now(UTC) - timedelta(minutes=ALPACA_FREE_TIER_DELAY_MIN)
    start_time = end_time - timedelta(minutes=minutes)

    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }
    params: dict[str, Any] = {
        "symbols": ",".join(SYMBOLS),
        "start": start_time.isoformat().replace("+00:00", "Z"),
        "end": end_time.isoformat().replace("+00:00", "Z"),
        "feed": "iex",
        "limit": 10000,
    }

    logger.info(
        "fetching trades for %d symbols from %s to %s (feed=iex)",
        len(SYMBOLS),
        params["start"],
        params["end"],
    )

    all_trades: list[dict[str, Any]] = []
    page_token: str | None = None
    page_num = 0

    while True:
        page_num += 1
        page_params = params.copy()
        if page_token:
            page_params["page_token"] = page_token

        data = _fetch_page(headers, page_params)

        trades_by_symbol: dict[str, list[dict[str, Any]]] = data.get("trades") or {}
        page_count = 0
        for symbol, trades_list in trades_by_symbol.items():
            for t in trades_list:
                # Inject symbol + type so serialize_trade (shared with WS path) works
                all_trades.append({**t, "S": symbol, "T": "t"})
                page_count += 1

        logger.info(
            "page %d: +%d trades (running total: %d)",
            page_num,
            page_count,
            len(all_trades),
        )

        page_token = data.get("next_page_token")
        if not page_token:
            break

    return all_trades


def produce_to_kafka(trades: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Produce every trade to Kafka. Returns (produced, errors, undelivered_after_flush)."""
    producer = make_producer()

    produced = 0
    errors = 0

    for trade in trades:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=trade["S"].encode("utf-8"),
                value=serialize_trade(trade),
                on_delivery=delivery_callback,
            )
            produced += 1
        except BufferError:
            # librdkafka internal buffer full; give it time to drain
            logger.warning("librdkafka buffer full at %d; polling to drain", produced)
            producer.poll(0.5)
            # retry this trade
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=trade["S"].encode("utf-8"),
                    value=serialize_trade(trade),
                    on_delivery=delivery_callback,
                )
                produced += 1
            except Exception as retry_err:
                logger.error("produce retry also failed: %s", retry_err)
                errors += 1
        except Exception as e:
            logger.error("produce failed: %s", e)
            errors += 1

        # Periodically serve delivery callbacks so we don't backlog
        if produced % 500 == 0 and produced > 0:
            producer.poll(0)

    logger.info("flushing producer (timeout=%ds)", PRODUCER_FLUSH_TIMEOUT_SEC)
    remaining = producer.flush(PRODUCER_FLUSH_TIMEOUT_SEC)
    return produced, errors, remaining


def main() -> None:
    parser = argparse.ArgumentParser(description="Alpaca REST backfill to Kafka")
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="Minutes of trade history to fetch (default: 30)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    load_dotenv()

    trades = fetch_trades(args.minutes)
    logger.info("total fetched: %d trades across %d symbols", len(trades), len(SYMBOLS))

    if not trades:
        logger.warning("no trades returned; nothing to produce")
        return

    produced, errors, remaining = produce_to_kafka(trades)
    logger.info(
        "BACKFILL COMPLETE: produced=%d errors=%d undelivered_after_flush=%d",
        produced,
        errors,
        remaining,
    )


if __name__ == "__main__":
    main()
