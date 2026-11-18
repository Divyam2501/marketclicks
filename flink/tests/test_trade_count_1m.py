"""Unit tests for job 1's pure logic. No Kafka, no JVM."""

import json

from trade_count_1m import CountTrades, parse_trade, window_record

# 2026-11-13T14:45:50.629Z
TS_MS = 1794581150629


def trade(**overrides: object) -> str:
    msg = {
        "event_type": "t",
        "symbol": "MSFT",
        "price": 510.1,
        "size": 100,
        "timestamp": "2026-11-13T14:45:50.6298733Z",
        "conditions": ["@"],
    }
    msg.update(overrides)
    return json.dumps(msg)


def test_parse_valid_trade_keeps_milliseconds() -> None:
    assert parse_trade(trade()) == ("MSFT", TS_MS)


def test_parse_nanosecond_timestamp() -> None:
    assert parse_trade(trade(timestamp="2026-11-13T14:45:50.629873312Z")) == ("MSFT", TS_MS)


def test_parse_whole_second_timestamp() -> None:
    assert parse_trade(trade(timestamp="2026-11-13T14:45:50Z")) == ("MSFT", TS_MS - 629)


def test_parse_rejects_invalid_json() -> None:
    assert parse_trade("{not json") is None


def test_parse_rejects_missing_fields() -> None:
    assert parse_trade(json.dumps({"symbol": "MSFT"})) is None
    assert parse_trade(json.dumps({"timestamp": "2026-11-13T14:45:50Z"})) is None


def test_parse_rejects_bad_values() -> None:
    assert parse_trade(trade(symbol="")) is None
    assert parse_trade(trade(symbol=None)) is None
    assert parse_trade(trade(timestamp="yesterday")) is None
    assert parse_trade(trade(timestamp=None)) is None


def test_parse_rejects_timestamp_without_timezone() -> None:
    # Without a zone, the epoch value would depend on the machine's local time.
    assert parse_trade(trade(timestamp="2026-11-13T14:45:50")) is None


def test_count_trades_is_incremental_and_mergeable() -> None:
    agg = CountTrades()
    acc = agg.create_accumulator()
    for _ in range(3):
        acc = agg.add(("MSFT", TS_MS), acc)
    assert agg.get_result(acc) == 3
    assert agg.merge(acc, 2) == 5


def test_window_record_shape() -> None:
    start = 1794581100000  # 14:45:00Z
    record = json.loads(window_record("MSFT", start, start + 60_000, 412))
    assert record == {
        "symbol": "MSFT",
        "window_start": "2026-11-13T14:45:00Z",
        "window_end": "2026-11-13T14:46:00Z",
        "trade_count": 412,
    }
