"""Tests for alpaca_feed reconnect, producer backpressure and shutdown.

No network: _connect_and_stream is replaced by scripted fakes, the Kafka
producer by FakeProducer, and asyncio.sleep is recorded instead of waited.
"""

import asyncio
import os
import signal
from collections.abc import Callable
from typing import Any

import pytest

from tickstore.ingest import alpaca_feed
from tickstore.ingest.alpaca_feed import AuthError


class FakeProducer:
    """Stands in for confluent_kafka.Producer; can reject the first N produce calls."""

    def __init__(self, buffer_full_times: int = 0) -> None:
        self.produced: list[bytes] = []
        self.buffer_full_times = buffer_full_times
        self.flush_timeouts: list[float] = []

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        if self.buffer_full_times > 0:
            self.buffer_full_times -= 1
            raise BufferError("Local: Queue full")
        self.produced.append(key)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_timeouts.append(timeout)
        return 0


class StopTest(Exception):
    """Raised by a scripted fake to end run_with_reconnect's infinite loop."""


def trade(symbol: str) -> dict[str, Any]:
    return {"T": "t", "S": symbol, "p": 1.0, "s": 1, "t": "2026-11-11T15:00:00Z"}


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record asyncio.sleep durations instead of waiting; jitter fixed at 0."""
    recorded: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(alpaca_feed.random, "uniform", lambda a, b: 0.0)
    return recorded


def script(monkeypatch: pytest.MonkeyPatch, steps: list[str]) -> None:
    """Replace _connect_and_stream with a fake that plays one step per call.

    Steps: "auth_fail", "auth_ok_then_drop" (auth succeeds, then a network
    error), "drop" (network error before auth), "clean_close", "stop".
    """
    remaining = list(steps)

    async def fake(queue: asyncio.Queue, on_authenticated: Callable[[], None]) -> None:
        step = remaining.pop(0)
        if step == "auth_fail":
            raise AuthError("auth failed: code=402")
        if step == "auth_ok_then_drop":
            on_authenticated()
            raise ConnectionResetError("dropped")
        if step == "drop":
            raise ConnectionResetError("dropped")
        if step == "clean_close":
            return
        raise StopTest

    monkeypatch.setattr(alpaca_feed, "_connect_and_stream", fake)


# --- reconnect -------------------------------------------------------------


async def test_auth_exhausted_raises(monkeypatch: pytest.MonkeyPatch, sleeps: list[float]) -> None:
    script(monkeypatch, ["auth_fail", "auth_fail", "stop"])
    with pytest.raises(AuthError):
        await alpaca_feed.run_with_reconnect(asyncio.Queue())
    assert sleeps == [alpaca_feed.AUTH_RETRY_WAIT_SEC]


async def test_auth_counter_resets_after_successful_auth(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # Two auth failures separated by a successful session must not be fatal.
    script(monkeypatch, ["auth_fail", "auth_ok_then_drop", "auth_fail", "stop"])
    with pytest.raises(StopTest):
        await alpaca_feed.run_with_reconnect(asyncio.Queue())


async def test_network_error_after_auth_retry_keeps_reconnecting(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # Bug 2: this used to escape the nested auth-retry handler and kill the reader.
    script(monkeypatch, ["auth_fail", "auth_ok_then_drop", "drop", "stop"])
    with pytest.raises(StopTest):
        await alpaca_feed.run_with_reconnect(asyncio.Queue())


async def test_clean_close_backs_off(monkeypatch: pytest.MonkeyPatch, sleeps: list[float]) -> None:
    # Bug 4: a clean server close used to reconnect immediately with no wait.
    script(monkeypatch, ["clean_close", "clean_close", "stop"])
    with pytest.raises(StopTest):
        await alpaca_feed.run_with_reconnect(asyncio.Queue())
    assert sleeps == [2.0, 4.0]


# --- producer --------------------------------------------------------------


async def test_buffer_full_retries_instead_of_dropping(sleeps: list[float]) -> None:
    # Bug 3: BufferError used to drop the message.
    producer = FakeProducer(buffer_full_times=3)
    queue: asyncio.Queue = asyncio.Queue()
    for symbol in ("AAPL", "MSFT"):
        queue.put_nowait(trade(symbol))

    task = asyncio.create_task(alpaca_feed.producer_task(queue, producer))
    await asyncio.wait_for(queue.join(), timeout=1)
    task.cancel()

    assert producer.produced == [b"AAPL", b"MSFT"]
    assert len(sleeps) == 3


# --- shutdown / exit codes -------------------------------------------------


@pytest.fixture
def fake_env(monkeypatch: pytest.MonkeyPatch) -> FakeProducer:
    producer = FakeProducer()
    monkeypatch.setattr(alpaca_feed, "load_dotenv", lambda: None)
    monkeypatch.setattr(alpaca_feed, "make_producer", lambda: producer)
    return producer


async def test_sigterm_drains_queue_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch, fake_env: FakeProducer, caplog: pytest.LogCaptureFixture
) -> None:
    # The Nov 10 finding: shutdown must drain with the producer task still alive.
    # The producer stalls on a full buffer so the queue is still backed up when
    # SIGTERM is handled; otherwise it empties the queue first and the drain
    # step is never exercised.
    fake_env.buffer_full_times = 20
    monkeypatch.setattr(alpaca_feed, "BUFFER_FULL_RETRY_SEC", 0.0)
    symbols = [f"SYM{i}" for i in range(50)]

    async def reader(queue: asyncio.Queue) -> None:
        for s in symbols:
            await queue.put(trade(s))
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.Event().wait()  # block like a live stream

    monkeypatch.setattr(alpaca_feed, "run_with_reconnect", reader)
    caplog.set_level("INFO", logger="tickstore.alpaca_feed")

    assert await alpaca_feed._run() == alpaca_feed.EXIT_OK
    drain_logs = [r.getMessage() for r in caplog.records if "draining queue" in r.message]
    assert drain_logs and "depth=0," not in drain_logs[0]  # queue was backed up
    assert fake_env.produced == [s.encode() for s in symbols]
    assert len(fake_env.flush_timeouts) == 1


async def test_auth_exhausted_flushes_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, fake_env: FakeProducer
) -> None:
    # Bug 1: auth exhaustion used to hang forever with the pod still Running.
    async def reader(queue: asyncio.Queue) -> None:
        await queue.put(trade("SPY"))
        raise AuthError("auth failed: code=402")

    monkeypatch.setattr(alpaca_feed, "run_with_reconnect", reader)

    assert await asyncio.wait_for(alpaca_feed._run(), timeout=5) == alpaca_feed.EXIT_FAILURE
    assert fake_env.produced == [b"SPY"]
