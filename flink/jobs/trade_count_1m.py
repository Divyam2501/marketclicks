"""Job 1: trades per symbol per 1-minute event-time window. Design: docs/flink-design.md.

    trades.raw -> parse -> event time + watermarks -> key_by(symbol)
      -> 1-min tumbling window -> count -> metrics.trade_count_1m (or stdout)
    side outputs: malformed records, late trades

Live:    uv run python jobs/trade_count_1m.py --from latest --sink kafka
Replay:  uv run python jobs/trade_count_1m.py --from 2026-11-13T14:45:00+00:00 \\
             --until 2026-11-13T21:55:00+00:00 --sink print
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows

FLINK_DIR = Path(__file__).resolve().parent.parent
CONNECTOR_JAR = FLINK_DIR / "jars" / "flink-sql-connector-kafka-5.0.0-2.2.jar"
CHECKPOINT_DIR = FLINK_DIR / "checkpoints"

SOURCE_TOPIC = "trades.raw"
SINK_TOPIC = "metrics.trade_count_1m"
GROUP_ID = "flink-trade-count-1m"

# One source subtask per trades.raw partition, so watermarks are per partition
# (PyFlink generates them after the source; see design §4.4).
PARALLELISM = 3
# Plain ints, not Duration: building a Duration starts the JVM, and the unit
# tests import this module without Java.
OUT_OF_ORDERNESS_SEC = 2  # measured max 51 ms on Nov 13 (§4.4)
IDLENESS_SEC = 30  # partition 2 is always empty
CHECKPOINT_INTERVAL_MS = 30_000

PLAIN_LOGIN_MODULE = (
    "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule"
)

TRADE_TYPE = Types.TUPLE([Types.STRING(), Types.LONG()])  # (symbol, event time ms)
MALFORMED = OutputTag("malformed", Types.STRING())
# Not TRADE_TYPE: PyFlink 2.2.1 OutputTag pickling clears the Java type of the
# tag itself but not of a tuple's field types, so a tuple-typed tag fails with
# "cannot pickle '_thread.RLock'". A pickled type carries the tuple as-is.
LATE = OutputTag("late", Types.PICKLED_BYTE_ARRAY())


# --- Pure functions (unit-tested without Flink) ------------------------------


def parse_trade(raw: str) -> tuple[str, int] | None:
    """Return (symbol, event time in epoch ms), or None if the record is unusable.

    Event time is the trade's own exchange timestamp, e.g.
    "2026-11-13T14:45:50.6298733Z". Python keeps microseconds and drops the
    rest; we keep milliseconds.
    """
    try:
        msg = json.loads(raw)
        symbol = msg["symbol"]
        ts = datetime.fromisoformat(msg["timestamp"])
    except (ValueError, TypeError, KeyError):
        return None
    if not isinstance(symbol, str) or not symbol or ts.tzinfo is None:
        return None
    return symbol, int(ts.timestamp() * 1000)


def iso_ms(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def window_record(symbol: str, start_ms: int, end_ms: int, count: int) -> str:
    """JSON value written for one fired window (design §4.8)."""
    return json.dumps(
        {
            "symbol": symbol,
            "window_start": iso_ms(start_ms),
            "window_end": iso_ms(end_ms),
            "trade_count": count,
        }
    )


# --- Flink functions ---------------------------------------------------------


class ParseTrade(ProcessFunction):
    """Parse JSON; unusable records go to the MALFORMED side output, never crash."""

    def process_element(self, value: str, ctx: ProcessFunction.Context) -> Iterator[Any]:
        trade = parse_trade(value)
        if trade is None:
            yield MALFORMED, value
        else:
            yield trade


class TradeTime(TimestampAssigner):
    def extract_timestamp(self, value: tuple[str, int], record_timestamp: int) -> int:
        return value[1]


class CountTrades(AggregateFunction):
    """Incremental count: one integer per symbol per open window (§4.5)."""

    def create_accumulator(self) -> int:
        return 0

    def add(self, value: tuple[str, int], accumulator: int) -> int:
        return accumulator + 1

    def get_result(self, accumulator: int) -> int:
        return accumulator

    def merge(self, acc_a: int, acc_b: int) -> int:
        return acc_a + acc_b


class EmitWindow(ProcessWindowFunction):
    """Attach symbol and window bounds to the single count from CountTrades."""

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[int],
    ) -> Iterator[str]:
        window = context.window()
        for count in elements:
            yield window_record(key, window.start, window.end, count)


# --- Wiring ------------------------------------------------------------------


def kafka_properties() -> dict[str, str]:
    jaas = (
        f"{PLAIN_LOGIN_MODULE} required "
        f'username="{os.environ["CONFLUENT_API_KEY"]}" '
        f'password="{os.environ["CONFLUENT_API_SECRET"]}";'
    )
    return {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": jaas,
    }


def to_epoch_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def build_source(start: str, until: str | None) -> KafkaSource:
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["CONFLUENT_BOOTSTRAP_SERVERS"])
        .set_topics(SOURCE_TOPIC)
        .set_group_id(GROUP_ID)
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if start == "latest":
        builder.set_starting_offsets(KafkaOffsetsInitializer.latest())
    else:
        builder.set_starting_offsets(KafkaOffsetsInitializer.timestamp(to_epoch_ms(start)))
    if until is not None:
        # Bounded replay: at the end Flink advances the watermark to +inf, so
        # every remaining window fires.
        builder.set_bounded(KafkaOffsetsInitializer.timestamp(to_epoch_ms(until)))
    for key, value in kafka_properties().items():
        builder.set_property(key, value)
    return builder.build()


def build_sink() -> KafkaSink:
    # No record key: PyFlink's key serializer sees the whole record, not one
    # field. The topic has one partition, and (symbol, window_start) in the
    # value identifies a window (§4.8).
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(os.environ["CONFLUENT_BOOTSTRAP_SERVERS"])
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    for key, value in kafka_properties().items():
        builder.set_property(key, value)
    return builder.build()


def build_env(parallelism: int, restore: str | None) -> StreamExecutionEnvironment:
    config = Configuration()
    config.set_string("execution.checkpointing.dir", CHECKPOINT_DIR.as_uri())
    # Keep the last checkpoint when the job is cancelled or killed, so it can be
    # restored with --restore (default deletes it on cancellation).
    config.set_string(
        "execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION"
    )
    if restore is not None:
        # Window state and Kafka offsets both come from the checkpoint; --from is
        # ignored for partitions the checkpoint covers (§4.7).
        config.set_string("execution.state-recovery.path", Path(restore).resolve().as_uri())
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(parallelism)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.add_jars(CONNECTOR_JAR.as_uri())
    return env


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="start", default="latest", help="'latest' or ISO time")
    parser.add_argument("--until", default=None, help="ISO time; makes the run bounded")
    parser.add_argument("--sink", choices=["print", "kafka"], default="print")
    parser.add_argument("--parallelism", type=int, default=PARALLELISM)
    parser.add_argument("--restore", default=None, help="checkpoint dir (chk-N) to resume from")
    args = parser.parse_args()

    load_dotenv(FLINK_DIR.parent / ".env")
    env = build_env(args.parallelism, args.restore)

    raw = env.from_source(
        build_source(args.start, args.until),
        WatermarkStrategy.no_watermarks(),  # watermarks come after parsing
        "trades.raw",
    )
    trades = raw.process(ParseTrade(), output_type=TRADE_TYPE).name("parse")

    watermarks = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(OUT_OF_ORDERNESS_SEC))
        .with_idleness(Duration.of_seconds(IDLENESS_SEC))
        .with_timestamp_assigner(TradeTime())
    )
    counts = (
        trades.assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda t: t[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .side_output_late_data(LATE)
        .aggregate(
            CountTrades(),
            window_function=EmitWindow(),
            accumulator_type=Types.LONG(),
            output_type=Types.STRING(),
        )
        .name("count-1m")
    )

    if args.sink == "kafka":
        counts.sink_to(build_sink()).name(SINK_TOPIC)
    else:
        counts.map(lambda r: "COUNT " + r, output_type=Types.STRING()).print()

    # Friday turns these into per-window data-quality metrics.
    trades.get_side_output(MALFORMED).map(
        lambda r: "MALFORMED " + r, output_type=Types.STRING()
    ).print()
    counts.get_side_output(LATE).map(
        lambda t: f"LATE {t[0]} {iso_ms(t[1])}", output_type=Types.STRING()
    ).print()

    env.execute("trade-count-1m")


if __name__ == "__main__":
    main()
