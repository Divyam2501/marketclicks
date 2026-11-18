"""Hello-world: read a bounded slice of trades.raw and count trades per symbol.

Proves the Flink Kafka source can reach Confluent and parse our JSON. Bounded
by Kafka record timestamp (producer CreateTime), run in BATCH mode so only the
final count per symbol is printed.

    cd flink && uv run python jobs/hello_kafka.py 2026-11-13T14:30:00Z 2026-11-13T14:35:00Z
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

FLINK_DIR = Path(__file__).resolve().parent.parent
CONNECTOR_JAR = FLINK_DIR / "jars" / "flink-sql-connector-kafka-5.0.0-2.2.jar"
TOPIC = "trades.raw"
GROUP_ID = "flink-hello"  # never commits: no checkpointing in batch mode

# The SQL connector JAR shades kafka-clients, so the login module lives under
# Flink's shaded package name, not org.apache.kafka.
PLAIN_LOGIN_MODULE = (
    "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule"
)


def to_epoch_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def kafka_source(start_ms: int, end_ms: int) -> KafkaSource:
    jaas = (
        f'{PLAIN_LOGIN_MODULE} required '
        f'username="{os.environ["CONFLUENT_API_KEY"]}" '
        f'password="{os.environ["CONFLUENT_API_SECRET"]}";'
    )
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["CONFLUENT_BOOTSTRAP_SERVERS"])
        .set_topics(TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.timestamp(start_ms))
        .set_bounded(KafkaOffsetsInitializer.timestamp(end_ms))
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("security.protocol", "SASL_SSL")
        .set_property("sasl.mechanism", "PLAIN")
        .set_property("sasl.jaas.config", jaas)
        .build()
    )


def main() -> None:
    start_iso, end_iso = sys.argv[1], sys.argv[2]
    load_dotenv(FLINK_DIR.parent / ".env")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)
    env.add_jars(CONNECTOR_JAR.as_uri())

    source = kafka_source(to_epoch_ms(start_iso), to_epoch_ms(end_iso))
    trades = env.from_source(source, WatermarkStrategy.no_watermarks(), "trades.raw")

    (
        trades.map(
            lambda raw: (json.loads(raw)["symbol"], 1),
            output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
        )
        .key_by(lambda t: t[0])
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
        .print()
    )
    env.execute("hello-kafka")


if __name__ == "__main__":
    main()
