import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from config.config_loader import load_config, get_kafka_config, get_paths
from processing.spark_session import get_spark_session


# ── Schema for incoming Kafka messages ────────────────────
STOCK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("date",   StringType()),
    StructField("open",   StringType()),
    StructField("high",   StringType()),
    StructField("low",    StringType()),
    StructField("close",  StringType()),
    StructField("volume", StringType()),
])


def get_consumer(kafka_cfg: dict) -> KafkaConsumer:
    """
    Creates and returns a Kafka consumer.
    value_deserializer converts JSON bytes back to Python dict.
    """
    return KafkaConsumer(
        kafka_cfg["topic"],
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=kafka_cfg["group_id"],
        auto_offset_reset=kafka_cfg["auto_offset_reset"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,   # stop after 10s of no messages
    )


def consume_to_bronze(max_messages: int = None):
    """
    Consumes messages from Kafka topic and writes
    them to the Bronze Delta Lake table.

    Process:
    1. Read messages from Kafka in batches
    2. Convert batch to Spark DataFrame
    3. Add audit metadata columns
    4. Append to Bronze Delta table
    """
    config    = load_config()
    kafka_cfg = get_kafka_config(config)
    paths     = get_paths(config)
    bronze    = paths["bronze"]
    spark     = get_spark_session("StockConsumer")

    consumer  = get_consumer(kafka_cfg)
    batch     = []
    total     = 0
    batch_num = 0

    print(f"\nStarting Kafka consumer ← topic: {kafka_cfg['topic']}")
    print("Waiting for messages...\n")

    for message in consumer:
        batch.append(message.value)
        total += 1

        # write every 500 messages as a micro-batch
        if len(batch) >= 500:
            batch_num += 1
            _write_batch(spark, batch, bronze, batch_num)
            batch = []

        if max_messages and total >= max_messages:
            break

    # write any remaining messages
    if batch:
        batch_num += 1
        _write_batch(spark, batch, bronze, batch_num)

    consumer.close()
    print(f"\nConsumer complete — {total} messages written to Bronze")
    spark.stop()


def _write_batch(spark: SparkSession,
                 batch: list,
                 bronze_path: str,
                 batch_num: int):
    """
    Converts a list of message dicts to a Spark DataFrame
    and appends to the Bronze Delta table.
    """
    df = spark.createDataFrame(batch, schema=STOCK_SCHEMA)

    # add audit metadata
    df = (
        df
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_batch_number", F.lit(batch_num))
        .withColumn("_source",       F.lit("kafka"))
    )

    out = f"{bronze_path}/stock_prices"
    df.write.format("delta").mode("append").save(out)
    print(f"  Batch {batch_num}: {df.count()} rows → {out}")


if __name__ == "__main__":
    consume_to_bronze()