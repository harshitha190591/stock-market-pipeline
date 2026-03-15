import csv
import json
import time
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka import KafkaProducer
from config.config_loader import load_config, get_kafka_config

def get_producer(kafka_cfg: dict) -> KafkaProducer:
    """
    Creates and returns a Kafka producer.
    value_serializer converts Python dict to JSON bytes
    so Kafka can transmit it over the network.
    """
    return KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",           # wait for all brokers to confirm
        retries=3,            # retry 3 times on failure
    )


def stream_stock_file(producer: KafkaProducer,
                      symbol: str,
                      file_path: str,
                      topic: str,
                      batch_size: int = 100,
                      delay: float = 0.01):
    """
    Reads a stock CSV file row by row and sends each row
    to a Kafka topic — simulating a real-time market feed.

    Each message contains:
    - symbol: stock ticker (AAPL, TSLA etc.)
    - date, open, high, low, close, volume
    """
    if not os.path.exists(file_path):
        print(f"  File not found: {file_path}")
        return 0

    count = 0
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = {
                "symbol":   symbol,
                "date":     row.get("Date", row.get("date", "")),
                "open":     row.get("Open", row.get("open", 0)),
                "high":     row.get("High", row.get("high", 0)),
                "low":      row.get("Low",  row.get("low",  0)),
                "close":    row.get("Close", row.get("close", 0)),
                "volume":   row.get("Volume", row.get("volume", 0)),
            }
            producer.send(topic, value=message)
            count += 1

            # flush every batch_size messages
            if count % batch_size == 0:
                producer.flush()
                print(f"  [{symbol}] Sent {count} rows...")
                time.sleep(delay)

    producer.flush()
    print(f"  [{symbol}] Done — {count} total rows sent")
    return count


def run_producer():
    """
    Main entry point — loads config and streams
    all configured stock files to Kafka.
    """
    config     = load_config()
    kafka_cfg  = get_kafka_config(config)
    topic      = kafka_cfg["topic"]
    batch_size = config["pipeline"]["batch_size"]
    raw_files  = config["stocks"]["raw_files"]

    print(f"\nStarting Kafka producer → topic: {topic}")
    print(f"Bootstrap servers: {kafka_cfg['bootstrap_servers']}\n")

    producer   = get_producer(kafka_cfg)
    total      = 0

    for symbol, file_path in raw_files.items():
        print(f"Streaming {symbol}...")
        count  = stream_stock_file(
            producer, symbol, file_path, topic, batch_size
        )
        total += count

    producer.close()
    print(f"\nProducer complete — {total} total messages sent to '{topic}'")


if __name__ == "__main__":
    run_producer()