import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from config.config_loader import load_config, get_paths, get_quality_config
from processing.spark_session import get_spark_session
from processing.transformations import (
    cast_schema,
    add_daily_return,
    add_price_range,
    filter_valid_prices,
    deduplicate,
    add_price_spike_flag,
)


def run_bronze_to_silver():
    """
    Reads raw Bronze Delta table and produces a clean Silver table.

    Steps:
    1. Read Bronze Delta table
    2. Cast all columns to correct types
    3. Filter invalid prices
    4. Deduplicate on symbol + date
    5. Add derived columns (daily return, price range)
    6. Flag suspicious price spikes
    7. Write to Silver Delta table
    """
    config     = load_config()
    paths      = get_paths(config)
    quality    = get_quality_config(config)
    spark      = get_spark_session("BronzeToSilver")

    bronze_path = f"{paths['bronze']}/stock_prices"
    silver_path = f"{paths['silver']}/stock_prices"

    print("\n── Bronze → Silver ──────────────────────────────")
    print(f"Reading from : {bronze_path}")

    # ── Step 1: Read Bronze ───────────────────────────────
    df = spark.read.format("delta").load(bronze_path)
    print(f"Bronze rows  : {df.count()}")

    # ── Step 2: Cast schema ───────────────────────────────
    df = cast_schema(df)

    # ── Step 3: Filter invalid prices ─────────────────────
    df = filter_valid_prices(df)

    # ── Step 4: Deduplicate ───────────────────────────────
    df = deduplicate(df, ["symbol", "date"])
    print(f"After dedup  : {df.count()}")

    # ── Step 5: Add derived columns ───────────────────────
    df = add_daily_return(df)
    df = add_price_range(df)

    # ── Step 6: Flag price spikes ─────────────────────────
    df = add_price_spike_flag(df, quality["max_price_spike"])

    # ── Step 7: Add audit columns ─────────────────────────
    df = (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_layer",        F.lit("silver"))
    )

    # ── Step 8: Write Silver ──────────────────────────────
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("symbol")     # partition by symbol for faster queries
        .save(silver_path)
    )

    print(f"Silver rows  : {df.count()}")
    print(f"Written to   : {silver_path}")
    print("── Bronze → Silver complete ─────────────────────\n")

    spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()