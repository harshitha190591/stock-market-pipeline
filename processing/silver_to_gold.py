import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config.config_loader import load_config, get_paths
from processing.spark_session import get_spark_session
from processing.transformations import add_moving_average, add_volume_spike_flag


def run_silver_to_gold():
    """
    Reads Silver Delta table and produces three Gold tables:

    1. stock_summary     — daily OHLCV with moving averages
    2. top_movers        — top 10 best and worst daily performers
    3. volume_analysis   — days with unusual volume spikes
    """
    config  = load_config()
    paths   = get_paths(config)
    windows = config["pipeline"]["moving_avg_windows"]
    top_n   = config["pipeline"]["top_movers_count"]
    spark   = get_spark_session("SilverToGold")

    silver_path = f"{paths['silver']}/stock_prices"
    gold_path   = paths["gold"]

    print("\n── Silver → Gold ────────────────────────────────")
    df = spark.read.format("delta").load(silver_path)
    print(f"Silver rows  : {df.count()}")

    # ── Gold 1: Stock summary with moving averages ────────
    df_ma = df
    for window in windows:
        df_ma = add_moving_average(df_ma, window)

    stock_summary = (
        df_ma
        .select(
            "symbol", "date", "open", "high", "low",
            "close", "volume", "daily_return_pct",
            "price_range", "price_spike_flag",
            *[f"ma_{w}d" for w in windows],
            "_processed_at"
        )
        .orderBy("symbol", "date")
    )
    stock_summary.write.format("delta").mode("overwrite") \
        .partitionBy("symbol").save(f"{gold_path}/stock_summary")
    print(f"stock_summary       : {stock_summary.count()} rows")

    # ── Gold 2: Top movers per day ────────────────────────
    top_movers = (
        df
        .filter(F.col("price_spike_flag") == False)   # exclude bad data
        .withColumn("direction",
            F.when(F.col("daily_return_pct") > 0, "GAINER")
             .otherwise("LOSER"))
        .withColumn("abs_return",
            F.abs(F.col("daily_return_pct")))
        .withColumn("rank",
            F.dense_rank().over(
                Window.partitionBy("date", "direction")
                .orderBy(F.col("abs_return").desc())
            )
        )
        .filter(F.col("rank") <= top_n)
        .select("date", "symbol", "direction",
                "daily_return_pct", "abs_return",
                "close", "volume", "rank")
        .orderBy("date", "direction", "rank")
    )
    top_movers.write.format("delta").mode("overwrite") \
        .save(f"{gold_path}/top_movers")
    print(f"top_movers          : {top_movers.count()} rows")

    # ── Gold 3: Volume analysis ───────────────────────────
    df_vol = add_volume_spike_flag(df)
    volume_analysis = (
        df_vol
        .filter(F.col("volume_spike") == True)
        .select("symbol", "date", "volume",
                "avg_30d_volume", "close",
                "daily_return_pct", "volume_spike")
        .orderBy("date", "symbol")
    )
    volume_analysis.write.format("delta").mode("overwrite") \
        .save(f"{gold_path}/volume_analysis")
    print(f"volume_analysis     : {volume_analysis.count()} rows")

    print(f"\nAll Gold tables written to: {gold_path}")
    print("── Silver → Gold complete ───────────────────────\n")
    spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()