from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def cast_schema(df: DataFrame) -> DataFrame:
    """
    Casts all columns to correct types.
    Raw Kafka data comes in as strings — this fixes that.
    """
    return (
        df
        .withColumn("date",   F.to_date("date", "yyyy-MM-dd"))
        .withColumn("open",   F.col("open").cast("double"))
        .withColumn("high",   F.col("high").cast("double"))
        .withColumn("low",    F.col("low").cast("double"))
        .withColumn("close",  F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("long"))
    )


def add_daily_return(df: DataFrame) -> DataFrame:
    """
    Calculates daily percentage return for each stock.
    Formula: (close - open) / open * 100
    Example: open=100, close=105 → return = 5.0%
    """
    return df.withColumn(
        "daily_return_pct",
        F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4)
    )


def add_price_range(df: DataFrame) -> DataFrame:
    """
    Calculates intraday price range.
    Formula: high - low
    Shows how volatile the stock was on a given day.
    """
    return df.withColumn(
        "price_range",
        F.round(F.col("high") - F.col("low"), 4)
    )


def add_moving_average(df: DataFrame, window_days: int) -> DataFrame:
    """
    Calculates rolling moving average of close price.
    Partitioned by symbol so each stock gets its own moving average.
    Window: looks back window_days rows ordered by date.
    Example: 7-day MA smooths out daily noise to show the trend.
    """
    col_name = f"ma_{window_days}d"
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-window_days + 1, 0)
    )
    return df.withColumn(
        col_name,
        F.round(F.avg("close").over(w), 4)
    )


def add_volume_spike_flag(df: DataFrame) -> DataFrame:
    """
    Flags days where volume is 2x the 30-day average.
    High volume often signals important market events.
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-29, 0)
    )
    return (
        df
        .withColumn("avg_30d_volume", F.avg("volume").over(w))
        .withColumn(
            "volume_spike",
            F.when(F.col("volume") > F.col("avg_30d_volume") * 2, True)
             .otherwise(False)
        )
    )


def add_price_spike_flag(df: DataFrame, max_pct: float = 0.50) -> DataFrame:
    """
    Flags days where price moved more than max_pct in one day.
    Used in quality checks — a 50%+ move is likely bad data.
    """
    return df.withColumn(
        "price_spike_flag",
        F.when(F.abs(F.col("daily_return_pct")) > max_pct * 100, True)
         .otherwise(False)
    )


def deduplicate(df: DataFrame, key_cols: list) -> DataFrame:
    """
    Removes duplicate rows based on key columns.
    Keeps the first occurrence.
    """
    return df.dropDuplicates(key_cols)


def filter_valid_prices(df: DataFrame) -> DataFrame:
    """
    Removes rows with invalid price values.
    Prices must be positive — zero or negative prices are bad data.
    """
    return (
        df
        .filter(F.col("open")   > 0)
        .filter(F.col("high")   > 0)
        .filter(F.col("low")    > 0)
        .filter(F.col("close")  > 0)
        .filter(F.col("volume") >= 0)
        .filter(F.col("high")   >= F.col("low"))  # high must be >= low
    )