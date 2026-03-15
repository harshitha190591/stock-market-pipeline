import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date


# ── Shared Spark session for all tests ────────────────────
# scope="session" means one Spark session for ALL tests
# much faster than creating a new one per test
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("StockPipelineTests")
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ── Sample stock data for unit tests ──────────────────────
@pytest.fixture
def sample_stock_df(spark):
    """
    Creates a small realistic stock DataFrame for testing.
    Uses known values so we can assert exact results.
    """
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("date",   StringType()),
        StructField("open",   StringType()),
        StructField("high",   StringType()),
        StructField("low",    StringType()),
        StructField("close",  StringType()),
        StructField("volume", StringType()),
    ])
    data = [
        ("AAPL", "2020-01-02", "296.24", "300.60", "295.19", "300.35", "33870100"),
        ("AAPL", "2020-01-03", "297.15", "300.58", "296.50", "297.43", "36580700"),
        ("AAPL", "2020-01-06", "293.79", "299.96", "292.75", "299.80", "29596800"),
        ("AAPL", "2020-01-07", "299.84", "300.90", "297.48", "298.39", "27218000"),
        ("AAPL", "2020-01-08", "297.16", "304.44", "296.20", "303.19", "33019800"),
        ("TSLA", "2020-01-02", "84.90",  "86.14",  "84.34",  "86.05",  "47660500"),
        ("TSLA", "2020-01-03", "88.10",  "90.80",  "87.38",  "88.60",  "88892500"),
        ("TSLA", "2020-01-06", "88.09",  "90.31",  "88.00",  "90.31",  "50665800"),
    ]
    return spark.createDataFrame(data, schema=schema)


# ── Sample data with intentional quality issues ────────────
@pytest.fixture
def dirty_stock_df(spark):
    """
    DataFrame with known quality issues for testing
    our validation and cleaning functions.
    """
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("date",   StringType()),
        StructField("open",   StringType()),
        StructField("high",   StringType()),
        StructField("low",    StringType()),
        StructField("close",  StringType()),
        StructField("volume", StringType()),
    ])
    data = [
        ("AAPL", "2020-01-02", "296.24", "300.60", "295.19", "300.35", "33870100"),
        ("AAPL", "2020-01-02", "296.24", "300.60", "295.19", "300.35", "33870100"),  # duplicate
        ("AAPL", "2020-01-03", "-10.0",  "300.58", "296.50", "297.43", "36580700"),  # negative price
        ("AAPL", "2020-01-06", "293.79", "290.00", "292.75", "299.80", "29596800"),  # high < low
        (None,   "2020-01-07", "299.84", "300.90", "297.48", "298.39", "27218000"),  # null symbol
    ]
    return spark.createDataFrame(data, schema=schema)