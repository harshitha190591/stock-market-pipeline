import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import functions as F
from config.config_loader import load_config, get_paths


class TestBronzeLayer:
    """Integration tests for the Bronze Delta table."""

    def test_bronze_table_exists(self, spark):
        config = load_config()
        paths  = get_paths(config)
        path   = f"{paths['bronze']}/stock_prices"
        assert os.path.exists(path), f"Bronze table not found at {path}"

    def test_bronze_has_rows(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df = spark.read.format("delta").load(f"{paths['bronze']}/stock_prices")
        assert df.count() > 0

    def test_bronze_has_audit_columns(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df = spark.read.format("delta").load(f"{paths['bronze']}/stock_prices")
        assert "_ingested_at"  in df.columns
        assert "_batch_number" in df.columns
        assert "_source"       in df.columns

    def test_bronze_source_is_kafka(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df = spark.read.format("delta").load(f"{paths['bronze']}/stock_prices")
        non_kafka = df.filter(F.col("_source") != "kafka").count()
        assert non_kafka == 0


class TestSilverLayer:
    """Integration tests for the Silver Delta table."""

    def test_silver_table_exists(self, spark):
        config = load_config()
        paths  = get_paths(config)
        path   = f"{paths['silver']}/stock_prices"
        assert os.path.exists(path), f"Silver table not found at {path}"

    def test_silver_has_rows(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df = spark.read.format("delta").load(f"{paths['silver']}/stock_prices")
        assert df.count() > 0

    def test_silver_no_null_symbols(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df     = spark.read.format("delta").load(f"{paths['silver']}/stock_prices")
        nulls  = df.filter(F.col("symbol").isNull()).count()
        assert nulls == 0

    def test_silver_no_negative_prices(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df     = spark.read.format("delta").load(f"{paths['silver']}/stock_prices")
        negs   = df.filter(
            (F.col("open")  <= 0) | (F.col("high") <= 0) |
            (F.col("low")   <= 0) | (F.col("close") <= 0)
        ).count()
        assert negs == 0

    def test_silver_no_duplicates(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df     = spark.read.format("delta").load(f"{paths['silver']}/stock_prices")
        dupes  = df.groupBy("symbol", "date").count() \
                   .filter(F.col("count") > 1).count()
        assert dupes == 0

    def test_silver_has_derived_columns(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df     = spark.read.format("delta").load(f"{paths['silver']}/stock_prices")
        assert "daily_return_pct" in df.columns
        assert "price_range"      in df.columns
        assert "price_spike_flag" in df.columns

    def test_silver_row_count_less_than_bronze(self, spark):
        """Silver should have fewer or equal rows than Bronze after dedup."""
        config = load_config()
        paths  = get_paths(config)
        bronze = spark.read.format("delta").load(f"{paths['bronze']}/stock_prices").count()
        silver = spark.read.format("delta").load(f"{paths['silver']}/stock_prices").count()
        assert silver <= bronze

    def test_all_symbols_present(self, spark):
        config  = load_config()
        paths   = get_paths(config)
        symbols = config["stocks"]["symbols"]
        df      = spark.read.format("delta").load(f"{paths['silver']}/stock_prices")
        found   = [r["symbol"] for r in df.select("symbol").distinct().collect()]
        for symbol in symbols:
            assert symbol in found, f"{symbol} missing from Silver table"


class TestGoldLayer:
    """Integration tests for the Gold Delta tables."""

    def test_stock_summary_exists(self, spark):
        config = load_config()
        paths  = get_paths(config)
        path   = f"{paths['gold']}/stock_summary"
        assert os.path.exists(path)

    def test_top_movers_exists(self, spark):
        config = load_config()
        paths  = get_paths(config)
        path   = f"{paths['gold']}/top_movers"
        assert os.path.exists(path)

    def test_volume_analysis_exists(self, spark):
        config = load_config()
        paths  = get_paths(config)
        path   = f"{paths['gold']}/volume_analysis"
        assert os.path.exists(path)

    def test_stock_summary_has_moving_averages(self, spark):
        config = load_config()
        paths  = get_paths(config)
        df     = spark.read.format("delta").load(f"{paths['gold']}/stock_summary")
        assert "ma_7d"  in df.columns
        assert "ma_30d" in df.columns
        assert "ma_90d" in df.columns

    def test_gold_row_count_matches_silver(self, spark):
        """stock_summary should have same row count as Silver."""
        config = load_config()
        paths  = get_paths(config)
        silver = spark.read.format("delta").load(f"{paths['silver']}/stock_prices").count()
        gold   = spark.read.format("delta").load(f"{paths['gold']}/stock_summary").count()
        assert gold == silver