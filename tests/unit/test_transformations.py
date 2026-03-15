import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import functions as F
from processing.transformations import (
    cast_schema,
    add_daily_return,
    add_price_range,
    filter_valid_prices,
    deduplicate,
    add_price_spike_flag,
    add_moving_average,
)


class TestCastSchema:
    """Tests for cast_schema() — verifies type casting works correctly."""

    def test_close_is_double(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        assert dict(df.dtypes)["close"] == "double"

    def test_volume_is_long(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        assert dict(df.dtypes)["volume"] == "bigint"

    def test_date_is_date_type(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        assert dict(df.dtypes)["date"] == "date"

    def test_no_rows_lost_on_cast(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        assert df.count() == sample_stock_df.count()


class TestDailyReturn:
    """Tests for add_daily_return() — verifies percentage return calculation."""

    def test_column_exists(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        df = add_daily_return(df)
        assert "daily_return_pct" in df.columns

    def test_positive_return_when_close_above_open(self, sample_stock_df):
        """AAPL 2020-01-02: open=296.24, close=300.35 → positive return"""
        df = cast_schema(sample_stock_df)
        df = add_daily_return(df)
        row = df.filter(
            (F.col("symbol") == "AAPL") & (F.col("date") == "2020-01-02")
        ).collect()[0]
        assert row["daily_return_pct"] > 0

    def test_negative_return_when_close_below_open(self, sample_stock_df):
        """AAPL 2020-01-07: open=299.84, close=298.39 → negative return"""
        df = cast_schema(sample_stock_df)
        df = add_daily_return(df)
        row = df.filter(
            (F.col("symbol") == "AAPL") & (F.col("date") == "2020-01-07")
        ).collect()[0]
        assert row["daily_return_pct"] < 0


class TestPriceRange:
    """Tests for add_price_range() — verifies high-low range calculation."""

    def test_column_exists(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        df = add_price_range(df)
        assert "price_range" in df.columns

    def test_range_is_high_minus_low(self, sample_stock_df):
        """AAPL 2020-01-02: high=300.60, low=295.19 → range=5.41"""
        df = cast_schema(sample_stock_df)
        df = add_price_range(df)
        row = df.filter(
            (F.col("symbol") == "AAPL") & (F.col("date") == "2020-01-02")
        ).collect()[0]
        expected = round(300.60 - 295.19, 4)
        assert abs(row["price_range"] - expected) < 0.01

    def test_range_always_positive(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        df = add_price_range(df)
        negative = df.filter(F.col("price_range") < 0).count()
        assert negative == 0


class TestFilterValidPrices:
    """Tests for filter_valid_prices() — verifies bad rows are removed."""

    def test_removes_negative_prices(self, dirty_stock_df):
        df = cast_schema(dirty_stock_df)
        df = filter_valid_prices(df)
        negative = df.filter(F.col("open") <= 0).count()
        assert negative == 0

    def test_removes_high_lt_low(self, dirty_stock_df):
        df = cast_schema(dirty_stock_df)
        df = filter_valid_prices(df)
        invalid = df.filter(F.col("high") < F.col("low")).count()
        assert invalid == 0

    def test_valid_rows_kept(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        before = df.count()
        df = filter_valid_prices(df)
        assert df.count() == before  # no valid rows should be removed


class TestDeduplicate:
    """Tests for deduplicate() — verifies duplicate removal."""

    def test_removes_duplicates(self, dirty_stock_df):
        df = cast_schema(dirty_stock_df)
        df = deduplicate(df, ["symbol", "date"])
        # count symbol+date combinations
        dupes = df.groupBy("symbol", "date").count() \
                  .filter(F.col("count") > 1).count()
        assert dupes == 0

    def test_unique_rows_kept(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        before = df.count()
        df = deduplicate(df, ["symbol", "date"])
        assert df.count() == before  # no dupes in clean data


class TestMovingAverage:
    """Tests for add_moving_average() — verifies rolling average calculation."""

    def test_column_created(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        df = add_moving_average(df, 3)
        assert "ma_3d" in df.columns

    def test_ma_not_null_after_window(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        df = add_moving_average(df, 3)
        # after 3 rows, MA should be populated
        nulls = df.filter(F.col("ma_3d").isNull()).count()
        assert nulls == 0

    def test_ma_per_symbol(self, sample_stock_df):
        """MA should be calculated separately for each symbol."""
        df = cast_schema(sample_stock_df)
        df = add_moving_average(df, 3)
        symbols = df.select("symbol").distinct().count()
        assert symbols == 2  # AAPL and TSLA


class TestPriceSpikeFlag:
    """Tests for add_price_spike_flag() — verifies spike detection."""

    def test_column_exists(self, sample_stock_df):
        df = cast_schema(sample_stock_df)
        df = add_daily_return(df)
        df = add_price_spike_flag(df, 0.50)
        assert "price_spike_flag" in df.columns

    def test_normal_moves_not_flagged(self, sample_stock_df):
        """Normal daily moves should not be flagged as spikes."""
        df = cast_schema(sample_stock_df)
        df = add_daily_return(df)
        df = add_price_spike_flag(df, 0.50)
        spikes = df.filter(F.col("price_spike_flag") == True).count()
        assert spikes == 0  # no 50%+ moves in our sample data