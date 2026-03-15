import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F
from config.config_loader import load_config, get_paths
from processing.spark_session import get_spark_session


def register_tables(spark, paths):
    """
    Registers Delta tables as Spark SQL temp views
    so we can query them with plain SQL.
    """
    tables = {
        "stock_prices_silver": f"{paths['silver']}/stock_prices",
        "stock_summary":       f"{paths['gold']}/stock_summary",
        "top_movers":          f"{paths['gold']}/top_movers",
        "volume_analysis":     f"{paths['gold']}/volume_analysis",
    }
    for name, path in tables.items():
        spark.read.format("delta").load(path) \
             .createOrReplaceTempView(name)
        print(f"  Registered: {name}")


def run_dq_checks(spark):
    """
    Runs all SQL data quality checks.
    Returns True if all pass, False if any fail.
    """
    print("\n── SQL Data Quality Checks ──────────────────────")

    checks = [
        ("null_symbols",
         "SELECT COUNT(*) as cnt FROM stock_prices_silver WHERE symbol IS NULL"),
        ("null_dates",
         "SELECT COUNT(*) as cnt FROM stock_prices_silver WHERE date IS NULL"),
        ("negative_prices",
         "SELECT COUNT(*) as cnt FROM stock_prices_silver WHERE open<=0 OR high<=0 OR low<=0 OR close<=0"),
        ("high_lt_low",
         "SELECT COUNT(*) as cnt FROM stock_prices_silver WHERE high < low"),
        ("duplicates",
         "SELECT COUNT(*) as cnt FROM (SELECT symbol, date, COUNT(*) c FROM stock_prices_silver GROUP BY symbol, date HAVING c > 1)"),
        ("low_row_count",
         "SELECT COUNT(*) as cnt FROM (SELECT symbol, COUNT(*) c FROM stock_prices_silver GROUP BY symbol HAVING c < 100)"),
    ]

    all_passed = True
    for check_name, query in checks:
        result = spark.sql(query).collect()[0]["cnt"]
        status = "PASS" if result == 0 else "FAIL"
        if result > 0:
            all_passed = False
        print(f"  [{status}] {check_name:<20} fail_count={result}")

    return all_passed


def run_views(spark):
    """
    Creates analytical SQL views and runs sample queries.
    """
    print("\n── Creating SQL Views ───────────────────────────")

    # ── View 1: Stock summary with signals ────────────────
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_stock_summary AS
        SELECT
            symbol, date, open, high, low, close, volume,
            ROUND(daily_return_pct, 2)  AS daily_return_pct,
            ROUND(ma_7d,  2)            AS ma_7d,
            ROUND(ma_30d, 2)            AS ma_30d,
            ROUND(ma_90d, 2)            AS ma_90d,
            CASE
                WHEN close > ma_30d THEN 'ABOVE'
                WHEN close < ma_30d THEN 'BELOW'
                ELSE 'AT'
            END                         AS ma30_signal,
            CASE
                WHEN daily_return_pct >=  3 THEN 'STRONG GAIN'
                WHEN daily_return_pct >=  1 THEN 'GAIN'
                WHEN daily_return_pct <= -3 THEN 'STRONG LOSS'
                WHEN daily_return_pct <= -1 THEN 'LOSS'
                ELSE 'FLAT'
            END                         AS performance_label,
            price_spike_flag
        FROM stock_summary
    """)
    print("  Created: vw_stock_summary")

    # ── View 2: Top movers ────────────────────────────────
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_top_movers AS
        SELECT date, symbol, direction,
               ROUND(daily_return_pct, 2) AS daily_return_pct,
               ROUND(close, 2) AS close, volume, rank
        FROM top_movers WHERE rank <= 5
        ORDER BY date DESC, direction, rank
    """)
    print("  Created: vw_top_movers")

    # ── View 3: Volume spikes ─────────────────────────────
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_volume_spikes AS
        SELECT symbol, date, volume,
               ROUND(avg_30d_volume, 0)          AS avg_30d_volume,
               ROUND(volume / avg_30d_volume, 2) AS volume_ratio,
               ROUND(daily_return_pct, 2)        AS daily_return_pct,
               close
        FROM volume_analysis
        ORDER BY volume_ratio DESC
    """)
    print("  Created: vw_volume_spikes")


def run_sample_queries(spark):
    """Runs sample analytical queries to validate the views."""
    print("\n── Sample Query Results ─────────────────────────")

    print("\n  Top 5 best single-day gains (AAPL):")
    spark.sql("""
        SELECT date, close, daily_return_pct, performance_label
        FROM vw_stock_summary
        WHERE symbol = 'AAPL'
        ORDER BY daily_return_pct DESC
        LIMIT 5
    """).show()

    print("  Moving average signals per symbol (latest date):")
    spark.sql("""
        SELECT symbol, MAX(date) as latest_date,
               LAST(ma30_signal) as ma30_signal,
               LAST(performance_label) as last_performance
        FROM vw_stock_summary
        GROUP BY symbol
        ORDER BY symbol
    """).show()

    print("  Top 5 volume spikes:")
    spark.sql("""
        SELECT symbol, date, volume_ratio, daily_return_pct
        FROM vw_volume_spikes
        LIMIT 5
    """).show()


def run_sql_layer():
    config = load_config()
    paths  = get_paths(config)
    spark  = get_spark_session("SQLLayer")

    print("\nRegistering Delta tables as SQL views...")
    register_tables(spark, paths)

    passed = run_dq_checks(spark)
    if not passed:
        raise Exception("SQL quality checks failed — Gold blocked!")

    run_views(spark)
    run_sample_queries(spark)

    print("\nSQL layer complete!")
    spark.stop()


if __name__ == "__main__":
    run_sql_layer()