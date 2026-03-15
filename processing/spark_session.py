from pyspark.sql import SparkSession
from config.config_loader import load_config, get_spark_config


def get_spark_session(app_name: str = None) -> SparkSession:
    """
    Creates and returns a shared SparkSession.
    All pipeline files call this instead of building Spark themselves.
    This ensures consistent config across the whole pipeline.
    """
    config      = load_config()
    spark_cfg   = get_spark_config(config)

    name = app_name or spark_cfg.get("app_name", "StockMarketPipeline")

    spark = (
        SparkSession.builder
        .appName(name)
        .master(spark_cfg.get("master", "local[*]"))
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.shuffle.partitions",
                str(spark_cfg.get("shuffle_partitions", 4)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ── Quick test ────────────────────────────────────────────
if __name__ == "__main__":
    spark = get_spark_session()
    print(f"Spark session created: {spark.version}")
    spark.stop()