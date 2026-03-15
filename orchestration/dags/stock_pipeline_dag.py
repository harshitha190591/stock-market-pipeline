import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from config.config_loader import load_config

config  = load_config()
af_cfg  = config["airflow"]

# ── Default args ──────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          af_cfg["retries"],
    "retry_delay":      timedelta(minutes=af_cfg["retry_delay"]),
    "email_on_failure": False,
    "sla":              timedelta(hours=af_cfg["sla_hours"]),
}

# ── SLA miss callback ─────────────────────────────────────
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA BREACH — DAG: {dag.dag_id} | Tasks: {task_list}")


# ═══════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ═══════════════════════════════════════════════════════════
def task_run_producer(**ctx):
    from ingestion.kafka_producer import run_producer
    print(f"[{ctx['ts']}] Starting Kafka producer...")
    run_producer()


def task_run_consumer(**ctx):
    from ingestion.kafka_consumer import consume_to_bronze
    print(f"[{ctx['ts']}] Starting Kafka consumer...")
    consume_to_bronze()


def task_sql_quality_checks(**ctx):
    """Runs SQL DQ checks — blocks Silver if any fail."""
    from processing.spark_session import get_spark_session
    from config.config_loader import load_config, get_paths
    from pyspark.sql import functions as F

    config = load_config()
    paths  = get_paths(config)
    spark  = get_spark_session("DQChecks")

    df = spark.read.format("delta").load(f"{paths['bronze']}/stock_prices")
    df.createOrReplaceTempView("stock_prices_silver")

    checks = [
        ("null_symbols",    "SELECT COUNT(*) as cnt FROM stock_prices_silver WHERE symbol IS NULL"),
        ("negative_prices", "SELECT COUNT(*) as cnt FROM stock_prices_silver WHERE open<=0 OR close<=0"),
        ("duplicates",      "SELECT COUNT(*) as cnt FROM (SELECT symbol,date,COUNT(*) c FROM stock_prices_silver GROUP BY symbol,date HAVING c>1)"),
    ]

    failed = []
    for name, query in checks:
        result = spark.sql(query).collect()[0]["cnt"]
        status = "PASS" if result == 0 else "FAIL"
        print(f"  [{status}] {name}")
        if result > 0:
            failed.append(name)

    spark.stop()
    if failed:
        raise ValueError(f"DQ checks failed: {failed}")
    print("All DQ checks passed!")


def task_bronze_to_silver(**ctx):
    from processing.bronze_to_silver import run_bronze_to_silver
    print(f"[{ctx['ts']}] Running Bronze → Silver...")
    run_bronze_to_silver()


def task_silver_to_gold(**ctx):
    from processing.silver_to_gold import run_silver_to_gold
    print(f"[{ctx['ts']}] Running Silver → Gold...")
    run_silver_to_gold()


def task_run_sql_layer(**ctx):
    from sql.run_sql import run_sql_layer
    print(f"[{ctx['ts']}] Running SQL views and checks...")
    run_sql_layer()


def task_run_tests(**ctx):
    """Runs pytest integration tests as final validation."""
    import subprocess
    result = subprocess.run(
        ["python3", "-m", "pytest", "tests/integration/", "-v", "--tb=short"],
        capture_output=True, text=True,
        cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Integration tests failed!\n{result.stderr}")
    print("All integration tests passed!")


def task_log_completion(**ctx):
    print(f"Stock pipeline complete for {ctx['ds']}")
    print(f"  DAG    : {ctx['dag'].dag_id}")
    print(f"  Run ID : {ctx['run_id']}")


# ═══════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════
with DAG(
    dag_id="stock_market_pipeline",
    description="Real-time NYSE/NASDAQ stock pipeline — Kafka → Bronze → Silver → Gold → SQL",
    default_args=default_args,
    schedule_interval=af_cfg["schedule"],  # weekdays at 18:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stocks", "kafka", "delta-lake", "real-time"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    start          = EmptyOperator(task_id="start")

    produce        = PythonOperator(
        task_id="kafka_producer",
        python_callable=task_run_producer,
    )
    consume        = PythonOperator(
        task_id="kafka_consumer",
        python_callable=task_run_consumer,
    )
    dq_checks      = PythonOperator(
        task_id="sql_quality_checks",
        python_callable=task_sql_quality_checks,
    )
    bronze_silver  = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=task_bronze_to_silver,
    )
    silver_gold    = PythonOperator(
        task_id="silver_to_gold",
        python_callable=task_silver_to_gold,
    )
    sql_layer      = PythonOperator(
        task_id="sql_layer",
        python_callable=task_run_sql_layer,
    )
    run_tests      = PythonOperator(
        task_id="integration_tests",
        python_callable=task_run_tests,
    )
    complete       = PythonOperator(
        task_id="log_completion",
        python_callable=task_log_completion,
    )

    end            = EmptyOperator(task_id="end")

    # ── Pipeline dependency chain ─────────────────────────
    (
        start
        >> produce
        >> consume
        >> dq_checks
        >> bronze_silver
        >> silver_gold
        >> sql_layer
        >> run_tests
        >> complete
        >> end
    )