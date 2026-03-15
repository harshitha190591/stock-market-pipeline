# Stock Market Pipeline

A real-time NYSE/NASDAQ stock market data pipeline built with Kafka, PySpark, Delta Lake, Apache Airflow, and SQL. Designed with production-style modular architecture — one file per responsibility.

---

## Architecture

```
Kaggle Stock CSVs (AAPL, TSLA, MSFT, AMZN, GOGL)
           │
           ▼
┌─────────────────┐
│  Kafka Producer │  Streams CSV rows to Kafka topic
└─────────────────┘
           │
           ▼
┌─────────────────┐
│  Kafka Consumer │  Reads from Kafka → writes to Bronze Delta
└─────────────────┘
           │
           ▼
┌─────────────────┐
│   SQL DQ Gate   │  Blocks pipeline if quality checks fail
└─────────────────┘
           │
           ▼
┌─────────────────┐
│     Silver      │  Clean, deduplicated, typed, enriched
└─────────────────┘
           │
           ▼
┌─────────────────┐
│      Gold       │  stock_summary · top_movers · volume_analysis
└─────────────────┘
           │
           ▼
┌─────────────────┐
│   SQL Views     │  Analytical views · moving avg · signals
└─────────────────┘
           │
           ▼
┌─────────────────┐
│ Airflow DAG     │  Orchestrates full pipeline · weekdays 18:00
└─────────────────┘
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Streaming | Apache Kafka |
| Processing | PySpark, Delta Lake |
| Orchestration | Apache Airflow |
| SQL Layer | Spark SQL, SQL views, stored procedures |
| Testing | pytest (unit + integration) |
| Config | YAML-driven, no hardcoded values |
| CI/CD | GitHub Actions |

---

## Project Structure

```
stock-market-pipeline/
├── config/
│   ├── config.yaml          # all settings — paths, Kafka, thresholds
│   └── config_loader.py     # loads config, used by all modules
├── ingestion/
│   ├── kafka_producer.py    # streams CSV rows to Kafka topic
│   └── kafka_consumer.py    # reads Kafka, writes to Bronze Delta
├── processing/
│   ├── spark_session.py     # shared SparkSession factory
│   ├── transformations.py   # reusable PySpark functions
│   ├── bronze_to_silver.py  # clean, validate, deduplicate
│   └── silver_to_gold.py    # aggregations, moving averages
├── sql/
│   ├── views/               # analytical SQL views
│   ├── procedures/          # moving average stored procedures
│   ├── quality/             # SQL data quality checks
│   └── run_sql.py           # executes SQL layer via Spark
├── tests/
│   ├── conftest.py          # shared pytest fixtures
│   ├── unit/                # unit tests per transformation function
│   └── integration/         # end-to-end pipeline tests
└── orchestration/
    └── dags/                # Airflow DAG
```

---

## Key Features

### 1. Real-Time Kafka Streaming
- Producer reads Kaggle stock CSV files and streams row by row to Kafka
- Consumer reads in micro-batches and writes to Bronze Delta table
- Simulates a live market data feed

### 2. Modular PySpark Processing
- `transformations.py` — reusable functions: moving averages, daily returns, volume spikes, price spike detection
- `bronze_to_silver.py` — schema casting, deduplication, validation, derived columns
- `silver_to_gold.py` — three Gold tables: stock summary, top movers, volume analysis

### 3. SQL Analytical Layer
- Views: `vw_stock_summary`, `vw_top_movers`, `vw_volume_spikes`
- Moving average crossover signals (Golden Cross / Death Cross)
- Performance labels: STRONG GAIN, GAIN, FLAT, LOSS, STRONG LOSS

### 4. Full pytest Suite
- Unit tests for every transformation function
- Integration tests for Bronze, Silver and Gold layers
- Shared fixtures via `conftest.py`

### 5. Config-Driven Architecture
- All settings in `config/config.yaml`
- No hardcoded values anywhere in the codebase
- Easy to add new stocks or change thresholds

### 6. Airflow Orchestration
- Runs weekdays at 18:00 (after market close)
- Full chain: Kafka → DQ checks → Silver → Gold → SQL → Tests
- SLA monitoring and retry logic

---

## Getting Started

### Prerequisites
- Python 3.12
- Java 17 (for Kafka and PySpark)
- Apache Kafka

### Setup

```bash
git clone https://github.com/harshitha190591/stock-market-pipeline.git
cd stock-market-pipeline
python3 -m venv venv
source venv/bin/activate
pip install pyspark==3.5.0 delta-spark==3.1.0 kafka-python pytest pyyaml
```

### Start Kafka

```bash
kafka-storage format --standalone -t $(kafka-storage random-uuid) \
  -c /opt/homebrew/etc/kafka/server.properties
kafka-server-start /opt/homebrew/etc/kafka/server.properties
kafka-topics --create --topic stock_prices \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Run Pipeline

```bash
# stream data to Kafka
python3 -m ingestion.kafka_producer

# consume to Bronze Delta
python3 -m ingestion.kafka_consumer

# Bronze → Silver → Gold
python3 -m processing.bronze_to_silver
python3 -m processing.silver_to_gold

# SQL views and quality checks
python3 -m sql.run_sql

# run tests
pytest tests/ -v
```

---

## Gold Tables

| Table | Description |
|---|---|
| `stock_summary` | Daily OHLCV with 7/30/90-day moving averages |
| `top_movers` | Top 10 daily gainers and losers |
| `volume_analysis` | Days with 2x+ average volume spikes |

---

## Author

**Harshitha Shetty** — Senior Data Engineer
[LinkedIn](https://linkedin.com/in/harshitha-shetty) | [GitHub](https://github.com/harshitha190591)