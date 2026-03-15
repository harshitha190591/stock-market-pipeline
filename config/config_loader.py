import os
import yaml

def load_config(config_path: str = None) -> dict:
    """
    Loads config.yaml and returns it as a dictionary.
    All pipeline files import this instead of hardcoding values.
    """
    if config_path is None:
        base = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base, "config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


def get_spark_config(config: dict) -> dict:
    """Returns just the Spark section of config."""
    return config.get("spark", {})


def get_kafka_config(config: dict) -> dict:
    """Returns just the Kafka section of config."""
    return config.get("kafka", {})


def get_paths(config: dict) -> dict:
    """Returns just the paths section of config."""
    return config.get("paths", {})


def get_quality_config(config: dict) -> dict:
    """Returns just the quality thresholds section of config."""
    return config.get("quality", {})


# ── Quick test when run directly ──────────────────────────
if __name__ == "__main__":
    cfg = load_config()
    print("Config loaded successfully!")
    print(f"  Project  : {cfg['project']['name']}")
    print(f"  Stocks   : {cfg['stocks']['symbols']}")
    print(f"  Kafka    : {cfg['kafka']['bootstrap_servers']}")
    print(f"  Schedule : {cfg['airflow']['schedule']}")