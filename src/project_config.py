"""Shared paths and Spark-session settings for the project scripts."""

import os
from pathlib import Path

from pyspark.sql import SparkSession


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPOSITORY_ROOT / "401-project"
DATA_DIR = PROJECT_DIR / "data"
REPORTS_DIR = PROJECT_DIR / "reports"


def data_path(*parts: str) -> str:
    return str(DATA_DIR.joinpath(*parts))


def reports_path(*parts: str) -> str:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return str(REPORTS_DIR.joinpath(*parts))


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "8g"))
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
