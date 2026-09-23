import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(os.getenv("AIRFLOW_HOME", "/home/jovyan")).resolve()
SRC_DIR = BASE_DIR / "src"
PYTHON_BIN = "/opt/conda/bin/python"

DOWNLOAD_SCRIPT = "reader.py"
BRONZE_SCRIPT = "bronze_ingestion.py"
SILVER_SCRIPT = "pipeline.py"

RAW_DIR = SRC_DIR / "data" / "azure_tradecorp_raw"
CURRENCY_FILE = SRC_DIR / "data" / "currency" / "country_currency.csv"
SILVER_DIR = SRC_DIR / "data" / "silver"

EXPECTED_RAW_FILES = [
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order_details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv",
]


# ---------------------------------------------------------------------------
# Validation Functions
# ---------------------------------------------------------------------------
def check_raw_files() -> None:
    """Fails the run if a source CSV is missing or empty."""
    expected = [RAW_DIR / name for name in EXPECTED_RAW_FILES] + [CURRENCY_FILE]
    missing_or_empty = []

    logger.info("Verifying raw ingestion files at path: %s", RAW_DIR)

    for path in expected:
        if not path.is_file():
            logger.error("Missing file: %s", path)
            missing_or_empty.append(f"{path.name} (Missing)")
        elif path.stat().st_size == 0:
            logger.error("Empty file detected: %s", path)
            missing_or_empty.append(f"{path.name} (0 bytes)")
        else:
            logger.info("Found valid raw file: %s (%d bytes)", path.name, path.stat().st_size)

    if missing_or_empty:
        raise FileNotFoundError(f"Validation failed for raw file(s): {', '.join(missing_or_empty)}")


def check_silver_output() -> None:
    """Fails the run if Spark did not finish writing the Silver table."""
    silver_table = SILVER_DIR / "orders_enriched"
    marker = silver_table / "_SUCCESS"

    logger.info("Verifying Silver partition completeness at: %s", silver_table)

    if not marker.is_file():
        raise FileNotFoundError(
            f"Silver layer write check failed! '_SUCCESS' marker missing at {marker}"
        )

    logger.info("Silver write verified successfully! Marker present at %s", marker)


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "tradecorp_data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="tradecorp_etl",
    description="Azure raw CSVs -> Bronze Parquet -> Silver orders_enriched",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # Shared folder overwrite safety
    tags=["tradecorp", "pyspark", "etl", "silver"],
) as dag:

    extract_from_azure = BashOperator(
        task_id="extract_from_azure",
        bash_command=f"cd {SRC_DIR.as_posix()} && {PYTHON_BIN} {DOWNLOAD_SCRIPT}",
        doc_md="Downloads raw CSV datasets from Azure Blob Storage into local raw directory.",
    )

    check_raw = PythonOperator(
        task_id="check_raw_files",
        python_callable=check_raw_files,
        doc_md="Validates that all expected CSVs and lookup files exist and are not empty.",
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"cd {SRC_DIR.as_posix()} && {PYTHON_BIN} {BRONZE_SCRIPT}",
        doc_md="Converts raw CSV inputs to Parquet in the Bronze storage layer.",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=f"cd {SRC_DIR.as_posix()} && {PYTHON_BIN} {SILVER_SCRIPT}",
        doc_md="Joins tables, applies transformations, enriches with currency, and writes to Silver.",
    )

    check_silver = PythonOperator(
        task_id="check_silver_output",
        python_callable=check_silver_output,
        doc_md="Ensures Spark completed writing the Silver dataset by checking for _SUCCESS.",
    )

    # Task Pipeline Pipeline Dependency Graph
    extract_from_azure >> check_raw >> ingest_bronze >> build_silver >> check_silver