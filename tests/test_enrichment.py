import os
import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

#path

# /home/jovyan/tests/conftest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src")) 


import CurrencyEnrichment
from CurrencyEnrichment import CountryCurrencyTransformer


@pytest.fixture(scope="session")
def spark():
    """Initializes a local SparkSession for pytest."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-currency-enrichment")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def mock_currency_csv():
    """Creates a temporary CSV file mimicking country_currency.csv."""
    csv_content = (
        "country,currency_code,currency_name\n"
        "France,EUR,Euro\n"
        "USA,USD,US Dollar\n"
        "UK,GBP,British Pound\n"
    )
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".csv") as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    yield tmp_path

    # Cleanup temporary file after test run
    if os.path.exists(tmp_path):
        os.remove(tmp_path)


def test_currency_enrichment_mapping(spark, mock_currency_csv):
    """Verifies that known countries are correctly enriched with currency info."""
    
    # 1. Create test input DataFrame
    input_data = [
        {"order_id": 1, "customer_country": "France"},
        {"order_id": 2, "customer_country": "USA"},
        {"order_id": 3, "customer_country": "UK"},
    ]
    df_input = spark.createDataFrame(input_data)

    # 2. Instantiate transformer pointing to mock CSV
    transformer = CountryCurrencyTransformer(spark, csv_path=mock_currency_csv)
    df_result = transformer.enrich(df_input, country_column="customer_country")

    # 3. Define Expected Output
    expected_data = [
        {"order_id": 1, "customer_country": "France", "currency_code": "EUR", "currency_name": "Euro"},
        {"order_id": 2, "customer_country": "USA", "currency_code": "USD", "currency_name": "US Dollar"},
        {"order_id": 3, "customer_country": "UK", "currency_code": "GBP", "currency_name": "British Pound"},
    ]
    df_expected = spark.createDataFrame(expected_data)

    # Assert equality (PySpark 3.5+)
    assertDataFrameEqual(df_result, df_expected)


def test_currency_enrichment_unknown_country(spark, mock_currency_csv):
    """Verifies behavior when input contains NULL or non-matching countries."""
    
    input_data = [
        {"order_id": 1, "customer_country": "France"},
        {"order_id": 2, "customer_country": "Atlantis"},  # Unknown country
        {"order_id": 3, "customer_country": None},        # Null country
    ]
    df_input = spark.createDataFrame(input_data)

    transformer = CountryCurrencyTransformer(spark, csv_path=mock_currency_csv)
    df_result = transformer.enrich(df_input, country_column="customer_country")

    # Unknown/Null countries should result in NULL currency fields (Left Join behavior)
    expected_data = [
        {"order_id": 1, "customer_country": "France", "currency_code": "EUR", "currency_name": "Euro"},
        {"order_id": 2, "customer_country": "Atlantis", "currency_code": None, "currency_name": None},
        {"order_id": 3, "customer_country": None, "currency_code": None, "currency_name": None},
    ]
    df_expected = spark.createDataFrame(expected_data)

    assertDataFrameEqual(df_result, df_expected)


def test_currency_enrichment_columns_preserved(spark, mock_currency_csv):
    """Ensures existing columns are kept and currency columns are appended."""
    
    input_data = [
        {"order_id": 1001, "customer_country": "France", "amount": 250.0}
    ]
    df_input = spark.createDataFrame(input_data)

    transformer = CountryCurrencyTransformer(spark, csv_path=mock_currency_csv)
    df_result = transformer.enrich(df_input, country_column="customer_country")

    expected_cols = {"order_id", "customer_country", "amount", "currency_code", "currency_name"}
    assert set(df_result.columns) == expected_cols