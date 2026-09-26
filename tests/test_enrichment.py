import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import CurrencyEnrichment


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("pytest-currency-enrichment").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def currency_csv():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", encoding="utf-8", delete=False) as temporary_file:
        temporary_file.write("country,currency\nFrance,EUR\nUSA,USD\nUK,GBP\n")
        path = temporary_file.name
    try:
        yield path
    finally:
        Path(path).unlink(missing_ok=True)


def test_enriches_known_countries_and_preserves_input_columns(spark: Any, currency_csv: str):
    data = [
        {"order_id": 1, "customer_country": "FRANCE", "amount": 125.5},
        {"order_id": 2, "customer_country": "USA", "amount": 250.0},
        {"order_id": 3, "customer_country": "UK", "amount": 75.0},
    ]
    result = CurrencyEnrichment.CountryCurrencyTransformer(spark, currency_csv_path=currency_csv).enrich(spark.createDataFrame(data))
    assert result.columns == ["amount", "customer_country", "order_id", "currency"]
    assert {row.order_id: row.currency for row in result.collect()} == {1: "EUR", 2: "USD", 3: "GBP"}


def test_unknown_or_null_country_keeps_row_with_null_currency(spark: Any, currency_csv: str):
    data = [{"order_id": 1, "customer_country": "FRANCE"}, {"order_id": 2, "customer_country": "ATLANTIS"}, {"order_id": 3, "customer_country": None}]
    result = CurrencyEnrichment.CountryCurrencyTransformer(spark, currency_csv_path=currency_csv).enrich(spark.createDataFrame(data))
    assert result.count() == 3
    assert {row.order_id: row.currency for row in result.collect()} == {1: "EUR", 2: None, 3: None}


def test_reuses_the_loaded_reference_dataframe(spark: Any, currency_csv: str):
    transformer = CurrencyEnrichment.CountryCurrencyTransformer(spark, currency_csv_path=currency_csv)
    first_reference = transformer.load_currency()
    assert first_reference is transformer.load_currency()
    assert first_reference.columns == ["country", "currency"]


def test_rejects_a_missing_country_column(spark: Any, currency_csv: str):
    transformer = CurrencyEnrichment.CountryCurrencyTransformer(spark, currency_csv_path=currency_csv)
    with pytest.raises(ValueError, match="'customer_country' column not found"):
        transformer.enrich(spark.createDataFrame([{"order_id": 1}]))


def test_rejects_an_invalid_reference_file(spark: Any):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", encoding="utf-8", delete=False) as temporary_file:
        temporary_file.write("country,currency_code\nFRANCE,EUR\n")
        path = temporary_file.name
    try:
        transformer = CurrencyEnrichment.CountryCurrencyTransformer(spark, currency_csv_path=path)
        with pytest.raises(ValueError, match="missing columns: currency"):
            transformer.load_currency()
    finally:
        Path(path).unlink(missing_ok=True)
        
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
