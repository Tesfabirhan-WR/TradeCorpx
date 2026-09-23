"""
run-test-customer.py

Tests unitaires pour utils.clean_customers().
Lance avec: pytest run-test-customer.py -v
"""
# /home/jovyan/tests/conftest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src")) 

import pytest  # pyright: ignore[reportMissingImports]
from pyspark.sql import SparkSession

from utils import clean_customers


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test-clean-customers")
        .master("local[2]")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def raw_customers(spark):
    columns = ["customer_id", "contact_name", "country", "city"]
    data = [
        ("C001", "  jean dupont  ", "  france ", "Lyon"),
        ("C002", "marie curie", "FRANCE", "Paris"),
        ("C003", "  JOHN smith", "united kingdom", "London"),
        # Duplicate customer_id → should be deduplicated by clean_customers
        ("C001", "jean dupont", "france", "Lyon"),
    ]
    return spark.createDataFrame(data, columns)


def test_trims_whitespace(spark, raw_customers):
    result = clean_customers(raw_customers).collect()
    for row in result:
        assert row["contact_name"] == row["contact_name"].strip()
        assert row["country"] == row["country"].strip()


def test_contact_name_is_title_case(spark, raw_customers):
    result = {row["customer_id"]: row["contact_name"] for row in clean_customers(raw_customers).collect()}
    assert result["C001"] == "Jean Dupont"
    assert result["C002"] == "Marie Curie"
    assert result["C003"] == "John Smith"


def test_country_is_uppercase(spark, raw_customers):
    result = {row["customer_id"]: row["country"] for row in clean_customers(raw_customers).collect()}
    assert result["C001"] == "FRANCE"
    assert result["C002"] == "FRANCE"
    assert result["C003"] == "UNITED KINGDOM"


def test_deduplicates_on_customer_id(spark, raw_customers):
    result = clean_customers(raw_customers).collect()
    customer_ids = [row["customer_id"] for row in result]
    assert len(customer_ids) == len(set(customer_ids)), "customer_id should be unique after cleaning"
    assert customer_ids.count("C001") == 1


def test_output_row_count(spark, raw_customers):
    # 4 input rows, 1 duplicate C001 → 3 distinct customers expected
    result = clean_customers(raw_customers)
    assert result.count() == 3


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))