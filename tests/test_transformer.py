from datetime import date
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transformer import build_data_enriched


EXPECTED_COLUMNS = [
    "order_id", "customer_id", "employee_id", "product_id", "order_date",
    "required_date", "shipped_date", "freight", "is_shipped", "prix_unitaire",
    "quantite", "discount", "sous_total", "customer_name", "customer_country",
    "customer_city", "product_name", "category_name", "en_stock", "full_name",
    "shipper_name",
]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-transformers")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_bronze_dfs(spark):
    customers = spark.createDataFrame([
        {"customer_id": "CUST1", "company_name": "ACME Corp", "contact_name": "Jane Doe", "country": "France", "city": "Paris"}
    ])
    orders = spark.createDataFrame([
        {"order_id": 1001, "customer_id": "CUST1", "employee_id": 5, "shipper_id": 2, "order_date": "2026-01-01", "required_date": "2026-01-10", "shipped_date": "2026-01-05", "freight": 15.5},
        {"order_id": 1002, "customer_id": "CUST1", "employee_id": 5, "shipper_id": 2, "order_date": "2026-01-02", "required_date": "2026-01-11", "shipped_date": None, "freight": 5.0},
    ])
    order_details = spark.createDataFrame([
        {"order_id": 1001, "product_id": 10, "prix_unitaire": 20.0, "quantite": 2, "discount": 0.0},
        {"order_id": 1001, "product_id": 99, "prix_unitaire": 10.0, "quantite": 2, "discount": 0.25},
        {"order_id": 1002, "product_id": 10, "prix_unitaire": 20.0, "quantite": 1, "discount": 0.0},
    ])
    products = spark.createDataFrame([
        {"product_id": 10, "product_name": "Widget A", "category_id": 1, "unit_price": 20.0, "units_in_stock": 50, "discontinued": 0}
    ])
    categories = spark.createDataFrame([{"category_id": 1, "category_name": "Electronics"}])
    employees = spark.createDataFrame([{"employee_id": 5, "first_name": "John", "last_name": "Doe"}])
    shippers = spark.createDataFrame([{"shipper_id": 2, "company_name": "Express Delivery"}])
    return {
        "customers": customers, "orders": orders, "order_details": order_details,
        "products": products, "categories": categories, "employees": employees,
        "shippers": shippers,
    }


def test_build_data_enriched(spark, sample_bronze_dfs):
    result = build_data_enriched(sample_bronze_dfs)
    expected = spark.createDataFrame([
        {"order_id": 1001, "customer_id": "CUST1", "employee_id": 5, "product_id": 10, "order_date": date(2026, 1, 1), "required_date": date(2026, 1, 10), "shipped_date": date(2026, 1, 5), "freight": 15.5, "is_shipped": True, "prix_unitaire": 20.0, "quantite": 2, "discount": 0.0, "sous_total": 40.0, "customer_name": "ACME Corp", "customer_country": "FRANCE", "customer_city": "Paris", "product_name": "Widget A", "category_name": "Electronics", "en_stock": True, "full_name": "John Doe", "shipper_name": "Express Delivery"},
        {"order_id": 1001, "customer_id": "CUST1", "employee_id": 5, "product_id": 99, "order_date": date(2026, 1, 1), "required_date": date(2026, 1, 10), "shipped_date": date(2026, 1, 5), "freight": 15.5, "is_shipped": True, "prix_unitaire": 10.0, "quantite": 2, "discount": 0.25, "sous_total": 15.0, "customer_name": "ACME Corp", "customer_country": "FRANCE", "customer_city": "Paris", "product_name": None, "category_name": None, "en_stock": None, "full_name": "John Doe", "shipper_name": "Express Delivery"},
    ]).select(*EXPECTED_COLUMNS)
    assert result.columns == EXPECTED_COLUMNS
    assert [(field.name, field.dataType) for field in result.schema] == [(field.name, field.dataType) for field in expected.schema]
    assert sorted(result.collect(), key=lambda row: row.product_id) == sorted(expected.collect(), key=lambda row: row.product_id)


def test_build_data_enriched_schema_columns(sample_bronze_dfs):
    assert build_data_enriched(sample_bronze_dfs).columns == EXPECTED_COLUMNS


def test_build_data_enriched_rejects_missing_tables(sample_bronze_dfs):
    del sample_bronze_dfs["orders"]
    with pytest.raises(ValueError, match="Missing Bronze tables: orders"):
        build_data_enriched(sample_bronze_dfs)