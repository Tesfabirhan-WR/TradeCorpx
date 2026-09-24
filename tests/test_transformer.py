from datetime import date
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from transformer import build_data_enriched


EXPECTED_COLUMNS = [
    "order_id",
    "customer_id",
    "employee_id",
    "product_id",
    "order_date",
    "required_date",
    "shipped_date",
    "freight",
    "is_shipped",
    "prix_unitaire",
    "quantite",
    "discount",
    "sous_total",
    "customer_name",
    "customer_country",
    "customer_city",
    "product_name",
    "category_name",
    "en_stock",
    "full_name",
    "shipper_name",
]


@pytest.fixture(scope="session")
def spark():
    """Initializes a local SparkSession for pytest."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-transformers")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_bronze_dfs(spark):
    """Provides mock Bronze DataFrames representing input tables."""
    
    # 1. Customers
    customers_data = [
        {"customer_id": "CUST1", "company_name": "ACME Corp", "contact_name": "Jane Doe", "country": "France", "city": "Paris"}
    ]
    df_customers = spark.createDataFrame(customers_data)

    # 2. Orders
    orders_data = [
        {
            "order_id": 1001,
            "customer_id": "CUST1",
            "employee_id": 5,
            "shipper_id": 2,
            "order_date": "2026-01-01",
            "required_date": "2026-01-10",
            "shipped_date": "2026-01-05",
            "freight": 15.5,
        },
        {
            "order_id": 1002,
            "customer_id": "CUST1",
            "employee_id": 5,
            "shipper_id": 2,
            "order_date": "2026-01-02",
            "required_date": "2026-01-11",
            "shipped_date": None,
            "freight": 5.0,
        },
    ]
    df_orders = spark.createDataFrame(orders_data)

    # 3. Order details (already in the Bronze column names consumed by the transformer)
    order_details_data = [
        {
            "order_id": 1001,
            "product_id": 10,
            "prix_unitaire": 20.0,
            "quantite": 2,
            "discount": 0.0,
        },
        {
            "order_id": 1001,
            "product_id": 99,
            "prix_unitaire": 10.0,
            "quantite": 2,
            "discount": 0.25,
        },
        {
            "order_id": 1002,
            "product_id": 10,
            "prix_unitaire": 20.0,
            "quantite": 1,
            "discount": 0.0,
        },
    ]
    df_order_details = spark.createDataFrame(order_details_data)

    # 4. Products
    products_data = [
        {"product_id": 10, "product_name": "Widget A", "category_id": 1, "unit_price": 20.0, "units_in_stock": 50, "discontinued": 0}
    ]
    df_products = spark.createDataFrame(products_data)

    # 5. Categories
    categories_data = [
        {"category_id": 1, "category_name": "Electronics"}
    ]
    df_categories = spark.createDataFrame(categories_data)

    # 6. Employees
    employees_data = [
        {"employee_id": 5, "first_name": "John", "last_name": "Doe"}
    ]
    df_employees = spark.createDataFrame(employees_data)

    # 7. Shippers
    shippers_data = [
        {"shipper_id": 2, "company_name": "Express Delivery"}
    ]
    df_shippers = spark.createDataFrame(shippers_data)

    return {
        "customers": df_customers,
        "orders": df_orders,
        "order_details": df_order_details,
        "products": df_products,
        "categories": df_categories,
        "employees": df_employees,
        "shippers": df_shippers,
    }


def test_build_data_enriched(spark, sample_bronze_dfs):
    """Verifies that build_data_enriched correctly joins, cleans, and projects data."""

    # Execute transformation
    df_result = build_data_enriched(sample_bronze_dfs)

    # Unshipped orders are removed by clean_orders. An unknown product remains
    # because the product join is a left join.
    expected_data = [
        {
            "order_id": 1001,
            "customer_id": "CUST1",
            "employee_id": 5,
            "product_id": 10,
            "order_date": date(2026, 1, 1),
            "required_date": date(2026, 1, 10),
            "shipped_date": date(2026, 1, 5),
            "freight": 15.5,
            "is_shipped": True,
            "prix_unitaire": 20.0,
            "quantite": 2,
            "discount": 0.0,
            "sous_total": 40.0,  # Calculated via add_sous_total (20.0 * 2)
            "customer_name": "ACME Corp",
            "customer_country": "FRANCE",
            "customer_city": "Paris",
            "product_name": "Widget A",
            "category_name": "Electronics",
            "en_stock": True,
            "full_name": "John Doe",
            "shipper_name": "Express Delivery",
        },
        {
            "order_id": 1001,
            "customer_id": "CUST1",
            "employee_id": 5,
            "product_id": 99,
            "order_date": date(2026, 1, 1),
            "required_date": date(2026, 1, 10),
            "shipped_date": date(2026, 1, 5),
            "freight": 15.5,
            "is_shipped": True,
            "prix_unitaire": 10.0,
            "quantite": 2,
            "discount": 0.25,
            "sous_total": 15.0,
            "customer_name": "ACME Corp",
            "customer_country": "FRANCE",
            "customer_city": "Paris",
            "product_name": None,
            "category_name": None,
            "en_stock": None,
            "full_name": "John Doe",
            "shipper_name": "Express Delivery",
        },
    ]
    df_expected = spark.createDataFrame(expected_data).select(*EXPECTED_COLUMNS)

    assert df_result.columns == EXPECTED_COLUMNS
    assert [(field.name, field.dataType) for field in df_result.schema] == [
        (field.name, field.dataType) for field in df_expected.schema
    ]
    assert sorted(df_result.collect(), key=lambda row: row.product_id) == sorted(
        df_expected.collect(), key=lambda row: row.product_id
    )


def test_build_data_enriched_schema_columns(sample_bronze_dfs):
    """Ensures that all 21 expected output columns exist in the output DataFrame."""

    df_result = build_data_enriched(sample_bronze_dfs)

    assert df_result.columns == EXPECTED_COLUMNS


def test_build_data_enriched_rejects_missing_tables(sample_bronze_dfs):
    del sample_bronze_dfs["orders"]
    with pytest.raises(ValueError, match="Missing Bronze tables: orders"):
        build_data_enriched(sample_bronze_dfs)
