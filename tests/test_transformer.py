import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from transformer import build_data_enriched

# /home/jovyan/tests/conftest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src")) 


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
        {"customer_id": "CUST1", "company_name": "ACME Corp", "country": "France", "city": "Paris"}
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
            "is_shipped": True,
        }
    ]
    df_orders = spark.createDataFrame(orders_data)

    # 3. Order Details (already pre-cleaned with prix_unitaire, quantite, discount)
    order_details_data = [
        {
            "order_id": 1001,
            "product_id": 10,
            "prix_unitaire": 20.0,
            "quantite": 2,
            "discount": 0.0,
        }
    ]
    df_order_details = spark.createDataFrame(order_details_data)

    # 4. Products
    products_data = [
        {"product_id": 10, "product_name": "Widget A", "category_id": 1, "en_stock": 50}
    ]
    df_products = spark.createDataFrame(products_data)

    # 5. Categories
    categories_data = [
        {"category_id": 1, "category_name": "Electronics"}
    ]
    df_categories = spark.createDataFrame(categories_data)

    # 6. Employees
    employees_data = [
        {"employee_id": 5, "full_name": "John Doe"}
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

    # Expected Output Schema and Data
    expected_data = [
        {
            "order_id": 1001,
            "customer_id": "CUST1",
            "employee_id": 5,
            "product_id": 10,
            "order_date": "2026-01-01",
            "required_date": "2026-01-10",
            "shipped_date": "2026-01-05",
            "freight": 15.5,
            "is_shipped": True,
            "prix_unitaire": 20.0,
            "quantite": 2,
            "discount": 0.0,
            "sous_total": 40.0,  # Calculated via add_sous_total (20.0 * 2)
            "customer_name": "ACME Corp",
            "customer_country": "France",
            "customer_city": "Paris",
            "product_name": "Widget A",
            "category_name": "Electronics",
            "en_stock": 50,
            "full_name": "John Doe",
            "shipper_name": "Express Delivery",
        }
    ]
    df_expected = spark.createDataFrame(expected_data)

    # PySpark 3.5+ equality assertion (compares both content and schema)
    assertDataFrameEqual(df_result, df_expected)


def test_build_data_enriched_schema_columns(sample_bronze_dfs):
    """Ensures that all 21 expected output columns exist in the output DataFrame."""

    df_result = build_data_enriched(sample_bronze_dfs)

    expected_columns = {
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
    }

    assert set(df_result.columns) == expected_columns