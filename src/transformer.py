from pyspark.sql import DataFrame

from utils import (
    clean_customers,
    clean_orders,
    add_sous_total,
    clean_employees,
    clean_products
)


def build_data_enriched(dataframes: dict) -> DataFrame:

    # 1. Nettoyage individuel
    df_customers = clean_customers(dataframes["customers"]) \
        .withColumnRenamed("company_name", "customer_name") \
        .withColumnRenamed("country", "customer_country") \
        .withColumnRenamed("city", "customer_city")
        
    df_orders = clean_orders(dataframes["orders"])
    
    df_order_details = add_sous_total(dataframes["order_details"])
    
    df_employees = clean_employees(dataframes["employees"])
    
    df_products = clean_products(dataframes["products"])

    df_categories = dataframes["categories"]
    df_shippers = dataframes["shippers"] \
        .withColumnRenamed("company_name", "shipper_name")

    # 2. Produits enrichis
    df_products_enriched = df_products.join(
        df_categories.select("category_id", "category_name"),
        on="category_id",
        how="left"
    )

    # 3. Jointure globale
    df_orders_enriched = (
        df_order_details
        .join(df_orders, on="order_id", how="inner")
        .join(df_customers, on="customer_id", how="left")
        .join(df_products_enriched, on="product_id", how="left")
        .join(df_employees, on="employee_id", how="left")
        .join(df_shippers, on="shipper_id", how="left")
    )

    # 4. Sélection finale
    df_full_final = df_orders_enriched.select(
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
        "shipper_name"
    )

    return df_full_final

if __name__ == "__main__":
    import traceback
    from pyspark.sql import SparkSession
    from reader import DataReader
    from CurrencyEnrichment import CountryCurrencyTransformer

    spark = SparkSession.builder \
        .appName("TradeCorpTransformer") \
        .getOrCreate()

    try:
        bronze_path = "/home/jovyan/src/data/bronze"
        table_names = [
            "customers", "orders", "order_details", "products",
            "categories", "suppliers", "employees", "shippers", "currency",
        ]
        dfs = DataReader.load_bronze_data(spark, bronze_path, table_names)

        df_enriched = build_data_enriched(dfs)

        currency_transformer = CountryCurrencyTransformer(spark)
        df_final = currency_transformer.enrich(df_enriched, country_column="customer_country")

        output_dir_path = "/home/jovyan/src/data/transformed_tradecorp_data"
        
        # Action trigger to force immediate evaluation and surface hidden runtime errors
        print(f"Row count to write: {df_final.count()}")
        df_final.write.mode("overwrite").parquet(output_dir_path)
        print("Pipeline finished successfully.")

    except Exception as e:
        print("\n--- ERROR DETECTED IN PYSPARK PIPELINE ---")
        traceback.print_exc()

    finally:
        spark.stop()