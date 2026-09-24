import pyspark.sql
from pyspark.sql.functions import col, trim, upper, lit, initcap, concat_ws


class DATA_PATH_currency:
    """Small path adapter used to load the currency reference dataset."""

    def __init__(self, base_path="/home/jovyan/src/currency"):
        self.base_path = base_path

    @property
    def file_path(self):
        return f"{self.base_path}/country_currency.csv"

    def read(self, spark):
        """Read the currency CSV with its header and inferred schema."""
        return spark.read.csv(self.file_path, header=True, inferSchema=True)


def data_extraction():
    #spark session 

    spark = spark_session()

    DATA_PATH = "/home/jovyan/src/data/azure_tradecorp_raw"   # adapte selon ton volume Docker
    currency_path = DATA_PATH_currency()

# Lecture des fichiers CSV
    df_customers      = spark.read.csv(f"{DATA_PATH}/customers.csv", header=True, inferSchema=True)
    df_orders         = spark.read.csv(f"{DATA_PATH}/orders.csv", header=True, inferSchema=True)
    df_order_details  = spark.read.csv(f"{DATA_PATH}/order_details.csv", header=True, inferSchema=True)
    df_products       = spark.read.csv(f"{DATA_PATH}/products.csv", header=True, inferSchema=True)
    df_categories     = spark.read.csv(f"{DATA_PATH}/categories.csv", header=True, inferSchema=True)
    df_suppliers      = spark.read.csv(f"{DATA_PATH}/suppliers.csv", header=True, inferSchema=True)
    df_employees      = spark.read.csv(f"{DATA_PATH}/employees.csv", header=True, inferSchema=True)
    df_shippers       = spark.read.csv(f"{DATA_PATH}/shippers.csv", header=True, inferSchema=True)
    df_currency       = currency_path.read(spark)

    return df_customers,df_orders,df_order_details,df_products,df_categories,df_suppliers,df_employees,df_shippers, df_currency

def spark_session(app_name="TradeCorp ETL Utils", master=None, configs=None):
    """Return a reusable Spark session for the ETL utilities.

    ``master`` and ``configs`` are optional to support notebooks, tests, and
    command-line jobs without creating multiple Spark contexts.
    """
    builder = (
        pyspark.sql.SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )

    if master:
        builder = builder.master(master)
    for key, value in (configs or {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

def clean_orders(df):
    from pyspark.sql.functions import col, when
    from pyspark.sql.types import DateType, DoubleType
    return (
        df.filter(col("shipped_date").isNotNull())
          .withColumn("order_date", col("order_date").cast(DateType()))
          .withColumn("required_date", col("required_date").cast(DateType()))
          .withColumn("shipped_date", col("shipped_date").cast(DateType()))
          .withColumn("freight", col("freight").cast(DoubleType()))
          .withColumnRenamed("ship_via", "shipper_id")
          .withColumn("is_shipped", when(col("shipped_date").isNotNull(), True).otherwise(False))
    )

# products clean


def clean_products(df_products: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    median_price = df_products.approxQuantile("unit_price",[0.5],0.01)[0]

    return (df_products.fillna({"unit_price": median_price}) \
            .withColumn("en_stock", col("units_in_stock") > 0) \
                .filter((col("units_in_stock") > 0) & (col("discontinued") == 0))
    )

# Order_details clean
def clean_order_details(df):
    from pyspark.sql.functions import col
    from pyspark.sql.types import DoubleType, IntegerType
    return (
        df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
          .withColumn("quantity", col("quantity").cast(IntegerType()))
          .withColumn("discount", col("discount").cast(DoubleType()))
          .withColumnRenamed("unit_price", "prix_unitaire")
          .withColumnRenamed("quantity", "quantite")
    )


def add_sous_total(df):
    from pyspark.sql.functions import col, lit, round
    return df.withColumn(
        "sous_total",
        round(col("prix_unitaire") * col("quantite") * (lit(1) - col("discount")), 2)
    )

def clean_customers(df_customers):
    # TRIM sur toutes les colonnes texte

    df_customers_trimmed = df_customers

    for c, t in df_customers.dtypes:
        if t == "string":
            df_customers_trimmed = df_customers_trimmed.withColumn(c, trim(col(c)))

# Mettre contact_name en Title Case avec initcap()
    df_customers_trimmed = df_customers_trimmed.withColumn( "contact_name", initcap(col("contact_name"))
)
# Mettre country en MAJUSCULES avec upper()
    df_customers_trimmed = df_customers_trimmed.withColumn("country",upper(col("country")))

    df_customers_clean = df_customers_trimmed.dropDuplicates(["customer_id"])
    return df_customers_clean

# Clean employees

#clean employees
def clean_employees(df):
    columns_to_keep = ["employee_id", "first_name", "last_name", "title", "hire_date", "city", "country" ]
    return(
            df.select([column for column in columns_to_keep if column in df.columns])
            .withColumn("full_name", concat_ws(" ", trim(col("first_name")), trim(col("last_name"))))
    )

#clean shippers
def clean_shippers(df):
    """Return the shippers columns used by downstream ETL jobs."""
    required_columns = {"shipper_id", "company_name", "phone"}
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(
            "shippers DataFrame is missing columns: "
            + ", ".join(sorted(missing_columns))
        )

    return (
        df.select("shipper_id", "company_name", "phone")
        .withColumn("company_name", trim(col("company_name")))
        .withColumn("phone", trim(col("phone")))
        .dropDuplicates(["shipper_id"])
    )

#clean categories

def clean_categories(df):
    """Return categories with the columns required by downstream ETL jobs."""
    columns_to_keep: list[str] = ["category_id", "category_name", "description", "picture"]
    missing_columns = set(columns_to_keep).difference(df.columns)
    if missing_columns:
        raise ValueError(
            "categories DataFrame is missing columns: "
            + ", ".join(sorted(missing_columns))
        )

    cleaned = df.select(*columns_to_keep)
    for column_name, data_type in cleaned.dtypes:
        if data_type == "string":
            cleaned = cleaned.withColumn(column_name, trim(col(column_name)))

    return cleaned.dropDuplicates(["category_id"])

# clean suppliers

def clean_suppliers(df):
    """Return a cleaned supplier DataFrame for downstream ETL jobs."""
    columns_to_keep = [
        "supplier_id", "company_name", "contact_name", "contact_title",
        "address", "city", "region", "postal_code", "country", "phone",
        "fax", "homepage",
    ]
    missing_columns = set(columns_to_keep).difference(df.columns)
    if missing_columns:
        raise ValueError(
            "suppliers DataFrame is missing columns: "
            + ", ".join(sorted(missing_columns))
        )

    cleaned = df.select(*columns_to_keep)
    for column_name, data_type in cleaned.dtypes:
        if data_type == "string":
            cleaned = cleaned.withColumn(column_name, trim(col(column_name)))

    return (
        cleaned
        .withColumn("company_name", initcap(col("company_name")))
        .withColumn("contact_name", initcap(col("contact_name")))
        .withColumn("country", upper(col("country")))
        .dropDuplicates(["supplier_id"])
    )
#clean currency
def clean_currency(df):
    """Normalize the currency reference data for downstream joins."""
    from pyspark.sql.functions import when

    cleaned = df
    for column_name, data_type in df.dtypes:
        if data_type == "string":
            cleaned = cleaned.withColumn(
                column_name,
                trim(col(column_name)),
            ).withColumn(
                column_name,
                when(col(column_name) == "", lit(None)).otherwise(col(column_name)),
            )

    return cleaned.dropDuplicates()



def write_bronze(df, table_name, output_path, mode="overwrite"):
    """Write a cleaned DataFrame to <output_path>/<table_name> as Parquet."""
    (
        df.write
        .mode(mode)
        .parquet(f"{output_path}/{table_name}")
    )


if __name__=="__main__":

    df_customers, df_orders, df_order_details, df_products, df_categories, df_suppliers, df_employees, df_shippers, df_currency = data_extraction()

    PATH = "/home/jovyan/data/bronze"

    # Appliquer le nettoyage à chaque table
    tables = {
        "customers":     clean_customers(df_customers),
        "orders":        clean_orders(df_orders),
        "order_details": clean_order_details(df_order_details),
        "products":      clean_products(df_products),
        "categories":    clean_categories(df_categories),
        "suppliers":     clean_suppliers(df_suppliers),
        "employees":     clean_employees(df_employees),
        "shippers":      clean_shippers(df_shippers),
        "currency":      clean_currency(df_currency),
    }

    # Écrire chaque table nettoyée en Parquet dans le dossier bronze
    for table_name, df_clean in tables.items():
        write_bronze(df_clean, table_name, PATH)
        print(f"[bronze] {table_name} → {PATH}/{table_name}")
