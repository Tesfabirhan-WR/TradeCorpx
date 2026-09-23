"""
currency_enrichment.py

Reads the country -> currency reference CSV and enriches an orders_enriched
DataFrame with a "currency" column, joined on country.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, upper

logger = logging.getLogger(__name__)

# Local folder for the currency reference data
CURRENCY_DIR = "/home/jovyan/src/currency"
CURRENCY_CSV_PATH = f"{CURRENCY_DIR}/country_currency.csv"


class CountryCurrencyTransformer:
    """Enriches a DataFrame with a currency code based on a country column."""

    def __init__(self, spark: SparkSession, currency_csv_path: str = CURRENCY_CSV_PATH):
        self.spark = spark
        self.currency_csv_path = currency_csv_path
        self._df_currency: DataFrame | None = None

    def load_currency(self) -> DataFrame:
        """Reads and cleans the country/currency reference CSV (cached after the first call)."""
        if self._df_currency is None:
            df = self.spark.read.csv(self.currency_csv_path, header=True, inferSchema=True)

            required_columns = {"country", "currency"}
            missing_columns = required_columns.difference(df.columns)
            if missing_columns:
                raise ValueError(
                    "country_currency.csv is missing columns: "
                    + ", ".join(sorted(missing_columns))
                )

            self._df_currency = (
                df
                .withColumn("country", upper(trim(col("country"))))
                .withColumn("currency", trim(col("currency")))
                .dropDuplicates(["country"])
            )
            logger.info(
                "Loaded %d country/currency rows from %s",
                self._df_currency.count(),
                self.currency_csv_path,
            )
        return self._df_currency

    def enrich(self, df: DataFrame, country_column: str = "customer_country") -> DataFrame:
        """Left-joins df to the currency reference on country_column, adding a "currency" column.

        Countries not found in the reference CSV get a null currency rather than
        dropping the row (left join), so this never changes df's row count.
        """
        if country_column not in df.columns:
            raise ValueError(
                f"'{country_column}' column not found in DataFrame; cannot enrich with currency"
            )

        df_currency = self.load_currency()

        return (
            df.join(
                df_currency,
                df[country_column] == df_currency["country"],
                how="left",
            )
            .drop(df_currency["country"])
        )


if __name__ == "__main__":
    # Quick manual check: python currency_enrichment.py
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    spark = SparkSession.builder.appName("test-currency-enrichment").master("local[1]").getOrCreate()

    sample = spark.createDataFrame(
        [("Acme", "FRANCE"), ("Globex", "GERMANY"), ("Nowhere Inc", "ATLANTIS")],
        ["customer_name", "customer_country"],
    )

    transformer = CountryCurrencyTransformer(spark)
    result = transformer.enrich(sample)
    result.show()

    spark.stop()