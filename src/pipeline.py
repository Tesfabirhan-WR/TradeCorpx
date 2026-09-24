import logging
import os
from pyspark.sql import SparkSession

from CurrencyEnrichment import CountryCurrencyTransformer
from reader import DataReader
from transformer import build_data_enriched
from writer import DataWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class SilverPipeline:

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

        self.tables_to_load = (
            "customers",
            "orders",
            "order_details",
            "employees",
            "products",
            "categories",
            "shippers",
        )

    @staticmethod
    def load_azure_tradecorp_raw(
        spark: SparkSession, bronze_path: str, table_names: tuple[str, ...]
    ) -> dict:
        """Load each TradeCorp bronze table as a Spark DataFrame."""
        dataframes = {}
        for table_name in table_names:
            table_path = os.path.join(bronze_path, table_name)
            logger.info("Loading bronze table '%s' from %s", table_name, table_path)
            dataframes[table_name] = spark.read.parquet(table_path)
        return dataframes

    def run(self) -> int:
        """Run the silver ETL pipeline and return the written record count."""
        logger.info("Starting TradeCorp Silver ETL Pipeline...")

        # 1. Read Raw Bronze Data
        raw_dataframes = self.load_azure_tradecorp_raw(
            spark=self.spark,
            bronze_path=self.bronze_path,
            table_names=self.tables_to_load,
        )

        # 2. Perform Star-Schema Transformation & Join Operations
        df_orders_enriched = build_data_enriched(raw_dataframes)

        # 3. Currency Enrichment Transformation
        currency_path = os.getenv("CURRENCY_CSV_PATH")
        currency_transformer = (
            CountryCurrencyTransformer(self.spark, currency_csv_path=currency_path)
            if currency_path else CountryCurrencyTransformer(self.spark)
        )
        df_final_enriched = currency_transformer.enrich(
            df=df_orders_enriched,
            country_column="customer_country",
        )

        # 4. Write Enriched Result to Local Silver Storage Path
        table_name = "orders_enriched"
        
        # Option A: Local Parquet write only
        output_target_path = DataWriter.write_parquet_local(
            df=df_final_enriched,
            table_name=table_name,
            output_dir=self.silver_path,
            mode="overwrite",
        )

        # Option B: Full Silver Write (Uncomment if uploading to Azure/PostgreSQL)
        # results = DataWriter.write_silver(
        #     df=df_final_enriched,
        #     table_name=table_name,
        #     to_azure=True,
        #     to_postgres=True,
        #     local_tmp_dir=self.silver_path,
        #     mode="overwrite"
        # )
        # output_target_path = results["local_path"]

        # 5. Read Back Silver Table to Verify Count Without Recomputing Lineage
        written_df = DataReader.read_parquet(self.spark, output_target_path)
        record_count = written_df.count()

        logger.info(
            "Silver pipeline execution completed successfully! Written records count: %d",
            record_count,
        )
        return record_count


def main():
    spark = (
        SparkSession.builder
        .appName("TradeCorp Silver Pipeline Orchestrator")
        .getOrCreate()
    )

    BRONZE_PATH = os.getenv("BRONZE_PATH", "/home/jovyan/data/bronze")
    SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/data/silver")

    pipeline = SilverPipeline(
        spark=spark,
        bronze_path=BRONZE_PATH,
        silver_path=SILVER_PATH,
    )

    try:
        pipeline.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
