from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class CountryCurrencyUploader:
    """
    Handles upload of country-currency reference data.
    """

    REQUIRED_COLUMNS = [
        "country_code",
        "country_name",
        "currency_code",
        "currency_name"
    ]

    @classmethod
    def transform(cls, df: DataFrame) -> DataFrame:
        """
        Clean and standardize country/currency data.
        """

        missing = [
            col for col in cls.REQUIRED_COLUMNS
            if col not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}"
            )

        return (
            df.select(*cls.REQUIRED_COLUMNS)
            .dropDuplicates(["country_code"])
            .withColumn(
                "country_code",
                F.upper(F.trim(F.col("country_code")))
            )
            .withColumn(
                "currency_code",
                F.upper(F.trim(F.col("currency_code")))
            )
            .withColumn(
                "country_name",
                F.initcap(F.trim(F.col("country_name")))
            )
            .withColumn(
                "currency_name",
                F.initcap(F.trim(F.col("currency_name")))
            )
        )

    @staticmethod
    def upload_to_postgres(
        df: DataFrame,
        jdbc_url: str,
        table_name: str,
        user: str,
        password: str,
        mode: str = "overwrite"
    ):
        """
        Upload dataframe to PostgreSQL.
        """

        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .mode(mode)
            .save()
        )

        print(
            f"Country/Currency data uploaded to {table_name}"
        )