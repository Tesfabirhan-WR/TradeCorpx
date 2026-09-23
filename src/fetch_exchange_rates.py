# src/exchange_rate.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ExchangeRateTransformer:
    """
    Currency conversion utilities.
    """

    @staticmethod
    def apply_exchange_rates(
        df: DataFrame,
        rates: dict,
        source_currency_col: str = "currency",
        amount_col: str = "amount",
        target_currency: str = "EUR"
    ) -> DataFrame:
        """
        Convert transaction amounts into target currency.

        Example rates:
        {
            "USD": 0.92,
            "GBP": 1.17,
            "EUR": 1.00
        }
        """

        exchange_expr = F.create_map(
            *[F.lit(x) for pair in rates.items() for x in pair]
        )

        return (
            df
            .withColumn(
                "exchange_rate",
                exchange_expr[F.col(source_currency_col)]
            )
            .withColumn(
                f"amount_{target_currency.lower()}",
                F.round(
                    F.col(amount_col) * F.col("exchange_rate"),
                    2
                )
            )
        )