from __future__ import annotations

from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.silver.prices import build_silver_prices, write_silver_prices


def main() -> None:
    settings = Settings.from_env()
    spark = get_spark_session("phase2-silver-prices")

    bronze_path = f"{settings.data_root}/bronze/prices"
    silver_path = f"{settings.data_root}/silver/prices"

    df = build_silver_prices(spark, bronze_path)
    write_silver_prices(df, silver_path)

    spark.read.format("delta").load(silver_path).orderBy("timestamp").show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
