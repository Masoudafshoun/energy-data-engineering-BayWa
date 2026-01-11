from __future__ import annotations

from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.gold.daily_prices import build_gold_daily_prices, write_gold_daily_prices


def main() -> None:
    s = Settings.from_env()
    spark = get_spark_session("phase3-gold-daily-prices")

    silver_prices = f"{s.data_root}/silver/prices"
    gold_out = f"{s.data_root}/gold/prices_daily"

    df = build_gold_daily_prices(spark, silver_prices)
    write_gold_daily_prices(df, gold_out)

    spark.read.format("delta").load(gold_out).show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
