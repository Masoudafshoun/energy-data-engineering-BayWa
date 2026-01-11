from __future__ import annotations

from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.gold.price_vs_renewables import (
    build_gold_price_vs_renewables_hourly,
    write_gold_price_vs_renewables_hourly,
)


def main() -> None:
    s = Settings.from_env()
    spark = get_spark_session("phase3-gold-price-vs-renewables")

    silver_prices = f"{s.data_root}/silver/prices"
    silver_power = f"{s.data_root}/silver/public_power"
    gold_out = f"{s.data_root}/gold/price_vs_renewables_hourly"

    df = build_gold_price_vs_renewables_hourly(
        spark,
        silver_prices_path=silver_prices,
        silver_power_path=silver_power,
        bidding_zone=s.price_bzn,
    )
    write_gold_price_vs_renewables_hourly(df, gold_out)

    spark.read.format("delta").load(gold_out).show(30, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
