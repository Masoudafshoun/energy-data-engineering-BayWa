from __future__ import annotations

from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.quality.checks import (
    assert_no_nulls,
    assert_non_negative,
    assert_row_count_min,
)


def main() -> None:
    s = Settings.from_env()
    spark = get_spark_session("quality-checks")

    # Load the updated Gold tables
    gold_daily = spark.read.format("delta").load(f"{s.data_root}/gold/prices_daily")
    gold_mix = spark.read.format("delta").load(f"{s.data_root}/gold/power_mix_daily")
    gold_corr = spark.read.format("delta").load(f"{s.data_root}/gold/price_vs_offshore_daily")

    # --- 1. gold.prices_daily (Daily Price KPIs)
    # Updated to check for 'avg_price_eur_mwh' per your new schema
    assert_row_count_min(gold_daily, 1, "gold.prices_daily")
    assert_no_nulls(
        gold_daily, 
        ["date", "bidding_zone", "avg_price_eur_mwh"], 
        "gold.prices_daily"
    )

    # --- 2. gold.power_mix_daily (Trend of Daily Production)
    # Updated path and columns to match Daily grain and 'daily_total_mwh'
    assert_row_count_min(gold_mix, 1, "gold.power_mix_daily")
    assert_no_nulls(
        gold_mix, 
        ["date", "country", "production_type", "daily_total_mwh"], 
        "gold.power_mix_daily"
    )

    # --- 3. gold.price_vs_offshore_daily (Correlation Analysis)
    # Updated to check 'offshore_wind_mwh' and daily average price
    assert_row_count_min(gold_corr, 1, "gold.price_vs_offshore_daily")
    assert_no_nulls(
        gold_corr, 
        ["date", "avg_price_eur_mwh", "offshore_wind_mwh"], 
        "gold.price_vs_offshore_daily"
    )
    # Ensure production is not negative for offshore wind correlation
    assert_non_negative(gold_corr, "offshore_wind_mwh", "gold.price_vs_offshore_daily")

    print("✅ All daily Gold layer quality checks passed.")
    spark.stop()


if __name__ == "__main__":
    main()