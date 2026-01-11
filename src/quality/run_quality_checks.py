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

    gold_daily = spark.read.format("delta").load(f"{s.data_root}/gold/prices_daily")
    gold_mix = spark.read.format("delta").load(f"{s.data_root}/gold/power_mix_hourly")
    gold_join = spark.read.format("delta").load(f"{s.data_root}/gold/price_vs_renewables_hourly")

    # --- prices_daily
    assert_row_count_min(gold_daily, 1, "gold.prices_daily")
    assert_no_nulls(gold_daily, ["date", "bidding_zone", "avg_price"], "gold.prices_daily")

    # --- power_mix_hourly
    assert_row_count_min(gold_mix, 1, "gold.power_mix_hourly")
    assert_no_nulls(gold_mix, ["date", "hour_ts", "country", "production_type", "avg_mw"], "gold.power_mix_hourly")

    # --- price_vs_renewables_hourly
    assert_row_count_min(gold_join, 1, "gold.price_vs_renewables_hourly")
    assert_no_nulls(gold_join, ["date", "hour_ts", "bidding_zone", "avg_price_hourly"], "gold.price_vs_renewables_hourly")
    assert_non_negative(gold_join, "load_mw", "gold.price_vs_renewables_hourly")

    print("✅ All quality checks passed.")
    spark.stop()


if __name__ == "__main__":
    main()
