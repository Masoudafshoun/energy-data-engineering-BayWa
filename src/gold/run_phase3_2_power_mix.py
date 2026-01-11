from __future__ import annotations

from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.gold.power_mix import build_gold_power_mix_hourly, write_gold_power_mix_hourly


def main() -> None:
    s = Settings.from_env()
    spark = get_spark_session("phase3-gold-power-mix")

    silver_power = f"{s.data_root}/silver/public_power"
    gold_out = f"{s.data_root}/gold/power_mix_hourly"

    df = build_gold_power_mix_hourly(spark, silver_power)
    write_gold_power_mix_hourly(df, gold_out)

    spark.read.format("delta").load(gold_out).show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
