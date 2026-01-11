from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.gold.power_mix import build_gold_power_mix_daily, write_gold_power_mix_daily

def main():
    s = Settings.from_env()
    spark = get_spark_session("phase3-2-daily-power-mix")
    df = build_gold_power_mix_daily(spark, f"{s.data_root}/silver/public_power")
    write_gold_power_mix_daily(df, f"{s.data_root}/gold/power_mix_daily")
    df.show(5)
    spark.stop()

if __name__ == "__main__":
    main()