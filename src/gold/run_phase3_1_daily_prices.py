from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.gold.daily_prices import build_gold_daily_prices, write_gold_daily_prices

def main():
    s = Settings.from_env()
    spark = get_spark_session("phase3-1-daily-prices")
    df = build_gold_daily_prices(spark, f"{s.data_root}/silver/prices")
    write_gold_daily_prices(df, f"{s.data_root}/gold/prices_daily")
    df.show(5)
    spark.stop()

if __name__ == "__main__":
    main()