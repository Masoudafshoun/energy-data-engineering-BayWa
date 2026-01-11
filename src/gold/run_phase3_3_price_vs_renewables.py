from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.gold.price_vs_renewables import build_gold_price_vs_offshore_daily, write_gold_price_vs_offshore_daily

def main():
    s = Settings.from_env()
    spark = get_spark_session("phase3-3-correlation-analysis")
    
    # Joins the two previously created Gold tables
    df = build_gold_price_vs_offshore_daily(
        spark,
        gold_prices_daily_path=f"{s.data_root}/gold/prices_daily",
        gold_power_mix_daily_path=f"{s.data_root}/gold/power_mix_daily"
    )
    
    write_gold_price_vs_offshore_daily(df, f"{s.data_root}/gold/price_vs_offshore_daily")
    print("--- Final Correlation Table (Daily Price vs Offshore Wind) ---")
    df.show(20)
    spark.stop()

if __name__ == "__main__":
    main()