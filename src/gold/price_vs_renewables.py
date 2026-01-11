from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def build_gold_price_vs_offshore_daily(
    spark: SparkSession,
    gold_prices_daily_path: str,
    gold_power_mix_daily_path: str,
) -> DataFrame:
    prices = spark.read.format("delta").load(gold_prices_daily_path)
    power = spark.read.format("delta").load(gold_power_mix_daily_path)

    # Filter specifically for Offshore Wind as per requirements
    offshore_wind = power.filter(F.col("production_type") == "Wind offshore") \
                         .select("date", "daily_total_mwh") \
                         .withColumnRenamed("daily_total_mwh", "offshore_wind_mwh")

    # Join with daily prices
    # USE CASE: Enables correlation analysis of daily price against net power for offshore wind
    return (
        prices.join(offshore_wind, on="date", how="inner")
        .select("date", "avg_price_eur_mwh", "offshore_wind_mwh")
        .orderBy("date")
    )

def write_gold_price_vs_offshore_daily(df: DataFrame, gold_path: str) -> None:
    df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)