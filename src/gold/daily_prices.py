from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def build_gold_daily_prices(spark: SparkSession, silver_prices_path: str) -> DataFrame:
    df = spark.read.format("delta").load(silver_prices_path)
    # Aggregating to daily grain as required by the use-case
    return (
        df.groupBy("date", "bidding_zone")
        .agg(
            F.avg("price").alias("avg_price_eur_mwh"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price")
        )
        .orderBy("date")
    )

def write_gold_daily_prices(df: DataFrame, gold_path: str) -> None:
    df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)