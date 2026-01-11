from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_gold_power_mix_hourly(spark: SparkSession, silver_power_path: str) -> DataFrame:
    df = spark.read.format("delta").load(silver_power_path)

    # Make hourly bucket
    df = df.withColumn("hour_ts", F.date_trunc("hour", F.col("timestamp_utc")))
    df = df.withColumn("date", F.to_date("hour_ts"))

    out = (
        df.groupBy("date", "hour_ts", "country", "production_type")
        .agg(F.avg("value").alias("avg_mw"))
        .orderBy("hour_ts", "production_type")
    )
    return out


def write_gold_power_mix_hourly(df: DataFrame, gold_path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save(gold_path)
    )
