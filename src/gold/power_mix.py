from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def build_gold_power_mix_daily(spark: SparkSession, silver_power_path: str) -> DataFrame:
    # Load the Silver table where the column is named 'day'
    df = spark.read.format("delta").load(silver_power_path)
    
    # Use 'day' for the grouping and alias it to 'date' for the Gold layer
    return (
        df.groupBy(F.col("day").alias("date"), "country", "production_type")
        .agg(F.sum("value").alias("daily_total_mwh"))
        .orderBy("date", "production_type")
    )

def write_gold_power_mix_daily(df: DataFrame, gold_path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") # Handle schema changes safely
        .partitionBy("date")
        .save(gold_path)
    )