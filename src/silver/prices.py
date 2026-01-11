from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_silver_prices(
    spark: SparkSession,
    bronze_path: str,
) -> DataFrame:
    df = spark.read.format("delta").load(bronze_path)

    # Ensure timestamp is timestamp type
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    # Standard date column for partitioning
    df = df.withColumn("date", F.to_date("timestamp"))

    # Normalize price type
    df = df.withColumn("price", F.col("price").cast("double"))

    # Resolution handling:
    # Your Bronze had "resolution" sometimes 'unknown'. We'll compute it from timestamp gaps is expensive.
    # Instead, infer by whether minute is 0/15/30/45:
    df = df.withColumn(
        "resolution_minutes",
        F.when(F.minute("timestamp").isin([15, 30, 45]), F.lit(15)).otherwise(F.lit(60))
    )

    # Deduplicate: keep latest ingestion for the same timestamp & zone
    # (Assumes ingestion_ts exists)
    w = Window.partitionBy("timestamp", "bidding_zone").orderBy(F.col("ingestion_ts").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # Select final columns (keep what you need)
    df = df.select(
        "timestamp",
        "date",
        "bidding_zone",
        "price",
        "resolution_minutes",
        "data_date",
        "ingestion_ts",
    )

    return df


def write_silver_prices(
    df: DataFrame,
    silver_path: str,
) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save(silver_path)
    )
