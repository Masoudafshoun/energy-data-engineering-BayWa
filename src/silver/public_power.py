from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_silver_public_power_from_json_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    """
    Bronze schema: day, dataset, country, ingested_at_utc, payload_json (string)

    payload_json contains:
      - unix_seconds: [ ... ]  (epoch seconds; aligned by index)
      - production_types: [ { "name": str, "data": [ ... ] }, ... ]
        where data[i] corresponds to unix_seconds[i]

    Output (long format):
      day, country, dataset, ingested_at_utc, timestamp_utc, production_type, value
    """

    bronze = spark.read.format("delta").load(bronze_path)

    schema = "struct<unix_seconds:array<long>, production_types:array<struct<name:string,data:array<double>>> >"
    parsed = bronze.withColumn("j", F.from_json(F.col("payload_json"), schema))

    ts_rows = parsed.select(
        "day",
        "country",
        "dataset",
        "ingested_at_utc",
        F.posexplode(F.col("j.unix_seconds")).alias("pos", "unix_seconds"),
        F.col("j.production_types").alias("production_types"),
    )

    long_df = (
        ts_rows
        .withColumn("pt", F.explode(F.col("production_types")))
        .withColumn("production_type", F.col("pt.name"))
        .withColumn("value", F.expr("element_at(pt.data, pos + 1)"))  # element_at is 1-based
        .withColumn("timestamp_utc", F.to_timestamp(F.from_unixtime(F.col("unix_seconds"))))
        .drop("production_types", "pt", "pos", "unix_seconds")
        .filter(F.col("value").isNotNull())
    )

    return long_df


def write_silver_public_power(df: DataFrame, silver_path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
