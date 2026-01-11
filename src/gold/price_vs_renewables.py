from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RENEWABLE_TYPES = {
    "Solar",
    "Wind onshore",
    "Wind offshore",
    "Hydro Run-of-River",
    "Hydro water reservoir",
    "Biomass",
    "Geothermal",
}


def build_gold_price_vs_renewables_hourly(
    spark: SparkSession,
    silver_prices_path: str,
    silver_power_path: str,
    bidding_zone: str = "DE-LU",
) -> DataFrame:
    prices = spark.read.format("delta").load(silver_prices_path)
    power = spark.read.format("delta").load(silver_power_path)

    # ---- prices hourly
    prices_h = (
        prices.filter(F.col("bidding_zone") == bidding_zone)
        .withColumn("hour_ts", F.date_trunc("hour", F.col("timestamp")))
        .withColumn("date", F.to_date("hour_ts"))
        .groupBy("date", "hour_ts", "bidding_zone")
        .agg(F.avg("price").alias("avg_price_hourly"))
    )

    # ---- power hourly totals + renewable totals
    power_h = (
        power.withColumn("hour_ts", F.date_trunc("hour", F.col("timestamp_utc")))
        .withColumn("date", F.to_date("hour_ts"))
        .groupBy("date", "hour_ts", "country")
        .agg(
            F.sum(F.when(F.col("production_type").isin(list(RENEWABLE_TYPES)), F.col("value")).otherwise(F.lit(0.0))).alias("renewables_mw"),
            F.sum(F.when(F.col("production_type") == "Load", F.col("value")).otherwise(F.lit(0.0))).alias("load_mw"),
        )
        .withColumn(
            "renewables_share_of_load",
            F.when(F.col("load_mw") > 0, F.col("renewables_mw") / F.col("load_mw")).otherwise(F.lit(None))
        )
    )

    # ---- join
    joined = (
        prices_h.join(power_h, on=["date", "hour_ts"], how="inner")
        .select(
            "date",
            "hour_ts",
            "bidding_zone",
            "avg_price_hourly",
            "country",
            "renewables_mw",
            "load_mw",
            "renewables_share_of_load",
        )
        .orderBy("hour_ts")
    )

    return joined


def write_gold_price_vs_renewables_hourly(df: DataFrame, gold_path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save(gold_path)
    )
