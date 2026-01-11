from __future__ import annotations

from src.config.settings import Settings
from src.utils.spark_session import get_spark_session
from src.silver.public_power import (
    build_silver_public_power_from_json_bronze,
    write_silver_public_power,
)


def main() -> None:
    settings = Settings.from_env()
    spark = get_spark_session("phase2-silver-public-power")

    bronze_path = f"{settings.data_root}/bronze/public_power"
    silver_path = f"{settings.data_root}/silver/public_power"

    df = build_silver_public_power_from_json_bronze(spark, bronze_path)
    write_silver_public_power(df, silver_path)

    spark.read.format("delta").load(silver_path).orderBy("timestamp_utc").show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
