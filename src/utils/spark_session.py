from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name: str = "energy-data-pipeline"):
    """
    Create and return a SparkSession with Delta Lake support.
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark
