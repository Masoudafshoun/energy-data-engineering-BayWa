from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark_session

RAW_PATH = "data/raw/sample.csv"
BRONZE_PATH = "data/bronze/users"

spark = get_spark_session("bronze-ingestion")

df = spark.read.option("header", True).csv(RAW_PATH)

df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

df.show()

spark.stop()
