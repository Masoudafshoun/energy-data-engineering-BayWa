from src.utils.spark_session import get_spark_session

spark = get_spark_session()

print("Spark version:", spark.version)

spark.stop()
