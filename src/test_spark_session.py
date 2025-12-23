from utils.spark_session import get_spark_session

spark = get_spark_session("spark-session-test")
print(spark.version)
spark.stop()

