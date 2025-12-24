from pyspark.sql import Row
from src.utils.spark_session import get_spark_session

DELTA_PATH = "data/delta/people"

spark = get_spark_session("delta-io-test")

data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
]

df = spark.createDataFrame(data, ["id", "name"])

df.write.format("delta").mode("overwrite").save(DELTA_PATH)

df_read = spark.read.format("delta").load(DELTA_PATH)
df_read.show()

spark.stop()
