from pyspark.sql import DataFrame

def write_price_bronze(df: DataFrame, path: str):
    (
        df.write
        .format("delta")
        .mode("append")
        .save(path)
    )