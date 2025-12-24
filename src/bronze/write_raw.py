from __future__ import annotations

import json
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def json_to_single_row_df(spark, payload: Any, metadata: Dict[str, Any]) -> DataFrame:
    """
    Stores the full API payload as a single JSON string row + metadata.
    This is perfect for Bronze (raw, reproducible).
    """
    payload_str = json.dumps(payload, ensure_ascii=False)

    meta_cols = {k: F.lit(str(v)) for k, v in metadata.items()}
    df = spark.createDataFrame([(payload_str,)], ["payload_json"])

    for k, v in meta_cols.items():
        df = df.withColumn(k, v)

    # Standard metadata columns
    df = df.withColumn("ingested_at_utc", F.current_timestamp())

    return df


def write_bronze_delta(df: DataFrame, path: str) -> None:
    (
        df.write.format("delta")
        .mode("append")
        .save(path)
    )
