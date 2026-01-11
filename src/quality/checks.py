from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_no_nulls(df: DataFrame, cols: list[str], table_name: str) -> None:
    checks = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
    row = df.select(*checks).collect()[0].asDict()
    bad = {k: v for k, v in row.items() if v and v > 0}
    if bad:
        raise ValueError(f"[{table_name}] Null check failed: {bad}")


def assert_non_negative(df: DataFrame, col: str, table_name: str) -> None:
    n_bad = df.filter(F.col(col) < 0).count()
    if n_bad > 0:
        raise ValueError(f"[{table_name}] Non-negative check failed for '{col}': {n_bad} rows")


def assert_row_count_min(df: DataFrame, min_rows: int, table_name: str) -> None:
    n = df.count()
    if n < min_rows:
        raise ValueError(f"[{table_name}] Row count too small: {n} < {min_rows}")
