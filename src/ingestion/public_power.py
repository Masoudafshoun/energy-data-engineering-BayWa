from __future__ import annotations

from datetime import date
from typing import List

from src.api.energy_charts_client import EnergyChartsClient
from src.bronze.write_raw import json_to_single_row_df, write_bronze_delta


def ingest_public_power_day(spark, client: EnergyChartsClient, country: str, day: date, bronze_path: str) -> None:
    day_str = day.isoformat()
    payload = client.public_power(country=country, start=day_str, end=day_str)

    df = json_to_single_row_df(
        spark,
        payload=payload,
        metadata={
            "dataset": "public_power",
            "country": country,
            "day": day_str,
        },
    )

    write_bronze_delta(df, bronze_path)


def ingest_public_power_range(spark, country: str, days: List[date], bronze_path: str) -> None:
    client = EnergyChartsClient()
    for d in days:
        ingest_public_power_day(spark, client, country, d, bronze_path)
