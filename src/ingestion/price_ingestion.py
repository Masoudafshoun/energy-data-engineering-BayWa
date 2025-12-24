import requests
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.functions import lit


BASE_URL = "https://api.energy-charts.info/price"


def ingest_price_for_day(
    spark,
    date: str,
    bidding_zone: str,
    output_path: str,
):
    params = {
        "bzn": bidding_zone,
        "start": date,
        "end": date,
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    # ---- IMPORTANT: Energy-Charts schema ----
    unix_seconds = data["unix_seconds"]
    prices = data["price"]
    resolution = data.get("resolution", "unknown")

    rows = []
    for ts, price in zip(unix_seconds, prices):
        rows.append(
            Row(
                timestamp=datetime.utcfromtimestamp(ts),
                price=float(price),
                resolution=resolution,
                bidding_zone=bidding_zone,
                data_date=date,
                ingestion_ts=datetime.utcnow(),
            )
        )

    if not rows:
        print(f"No price data for {date}")
        return

    df = spark.createDataFrame(rows)

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("data_date")
        .save(output_path)
    )
