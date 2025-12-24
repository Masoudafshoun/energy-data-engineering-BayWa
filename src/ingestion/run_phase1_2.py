from datetime import datetime, timedelta

from src.utils.spark_session import get_spark_session
from src.config.price_config import (
    START_DATE,
    END_DATE,
    MAX_DAYS,
    BIDDING_ZONE
)
from src.ingestion.price_ingestion import ingest_price_for_day

OUTPUT_PATH = "data/bronze/prices"


def daterange(start, end, max_days):
    current = start
    count = 0
    while current <= end and count < max_days:
        yield current
        current += timedelta(days=1)
        count += 1


if __name__ == "__main__":
    spark = get_spark_session("price-bronze-ingestion")

    start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end = datetime.strptime(END_DATE, "%Y-%m-%d").date()

    for day in daterange(start, end, MAX_DAYS):
        ingest_price_for_day(
            spark,
            day.strftime("%Y-%m-%d"),
            BIDDING_ZONE,
            OUTPUT_PATH
        )

    spark.stop()
