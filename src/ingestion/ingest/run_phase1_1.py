from __future__ import annotations

import os
from pathlib import Path

from src.config.settings import Settings
from src.utils.dates import daterange, limited_days
from src.utils.spark_session import get_spark_session
from src.ingestion.public_power import ingest_public_power_range


def main() -> None:
    settings = Settings.from_env()

    # Build day list (daily chunks)
    days = list(daterange(settings.start_date, settings.end_date))
    days = limited_days(days, settings.max_days)

    # Data root (delta tables live here)
    data_root = Path(settings.data_root).resolve()
    bronze_public_power_path = str(data_root / "bronze" / "public_power")

    # Create folders (safe)
    os.makedirs(bronze_public_power_path, exist_ok=True)

    spark = get_spark_session("phase1_1_public_power")

    try:
        ingest_public_power_range(
            spark=spark,
            country=settings.country,
            days=days,
            bronze_path=bronze_public_power_path,
        )
        print(f"✅ Ingested {len(days)} day(s) to Delta: {bronze_public_power_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
