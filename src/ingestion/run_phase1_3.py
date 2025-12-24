from datetime import timedelta

from src.utils.spark_session import get_spark_session
from src.api.energy_charts_client import EnergyChartsClient
from src.ingestion.public_power import ingest_public_power_day
from src.config.settings import Settings


def main():
    # Load settings from environment
    settings = Settings.from_env()

    spark = get_spark_session("phase1-public-power")
    client = EnergyChartsClient()

    bronze_path = f"{settings.data_root}/bronze/public_power"

    # Build date range
    days = []
    current = settings.start_date
    while current <= settings.end_date:
        days.append(current)
        current += timedelta(days=1)

    # Limit volume for local runs
    if settings.max_days > 0:
        days = days[: settings.max_days]

    print(f"Ingesting {len(days)} day(s): {days}")

    for day in days:
        ingest_public_power_day(
            spark=spark,
            client=client,
            country=settings.country,
            bronze_path=bronze_path,
            day=day,
        )

    spark.stop()


if __name__ == "__main__":
    main()
