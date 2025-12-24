from datetime import date, timedelta


def resolve_ingestion_dates(config: dict):
    """
    Returns a list of dates to ingest based on config.
    Enforces safety limits.
    """
    mode = config["mode"]

    if mode == "daily":
        return [date.today() - timedelta(days=1)]

    if mode == "backfill":
        start = config["backfill"]["start_date"]
        end = config["backfill"]["end_date"]

        if start > end:
            raise ValueError("start_date must be <= end_date")

        days = (end - start).days + 1

        if days > config["max_days_per_run"]:
            raise ValueError(
                f"Backfill window too large ({days} days). "
                f"Max allowed: {config['max_days_per_run']}"
            )

        return [start + timedelta(days=i) for i in range(days)]

    raise ValueError(f"Unknown ingestion mode: {mode}")
