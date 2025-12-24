from src.api.energy_charts_client import fetch
from src.utils.resolution import price_resolution_for_date

def ingest_prices(start_date, end_date, bidding_zone="DE-LU"):
    resolution = price_resolution_for_date(start_date)

    return fetch(
        endpoint="price",
        params={
            "bzn": bidding_zone,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "resolution": resolution
        }
    )