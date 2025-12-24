import requests

BASE_URL = "https://api.energy-charts.info/price"

def fetch_prices(start_date: str, end_date: str, bidding_zone: str):
    params = {
        "bzn": bidding_zone,
        "start": start_date,
        "end": end_date
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()