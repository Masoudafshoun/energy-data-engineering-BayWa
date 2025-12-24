from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests


class EnergyChartsClient:
    def __init__(self, base_url: str = "https://api.energy-charts.info"):
        self.base_url = base_url.rstrip("/")

    def _get(self, path: str, params: Dict[str, Any], retries: int = 3, backoff_s: float = 1.0) -> Any:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        last_err: Optional[Exception] = None

        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(backoff_s * attempt)
                else:
                    raise RuntimeError(f"GET failed after {retries} attempts: {url}") from last_err

        raise RuntimeError("Unreachable")

    def public_power(self, country: str, start: str, end: str) -> Any:
        # daily timestamp format: YYYY-MM-DD
        return self._get("/public_power", {"country": country, "start": start, "end": end})

    def price(self, bzn: str, start: str, end: str) -> Any:
        return self._get("/price", {"bzn": bzn, "start": start, "end": end})
