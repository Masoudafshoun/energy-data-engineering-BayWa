from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os


def _get_env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _parse_date(s: str) -> date:
    # expects YYYY-MM-DD
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


@dataclass(frozen=True)
class Settings:
    # Where delta tables are stored (local, inside WSL filesystem)
    data_root: str

    # Backfill window (inclusive)
    # Aligned to Jan 2024 to match existing price data
    start_date: date
    end_date: date

    # Ingestion frequency (daily chunks)
    chunk: str  # "daily"

    # Limit how much we ingest for local runs (0 = no limit)
    max_days: int

    # API defaults
    country: str          # "de"
    price_bzn: str        # "DE-LU"

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            data_root=_get_env("DATA_ROOT", "./data"),
            # Default dates changed from 2025 to 2024 to ensure data overlap
            start_date=_parse_date(_get_env("START_DATE", "2024-01-01")),
            end_date=_parse_date(_get_env("END_DATE", "2024-01-07")),
            chunk=_get_env("CHUNK", "daily"),
            max_days=int(_get_env("MAX_DAYS", "7")),
            country=_get_env("COUNTRY", "de"),
            price_bzn=_get_env("PRICE_BZN", "DE-LU"),
        )