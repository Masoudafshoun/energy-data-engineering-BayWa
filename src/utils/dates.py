from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, List


def daterange(start: date, end: date) -> Iterable[date]:
    """Inclusive date range"""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def limited_days(days: List[date], max_days: int) -> List[date]:
    """Restrict volume for local runs. max_days=0 means no limit."""
    if max_days <= 0:
        return days
    return days[:max_days]
