from __future__ import annotations
import re

_PERIOD = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def validate_period(period: str) -> str:
    if not _PERIOD.match(period):
        raise ValueError(f"invalid period {period!r}; expected YYYY-MM, e.g. 2025-01")
    return period


def split_period(period: str) -> tuple[str, str]:
    """'2025-01' -> ('2025', '01')."""
    year, month = validate_period(period).split("-")
    return year, month


def month_range(start: str, end: str) -> list[str]:
    """Return every period from start to end, both inclusive."""
    start_year, start_month = map(int, split_period(start))
    end_year, end_month = map(int, split_period(end))
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(f"start {start} is after end {end}")

    periods = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return periods