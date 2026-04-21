from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


BROKER_TIMEZONE = ZoneInfo("Australia/Sydney")
BROKER_DAY_START_HOUR = 8


def brokerday_for_timestamp(value: datetime) -> date:
    localized = value.astimezone(BROKER_TIMEZONE)
    if localized.timetz().replace(tzinfo=None) < dt_time(hour=BROKER_DAY_START_HOUR):
        localized = localized - timedelta(days=1)
    return localized.date()


def brokerday_bounds(day_value: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(day_value, dt_time(hour=BROKER_DAY_START_HOUR), tzinfo=BROKER_TIMEZONE)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def tick_mid(row: Dict[str, Any]) -> Optional[float]:
    mid = row.get("mid")
    if mid is not None:
        return float(mid)
    bid = row.get("bid")
    ask = row.get("ask")
    if bid is None and ask is None:
        return None
    if bid is None:
        return float(ask)
    if ask is None:
        return float(bid)
    return (float(bid) + float(ask)) / 2.0
