from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Iterable


BOLLINGER_PERIOD = 20
BOLLINGER_DEVIATIONS = 2.0


def build_bollinger_payload(
    minute_closes: Iterable[dict[str, Any]], *, visible_start_id: int
) -> dict[str, Any]:
    minutes = sorted(
        ({"bucket": _utc(item["bucket"]), "tickId": int(item["tickid"]), "close": float(item["close"])} for item in minute_closes),
        key=lambda item: item["bucket"],
    )
    five_minute: dict[int, dict[str, Any]] = {}
    for item in minutes:
        bucket_ms = int(item["bucket"].timestamp() * 1000)
        five_minute[bucket_ms - bucket_ms % 300_000] = item
    return {
        "period": BOLLINGER_PERIOD,
        "deviations": BOLLINGER_DEVIATIONS,
        "oneMinute": _rolling(minutes, visible_start_id),
        "fiveMinute": _rolling(list(five_minute.values()), visible_start_id),
    }


def _rolling(candles: list[dict[str, Any]], visible_start_id: int) -> list[dict[str, Any]]:
    result = []
    closes: list[float] = []
    for candle in candles:
        closes.append(float(candle["close"]))
        if len(closes) < BOLLINGER_PERIOD:
            continue
        window = closes[-BOLLINGER_PERIOD:]
        middle = sum(window) / BOLLINGER_PERIOD
        variance = sum((value - middle) ** 2 for value in window) / BOLLINGER_PERIOD
        deviation = math.sqrt(variance) * BOLLINGER_DEVIATIONS
        if int(candle["tickId"]) < visible_start_id:
            continue
        result.append({
            "tickId": int(candle["tickId"]),
            "timestampMs": int(candle["bucket"].timestamp() * 1000),
            "middle": middle,
            "upper": middle + deviation,
            "lower": middle - deviation,
        })
    return result


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

