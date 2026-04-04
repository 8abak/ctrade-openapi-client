from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


ZONE_STATE_PROVISIONAL = "provisional"
ZONE_STATE_ACTIVE = "active"
ZONE_STATE_CLOSED = "closed"
ZONE_STATES = {
    ZONE_STATE_PROVISIONAL,
    ZONE_STATE_ACTIVE,
    ZONE_STATE_CLOSED,
}


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def round_price(value: float) -> float:
    return round(float(value), 6)


def format_duration_ms(duration_ms: int) -> str:
    total_ms = max(0, int(duration_ms))
    if total_ms < 1000:
        return "{0} ms".format(total_ms)
    total_seconds, remainder_ms = divmod(total_ms, 1000)
    hours, remainder_seconds = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder_seconds, 60)
    parts: List[str] = []
    if hours:
        parts.append("{0}h".format(hours))
    if minutes:
        parts.append("{0}m".format(minutes))
    if seconds:
        parts.append("{0}s".format(seconds))
    if not parts and remainder_ms:
        parts.append("{0}ms".format(remainder_ms))
    return " ".join(parts[:3]) if parts else "0 ms"


def price_series_mid(row: Dict[str, Any]) -> Optional[float]:
    mid_value = row.get("mid")
    if mid_value is not None:
        return float(mid_value)
    if row.get("bid") is not None and row.get("ask") is not None:
        return round((float(row["bid"]) + float(row["ask"])) / 2.0, 2)
    return None


def zone_contains_with_tolerance(price: float, low: float, high: float, tolerance: float) -> bool:
    return price >= low - tolerance and price <= high + tolerance


def zone_touch_side(price: float, low: float, high: float, tolerance: float) -> Optional[str]:
    if abs(price - low) <= tolerance:
        return "low"
    if abs(price - high) <= tolerance:
        return "high"
    return None


def pivot_breakout_direction(
    pivot: Dict[str, Any],
    *,
    low: float,
    high: float,
    tolerance: float,
) -> Optional[str]:
    price = float(pivot["pivot_price"])
    if price > high + tolerance:
        return "up"
    if price < low - tolerance:
        return "down"
    return None


def zone_birth_candidate(
    pivots: List[Dict[str, Any]],
    *,
    same_side_tolerance: float,
    min_height: float,
    max_height: float,
) -> Optional[Dict[str, Any]]:
    if len(pivots) < 3:
        return None

    anchor_a = pivots[-3]
    anchor_b = pivots[-2]
    anchor_c = pivots[-1]
    pattern = (
        str(anchor_a.get("direction") or "").lower(),
        str(anchor_b.get("direction") or "").lower(),
        str(anchor_c.get("direction") or "").lower(),
    )
    if pattern == ("high", "low", "high"):
        zone_low = float(anchor_b["pivot_price"])
        zone_high = max(float(anchor_a["pivot_price"]), float(anchor_c["pivot_price"]))
        birth_rule = "Repeated high near prior high; middle low defines the bracket."
        pattern_type = "H-L-H"
    elif pattern == ("low", "high", "low"):
        zone_low = min(float(anchor_a["pivot_price"]), float(anchor_c["pivot_price"]))
        zone_high = float(anchor_b["pivot_price"])
        birth_rule = "Repeated low near prior low; middle high defines the bracket."
        pattern_type = "L-H-L"
    else:
        return None

    same_side_distance = abs(float(anchor_c["pivot_price"]) - float(anchor_a["pivot_price"]))
    if same_side_distance > same_side_tolerance:
        return None

    zone_height = zone_high - zone_low
    if zone_height < min_height or zone_height > max_height:
        return None

    anchor_pivots = []
    for pivot in (anchor_a, anchor_b, anchor_c):
        anchor_pivots.append(
            {
                "pivotId": int(pivot["pivot_id"]),
                "direction": str(pivot["direction"]).lower(),
                "price": round_price(float(pivot["pivot_price"])),
                "sourceTickId": int(pivot["source_tick_id"]),
                "timestamp": pivot["source_timestamp"].isoformat(),
                "timestampMs": dt_to_ms(pivot["source_timestamp"]),
                "selectedVisibleFromTickId": int(pivot["visible_from_tick_id"]),
            }
        )

    return {
        "pattern": pattern_type,
        "birth_rule": birth_rule,
        "zone_low": zone_low,
        "zone_high": zone_high,
        "same_side_distance": same_side_distance,
        "start_pivot": dict(anchor_a),
        "middle_pivot": dict(anchor_b),
        "end_pivot": dict(anchor_c),
        "anchor_pivots": anchor_pivots,
    }


def zone_birth_rule(pattern: str) -> str:
    if pattern == "H-L-H":
        return "Repeated high near prior high; middle low defines the bracket."
    if pattern == "L-H-L":
        return "Repeated low near prior low; middle high defines the bracket."
    return "Pivot bracket zone."


def serialize_zonebox_row(row: Dict[str, Any]) -> Dict[str, Any]:
    starttime = row.get("starttime")
    endtime = row.get("endtime")
    lasttime = row.get("lasttime") or endtime or starttime
    duration_inside_ms = int(row.get("durationms") or 0)
    episode_duration_ms = max(
        duration_inside_ms,
        int((lasttime - starttime).total_seconds() * 1000.0) if starttime and lasttime else duration_inside_ms,
    )
    initial_low = float(row["initialzonelow"])
    initial_high = float(row["initialzonehigh"])
    current_low = float(row["zonelow"])
    current_high = float(row["zonehigh"])
    anchor_pivots = [
        {
            "pivotId": int(row["startpivotid"]),
            "direction": "high" if str(row["pattern"]) == "H-L-H" else "low",
            "price": round_price(float(row["startpivotprice"])),
            "sourceTickId": int(row["startpivottickid"]),
            "timestamp": row["startpivottime"].isoformat() if row.get("startpivottime") else None,
            "timestampMs": dt_to_ms(row.get("startpivottime")),
            "selectedVisibleFromTickId": int(row["startpivottickid"]),
        },
        {
            "pivotId": int(row["middlepivotid"]),
            "direction": "low" if str(row["pattern"]) == "H-L-H" else "high",
            "price": round_price(float(row["middlepivotprice"])),
            "sourceTickId": int(row["middlepivottickid"]),
            "timestamp": row["middlepivottime"].isoformat() if row.get("middlepivottime") else None,
            "timestampMs": dt_to_ms(row.get("middlepivottime")),
            "selectedVisibleFromTickId": int(row["middlepivottickid"]),
        },
        {
            "pivotId": int(row["endpivotid"]),
            "direction": "high" if str(row["pattern"]) == "H-L-H" else "low",
            "price": round_price(float(row["endpivotprice"])),
            "sourceTickId": int(row["endpivottickid"]),
            "timestamp": row["endpivottime"].isoformat() if row.get("endpivottime") else None,
            "timestampMs": dt_to_ms(row.get("endpivottime")),
            "selectedVisibleFromTickId": int(row["endpivottickid"]),
        },
    ]
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "selectedLevel": row["level"],
        "status": row["state"],
        "startTickId": row["starttickid"],
        "endTickId": row["endtickid"],
        "rightTickId": row["lasttickid"],
        "startTimestamp": starttime.isoformat() if starttime else None,
        "endTimestamp": endtime.isoformat() if endtime else None,
        "rightTimestamp": lasttime.isoformat() if lasttime else None,
        "startTimestampMs": dt_to_ms(starttime),
        "endTimestampMs": dt_to_ms(endtime),
        "rightTimestampMs": dt_to_ms(lasttime),
        "zoneLow": round_price(current_low),
        "zoneHigh": round_price(current_high),
        "zoneHeight": round_price(float(row["zoneheight"])),
        "initialZoneLow": round_price(initial_low),
        "initialZoneHigh": round_price(initial_high),
        "initialZoneHeight": round_price(initial_high - initial_low),
        "tickCountInside": int(row["tickcountinside"] or 0),
        "durationInsideMs": duration_inside_ms,
        "durationInsideLabel": format_duration_ms(duration_inside_ms),
        "episodeDurationMs": episode_duration_ms,
        "episodeDurationLabel": format_duration_ms(episode_duration_ms),
        "openTimestamp": starttime.isoformat() if starttime else None,
        "closeTimestamp": endtime.isoformat() if endtime else None,
        "touchCount": int(row.get("touchcount") or 0),
        "revisitCount": int(row.get("revisitcount") or 0),
        "patternType": row["pattern"],
        "birthRule": zone_birth_rule(str(row["pattern"])),
        "sameSideDistance": round_price(float(row["samesidedistance"])),
        "sameSideTolerance": round_price(float(row["samesidetoleranceused"])),
        "continuationTolerance": round_price(float(row["continuationovershootused"])),
        "maxAllowedOvershoot": round_price(float(row["continuationovershootused"])),
        "breakoutDirection": row.get("breakdirection"),
        "breakoutTickId": row.get("breaktickid"),
        "breakoutTimestamp": endtime.isoformat() if endtime else None,
        "breakoutTimestampMs": dt_to_ms(endtime),
        "anchorStartPivotId": row["startpivotid"],
        "anchorMiddlePivotId": row["middlepivotid"],
        "anchorEndPivotId": row["endpivotid"],
        "anchorPivots": anchor_pivots,
        "parentStartPivotId": row["startpivotid"],
        "parentEndPivotId": row["endpivotid"],
        "contextStartPivotId": row["startpivotid"],
        "contextEndPivotId": row["endpivotid"],
        "derivedFromAcceptance": True,
        "seedLow": round_price(initial_low),
        "seedHigh": round_price(initial_high),
        "seedHeight": round_price(initial_high - initial_low),
    }
