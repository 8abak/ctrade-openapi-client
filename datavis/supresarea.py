from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from datavis.zonebox import dt_to_ms, price_series_mid, round_price


AREA_SIDE_TOP = "top"
AREA_SIDE_BOTTOM = "bottom"
AREA_SIDES = {
    AREA_SIDE_TOP,
    AREA_SIDE_BOTTOM,
}

AREA_STATE_ACTIVE = "active"
AREA_STATE_USED = "used"
AREA_STATE_CLOSED = "closed"
AREA_STATES = {
    AREA_STATE_ACTIVE,
    AREA_STATE_USED,
    AREA_STATE_CLOSED,
}


def area_side_from_pattern(pattern: str) -> str:
    return AREA_SIDE_TOP if str(pattern) == "H-L-H" else AREA_SIDE_BOTTOM


def level_flags(level: int) -> tuple[bool, bool]:
    effective_level = max(0, int(level or 0))
    return effective_level >= 1, effective_level >= 2


def executable_price_for_side(side: str, tick_row: Dict[str, Any]) -> Optional[float]:
    if side == AREA_SIDE_TOP:
        value = tick_row.get("ask")
        if value is not None:
            return float(value)
    elif side == AREA_SIDE_BOTTOM:
        value = tick_row.get("bid")
        if value is not None:
            return float(value)
    return price_series_mid(tick_row)


def departure_price_for_side(side: str, tick_row: Dict[str, Any]) -> Optional[float]:
    mid_value = price_series_mid(tick_row)
    if mid_value is not None:
        return float(mid_value)
    return executable_price_for_side(side, tick_row)


def departure_distance_for_side(
    side: str,
    *,
    low: float,
    high: float,
    price_value: Optional[float],
) -> float:
    if price_value is None:
        return 0.0
    if side == AREA_SIDE_TOP:
        return max(0.0, float(low) - float(price_value))
    return max(0.0, float(price_value) - float(high))


def priority_score(
    *,
    original_height: float,
    initial_departure_distance: float,
    is_level1_extreme: bool,
    is_level2_extreme: bool,
    touch_count: int,
) -> float:
    score = 10.0
    if is_level1_extreme:
        score += 25.0
    if is_level2_extreme:
        score += 50.0
    if original_height > 0:
        score += min(20.0, (float(initial_departure_distance) / float(original_height)) * 8.0)
    if int(touch_count or 0) == 0:
        score += 5.0
    return round(score, 6)


def update_priority(area: Dict[str, Any]) -> None:
    area["priorityscore"] = priority_score(
        original_height=float(area["originalheight"]),
        initial_departure_distance=float(area.get("initialdeparturedistance") or 0.0),
        is_level1_extreme=bool(area.get("isl1extreme")),
        is_level2_extreme=bool(area.get("isl2extreme")),
        touch_count=int(area.get("touchcount") or 0),
    )


def apply_level_qualification(area: Dict[str, Any], level: int) -> bool:
    next_l1, next_l2 = level_flags(level)
    changed = False
    if bool(area.get("isl1extreme")) != next_l1:
        area["isl1extreme"] = next_l1
        area["parentl1pivotid"] = int(area["sourcepivotid"]) if next_l1 else None
        changed = True
    if bool(area.get("isl2extreme")) != next_l2:
        area["isl2extreme"] = next_l2
        area["parentl2pivotid"] = int(area["sourcepivotid"]) if next_l2 else None
        changed = True
    if changed:
        update_priority(area)
    return changed


def build_area_from_candidate(
    *,
    symbol: str,
    candidate: Dict[str, Any],
    birth_tick: Dict[str, Any],
    source_level: int,
    break_ticks: int,
    break_tolerance: float,
) -> Optional[Dict[str, Any]]:
    pattern = str(candidate["pattern"])
    side = area_side_from_pattern(pattern)
    low = float(candidate["zone_low"])
    high = float(candidate["zone_high"])
    birth_price = departure_price_for_side(side, birth_tick)
    if birth_price is None:
        return None
    if side == AREA_SIDE_TOP and birth_price >= low:
        return None
    if side == AREA_SIDE_BOTTOM and birth_price <= high:
        return None

    source_pivot = dict(candidate["end_pivot"])
    is_level1_extreme, is_level2_extreme = level_flags(source_level)
    area = {
        "symbol": symbol,
        "pattern": pattern,
        "side": side,
        "state": AREA_STATE_ACTIVE,
        "sourcelevel": 0,
        "sourcepivotid": int(source_pivot["pivot_id"]),
        "startpivotid": int(candidate["start_pivot"]["pivot_id"]),
        "middlepivotid": int(candidate["middle_pivot"]["pivot_id"]),
        "endpivotid": int(candidate["end_pivot"]["pivot_id"]),
        "startpivottickid": int(candidate["start_pivot"]["source_tick_id"]),
        "middlepivottickid": int(candidate["middle_pivot"]["source_tick_id"]),
        "endpivottickid": int(candidate["end_pivot"]["source_tick_id"]),
        "startpivottime": candidate["start_pivot"]["source_timestamp"],
        "middlepivottime": candidate["middle_pivot"]["source_timestamp"],
        "endpivottime": candidate["end_pivot"]["source_timestamp"],
        "startpivotprice": float(candidate["start_pivot"]["pivot_price"]),
        "middlepivotprice": float(candidate["middle_pivot"]["pivot_price"]),
        "endpivotprice": float(candidate["end_pivot"]["pivot_price"]),
        "parentl1pivotid": int(source_pivot["pivot_id"]) if is_level1_extreme else None,
        "parentl2pivotid": int(source_pivot["pivot_id"]) if is_level2_extreme else None,
        "isl1extreme": is_level1_extreme,
        "isl2extreme": is_level2_extreme,
        "birthtickid": int(birth_tick["id"]),
        "birthtime": birth_tick["timestamp"],
        "originallow": low,
        "originalhigh": high,
        "currentlow": low,
        "currenthigh": high,
        "originalheight": high - low,
        "activeheight": high - low,
        "firsttouchtickid": None,
        "firsttouchtime": None,
        "fullusetickid": None,
        "fullusetime": None,
        "touchcount": 0,
        "maxpenetration": 0.0,
        "firstbreaktickid": None,
        "firstbreaktime": None,
        "closetickid": None,
        "closetime": None,
        "closereason": None,
        "priorityscore": 0.0,
        "initialdeparturedistance": departure_distance_for_side(
            side,
            low=low,
            high=high,
            price_value=birth_price,
        ),
        "untoucheddurationms": 0,
        "breakticksused": int(break_ticks),
        "breaktoleranceused": float(break_tolerance),
        "outsidestreak": 0,
        "outsidedirection": None,
        "insideactive": False,
    }
    update_priority(area)
    return area


def area_event_row(
    *,
    area: Dict[str, Any],
    event_type: str,
    tick_row: Dict[str, Any],
    price_value: Optional[float],
    penetration: Optional[float] = None,
    state_before: Optional[str] = None,
    state_after: Optional[str] = None,
    details: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "areaid": int(area["id"]),
        "symbol": area["symbol"],
        "eventtype": event_type,
        "tickid": int(tick_row["id"]),
        "eventtime": tick_row["timestamp"],
        "price": float(price_value) if price_value is not None else None,
        "lowprice": float(area["currentlow"]),
        "highprice": float(area["currenthigh"]),
        "penetration": float(penetration) if penetration is not None else None,
        "statebefore": state_before,
        "stateafter": state_after,
        "details": details,
    }


def sync_departure_metrics(area: Dict[str, Any], tick_row: Dict[str, Any]) -> bool:
    if area.get("firsttouchtime") is not None or str(area.get("state")) != AREA_STATE_ACTIVE:
        return False
    departure_price = departure_price_for_side(str(area["side"]), tick_row)
    next_distance = departure_distance_for_side(
        str(area["side"]),
        low=float(area["originallow"]),
        high=float(area["originalhigh"]),
        price_value=departure_price,
    )
    if next_distance <= float(area.get("initialdeparturedistance") or 0.0):
        return False
    area["initialdeparturedistance"] = float(next_distance)
    update_priority(area)
    return True


def freeze_untouched_duration(area: Dict[str, Any], touch_time: datetime) -> None:
    birth_time = area.get("birthtime")
    if birth_time is None:
        area["untoucheddurationms"] = 0
        return
    birth_ms = dt_to_ms(birth_time) or 0
    touch_ms = dt_to_ms(touch_time) or birth_ms
    area["untoucheddurationms"] = max(0, touch_ms - birth_ms)


def serialize_supresarea_row(row: Dict[str, Any]) -> Dict[str, Any]:
    birthtime = row.get("birthtime")
    firsttouchtime = row.get("firsttouchtime")
    fullusetime = row.get("fullusetime")
    firstbreaktime = row.get("firstbreaktime")
    closetime = row.get("closetime")
    display_low = float(row["currentlow"]) if str(row["state"]) == AREA_STATE_ACTIVE else float(row["originallow"])
    display_high = float(row["currenthigh"]) if str(row["state"]) == AREA_STATE_ACTIVE else float(row["originalhigh"])
    return {
        "id": int(row["id"]),
        "symbol": row["symbol"],
        "pattern": row["pattern"],
        "side": row["side"],
        "state": row["state"],
        "sourceLevel": int(row["sourcelevel"]),
        "sourcePivotId": int(row["sourcepivotid"]),
        "startPivotId": int(row["startpivotid"]),
        "middlePivotId": int(row["middlepivotid"]),
        "endPivotId": int(row["endpivotid"]),
        "startPivotTickId": int(row["startpivottickid"]),
        "middlePivotTickId": int(row["middlepivottickid"]),
        "endPivotTickId": int(row["endpivottickid"]),
        "parentL1PivotId": row.get("parentl1pivotid"),
        "parentL2PivotId": row.get("parentl2pivotid"),
        "isLevel1Extreme": bool(row.get("isl1extreme")),
        "isLevel2Extreme": bool(row.get("isl2extreme")),
        "birthTickId": int(row["birthtickid"]),
        "birthTime": birthtime.isoformat() if birthtime else None,
        "birthTimeMs": dt_to_ms(birthtime),
        "originalLow": round_price(float(row["originallow"])),
        "originalHigh": round_price(float(row["originalhigh"])),
        "currentLow": round_price(float(row["currentlow"])),
        "currentHigh": round_price(float(row["currenthigh"])),
        "displayLow": round_price(display_low),
        "displayHigh": round_price(display_high),
        "originalHeight": round_price(float(row["originalheight"])),
        "activeHeight": round_price(float(row["activeheight"])),
        "firstTouchTickId": row.get("firsttouchtickid"),
        "firstTouchTime": firsttouchtime.isoformat() if firsttouchtime else None,
        "firstTouchTimeMs": dt_to_ms(firsttouchtime),
        "fullUseTickId": row.get("fullusetickid"),
        "fullUseTime": fullusetime.isoformat() if fullusetime else None,
        "fullUseTimeMs": dt_to_ms(fullusetime),
        "touchCount": int(row.get("touchcount") or 0),
        "maxPenetration": round_price(float(row.get("maxpenetration") or 0.0)),
        "firstBreakTickId": row.get("firstbreaktickid"),
        "firstBreakTime": firstbreaktime.isoformat() if firstbreaktime else None,
        "firstBreakTimeMs": dt_to_ms(firstbreaktime),
        "closeTickId": row.get("closetickid"),
        "closeTime": closetime.isoformat() if closetime else None,
        "closeTimeMs": dt_to_ms(closetime),
        "closeReason": row.get("closereason"),
        "priorityScore": float(row.get("priorityscore") or 0.0),
        "initialDepartureDistance": round_price(float(row.get("initialdeparturedistance") or 0.0)),
        "untouchedDurationMs": int(row.get("untoucheddurationms") or 0),
        "breakTicksUsed": int(row.get("breakticksused") or 0),
        "breakToleranceUsed": round_price(float(row.get("breaktoleranceused") or 0.0)),
    }
