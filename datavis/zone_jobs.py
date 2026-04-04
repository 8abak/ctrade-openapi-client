#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg2.extras

from datavis.db import db_connect
from datavis.zonebox import (
    ZONE_STATE_ACTIVE,
    ZONE_STATE_CLOSED,
    ZONE_STATE_PROVISIONAL,
    dt_to_ms,
    pivot_breakout_direction,
    price_series_mid,
    zone_birth_candidate,
    zone_contains_with_tolerance,
    zone_touch_side,
)


TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
POLL_SECONDS = max(0.02, float(os.getenv("ZONEBOX_POLL_SECONDS", "0.05")))
IDLE_POLL_SECONDS = max(POLL_SECONDS, float(os.getenv("ZONEBOX_IDLE_POLL_SECONDS", "0.20")))
BATCH_SIZE = max(1, int(os.getenv("ZONEBOX_BATCH_SIZE", "500")))
MAX_LEVEL = 3
VISIBLE_PIVOT_TAIL = 12
DEFAULT_REVIEW_TIMEZONE = os.getenv("DATAVIS_ZONE_TIMEZONE", "Australia/Sydney")
DEFAULT_ZONE_MIN_DWELL_TICKS = int(os.getenv("DATAVIS_ZONE_MIN_DWELL_TICKS", "24"))
DEFAULT_ZONE_MIN_DWELL_MS = int(os.getenv("DATAVIS_ZONE_MIN_DWELL_MS", "3000"))
DEFAULT_ZONE_SAME_SIDE_TOLERANCE = float(os.getenv("DATAVIS_ZONE_SAME_SIDE_TOLERANCE", "0.24"))
DEFAULT_ZONE_ALLOWED_OVERSHOOT = float(os.getenv("DATAVIS_ZONE_ALLOWED_OVERSHOOT", "0.18"))
DEFAULT_ZONE_BREAKOUT_TICKS = int(os.getenv("DATAVIS_ZONE_BREAKOUT_TICKS", "4"))
DEFAULT_ZONE_BREAKOUT_TOLERANCE = float(os.getenv("DATAVIS_ZONE_BREAKOUT_TOLERANCE", "0.24"))
DEFAULT_ZONE_MIN_HEIGHT = float(os.getenv("DATAVIS_ZONE_MIN_HEIGHT", "0.05"))
DEFAULT_ZONE_MAX_HEIGHT = float(os.getenv("DATAVIS_ZONE_MAX_HEIGHT", "1.60"))
PRICE_SOURCE = "mid"

REQUIRED_FAST_ZIG_COLUMNS = {
    "pivot_id",
    "symbol",
    "source_tick_id",
    "source_timestamp",
    "direction",
    "pivot_price",
    "level",
    "state",
    "visible_from_tick_id",
    "visible_to_tick_id",
}
REQUIRED_ZONEBOX_COLUMNS = {
    "id",
    "symbol",
    "level",
    "state",
    "pattern",
    "startpivotid",
    "middlepivotid",
    "endpivotid",
    "starttickid",
    "zonehigh",
    "zonelow",
    "lasttickid",
    "updated_at",
}
REQUIRED_ZONEBOXSTATE_COLUMNS = {
    "id",
    "symbol",
    "level",
    "lastprocessedtickid",
    "lastprocessedpivotid",
    "activezoneid",
    "updated_at",
}

STOP = False


def shutdown(*_: Any) -> None:
    global STOP
    STOP = True


def current_zone_settings() -> Dict[str, Any]:
    return {
        "minDwellTicks": DEFAULT_ZONE_MIN_DWELL_TICKS,
        "minDwellMs": DEFAULT_ZONE_MIN_DWELL_MS,
        "sameSideTolerance": DEFAULT_ZONE_SAME_SIDE_TOLERANCE,
        "allowedOvershoot": DEFAULT_ZONE_ALLOWED_OVERSHOOT,
        "breakoutTicks": DEFAULT_ZONE_BREAKOUT_TICKS,
        "breakoutTolerance": DEFAULT_ZONE_BREAKOUT_TOLERANCE,
        "minHeight": DEFAULT_ZONE_MIN_HEIGHT,
        "maxHeight": DEFAULT_ZONE_MAX_HEIGHT,
    }


def parse_time_argument(value: str, timezone_name: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    target_tz = ZoneInfo(timezone_name)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=target_tz)
    else:
        parsed = parsed.astimezone(target_tz)
    return parsed.astimezone(timezone.utc)


def fast_zig_event_columns_sql() -> str:
    return """
        version_id,
        pivot_id,
        symbol,
        source_tick_id,
        source_timestamp,
        direction,
        pivot_price,
        level,
        state,
        visible_from_tick_id,
        visible_to_tick_id
    """


def ensure_storage_ready(conn: Any) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                to_regclass('public.fast_zig_pivots') AS fast_zig_pivots,
                to_regclass('public.zonebox') AS zonebox,
                to_regclass('public.zoneboxstate') AS zoneboxstate
            """
        )
        row = dict(cur.fetchone() or {})
        if not row.get("fast_zig_pivots") or not row.get("zonebox") or not row.get("zoneboxstate"):
            raise RuntimeError(
                "zone storage is missing; apply deploy/sql/20260403_fast_zig.sql, deploy/sql/20260404_fast_zig_levels.sql, and deploy/sql/20260405_zonebox.sql first"
            )
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('fast_zig_pivots', 'zonebox', 'zoneboxstate')
            """
        )
        columns: Dict[str, set[str]] = {
            "fast_zig_pivots": set(),
            "zonebox": set(),
            "zoneboxstate": set(),
        }
        for info in cur.fetchall():
            columns.setdefault(info["table_name"], set()).add(info["column_name"])
    if not REQUIRED_FAST_ZIG_COLUMNS.issubset(columns["fast_zig_pivots"]):
        missing = sorted(REQUIRED_FAST_ZIG_COLUMNS - columns["fast_zig_pivots"])
        raise RuntimeError("fast_zig_pivots schema is incomplete; missing columns: {0}".format(", ".join(missing)))
    if not REQUIRED_ZONEBOX_COLUMNS.issubset(columns["zonebox"]):
        missing = sorted(REQUIRED_ZONEBOX_COLUMNS - columns["zonebox"])
        raise RuntimeError("zonebox schema is incomplete; missing columns: {0}".format(", ".join(missing)))
    if not REQUIRED_ZONEBOXSTATE_COLUMNS.issubset(columns["zoneboxstate"]):
        missing = sorted(REQUIRED_ZONEBOXSTATE_COLUMNS - columns["zoneboxstate"])
        raise RuntimeError("zoneboxstate schema is incomplete; missing columns: {0}".format(", ".join(missing)))


def resolve_tick_at_time(cur: Any, symbol: str, timestamp_value: datetime) -> Optional[int]:
    cur.execute(
        """
        SELECT id, timestamp
        FROM public.ticks
        WHERE symbol = %s AND timestamp >= %s
        ORDER BY timestamp ASC, id ASC
        LIMIT 1
        """,
        (symbol, timestamp_value),
    )
    next_row = cur.fetchone()
    cur.execute(
        """
        SELECT id, timestamp
        FROM public.ticks
        WHERE symbol = %s AND timestamp < %s
        ORDER BY timestamp DESC, id DESC
        LIMIT 1
        """,
        (symbol, timestamp_value),
    )
    previous_row = cur.fetchone()
    if not previous_row and not next_row:
        return None
    if previous_row and next_row:
        previous_delta = abs((timestamp_value - previous_row["timestamp"]).total_seconds())
        next_delta = abs((next_row["timestamp"] - timestamp_value).total_seconds())
        return int(next_row["id"] if next_delta < previous_delta else previous_row["id"])
    resolved = next_row or previous_row
    return int(resolved["id"])


def fetch_latest_tick_id(cur: Any, symbol: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(MAX(id), 0) AS last_id
        FROM public.ticks
        WHERE symbol = %s
        """,
        (symbol,),
    )
    row = cur.fetchone() or {}
    return int(row.get("last_id") or 0)


def fetch_latest_pivot_id_before_tick(cur: Any, symbol: str, tick_id: int) -> int:
    cur.execute(
        """
        SELECT COALESCE(MAX(pivot_id), 0) AS pivot_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND visible_from_tick_id <= %s
        """,
        (symbol, tick_id),
    )
    row = cur.fetchone() or {}
    return int(row.get("pivot_id") or 0)


def fetch_ticks_after(cur: Any, symbol: str, after_id: int, limit: int, upto_id: Optional[int] = None) -> List[Dict[str, Any]]:
    if upto_id is None:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, after_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s AND id > %s AND id <= %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, after_id, upto_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def fetch_pivot_events(cur: Any, symbol: str, after_tick_id: int, upto_tick_id: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {columns}
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND visible_from_tick_id > %s
          AND visible_from_tick_id <= %s
        ORDER BY visible_from_tick_id ASC, pivot_id ASC, version_id ASC
        """.format(columns=fast_zig_event_columns_sql()),
        (symbol, after_tick_id, upto_tick_id),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_visible_selected_pivot_tail(
    cur: Any,
    *,
    symbol: str,
    level: int,
    cursor_tick_id: int,
    limit: int = VISIBLE_PIVOT_TAIL,
) -> List[Dict[str, Any]]:
    if cursor_tick_id <= 0:
        return []
    cur.execute(
        """
        SELECT *
        FROM (
            SELECT DISTINCT ON (pivot_id)
                {columns}
            FROM public.fast_zig_pivots
            WHERE symbol = %s
              AND visible_from_tick_id <= %s
              AND (visible_to_tick_id IS NULL OR visible_to_tick_id >= %s)
            ORDER BY pivot_id DESC, version_id DESC
            LIMIT %s
        ) current_pivots
        WHERE level >= %s
        ORDER BY pivot_id ASC
        """.format(columns=fast_zig_event_columns_sql()),
        (symbol, cursor_tick_id, cursor_tick_id, limit * 3, level),
    )
    rows = [dict(row) for row in cur.fetchall()]
    return rows[-limit:]


def load_zone_state(cur: Any, *, symbol: str, level: int) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT id, symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid, updated_at
        FROM public.zoneboxstate
        WHERE symbol = %s AND level = %s
        """,
        (symbol, level),
    )
    row = cur.fetchone()
    if row:
        return dict(row)
    cur.execute(
        """
        INSERT INTO public.zoneboxstate (symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid, updated_at)
        VALUES (%s, %s, 0, 0, NULL, NOW())
        RETURNING id, symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid, updated_at
        """,
        (symbol, level),
    )
    return dict(cur.fetchone())


def store_zone_state(
    cur: Any,
    *,
    symbol: str,
    level: int,
    lastprocessedtickid: int,
    lastprocessedpivotid: int,
    activezoneid: Optional[int],
) -> None:
    cur.execute(
        """
        INSERT INTO public.zoneboxstate (symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (symbol, level) DO UPDATE
        SET lastprocessedtickid = EXCLUDED.lastprocessedtickid,
            lastprocessedpivotid = EXCLUDED.lastprocessedpivotid,
            activezoneid = EXCLUDED.activezoneid,
            updated_at = NOW()
        """,
        (symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid),
    )


def fetch_active_zone(cur: Any, activezoneid: Optional[int]) -> Optional[Dict[str, Any]]:
    if activezoneid is None:
        return None
    cur.execute(
        "SELECT * FROM public.zonebox WHERE id = %s",
        (activezoneid,),
    )
    row = cur.fetchone()
    if not row:
        return None
    payload = dict(row)
    if payload.get("state") == ZONE_STATE_CLOSED:
        return None
    return payload


def insert_zone(cur: Any, zone: Dict[str, Any]) -> int:
    cur.execute(
        """
        INSERT INTO public.zonebox (
            symbol, level, state, pattern, pricesource,
            startpivotid, middlepivotid, endpivotid,
            startpivottickid, middlepivottickid, endpivottickid,
            startpivottime, middlepivottime, endpivottime,
            startpivotprice, middlepivotprice, endpivotprice,
            starttickid, endtickid, starttime, endtime,
            initialzonehigh, initialzonelow, zonehigh, zonelow, zoneheight,
            samesidedistance, samesidetoleranceused,
            tickcountinside, durationms,
            continuationovershootused, breakticksused, breaktoleranceused,
            breakdirection, breaktickid,
            lasttickid, lasttime, lastinsidetickid, lastinsidetime,
            touchcount, revisitcount, lasttouchside,
            outsidestreak, outsidedirection,
            created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            NOW(), NOW()
        )
        RETURNING id
        """,
        (
            zone["symbol"], zone["level"], zone["state"], zone["pattern"], zone["pricesource"],
            zone["startpivotid"], zone["middlepivotid"], zone["endpivotid"],
            zone["startpivottickid"], zone["middlepivottickid"], zone["endpivottickid"],
            zone["startpivottime"], zone["middlepivottime"], zone["endpivottime"],
            zone["startpivotprice"], zone["middlepivotprice"], zone["endpivotprice"],
            zone["starttickid"], zone["endtickid"], zone["starttime"], zone["endtime"],
            zone["initialzonehigh"], zone["initialzonelow"], zone["zonehigh"], zone["zonelow"], zone["zoneheight"],
            zone["samesidedistance"], zone["samesidetoleranceused"],
            zone["tickcountinside"], zone["durationms"],
            zone["continuationovershootused"], zone["breakticksused"], zone["breaktoleranceused"],
            zone["breakdirection"], zone["breaktickid"],
            zone["lasttickid"], zone["lasttime"], zone["lastinsidetickid"], zone["lastinsidetime"],
            zone["touchcount"], zone["revisitcount"], zone["lasttouchside"],
            zone["outsidestreak"], zone["outsidedirection"],
        ),
    )
    return int(cur.fetchone()["id"])


def update_zone(cur: Any, zone: Dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE public.zonebox
        SET state = %s,
            endtickid = %s,
            endtime = %s,
            zonehigh = %s,
            zonelow = %s,
            zoneheight = %s,
            tickcountinside = %s,
            durationms = %s,
            breakdirection = %s,
            breaktickid = %s,
            lasttickid = %s,
            lasttime = %s,
            lastinsidetickid = %s,
            lastinsidetime = %s,
            touchcount = %s,
            revisitcount = %s,
            lasttouchside = %s,
            outsidestreak = %s,
            outsidedirection = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            zone["state"],
            zone["endtickid"],
            zone["endtime"],
            zone["zonehigh"],
            zone["zonelow"],
            zone["zoneheight"],
            zone["tickcountinside"],
            zone["durationms"],
            zone["breakdirection"],
            zone["breaktickid"],
            zone["lasttickid"],
            zone["lasttime"],
            zone["lastinsidetickid"],
            zone["lastinsidetime"],
            zone["touchcount"],
            zone["revisitcount"],
            zone["lasttouchside"],
            zone["outsidestreak"],
            zone["outsidedirection"],
            zone["id"],
        ),
    )


def sync_visible_pivots(visible_pivots: List[Dict[str, Any]], event: Dict[str, Any], selected_level: int) -> bool:
    pivot_id = int(event["pivot_id"])
    existing_index = next((index for index, pivot in enumerate(visible_pivots) if int(pivot["pivot_id"]) == pivot_id), -1)
    if existing_index >= 0:
        visible_pivots.pop(existing_index)
    if int(event["level"]) >= selected_level:
        visible_pivots.append(dict(event))
        visible_pivots.sort(key=lambda pivot: int(pivot["pivot_id"]))
        if len(visible_pivots) > VISIBLE_PIVOT_TAIL:
            del visible_pivots[:-VISIBLE_PIVOT_TAIL]
        return True
    return False


def create_zone(
    *,
    symbol: str,
    level: int,
    candidate: Dict[str, Any],
    tick_row: Dict[str, Any],
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    tick_id = int(tick_row["id"])
    tick_time = tick_row["timestamp"]
    zone_high = float(candidate["zone_high"])
    zone_low = float(candidate["zone_low"])
    return {
        "symbol": symbol,
        "level": level,
        "state": ZONE_STATE_PROVISIONAL,
        "pattern": candidate["pattern"],
        "pricesource": PRICE_SOURCE,
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
        "starttickid": tick_id,
        "endtickid": None,
        "starttime": tick_time,
        "endtime": None,
        "initialzonehigh": zone_high,
        "initialzonelow": zone_low,
        "zonehigh": zone_high,
        "zonelow": zone_low,
        "zoneheight": zone_high - zone_low,
        "samesidedistance": float(candidate["same_side_distance"]),
        "samesidetoleranceused": float(settings["sameSideTolerance"]),
        "tickcountinside": 1,
        "durationms": 0,
        "continuationovershootused": float(settings["allowedOvershoot"]),
        "breakticksused": int(settings["breakoutTicks"]),
        "breaktoleranceused": float(settings["breakoutTolerance"]),
        "breakdirection": None,
        "breaktickid": None,
        "lasttickid": tick_id,
        "lasttime": tick_time,
        "lastinsidetickid": tick_id,
        "lastinsidetime": tick_time,
        "touchcount": 0,
        "revisitcount": 0,
        "lasttouchside": None,
        "outsidestreak": 0,
        "outsidedirection": None,
    }


def maybe_promote_zone(zone: Dict[str, Any], settings: Dict[str, Any]) -> None:
    if zone["state"] != ZONE_STATE_PROVISIONAL:
        return
    start_ms = dt_to_ms(zone["starttime"]) or 0
    last_inside_ms = dt_to_ms(zone["lastinsidetime"]) or start_ms
    if (
        int(zone["tickcountinside"]) >= int(settings["minDwellTicks"])
        and last_inside_ms - start_ms >= int(settings["minDwellMs"])
    ):
        zone["state"] = ZONE_STATE_ACTIVE


def update_zone_from_tick(
    zone: Dict[str, Any],
    *,
    tick_row: Dict[str, Any],
    price_value: float,
    settings: Dict[str, Any],
    pivot_confirm_direction: Optional[str],
) -> bool:
    tick_id = int(tick_row["id"])
    tick_time = tick_row["timestamp"]
    zone["lasttickid"] = tick_id
    zone["lasttime"] = tick_time

    if zone_contains_with_tolerance(
        price_value,
        float(zone["zonelow"]),
        float(zone["zonehigh"]),
        float(zone["continuationovershootused"]),
    ):
        if int(zone["outsidestreak"]) > 0:
            zone["revisitcount"] = int(zone["revisitcount"]) + 1
        zone["outsidestreak"] = 0
        zone["outsidedirection"] = None
        zone["tickcountinside"] = int(zone["tickcountinside"]) + 1
        zone["lastinsidetickid"] = tick_id
        zone["lastinsidetime"] = tick_time
        zone["durationms"] = max(0, (dt_to_ms(tick_time) or 0) - (dt_to_ms(zone["starttime"]) or 0))
        touch_side = zone_touch_side(price_value, float(zone["zonelow"]), float(zone["zonehigh"]), max(float(zone["continuationovershootused"]) * 0.6, 0.01))
        if touch_side and touch_side != zone.get("lasttouchside"):
            zone["touchcount"] = int(zone["touchcount"]) + 1
        zone["lasttouchside"] = touch_side
        maybe_promote_zone(zone, settings)
        return False

    breakout_direction: Optional[str] = None
    if price_value > float(zone["zonehigh"]) + float(zone["breaktoleranceused"]):
        breakout_direction = "up"
    elif price_value < float(zone["zonelow"]) - float(zone["breaktoleranceused"]):
        breakout_direction = "down"

    if breakout_direction is None:
        zone["outsidestreak"] = 0
        zone["outsidedirection"] = None
        zone["lasttouchside"] = None
        return False

    if zone.get("outsidedirection") == breakout_direction:
        zone["outsidestreak"] = int(zone["outsidestreak"]) + 1
    else:
        zone["outsidedirection"] = breakout_direction
        zone["outsidestreak"] = 1

    if int(zone["outsidestreak"]) < int(zone["breakticksused"]) and pivot_confirm_direction != breakout_direction:
        return False

    zone["state"] = ZONE_STATE_CLOSED
    zone["breakdirection"] = breakout_direction
    zone["breaktickid"] = tick_id
    zone["endtickid"] = tick_id
    zone["endtime"] = tick_time
    zone["lasttime"] = tick_time
    return True


def process_level_batch(
    cur: Any,
    *,
    symbol: str,
    level: int,
    state_row: Dict[str, Any],
    tick_rows: List[Dict[str, Any]],
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    if not tick_rows:
        return state_row

    lastprocessedtickid = int(state_row["lastprocessedtickid"] or 0)
    lastprocessedpivotid = int(state_row["lastprocessedpivotid"] or 0)
    visible_pivots = fetch_visible_selected_pivot_tail(
        cur,
        symbol=symbol,
        level=level,
        cursor_tick_id=lastprocessedtickid,
    )
    pivot_events = fetch_pivot_events(cur, symbol, lastprocessedtickid, int(tick_rows[-1]["id"]))
    event_index = 0
    active_zone = fetch_active_zone(cur, state_row.get("activezoneid"))

    for tick_row in tick_rows:
        tick_id = int(tick_row["id"])
        price_value = price_series_mid(tick_row)
        if price_value is None:
            lastprocessedtickid = tick_id
            continue

        pivot_confirm_direction: Optional[str] = None
        zone_born_this_tick = False
        while event_index < len(pivot_events) and int(pivot_events[event_index]["visible_from_tick_id"]) <= tick_id:
            event = pivot_events[event_index]
            lastprocessedpivotid = max(lastprocessedpivotid, int(event["pivot_id"]))
            qualifies_for_level = sync_visible_pivots(visible_pivots, event, level)
            if active_zone is not None and qualifies_for_level:
                pivot_direction = pivot_breakout_direction(
                    event,
                    low=float(active_zone["zonelow"]),
                    high=float(active_zone["zonehigh"]),
                    tolerance=float(active_zone["continuationovershootused"]),
                )
                if pivot_direction is not None:
                    pivot_confirm_direction = pivot_direction
            if active_zone is None and qualifies_for_level and len(visible_pivots) >= 3:
                candidate = zone_birth_candidate(
                    visible_pivots[-3:],
                    same_side_tolerance=float(settings["sameSideTolerance"]),
                    min_height=float(settings["minHeight"]),
                    max_height=float(settings["maxHeight"]),
                )
                if candidate is not None and zone_contains_with_tolerance(
                    float(price_value),
                    float(candidate["zone_low"]),
                    float(candidate["zone_high"]),
                    float(settings["allowedOvershoot"]),
                ):
                    active_zone = create_zone(
                        symbol=symbol,
                        level=level,
                        candidate=candidate,
                        tick_row=tick_row,
                        settings=settings,
                    )
                    touch_side = zone_touch_side(
                        float(price_value),
                        float(active_zone["zonelow"]),
                        float(active_zone["zonehigh"]),
                        max(float(active_zone["continuationovershootused"]) * 0.6, 0.01),
                    )
                    if touch_side:
                        active_zone["touchcount"] = 1
                        active_zone["lasttouchside"] = touch_side
                    maybe_promote_zone(active_zone, settings)
                    active_zone["id"] = insert_zone(cur, active_zone)
                    zone_born_this_tick = True
            event_index += 1

        if active_zone is not None and not zone_born_this_tick:
            closed = update_zone_from_tick(
                active_zone,
                tick_row=tick_row,
                price_value=float(price_value),
                settings=settings,
                pivot_confirm_direction=pivot_confirm_direction,
            )
            update_zone(cur, active_zone)
            if closed:
                active_zone = None

        lastprocessedtickid = tick_id

    store_zone_state(
        cur,
        symbol=symbol,
        level=level,
        lastprocessedtickid=lastprocessedtickid,
        lastprocessedpivotid=lastprocessedpivotid,
        activezoneid=int(active_zone["id"]) if active_zone is not None else None,
    )
    state_row["lastprocessedtickid"] = lastprocessedtickid
    state_row["lastprocessedpivotid"] = lastprocessedpivotid
    state_row["activezoneid"] = int(active_zone["id"]) if active_zone is not None else None
    return state_row


def process_level_once(
    cur: Any,
    *,
    symbol: str,
    level: int,
    settings: Dict[str, Any],
    upto_tick_id: Optional[int] = None,
) -> int:
    state_row = load_zone_state(cur, symbol=symbol, level=level)
    tick_rows = fetch_ticks_after(
        cur,
        symbol=symbol,
        after_id=int(state_row["lastprocessedtickid"] or 0),
        limit=BATCH_SIZE,
        upto_id=upto_tick_id,
    )
    if not tick_rows:
        return 0
    process_level_batch(
        cur,
        symbol=symbol,
        level=level,
        state_row=state_row,
        tick_rows=tick_rows,
        settings=settings,
    )
    return len(tick_rows)


def reset_level_state(
    cur: Any,
    *,
    symbol: str,
    level: int,
    from_tick_id: Optional[int] = None,
) -> None:
    load_zone_state(cur, symbol=symbol, level=level)
    if from_tick_id is None:
        cur.execute(
            """
            UPDATE public.zoneboxstate
            SET activezoneid = NULL,
                lastprocessedtickid = 0,
                lastprocessedpivotid = 0,
                updated_at = NOW()
            WHERE symbol = %s AND level = %s
            """,
            (symbol, level),
        )
        cur.execute(
            "DELETE FROM public.zonebox WHERE symbol = %s AND level = %s",
            (symbol, level),
        )
        return

    reset_tick_id = max(0, int(from_tick_id) - 1)
    prior_pivot_id = fetch_latest_pivot_id_before_tick(cur, symbol, reset_tick_id)
    cur.execute(
        """
        UPDATE public.zoneboxstate
        SET activezoneid = NULL,
            lastprocessedtickid = %s,
            lastprocessedpivotid = %s,
            updated_at = NOW()
        WHERE symbol = %s AND level = %s
        """,
        (reset_tick_id, prior_pivot_id, symbol, level),
    )
    cur.execute(
        """
        DELETE FROM public.zonebox
        WHERE symbol = %s
          AND level = %s
          AND lasttickid >= %s
        """,
        (symbol, level, from_tick_id),
    )


def log_progress(*, symbol: str, level: int, processed: int, last_tick_id: int, last_pivot_id: int, batch_ms: float) -> None:
    print(
        "zonebox stats symbol={0} level={1} tick={2} pivots={3} batch={4} batch_ms={5:.2f}".format(
            symbol,
            level,
            last_tick_id,
            last_pivot_id,
            processed,
            batch_ms,
        ),
        flush=True,
    )


def run_loop(symbol: str, levels: List[int]) -> None:
    settings = current_zone_settings()
    last_log = time.time()
    idle_sleep = POLL_SECONDS

    while not STOP:
        conn = None
        try:
            conn = db_connect()
            conn.autocommit = False
            ensure_storage_ready(conn)

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                processed_any = False
                for level in levels:
                    batch_started = time.perf_counter()
                    processed = process_level_once(cur, symbol=symbol, level=level, settings=settings)
                    if not processed:
                        continue
                    processed_any = True
                    state_row = load_zone_state(cur, symbol=symbol, level=level)
                    if time.time() - last_log >= 5.0:
                        log_progress(
                            symbol=symbol,
                            level=level,
                            processed=processed,
                            last_tick_id=int(state_row["lastprocessedtickid"] or 0),
                            last_pivot_id=int(state_row["lastprocessedpivotid"] or 0),
                            batch_ms=(time.perf_counter() - batch_started) * 1000.0,
                        )
                        last_log = time.time()
                conn.commit()

            if processed_any:
                idle_sleep = POLL_SECONDS
                continue
            time.sleep(idle_sleep)
            idle_sleep = IDLE_POLL_SECONDS
        except Exception as exc:
            print("zonebox error: {0}".format(exc), flush=True)
            try:
                if conn and not conn.closed:
                    conn.rollback()
            except Exception:
                pass
            time.sleep(1.0)
        finally:
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass


def parse_levels(raw_level: Optional[str]) -> List[int]:
    if raw_level is None or raw_level.lower() == "all":
        return list(range(0, MAX_LEVEL + 1))
    level = int(raw_level)
    if level < 0 or level > MAX_LEVEL:
        raise ValueError("level must be between 0 and {0}".format(MAX_LEVEL))
    return [level]


def run_reset(symbol: str, levels: List[int], from_time: Optional[str], timezone_name: str) -> None:
    conn = db_connect()
    conn.autocommit = False
    try:
        ensure_storage_ready(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            from_tick_id = None
            if from_time:
                resolved = resolve_tick_at_time(cur, symbol, parse_time_argument(from_time, timezone_name))
                if resolved is None:
                    raise RuntimeError("No ticks found for reset window.")
                from_tick_id = resolved
            for level in levels:
                reset_level_state(cur, symbol=symbol, level=level, from_tick_id=from_tick_id)
        conn.commit()
    finally:
        conn.close()


def run_rebuild(symbol: str, levels: List[int], start_time: str, end_time: Optional[str], timezone_name: str) -> None:
    conn = db_connect()
    conn.autocommit = False
    try:
        ensure_storage_ready(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            start_tick_id = resolve_tick_at_time(cur, symbol, parse_time_argument(start_time, timezone_name))
            if start_tick_id is None:
                raise RuntimeError("No ticks found for rebuild start.")
            if end_time:
                end_tick_id = resolve_tick_at_time(cur, symbol, parse_time_argument(end_time, timezone_name))
                if end_tick_id is None:
                    raise RuntimeError("No ticks found for rebuild end.")
            else:
                end_tick_id = fetch_latest_tick_id(cur, symbol)
            if end_tick_id < start_tick_id:
                raise RuntimeError("Rebuild end must be at or after rebuild start.")
            for level in levels:
                reset_level_state(cur, symbol=symbol, level=level, from_tick_id=start_tick_id)
            conn.commit()

            settings = current_zone_settings()
            for level in levels:
                while True:
                    processed = process_level_once(
                        cur,
                        symbol=symbol,
                        level=level,
                        settings=settings,
                        upto_tick_id=end_tick_id,
                    )
                    conn.commit()
                    if not processed:
                        break
                    state_row = load_zone_state(cur, symbol=symbol, level=level)
                    if int(state_row["lastprocessedtickid"] or 0) >= end_tick_id:
                        break
        conn.commit()
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persist and rebuild mini-zone episodes from ticks + fast zig pivots.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_loop_parser = subparsers.add_parser("run-loop", help="Run the incremental zonebox polling loop.")
    run_loop_parser.add_argument("--symbol", default=TICK_SYMBOL)
    run_loop_parser.add_argument("--level", default="all")

    reset_parser = subparsers.add_parser("reset", help="Clear persisted zone rows for a stream, optionally from a chosen time onward.")
    reset_parser.add_argument("--symbol", default=TICK_SYMBOL)
    reset_parser.add_argument("--level", default="all")
    reset_parser.add_argument("--from-time", dest="from_time")
    reset_parser.add_argument("--timezone", default=DEFAULT_REVIEW_TIMEZONE)

    rebuild_parser = subparsers.add_parser("rebuild", help="Reset from a chosen start time and rebuild up to an end time.")
    rebuild_parser.add_argument("--symbol", default=TICK_SYMBOL)
    rebuild_parser.add_argument("--level", default="all")
    rebuild_parser.add_argument("--start-time", required=True)
    rebuild_parser.add_argument("--end-time")
    rebuild_parser.add_argument("--timezone", default=DEFAULT_REVIEW_TIMEZONE)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    levels = parse_levels(getattr(args, "level", "all"))

    if args.command == "run-loop":
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
        run_loop(symbol=args.symbol, levels=levels)
        return
    if args.command == "reset":
        run_reset(symbol=args.symbol, levels=levels, from_time=args.from_time, timezone_name=args.timezone)
        return
    if args.command == "rebuild":
        run_rebuild(
            symbol=args.symbol,
            levels=levels,
            start_time=args.start_time,
            end_time=args.end_time,
            timezone_name=args.timezone,
        )
        return
    parser.error("Unknown command.")


if __name__ == "__main__":
    main()
