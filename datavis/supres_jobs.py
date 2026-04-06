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
from datavis.supresarea import (
    AREA_SIDE_TOP,
    AREA_STATE_ACTIVE,
    AREA_STATE_CLOSED,
    AREA_STATE_USED,
    apply_level_qualification,
    area_event_row,
    build_area_from_candidate,
    executable_price_for_side,
    freeze_untouched_duration,
    sync_departure_metrics,
    update_priority,
)
from datavis.zonebox import price_series_mid, zone_birth_candidate


TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
POLL_SECONDS = max(0.02, float(os.getenv("SUPRES_POLL_SECONDS", "0.05")))
IDLE_POLL_SECONDS = max(POLL_SECONDS, float(os.getenv("SUPRES_IDLE_POLL_SECONDS", "0.20")))
DEFAULT_BATCH_SIZE = max(1, int(os.getenv("SUPRES_BATCH_SIZE", "500")))
DEFAULT_REVIEW_TIMEZONE = os.getenv("DATAVIS_SUPRES_TIMEZONE", "Australia/Sydney")
DEFAULT_SAME_SIDE_TOLERANCE = float(os.getenv("DATAVIS_ZONE_SAME_SIDE_TOLERANCE", "0.24"))
DEFAULT_MIN_HEIGHT = float(os.getenv("DATAVIS_ZONE_MIN_HEIGHT", "0.05"))
DEFAULT_MAX_HEIGHT = float(os.getenv("DATAVIS_ZONE_MAX_HEIGHT", "1.60"))
DEFAULT_BREAK_TICKS = int(os.getenv("DATAVIS_SUPRES_BREAK_TICKS", os.getenv("DATAVIS_ZONE_BREAKOUT_TICKS", "4")))
DEFAULT_BREAK_TOLERANCE = float(os.getenv("DATAVIS_SUPRES_BREAK_TOLERANCE", os.getenv("DATAVIS_ZONE_BREAKOUT_TOLERANCE", "0.24")))
VISIBLE_PIVOT_TAIL = 12

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
REQUIRED_SUPRESAREA_COLUMNS = {
    "id",
    "symbol",
    "side",
    "state",
    "sourcepivotid",
    "birthtickid",
    "originallow",
    "originalhigh",
    "currentlow",
    "currenthigh",
    "activeheight",
    "updated_at",
}
REQUIRED_SUPRESSTATE_COLUMNS = {
    "symbol",
    "lastprocessedtickid",
    "lastprocessedpivotid",
    "updated_at",
}
REQUIRED_SUPRESAREAEVENT_COLUMNS = {
    "id",
    "areaid",
    "eventtype",
    "tickid",
    "eventtime",
}

STOP = False


def shutdown(*_: Any) -> None:
    global STOP
    STOP = True


def current_settings() -> Dict[str, Any]:
    return {
        "sameSideTolerance": float(DEFAULT_SAME_SIDE_TOLERANCE),
        "minHeight": float(DEFAULT_MIN_HEIGHT),
        "maxHeight": float(DEFAULT_MAX_HEIGHT),
        "breakTicks": int(DEFAULT_BREAK_TICKS),
        "breakTolerance": float(DEFAULT_BREAK_TOLERANCE),
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
                to_regclass('public.supresarea') AS supresarea,
                to_regclass('public.supresareaevent') AS supresareaevent,
                to_regclass('public.supresstate') AS supresstate
            """
        )
        row = dict(cur.fetchone() or {})
        if not row.get("fast_zig_pivots") or not row.get("supresarea") or not row.get("supresareaevent") or not row.get("supresstate"):
            raise RuntimeError(
                "unused-area storage is missing; apply deploy/sql/20260403_fast_zig.sql, deploy/sql/20260404_fast_zig_levels.sql, and deploy/sql/20260406_supresarea.sql first"
            )
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('fast_zig_pivots', 'supresarea', 'supresareaevent', 'supresstate')
            """
        )
        columns: Dict[str, set[str]] = {
            "fast_zig_pivots": set(),
            "supresarea": set(),
            "supresareaevent": set(),
            "supresstate": set(),
        }
        for info in cur.fetchall():
            columns.setdefault(info["table_name"], set()).add(info["column_name"])
    if not REQUIRED_FAST_ZIG_COLUMNS.issubset(columns["fast_zig_pivots"]):
        missing = sorted(REQUIRED_FAST_ZIG_COLUMNS - columns["fast_zig_pivots"])
        raise RuntimeError("fast_zig_pivots schema is incomplete; missing columns: {0}".format(", ".join(missing)))
    if not REQUIRED_SUPRESAREA_COLUMNS.issubset(columns["supresarea"]):
        missing = sorted(REQUIRED_SUPRESAREA_COLUMNS - columns["supresarea"])
        raise RuntimeError("supresarea schema is incomplete; missing columns: {0}".format(", ".join(missing)))
    if not REQUIRED_SUPRESAREAEVENT_COLUMNS.issubset(columns["supresareaevent"]):
        missing = sorted(REQUIRED_SUPRESAREAEVENT_COLUMNS - columns["supresareaevent"])
        raise RuntimeError("supresareaevent schema is incomplete; missing columns: {0}".format(", ".join(missing)))
    if not REQUIRED_SUPRESSTATE_COLUMNS.issubset(columns["supresstate"]):
        missing = sorted(REQUIRED_SUPRESSTATE_COLUMNS - columns["supresstate"])
        raise RuntimeError("supresstate schema is incomplete; missing columns: {0}".format(", ".join(missing)))


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


def fetch_visible_l0_pivot_tail(cur: Any, *, symbol: str, cursor_tick_id: int, limit: int = VISIBLE_PIVOT_TAIL) -> List[Dict[str, Any]]:
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
        ORDER BY pivot_id ASC
        """.format(columns=fast_zig_event_columns_sql()),
        (symbol, cursor_tick_id, cursor_tick_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def sync_visible_pivots(visible_pivots: List[Dict[str, Any]], event: Dict[str, Any]) -> None:
    pivot_id = int(event["pivot_id"])
    existing_index = next((index for index, pivot in enumerate(visible_pivots) if int(pivot["pivot_id"]) == pivot_id), -1)
    if existing_index >= 0:
        visible_pivots.pop(existing_index)
    visible_pivots.append(dict(event))
    visible_pivots.sort(key=lambda pivot: int(pivot["pivot_id"]))
    if len(visible_pivots) > VISIBLE_PIVOT_TAIL:
        del visible_pivots[:-VISIBLE_PIVOT_TAIL]


def load_state(cur: Any, *, symbol: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT id, symbol, lastprocessedtickid, lastprocessedpivotid, updated_at
        FROM public.supresstate
        WHERE symbol = %s
        """,
        (symbol,),
    )
    row = cur.fetchone()
    if row:
        return dict(row)
    cur.execute(
        """
        INSERT INTO public.supresstate (symbol, lastprocessedtickid, lastprocessedpivotid, updated_at)
        VALUES (%s, 0, 0, NOW())
        RETURNING id, symbol, lastprocessedtickid, lastprocessedpivotid, updated_at
        """,
        (symbol,),
    )
    return dict(cur.fetchone())


def store_state(cur: Any, *, symbol: str, lastprocessedtickid: int, lastprocessedpivotid: int) -> None:
    cur.execute(
        """
        INSERT INTO public.supresstate (symbol, lastprocessedtickid, lastprocessedpivotid, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE
        SET lastprocessedtickid = EXCLUDED.lastprocessedtickid,
            lastprocessedpivotid = EXCLUDED.lastprocessedpivotid,
            updated_at = NOW()
        """,
        (symbol, lastprocessedtickid, lastprocessedpivotid),
    )


def fetch_open_areas(cur: Any, *, symbol: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM public.supresarea
        WHERE symbol = %s
          AND state IN ('active', 'used')
        ORDER BY birthtickid ASC, id ASC
        """,
        (symbol,),
    )
    return [dict(row) for row in cur.fetchall()]


def insert_area(cur: Any, area: Dict[str, Any]) -> Optional[int]:
    cur.execute(
        """
        INSERT INTO public.supresarea (
            symbol, pattern, side, state, sourcelevel,
            sourcepivotid, startpivotid, middlepivotid, endpivotid,
            startpivottickid, middlepivottickid, endpivottickid,
            startpivottime, middlepivottime, endpivottime,
            startpivotprice, middlepivotprice, endpivotprice,
            parentl1pivotid, parentl2pivotid, isl1extreme, isl2extreme,
            birthtickid, birthtime,
            originallow, originalhigh, currentlow, currenthigh,
            originalheight, activeheight,
            firsttouchtickid, firsttouchtime,
            fullusetickid, fullusetime,
            touchcount, maxpenetration,
            firstbreaktickid, firstbreaktime,
            closetickid, closetime, closereason,
            priorityscore, initialdeparturedistance, untoucheddurationms,
            breakticksused, breaktoleranceused,
            outsidestreak, outsidedirection, insideactive,
            created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            NOW(), NOW()
        )
        ON CONFLICT (symbol, sourcepivotid) DO NOTHING
        RETURNING id
        """,
        (
            area["symbol"], area["pattern"], area["side"], area["state"], area["sourcelevel"],
            area["sourcepivotid"], area["startpivotid"], area["middlepivotid"], area["endpivotid"],
            area["startpivottickid"], area["middlepivottickid"], area["endpivottickid"],
            area["startpivottime"], area["middlepivottime"], area["endpivottime"],
            area["startpivotprice"], area["middlepivotprice"], area["endpivotprice"],
            area["parentl1pivotid"], area["parentl2pivotid"], area["isl1extreme"], area["isl2extreme"],
            area["birthtickid"], area["birthtime"],
            area["originallow"], area["originalhigh"], area["currentlow"], area["currenthigh"],
            area["originalheight"], area["activeheight"],
            area["firsttouchtickid"], area["firsttouchtime"],
            area["fullusetickid"], area["fullusetime"],
            area["touchcount"], area["maxpenetration"],
            area["firstbreaktickid"], area["firstbreaktime"],
            area["closetickid"], area["closetime"], area["closereason"],
            area["priorityscore"], area["initialdeparturedistance"], area["untoucheddurationms"],
            area["breakticksused"], area["breaktoleranceused"],
            area["outsidestreak"], area["outsidedirection"], area["insideactive"],
        ),
    )
    row = cur.fetchone()
    return int(row["id"]) if row else None


def update_area(cur: Any, area: Dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE public.supresarea
        SET state = %s,
            parentl1pivotid = %s,
            parentl2pivotid = %s,
            isl1extreme = %s,
            isl2extreme = %s,
            currentlow = %s,
            currenthigh = %s,
            activeheight = %s,
            firsttouchtickid = %s,
            firsttouchtime = %s,
            fullusetickid = %s,
            fullusetime = %s,
            touchcount = %s,
            maxpenetration = %s,
            firstbreaktickid = %s,
            firstbreaktime = %s,
            closetickid = %s,
            closetime = %s,
            closereason = %s,
            priorityscore = %s,
            initialdeparturedistance = %s,
            untoucheddurationms = %s,
            outsidestreak = %s,
            outsidedirection = %s,
            insideactive = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            area["state"],
            area.get("parentl1pivotid"),
            area.get("parentl2pivotid"),
            bool(area.get("isl1extreme")),
            bool(area.get("isl2extreme")),
            area["currentlow"],
            area["currenthigh"],
            area["activeheight"],
            area.get("firsttouchtickid"),
            area.get("firsttouchtime"),
            area.get("fullusetickid"),
            area.get("fullusetime"),
            area["touchcount"],
            area["maxpenetration"],
            area.get("firstbreaktickid"),
            area.get("firstbreaktime"),
            area.get("closetickid"),
            area.get("closetime"),
            area.get("closereason"),
            area["priorityscore"],
            area["initialdeparturedistance"],
            area["untoucheddurationms"],
            area["outsidestreak"],
            area.get("outsidedirection"),
            bool(area.get("insideactive")),
            area["id"],
        ),
    )


def insert_event(cur: Any, event_row: Dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO public.supresareaevent (
            areaid, symbol, eventtype, tickid, eventtime,
            price, lowprice, highprice, penetration,
            statebefore, stateafter, details, created_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, NOW()
        )
        """,
        (
            event_row["areaid"], event_row["symbol"], event_row["eventtype"], event_row["tickid"], event_row["eventtime"],
            event_row.get("price"), event_row.get("lowprice"), event_row.get("highprice"), event_row.get("penetration"),
            event_row.get("statebefore"), event_row.get("stateafter"), event_row.get("details"),
        ),
    )


def maybe_mark_break_event(area: Dict[str, Any], tick_row: Dict[str, Any], price_value: float) -> Optional[Dict[str, Any]]:
    if area.get("firstbreaktickid") is not None:
        return None
    area["firstbreaktickid"] = int(tick_row["id"])
    area["firstbreaktime"] = tick_row["timestamp"]
    return area_event_row(
        area=area,
        event_type="break",
        tick_row=tick_row,
        price_value=price_value,
        state_before=area["state"],
        state_after=area["state"],
        details="Break threshold crossed.",
    )


def register_touch(area: Dict[str, Any], tick_row: Dict[str, Any], price_value: float, details: str) -> Optional[Dict[str, Any]]:
    first_touch = area.get("firsttouchtime") is None
    entering = not bool(area.get("insideactive"))
    if first_touch:
        area["firsttouchtickid"] = int(tick_row["id"])
        area["firsttouchtime"] = tick_row["timestamp"]
        freeze_untouched_duration(area, tick_row["timestamp"])
    if entering:
        area["touchcount"] = int(area.get("touchcount") or 0) + 1
        return area_event_row(
            area=area,
            event_type="touch",
            tick_row=tick_row,
            price_value=price_value,
            state_before=AREA_STATE_ACTIVE,
            state_after=AREA_STATE_ACTIVE,
            details=details,
        )
    return None


def update_area_from_tick(area: Dict[str, Any], tick_row: Dict[str, Any]) -> tuple[bool, List[Dict[str, Any]]]:
    changed = False
    events: List[Dict[str, Any]] = []
    if str(area.get("state")) == AREA_STATE_CLOSED:
        return False, events

    if sync_departure_metrics(area, tick_row):
        changed = True

    side = str(area["side"])
    price_value = executable_price_for_side(side, tick_row)
    if price_value is None:
        return changed, events

    original_low = float(area["originallow"])
    original_high = float(area["originalhigh"])
    current_low = float(area["currentlow"])
    current_high = float(area["currenthigh"])

    if str(area["state"]) == AREA_STATE_ACTIVE:
        touched_active = False
        if side == AREA_SIDE_TOP and price_value >= current_low:
            touched_active = True
            touch_event = register_touch(
                area,
                tick_row,
                float(price_value),
                "Executable price entered the remaining top area.",
            )
            if touch_event is not None:
                events.append(touch_event)
            if price_value >= current_high:
                area["currentlow"] = current_high
                area["activeheight"] = 0.0
                area["maxpenetration"] = max(float(area.get("maxpenetration") or 0.0), float(area["originalheight"]))
                area["fullusetickid"] = area.get("fullusetickid") or int(tick_row["id"])
                area["fullusetime"] = area.get("fullusetime") or tick_row["timestamp"]
                area["state"] = AREA_STATE_USED
                area["insideactive"] = False
                events.append(area_event_row(
                    area=area,
                    event_type="fulluse",
                    tick_row=tick_row,
                    price_value=price_value,
                    penetration=float(area["originalheight"]),
                    state_before=AREA_STATE_ACTIVE,
                    state_after=AREA_STATE_USED,
                    details="Remaining top area was fully consumed.",
                ))
            elif price_value > current_low:
                penetration = max(0.0, min(current_high, price_value) - original_low)
                area["currentlow"] = float(price_value)
                area["activeheight"] = max(0.0, current_high - float(area["currentlow"]))
                area["maxpenetration"] = max(float(area.get("maxpenetration") or 0.0), penetration)
                area["insideactive"] = True
                events.append(area_event_row(
                    area=area,
                    event_type="partialuse",
                    tick_row=tick_row,
                    price_value=price_value,
                    penetration=penetration,
                    state_before=AREA_STATE_ACTIVE,
                    state_after=AREA_STATE_ACTIVE,
                    details="Lower portion of the top area was consumed.",
                ))
            else:
                area["insideactive"] = True
            update_priority(area)
            changed = True
        elif side != AREA_SIDE_TOP and price_value <= current_high:
            touched_active = True
            touch_event = register_touch(
                area,
                tick_row,
                float(price_value),
                "Executable price entered the remaining bottom area.",
            )
            if touch_event is not None:
                events.append(touch_event)
            if price_value <= current_low:
                area["currenthigh"] = current_low
                area["activeheight"] = 0.0
                area["maxpenetration"] = max(float(area.get("maxpenetration") or 0.0), float(area["originalheight"]))
                area["fullusetickid"] = area.get("fullusetickid") or int(tick_row["id"])
                area["fullusetime"] = area.get("fullusetime") or tick_row["timestamp"]
                area["state"] = AREA_STATE_USED
                area["insideactive"] = False
                events.append(area_event_row(
                    area=area,
                    event_type="fulluse",
                    tick_row=tick_row,
                    price_value=price_value,
                    penetration=float(area["originalheight"]),
                    state_before=AREA_STATE_ACTIVE,
                    state_after=AREA_STATE_USED,
                    details="Remaining bottom area was fully consumed.",
                ))
            elif price_value < current_high:
                penetration = max(0.0, original_high - max(current_low, price_value))
                area["currenthigh"] = float(price_value)
                area["activeheight"] = max(0.0, float(area["currenthigh"]) - current_low)
                area["maxpenetration"] = max(float(area.get("maxpenetration") or 0.0), penetration)
                area["insideactive"] = True
                events.append(area_event_row(
                    area=area,
                    event_type="partialuse",
                    tick_row=tick_row,
                    price_value=price_value,
                    penetration=penetration,
                    state_before=AREA_STATE_ACTIVE,
                    state_after=AREA_STATE_ACTIVE,
                    details="Upper portion of the bottom area was consumed.",
                ))
            else:
                area["insideactive"] = True
            update_priority(area)
            changed = True
        if not touched_active and area.get("insideactive"):
            area["insideactive"] = False
            changed = True

    break_direction: Optional[str] = None
    if side == AREA_SIDE_TOP and price_value > original_high + float(area["breaktoleranceused"]):
        break_direction = "up"
    elif side != AREA_SIDE_TOP and price_value < original_low - float(area["breaktoleranceused"]):
        break_direction = "down"

    if break_direction is None:
        if int(area.get("outsidestreak") or 0) != 0 or area.get("outsidedirection") is not None:
            area["outsidestreak"] = 0
            area["outsidedirection"] = None
            changed = True
        return changed, events

    break_event = maybe_mark_break_event(area, tick_row, float(price_value))
    if break_event is not None:
        events.append(break_event)
        changed = True

    if area.get("outsidedirection") == break_direction:
        area["outsidestreak"] = int(area.get("outsidestreak") or 0) + 1
    else:
        area["outsidedirection"] = break_direction
        area["outsidestreak"] = 1
    changed = True

    if int(area["outsidestreak"]) < int(area["breakticksused"]):
        return changed, events

    if str(area["state"]) != AREA_STATE_CLOSED:
        state_before_close = str(area["state"])
        area["state"] = AREA_STATE_CLOSED
        area["closetickid"] = int(tick_row["id"])
        area["closetime"] = tick_row["timestamp"]
        area["closereason"] = "break_above_original_high" if side == AREA_SIDE_TOP else "break_below_original_low"
        area["insideactive"] = False
        events.append(area_event_row(
            area=area,
            event_type="close",
            tick_row=tick_row,
            price_value=price_value,
            state_before=state_before_close,
            state_after=AREA_STATE_CLOSED,
            details="Unused area was closed by breakout confirmation.",
        ))
        changed = True
    return changed, events


def update_area_qualification(cur: Any, *, symbol: str, pivot_id: int, level: int) -> None:
    cur.execute(
        """
        SELECT *
        FROM public.supresarea
        WHERE symbol = %s
          AND sourcepivotid = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (symbol, pivot_id),
    )
    row = cur.fetchone()
    if not row:
        return
    area = dict(row)
    if not apply_level_qualification(area, level):
        return
    update_area(cur, area)


def process_batch(
    cur: Any,
    *,
    symbol: str,
    state_row: Dict[str, Any],
    tick_rows: List[Dict[str, Any]],
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    if not tick_rows:
        return state_row

    lastprocessedtickid = int(state_row["lastprocessedtickid"] or 0)
    lastprocessedpivotid = int(state_row["lastprocessedpivotid"] or 0)
    visible_pivots = fetch_visible_l0_pivot_tail(cur, symbol=symbol, cursor_tick_id=lastprocessedtickid)
    pivot_events = fetch_pivot_events(cur, symbol, lastprocessedtickid, int(tick_rows[-1]["id"]))
    event_index = 0
    open_areas = fetch_open_areas(cur, symbol=symbol)
    open_areas_by_source = {int(area["sourcepivotid"]): area for area in open_areas}

    for tick_row in tick_rows:
        tick_id = int(tick_row["id"])

        while event_index < len(pivot_events) and int(pivot_events[event_index]["visible_from_tick_id"]) <= tick_id:
            event = dict(pivot_events[event_index])
            sync_visible_pivots(visible_pivots, event)
            lastprocessedpivotid = max(lastprocessedpivotid, int(event["pivot_id"]))

            current_area = open_areas_by_source.get(int(event["pivot_id"]))
            if current_area is not None:
                if apply_level_qualification(current_area, int(event["level"])):
                    update_area(cur, current_area)
            else:
                update_area_qualification(cur, symbol=symbol, pivot_id=int(event["pivot_id"]), level=int(event["level"]))

            if len(visible_pivots) >= 3:
                candidate = zone_birth_candidate(
                    visible_pivots[-3:],
                    same_side_tolerance=float(settings["sameSideTolerance"]),
                    min_height=float(settings["minHeight"]),
                    max_height=float(settings["maxHeight"]),
                )
                if candidate is not None:
                    area = build_area_from_candidate(
                        symbol=symbol,
                        candidate=candidate,
                        birth_tick=tick_row,
                        source_level=int(event["level"]),
                        break_ticks=int(settings["breakTicks"]),
                        break_tolerance=float(settings["breakTolerance"]),
                    )
                    if area is not None:
                        area_id = insert_area(cur, area)
                        if area_id is not None:
                            area["id"] = area_id
                            insert_event(cur, area_event_row(
                                area=area,
                                event_type="birth",
                                tick_row=tick_row,
                                price_value=price_series_mid(tick_row),
                                state_before=None,
                                state_after=AREA_STATE_ACTIVE,
                                details="Unused area was born from the confirmed L0 structure.",
                            ))
                            open_areas.append(area)
                            open_areas_by_source[int(area["sourcepivotid"])] = area
            event_index += 1

        for area in list(open_areas):
            changed, events = update_area_from_tick(area, tick_row)
            if not changed:
                continue
            update_priority(area)
            update_area(cur, area)
            for event_row in events:
                insert_event(cur, event_row)
            if str(area["state"]) == AREA_STATE_CLOSED:
                open_areas.remove(area)
                open_areas_by_source.pop(int(area["sourcepivotid"]), None)

        lastprocessedtickid = tick_id

    store_state(
        cur,
        symbol=symbol,
        lastprocessedtickid=lastprocessedtickid,
        lastprocessedpivotid=lastprocessedpivotid,
    )
    state_row["lastprocessedtickid"] = lastprocessedtickid
    state_row["lastprocessedpivotid"] = lastprocessedpivotid
    return state_row


def process_once(
    cur: Any,
    *,
    symbol: str,
    settings: Dict[str, Any],
    batch_size: int,
    upto_tick_id: Optional[int] = None,
) -> int:
    state_row = load_state(cur, symbol=symbol)
    tick_rows = fetch_ticks_after(
        cur,
        symbol=symbol,
        after_id=int(state_row["lastprocessedtickid"] or 0),
        limit=batch_size,
        upto_id=upto_tick_id,
    )
    if not tick_rows:
        return 0
    process_batch(
        cur,
        symbol=symbol,
        state_row=state_row,
        tick_rows=tick_rows,
        settings=settings,
    )
    return len(tick_rows)


def reset_state(cur: Any, *, symbol: str, from_tick_id: Optional[int] = None) -> None:
    load_state(cur, symbol=symbol)
    if from_tick_id is None:
        cur.execute(
            """
            UPDATE public.supresstate
            SET lastprocessedtickid = 0,
                lastprocessedpivotid = 0,
                updated_at = NOW()
            WHERE symbol = %s
            """,
            (symbol,),
        )
        cur.execute("DELETE FROM public.supresarea WHERE symbol = %s", (symbol,))
        return

    reset_tick_id = max(0, int(from_tick_id) - 1)
    prior_pivot_id = fetch_latest_pivot_id_before_tick(cur, symbol, reset_tick_id)
    cur.execute(
        """
        UPDATE public.supresstate
        SET lastprocessedtickid = %s,
            lastprocessedpivotid = %s,
            updated_at = NOW()
        WHERE symbol = %s
        """,
        (reset_tick_id, prior_pivot_id, symbol),
    )
    cur.execute(
        """
        DELETE FROM public.supresarea
        WHERE symbol = %s
          AND birthtickid >= %s
        """,
        (symbol, from_tick_id),
    )


def log_progress(*, symbol: str, processed: int, last_tick_id: int, last_pivot_id: int, batch_ms: float) -> None:
    print(
        "supres stats symbol={0} tick={1} pivots={2} batch={3} batch_ms={4:.2f}".format(
            symbol,
            last_tick_id,
            last_pivot_id,
            processed,
            batch_ms,
        ),
        flush=True,
    )


def run_loop(symbol: str, batch_size: int) -> None:
    settings = current_settings()
    last_log = time.time()
    idle_sleep = POLL_SECONDS

    while not STOP:
        conn = None
        try:
            conn = db_connect()
            conn.autocommit = False
            ensure_storage_ready(conn)

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                batch_started = time.perf_counter()
                processed = process_once(
                    cur,
                    symbol=symbol,
                    settings=settings,
                    batch_size=batch_size,
                )
                conn.commit()

                if processed:
                    state_row = load_state(cur, symbol=symbol)
                    if time.time() - last_log >= 5.0:
                        log_progress(
                            symbol=symbol,
                            processed=processed,
                            last_tick_id=int(state_row["lastprocessedtickid"] or 0),
                            last_pivot_id=int(state_row["lastprocessedpivotid"] or 0),
                            batch_ms=(time.perf_counter() - batch_started) * 1000.0,
                        )
                        last_log = time.time()
                    idle_sleep = POLL_SECONDS
                    continue

            time.sleep(idle_sleep)
            idle_sleep = IDLE_POLL_SECONDS
        except Exception as exc:
            print("supres error: {0}".format(exc), flush=True)
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


def run_reset(symbol: str, from_time: Optional[str], timezone_name: str) -> None:
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
            reset_state(cur, symbol=symbol, from_tick_id=from_tick_id)
        conn.commit()
    finally:
        conn.close()


def run_rebuild(
    symbol: str,
    start_time: str,
    end_time: Optional[str],
    timezone_name: str,
    batch_size: int,
) -> None:
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

            reset_state(cur, symbol=symbol, from_tick_id=start_tick_id)
            conn.commit()

            settings = current_settings()
            while True:
                processed = process_once(
                    cur,
                    symbol=symbol,
                    settings=settings,
                    batch_size=batch_size,
                    upto_tick_id=end_tick_id,
                )
                conn.commit()
                if not processed:
                    break
                state_row = load_state(cur, symbol=symbol)
                if int(state_row["lastprocessedtickid"] or 0) >= end_tick_id:
                    break
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persist and rebuild unused supply/resistance areas from fast-zig pivots.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_loop_parser = subparsers.add_parser("run-loop", help="Run the incremental unused-area polling loop.")
    run_loop_parser.add_argument("--symbol", default=TICK_SYMBOL)
    run_loop_parser.add_argument("--chunk-size", dest="chunk_size", type=int, default=DEFAULT_BATCH_SIZE)

    reset_parser = subparsers.add_parser("reset", help="Clear persisted unused-area rows, optionally from a chosen time onward.")
    reset_parser.add_argument("--symbol", default=TICK_SYMBOL)
    reset_parser.add_argument("--from-time", dest="from_time")
    reset_parser.add_argument("--timezone", default=DEFAULT_REVIEW_TIMEZONE)

    rebuild_parser = subparsers.add_parser("rebuild", help="Reset from a chosen start time and rebuild up to an end time.")
    rebuild_parser.add_argument("--symbol", default=TICK_SYMBOL)
    rebuild_parser.add_argument("--start-time", required=True)
    rebuild_parser.add_argument("--end-time")
    rebuild_parser.add_argument("--timezone", default=DEFAULT_REVIEW_TIMEZONE)
    rebuild_parser.add_argument("--chunk-size", dest="chunk_size", type=int, default=DEFAULT_BATCH_SIZE)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run-loop":
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)
        run_loop(symbol=args.symbol, batch_size=max(1, int(args.chunk_size)))
        return
    if args.command == "reset":
        run_reset(symbol=args.symbol, from_time=args.from_time, timezone_name=args.timezone)
        return
    if args.command == "rebuild":
        run_rebuild(
            symbol=args.symbol,
            start_time=args.start_time,
            end_time=args.end_time,
            timezone_name=args.timezone,
            batch_size=max(1, int(args.chunk_size)),
        )
        return
    parser.error("Unknown command.")


if __name__ == "__main__":
    main()
