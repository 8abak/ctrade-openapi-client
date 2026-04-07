#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import secrets
import time
from bisect import bisect_left, bisect_right
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import psycopg2.extras
from psycopg2 import extensions as pg_extensions
from psycopg2 import sql as pg_sql
import sqlparse
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from datavis.db import db_connect as shared_db_connect
from datavis.supresarea import (
    AREA_SIDE_BOTTOM,
    AREA_SIDE_TOP,
    AREA_SIDES,
    AREA_STATE_ACTIVE,
    AREA_STATE_CLOSED,
    AREA_STATE_USED,
    AREA_STATES,
    serialize_supresarea_row,
)
from datavis.zonebox import (
    ZONE_STATE_ACTIVE,
    ZONE_STATE_CLOSED,
    ZONE_STATE_PROVISIONAL,
    serialize_zonebox_row,
)

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
ASSETS_DIR = FRONTEND_DIR / "assets"

load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgresql+psycopg2://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://", 1)

TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
DEFAULT_WINDOW = 2000
MAX_TICK_WINDOW = 10000
MAX_ZIG_WINDOW = 100000
DEFAULT_HISTORY_LIMIT = 2000
MAX_TICK_HISTORY_LIMIT = 10000
MAX_ZIG_HISTORY_LIMIT = 50000
MAX_ZIG_CANDLE_WINDOW = 10000
MAX_ZIG_CANDLE_HISTORY_LIMIT = 10000
MAX_STREAM_BATCH = 1000
MAX_ZIG_STREAM_BATCH = 500
MAX_ZONE_MIN_DWELL_TICKS = 500
MAX_ZONE_MIN_DWELL_MS = 300000
MAX_ZONE_SAME_SIDE_TOLERANCE = 10.0
MAX_ZONE_ALLOWED_OVERSHOOT = 10.0
MAX_ZONE_BREAKOUT_TICKS = 64
MAX_ZONE_BREAKOUT_TOLERANCE = 10.0
MAX_ZONE_HEIGHT = 25.0
MAX_AREA_BREAK_TICKS = 64
MAX_AREA_BREAK_TOLERANCE = 10.0
MAX_QUERY_ROWS = 1000
DEFAULT_SQL_PREVIEW_LIMIT = 100
MAX_SQL_PREVIEW_LIMIT = 500
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_LOCK_TIMEOUT_MS", "3000"))
STREAM_POLL_SECONDS = max(0.02, float(os.getenv("DATAVIS_STREAM_POLL_SECONDS", "0.05")))
STREAM_IDLE_POLL_SECONDS = max(
    STREAM_POLL_SECONDS,
    float(os.getenv("DATAVIS_STREAM_IDLE_POLL_SECONDS", "0.10")),
)
STREAM_HEARTBEAT_SECONDS = max(
    STREAM_IDLE_POLL_SECONDS,
    float(os.getenv("DATAVIS_STREAM_HEARTBEAT_SECONDS", "5.0")),
)
DEFAULT_DISPLAY_MODE = "ticks"
DISPLAY_MODE_RE = "^(ticks|ticks-zig|zig)$"
PRICE_SERIES_RE = "^(mid|ask|bid)$"
MAX_ZIG_LEVEL = 3
LEVEL_ZERO_PROVING_TICKS = 4
DEFAULT_ZONE_MIN_DWELL_TICKS = int(os.getenv("DATAVIS_ZONE_MIN_DWELL_TICKS", "24"))
DEFAULT_ZONE_MIN_DWELL_MS = int(os.getenv("DATAVIS_ZONE_MIN_DWELL_MS", "3000"))
DEFAULT_ZONE_SAME_SIDE_TOLERANCE = float(os.getenv("DATAVIS_ZONE_SAME_SIDE_TOLERANCE", "0.24"))
DEFAULT_ZONE_ALLOWED_OVERSHOOT = float(os.getenv("DATAVIS_ZONE_ALLOWED_OVERSHOOT", "0.18"))
DEFAULT_ZONE_BREAKOUT_TICKS = int(os.getenv("DATAVIS_ZONE_BREAKOUT_TICKS", "4"))
DEFAULT_ZONE_BREAKOUT_TOLERANCE = float(os.getenv("DATAVIS_ZONE_BREAKOUT_TOLERANCE", "0.24"))
DEFAULT_ZONE_MIN_HEIGHT = float(os.getenv("DATAVIS_ZONE_MIN_HEIGHT", "0.05"))
DEFAULT_ZONE_MAX_HEIGHT = float(os.getenv("DATAVIS_ZONE_MAX_HEIGHT", "1.60"))
ZONE_CONTEXT_PIVOTS = 3
ZONE_WARMUP_MULTIPLIER = 4
ZONE_MIN_WARMUP_TICKS = 64
SQL_ADMIN_USER = os.getenv("DATAVIS_SQL_ADMIN_USER", "").strip()
SQL_ADMIN_PASSWORD = os.getenv("DATAVIS_SQL_ADMIN_PASSWORD", "")
DEFAULT_REVIEW_TIMEZONE = "Australia/Sydney"
SQL_EXPOSED_TABLES = {
    ("public", "ticks"): {
        "schema": "public",
        "name": "ticks",
        "kind": "table",
        "default_order_by": "id",
        "default_order_dir": "desc",
        "select_sql": "SELECT id, timestamp, bid, ask, mid, spread\nFROM public.ticks\nORDER BY id DESC\nLIMIT 100;",
    },
    ("public", "fast_zig_pivots"): {
        "schema": "public",
        "name": "fast_zig_pivots",
        "kind": "table",
        "default_order_by": "version_id",
        "default_order_dir": "desc",
        "select_sql": (
            "SELECT version_id, pivot_id, source_tick_id, source_timestamp, direction, pivot_price, level, state,\n"
            "       visible_from_tick_id, visible_to_tick_id\n"
            "FROM public.fast_zig_pivots\n"
            "ORDER BY pivot_id DESC, version_id DESC\n"
            "LIMIT 100;"
        ),
    },
    ("public", "fast_zig_state"): {
        "schema": "public",
        "name": "fast_zig_state",
        "kind": "table",
        "default_order_by": "symbol",
        "default_order_dir": "asc",
        "select_sql": "SELECT symbol, last_processed_tick_id, last_pivot_id, updated_at\nFROM public.fast_zig_state\nORDER BY symbol ASC\nLIMIT 100;",
    },
    ("public", "zonebox"): {
        "schema": "public",
        "name": "zonebox",
        "kind": "table",
        "default_order_by": "id",
        "default_order_dir": "desc",
        "select_sql": (
            "SELECT id, symbol, level, state, pattern, starttickid, endtickid, starttime, endtime,\n"
            "       initialzonelow, initialzonehigh, zonelow, zonehigh, tickcountinside, durationms,\n"
            "       breakdirection, breaktickid, lasttickid, updated_at\n"
            "FROM public.zonebox\n"
            "ORDER BY id DESC\n"
            "LIMIT 100;"
        ),
    },
    ("public", "zoneboxstate"): {
        "schema": "public",
        "name": "zoneboxstate",
        "kind": "table",
        "default_order_by": "id",
        "default_order_dir": "desc",
        "select_sql": (
            "SELECT id, symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid, updated_at\n"
            "FROM public.zoneboxstate\n"
            "ORDER BY id DESC\n"
            "LIMIT 100;"
        ),
    },
    ("public", "supresarea"): {
        "schema": "public",
        "name": "supresarea",
        "kind": "table",
        "default_order_by": "id",
        "default_order_dir": "desc",
        "select_sql": (
            "SELECT id, symbol, side, state, sourcepivotid, birthtickid, birthtime,\n"
            "       originallow, originalhigh, currentlow, currenthigh, activeheight,\n"
            "       touchcount, maxpenetration, priorityscore, isl1extreme, isl2extreme, updated_at\n"
            "FROM public.supresarea\n"
            "ORDER BY id DESC\n"
            "LIMIT 100;"
        ),
    },
    ("public", "supresareaevent"): {
        "schema": "public",
        "name": "supresareaevent",
        "kind": "table",
        "default_order_by": "id",
        "default_order_dir": "desc",
        "select_sql": (
            "SELECT id, areaid, symbol, eventtype, tickid, eventtime, price, penetration,\n"
            "       statebefore, stateafter, details, created_at\n"
            "FROM public.supresareaevent\n"
            "ORDER BY id DESC\n"
            "LIMIT 100;"
        ),
    },
    ("public", "supresstate"): {
        "schema": "public",
        "name": "supresstate",
        "kind": "table",
        "default_order_by": "id",
        "default_order_dir": "desc",
        "select_sql": (
            "SELECT id, symbol, lastprocessedtickid, lastprocessedpivotid, updated_at\n"
            "FROM public.supresstate\n"
            "ORDER BY id DESC\n"
            "LIMIT 100;"
        ),
    },
}
ZIG_REQUIRED_PIVOT_COLUMNS = {
    "level",
    "state",
    "updated_at",
}
ZIG_REQUIRED_STATE_COLUMNS = {
    "symbol",
    "last_processed_tick_id",
    "last_pivot_id",
    "updated_at",
}
ZONEBOX_REQUIRED_COLUMNS = {
    "id",
    "symbol",
    "level",
    "state",
    "pattern",
    "starttickid",
    "endtickid",
    "starttime",
    "endtime",
    "initialzonelow",
    "initialzonehigh",
    "zonelow",
    "zonehigh",
    "zoneheight",
    "samesidedistance",
    "samesidetoleranceused",
    "tickcountinside",
    "durationms",
    "continuationovershootused",
    "breakticksused",
    "breaktoleranceused",
    "breakdirection",
    "breaktickid",
    "lasttickid",
    "lasttime",
    "touchcount",
    "revisitcount",
    "updated_at",
}
ZONEBOXSTATE_REQUIRED_COLUMNS = {
    "id",
    "symbol",
    "level",
    "lastprocessedtickid",
    "lastprocessedpivotid",
    "activezoneid",
    "updated_at",
}
SUPRESAREA_REQUIRED_COLUMNS = {
    "id",
    "symbol",
    "side",
    "state",
    "sourcepivotid",
    "birthtickid",
    "birthtime",
    "originallow",
    "originalhigh",
    "currentlow",
    "currenthigh",
    "activeheight",
    "touchcount",
    "maxpenetration",
    "priorityscore",
    "updated_at",
}
SUPRESAREAEVENT_REQUIRED_COLUMNS = {
    "id",
    "areaid",
    "eventtype",
    "tickid",
    "eventtime",
}
SUPRESSTATE_REQUIRED_COLUMNS = {
    "id",
    "symbol",
    "lastprocessedtickid",
    "lastprocessedpivotid",
    "updated_at",
}


class QueryRequest(BaseModel):
    sql: str


security = HTTPBasic(auto_error=False)

app = FastAPI(title="datavis.au", version="2.0.0")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def ensure_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return DATABASE_URL


@contextmanager
def db_connection(readonly: bool = False, autocommit: bool = False):
    if DATABASE_URL:
        conn = psycopg2.connect(ensure_database_url())
        conn.autocommit = autocommit
        if readonly:
            conn.set_session(readonly=True, autocommit=autocommit)
    else:
        conn = shared_db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def clamp_float(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(float(value), maximum))


def now_ms() -> int:
    return int(time.time() * 1000)


def elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 2)


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def serialize_tick_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = row["timestamp"]
    mid_value = row.get("mid")
    if mid_value is None and row.get("bid") is not None and row.get("ask") is not None:
        mid_value = round((float(row["bid"]) + float(row["ask"])) / 2.0, 2)
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": row.get("bid"),
        "ask": row.get("ask"),
        "mid": mid_value,
        "spread": row.get("spread"),
    }


def serialize_zig_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = row["source_timestamp"]
    return {
        "versionId": row["version_id"],
        "pivotId": row["pivot_id"],
        "symbol": row["symbol"],
        "sourceTickId": row["source_tick_id"],
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "direction": row["direction"],
        "price": row["pivot_price"],
        "level": row["level"],
        "state": row["state"],
        "visibleFromTickId": row["visible_from_tick_id"],
        "visibleToTickId": row.get("visible_to_tick_id"),
    }


def includes_ticks(display_mode: str) -> bool:
    return display_mode in {"ticks", "ticks-zig"}


def includes_zig(display_mode: str) -> bool:
    return display_mode in {"ticks-zig", "zig"}


def resolve_live_layers(
    *,
    display_mode: str,
    show_ticks: Optional[bool],
    show_zigs: Optional[bool],
    show_zones: Optional[bool],
    show_areas: Optional[bool],
) -> Dict[str, bool]:
    flags = {
        "ticks": includes_ticks(display_mode) if show_ticks is None else bool(show_ticks),
        "zigs": includes_zig(display_mode) if show_zigs is None else bool(show_zigs),
        "zones": False if show_zones is None else bool(show_zones),
        "areas": False if show_areas is None else bool(show_areas),
    }
    if not any(flags.values()):
        flags["ticks"] = True
    return flags


def clamp_live_window(value: int, layers: Dict[str, bool]) -> int:
    return clamp_int(value, 1, MAX_TICK_WINDOW)


def clamp_live_history_limit(value: int, layers: Dict[str, bool]) -> int:
    return clamp_int(value, 1, MAX_TICK_HISTORY_LIMIT)


def clamp_window(value: int, display_mode: str) -> int:
    maximum = MAX_ZIG_WINDOW if display_mode == "zig" else MAX_TICK_WINDOW
    return clamp_int(value, 1, maximum)


def clamp_history_limit(value: int, display_mode: str) -> int:
    maximum = MAX_ZIG_HISTORY_LIMIT if display_mode == "zig" else MAX_TICK_HISTORY_LIMIT
    return clamp_int(value, 1, maximum)


def parse_area_states(raw_value: Optional[str]) -> List[str]:
    if raw_value is None or not raw_value.strip():
        return [AREA_STATE_ACTIVE]
    selected = []
    seen = set()
    for item in raw_value.split(","):
        value = item.strip().lower()
        if value in AREA_STATES and value not in seen:
            selected.append(value)
            seen.add(value)
    if not selected:
        raise HTTPException(status_code=400, detail="Invalid areaStates filter.")
    return selected


def parse_area_sides(raw_value: Optional[str]) -> List[str]:
    if raw_value is None or not raw_value.strip():
        return [AREA_SIDE_TOP, AREA_SIDE_BOTTOM]
    selected = []
    seen = set()
    for item in raw_value.split(","):
        value = item.strip().lower()
        if value in AREA_SIDES and value not in seen:
            selected.append(value)
            seen.add(value)
    if not selected:
        raise HTTPException(status_code=400, detail="Invalid areaSides filter.")
    return selected


def serialize_metrics_payload(
    *,
    fetch_ms: float,
    serialize_ms: float,
    latest_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "serverSentAtMs": now_ms(),
        "fetchLatencyMs": fetch_ms,
        "serializeLatencyMs": serialize_ms,
        "dbLatestId": latest_row.get("id") if latest_row else None,
        "dbLatestTimestamp": serialize_value(latest_row.get("timestamp")) if latest_row else None,
        "dbLatestTimestampMs": dt_to_ms(latest_row.get("timestamp")) if latest_row else None,
    }


def format_sse(payload: Dict[str, Any], *, event_name: Optional[str] = None) -> str:
    if event_name:
        return "event: {0}\ndata: {1}\n\n".format(event_name, json.dumps(payload))
    return "data: {0}\n\n".format(json.dumps(payload))


def require_sql_admin(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> str:
    if not SQL_ADMIN_USER or not SQL_ADMIN_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SQL admin credentials are not configured.",
        )

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SQL admin authentication is required.",
            headers={"WWW-Authenticate": 'Basic realm=\"datavis SQL\"'},
        )

    valid_user = secrets.compare_digest(credentials.username or "", SQL_ADMIN_USER)
    valid_password = secrets.compare_digest(credentials.password or "", SQL_ADMIN_PASSWORD)
    if not (valid_user and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid SQL admin credentials.",
            headers={"WWW-Authenticate": 'Basic realm=\"datavis SQL\"'},
        )

    return credentials.username


def parse_review_timestamp(raw_value: str, timezone_name: str) -> datetime:
    try:
        target_tz = ZoneInfo(timezone_name or DEFAULT_REVIEW_TIMEZONE)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=400, detail="Unsupported review timezone.") from exc

    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid review timestamp.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=target_tz)
    else:
        parsed = parsed.astimezone(target_tz)
    return parsed.astimezone(timezone.utc)


def resolve_tick_at_timestamp(timestamp_value: datetime) -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND timestamp >= %s
                ORDER BY timestamp ASC, id ASC
                LIMIT 1
                """,
                (TICK_SYMBOL, timestamp_value),
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
                (TICK_SYMBOL, timestamp_value),
            )
            previous_row = cur.fetchone()

    if not previous_row and not next_row:
        raise HTTPException(status_code=404, detail="No ticks are available for review.")

    if previous_row and next_row:
        previous_delta = abs((timestamp_value - previous_row["timestamp"]).total_seconds())
        next_delta = abs((next_row["timestamp"] - timestamp_value).total_seconds())
        resolved = next_row if next_delta < previous_delta else previous_row
    else:
        resolved = next_row or previous_row

    return {
        "id": int(resolved["id"]),
        "timestamp": resolved["timestamp"],
    }


def fetch_tick_bounds() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            row = query_tick_bounds(cur)
    return {
        "firstId": row.get("first_id"),
        "lastId": row.get("last_id"),
        "firstTimestamp": row.get("first_timestamp"),
        "lastTimestamp": row.get("last_timestamp"),
    }


def query_tick_bounds(cur: Any) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
            MIN(id) AS first_id,
            MAX(id) AS last_id,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp
        FROM public.ticks
        WHERE symbol = %s
        """,
        (TICK_SYMBOL,),
    )
    return dict(cur.fetchone() or {})


def query_latest_tick(cur: Any) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, timestamp
        FROM public.ticks
        WHERE symbol = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def tick_select_sql(select_sql: str, where_sql: str, order_sql: str, limit_sql: str) -> str:
    return """
        SELECT {select_sql}
        FROM public.ticks
        WHERE symbol = %s {where_clause}
        ORDER BY {order_clause}
        {limit_clause}
    """.format(select_sql=select_sql, where_clause=where_sql, order_clause=order_sql, limit_clause=limit_sql)


def tick_columns(include_rows: bool) -> str:
    if include_rows:
        return "id, symbol, timestamp, bid, ask, mid, spread"
    return "id, timestamp"


def fetch_bootstrap_tick_rows(
    cur: Any,
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    end_id: Optional[int],
    include_rows: bool,
) -> List[Dict[str, Any]]:
    select_sql = tick_columns(include_rows)
    if mode == "live":
        cur.execute(
            """
            SELECT {select_sql}
            FROM (
                SELECT {select_sql}
                FROM public.ticks
                WHERE symbol = %s
                ORDER BY id DESC
                LIMIT %s
            ) recent
            ORDER BY id ASC
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, window),
        )
    else:
        if start_id is None:
            raise HTTPException(status_code=400, detail="Review mode requires an id value.")
        if end_id is None:
            cur.execute(
                tick_select_sql(select_sql, "AND id >= %s", "id ASC", "LIMIT %s"),
                (TICK_SYMBOL, start_id, window),
            )
        else:
            cur.execute(
                tick_select_sql(select_sql, "AND id >= %s AND id <= %s", "id ASC", "LIMIT %s"),
                (TICK_SYMBOL, start_id, end_id, window),
            )
    return [dict(row) for row in cur.fetchall()]


def query_rows_after(
    cur: Any,
    after_id: int,
    limit: int,
    *,
    end_id: Optional[int] = None,
    include_rows: bool = True,
) -> List[Dict[str, Any]]:
    select_sql = tick_columns(include_rows)
    if end_id is None:
        cur.execute(
            tick_select_sql(select_sql, "AND id > %s", "id ASC", "LIMIT %s"),
            (TICK_SYMBOL, after_id, limit),
        )
    else:
        cur.execute(
            tick_select_sql(select_sql, "AND id > %s AND id <= %s", "id ASC", "LIMIT %s"),
            (TICK_SYMBOL, after_id, end_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_rows_before(cur: Any, before_id: int, limit: int, *, include_rows: bool = True) -> List[Dict[str, Any]]:
    select_sql = tick_columns(include_rows)
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id < %s
            ORDER BY id DESC
            LIMIT %s
        ) older
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, before_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_rows_after(
    after_id: int,
    limit: int,
    end_id: Optional[int] = None,
    *,
    include_rows: bool = True,
) -> List[Dict[str, Any]]:
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return query_rows_after(cur, after_id, limit, end_id=end_id, include_rows=include_rows)


def fetch_rows_before(before_id: int, limit: int, *, include_rows: bool = True) -> List[Dict[str, Any]]:
    limit = clamp_int(limit, 1, MAX_TICK_HISTORY_LIMIT if include_rows else MAX_ZIG_HISTORY_LIMIT)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return query_rows_before(cur, before_id, limit, include_rows=include_rows)


def serialize_tick_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_tick_row(row) for row in rows]


def fetch_zig_snapshot_rows(
    cur: Any,
    *,
    range_start_id: Optional[int],
    range_end_id: Optional[int],
    cursor_id: Optional[int],
    include_left_neighbor: bool = True,
) -> List[Dict[str, Any]]:
    if range_start_id is None or range_end_id is None or cursor_id is None:
        return []

    cur.execute(
        """
        SELECT DISTINCT ON (pivot_id)
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
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND source_tick_id BETWEEN %s AND %s
          AND visible_from_tick_id <= %s
          AND (visible_to_tick_id IS NULL OR visible_to_tick_id >= %s)
        ORDER BY pivot_id ASC, version_id DESC
        """,
        (TICK_SYMBOL, range_start_id, range_end_id, cursor_id, cursor_id),
    )
    rows = [dict(row) for row in cur.fetchall()]
    if not rows or not include_left_neighbor:
        return rows

    cur.execute(
        """
        SELECT DISTINCT ON (pivot_id)
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
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND source_tick_id < %s
          AND visible_from_tick_id <= %s
          AND (visible_to_tick_id IS NULL OR visible_to_tick_id >= %s)
        ORDER BY pivot_id DESC, version_id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL, range_start_id, cursor_id, cursor_id),
    )
    previous = cur.fetchone()
    if previous and int(previous["pivot_id"]) != int(rows[0]["pivot_id"]):
        rows.insert(0, dict(previous))
    return rows


def fetch_zig_changes(cur: Any, *, after_tick_id: int, upto_tick_id: Optional[int]) -> List[Dict[str, Any]]:
    if upto_tick_id is not None and upto_tick_id <= after_tick_id:
        return []
    parameters: List[Any] = [TICK_SYMBOL, after_tick_id]
    where_parts = ["symbol = %s", "visible_from_tick_id > %s"]
    if upto_tick_id is not None:
        where_parts.append("visible_from_tick_id <= %s")
        parameters.append(upto_tick_id)
    parameters.append(MAX_ZIG_STREAM_BATCH)
    cur.execute(
        """
        SELECT
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
        FROM public.fast_zig_pivots
        WHERE {where_sql}
        ORDER BY visible_from_tick_id ASC, pivot_id ASC, version_id ASC
        LIMIT %s
        """.format(where_sql=" AND ".join(where_parts)),
        tuple(parameters),
    )
    return [dict(row) for row in cur.fetchall()]


def serialize_zig_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_zig_row(row) for row in rows]


def clamp_zig_candle_window(value: int) -> int:
    return clamp_int(value, 1, MAX_ZIG_CANDLE_WINDOW)


def clamp_zig_candle_history_limit(value: int) -> int:
    return clamp_int(value, 1, MAX_ZIG_CANDLE_HISTORY_LIMIT)


def clamp_zig_level(value: int) -> int:
    return clamp_int(value, 0, MAX_ZIG_LEVEL)


def clamp_zone_min_dwell_ticks(value: int) -> int:
    return clamp_int(value, 4, MAX_ZONE_MIN_DWELL_TICKS)


def clamp_zone_min_dwell_ms(value: int) -> int:
    return clamp_int(value, 100, MAX_ZONE_MIN_DWELL_MS)


def clamp_zone_same_side_tolerance(value: float) -> float:
    return round(clamp_float(value, 0.0, MAX_ZONE_SAME_SIDE_TOLERANCE), 6)


def clamp_zone_allowed_overshoot(value: float) -> float:
    return round(clamp_float(value, 0.0, MAX_ZONE_ALLOWED_OVERSHOOT), 6)


def clamp_zone_breakout_ticks(value: int) -> int:
    return clamp_int(value, 1, MAX_ZONE_BREAKOUT_TICKS)


def clamp_zone_breakout_tolerance(value: float) -> float:
    return round(clamp_float(value, 0.0, MAX_ZONE_BREAKOUT_TOLERANCE), 6)


def clamp_zone_height_value(value: float) -> float:
    return round(clamp_float(value, 0.0, MAX_ZONE_HEIGHT), 6)


def build_zone_settings(
    *,
    enabled: bool,
    min_dwell_ticks: int,
    min_dwell_ms: int,
    same_side_tolerance: float,
    allowed_overshoot: float,
    breakout_ticks: int,
    breakout_tolerance: float,
    min_height: float,
    max_height: float,
) -> Dict[str, Any]:
    effective_min_height = clamp_zone_height_value(min_height)
    effective_max_height = clamp_zone_height_value(max_height)
    if effective_max_height < effective_min_height:
        effective_max_height = effective_min_height
    effective_min_dwell_ticks = clamp_zone_min_dwell_ticks(min_dwell_ticks)
    effective_min_dwell_ms = clamp_zone_min_dwell_ms(min_dwell_ms)
    return {
        "enabled": bool(enabled),
        "minDwellTicks": effective_min_dwell_ticks,
        "minDwellMs": effective_min_dwell_ms,
        "sameSideTolerance": clamp_zone_same_side_tolerance(same_side_tolerance),
        "allowedOvershoot": clamp_zone_allowed_overshoot(allowed_overshoot),
        "continuationTolerance": clamp_zone_allowed_overshoot(allowed_overshoot),
        "breakoutTicks": clamp_zone_breakout_ticks(breakout_ticks),
        "breakoutTolerance": clamp_zone_breakout_tolerance(breakout_tolerance),
        "minHeight": effective_min_height,
        "maxHeight": effective_max_height,
        "contextPivots": ZONE_CONTEXT_PIVOTS,
        "warmupTicks": max(ZONE_MIN_WARMUP_TICKS, effective_min_dwell_ticks * ZONE_WARMUP_MULTIPLIER),
    }


def price_series_value(row: Dict[str, Any], series: str) -> Optional[float]:
    direct_value = row.get(series)
    if direct_value is not None:
        return float(direct_value)
    if series == "mid" and row.get("bid") is not None and row.get("ask") is not None:
        return round((float(row["bid"]) + float(row["ask"])) / 2.0, 2)
    return None


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


def zig_level_boundary_start_tick_id(pivot: Dict[str, Any], selected_level: int) -> Optional[int]:
    source_tick_id = pivot.get("source_tick_id")
    if source_tick_id is None:
        return None
    if selected_level == 0:
        return max(1, int(source_tick_id) - LEVEL_ZERO_PROVING_TICKS)
    visible_tick_id = pivot.get("selected_visible_from_tick_id")
    if visible_tick_id is None:
        return None
    return int(visible_tick_id)


def zig_level_boundary_end_tick_id(pivot: Dict[str, Any]) -> Optional[int]:
    visible_tick_id = pivot.get("selected_visible_from_tick_id")
    if visible_tick_id is None:
        return None
    return int(visible_tick_id)


def query_tick_window_before_cursor(
    cur: Any,
    *,
    cursor_id: int,
    window: int,
    offset: int = 0,
    minimum_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    where_parts = ["symbol = %s", "id <= %s"]
    parameters: List[Any] = [TICK_SYMBOL, cursor_id]
    if minimum_id is not None:
        where_parts.append("id >= %s")
        parameters.append(minimum_id)
    parameters.extend([offset, window])
    cur.execute(
        """
        SELECT id, timestamp
        FROM (
            SELECT id, timestamp
            FROM public.ticks
            WHERE {where_sql}
            ORDER BY id DESC
            OFFSET %s
            LIMIT %s
        ) recent
        ORDER BY id ASC
        """.format(where_sql=" AND ".join(where_parts)),
        tuple(parameters),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_zig_candle_pivot_ids(
    cur: Any,
    *,
    range_start_id: int,
    cursor_id: int,
    selected_level: int,
) -> List[int]:
    cur.execute(
        """
        SELECT DISTINCT pivot_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND level >= %s
          AND visible_from_tick_id <= %s
          AND visible_from_tick_id >= %s
        ORDER BY pivot_id ASC
        """,
        (TICK_SYMBOL, selected_level, cursor_id, range_start_id),
    )
    pivot_ids = [int(row["pivot_id"]) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT pivot_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND level >= %s
          AND visible_from_tick_id <= %s
          AND visible_from_tick_id < %s
        ORDER BY visible_from_tick_id DESC, pivot_id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL, selected_level, cursor_id, range_start_id),
    )
    previous = cur.fetchone()
    if previous:
        previous_id = int(previous["pivot_id"])
        if not pivot_ids or pivot_ids[0] != previous_id:
            pivot_ids.insert(0, previous_id)
    return pivot_ids


def fetch_zig_candle_pivots(
    cur: Any,
    *,
    pivot_ids: List[int],
    cursor_id: int,
    selected_level: int,
) -> List[Dict[str, Any]]:
    if not pivot_ids:
        return []

    cur.execute(
        """
        SELECT pivot_id, MIN(visible_from_tick_id) AS selected_visible_from_tick_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND pivot_id = ANY(%s)
          AND level >= %s
          AND visible_from_tick_id <= %s
        GROUP BY pivot_id
        """,
        (TICK_SYMBOL, pivot_ids, selected_level, cursor_id),
    )
    selected_bounds = {
        int(row["pivot_id"]): int(row["selected_visible_from_tick_id"])
        for row in cur.fetchall()
        if row.get("selected_visible_from_tick_id") is not None
    }
    if not selected_bounds:
        return []

    cur.execute(
        """
        SELECT DISTINCT ON (pivot_id)
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
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND pivot_id = ANY(%s)
          AND level >= %s
          AND visible_from_tick_id <= %s
          AND (visible_to_tick_id IS NULL OR visible_to_tick_id >= %s)
        ORDER BY pivot_id ASC, version_id DESC
        """,
        (TICK_SYMBOL, list(selected_bounds.keys()), selected_level, cursor_id, cursor_id),
    )
    rows = []
    for row in cur.fetchall():
        item = dict(row)
        item["selected_visible_from_tick_id"] = selected_bounds.get(int(item["pivot_id"]))
        if item["selected_visible_from_tick_id"] is not None:
            rows.append(item)
    return rows


def fetch_selected_zig_pivot_ids(
    cur: Any,
    *,
    range_start_id: int,
    cursor_id: int,
    selected_level: int,
    left_context_count: int = ZONE_CONTEXT_PIVOTS + 1,
) -> List[int]:
    cur.execute(
        """
        SELECT DISTINCT pivot_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND level >= %s
          AND visible_from_tick_id <= %s
          AND visible_from_tick_id >= %s
        ORDER BY pivot_id ASC
        """,
        (TICK_SYMBOL, selected_level, cursor_id, range_start_id),
    )
    pivot_ids = [int(row["pivot_id"]) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT pivot_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND level >= %s
          AND visible_from_tick_id < %s
        ORDER BY visible_from_tick_id DESC, pivot_id DESC
        LIMIT %s
        """,
        (TICK_SYMBOL, selected_level, range_start_id, left_context_count),
    )
    left_context_ids = [int(row["pivot_id"]) for row in cur.fetchall()]
    left_context_ids.reverse()
    combined_ids: List[int] = []
    seen_ids = set()
    for pivot_id in left_context_ids + pivot_ids:
        if pivot_id in seen_ids:
            continue
        seen_ids.add(pivot_id)
        combined_ids.append(pivot_id)
    return combined_ids


def fetch_ticks_for_zig_candle_range(cur: Any, *, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM public.ticks
        WHERE symbol = %s
          AND id >= %s
          AND id <= %s
        ORDER BY id ASC
        """,
        (TICK_SYMBOL, start_id, end_id),
    )
    return [dict(row) for row in cur.fetchall()]


def pivots_alternate(pivots: List[Dict[str, Any]]) -> bool:
    if len(pivots) < 2:
        return False
    return all(str(pivots[index]["direction"]) != str(pivots[index - 1]["direction"]) for index in range(1, len(pivots)))


def round_price(value: float) -> float:
    return round(float(value), 6)


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
    pattern = (str(anchor_a.get("direction") or "").lower(), str(anchor_b.get("direction") or "").lower(), str(anchor_c.get("direction") or "").lower())
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
                "selectedVisibleFromTickId": int(pivot["selected_visible_from_tick_id"]),
            }
        )

    return {
        "pattern_type": pattern_type,
        "birth_rule": birth_rule,
        "zone_low": zone_low,
        "zone_high": zone_high,
        "same_side_distance": same_side_distance,
        "anchor_start_pivot_id": int(anchor_a["pivot_id"]),
        "anchor_middle_pivot_id": int(anchor_b["pivot_id"]),
        "anchor_end_pivot_id": int(anchor_c["pivot_id"]),
        "anchor_pivots": anchor_pivots,
    }


def zone_payload_from_state(zone: Dict[str, Any]) -> Dict[str, Any]:
    duration_inside_ms = max(0, int(zone["last_inside_timestamp_ms"]) - int(zone["start_timestamp_ms"]))
    episode_duration_ms = max(0, int(zone["right_timestamp_ms"]) - int(zone["start_timestamp_ms"]))
    return {
        "id": zone["id"],
        "symbol": zone["symbol"],
        "selectedLevel": zone["selected_level"],
        "status": zone["status"],
        "startTickId": zone["start_tick_id"],
        "endTickId": zone["breakout_tick_id"] if zone["status"] == "closed" else None,
        "rightTickId": zone["right_tick_id"],
        "startTimestamp": zone["start_timestamp"],
        "endTimestamp": zone["breakout_timestamp"] if zone["status"] == "closed" else None,
        "rightTimestamp": zone["right_timestamp"],
        "startTimestampMs": zone["start_timestamp_ms"],
        "endTimestampMs": zone["breakout_timestamp_ms"] if zone["status"] == "closed" else None,
        "rightTimestampMs": zone["right_timestamp_ms"],
        "zoneLow": round_price(zone["zone_low"]),
        "zoneHigh": round_price(zone["zone_high"]),
        "zoneHeight": round_price(zone["zone_high"] - zone["zone_low"]),
        "initialZoneLow": round_price(zone["initial_zone_low"]),
        "initialZoneHigh": round_price(zone["initial_zone_high"]),
        "initialZoneHeight": round_price(zone["initial_zone_high"] - zone["initial_zone_low"]),
        "tickCountInside": zone["tick_count_inside"],
        "durationInsideMs": duration_inside_ms,
        "durationInsideLabel": format_duration_ms(duration_inside_ms),
        "episodeDurationMs": episode_duration_ms,
        "episodeDurationLabel": format_duration_ms(episode_duration_ms),
        "openTimestamp": zone["start_timestamp"],
        "closeTimestamp": zone["breakout_timestamp"] if zone["status"] == "closed" else None,
        "touchCount": zone["touch_count"],
        "revisitCount": zone["revisit_count"],
        "patternType": zone["pattern_type"],
        "birthRule": zone["birth_rule"],
        "sameSideDistance": round_price(zone["same_side_distance"]),
        "sameSideTolerance": round_price(zone["same_side_tolerance"]),
        "continuationTolerance": round_price(zone["allowed_overshoot"]),
        "maxAllowedOvershoot": round_price(zone["allowed_overshoot"]),
        "breakoutDirection": zone["breakout_direction"],
        "breakoutTickId": zone["breakout_tick_id"],
        "breakoutTimestamp": zone["breakout_timestamp"],
        "breakoutTimestampMs": zone["breakout_timestamp_ms"],
        "anchorStartPivotId": zone["anchor_start_pivot_id"],
        "anchorMiddlePivotId": zone["anchor_middle_pivot_id"],
        "anchorEndPivotId": zone["anchor_end_pivot_id"],
        "anchorPivots": zone["anchor_pivots"],
        "parentStartPivotId": zone["parent_start_pivot_id"],
        "parentEndPivotId": zone["parent_end_pivot_id"],
        "contextStartPivotId": zone["context_start_pivot_id"],
        "contextEndPivotId": zone["context_end_pivot_id"],
        "derivedFromAcceptance": True,
        "seedLow": round_price(zone["initial_zone_low"]),
        "seedHigh": round_price(zone["initial_zone_high"]),
        "seedHeight": round_price(zone["initial_zone_high"] - zone["initial_zone_low"]),
    }


def build_zig_zone_rows(
    cur: Any,
    *,
    range_start_id: Optional[int],
    cursor_id: Optional[int],
    selected_level: int,
    series: str,
    zone_settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if (
        not zone_settings.get("enabled")
        or range_start_id is None
        or cursor_id is None
        or cursor_id < range_start_id
    ):
        return []

    warmup_rows = query_rows_before(
        cur,
        range_start_id,
        int(zone_settings["warmupTicks"]),
        include_rows=True,
    )
    visible_rows = fetch_ticks_for_zig_candle_range(cur, start_id=range_start_id, end_id=cursor_id)
    tick_rows = warmup_rows + visible_rows
    if not tick_rows:
        return []

    context_start_id = int(tick_rows[0]["id"])
    pivot_ids = fetch_selected_zig_pivot_ids(
        cur,
        range_start_id=context_start_id,
        cursor_id=cursor_id,
        selected_level=selected_level,
    )
    pivots = fetch_zig_candle_pivots(
        cur,
        pivot_ids=pivot_ids,
        cursor_id=cursor_id,
        selected_level=selected_level,
    )
    if len(pivots) < int(zone_settings["contextPivots"]):
        return []

    visible_pivots: List[Dict[str, Any]] = []
    pivot_index = 0
    zones: List[Dict[str, Any]] = []
    active_zone: Optional[Dict[str, Any]] = None
    next_zone_id = 1
    touch_tolerance = max(float(zone_settings["allowedOvershoot"]) * 0.6, 0.01)

    for tick_row in tick_rows:
        tick_id = int(tick_row["id"])
        newly_visible_pivots: List[Dict[str, Any]] = []
        while pivot_index < len(pivots) and int(pivots[pivot_index]["selected_visible_from_tick_id"]) <= tick_id:
            visible_pivots.append(pivots[pivot_index])
            newly_visible_pivots.append(pivots[pivot_index])
            if len(visible_pivots) > 12:
                visible_pivots = visible_pivots[-12:]
            pivot_index += 1

        price = price_series_value(tick_row, series)
        if price is None:
            continue
        timestamp_ms = dt_to_ms(tick_row["timestamp"])
        if timestamp_ms is None:
            continue
        price_value = float(price)

        if active_zone is None:
            if len(newly_visible_pivots) == 0 or len(visible_pivots) < int(zone_settings["contextPivots"]):
                continue
            anchor_pivots = visible_pivots[-int(zone_settings["contextPivots"]):]
            if not pivots_alternate(anchor_pivots):
                continue

            candidate = zone_birth_candidate(
                anchor_pivots,
                same_side_tolerance=float(zone_settings["sameSideTolerance"]),
                min_height=float(zone_settings["minHeight"]),
                max_height=float(zone_settings["maxHeight"]),
            )
            if candidate is None:
                continue

            if not zone_contains_with_tolerance(
                price_value,
                float(candidate["zone_low"]),
                float(candidate["zone_high"]),
                float(zone_settings["allowedOvershoot"]),
            ):
                continue

            active_zone = {
                "id": "zone:{0}:{1}".format(selected_level, next_zone_id),
                "symbol": TICK_SYMBOL,
                "selected_level": selected_level,
                "status": "provisional",
                "start_tick_id": tick_id,
                "start_timestamp": tick_row["timestamp"].isoformat(),
                "start_timestamp_ms": timestamp_ms,
                "right_tick_id": tick_id,
                "right_timestamp": tick_row["timestamp"].isoformat(),
                "right_timestamp_ms": timestamp_ms,
                "last_inside_timestamp_ms": timestamp_ms,
                "zone_low": float(candidate["zone_low"]),
                "zone_high": float(candidate["zone_high"]),
                "initial_zone_low": float(candidate["zone_low"]),
                "initial_zone_high": float(candidate["zone_high"]),
                "tick_count_inside": 1,
                "touch_count": 0,
                "revisit_count": 0,
                "pattern_type": candidate["pattern_type"],
                "birth_rule": candidate["birth_rule"],
                "same_side_distance": float(candidate["same_side_distance"]),
                "same_side_tolerance": float(zone_settings["sameSideTolerance"]),
                "allowed_overshoot": float(zone_settings["allowedOvershoot"]),
                "breakout_direction": None,
                "breakout_tick_id": None,
                "breakout_timestamp": None,
                "breakout_timestamp_ms": None,
                "outside_direction": None,
                "outside_streak": 0,
                "anchor_start_pivot_id": int(candidate["anchor_start_pivot_id"]),
                "anchor_middle_pivot_id": int(candidate["anchor_middle_pivot_id"]),
                "anchor_end_pivot_id": int(candidate["anchor_end_pivot_id"]),
                "anchor_pivots": candidate["anchor_pivots"],
                "parent_start_pivot_id": int(candidate["anchor_start_pivot_id"]),
                "parent_end_pivot_id": int(candidate["anchor_end_pivot_id"]),
                "context_start_pivot_id": int(candidate["anchor_start_pivot_id"]),
                "context_end_pivot_id": int(candidate["anchor_end_pivot_id"]),
                "last_touch_side": None,
            }
            next_zone_id += 1
            touch_side = zone_touch_side(price_value, float(candidate["zone_low"]), float(candidate["zone_high"]), touch_tolerance)
            if touch_side:
                active_zone["touch_count"] = 1
                active_zone["last_touch_side"] = touch_side
            if (
                active_zone["tick_count_inside"] >= int(zone_settings["minDwellTicks"])
                and timestamp_ms - int(active_zone["start_timestamp_ms"]) >= int(zone_settings["minDwellMs"])
            ):
                active_zone["status"] = "active"
            continue

        active_zone["right_tick_id"] = tick_id
        active_zone["right_timestamp"] = tick_row["timestamp"].isoformat()
        active_zone["right_timestamp_ms"] = timestamp_ms
        if visible_pivots:
            active_zone["context_end_pivot_id"] = int(visible_pivots[-1]["pivot_id"])

        pivot_confirm_direction: Optional[str] = None
        for pivot in newly_visible_pivots:
            direction = pivot_breakout_direction(
                pivot,
                low=float(active_zone["zone_low"]),
                high=float(active_zone["zone_high"]),
                tolerance=float(zone_settings["allowedOvershoot"]),
            )
            if direction is not None:
                pivot_confirm_direction = direction
                break

        within_overshoot = zone_contains_with_tolerance(
            price_value,
            float(active_zone["zone_low"]),
            float(active_zone["zone_high"]),
            float(zone_settings["allowedOvershoot"]),
        )
        if within_overshoot:
            if active_zone["outside_streak"] > 0:
                active_zone["revisit_count"] += 1
            active_zone["outside_streak"] = 0
            active_zone["outside_direction"] = None
            active_zone["tick_count_inside"] += 1
            active_zone["last_inside_timestamp_ms"] = timestamp_ms
            touch_side = zone_touch_side(price_value, float(active_zone["zone_low"]), float(active_zone["zone_high"]), touch_tolerance)
            if touch_side and touch_side != active_zone.get("last_touch_side"):
                active_zone["touch_count"] += 1
            active_zone["last_touch_side"] = touch_side
            if (
                active_zone["status"] == "provisional"
                and active_zone["tick_count_inside"] >= int(zone_settings["minDwellTicks"])
                and timestamp_ms - int(active_zone["start_timestamp_ms"]) >= int(zone_settings["minDwellMs"])
            ):
                active_zone["status"] = "active"
            continue

        breakout_direction: Optional[str] = None
        if price_value > float(active_zone["zone_high"]) + float(zone_settings["breakoutTolerance"]):
            breakout_direction = "up"
        elif price_value < float(active_zone["zone_low"]) - float(zone_settings["breakoutTolerance"]):
            breakout_direction = "down"

        if breakout_direction is None:
            active_zone["outside_streak"] = 0
            active_zone["outside_direction"] = None
            active_zone["last_touch_side"] = None
            continue

        if active_zone["outside_direction"] == breakout_direction:
            active_zone["outside_streak"] += 1
        else:
            active_zone["outside_direction"] = breakout_direction
            active_zone["outside_streak"] = 1

        if (
            active_zone["outside_streak"] < int(zone_settings["breakoutTicks"])
            and pivot_confirm_direction != breakout_direction
        ):
            continue

        active_zone["status"] = "closed"
        active_zone["breakout_direction"] = breakout_direction
        active_zone["breakout_tick_id"] = tick_id
        active_zone["breakout_timestamp"] = tick_row["timestamp"].isoformat()
        active_zone["breakout_timestamp_ms"] = timestamp_ms
        if int(active_zone["right_tick_id"]) >= range_start_id:
            zones.append(zone_payload_from_state(active_zone))
        active_zone = None

    if active_zone is not None and int(active_zone["right_tick_id"]) >= range_start_id:
        zones.append(zone_payload_from_state(active_zone))
    return zones


def build_zig_candle_rows(
    cur: Any,
    *,
    range_start_id: Optional[int],
    cursor_id: Optional[int],
    selected_level: int,
    series: str,
    include_provisional: bool,
) -> List[Dict[str, Any]]:
    if range_start_id is None or cursor_id is None or cursor_id < range_start_id:
        return []

    pivot_ids = fetch_zig_candle_pivot_ids(
        cur,
        range_start_id=range_start_id,
        cursor_id=cursor_id,
        selected_level=selected_level,
    )
    pivots = fetch_zig_candle_pivots(
        cur,
        pivot_ids=pivot_ids,
        cursor_id=cursor_id,
        selected_level=selected_level,
    )
    if not pivots:
        return []

    bar_specs: List[Dict[str, Any]] = []
    for index in range(1, len(pivots)):
        start_pivot = pivots[index - 1]
        end_pivot = pivots[index]
        start_tick_id = zig_level_boundary_start_tick_id(start_pivot, selected_level)
        end_tick_id = zig_level_boundary_end_tick_id(end_pivot)
        if start_tick_id is None or end_tick_id is None or end_tick_id < start_tick_id:
            continue
        is_last_pair = index == len(pivots) - 1
        is_candidate_pair = is_last_pair and str(end_pivot.get("state") or "") == "candidate"
        bar_specs.append(
            {
                "start_pivot": start_pivot,
                "end_pivot": end_pivot,
                "start_tick_id": start_tick_id,
                "end_tick_id": end_tick_id,
                "is_final": not is_candidate_pair,
                "bar_state": "candidate" if is_candidate_pair else "final",
            }
        )

    if include_provisional:
        last_pivot = pivots[-1]
        if str(last_pivot.get("state") or "") != "candidate":
            start_tick_id = zig_level_boundary_start_tick_id(last_pivot, selected_level)
            if start_tick_id is not None and cursor_id >= start_tick_id:
                bar_specs.append(
                    {
                        "start_pivot": last_pivot,
                        "end_pivot": None,
                        "start_tick_id": start_tick_id,
                        "end_tick_id": cursor_id,
                        "is_final": False,
                        "bar_state": "active",
                    }
                )

    included_specs = [
        spec
        for spec in bar_specs
        if spec["end_tick_id"] >= range_start_id and spec["start_tick_id"] <= cursor_id
    ]
    if not included_specs:
        return []

    fetch_start_id = min(spec["start_tick_id"] for spec in included_specs)
    fetch_end_id = max(spec["end_tick_id"] for spec in included_specs)
    tick_rows = fetch_ticks_for_zig_candle_range(cur, start_id=fetch_start_id, end_id=fetch_end_id)
    if not tick_rows:
        return []

    tick_ids = [int(row["id"]) for row in tick_rows]
    bars: List[Dict[str, Any]] = []
    for spec in included_specs:
        left_index = bisect_left(tick_ids, spec["start_tick_id"])
        right_index = bisect_right(tick_ids, spec["end_tick_id"])
        segment_rows = tick_rows[left_index:right_index]
        if not segment_rows:
            continue
        prices = [price_series_value(row, series) for row in segment_rows]
        prices = [value for value in prices if value is not None]
        if not prices:
            continue
        first_tick = segment_rows[0]
        last_tick = segment_rows[-1]
        open_price = prices[0]
        close_price = prices[-1]
        high_price = max(prices)
        low_price = min(prices)
        duration_ms = max(0, dt_to_ms(last_tick["timestamp"]) - dt_to_ms(first_tick["timestamp"]))
        start_pivot = spec["start_pivot"]
        end_pivot = spec["end_pivot"]
        if end_pivot is not None:
            direction = "up" if float(end_pivot["pivot_price"]) >= float(start_pivot["pivot_price"]) else "down"
        elif close_price > open_price:
            direction = "up"
        elif close_price < open_price:
            direction = "down"
        else:
            direction = "flat"
        bars.append(
            {
                "id": "{0}:{1}".format(
                    int(start_pivot["pivot_id"]),
                    "active" if end_pivot is None else int(end_pivot["pivot_id"]),
                ),
                "symbol": TICK_SYMBOL,
                "level": selected_level,
                "series": series,
                "barState": spec["bar_state"],
                "isFinal": bool(spec["is_final"]),
                "isProvisional": not bool(spec["is_final"]),
                "direction": direction,
                "open": round(open_price, 6),
                "high": round(high_price, 6),
                "low": round(low_price, 6),
                "close": round(close_price, 6),
                "startTickId": int(first_tick["id"]),
                "endTickId": int(last_tick["id"]),
                "startTimestamp": first_tick["timestamp"].isoformat(),
                "endTimestamp": last_tick["timestamp"].isoformat(),
                "startTimestampMs": dt_to_ms(first_tick["timestamp"]),
                "endTimestampMs": dt_to_ms(last_tick["timestamp"]),
                "durationMs": duration_ms,
                "durationLabel": format_duration_ms(duration_ms),
                "tickCount": len(segment_rows),
                "priceRange": round(high_price - low_price, 6),
                "netMove": round(close_price - open_price, 6),
                "startPivotId": int(start_pivot["pivot_id"]),
                "endPivotId": int(end_pivot["pivot_id"]) if end_pivot is not None else None,
                "startPivotPrice": float(start_pivot["pivot_price"]),
                "endPivotPrice": float(end_pivot["pivot_price"]) if end_pivot is not None else None,
                "startPivotDirection": str(start_pivot["direction"]),
                "endPivotDirection": str(end_pivot["direction"]) if end_pivot is not None else None,
                "startPivotTickId": int(start_pivot["source_tick_id"]),
                "endPivotTickId": int(end_pivot["source_tick_id"]) if end_pivot is not None else None,
                "startBoundaryTickId": int(spec["start_tick_id"]),
                "endBoundaryTickId": int(spec["end_tick_id"]),
                "startVisibleFromTickId": int(start_pivot["selected_visible_from_tick_id"]),
                "endVisibleFromTickId": int(end_pivot["selected_visible_from_tick_id"]) if end_pivot is not None else None,
                "sourceVisibleFromTickId": int(start_pivot["visible_from_tick_id"]) if start_pivot.get("visible_from_tick_id") is not None else None,
                "sourceVisibleToTickId": int(start_pivot["visible_to_tick_id"]) if start_pivot.get("visible_to_tick_id") is not None else None,
                "labelTimestampMs": dt_to_ms(last_tick["timestamp"]),
            }
        )
    return bars


def build_zig_candle_range_payload(
    *,
    mode: str,
    window: int,
    selected_level: int,
    series: str,
    range_rows: List[Dict[str, Any]],
    candle_rows: List[Dict[str, Any]],
    zone_rows: List[Dict[str, Any]],
    area_rows: List[Dict[str, Any]],
    zone_settings: Dict[str, Any],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    serialize_ms: float,
) -> Dict[str, Any]:
    first_row = range_rows[0] if range_rows else None
    last_row = range_rows[-1] if range_rows else None
    first_row_id = first_row["id"] if first_row else None
    last_row_id = last_row["id"] if last_row else None
    return {
        "bars": candle_rows,
        "barCount": len(candle_rows),
        "zones": zone_rows,
        "zoneCount": len(zone_rows),
        "areaRows": area_rows,
        "areaCount": len(area_rows),
        "zoneConfig": zone_settings,
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "level": selected_level,
        "series": series,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=last_row,
        ),
    }


def load_zig_candle_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    zone_settings: Dict[str, Any],
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_level = clamp_zig_level(selected_level)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            review_end_id = bounds["lastId"] if mode == "review" else None
            review_end_timestamp = bounds["lastTimestamp"] if mode == "review" else None
            range_rows = fetch_bootstrap_tick_rows(
                cur,
                mode=mode,
                start_id=start_id,
                window=effective_window,
                end_id=review_end_id,
                include_rows=False,
            )
            range_first_id = range_rows[0]["id"] if range_rows else None
            range_last_id = range_rows[-1]["id"] if range_rows else None
            candle_rows = (
                build_zig_candle_rows(
                    cur,
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    selected_level=effective_level,
                    series=series,
                    include_provisional=include_provisional,
                )
                if zig_storage_ready(cur)
                else []
            )
            if zone_storage_ready(cur):
                zone_rows = fetch_persisted_zone_rows(
                    cur,
                    enabled=bool(zone_settings.get("enabled")),
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    selected_level=effective_level,
                )
                effective_zone_settings = persisted_zone_settings(enabled=bool(zone_settings.get("enabled")))
            else:
                zone_rows = (
                    build_zig_zone_rows(
                        cur,
                        range_start_id=range_first_id,
                        cursor_id=range_last_id,
                        selected_level=effective_level,
                        series=series,
                        zone_settings=zone_settings,
                    )
                    if zig_storage_ready(cur)
                    else []
                )
                effective_zone_settings = zone_settings
            area_rows = (
                fetch_supres_rows(
                    cur,
                    enabled=show_areas and supres_storage_ready(cur),
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    states=area_states,
                    sides=area_sides,
                    higher_only=area_higher_only,
                )
                if range_first_id is not None and range_last_id is not None
                else []
            )
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zig_candle_range_payload(
        mode=mode,
        window=effective_window,
        selected_level=effective_level,
        series=series,
        range_rows=range_rows,
        candle_rows=candle_rows,
        zone_rows=zone_rows,
        area_rows=area_rows,
        zone_settings=effective_zone_settings,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def load_zig_candle_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    review_start_id: Optional[int],
    zone_settings: Dict[str, Any],
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_level = clamp_zig_level(selected_level)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            step_rows = query_rows_after(cur, after_id, clamp_int(limit, 1, MAX_STREAM_BATCH), end_id=end_id, include_rows=False)
            next_last_id = step_rows[-1]["id"] if step_rows else after_id
            range_rows = query_tick_window_before_cursor(
                cur,
                cursor_id=next_last_id,
                window=effective_window,
                minimum_id=review_start_id,
            ) if next_last_id else []
            range_first_id = range_rows[0]["id"] if range_rows else None
            range_last_id = range_rows[-1]["id"] if range_rows else None
            candle_rows = (
                build_zig_candle_rows(
                    cur,
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    selected_level=effective_level,
                    series=series,
                    include_provisional=include_provisional,
                )
                if zig_storage_ready(cur)
                else []
            )
            if zone_storage_ready(cur):
                zone_rows = fetch_persisted_zone_rows(
                    cur,
                    enabled=bool(zone_settings.get("enabled")),
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    selected_level=effective_level,
                )
                effective_zone_settings = persisted_zone_settings(enabled=bool(zone_settings.get("enabled")))
            else:
                zone_rows = (
                    build_zig_zone_rows(
                        cur,
                        range_start_id=range_first_id,
                        cursor_id=range_last_id,
                        selected_level=effective_level,
                        series=series,
                        zone_settings=zone_settings,
                    )
                    if zig_storage_ready(cur)
                    else []
                )
                effective_zone_settings = zone_settings
            area_rows = (
                fetch_supres_rows(
                    cur,
                    enabled=show_areas and supres_storage_ready(cur),
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    states=area_states,
                    sides=area_sides,
                    higher_only=area_higher_only,
                )
                if range_first_id is not None and range_last_id is not None
                else []
            )
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zig_candle_range_payload(
        mode="review" if review_start_id is not None else "live",
        window=effective_window,
        selected_level=effective_level,
        series=series,
        range_rows=range_rows,
        candle_rows=candle_rows,
        zone_rows=zone_rows,
        area_rows=area_rows,
        zone_settings=effective_zone_settings,
        review_end_id=end_id,
        review_end_timestamp=bounds.get("lastTimestamp") if review_start_id is not None else None,
        bounds=bounds,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["lastId"] = range_last_id
    payload["endId"] = end_id
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def load_zig_candle_previous_payload(
    *,
    current_last_id: int,
    limit: int,
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    zone_settings: Dict[str, Any],
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_limit = clamp_zig_candle_history_limit(limit)
    effective_level = clamp_zig_level(selected_level)
    expanded_window = clamp_zig_candle_window(effective_window + effective_limit)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            range_rows = query_tick_window_before_cursor(
                cur,
                cursor_id=current_last_id,
                window=expanded_window,
            )
            range_first_id = range_rows[0]["id"] if range_rows else None
            range_last_id = range_rows[-1]["id"] if range_rows else None
            candle_rows = (
                build_zig_candle_rows(
                    cur,
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    selected_level=effective_level,
                    series=series,
                    include_provisional=include_provisional,
                )
                if zig_storage_ready(cur)
                else []
            )
            if zone_storage_ready(cur):
                zone_rows = fetch_persisted_zone_rows(
                    cur,
                    enabled=bool(zone_settings.get("enabled")),
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    selected_level=effective_level,
                )
                effective_zone_settings = persisted_zone_settings(enabled=bool(zone_settings.get("enabled")))
            else:
                zone_rows = (
                    build_zig_zone_rows(
                        cur,
                        range_start_id=range_first_id,
                        cursor_id=range_last_id,
                        selected_level=effective_level,
                        series=series,
                        zone_settings=zone_settings,
                    )
                    if zig_storage_ready(cur)
                    else []
                )
                effective_zone_settings = zone_settings
            area_rows = (
                fetch_supres_rows(
                    cur,
                    enabled=show_areas and supres_storage_ready(cur),
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    states=area_states,
                    sides=area_sides,
                    higher_only=area_higher_only,
                )
                if range_first_id is not None and range_last_id is not None
                else []
            )
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zig_candle_range_payload(
        mode="live",
        window=expanded_window,
        selected_level=effective_level,
        series=series,
        range_rows=range_rows,
        candle_rows=candle_rows,
        zone_rows=zone_rows,
        area_rows=area_rows,
        zone_settings=effective_zone_settings,
        review_end_id=None,
        review_end_timestamp=None,
        bounds=bounds,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["beforeId"] = range_first_id
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def stream_zig_candle_events(
    *,
    after_id: int,
    limit: int,
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    zone_settings: Dict[str, Any],
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_window = clamp_zig_candle_window(window)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_level = clamp_zig_level(selected_level)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                zig_ready = zig_storage_ready(cur)
                bounds_row = query_tick_bounds(cur)
                bounds = {
                    "firstId": bounds_row.get("first_id"),
                    "lastId": bounds_row.get("last_id"),
                    "firstTimestamp": bounds_row.get("first_timestamp"),
                    "lastTimestamp": bounds_row.get("last_timestamp"),
                }
                while True:
                    fetch_started = time.perf_counter()
                    step_rows = query_rows_after(cur, last_id, effective_limit, include_rows=False)
                    latest_tick_row = step_rows[-1] if step_rows else None
                    next_last_id = int(latest_tick_row["id"]) if latest_tick_row else last_id

                    if next_last_id > last_id:
                        range_rows = query_tick_window_before_cursor(
                            cur,
                            cursor_id=next_last_id,
                            window=effective_window,
                        )
                        range_first_id = range_rows[0]["id"] if range_rows else None
                        range_last_id = range_rows[-1]["id"] if range_rows else None
                        candle_rows = (
                            build_zig_candle_rows(
                                cur,
                                range_start_id=range_first_id,
                                cursor_id=range_last_id,
                                selected_level=effective_level,
                                series=series,
                                include_provisional=include_provisional,
                            )
                            if zig_ready
                            else []
                        )
                        if zone_storage_ready(cur):
                            zone_rows = fetch_persisted_zone_rows(
                                cur,
                                enabled=bool(zone_settings.get("enabled")),
                                range_start_id=range_first_id,
                                cursor_id=range_last_id,
                                selected_level=effective_level,
                            )
                            effective_zone_settings = persisted_zone_settings(enabled=bool(zone_settings.get("enabled")))
                        else:
                            zone_rows = (
                                build_zig_zone_rows(
                                    cur,
                                    range_start_id=range_first_id,
                                    cursor_id=range_last_id,
                                    selected_level=effective_level,
                                    series=series,
                                    zone_settings=zone_settings,
                                )
                                if zig_ready
                                else []
                            )
                            effective_zone_settings = zone_settings
                        area_rows = (
                            fetch_supres_rows(
                                cur,
                                enabled=show_areas and supres_storage_ready(cur),
                                range_start_id=range_first_id,
                                cursor_id=range_last_id,
                                states=area_states,
                                sides=area_sides,
                                higher_only=area_higher_only,
                            )
                            if range_first_id is not None and range_last_id is not None
                            else []
                        )
                        fetch_ms = elapsed_ms(fetch_started)
                        serialize_started = time.perf_counter()
                        payload = build_zig_candle_range_payload(
                            mode="live",
                            window=effective_window,
                            selected_level=effective_level,
                            series=series,
                            range_rows=range_rows,
                            candle_rows=candle_rows,
                            zone_rows=zone_rows,
                            area_rows=area_rows,
                            zone_settings=effective_zone_settings,
                            review_end_id=None,
                            review_end_timestamp=None,
                            bounds=bounds,
                            fetch_ms=fetch_ms,
                            serialize_ms=0.0,
                        )
                        payload["streamMode"] = "delta"
                        payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
                        last_id = next_last_id
                        yield format_sse(payload)
                        last_heartbeat = time.monotonic()
                        idle_sleep = STREAM_POLL_SECONDS
                        continue

                    now = time.monotonic()
                    fetch_ms = elapsed_ms(fetch_started)
                    if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                        latest_row = query_latest_tick(cur)
                        payload = {
                            "bars": [],
                            "barCount": 0,
                            "zones": [],
                            "zoneCount": 0,
                            "areaRows": [],
                            "areaCount": 0,
                            "lastId": last_id,
                            "window": effective_window,
                            "level": effective_level,
                            "series": series,
                            "streamMode": "heartbeat",
                            "pollSleepMs": round(idle_sleep * 1000.0, 2),
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=0.0,
                                latest_row=latest_row,
                            ),
                        }
                        yield format_sse(payload, event_name="heartbeat")
                        last_heartbeat = now
                    time.sleep(idle_sleep)
                    idle_sleep = STREAM_IDLE_POLL_SECONDS
    except GeneratorExit:
        return


def zig_storage_ready(cur: Any) -> bool:
    cur.execute(
        """
        SELECT
            to_regclass('public.fast_zig_pivots') AS pivots_table,
            to_regclass('public.fast_zig_state') AS state_table
        """
    )
    row = cur.fetchone() or {}
    if not row.get("pivots_table") or not row.get("state_table"):
        return False
    cur.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('fast_zig_pivots', 'fast_zig_state')
        """
    )
    columns: Dict[str, set[str]] = {
        "fast_zig_pivots": set(),
        "fast_zig_state": set(),
    }
    for info in cur.fetchall():
        columns.setdefault(info["table_name"], set()).add(info["column_name"])
    return (
        ZIG_REQUIRED_PIVOT_COLUMNS.issubset(columns.get("fast_zig_pivots", set()))
        and ZIG_REQUIRED_STATE_COLUMNS.issubset(columns.get("fast_zig_state", set()))
    )


def zone_storage_ready(cur: Any) -> bool:
    cur.execute(
        """
        SELECT
            to_regclass('public.zonebox') AS zonebox_table,
            to_regclass('public.zoneboxstate') AS zoneboxstate_table
        """
    )
    row = cur.fetchone() or {}
    if not row.get("zonebox_table") or not row.get("zoneboxstate_table"):
        return False
    cur.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('zonebox', 'zoneboxstate')
        """
    )
    columns: Dict[str, set[str]] = {
        "zonebox": set(),
        "zoneboxstate": set(),
    }
    for info in cur.fetchall():
        columns.setdefault(info["table_name"], set()).add(info["column_name"])
    return (
        ZONEBOX_REQUIRED_COLUMNS.issubset(columns.get("zonebox", set()))
        and ZONEBOXSTATE_REQUIRED_COLUMNS.issubset(columns.get("zoneboxstate", set()))
    )


def supres_storage_ready(cur: Any) -> bool:
    cur.execute(
        """
        SELECT
            to_regclass('public.supresarea') AS supresarea_table,
            to_regclass('public.supresareaevent') AS supresareaevent_table,
            to_regclass('public.supresstate') AS supresstate_table
        """
    )
    row = cur.fetchone() or {}
    if not row.get("supresarea_table") or not row.get("supresareaevent_table") or not row.get("supresstate_table"):
        return False
    cur.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('supresarea', 'supresareaevent', 'supresstate')
        """
    )
    columns: Dict[str, set[str]] = {
        "supresarea": set(),
        "supresareaevent": set(),
        "supresstate": set(),
    }
    for info in cur.fetchall():
        columns.setdefault(info["table_name"], set()).add(info["column_name"])
    return (
        SUPRESAREA_REQUIRED_COLUMNS.issubset(columns.get("supresarea", set()))
        and SUPRESAREAEVENT_REQUIRED_COLUMNS.issubset(columns.get("supresareaevent", set()))
        and SUPRESSTATE_REQUIRED_COLUMNS.issubset(columns.get("supresstate", set()))
    )


def persisted_zone_settings(*, enabled: bool) -> Dict[str, Any]:
    return build_zone_settings(
        enabled=enabled,
        min_dwell_ticks=DEFAULT_ZONE_MIN_DWELL_TICKS,
        min_dwell_ms=DEFAULT_ZONE_MIN_DWELL_MS,
        same_side_tolerance=DEFAULT_ZONE_SAME_SIDE_TOLERANCE,
        allowed_overshoot=DEFAULT_ZONE_ALLOWED_OVERSHOOT,
        breakout_ticks=DEFAULT_ZONE_BREAKOUT_TICKS,
        breakout_tolerance=DEFAULT_ZONE_BREAKOUT_TOLERANCE,
        min_height=DEFAULT_ZONE_MIN_HEIGHT,
        max_height=DEFAULT_ZONE_MAX_HEIGHT,
    )


def fetch_persisted_zone_rows(
    cur: Any,
    *,
    enabled: bool,
    range_start_id: Optional[int],
    cursor_id: Optional[int],
    selected_level: int,
) -> List[Dict[str, Any]]:
    if not enabled or range_start_id is None or cursor_id is None or cursor_id < range_start_id:
        return []
    cur.execute(
        """
        SELECT *
        FROM public.zonebox
        WHERE symbol = %s
          AND level = %s
          AND lasttickid >= %s
          AND starttickid <= %s
        ORDER BY starttickid ASC, id ASC
        """,
        (TICK_SYMBOL, selected_level, range_start_id, cursor_id),
    )
    return [serialize_zonebox_row(dict(row)) for row in cur.fetchall()]


def fetch_persisted_zone_rows_all_levels(
    cur: Any,
    *,
    enabled: bool,
    range_start_id: Optional[int],
    cursor_id: Optional[int],
) -> List[Dict[str, Any]]:
    if not enabled or range_start_id is None or cursor_id is None or cursor_id < range_start_id:
        return []
    cur.execute(
        """
        SELECT *
        FROM public.zonebox
        WHERE symbol = %s
          AND lasttickid >= %s
          AND starttickid <= %s
        ORDER BY level ASC, starttickid ASC, id ASC
        """,
        (TICK_SYMBOL, range_start_id, cursor_id),
    )
    return [serialize_zonebox_row(dict(row)) for row in cur.fetchall()]


def fetch_supres_rows(
    cur: Any,
    *,
    enabled: bool,
    range_start_id: Optional[int],
    cursor_id: Optional[int],
    states: List[str],
    sides: List[str],
    higher_only: bool,
) -> List[Dict[str, Any]]:
    if not enabled or cursor_id is None or range_start_id is None or cursor_id < range_start_id:
        return []
    cursor_row = query_tick_at_or_before(cur, cursor_id)
    cursor_timestamp = cursor_row.get("timestamp") if cursor_row else None
    cur.execute(
        """
        SELECT *
        FROM public.supresarea
        WHERE symbol = %s
          AND birthtickid <= %s
          AND (%s = false OR isl1extreme OR isl2extreme)
          AND state = ANY(%s)
          AND side = ANY(%s)
          AND (
                closetickid IS NULL
                OR closetickid >= %s
          )
        ORDER BY birthtickid ASC, priorityscore DESC, id ASC
        LIMIT %s
        """,
        (
            TICK_SYMBOL,
            cursor_id,
            bool(higher_only),
            states,
            sides,
            range_start_id,
            MAX_QUERY_ROWS,
        ),
    )
    payload_rows = []
    for row in cur.fetchall():
        payload = serialize_supresarea_row(dict(row))
        if payload.get("closeTickId") is not None:
            payload["rightTickId"] = payload["closeTickId"]
            payload["rightTime"] = payload.get("closeTime")
            payload["rightTimeMs"] = payload.get("closeTimeMs")
        else:
            payload["rightTickId"] = cursor_id
            payload["rightTime"] = cursor_timestamp.isoformat() if cursor_timestamp else None
            payload["rightTimeMs"] = dt_to_ms(cursor_timestamp)
        payload_rows.append(payload)
    return payload_rows


def query_tick_at_or_before(cur: Any, tick_id: int) -> Optional[Dict[str, Any]]:
    if tick_id <= 0:
        return None
    cur.execute(
        """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM public.ticks
        WHERE symbol = %s
          AND id <= %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL, tick_id),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_tick_rows_by_ids(cur: Any, tick_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not tick_ids:
        return {}
    unique_ids = sorted({int(tick_id) for tick_id in tick_ids if tick_id is not None and int(tick_id) > 0})
    if not unique_ids:
        return {}
    cur.execute(
        """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM public.ticks
        WHERE symbol = %s
          AND id = ANY(%s)
        ORDER BY id ASC
        """,
        (TICK_SYMBOL, unique_ids),
    )
    return {int(row["id"]): dict(row) for row in cur.fetchall()}


def fetch_zone_rows_starting_from(
    cur: Any,
    *,
    start_id: int,
    window: int,
    selected_level: int,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM public.zonebox
        WHERE symbol = %s
          AND level = %s
          AND lasttickid >= %s
        ORDER BY starttickid ASC, id ASC
        LIMIT %s
        """,
        (TICK_SYMBOL, selected_level, start_id, window),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_zone_rows_visible_before_cursor(
    cur: Any,
    *,
    cursor_id: int,
    window: int,
    selected_level: int,
    minimum_overlap_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if cursor_id <= 0:
        return []
    where_parts = [
        "symbol = %s",
        "level = %s",
        "starttickid <= %s",
    ]
    parameters: List[Any] = [TICK_SYMBOL, selected_level, cursor_id]
    if minimum_overlap_id is not None:
        where_parts.append("lasttickid >= %s")
        parameters.append(minimum_overlap_id)
    parameters.append(window)
    cur.execute(
        """
        SELECT *
        FROM (
            SELECT *
            FROM public.zonebox
            WHERE {where_sql}
            ORDER BY starttickid DESC, id DESC
            LIMIT %s
        ) recent
        ORDER BY starttickid ASC, id ASC
        """.format(where_sql=" AND ".join(where_parts)),
        tuple(parameters),
    )
    return [dict(row) for row in cur.fetchall()]


def has_persisted_zone_rows_before(
    cur: Any,
    *,
    first_row: Optional[Dict[str, Any]],
    selected_level: int,
    minimum_overlap_id: Optional[int] = None,
) -> bool:
    if not first_row:
        return False
    where_parts = [
        "symbol = %s",
        "level = %s",
        "(starttickid < %s OR (starttickid = %s AND id < %s))",
    ]
    parameters: List[Any] = [
        TICK_SYMBOL,
        selected_level,
        int(first_row["starttickid"]),
        int(first_row["starttickid"]),
        int(first_row["id"]),
    ]
    if minimum_overlap_id is not None:
        where_parts.append("lasttickid >= %s")
        parameters.append(minimum_overlap_id)
    cur.execute(
        """
        SELECT 1
        FROM public.zonebox
        WHERE {where_sql}
        LIMIT 1
        """.format(where_sql=" AND ".join(where_parts)),
        tuple(parameters),
    )
    return cur.fetchone() is not None


def capped_zone_payload_from_row(
    cur: Any,
    *,
    row: Dict[str, Any],
    cursor_id: int,
    series: str,
) -> Dict[str, Any]:
    base_payload = serialize_zonebox_row(dict(row))
    tick_rows = fetch_ticks_for_zig_candle_range(
        cur,
        start_id=int(row["starttickid"]),
        end_id=int(cursor_id),
    )
    priced_rows: List[Dict[str, Any]] = []
    for tick_row in tick_rows:
        price_value = price_series_value(tick_row, series)
        if price_value is None:
            continue
        priced_rows.append(
            {
                "id": int(tick_row["id"]),
                "timestamp": tick_row["timestamp"],
                "price": float(price_value),
            }
        )
    if not priced_rows:
        base_payload["status"] = ZONE_STATE_PROVISIONAL
        base_payload["endTickId"] = None
        base_payload["endTimestamp"] = None
        base_payload["endTimestampMs"] = None
        base_payload["rightTickId"] = int(row["starttickid"])
        base_payload["rightTimestamp"] = row["starttime"].isoformat() if row.get("starttime") else None
        base_payload["rightTimestampMs"] = dt_to_ms(row.get("starttime"))
        base_payload["tickCountInside"] = 0
        base_payload["durationInsideMs"] = 0
        base_payload["durationInsideLabel"] = format_duration_ms(0)
        base_payload["episodeDurationMs"] = 0
        base_payload["episodeDurationLabel"] = format_duration_ms(0)
        base_payload["closeTimestamp"] = None
        base_payload["touchCount"] = 0
        base_payload["revisitCount"] = 0
        base_payload["breakoutDirection"] = None
        base_payload["breakoutTickId"] = None
        base_payload["breakoutTimestamp"] = None
        base_payload["breakoutTimestampMs"] = None
        return base_payload

    start_ms = dt_to_ms(row.get("starttime")) or 0
    last_tick = priced_rows[-1]
    last_inside_tick = priced_rows[0]
    tick_count_inside = 0
    touch_count = 0
    revisit_count = 0
    outside_streak = 0
    last_touch_side: Optional[str] = None
    continuation_tolerance = float(row.get("continuationovershootused") or 0.0)
    touch_tolerance = max(continuation_tolerance * 0.6, 0.01)
    zone_low = float(row["zonelow"])
    zone_high = float(row["zonehigh"])

    for priced_row in priced_rows:
        if zone_contains_with_tolerance(
            priced_row["price"],
            zone_low,
            zone_high,
            continuation_tolerance,
        ):
            if outside_streak > 0:
                revisit_count += 1
            outside_streak = 0
            tick_count_inside += 1
            last_inside_tick = priced_row
            touch_side = zone_touch_side(
                priced_row["price"],
                zone_low,
                zone_high,
                touch_tolerance,
            )
            if touch_side and touch_side != last_touch_side:
                touch_count += 1
            last_touch_side = touch_side
            continue
        outside_streak += 1
        last_touch_side = None

    last_inside_ms = dt_to_ms(last_inside_tick["timestamp"]) or start_ms
    right_ms = dt_to_ms(last_tick["timestamp"]) or start_ms
    status = (
        ZONE_STATE_ACTIVE
        if tick_count_inside >= DEFAULT_ZONE_MIN_DWELL_TICKS
        and last_inside_ms - start_ms >= DEFAULT_ZONE_MIN_DWELL_MS
        else ZONE_STATE_PROVISIONAL
    )
    base_payload["status"] = status
    base_payload["endTickId"] = None
    base_payload["endTimestamp"] = None
    base_payload["endTimestampMs"] = None
    base_payload["rightTickId"] = int(last_tick["id"])
    base_payload["rightTimestamp"] = last_tick["timestamp"].isoformat()
    base_payload["rightTimestampMs"] = dt_to_ms(last_tick["timestamp"])
    base_payload["tickCountInside"] = tick_count_inside
    base_payload["durationInsideMs"] = max(0, last_inside_ms - start_ms)
    base_payload["durationInsideLabel"] = format_duration_ms(base_payload["durationInsideMs"])
    base_payload["episodeDurationMs"] = max(0, right_ms - start_ms)
    base_payload["episodeDurationLabel"] = format_duration_ms(base_payload["episodeDurationMs"])
    base_payload["closeTimestamp"] = None
    base_payload["touchCount"] = touch_count
    base_payload["revisitCount"] = revisit_count
    base_payload["breakoutDirection"] = None
    base_payload["breakoutTickId"] = None
    base_payload["breakoutTimestamp"] = None
    base_payload["breakoutTimestampMs"] = None
    return base_payload


def build_zone_candle_row(
    *,
    zone_payload: Dict[str, Any],
    start_tick: Dict[str, Any],
    close_tick: Dict[str, Any],
    series: str,
) -> Optional[Dict[str, Any]]:
    open_price = price_series_value(start_tick, series)
    close_price = price_series_value(close_tick, series)
    if open_price is None or close_price is None:
        return None
    direction = "flat"
    if close_price > open_price:
        direction = "up"
    elif close_price < open_price:
        direction = "down"
    close_tick_id = (
        int(zone_payload["endTickId"])
        if zone_payload["status"] == ZONE_STATE_CLOSED and zone_payload.get("endTickId") is not None
        else int(zone_payload["rightTickId"])
    )
    close_timestamp = (
        zone_payload["endTimestamp"]
        if zone_payload["status"] == ZONE_STATE_CLOSED and zone_payload.get("endTimestamp")
        else zone_payload["rightTimestamp"]
    )
    close_timestamp_ms = (
        zone_payload["endTimestampMs"]
        if zone_payload["status"] == ZONE_STATE_CLOSED and zone_payload.get("endTimestampMs") is not None
        else zone_payload["rightTimestampMs"]
    )
    return {
        "id": "zone-candle:{0}".format(zone_payload["id"]),
        "zoneId": zone_payload["id"],
        "symbol": zone_payload["symbol"],
        "level": zone_payload["selectedLevel"],
        "series": series,
        "barState": zone_payload["status"],
        "isFinal": zone_payload["status"] == ZONE_STATE_CLOSED,
        "isProvisional": zone_payload["status"] != ZONE_STATE_CLOSED,
        "direction": direction,
        "open": round(float(open_price), 6),
        "high": round(float(zone_payload["zoneHigh"]), 6),
        "low": round(float(zone_payload["zoneLow"]), 6),
        "close": round(float(close_price), 6),
        "startTickId": int(zone_payload["startTickId"]),
        "endTickId": close_tick_id,
        "rightTickId": int(zone_payload["rightTickId"]),
        "startTimestamp": zone_payload["startTimestamp"],
        "endTimestamp": close_timestamp,
        "startTimestampMs": zone_payload["startTimestampMs"],
        "endTimestampMs": close_timestamp_ms,
        "durationMs": int(zone_payload["episodeDurationMs"]),
        "durationLabel": zone_payload["episodeDurationLabel"],
        "tickCount": int(zone_payload["tickCountInside"]),
        "priceRange": round(float(zone_payload["zoneHigh"]) - float(zone_payload["zoneLow"]), 6),
        "netMove": round(float(close_price) - float(open_price), 6),
        "labelTimestampMs": close_timestamp_ms,
    }


def build_zone_episode_rows(
    cur: Any,
    *,
    zonebox_rows: List[Dict[str, Any]],
    cursor_id: int,
    series: str,
    include_provisional: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not zonebox_rows:
        return [], []

    visible_zone_rows: List[Dict[str, Any]] = []
    tick_ids: List[int] = []
    for row in zonebox_rows:
        start_tick_id = int(row["starttickid"])
        visible_until_id = int(row.get("endtickid") or row.get("lasttickid") or start_tick_id)
        if cursor_id < start_tick_id:
            continue
        if cursor_id < visible_until_id:
            zone_payload = capped_zone_payload_from_row(
                cur,
                row=row,
                cursor_id=cursor_id,
                series=series,
            )
        else:
            zone_payload = serialize_zonebox_row(dict(row))
        if not include_provisional and zone_payload["status"] != ZONE_STATE_CLOSED:
            continue
        visible_zone_rows.append(zone_payload)
        close_tick_id = (
            int(zone_payload["endTickId"])
            if zone_payload["status"] == ZONE_STATE_CLOSED and zone_payload.get("endTickId") is not None
            else int(zone_payload["rightTickId"])
        )
        tick_ids.extend([int(zone_payload["startTickId"]), close_tick_id])

    tick_rows_by_id = fetch_tick_rows_by_ids(cur, tick_ids)
    bars: List[Dict[str, Any]] = []
    filtered_zone_rows: List[Dict[str, Any]] = []
    for zone_payload in visible_zone_rows:
        close_tick_id = (
            int(zone_payload["endTickId"])
            if zone_payload["status"] == ZONE_STATE_CLOSED and zone_payload.get("endTickId") is not None
            else int(zone_payload["rightTickId"])
        )
        start_tick = tick_rows_by_id.get(int(zone_payload["startTickId"]))
        close_tick = tick_rows_by_id.get(close_tick_id)
        if not start_tick or not close_tick:
            continue
        candle_row = build_zone_candle_row(
            zone_payload=zone_payload,
            start_tick=start_tick,
            close_tick=close_tick,
            series=series,
        )
        if candle_row is None:
            continue
        bars.append(candle_row)
        filtered_zone_rows.append(zone_payload)
    return filtered_zone_rows, bars


def build_zone_range_payload(
    *,
    mode: str,
    window: int,
    selected_level: int,
    series: str,
    cursor_row: Optional[Dict[str, Any]],
    zone_rows: List[Dict[str, Any]],
    candle_rows: List[Dict[str, Any]],
    area_rows: List[Dict[str, Any]],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    has_more_left: bool,
    fetch_ms: float,
    serialize_ms: float,
) -> Dict[str, Any]:
    first_zone = zone_rows[0] if zone_rows else None
    last_zone = zone_rows[-1] if zone_rows else None
    cursor_id = cursor_row.get("id") if cursor_row else None
    cursor_timestamp = cursor_row.get("timestamp") if cursor_row else None
    return {
        "bars": candle_rows,
        "barCount": len(candle_rows),
        "zones": zone_rows,
        "zoneCount": len(zone_rows),
        "areaRows": area_rows,
        "areaCount": len(area_rows),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "level": selected_level,
        "series": series,
        "firstId": first_zone.get("startTickId") if first_zone else None,
        "lastId": cursor_id,
        "firstZoneId": first_zone.get("id") if first_zone else None,
        "lastZoneId": last_zone.get("id") if last_zone else None,
        "firstTimestamp": first_zone.get("startTimestamp") if first_zone else None,
        "lastTimestamp": last_zone.get("rightTimestamp") if last_zone else None,
        "firstTimestampMs": first_zone.get("startTimestampMs") if first_zone else None,
        "lastTimestampMs": last_zone.get("rightTimestampMs") if last_zone else None,
        "cursorId": cursor_id,
        "cursorTimestamp": serialize_value(cursor_timestamp),
        "cursorTimestampMs": dt_to_ms(cursor_timestamp),
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": has_more_left,
        "endReached": bool(mode == "review" and review_end_id is not None and cursor_id is not None and cursor_id >= review_end_id),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=cursor_row,
        ),
    }


def load_zone_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_level = clamp_zig_level(selected_level)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not zone_storage_ready(cur):
                raise HTTPException(status_code=503, detail="Persisted zone storage is not ready.")
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            review_end_id = bounds["lastId"] if mode == "review" else None
            review_end_timestamp = bounds["lastTimestamp"] if mode == "review" else None

            if mode == "review":
                if start_id is None:
                    raise HTTPException(status_code=400, detail="Review mode requires an id value.")
                seed_zone_rows = fetch_zone_rows_starting_from(
                    cur,
                    start_id=int(start_id),
                    window=effective_window,
                    selected_level=effective_level,
                )
                cursor_id = max(int(start_id), int(seed_zone_rows[-1]["starttickid"])) if seed_zone_rows else int(start_id)
                cursor_row = query_tick_at_or_before(cur, cursor_id)
                cursor_id = int(cursor_row["id"]) if cursor_row else int(start_id)
                zonebox_rows = fetch_zone_rows_visible_before_cursor(
                    cur,
                    cursor_id=cursor_id,
                    window=effective_window,
                    selected_level=effective_level,
                    minimum_overlap_id=int(start_id),
                )
                has_more_left = has_persisted_zone_rows_before(
                    cur,
                    first_row=zonebox_rows[0] if zonebox_rows else None,
                    selected_level=effective_level,
                )
            else:
                cursor_row = query_latest_tick(cur)
                cursor_id = int(cursor_row["id"]) if cursor_row else 0
                zonebox_rows = fetch_zone_rows_visible_before_cursor(
                    cur,
                    cursor_id=cursor_id,
                    window=effective_window,
                    selected_level=effective_level,
                )
                has_more_left = has_persisted_zone_rows_before(
                    cur,
                    first_row=zonebox_rows[0] if zonebox_rows else None,
                    selected_level=effective_level,
                )
            zone_rows, candle_rows = build_zone_episode_rows(
                cur,
                zonebox_rows=zonebox_rows,
                cursor_id=cursor_id,
                series=series,
                include_provisional=include_provisional,
            )
            area_rows = fetch_supres_rows(
                cur,
                enabled=show_areas and supres_storage_ready(cur),
                range_start_id=zone_rows[0]["startTickId"] if zone_rows else None,
                cursor_id=cursor_id,
                states=area_states,
                sides=area_sides,
                higher_only=area_higher_only,
            ) if cursor_id else []
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zone_range_payload(
        mode=mode,
        window=effective_window,
        selected_level=effective_level,
        series=series,
        cursor_row=cursor_row,
        zone_rows=zone_rows,
        candle_rows=candle_rows,
        area_rows=area_rows,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        has_more_left=has_more_left,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def load_zone_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    review_start_id: Optional[int],
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_level = clamp_zig_level(selected_level)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not zone_storage_ready(cur):
                raise HTTPException(status_code=503, detail="Persisted zone storage is not ready.")
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            step_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id, include_rows=False)
            next_cursor_id = int(step_rows[-1]["id"]) if step_rows else max(0, after_id)
            cursor_row = query_tick_at_or_before(cur, next_cursor_id)
            cursor_id = int(cursor_row["id"]) if cursor_row else next_cursor_id
            zonebox_rows = fetch_zone_rows_visible_before_cursor(
                cur,
                cursor_id=cursor_id,
                window=effective_window,
                selected_level=effective_level,
                minimum_overlap_id=int(review_start_id) if review_start_id is not None else None,
            )
            has_more_left = has_persisted_zone_rows_before(
                cur,
                first_row=zonebox_rows[0] if zonebox_rows else None,
                selected_level=effective_level,
            )
            zone_rows, candle_rows = build_zone_episode_rows(
                cur,
                zonebox_rows=zonebox_rows,
                cursor_id=cursor_id,
                series=series,
                include_provisional=include_provisional,
            )
            area_rows = fetch_supres_rows(
                cur,
                enabled=show_areas and supres_storage_ready(cur),
                range_start_id=zone_rows[0]["startTickId"] if zone_rows else None,
                cursor_id=cursor_id,
                states=area_states,
                sides=area_sides,
                higher_only=area_higher_only,
            ) if cursor_id else []
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zone_range_payload(
        mode="review" if review_start_id is not None else "live",
        window=effective_window,
        selected_level=effective_level,
        series=series,
        cursor_row=cursor_row,
        zone_rows=zone_rows,
        candle_rows=candle_rows,
        area_rows=area_rows,
        review_end_id=end_id,
        review_end_timestamp=bounds.get("lastTimestamp") if review_start_id is not None else None,
        has_more_left=has_more_left,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["lastId"] = cursor_id
    payload["endId"] = end_id
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def load_zone_previous_payload(
    *,
    current_last_id: int,
    limit: int,
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_limit = clamp_zig_candle_history_limit(limit)
    effective_level = clamp_zig_level(selected_level)
    expanded_window = clamp_zig_candle_window(effective_window + effective_limit)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not zone_storage_ready(cur):
                raise HTTPException(status_code=503, detail="Persisted zone storage is not ready.")
            cursor_row = query_tick_at_or_before(cur, current_last_id)
            cursor_id = int(cursor_row["id"]) if cursor_row else int(current_last_id)
            zonebox_rows = fetch_zone_rows_visible_before_cursor(
                cur,
                cursor_id=cursor_id,
                window=expanded_window,
                selected_level=effective_level,
            )
            has_more_left = has_persisted_zone_rows_before(
                cur,
                first_row=zonebox_rows[0] if zonebox_rows else None,
                selected_level=effective_level,
            )
            zone_rows, candle_rows = build_zone_episode_rows(
                cur,
                zonebox_rows=zonebox_rows,
                cursor_id=cursor_id,
                series=series,
                include_provisional=include_provisional,
            )
            area_rows = fetch_supres_rows(
                cur,
                enabled=show_areas and supres_storage_ready(cur),
                range_start_id=zone_rows[0]["startTickId"] if zone_rows else None,
                cursor_id=cursor_id,
                states=area_states,
                sides=area_sides,
                higher_only=area_higher_only,
            ) if cursor_id else []
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zone_range_payload(
        mode="live",
        window=expanded_window,
        selected_level=effective_level,
        series=series,
        cursor_row=cursor_row,
        zone_rows=zone_rows,
        candle_rows=candle_rows,
        area_rows=area_rows,
        review_end_id=None,
        review_end_timestamp=None,
        has_more_left=has_more_left,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["beforeId"] = zone_rows[0]["startTickId"] if zone_rows else None
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def stream_zone_events(
    *,
    after_id: int,
    limit: int,
    window: int,
    selected_level: int,
    series: str,
    include_provisional: bool,
    show_areas: bool,
    area_states: List[str],
    area_sides: List[str],
    area_higher_only: bool,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_window = clamp_zig_candle_window(window)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_level = clamp_zig_level(selected_level)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if not zone_storage_ready(cur):
                    raise HTTPException(status_code=503, detail="Persisted zone storage is not ready.")
                while True:
                    fetch_started = time.perf_counter()
                    step_rows = query_rows_after(cur, last_id, effective_limit, include_rows=False)
                    latest_tick_row = step_rows[-1] if step_rows else None
                    next_cursor_id = int(latest_tick_row["id"]) if latest_tick_row else last_id

                    if next_cursor_id > last_id:
                        cursor_row = query_tick_at_or_before(cur, next_cursor_id)
                        cursor_id = int(cursor_row["id"]) if cursor_row else next_cursor_id
                        zonebox_rows = fetch_zone_rows_visible_before_cursor(
                            cur,
                            cursor_id=cursor_id,
                            window=effective_window,
                            selected_level=effective_level,
                        )
                        has_more_left = has_persisted_zone_rows_before(
                            cur,
                            first_row=zonebox_rows[0] if zonebox_rows else None,
                            selected_level=effective_level,
                        )
                        zone_rows, candle_rows = build_zone_episode_rows(
                            cur,
                            zonebox_rows=zonebox_rows,
                            cursor_id=cursor_id,
                            series=series,
                            include_provisional=include_provisional,
                        )
                        area_rows = fetch_supres_rows(
                            cur,
                            enabled=show_areas and supres_storage_ready(cur),
                            range_start_id=zone_rows[0]["startTickId"] if zone_rows else None,
                            cursor_id=cursor_id,
                            states=area_states,
                            sides=area_sides,
                            higher_only=area_higher_only,
                        ) if cursor_id else []
                        fetch_ms = elapsed_ms(fetch_started)
                        serialize_started = time.perf_counter()
                        payload = build_zone_range_payload(
                            mode="live",
                            window=effective_window,
                            selected_level=effective_level,
                            series=series,
                            cursor_row=cursor_row,
                            zone_rows=zone_rows,
                            candle_rows=candle_rows,
                            area_rows=area_rows,
                            review_end_id=None,
                            review_end_timestamp=None,
                            has_more_left=has_more_left,
                            fetch_ms=fetch_ms,
                            serialize_ms=0.0,
                        )
                        payload["streamMode"] = "delta"
                        payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
                        last_id = cursor_id
                        yield format_sse(payload)
                        last_heartbeat = time.monotonic()
                        idle_sleep = STREAM_POLL_SECONDS
                        continue

                    now = time.monotonic()
                    fetch_ms = elapsed_ms(fetch_started)
                    if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                        latest_row = query_latest_tick(cur)
                        payload = {
                            "bars": [],
                            "barCount": 0,
                            "zones": [],
                            "zoneCount": 0,
                            "areaRows": [],
                            "areaCount": 0,
                            "lastId": last_id,
                            "window": effective_window,
                            "level": effective_level,
                            "series": series,
                            "streamMode": "heartbeat",
                            "pollSleepMs": round(idle_sleep * 1000.0, 2),
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=0.0,
                                latest_row=latest_row,
                            ),
                        }
                        yield format_sse(payload, event_name="heartbeat")
                        last_heartbeat = now
                    time.sleep(idle_sleep)
                    idle_sleep = STREAM_IDLE_POLL_SECONDS
    except GeneratorExit:
        return


def serialize_zoneboxstate_row(row: Dict[str, Any]) -> Dict[str, Any]:
    updated_at = row.get("updated_at")
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "level": row["level"],
        "lastProcessedTickId": row.get("lastprocessedtickid"),
        "lastProcessedPivotId": row.get("lastprocessedpivotid"),
        "activeZoneId": row.get("activezoneid"),
        "updatedAt": updated_at.isoformat() if updated_at else None,
        "updatedAtMs": dt_to_ms(updated_at),
    }


def fetch_zoneboxstate_snapshot(selected_level: int) -> Optional[Dict[str, Any]]:
    effective_level = clamp_zig_level(selected_level)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not zone_storage_ready(cur):
                raise HTTPException(
                    status_code=503,
                    detail="Persisted zone storage is not ready.",
                )
            cur.execute(
                """
                SELECT id, symbol, level, lastprocessedtickid, lastprocessedpivotid, activezoneid, updated_at
                FROM public.zoneboxstate
                WHERE symbol = %s
                  AND level = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (TICK_SYMBOL, effective_level),
            )
            row = cur.fetchone()
    return serialize_zoneboxstate_row(dict(row)) if row else None


def split_sql_script(sql_text: str) -> List[str]:
    text = (sql_text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="SQL text is required.")
    return [statement.strip() for statement in sqlparse.split(text) if statement.strip()]


def statement_head(statement: str) -> str:
    parsed = sqlparse.parse(statement)
    if not parsed:
        return ""
    for token in parsed[0].tokens:
        if token.is_whitespace or token.ttype in sqlparse.tokens.Comment:
            continue
        normalized = token.normalized.upper().strip()
        if normalized:
            return normalized.split(None, 1)[0]
    return ""


def describe_columns(description: Any) -> List[Dict[str, Any]]:
    if not description:
        return []
    return [{"name": item.name, "typeCode": item.type_code} for item in description]


def fetch_result_rows(cur: Any, max_rows: int = MAX_QUERY_ROWS) -> Tuple[List[List[Any]], bool]:
    rows = cur.fetchmany(max_rows + 1)
    truncated = len(rows) > max_rows
    rows = rows[:max_rows]
    return [[serialize_value(value) for value in row] for row in rows], truncated


def line_column_from_position(statement: str, position: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not position:
        return None, None
    try:
        absolute_position = max(int(position) - 1, 0)
    except (TypeError, ValueError):
        return None, None
    prefix = statement[:absolute_position]
    line = prefix.count("\n") + 1
    column = absolute_position - prefix.rfind("\n")
    return line, column


def fetch_sql_context(conn: Any) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                current_database() AS database_name,
                current_schema() AS current_schema,
                current_user AS current_user,
                current_setting('server_version') AS server_version
            """
        )
        row = cur.fetchone() or {}
    return {
        "database": row.get("database_name"),
        "currentSchema": row.get("current_schema"),
        "currentUser": row.get("current_user"),
        "serverVersion": row.get("server_version"),
    }


def serialize_pg_error(exc: Exception, statement: Optional[str] = None) -> Dict[str, Any]:
    diag = getattr(exc, "diag", None)
    position = getattr(diag, "statement_position", None) if diag else None
    line, column = line_column_from_position(statement or "", position)
    return {
        "message": getattr(diag, "message_primary", None) or str(exc),
        "detail": getattr(diag, "message_detail", None) if diag else None,
        "hint": getattr(diag, "message_hint", None) if diag else None,
        "position": int(position) if position and str(position).isdigit() else None,
        "line": line,
        "column": column,
        "sqlstate": getattr(exc, "pgcode", None),
        "statement": statement,
    }


def normalize_relation_reference(raw_reference: str) -> str:
    return raw_reference.replace(" ", "").replace('"', "").lower()


def validate_admin_query(sql_text: str) -> List[str]:
    statements = split_sql_script(sql_text)
    if not statements:
        raise HTTPException(status_code=400, detail="SQL text is required.")
    return statements


def relation_columns(conn: Any, schema_name: str, object_name: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                column_name,
                data_type,
                is_nullable = 'NO' AS not_null,
                column_default AS default_value
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, object_name),
        )
        return [
            {
                "name": row["column_name"],
                "dataType": row["data_type"],
                "notNull": row["not_null"],
                "default": row["default_value"],
                "isIdentity": False,
                "isGenerated": False,
            }
            for row in cur.fetchall()
        ]


def table_indexes(conn: Any, schema_name: str, object_name: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT indexname AS index_name, indexdef
            FROM pg_indexes
            WHERE schemaname = %s
              AND tablename = %s
            ORDER BY indexname
            """,
            (schema_name, object_name),
        )
        return [{"name": row["index_name"], "definition": row["indexdef"]} for row in cur.fetchall()]


def relation_summary(conn: Any, schema_name: str, object_name: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                c.oid,
                COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0)::bigint AS row_estimate,
                pg_total_relation_size(c.oid) AS total_bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                obj_description(c.oid, 'pg_class') AS comment
            FROM pg_class c
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s
              ON s.relid = c.oid
            WHERE n.nspname = %s
              AND c.relname = %s
              AND c.relkind IN ('r', 'p')
            """,
            (schema_name, object_name),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def sql_object_config(schema_name: str, object_name: str, object_kind: str = "table") -> Dict[str, Any]:
    config = SQL_EXPOSED_TABLES.get((schema_name, object_name))
    if not config or config["kind"] != object_kind:
        raise HTTPException(status_code=404, detail="That admin SQL object is not exposed.")
    return config


def schema_payload() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        tables = []
        for schema_name, object_name in SQL_EXPOSED_TABLES:
            summary = relation_summary(conn, schema_name, object_name)
            if not summary:
                continue
            tables.append(
                {
                    "name": object_name,
                    "schema": schema_name,
                    "kind": "table",
                    "rowEstimate": summary["row_estimate"],
                    "columns": relation_columns(conn, schema_name, object_name),
                    "indexes": table_indexes(conn, schema_name, object_name),
                    "totalSize": summary["total_size"],
                }
            )

    return {
        "context": context,
        "schemas": [
            {
                "schema": "public",
                "counts": {
                    "tables": len(tables),
                    "views": 0,
                    "materializedViews": 0,
                    "sequences": 0,
                    "functions": 0,
                },
                "objects": {
                    "tables": tables,
                    "views": [],
                    "materializedViews": [],
                    "sequences": [],
                    "functions": [],
                },
            }
        ],
    }

def load_object_details(schema_name: str, object_name: str, object_kind: str) -> Dict[str, Any]:
    config = sql_object_config(schema_name, object_name, object_kind)
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        summary = relation_summary(conn, schema_name, object_name)
        if not summary:
            raise HTTPException(status_code=404, detail="{0}.{1} was not found.".format(schema_name, object_name))
        columns = relation_columns(conn, schema_name, object_name)
        indexes = table_indexes(conn, schema_name, object_name)
    select_sql = config["select_sql"]

    return {
        "context": context,
        "object": {
            "schema": schema_name,
            "name": object_name,
            "kind": object_kind,
            "rowEstimate": summary["row_estimate"],
            "totalBytes": summary["total_bytes"],
            "totalSize": summary["total_size"],
            "comment": summary["comment"],
            "columns": columns,
            "indexes": indexes,
            "definition": None,
            "sequence": None,
        },
        "actions": {
            "insertSelect": select_sql,
            "insertExplain": "EXPLAIN\n{0}".format(select_sql),
        },
        "preview": {
            "orderBy": config["default_order_by"],
            "orderDir": config["default_order_dir"],
        },
    }


def preview_relation(
    schema_name: str,
    object_name: str,
    limit: int,
    offset: int,
    order_by: Optional[str],
    order_dir: str,
) -> Dict[str, Any]:
    config = sql_object_config(schema_name, object_name, "table")

    started = time.perf_counter()
    limit = clamp_int(limit, 1, MAX_SQL_PREVIEW_LIMIT)
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        columns = relation_columns(conn, schema_name, object_name)
        column_names = {column["name"] for column in columns}
        if order_by and order_by not in column_names:
            raise HTTPException(status_code=400, detail="Unknown sort column: {0}".format(order_by))

        direction = "DESC" if order_dir.lower() == "desc" else "ASC"
        effective_order = order_by or config["default_order_by"]
        if effective_order not in column_names:
            raise HTTPException(status_code=400, detail="Unknown sort column: {0}".format(effective_order))
        query = pg_sql.SQL("SELECT * FROM {} ORDER BY {} {} LIMIT %s OFFSET %s").format(
            pg_sql.SQL(".").join([pg_sql.Identifier(schema_name), pg_sql.Identifier(object_name)]),
            pg_sql.Identifier(effective_order),
            pg_sql.SQL(direction),
        )

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
            cur.execute(query, (limit + 1, offset))
            result_columns = describe_columns(cur.description)
            rows, truncated = fetch_result_rows(cur, limit)

    return {
        "context": context,
        "result": {
            "index": 1,
            "title": "{0}.{1}".format(schema_name, object_name),
            "statement": "preview",
            "statementType": "SELECT",
            "commandTag": "SELECT",
            "rowCount": len(rows),
            "elapsedMs": elapsed_ms(started),
            "columns": result_columns,
            "rows": rows,
            "truncated": truncated,
            "maxRows": limit,
            "hasResultSet": True,
            "source": {
                "schema": schema_name,
                "name": object_name,
                "kind": "preview",
                "orderBy": effective_order,
                "orderDir": direction.lower(),
                "offset": offset,
                "limit": limit,
            },
        },
    }


def execute_query(sql_text: str) -> Dict[str, Any]:
    statements = validate_admin_query(sql_text)
    started = time.perf_counter()
    active_statement: Optional[str] = None
    with db_connection(readonly=False, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
                cur.execute("SET lock_timeout = %s", (str(LOCK_TIMEOUT_MS),))
                cur.execute("SET idle_in_transaction_session_timeout = '30000'")
                results = []
                for index, statement in enumerate(statements, start=1):
                    active_statement = statement
                    statement_started = time.perf_counter()
                    cur.execute(statement)
                    has_result_set = cur.description is not None
                    columns = describe_columns(cur.description)
                    rows, truncated = fetch_result_rows(cur, MAX_QUERY_ROWS) if has_result_set else ([], False)
                    row_count = len(rows) if has_result_set else max(0, cur.rowcount)
                    results.append(
                        {
                            "index": index,
                            "statement": statement,
                            "statementType": statement_head(statement),
                            "commandTag": getattr(cur, "statusmessage", None) or statement_head(statement),
                            "rowCount": row_count,
                            "elapsedMs": elapsed_ms(statement_started),
                            "columns": columns,
                            "rows": rows,
                            "truncated": truncated,
                            "maxRows": MAX_QUERY_ROWS,
                            "hasResultSet": has_result_set,
                        }
                    )
            context = fetch_sql_context(conn)
            conn.commit()
        except Exception as exc:
            if conn.status != pg_extensions.STATUS_READY:
                conn.rollback()
            raise HTTPException(
                status_code=400,
                detail=serialize_pg_error(exc, statement=active_statement),
            ) from exc

    return {
        "success": True,
        "statementCount": len(statements),
        "transactionMode": "admin",
        "elapsedMs": elapsed_ms(started),
        "context": context,
        "results": results,
    }


def build_live_range_payload(
    *,
    mode: str,
    display_mode: str,
    live_layers: Dict[str, bool],
    window: int,
    range_rows: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    zig_rows: List[Dict[str, Any]],
    zone_rows: List[Dict[str, Any]],
    area_rows: List[Dict[str, Any]],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    serialize_ms: float,
) -> Dict[str, Any]:
    first_row = range_rows[0] if range_rows else None
    last_row = range_rows[-1] if range_rows else None
    first_row_id = first_row["id"] if first_row else None
    last_row_id = last_row["id"] if last_row else None
    return {
        "rows": rows,
        "rowCount": len(rows),
        "zigRows": zig_rows,
        "zigCount": len(zig_rows),
        "zoneRows": zone_rows,
        "zoneCount": len(zone_rows),
        "areaRows": area_rows,
        "areaCount": len(area_rows),
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "displayMode": display_mode,
        "layers": live_layers,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=last_row,
        ),
    }


def load_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    display_mode: str,
    show_ticks: Optional[bool] = None,
    show_zigs: Optional[bool] = None,
    show_zones: Optional[bool] = None,
    show_areas: Optional[bool] = None,
    area_states: Optional[List[str]] = None,
    area_sides: Optional[List[str]] = None,
    area_higher_only: bool = False,
) -> Dict[str, Any]:
    live_layers = resolve_live_layers(
        display_mode=display_mode,
        show_ticks=show_ticks,
        show_zigs=show_zigs,
        show_zones=show_zones,
        show_areas=show_areas,
    )
    effective_window = clamp_live_window(window, live_layers)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            review_end_id = bounds["lastId"] if mode == "review" else None
            review_end_timestamp = bounds["lastTimestamp"] if mode == "review" else None
            include_tick_rows = live_layers["ticks"]
            zig_ready = live_layers["zigs"] and zig_storage_ready(cur)
            zone_ready = live_layers["zones"] and zone_storage_ready(cur)
            area_ready = live_layers["areas"] and supres_storage_ready(cur)
            range_rows = fetch_bootstrap_tick_rows(
                cur,
                mode=mode,
                start_id=start_id,
                window=effective_window,
                end_id=review_end_id,
                include_rows=include_tick_rows,
            )
            raw_rows = range_rows
            if not include_tick_rows:
                range_rows = fetch_bootstrap_tick_rows(
                    cur,
                    mode=mode,
                    start_id=start_id,
                    window=effective_window,
                    end_id=review_end_id,
                    include_rows=False,
                )
            range_first_id = range_rows[0]["id"] if range_rows else None
            range_last_id = range_rows[-1]["id"] if range_rows else None
            zig_rows = (
                fetch_zig_snapshot_rows(
                    cur,
                    range_start_id=range_first_id,
                    range_end_id=range_last_id,
                    cursor_id=range_last_id,
                )
                if zig_ready
                else []
            )
            zone_rows = (
                fetch_persisted_zone_rows_all_levels(
                    cur,
                    enabled=True,
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                )
                if zone_ready
                else []
            )
            area_rows = (
                fetch_supres_rows(
                    cur,
                    enabled=area_ready,
                    range_start_id=range_first_id,
                    cursor_id=range_last_id,
                    states=area_states or [AREA_STATE_ACTIVE],
                    sides=area_sides or [AREA_SIDE_TOP, AREA_SIDE_BOTTOM],
                    higher_only=area_higher_only,
                )
                if area_ready and range_first_id is not None and range_last_id is not None
                else []
            )
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_live_range_payload(
        mode=mode,
        display_mode=display_mode,
        live_layers=live_layers,
        window=effective_window,
        range_rows=range_rows,
        rows=[],
        zig_rows=zig_rows,
        zone_rows=zone_rows,
        area_rows=area_rows,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
    )
    payload["rows"] = serialize_tick_rows(raw_rows) if include_tick_rows else []
    payload["rowCount"] = len(payload["rows"])
    payload["zigRows"] = serialize_zig_rows(zig_rows)
    payload["zigCount"] = len(payload["zigRows"])
    payload["zoneRows"] = zone_rows
    payload["zoneCount"] = len(zone_rows)
    payload["areaRows"] = area_rows
    payload["areaCount"] = len(area_rows)
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def load_next_payload(
    *,
    after_id: int,
    limit: int,
    window: int,
    display_mode: str,
    end_id: Optional[int],
    show_ticks: Optional[bool] = None,
    show_zigs: Optional[bool] = None,
    show_zones: Optional[bool] = None,
    show_areas: Optional[bool] = None,
    area_states: Optional[List[str]] = None,
    area_sides: Optional[List[str]] = None,
    area_higher_only: bool = False,
) -> Dict[str, Any]:
    live_layers = resolve_live_layers(
        display_mode=display_mode,
        show_ticks=show_ticks,
        show_zigs=show_zigs,
        show_zones=show_zones,
        show_areas=show_areas,
    )
    effective_window = clamp_live_window(window, live_layers)
    fetch_started = time.perf_counter()
    include_tick_rows = live_layers["ticks"]
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            zig_ready = live_layers["zigs"] and zig_storage_ready(cur)
            zone_ready = live_layers["zones"] and zone_storage_ready(cur)
            area_ready = live_layers["areas"] and supres_storage_ready(cur)
            tick_rows = query_rows_after(cur, after_id, limit, end_id=end_id, include_rows=include_tick_rows)
            last_seen_id = tick_rows[-1]["id"] if tick_rows else after_id
            zig_changes = (
                fetch_zig_changes(cur, after_tick_id=after_id, upto_tick_id=last_seen_id)
                if zig_ready
                else []
            )
            range_rows = query_tick_window_before_cursor(
                cur,
                cursor_id=last_seen_id,
                window=effective_window,
            ) if (zone_ready and last_seen_id) else []
            range_first_id = range_rows[0]["id"] if range_rows else None
            zone_rows = (
                fetch_persisted_zone_rows_all_levels(
                    cur,
                    enabled=True,
                    range_start_id=range_first_id,
                    cursor_id=last_seen_id,
                )
                if zone_ready and range_first_id is not None
                else []
            )
            area_rows = (
                fetch_supres_rows(
                    cur,
                    enabled=area_ready,
                    range_start_id=range_first_id,
                    cursor_id=last_seen_id,
                    states=area_states or [AREA_STATE_ACTIVE],
                    sides=area_sides or [AREA_SIDE_TOP, AREA_SIDE_BOTTOM],
                    higher_only=area_higher_only,
                )
                if area_ready and range_first_id is not None and last_seen_id is not None
                else []
            )
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    rows = serialize_tick_rows(tick_rows) if include_tick_rows else []
    zig_rows = serialize_zig_rows(zig_changes)
    serialize_ms = elapsed_ms(serialize_started)
    return {
        "rows": rows,
        "rowCount": len(rows),
        "zigChanges": zig_rows,
        "zigChangeCount": len(zig_rows),
        "zoneRows": zone_rows,
        "zoneCount": len(zone_rows),
        "areaRows": area_rows,
        "areaCount": len(area_rows),
        "lastId": last_seen_id,
        "endId": end_id,
        "displayMode": display_mode,
        "layers": live_layers,
        "endReached": bool(end_id is not None and last_seen_id >= end_id),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=tick_rows[-1] if tick_rows else None,
        ),
    }


def load_previous_payload(
    *,
    before_id: int,
    limit: int,
    display_mode: str,
    current_last_id: Optional[int],
    show_ticks: Optional[bool] = None,
    show_zigs: Optional[bool] = None,
    show_zones: Optional[bool] = None,
    show_areas: Optional[bool] = None,
    area_states: Optional[List[str]] = None,
    area_sides: Optional[List[str]] = None,
    area_higher_only: bool = False,
) -> Dict[str, Any]:
    live_layers = resolve_live_layers(
        display_mode=display_mode,
        show_ticks=show_ticks,
        show_zigs=show_zigs,
        show_zones=show_zones,
        show_areas=show_areas,
    )
    effective_limit = clamp_live_history_limit(limit, live_layers)
    fetch_started = time.perf_counter()
    include_tick_rows = live_layers["ticks"]
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            zig_ready = live_layers["zigs"] and zig_storage_ready(cur)
            zone_ready = live_layers["zones"] and zone_storage_ready(cur)
            area_ready = live_layers["areas"] and supres_storage_ready(cur)
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
            }
            previous_rows = query_rows_before(cur, before_id, effective_limit, include_rows=include_tick_rows)
            range_rows = previous_rows
            if not include_tick_rows:
                range_rows = query_rows_before(cur, before_id, effective_limit, include_rows=False)
            first_row = range_rows[0] if range_rows else None
            range_end_id = current_last_id or (range_rows[-1]["id"] if range_rows else None)
            zig_rows = (
                fetch_zig_snapshot_rows(
                    cur,
                    range_start_id=first_row["id"] if first_row else None,
                    range_end_id=range_end_id,
                    cursor_id=range_end_id,
                )
                if zig_ready
                else []
            )
            zone_rows = (
                fetch_persisted_zone_rows_all_levels(
                    cur,
                    enabled=True,
                    range_start_id=first_row["id"] if first_row else None,
                    cursor_id=range_end_id,
                )
                if zone_ready
                else []
            )
            area_rows = (
                fetch_supres_rows(
                    cur,
                    enabled=area_ready,
                    range_start_id=first_row["id"] if first_row else None,
                    cursor_id=range_end_id,
                    states=area_states or [AREA_STATE_ACTIVE],
                    sides=area_sides or [AREA_SIDE_TOP, AREA_SIDE_BOTTOM],
                    higher_only=area_higher_only,
                )
                if area_ready and first_row and range_end_id is not None
                else []
            )
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    rows = serialize_tick_rows(previous_rows) if include_tick_rows else []
    zig_payload = serialize_zig_rows(zig_rows)
    serialize_ms = elapsed_ms(serialize_started)
    first_row_id = first_row["id"] if first_row else None
    return {
        "rows": rows,
        "rowCount": len(rows),
        "zigRows": zig_payload,
        "zigCount": len(zig_payload),
        "zoneRows": zone_rows,
        "zoneCount": len(zone_rows),
        "areaRows": area_rows,
        "areaCount": len(area_rows),
        "firstId": first_row_id,
        "lastId": range_end_id,
        "beforeId": before_id,
        "displayMode": display_mode,
        "layers": live_layers,
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=range_rows[-1] if range_rows else None,
        ),
    }


def stream_events(
    after_id: int,
    limit: int,
    display_mode: str,
    *,
    window: int,
    show_ticks: Optional[bool] = None,
    show_zigs: Optional[bool] = None,
    show_zones: Optional[bool] = None,
    show_areas: Optional[bool] = None,
    area_states: Optional[List[str]] = None,
    area_sides: Optional[List[str]] = None,
    area_higher_only: bool = False,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    live_layers = resolve_live_layers(
        display_mode=display_mode,
        show_ticks=show_ticks,
        show_zigs=show_zigs,
        show_zones=show_zones,
        show_areas=show_areas,
    )
    effective_window = clamp_live_window(window, live_layers)
    include_tick_rows = live_layers["ticks"]
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                include_zig_rows = live_layers["zigs"] and zig_storage_ready(cur)
                include_zone_rows = live_layers["zones"] and zone_storage_ready(cur)
                include_area_rows = live_layers["areas"] and supres_storage_ready(cur)
                while True:
                    fetch_started = time.perf_counter()
                    tick_rows = query_rows_after(cur, last_id, limit, include_rows=include_tick_rows)
                    latest_tick_row = tick_rows[-1] if tick_rows else None
                    next_last_id = int(latest_tick_row["id"]) if latest_tick_row else last_id
                    zig_changes = (
                        fetch_zig_changes(cur, after_tick_id=last_id, upto_tick_id=next_last_id)
                        if include_zig_rows
                        else []
                    )
                    range_rows = query_tick_window_before_cursor(
                        cur,
                        cursor_id=next_last_id,
                        window=effective_window,
                    ) if (include_zone_rows and next_last_id) else []
                    range_first_id = range_rows[0]["id"] if range_rows else None
                    zone_rows = (
                        fetch_persisted_zone_rows_all_levels(
                            cur,
                            enabled=True,
                            range_start_id=range_first_id,
                            cursor_id=next_last_id,
                        )
                        if include_zone_rows and range_first_id is not None
                        else []
                    )
                    area_rows = (
                        fetch_supres_rows(
                            cur,
                            enabled=include_area_rows,
                            range_start_id=range_first_id,
                            cursor_id=next_last_id,
                            states=area_states or [AREA_STATE_ACTIVE],
                            sides=area_sides or [AREA_SIDE_TOP, AREA_SIDE_BOTTOM],
                            higher_only=area_higher_only,
                        )
                        if include_area_rows and range_first_id is not None and next_last_id is not None
                        else []
                    )
                    fetch_ms = elapsed_ms(fetch_started)

                    should_emit = (
                        (include_tick_rows and bool(tick_rows))
                        or bool(zig_changes)
                        or (include_zone_rows and next_last_id > last_id)
                        or (include_area_rows and next_last_id > last_id)
                    )
                    if should_emit:
                        serialize_started = time.perf_counter()
                        payload_rows = serialize_tick_rows(tick_rows) if include_tick_rows else []
                        payload_zig = serialize_zig_rows(zig_changes)
                        serialize_ms = elapsed_ms(serialize_started)
                        last_id = next_last_id
                        payload = {
                            "rows": payload_rows,
                            "rowCount": len(payload_rows),
                            "zigChanges": payload_zig,
                            "zigChangeCount": len(payload_zig),
                            "zoneRows": zone_rows,
                            "zoneCount": len(zone_rows),
                            "areaRows": area_rows,
                            "areaCount": len(area_rows),
                            "lastId": last_id,
                            "displayMode": display_mode,
                            "layers": live_layers,
                            "streamMode": "delta",
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=serialize_ms,
                                latest_row=latest_tick_row,
                            ),
                        }
                        yield format_sse(payload)
                        last_heartbeat = time.monotonic()
                        idle_sleep = STREAM_POLL_SECONDS
                        continue

                    if latest_tick_row and not include_tick_rows:
                        last_id = next_last_id

                    now = time.monotonic()
                    if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                        latest_row = query_latest_tick(cur)
                        payload = {
                            "rows": [],
                            "rowCount": 0,
                            "zigChanges": [],
                            "zigChangeCount": 0,
                            "zoneRows": [],
                            "zoneCount": 0,
                            "areaRows": [],
                            "areaCount": 0,
                            "lastId": last_id,
                            "displayMode": display_mode,
                            "layers": live_layers,
                            "streamMode": "heartbeat",
                            "pollSleepMs": round(idle_sleep * 1000.0, 2),
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=0.0,
                                latest_row=latest_row,
                            ),
                        }
                        yield format_sse(payload, event_name="heartbeat")
                        last_heartbeat = now
                    time.sleep(idle_sleep)
                    idle_sleep = STREAM_IDLE_POLL_SECONDS
    except GeneratorExit:
        return


@app.get("/", include_in_schema=False)
def home_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/live", include_in_schema=False)
def live_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "live.html")


@app.get("/zigcandles", include_in_schema=False)
def zig_candles_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "zigcandles.html")


@app.get("/zones", include_in_schema=False)
def zones_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "zones.html")


@app.get("/sql", include_in_schema=False)
def sql_page(_: str = Depends(require_sql_admin)) -> FileResponse:
    return FileResponse(FRONTEND_DIR / "sql.html")


@app.get("/api/health")
def api_health() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT MAX(id) AS last_id, MAX(timestamp) AS last_timestamp
                FROM public.ticks
                WHERE symbol = %s
                """,
                (TICK_SYMBOL,),
            )
            row = cur.fetchone() or {}
    return {
        "ok": True,
        "service": "datavis",
        "symbol": TICK_SYMBOL,
        "lastId": row.get("last_id"),
        "lastTimestamp": row.get("last_timestamp"),
    }


@app.get("/api/live/review-start")
def live_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    requested_ts = parse_review_timestamp(timestamp, timezoneName)
    resolved = resolve_tick_at_timestamp(requested_ts)
    resolved_local = resolved["timestamp"].astimezone(ZoneInfo(timezoneName))
    requested_local = requested_ts.astimezone(ZoneInfo(timezoneName))
    return {
        "symbol": TICK_SYMBOL,
        "timezone": timezoneName,
        "requestedTimestamp": requested_ts.isoformat(),
        "requestedLocal": requested_local.isoformat(),
        "resolvedId": resolved["id"],
        "resolvedTimestamp": resolved["timestamp"].isoformat(),
        "resolvedLocal": resolved_local.isoformat(),
    }


@app.get("/api/zigcandles/review-start")
def zig_candles_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/zones/review-start")
def zones_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/zones/state")
def zones_state(
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
) -> Dict[str, Any]:
    return {
        "symbol": TICK_SYMBOL,
        "level": clamp_zig_level(level),
        "state": fetch_zoneboxstate_snapshot(level),
    }


@app.get("/api/zones/bootstrap")
def zones_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    return load_zone_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/zones/next")
def zones_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    reviewStartId: Optional[int] = Query(None, ge=1),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    return load_zone_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        review_start_id=reviewStartId,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/zones/previous")
def zones_previous(
    currentLastId: int = Query(..., ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_ZIG_CANDLE_HISTORY_LIMIT),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    return load_zone_previous_payload(
        current_last_id=currentLastId,
        limit=limit,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/zones/stream")
def zones_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> StreamingResponse:
    return StreamingResponse(
        stream_zone_events(
            after_id=afterId,
            limit=limit,
            window=window,
            selected_level=level,
            series=series,
            include_provisional=provisional,
            show_areas=showAreas,
            area_states=parse_area_states(areaStates),
            area_sides=parse_area_sides(areaSides),
            area_higher_only=areaHigherOnly,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_WINDOW),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
    showTicks: Optional[bool] = Query(None),
    showZigs: Optional[bool] = Query(None),
    showZones: Optional[bool] = Query(None),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    return load_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        display_mode=display,
        show_ticks=showTicks,
        show_zigs=showZigs,
        show_zones=showZones,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_WINDOW),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
    showTicks: Optional[bool] = Query(None),
    showZigs: Optional[bool] = Query(None),
    showZones: Optional[bool] = Query(None),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    return load_next_payload(
        after_id=afterId,
        limit=limit,
        window=window,
        display_mode=display,
        end_id=endId,
        show_ticks=showTicks,
        show_zigs=showZigs,
        show_zones=showZones,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/live/previous")
def live_previous(
    beforeId: int = Query(..., ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_ZIG_HISTORY_LIMIT),
    currentLastId: Optional[int] = Query(None, ge=1),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
    showTicks: Optional[bool] = Query(None),
    showZigs: Optional[bool] = Query(None),
    showZones: Optional[bool] = Query(None),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    return load_previous_payload(
        before_id=beforeId,
        limit=limit,
        display_mode=display,
        current_last_id=currentLastId,
        show_ticks=showTicks,
        show_zigs=showZigs,
        show_zones=showZones,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/live/stream")
def live_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_WINDOW),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
    showTicks: Optional[bool] = Query(None),
    showZigs: Optional[bool] = Query(None),
    showZones: Optional[bool] = Query(None),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(
            afterId,
            limit,
            display,
            window=window,
            show_ticks=showTicks,
            show_zigs=showZigs,
            show_zones=showZones,
            show_areas=showAreas,
            area_states=parse_area_states(areaStates),
            area_sides=parse_area_sides(areaSides),
            area_higher_only=areaHigherOnly,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/zigcandles/bootstrap")
def zig_candles_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    zones: bool = Query(True),
    zoneMinTicks: int = Query(DEFAULT_ZONE_MIN_DWELL_TICKS, ge=4, le=MAX_ZONE_MIN_DWELL_TICKS),
    zoneMinMs: int = Query(DEFAULT_ZONE_MIN_DWELL_MS, ge=100, le=MAX_ZONE_MIN_DWELL_MS),
    zoneSameSideTolerance: float = Query(DEFAULT_ZONE_SAME_SIDE_TOLERANCE, ge=0.0, le=MAX_ZONE_SAME_SIDE_TOLERANCE),
    zoneOvershoot: float = Query(DEFAULT_ZONE_ALLOWED_OVERSHOOT, ge=0.0, le=MAX_ZONE_ALLOWED_OVERSHOOT),
    zoneBreakTicks: int = Query(DEFAULT_ZONE_BREAKOUT_TICKS, ge=1, le=MAX_ZONE_BREAKOUT_TICKS),
    zoneBreakTolerance: float = Query(DEFAULT_ZONE_BREAKOUT_TOLERANCE, ge=0.0, le=MAX_ZONE_BREAKOUT_TOLERANCE),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    zone_settings = build_zone_settings(
        enabled=zones,
        min_dwell_ticks=zoneMinTicks,
        min_dwell_ms=zoneMinMs,
        same_side_tolerance=zoneSameSideTolerance,
        allowed_overshoot=zoneOvershoot,
        breakout_ticks=zoneBreakTicks,
        breakout_tolerance=zoneBreakTolerance,
        min_height=DEFAULT_ZONE_MIN_HEIGHT,
        max_height=DEFAULT_ZONE_MAX_HEIGHT,
    )
    return load_zig_candle_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        zone_settings=zone_settings,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/zigcandles/next")
def zig_candles_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    reviewStartId: Optional[int] = Query(None, ge=1),
    zones: bool = Query(True),
    zoneMinTicks: int = Query(DEFAULT_ZONE_MIN_DWELL_TICKS, ge=4, le=MAX_ZONE_MIN_DWELL_TICKS),
    zoneMinMs: int = Query(DEFAULT_ZONE_MIN_DWELL_MS, ge=100, le=MAX_ZONE_MIN_DWELL_MS),
    zoneSameSideTolerance: float = Query(DEFAULT_ZONE_SAME_SIDE_TOLERANCE, ge=0.0, le=MAX_ZONE_SAME_SIDE_TOLERANCE),
    zoneOvershoot: float = Query(DEFAULT_ZONE_ALLOWED_OVERSHOOT, ge=0.0, le=MAX_ZONE_ALLOWED_OVERSHOOT),
    zoneBreakTicks: int = Query(DEFAULT_ZONE_BREAKOUT_TICKS, ge=1, le=MAX_ZONE_BREAKOUT_TICKS),
    zoneBreakTolerance: float = Query(DEFAULT_ZONE_BREAKOUT_TOLERANCE, ge=0.0, le=MAX_ZONE_BREAKOUT_TOLERANCE),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    zone_settings = build_zone_settings(
        enabled=zones,
        min_dwell_ticks=zoneMinTicks,
        min_dwell_ms=zoneMinMs,
        same_side_tolerance=zoneSameSideTolerance,
        allowed_overshoot=zoneOvershoot,
        breakout_ticks=zoneBreakTicks,
        breakout_tolerance=zoneBreakTolerance,
        min_height=DEFAULT_ZONE_MIN_HEIGHT,
        max_height=DEFAULT_ZONE_MAX_HEIGHT,
    )
    return load_zig_candle_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        review_start_id=reviewStartId,
        zone_settings=zone_settings,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/zigcandles/previous")
def zig_candles_previous(
    beforeId: int = Query(..., ge=1),
    currentLastId: int = Query(..., ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_ZIG_CANDLE_HISTORY_LIMIT),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    zones: bool = Query(True),
    zoneMinTicks: int = Query(DEFAULT_ZONE_MIN_DWELL_TICKS, ge=4, le=MAX_ZONE_MIN_DWELL_TICKS),
    zoneMinMs: int = Query(DEFAULT_ZONE_MIN_DWELL_MS, ge=100, le=MAX_ZONE_MIN_DWELL_MS),
    zoneSameSideTolerance: float = Query(DEFAULT_ZONE_SAME_SIDE_TOLERANCE, ge=0.0, le=MAX_ZONE_SAME_SIDE_TOLERANCE),
    zoneOvershoot: float = Query(DEFAULT_ZONE_ALLOWED_OVERSHOOT, ge=0.0, le=MAX_ZONE_ALLOWED_OVERSHOOT),
    zoneBreakTicks: int = Query(DEFAULT_ZONE_BREAKOUT_TICKS, ge=1, le=MAX_ZONE_BREAKOUT_TICKS),
    zoneBreakTolerance: float = Query(DEFAULT_ZONE_BREAKOUT_TOLERANCE, ge=0.0, le=MAX_ZONE_BREAKOUT_TOLERANCE),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> Dict[str, Any]:
    zone_settings = build_zone_settings(
        enabled=zones,
        min_dwell_ticks=zoneMinTicks,
        min_dwell_ms=zoneMinMs,
        same_side_tolerance=zoneSameSideTolerance,
        allowed_overshoot=zoneOvershoot,
        breakout_ticks=zoneBreakTicks,
        breakout_tolerance=zoneBreakTolerance,
        min_height=DEFAULT_ZONE_MIN_HEIGHT,
        max_height=DEFAULT_ZONE_MAX_HEIGHT,
    )
    return load_zig_candle_previous_payload(
        current_last_id=currentLastId,
        limit=limit,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        zone_settings=zone_settings,
        show_areas=showAreas,
        area_states=parse_area_states(areaStates),
        area_sides=parse_area_sides(areaSides),
        area_higher_only=areaHigherOnly,
    )


@app.get("/api/zigcandles/stream")
def zig_candles_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
    zones: bool = Query(True),
    zoneMinTicks: int = Query(DEFAULT_ZONE_MIN_DWELL_TICKS, ge=4, le=MAX_ZONE_MIN_DWELL_TICKS),
    zoneMinMs: int = Query(DEFAULT_ZONE_MIN_DWELL_MS, ge=100, le=MAX_ZONE_MIN_DWELL_MS),
    zoneSameSideTolerance: float = Query(DEFAULT_ZONE_SAME_SIDE_TOLERANCE, ge=0.0, le=MAX_ZONE_SAME_SIDE_TOLERANCE),
    zoneOvershoot: float = Query(DEFAULT_ZONE_ALLOWED_OVERSHOOT, ge=0.0, le=MAX_ZONE_ALLOWED_OVERSHOOT),
    zoneBreakTicks: int = Query(DEFAULT_ZONE_BREAKOUT_TICKS, ge=1, le=MAX_ZONE_BREAKOUT_TICKS),
    zoneBreakTolerance: float = Query(DEFAULT_ZONE_BREAKOUT_TOLERANCE, ge=0.0, le=MAX_ZONE_BREAKOUT_TOLERANCE),
    showAreas: bool = Query(False),
    areaStates: Optional[str] = Query(None),
    areaSides: Optional[str] = Query(None),
    areaHigherOnly: bool = Query(False),
) -> StreamingResponse:
    zone_settings = build_zone_settings(
        enabled=zones,
        min_dwell_ticks=zoneMinTicks,
        min_dwell_ms=zoneMinMs,
        same_side_tolerance=zoneSameSideTolerance,
        allowed_overshoot=zoneOvershoot,
        breakout_ticks=zoneBreakTicks,
        breakout_tolerance=zoneBreakTolerance,
        min_height=DEFAULT_ZONE_MIN_HEIGHT,
        max_height=DEFAULT_ZONE_MAX_HEIGHT,
    )
    return StreamingResponse(
        stream_zig_candle_events(
            after_id=afterId,
            limit=limit,
            window=window,
            selected_level=level,
            series=series,
            include_provisional=provisional,
            zone_settings=zone_settings,
            show_areas=showAreas,
            area_states=parse_area_states(areaStates),
            area_sides=parse_area_sides(areaSides),
            area_higher_only=areaHigherOnly,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/sql/schema")
def sql_schema(_: str = Depends(require_sql_admin)) -> Dict[str, Any]:
    return schema_payload()


@app.get("/api/sql/object")
def sql_object(
    schema: str = Query(..., min_length=1),
    name: str = Query(..., min_length=1),
    kind: str = Query(..., pattern="^(table)$"),
    _: str = Depends(require_sql_admin),
) -> Dict[str, Any]:
    return load_object_details(schema, name, kind)


@app.get("/api/sql/table-preview")
def sql_table_preview(
    schema: str = Query(..., min_length=1),
    name: str = Query(..., min_length=1),
    limit: int = Query(DEFAULT_SQL_PREVIEW_LIMIT, ge=1, le=MAX_SQL_PREVIEW_LIMIT),
    offset: int = Query(0, ge=0),
    orderBy: Optional[str] = Query(None, min_length=1),
    orderDir: str = Query("desc", pattern="^(asc|desc)$"),
    _: str = Depends(require_sql_admin),
) -> Dict[str, Any]:
    return preview_relation(schema, name, limit, offset, orderBy, orderDir)


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest, _: str = Depends(require_sql_admin)) -> Dict[str, Any]:
    return execute_query(payload.sql)
