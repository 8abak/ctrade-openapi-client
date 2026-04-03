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


def clamp_window(value: int, display_mode: str) -> int:
    maximum = MAX_ZIG_WINDOW if display_mode == "zig" else MAX_TICK_WINDOW
    return clamp_int(value, 1, maximum)


def clamp_history_limit(value: int, display_mode: str) -> int:
    maximum = MAX_ZIG_HISTORY_LIMIT if display_mode == "zig" else MAX_TICK_HISTORY_LIMIT
    return clamp_int(value, 1, maximum)


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
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zig_candle_range_payload(
        mode=mode,
        window=effective_window,
        selected_level=effective_level,
        series=series,
        range_rows=range_rows,
        candle_rows=candle_rows,
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
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zig_candle_range_payload(
        mode="review" if review_start_id is not None else "live",
        window=effective_window,
        selected_level=effective_level,
        series=series,
        range_rows=range_rows,
        candle_rows=candle_rows,
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
) -> Dict[str, Any]:
    effective_window = clamp_zig_candle_window(window)
    effective_limit = clamp_zig_candle_history_limit(limit)
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
            range_rows = query_tick_window_before_cursor(
                cur,
                cursor_id=current_last_id,
                window=effective_window,
                offset=effective_limit,
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
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_zig_candle_range_payload(
        mode="live",
        window=effective_window,
        selected_level=effective_level,
        series=series,
        range_rows=range_rows,
        candle_rows=candle_rows,
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
                        fetch_ms = elapsed_ms(fetch_started)
                        serialize_started = time.perf_counter()
                        payload = build_zig_candle_range_payload(
                            mode="live",
                            window=effective_window,
                            selected_level=effective_level,
                            series=series,
                            range_rows=range_rows,
                            candle_rows=candle_rows,
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
    window: int,
    range_rows: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    zig_rows: List[Dict[str, Any]],
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
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "displayMode": display_mode,
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
) -> Dict[str, Any]:
    effective_window = clamp_window(window, display_mode)
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
            include_tick_rows = includes_ticks(display_mode)
            zig_ready = includes_zig(display_mode) and zig_storage_ready(cur)
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
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    payload = build_live_range_payload(
        mode=mode,
        display_mode=display_mode,
        window=effective_window,
        range_rows=range_rows,
        rows=[],
        zig_rows=zig_rows,
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
    payload["metrics"]["serializeLatencyMs"] = elapsed_ms(serialize_started)
    return payload


def load_next_payload(
    *,
    after_id: int,
    limit: int,
    display_mode: str,
    end_id: Optional[int],
) -> Dict[str, Any]:
    fetch_started = time.perf_counter()
    include_tick_rows = includes_ticks(display_mode)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            zig_ready = includes_zig(display_mode) and zig_storage_ready(cur)
            tick_rows = query_rows_after(cur, after_id, limit, end_id=end_id, include_rows=include_tick_rows)
            last_seen_id = tick_rows[-1]["id"] if tick_rows else after_id
            zig_changes = (
                fetch_zig_changes(cur, after_tick_id=after_id, upto_tick_id=last_seen_id)
                if zig_ready
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
        "lastId": last_seen_id,
        "endId": end_id,
        "displayMode": display_mode,
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
) -> Dict[str, Any]:
    effective_limit = clamp_history_limit(limit, display_mode)
    fetch_started = time.perf_counter()
    include_tick_rows = includes_ticks(display_mode)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            zig_ready = includes_zig(display_mode) and zig_storage_ready(cur)
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
        "firstId": first_row_id,
        "lastId": range_end_id,
        "beforeId": before_id,
        "displayMode": display_mode,
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=range_rows[-1] if range_rows else None,
        ),
    }


def stream_events(after_id: int, limit: int, display_mode: str) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    include_tick_rows = includes_ticks(display_mode)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                include_zig_rows = includes_zig(display_mode) and zig_storage_ready(cur)
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
                    fetch_ms = elapsed_ms(fetch_started)

                    should_emit = (include_tick_rows and bool(tick_rows)) or bool(zig_changes)
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
                            "lastId": last_id,
                            "displayMode": display_mode,
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
                            "lastId": last_id,
                            "displayMode": display_mode,
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


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_WINDOW),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
) -> Dict[str, Any]:
    return load_bootstrap_payload(mode=mode, start_id=id, window=window, display_mode=display)


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
) -> Dict[str, Any]:
    return load_next_payload(after_id=afterId, limit=limit, display_mode=display, end_id=endId)


@app.get("/api/live/previous")
def live_previous(
    beforeId: int = Query(..., ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_ZIG_HISTORY_LIMIT),
    currentLastId: Optional[int] = Query(None, ge=1),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
) -> Dict[str, Any]:
    return load_previous_payload(before_id=beforeId, limit=limit, display_mode=display, current_last_id=currentLastId)


@app.get("/api/live/stream")
def live_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    display: str = Query(DEFAULT_DISPLAY_MODE, pattern=DISPLAY_MODE_RE),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(afterId, limit, display),
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
) -> Dict[str, Any]:
    return load_zig_candle_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
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
) -> Dict[str, Any]:
    return load_zig_candle_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
        review_start_id=reviewStartId,
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
) -> Dict[str, Any]:
    return load_zig_candle_previous_payload(
        current_last_id=currentLastId,
        limit=limit,
        window=window,
        selected_level=level,
        series=series,
        include_provisional=provisional,
    )


@app.get("/api/zigcandles/stream")
def zig_candles_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_ZIG_CANDLE_WINDOW),
    level: int = Query(0, ge=0, le=MAX_ZIG_LEVEL),
    series: str = Query("mid", pattern=PRICE_SERIES_RE),
    provisional: bool = Query(True),
) -> StreamingResponse:
    return StreamingResponse(
        stream_zig_candle_events(
            after_id=afterId,
            limit=limit,
            window=window,
            selected_level=level,
            series=series,
            include_provisional=provisional,
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
