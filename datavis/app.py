#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import secrets
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import psycopg2.extras
import sqlparse
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from datavis.db import db_connect as shared_db_connect
from datavis.structure import StructureEngine, replay_ticks

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
ASSETS_DIR = FRONTEND_DIR / "assets"

load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgresql+psycopg2://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://", 1)

TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
DEFAULT_WINDOW = int(os.getenv("DATAVIS_WINDOW", "2000"))
MAX_TICK_WINDOW = int(os.getenv("DATAVIS_MAX_WINDOW", "10000"))
DEFAULT_STRUCTURE_WINDOW = int(os.getenv("DATAVIS_STRUCTURE_WINDOW", "50000"))
MAX_STRUCTURE_WINDOW = int(os.getenv("DATAVIS_STRUCTURE_MAX_WINDOW", "200000"))
DEFAULT_HISTORY_LIMIT = 2000
MAX_STREAM_BATCH = 1000
MAX_QUERY_ROWS = int(os.getenv("DATAVIS_SQL_MAX_ROWS", "1000"))
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_LOCK_TIMEOUT_MS", "3000"))
SQL_ADMIN_USER = os.getenv("DATAVIS_SQL_ADMIN_USER", "").strip()
SQL_ADMIN_PASSWORD = os.getenv("DATAVIS_SQL_ADMIN_PASSWORD", "")
STREAM_POLL_SECONDS = max(0.02, float(os.getenv("DATAVIS_STREAM_POLL_SECONDS", "0.05")))
STREAM_IDLE_POLL_SECONDS = max(
    STREAM_POLL_SECONDS,
    float(os.getenv("DATAVIS_STREAM_IDLE_POLL_SECONDS", "0.10")),
)
STREAM_HEARTBEAT_SECONDS = max(
    STREAM_IDLE_POLL_SECONDS,
    float(os.getenv("DATAVIS_STREAM_HEARTBEAT_SECONDS", "5.0")),
)
DEFAULT_REVIEW_TIMEZONE = "Australia/Sydney"

app = FastAPI(title="datavis.au", version="3.0.0")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
security = HTTPBasic(auto_error=False)


class QueryRequest(BaseModel):
    sql: str


def ensure_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return DATABASE_URL


@contextmanager
def db_connection(readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
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
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return value.hex()
    return value


def serialize_tick_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = row["timestamp"]
    bid = float(row["bid"]) if row.get("bid") is not None else None
    ask = float(row["ask"]) if row.get("ask") is not None else None
    mid = float(row["mid"]) if row.get("mid") is not None else None
    if mid is None and bid is not None and ask is not None:
        mid = round((float(bid) + float(ask)) / 2.0, 5)
    spread = float(row["spread"]) if row.get("spread") is not None else None
    if spread is None and bid is not None and ask is not None:
        spread = round(float(ask) - float(bid), 5)
    return {
        "id": int(row["id"]),
        "symbol": row.get("symbol", TICK_SYMBOL),
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
    }


def serialize_tick_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_tick_row(row) for row in rows]


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


def require_sql_admin(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> Optional[str]:
    if not SQL_ADMIN_USER or not SQL_ADMIN_PASSWORD:
        return None

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SQL admin authentication is required.",
            headers={"WWW-Authenticate": 'Basic realm="datavis SQL"'},
        )

    valid_user = secrets.compare_digest(credentials.username or "", SQL_ADMIN_USER)
    valid_password = secrets.compare_digest(credentials.password or "", SQL_ADMIN_PASSWORD)
    if not (valid_user and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid SQL admin credentials.",
            headers={"WWW-Authenticate": 'Basic realm="datavis SQL"'},
        )

    return credentials.username


def describe_columns(description: Any) -> List[Dict[str, Any]]:
    if not description:
        return []
    return [{"name": item.name, "typeCode": item.type_code} for item in description]


def fetch_result_rows(cur: Any, limit: int) -> tuple[List[List[Any]], bool]:
    fetched = cur.fetchmany(limit + 1)
    truncated = len(fetched) > limit
    rows = fetched[:limit]
    return [[serialize_value(value) for value in row] for row in rows], truncated


def split_sql_script(sql_text: str) -> List[str]:
    text = (sql_text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="SQL text is required.")
    return [statement.strip() for statement in sqlparse.split(text) if statement.strip()]


def statement_head(statement: str) -> str:
    parsed = sqlparse.parse(statement)
    if not parsed:
        return "SQL"
    for token in parsed[0].tokens:
        if token.is_whitespace or token.ttype in sqlparse.tokens.Comment:
            continue
        normalized = token.normalized.upper().strip()
        if normalized:
            return normalized.split(None, 1)[0]
    return "SQL"


def line_column_from_position(sql_text: str, position: Optional[Any]) -> tuple[Optional[int], Optional[int]]:
    if not position or not str(position).isdigit():
        return None, None
    absolute_position = max(1, int(position))
    prefix = sql_text[: absolute_position - 1]
    line = prefix.count("\n") + 1
    column = absolute_position - prefix.rfind("\n")
    return line, column


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


def list_public_tables() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    CASE c.relkind WHEN 'p' THEN 'partitioned table' ELSE 'table' END AS kind,
                    COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0)::bigint AS row_estimate,
                    EXISTS (
                        SELECT 1
                        FROM pg_attribute a
                        WHERE a.attrelid = c.oid
                          AND a.attname = 'id'
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                    ) AS has_id
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s
                  ON s.relid = c.oid
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p')
                ORDER BY c.relname ASC
                """
            )
            tables = [
                {
                    "schema": row["schema_name"],
                    "name": row["table_name"],
                    "kind": row["kind"],
                    "rowEstimate": int(row["row_estimate"] or 0),
                    "hasId": bool(row["has_id"]),
                }
                for row in cur.fetchall()
            ]
    return {"context": context, "tables": tables}


def execute_query(sql_text: str) -> Dict[str, Any]:
    statements = split_sql_script(sql_text)
    started = time.perf_counter()
    active_statement: Optional[str] = None

    with db_connection(readonly=False, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (STATEMENT_TIMEOUT_MS,))
                cur.execute("SET LOCAL lock_timeout = %s", (LOCK_TIMEOUT_MS,))
                results = []
                for index, statement in enumerate(statements, start=1):
                    active_statement = statement
                    statement_started = time.perf_counter()
                    cur.execute(statement)
                    has_result_set = cur.description is not None
                    rows, truncated = fetch_result_rows(cur, MAX_QUERY_ROWS) if has_result_set else ([], False)
                    results.append(
                        {
                            "index": index,
                            "statement": statement,
                            "statementType": statement_head(statement),
                            "commandTag": getattr(cur, "statusmessage", None) or statement_head(statement),
                            "rowCount": len(rows) if has_result_set else max(0, cur.rowcount),
                            "elapsedMs": elapsed_ms(statement_started),
                            "columns": describe_columns(cur.description),
                            "rows": rows,
                            "truncated": truncated,
                            "maxRows": MAX_QUERY_ROWS,
                            "hasResultSet": has_result_set,
                        }
                    )
            context = fetch_sql_context(conn)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise HTTPException(
                status_code=400,
                detail=serialize_pg_error(exc, statement=active_statement),
            ) from exc

    return {
        "success": True,
        "statementCount": len(statements),
        "elapsedMs": elapsed_ms(started),
        "context": context,
        "results": results,
    }


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


def tick_columns() -> str:
    return "id, symbol, timestamp, bid, ask, mid, spread"


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


def query_bootstrap_rows(
    cur: Any,
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    end_id: Optional[int],
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
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
        return [dict(row) for row in cur.fetchall()]

    if start_id is None:
        raise HTTPException(status_code=400, detail="Review mode requires an id value.")
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id >= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, start_id, window),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id >= %s AND id <= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, start_id, end_id, window),
        )
    return [dict(row) for row in cur.fetchall()]


def query_rows_after(
    cur: Any,
    after_id: int,
    limit: int,
    *,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, after_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id > %s AND id <= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, after_id, end_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_rows_before(cur: Any, before_id: int, limit: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
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


def query_window_ending_at(cur: Any, cursor_id: int, window: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id <= %s
            ORDER BY id DESC
            LIMIT %s
        ) recent
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, cursor_id, window),
    )
    return [dict(row) for row in cur.fetchall()]


def query_rows_between(cur: Any, start_id: int, end_id: int, limit: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.ticks
        WHERE symbol = %s AND id >= %s AND id <= %s
        ORDER BY id ASC
        LIMIT %s
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, start_id, end_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


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

    return {"id": int(resolved["id"]), "timestamp": resolved["timestamp"]}


def empty_structure_payload() -> Dict[str, Any]:
    return {
        "structureBars": [],
        "rangeBoxes": [],
        "structureEvents": [],
    }


def structure_snapshot(rows: List[Dict[str, Any]], *, enabled: bool) -> Dict[str, Any]:
    if not enabled or not rows:
        return empty_structure_payload()
    try:
        return replay_ticks(TICK_SYMBOL, rows)
    except Exception:
        return empty_structure_payload()


def apply_structure_flags(payload: Dict[str, Any], *, show_events: bool, show_structure: bool, show_ranges: bool) -> Dict[str, Any]:
    if not show_events:
        payload["structureEvents"] = []
    if not show_structure:
        payload["structureBars"] = []
    if not show_ranges:
        payload["rangeBoxes"] = []
    return payload


def build_range_payload(
    *,
    mode: str,
    window: int,
    rows: List[Dict[str, Any]],
    replay_rows: List[Dict[str, Any]],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    first_row = replay_rows[0] if replay_rows else None
    last_row = replay_rows[-1] if replay_rows else None
    first_row_id = first_row["id"] if first_row else None
    last_row_id = last_row["id"] if last_row else None
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload = {
        "rows": serialize_tick_rows(rows) if show_ticks else [],
        "rowCount": len(rows) if show_ticks else 0,
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        **snapshot,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=last_row,
    )
    return payload


def build_structure_view_payload(
    *,
    mode: str,
    window: int,
    replay_rows: List[Dict[str, Any]],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    first_row = replay_rows[0] if replay_rows else None
    last_row = replay_rows[-1] if replay_rows else None
    first_row_id = first_row["id"] if first_row else None
    last_row_id = last_row["id"] if last_row else None
    duration_ms = 0
    if first_row and last_row:
        first_timestamp = first_row.get("timestamp")
        last_timestamp = last_row.get("timestamp")
        if first_timestamp is not None and last_timestamp is not None:
            duration_ms = max(0, int((last_timestamp - first_timestamp).total_seconds() * 1000))
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload = {
        "sourceTickCount": len(replay_rows),
        "tickSpan": max(0, (int(last_row_id) - int(first_row_id) + 1)) if first_row_id and last_row_id else 0,
        "durationMs": duration_ms,
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        **snapshot,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=last_row,
    )
    return payload


def load_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_TICK_WINDOW)
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
            rows = query_bootstrap_rows(
                cur,
                mode=mode,
                start_id=start_id,
                window=effective_window,
                end_id=review_end_id,
            )
    return build_range_payload(
        mode=mode,
        window=effective_window,
        rows=rows,
        replay_rows=rows,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_ticks=show_ticks,
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )


def load_structure_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_STRUCTURE_WINDOW)
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
            replay_rows = query_bootstrap_rows(
                cur,
                mode=mode,
                start_id=start_id,
                window=effective_window,
                end_id=review_end_id,
            )
    return build_structure_view_payload(
        mode=mode,
        window=effective_window,
        replay_rows=replay_rows,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )


def load_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, MAX_TICK_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            tick_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id)
            last_seen_id = int(tick_rows[-1]["id"]) if tick_rows else after_id
            replay_rows = (
                query_window_ending_at(cur, last_seen_id, effective_window)
                if (show_events or show_structure or show_ranges) and last_seen_id
                else []
            )
    serialize_started = time.perf_counter()
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    return {
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "lastId": last_seen_id,
        "endId": end_id,
        "endReached": bool(end_id is not None and last_seen_id >= end_id),
        **snapshot,
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=elapsed_ms(serialize_started),
            latest_row=tick_rows[-1] if tick_rows else None,
        ),
    }


def load_structure_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    window: int,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, MAX_STRUCTURE_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            tick_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id)
            last_seen_id = int(tick_rows[-1]["id"]) if tick_rows else after_id
            replay_rows = query_window_ending_at(cur, last_seen_id, effective_window) if last_seen_id else []
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
            }
    payload = build_structure_view_payload(
        mode="review" if end_id is not None else "live",
        window=effective_window,
        replay_rows=replay_rows,
        review_end_id=end_id,
        review_end_timestamp=None,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload["endId"] = end_id
    payload["endReached"] = bool(end_id is not None and last_seen_id >= end_id)
    return payload


def load_previous_payload(
    *,
    before_id: int,
    current_last_id: Optional[int],
    limit: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_TICK_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {"firstId": bounds_row.get("first_id"), "lastId": bounds_row.get("last_id")}
            previous_rows = query_rows_before(cur, before_id, effective_limit)
            first_row = previous_rows[0] if previous_rows else None
            range_end_id = current_last_id or (previous_rows[-1]["id"] if previous_rows else None)
            if (show_events or show_structure or show_ranges) and first_row and range_end_id:
                replay_rows = query_rows_between(cur, int(first_row["id"]), int(range_end_id), MAX_TICK_WINDOW)
            else:
                replay_rows = []
    serialize_started = time.perf_counter()
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    first_row_id = first_row["id"] if first_row else None
    return {
        "rows": serialize_tick_rows(previous_rows) if show_ticks else [],
        "rowCount": len(previous_rows) if show_ticks else 0,
        "firstId": first_row_id,
        "lastId": range_end_id,
        "beforeId": before_id,
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        **snapshot,
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=elapsed_ms(serialize_started),
            latest_row=(replay_rows[-1] if replay_rows else (previous_rows[-1] if previous_rows else None)),
        ),
    }


def load_structure_previous_payload(
    *,
    before_id: int,
    current_last_id: Optional[int],
    window: int,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_STRUCTURE_WINDOW)
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
            previous_rows = query_rows_before(cur, before_id, effective_window)
            first_row = previous_rows[0] if previous_rows else None
            range_end_id = current_last_id or (previous_rows[-1]["id"] if previous_rows else None)
            if first_row and range_end_id:
                replay_rows = query_rows_between(cur, int(first_row["id"]), int(range_end_id), effective_window)
            else:
                replay_rows = []
    payload = build_structure_view_payload(
        mode="review",
        window=effective_window,
        replay_rows=replay_rows,
        review_end_id=None,
        review_end_timestamp=None,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload["beforeId"] = before_id
    return payload


def stream_events(
    *,
    after_id: int,
    limit: int,
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
    max_window: int = MAX_TICK_WINDOW,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, max_window)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS
    structure_enabled = show_events or show_structure or show_ranges
    engine = StructureEngine(symbol=TICK_SYMBOL) if structure_enabled else None

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if engine is not None and last_id:
                    for row in query_window_ending_at(cur, last_id, effective_window):
                        try:
                            engine.process_tick(row)
                        except Exception:
                            engine = None
                            break

                while True:
                    fetch_started = time.perf_counter()
                    tick_rows = query_rows_after(cur, last_id, effective_limit)
                    fetch_ms = elapsed_ms(fetch_started)
                    if tick_rows:
                        serialize_started = time.perf_counter()
                        latest_tick_row = tick_rows[-1]
                        payload_rows = serialize_tick_rows(tick_rows) if show_ticks else []
                        updates = {"bars": [], "rangeBoxes": [], "events": []}
                        if engine is not None:
                            try:
                                for row in tick_rows:
                                    delta = engine.process_tick(row)
                                    updates["bars"].extend(delta["bars"])
                                    updates["rangeBoxes"].extend(delta["rangeBoxes"])
                                    updates["events"].extend(delta["events"])
                            except Exception:
                                updates = {"bars": [], "rangeBoxes": [], "events": []}
                                engine = None

                        last_id = int(latest_tick_row["id"])
                        payload = {
                            "rows": payload_rows,
                            "rowCount": len(payload_rows),
                            "structureBarUpdates": updates["bars"] if show_structure else [],
                            "rangeBoxUpdates": updates["rangeBoxes"] if show_ranges else [],
                            "structureEvents": updates["events"] if show_events else [],
                            "lastId": last_id,
                            "streamMode": "delta",
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=elapsed_ms(serialize_started),
                                latest_row=latest_tick_row,
                            ),
                        }
                        yield format_sse(payload)
                        last_heartbeat = time.monotonic()
                        idle_sleep = STREAM_POLL_SECONDS
                        continue

                    now = time.monotonic()
                    if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                        latest_row = query_latest_tick(cur)
                        payload = {
                            "rows": [],
                            "rowCount": 0,
                            "structureBarUpdates": [],
                            "rangeBoxUpdates": [],
                            "structureEvents": [],
                            "lastId": last_id,
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


@app.get("/structure", include_in_schema=False)
def structure_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "structure.html")


@app.get("/sql", include_in_schema=False)
def sql_page(_: Optional[str] = Depends(require_sql_admin)) -> FileResponse:
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
            row = dict(cur.fetchone() or {})
    return {
        "ok": True,
        "symbol": TICK_SYMBOL,
        "lastId": row.get("last_id"),
        "lastTimestamp": serialize_value(row.get("last_timestamp")),
        "lastTimestampMs": dt_to_ms(row.get("last_timestamp")),
        "serverTimeMs": now_ms(),
    }


@app.get("/api/sql/schema")
def sql_schema(_: Optional[str] = Depends(require_sql_admin)) -> Dict[str, Any]:
    return list_public_tables()


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest, _: Optional[str] = Depends(require_sql_admin)) -> Dict[str, Any]:
    return execute_query(payload.sql)


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


@app.get("/api/structure/review-start")
def structure_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/live/previous")
def live_previous(
    beforeId: int = Query(..., ge=1),
    currentLastId: Optional[int] = Query(None, ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_previous_payload(
        before_id=beforeId,
        current_last_id=currentLastId,
        limit=limit,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/live/stream")
def live_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(
            after_id=afterId,
            limit=limit,
            window=window,
            show_ticks=showTicks,
            show_events=showEvents,
            show_structure=showStructure,
            show_ranges=showRanges,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/structure/bootstrap")
def structure_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_structure_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/structure/next")
def structure_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_structure_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/structure/previous")
def structure_previous(
    beforeId: int = Query(..., ge=1),
    currentLastId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_structure_previous_payload(
        before_id=beforeId,
        current_last_id=currentLastId,
        window=window,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/structure/stream")
def structure_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(
            after_id=afterId,
            limit=limit,
            window=window,
            show_ticks=False,
            show_events=showEvents,
            show_structure=showStructure,
            show_ranges=showRanges,
            max_window=MAX_STRUCTURE_WINDOW,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
