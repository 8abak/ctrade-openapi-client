#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import secrets
import time
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
MAX_WINDOW = 10000
DEFAULT_HISTORY_LIMIT = 2000
MAX_HISTORY_LIMIT = 10000
MAX_STREAM_BATCH = 1000
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
SQL_ADMIN_USER = os.getenv("DATAVIS_SQL_ADMIN_USER", "").strip()
SQL_ADMIN_PASSWORD = os.getenv("DATAVIS_SQL_ADMIN_PASSWORD", "")
DEFAULT_REVIEW_TIMEZONE = "Australia/Sydney"
ALLOWED_SQL_HEADS = {"SELECT", "EXPLAIN"}
FORBIDDEN_SQL_RE = re.compile(
    r"\b("
    r"insert|update|delete|drop|alter|create|truncate|copy|grant|revoke|"
    r"vacuum|analyze|refresh|call|do|begin|commit|rollback|savepoint|release|"
    r"listen|notify|unlisten|set|reset|show"
    r")\b",
    re.IGNORECASE,
)


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
            row = dict(cur.fetchone() or {})
    return {
        "firstId": row.get("first_id"),
        "lastId": row.get("last_id"),
        "firstTimestamp": row.get("first_timestamp"),
        "lastTimestamp": row.get("last_timestamp"),
    }


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


def tick_select_sql(where_sql: str, order_sql: str, limit_sql: str) -> str:
    return """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM public.ticks
        WHERE symbol = %s {where_clause}
        ORDER BY {order_clause}
        {limit_clause}
    """.format(where_clause=where_sql, order_clause=order_sql, limit_clause=limit_sql)


def fetch_bootstrap_rows(
    mode: str,
    start_id: Optional[int],
    window: int,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    window = clamp_int(window, 1, MAX_WINDOW)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if mode == "live":
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread
                    FROM (
                        SELECT id, symbol, timestamp, bid, ask, mid, spread
                        FROM public.ticks
                        WHERE symbol = %s
                        ORDER BY id DESC
                        LIMIT %s
                    ) recent
                    ORDER BY id ASC
                    """,
                    (TICK_SYMBOL, window),
                )
            else:
                if start_id is None:
                    raise HTTPException(status_code=400, detail="Review mode requires an id value.")
                if end_id is None:
                    cur.execute(
                        tick_select_sql("AND id >= %s", "id ASC", "LIMIT %s"),
                        (TICK_SYMBOL, start_id, window),
                    )
                else:
                    cur.execute(
                        tick_select_sql("AND id >= %s AND id <= %s", "id ASC", "LIMIT %s"),
                        (TICK_SYMBOL, start_id, end_id, window),
                    )
            return [dict(row) for row in cur.fetchall()]


def query_rows_after(cur: Any, after_id: int, limit: int, *, end_id: Optional[int] = None) -> List[Dict[str, Any]]:
    if end_id is None:
        cur.execute(
            tick_select_sql("AND id > %s", "id ASC", "LIMIT %s"),
            (TICK_SYMBOL, after_id, limit),
        )
    else:
        cur.execute(
            tick_select_sql("AND id > %s AND id <= %s", "id ASC", "LIMIT %s"),
            (TICK_SYMBOL, after_id, end_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_rows_before(cur: Any, before_id: int, limit: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM (
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s AND id < %s
            ORDER BY id DESC
            LIMIT %s
        ) older
        ORDER BY id ASC
        """,
        (TICK_SYMBOL, before_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_rows_after(after_id: int, limit: int, end_id: Optional[int] = None) -> List[Dict[str, Any]]:
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return query_rows_after(cur, after_id, limit, end_id=end_id)


def fetch_rows_before(before_id: int, limit: int) -> List[Dict[str, Any]]:
    limit = clamp_int(limit, 1, MAX_HISTORY_LIMIT)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return query_rows_before(cur, before_id, limit)


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


def validate_ticks_query(sql_text: str) -> str:
    statements = split_sql_script(sql_text)
    if len(statements) != 1:
        raise HTTPException(status_code=400, detail="Only one read-only ticks query is allowed at a time.")

    statement = statements[0]
    head = statement_head(statement)
    if head not in ALLOWED_SQL_HEADS:
        raise HTTPException(status_code=400, detail="Only SELECT and EXPLAIN queries against public.ticks are allowed.")

    lowered = statement.lower()
    if FORBIDDEN_SQL_RE.search(lowered):
        raise HTTPException(status_code=400, detail="Only read-only queries against public.ticks are allowed.")
    if re.search(r"\b(join|union|intersect|except)\b", lowered):
        raise HTTPException(status_code=400, detail="The SQL page is limited to a single-table ticks query.")

    relation_matches = re.findall(r"\bfrom\s+((?:\"[^\"]+\"|\w+)(?:\s*\.\s*(?:\"[^\"]+\"|\w+))?)", statement, re.IGNORECASE)
    if not relation_matches:
        raise HTTPException(status_code=400, detail="Queries must read from public.ticks.")

    for relation in relation_matches:
        normalized = normalize_relation_reference(relation)
        if normalized not in {"ticks", "public.ticks"}:
            raise HTTPException(status_code=400, detail="Queries may only read from public.ticks.")

    return statement


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


def ticks_table_summary(conn: Any) -> Dict[str, Any]:
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
            WHERE n.nspname = 'public'
              AND c.relname = 'ticks'
              AND c.relkind IN ('r', 'p')
            """
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="public.ticks was not found.")
    return dict(row)


def schema_payload() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        summary = ticks_table_summary(conn)
        columns = relation_columns(conn, "public", "ticks")
        indexes = table_indexes(conn, "public", "ticks")

    return {
        "context": context,
        "schemas": [
            {
                "schema": "public",
                "counts": {
                    "tables": 1,
                    "views": 0,
                    "materializedViews": 0,
                    "sequences": 0,
                    "functions": 0,
                },
                "objects": {
                    "tables": [
                        {
                            "name": "ticks",
                            "schema": "public",
                            "kind": "table",
                            "rowEstimate": summary["row_estimate"],
                            "columns": columns,
                            "indexes": indexes,
                        }
                    ],
                    "views": [],
                    "materializedViews": [],
                    "sequences": [],
                    "functions": [],
                },
            }
        ],
    }


def assert_ticks_object(schema_name: str, object_name: str, object_kind: str) -> None:
    if schema_name != "public" or object_name != "ticks" or object_kind != "table":
        raise HTTPException(status_code=404, detail="Only public.ticks is exposed in the SQL page.")


def load_object_details(schema_name: str, object_name: str, object_kind: str) -> Dict[str, Any]:
    assert_ticks_object(schema_name, object_name, object_kind)
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        summary = ticks_table_summary(conn)
        columns = relation_columns(conn, "public", "ticks")
        indexes = table_indexes(conn, "public", "ticks")

    return {
        "context": context,
        "object": {
            "schema": "public",
            "name": "ticks",
            "kind": "table",
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
            "insertSelect": "SELECT id, timestamp, bid, ask, mid, spread\nFROM public.ticks\nORDER BY id DESC\nLIMIT 100;",
            "insertExplain": "EXPLAIN\nSELECT id, timestamp, bid, ask, mid, spread\nFROM public.ticks\nORDER BY id DESC\nLIMIT 100;",
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
    assert_ticks_object(schema_name, object_name, "table")

    started = time.perf_counter()
    limit = clamp_int(limit, 1, MAX_SQL_PREVIEW_LIMIT)
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        columns = relation_columns(conn, "public", "ticks")
        column_names = {column["name"] for column in columns}
        if order_by and order_by not in column_names:
            raise HTTPException(status_code=400, detail="Unknown sort column: {0}".format(order_by))

        direction = "DESC" if order_dir.lower() == "desc" else "ASC"
        effective_order = order_by or "id"
        query = pg_sql.SQL("SELECT * FROM {} ORDER BY {} {} LIMIT %s OFFSET %s").format(
            pg_sql.SQL(".").join([pg_sql.Identifier("public"), pg_sql.Identifier("ticks")]),
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
            "title": "public.ticks",
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
                "schema": "public",
                "name": "ticks",
                "kind": "preview",
                "orderBy": effective_order,
                "orderDir": direction.lower(),
                "offset": offset,
                "limit": limit,
            },
        },
    }


def execute_query(sql_text: str) -> Dict[str, Any]:
    statement = validate_ticks_query(sql_text)
    started = time.perf_counter()
    with db_connection(readonly=True, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
                cur.execute("SET lock_timeout = %s", (str(LOCK_TIMEOUT_MS),))
                cur.execute("SET idle_in_transaction_session_timeout = '5000'")
                statement_started = time.perf_counter()
                cur.execute(statement)
                columns = describe_columns(cur.description)
                rows, truncated = fetch_result_rows(cur, MAX_QUERY_ROWS)
            context = fetch_sql_context(conn)
        except Exception as exc:
            if conn.status != pg_extensions.STATUS_READY:
                conn.rollback()
            raise HTTPException(status_code=400, detail=serialize_pg_error(exc, statement=statement)) from exc

    return {
        "success": True,
        "statementCount": 1,
        "transactionMode": "readonly",
        "elapsedMs": elapsed_ms(started),
        "context": context,
        "results": [
            {
                "index": 1,
                "statement": statement,
                "statementType": statement_head(statement),
                "commandTag": statement_head(statement),
                "rowCount": len(rows),
                "elapsedMs": elapsed_ms(statement_started),
                "columns": columns,
                "rows": rows,
                "truncated": truncated,
                "maxRows": MAX_QUERY_ROWS,
                "hasResultSet": True,
            }
        ],
    }


def stream_events(after_id: int, limit: int) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                while True:
                    fetch_started = time.perf_counter()
                    rows = query_rows_after(cur, last_id, limit)
                    fetch_ms = elapsed_ms(fetch_started)
                    if rows:
                        serialize_started = time.perf_counter()
                        payload_rows = [serialize_tick_row(row) for row in rows]
                        serialize_ms = elapsed_ms(serialize_started)
                        last_id = payload_rows[-1]["id"]
                        payload = {
                            "rows": payload_rows,
                            "lastId": last_id,
                            "rowCount": len(payload_rows),
                            "streamMode": "delta",
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=serialize_ms,
                                latest_row=rows[-1],
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
                            "lastId": last_id,
                            "rowCount": 0,
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


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
) -> Dict[str, Any]:
    fetch_started = time.perf_counter()
    review_bounds = fetch_tick_bounds() if mode == "review" else None
    review_end_id = review_bounds["lastId"] if review_bounds else None
    review_end_timestamp = review_bounds["lastTimestamp"] if review_bounds else None
    raw_rows = fetch_bootstrap_rows(mode, id, window, end_id=review_end_id)
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    rows = [serialize_tick_row(row) for row in raw_rows]
    serialize_ms = elapsed_ms(serialize_started)
    last_row_id = rows[-1]["id"] if rows else None
    first_row_id = rows[0]["id"] if rows else None
    return {
        "rows": rows,
        "rowCount": len(rows),
        "firstId": first_row_id,
        "lastId": last_row_id,
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(review_bounds and first_row_id and review_bounds["firstId"] and first_row_id > review_bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=raw_rows[-1] if raw_rows else None,
        ),
    }


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
) -> Dict[str, Any]:
    fetch_started = time.perf_counter()
    raw_rows = fetch_rows_after(afterId, limit, end_id=endId)
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    rows = [serialize_tick_row(row) for row in raw_rows]
    serialize_ms = elapsed_ms(serialize_started)
    last_seen_id = rows[-1]["id"] if rows else afterId
    return {
        "rows": rows,
        "rowCount": len(rows),
        "lastId": last_seen_id,
        "endId": endId,
        "endReached": bool(endId is not None and last_seen_id >= endId),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=raw_rows[-1] if raw_rows else None,
        ),
    }


@app.get("/api/live/previous")
def live_previous(
    beforeId: int = Query(..., ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_HISTORY_LIMIT),
) -> Dict[str, Any]:
    fetch_started = time.perf_counter()
    bounds = fetch_tick_bounds()
    raw_rows = fetch_rows_before(beforeId, limit)
    fetch_ms = elapsed_ms(fetch_started)
    serialize_started = time.perf_counter()
    rows = [serialize_tick_row(row) for row in raw_rows]
    serialize_ms = elapsed_ms(serialize_started)
    first_row_id = rows[0]["id"] if rows else None
    return {
        "rows": rows,
        "rowCount": len(rows),
        "firstId": first_row_id,
        "lastId": rows[-1]["id"] if rows else None,
        "beforeId": beforeId,
        "hasMoreLeft": bool(first_row_id and bounds["firstId"] and first_row_id > bounds["firstId"]),
        "metrics": serialize_metrics_payload(
            fetch_ms=fetch_ms,
            serialize_ms=serialize_ms,
            latest_row=raw_rows[-1] if raw_rows else None,
        ),
    }


@app.get("/api/live/stream")
def live_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(afterId, limit),
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
