#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras
import sqlparse
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


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
MAX_STREAM_BATCH = 1000
MAX_QUERY_ROWS = 1000
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "5000"))
STREAM_POLL_SECONDS = float(os.getenv("DATAVIS_STREAM_POLL_SECONDS", "1.0"))
STREAM_KEEPALIVE_SECONDS = 15.0

ALLOWED_HEADS = ("SELECT", "WITH", "EXPLAIN")
BLOCKED_KEYWORDS = {
    "ALTER",
    "ANALYZE",
    "ATTACH",
    "CALL",
    "CLUSTER",
    "COMMENT",
    "COMMIT",
    "COPY",
    "CREATE",
    "DEALLOCATE",
    "DELETE",
    "DETACH",
    "DISCARD",
    "DO",
    "DROP",
    "EXECUTE",
    "GRANT",
    "IMPORT",
    "INSERT",
    "LISTEN",
    "LOAD",
    "LOCK",
    "MERGE",
    "NOTIFY",
    "PREPARE",
    "REFRESH",
    "REINDEX",
    "RESET",
    "REVOKE",
    "ROLLBACK",
    "SECURITY",
    "SET",
    "SHOW",
    "TRUNCATE",
    "UNLISTEN",
    "UPDATE",
    "VACUUM",
}


class QueryRequest(BaseModel):
    sql: str


app = FastAPI(title="datavis.au", version="1.0.0")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def ensure_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return DATABASE_URL


@contextmanager
def db_connection(readonly: bool = False):
    conn = psycopg2.connect(ensure_database_url())
    conn.autocommit = False
    if readonly:
        conn.set_session(readonly=True, autocommit=False)
    try:
        yield conn
    finally:
        conn.close()


def clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


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
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": row["bid"],
        "ask": row["ask"],
        "mid": row["mid"],
        "spread": row["spread"],
        "price": row["price"],
    }


def fetch_bootstrap_rows(mode: str, start_id: Optional[int], window: int) -> List[Dict[str, Any]]:
    window = clamp_int(window, 1, MAX_WINDOW)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if mode == "live":
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
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
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM public.ticks
                    WHERE symbol = %s AND id >= %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (TICK_SYMBOL, start_id, window),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_rows_after(after_id: int, limit: int) -> List[Dict[str, Any]]:
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, symbol, timestamp, bid, ask, mid, spread,
                       COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                FROM public.ticks
                WHERE symbol = %s AND id > %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (TICK_SYMBOL, after_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]


def clean_sql(sql_text: str) -> str:
    text = (sql_text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="SQL text is required.")

    statements = [statement.strip() for statement in sqlparse.split(text) if statement.strip()]
    if len(statements) != 1:
        raise HTTPException(status_code=400, detail="Only a single read-only statement is allowed.")

    statement = statements[0].rstrip(";").strip()
    normalized = sqlparse.format(statement, strip_comments=True)
    head = normalized.lstrip().split(None, 1)
    if not head or head[0].upper() not in ALLOWED_HEADS:
        raise HTTPException(status_code=400, detail="Only SELECT, WITH, and EXPLAIN queries are allowed.")

    flattened_keywords = {
        token.normalized.upper()
        for token in sqlparse.parse(statement)[0].flatten()
        if token.ttype in sqlparse.tokens.Keyword
    }
    blocked = sorted(keyword for keyword in flattened_keywords if keyword in BLOCKED_KEYWORDS)
    if blocked:
        raise HTTPException(
            status_code=400,
            detail="Blocked SQL keyword(s): {0}".format(", ".join(blocked)),
        )

    if re.search(r"\bpg_(read|write|ls|stat|file|logdir|monitor|rotate)\b", statement, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Server file/system helper functions are blocked.")

    return statement


def schema_payload() -> List[Dict[str, Any]]:
    sql = """
        SELECT
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type,
            c.udt_name,
            c.ordinal_position,
            COALESCE(s.n_live_tup, 0)::bigint AS row_estimate
        FROM information_schema.tables t
        JOIN information_schema.columns c
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        LEFT JOIN pg_stat_user_tables s
          ON s.schemaname = t.table_schema
         AND s.relname = t.table_name
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
    """
    grouped: Dict[str, Dict[str, Any]] = {}
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                schema_name = row["table_schema"]
                table_name = row["table_name"]
                schema = grouped.setdefault(schema_name, {"schema": schema_name, "tables": []})
                if not schema["tables"] or schema["tables"][-1]["name"] != table_name:
                    schema["tables"].append(
                        {
                            "name": table_name,
                            "schema": schema_name,
                            "rowEstimate": row["row_estimate"],
                            "columns": [],
                        }
                    )
                schema["tables"][-1]["columns"].append(
                    {
                        "name": row["column_name"],
                        "dataType": row["data_type"],
                        "udtName": row["udt_name"],
                    }
                )
    return list(grouped.values())


def execute_query(sql_text: str) -> Dict[str, Any]:
    statement = clean_sql(sql_text)
    started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
            cur.execute("SET LOCAL idle_in_transaction_session_timeout = '5000'")
            cur.execute(statement)

            description = cur.description or []
            columns = [item.name for item in description]
            rows = cur.fetchmany(MAX_QUERY_ROWS + 1)
            truncated = len(rows) > MAX_QUERY_ROWS
            rows = rows[:MAX_QUERY_ROWS]
            conn.rollback()

    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    return {
        "columns": columns,
        "rows": [[serialize_value(value) for value in row] for row in rows],
        "rowCount": len(rows),
        "truncated": truncated,
        "maxRows": MAX_QUERY_ROWS,
        "elapsedMs": elapsed_ms,
    }


def stream_events(after_id: int, limit: int) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    last_heartbeat = time.monotonic()

    try:
        while True:
            rows = fetch_rows_after(last_id, limit)
            if rows:
                payload_rows = [serialize_tick_row(row) for row in rows]
                last_id = payload_rows[-1]["id"]
                yield "data: {0}\n\n".format(
                    json.dumps(
                        {
                            "rows": payload_rows,
                            "lastId": last_id,
                            "rowCount": len(payload_rows),
                        }
                    )
                )
                last_heartbeat = time.monotonic()
            else:
                now = time.monotonic()
                if now - last_heartbeat >= STREAM_KEEPALIVE_SECONDS:
                    yield ": keepalive\n\n"
                    last_heartbeat = now
                time.sleep(STREAM_POLL_SECONDS)
    except GeneratorExit:
        return


@app.get("/", include_in_schema=False)
def home_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/live", include_in_schema=False)
def live_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "live.html")


@app.get("/sql", include_in_schema=False)
def sql_page() -> FileResponse:
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


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
) -> Dict[str, Any]:
    rows = [serialize_tick_row(row) for row in fetch_bootstrap_rows(mode, id, window)]
    return {
        "rows": rows,
        "rowCount": len(rows),
        "firstId": rows[0]["id"] if rows else None,
        "lastId": rows[-1]["id"] if rows else None,
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "priceColumn": "mid",
    }


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
) -> Dict[str, Any]:
    rows = [serialize_tick_row(row) for row in fetch_rows_after(afterId, limit)]
    return {
        "rows": rows,
        "rowCount": len(rows),
        "lastId": rows[-1]["id"] if rows else afterId,
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
def sql_schema() -> Dict[str, Any]:
    schemas = schema_payload()
    return {"schemas": schemas, "schemaCount": len(schemas)}


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest) -> Dict[str, Any]:
    return execute_query(payload.sql)
