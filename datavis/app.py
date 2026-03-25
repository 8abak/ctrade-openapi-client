#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import secrets
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

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

from datavis.regression import MIN_ANALYSIS_WINDOW, build_regression_payload


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
DEFAULT_SQL_PREVIEW_LIMIT = 100
MAX_SQL_PREVIEW_LIMIT = 500
DEFAULT_REGRESSION_FAST_WINDOW = int(os.getenv("DATAVIS_REGRESSION_FAST_WINDOW", "240"))
DEFAULT_REGRESSION_SLOW_WINDOW = int(os.getenv("DATAVIS_REGRESSION_SLOW_WINDOW", "1200"))
DEFAULT_REGRESSION_STEP = int(os.getenv("DATAVIS_REGRESSION_STEP", "120"))
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_LOCK_TIMEOUT_MS", "3000"))
STREAM_POLL_SECONDS = float(os.getenv("DATAVIS_STREAM_POLL_SECONDS", "1.0"))
STREAM_KEEPALIVE_SECONDS = 15.0
SQL_ADMIN_USER = os.getenv("DATAVIS_SQL_ADMIN_USER", "").strip()
SQL_ADMIN_PASSWORD = os.getenv("DATAVIS_SQL_ADMIN_PASSWORD", "")
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")
SYSTEM_SCHEMA_PREFIXES = ("pg_toast", "pg_temp_")
TRANSACTION_CONTROL_HEADS = {
    "BEGIN",
    "START",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
}


class QueryRequest(BaseModel):
    sql: str


security = HTTPBasic(auto_error=False)


app = FastAPI(title="datavis.au", version="1.0.0")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def ensure_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return DATABASE_URL


@contextmanager
def db_connection(readonly: bool = False, autocommit: bool = False):
    conn = psycopg2.connect(ensure_database_url())
    conn.autocommit = autocommit
    if readonly:
        conn.set_session(readonly=True, autocommit=autocommit)
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


def fetch_window_ending_at(end_id: int, window: int) -> List[Dict[str, Any]]:
    window = clamp_int(window, 1, MAX_WINDOW)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, symbol, timestamp, bid, ask, mid, spread,
                       COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                FROM (
                    SELECT id, symbol, timestamp, bid, ask, mid, spread
                    FROM public.ticks
                    WHERE symbol = %s AND id <= %s
                    ORDER BY id DESC
                    LIMIT %s
                ) recent
                ORDER BY id ASC
                """,
                (TICK_SYMBOL, end_id, window),
            )
            return [dict(row) for row in cur.fetchall()]


def build_regression_response(
    *,
    mode: str,
    series: str,
    window: int,
    fast_window_ticks: int,
    slow_window_ticks: int,
    rows: List[Dict[str, Any]],
    requested_start_id: Optional[int] = None,
    new_rows: Optional[List[Dict[str, Any]]] = None,
    persist: bool = True,
    advanced_from_id: Optional[int] = None,
) -> Dict[str, Any]:
    payload_rows = [serialize_tick_row(row) for row in rows]
    analysis = build_regression_payload(
        payload_rows,
        series=series,
        mode=mode,
        visible_window=window,
        fast_window_ticks=fast_window_ticks,
        slow_window_ticks=slow_window_ticks,
    )

    persistence = {"requested": persist, "stored": False, "snapshotId": None, "error": None}
    if persist and payload_rows:
        persistence = persist_regression_snapshot(
            mode=mode,
            series=series,
            payload_rows=payload_rows,
            analysis=analysis,
            advanced_from_id=advanced_from_id,
            new_row_count=len(new_rows or []),
        )

    return {
        "rows": payload_rows,
        "newRows": [serialize_tick_row(row) for row in (new_rows or [])],
        "rowCount": len(payload_rows),
        "newRowCount": len(new_rows or []),
        "advanced": bool(new_rows),
        "firstId": payload_rows[0]["id"] if payload_rows else None,
        "lastId": payload_rows[-1]["id"] if payload_rows else requested_start_id,
        "requestedId": requested_start_id,
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "series": series,
        "fastWindowTicks": analysis["fastWindowTicks"],
        "slowWindowTicks": analysis["slowWindowTicks"],
        "persistence": persistence,
        **analysis,
    }


def persist_regression_snapshot(
    *,
    mode: str,
    series: str,
    payload_rows: List[Dict[str, Any]],
    analysis: Dict[str, Any],
    advanced_from_id: Optional[int],
    new_row_count: int,
) -> Dict[str, Any]:
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                window = analysis["window"]
                fast = analysis["regressions"]["fast"]
                slow = analysis["regressions"]["slow"]
                relationship = analysis["relationship"]
                break_pressure = analysis["breakPressure"]

                cur.execute(
                    """
                    INSERT INTO public.regression_snapshot (
                        symbol,
                        mode,
                        series,
                        source_start_tick_id,
                        source_end_tick_id,
                        source_start_ts,
                        source_end_ts,
                        visible_window_ticks,
                        row_count,
                        fast_window_ticks,
                        slow_window_ticks,
                        advanced_from_tick_id,
                        new_row_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, mode, series, source_end_tick_id, visible_window_ticks, fast_window_ticks, slow_window_ticks)
                    DO UPDATE SET
                        source_start_tick_id = EXCLUDED.source_start_tick_id,
                        source_start_ts = EXCLUDED.source_start_ts,
                        source_end_ts = EXCLUDED.source_end_ts,
                        row_count = EXCLUDED.row_count,
                        advanced_from_tick_id = EXCLUDED.advanced_from_tick_id,
                        new_row_count = EXCLUDED.new_row_count,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        TICK_SYMBOL,
                        mode,
                        series,
                        window["firstId"],
                        window["lastId"],
                        window["firstTimestamp"],
                        window["lastTimestamp"],
                        analysis["visibleWindowTicks"],
                        window["rowCount"],
                        analysis["fastWindowTicks"],
                        analysis["slowWindowTicks"],
                        advanced_from_id,
                        new_row_count,
                    ),
                )
                snapshot_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO public.regression_metric (
                        snapshot_id,
                        fast_slope,
                        fast_intercept,
                        fast_angle_deg,
                        fast_r2,
                        fast_mae,
                        fast_residual_std,
                        fast_sse,
                        fast_start_tick_id,
                        fast_end_tick_id,
                        fast_price_change,
                        fast_duration_ms,
                        fast_tick_count,
                        fast_efficiency,
                        slow_slope,
                        slow_intercept,
                        slow_angle_deg,
                        slow_r2,
                        slow_mae,
                        slow_residual_std,
                        slow_sse,
                        slow_start_tick_id,
                        slow_end_tick_id,
                        slow_price_change,
                        slow_duration_ms,
                        slow_tick_count,
                        slow_efficiency,
                        slope_difference,
                        slope_ratio,
                        angle_difference_deg,
                        current_fast_slow_distance,
                        alignment_state,
                        directional_agreement,
                        fast_acceleration,
                        fast_accelerating,
                        fast_dominance_ratio
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (snapshot_id) DO UPDATE SET
                        fast_slope = EXCLUDED.fast_slope,
                        fast_intercept = EXCLUDED.fast_intercept,
                        fast_angle_deg = EXCLUDED.fast_angle_deg,
                        fast_r2 = EXCLUDED.fast_r2,
                        fast_mae = EXCLUDED.fast_mae,
                        fast_residual_std = EXCLUDED.fast_residual_std,
                        fast_sse = EXCLUDED.fast_sse,
                        fast_start_tick_id = EXCLUDED.fast_start_tick_id,
                        fast_end_tick_id = EXCLUDED.fast_end_tick_id,
                        fast_price_change = EXCLUDED.fast_price_change,
                        fast_duration_ms = EXCLUDED.fast_duration_ms,
                        fast_tick_count = EXCLUDED.fast_tick_count,
                        fast_efficiency = EXCLUDED.fast_efficiency,
                        slow_slope = EXCLUDED.slow_slope,
                        slow_intercept = EXCLUDED.slow_intercept,
                        slow_angle_deg = EXCLUDED.slow_angle_deg,
                        slow_r2 = EXCLUDED.slow_r2,
                        slow_mae = EXCLUDED.slow_mae,
                        slow_residual_std = EXCLUDED.slow_residual_std,
                        slow_sse = EXCLUDED.slow_sse,
                        slow_start_tick_id = EXCLUDED.slow_start_tick_id,
                        slow_end_tick_id = EXCLUDED.slow_end_tick_id,
                        slow_price_change = EXCLUDED.slow_price_change,
                        slow_duration_ms = EXCLUDED.slow_duration_ms,
                        slow_tick_count = EXCLUDED.slow_tick_count,
                        slow_efficiency = EXCLUDED.slow_efficiency,
                        slope_difference = EXCLUDED.slope_difference,
                        slope_ratio = EXCLUDED.slope_ratio,
                        angle_difference_deg = EXCLUDED.angle_difference_deg,
                        current_fast_slow_distance = EXCLUDED.current_fast_slow_distance,
                        alignment_state = EXCLUDED.alignment_state,
                        directional_agreement = EXCLUDED.directional_agreement,
                        fast_acceleration = EXCLUDED.fast_acceleration,
                        fast_accelerating = EXCLUDED.fast_accelerating,
                        fast_dominance_ratio = EXCLUDED.fast_dominance_ratio,
                        updated_at = NOW()
                    """,
                    (
                        snapshot_id,
                        fast["slope"],
                        fast["intercept"],
                        fast["angleDeg"],
                        fast["r2"],
                        fast["mae"],
                        fast["residualStd"],
                        fast["sse"],
                        fast["windowStartTickId"],
                        fast["windowEndTickId"],
                        fast["priceChange"],
                        fast["durationMs"],
                        fast["tickCount"],
                        fast["efficiency"],
                        slow["slope"],
                        slow["intercept"],
                        slow["angleDeg"],
                        slow["r2"],
                        slow["mae"],
                        slow["residualStd"],
                        slow["sse"],
                        slow["windowStartTickId"],
                        slow["windowEndTickId"],
                        slow["priceChange"],
                        slow["durationMs"],
                        slow["tickCount"],
                        slow["efficiency"],
                        relationship["slopeDifference"],
                        relationship["slopeRatio"],
                        relationship["angleDifferenceDeg"],
                        relationship["currentFastSlowDistance"],
                        relationship["alignmentState"],
                        relationship["directionalAgreement"],
                        relationship["fastAcceleration"],
                        relationship["fastAccelerating"],
                        relationship["fastDominanceRatio"],
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO public.regression_break_pressure (
                        snapshot_id,
                        recent_residual_window_ticks,
                        recent_residual_sign_imbalance,
                        recent_residual_run_length,
                        recent_positive_residual_ratio,
                        recent_negative_residual_ratio,
                        slow_fit_deterioration,
                        slow_fit_deterioration_pct,
                        fast_slow_disagreement_score,
                        best_candidate_split_tick_id,
                        best_two_line_improvement_pct,
                        best_two_line_left_sse,
                        best_two_line_right_sse,
                        best_two_line_total_sse,
                        break_pressure_score,
                        pressure_state,
                        confidence_state,
                        split_probe_window_ticks,
                        split_probe_min_segment_ticks
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (snapshot_id) DO UPDATE SET
                        recent_residual_window_ticks = EXCLUDED.recent_residual_window_ticks,
                        recent_residual_sign_imbalance = EXCLUDED.recent_residual_sign_imbalance,
                        recent_residual_run_length = EXCLUDED.recent_residual_run_length,
                        recent_positive_residual_ratio = EXCLUDED.recent_positive_residual_ratio,
                        recent_negative_residual_ratio = EXCLUDED.recent_negative_residual_ratio,
                        slow_fit_deterioration = EXCLUDED.slow_fit_deterioration,
                        slow_fit_deterioration_pct = EXCLUDED.slow_fit_deterioration_pct,
                        fast_slow_disagreement_score = EXCLUDED.fast_slow_disagreement_score,
                        best_candidate_split_tick_id = EXCLUDED.best_candidate_split_tick_id,
                        best_two_line_improvement_pct = EXCLUDED.best_two_line_improvement_pct,
                        best_two_line_left_sse = EXCLUDED.best_two_line_left_sse,
                        best_two_line_right_sse = EXCLUDED.best_two_line_right_sse,
                        best_two_line_total_sse = EXCLUDED.best_two_line_total_sse,
                        break_pressure_score = EXCLUDED.break_pressure_score,
                        pressure_state = EXCLUDED.pressure_state,
                        confidence_state = EXCLUDED.confidence_state,
                        split_probe_window_ticks = EXCLUDED.split_probe_window_ticks,
                        split_probe_min_segment_ticks = EXCLUDED.split_probe_min_segment_ticks,
                        updated_at = NOW()
                    """,
                    (
                        snapshot_id,
                        break_pressure["recentResidualWindowTicks"],
                        break_pressure["recentResidualSignImbalance"],
                        break_pressure["recentResidualRunLength"],
                        break_pressure["recentPositiveResidualRatio"],
                        break_pressure["recentNegativeResidualRatio"],
                        break_pressure["slowFitDeterioration"],
                        break_pressure["slowFitDeteriorationPct"],
                        break_pressure["fastSlowDisagreementScore"],
                        break_pressure["bestCandidateSplitTickId"],
                        break_pressure["bestTwoLineImprovementPct"],
                        break_pressure["bestTwoLineLeftSse"],
                        break_pressure["bestTwoLineRightSse"],
                        break_pressure["bestTwoLineTotalSse"],
                        break_pressure["breakPressureScore"],
                        break_pressure["pressureState"],
                        break_pressure["confidenceState"],
                        break_pressure["splitProbeWindowTicks"],
                        break_pressure["splitProbeMinSegmentTicks"],
                    ),
                )
            conn.commit()
        return {"requested": True, "stored": True, "snapshotId": snapshot_id, "error": None}
    except Exception as exc:
        return {"requested": True, "stored": False, "snapshotId": None, "error": str(exc)}


def is_system_schema(schema_name: str) -> bool:
    return schema_name in SYSTEM_SCHEMAS or schema_name.startswith(SYSTEM_SCHEMA_PREFIXES)


def quote_identifier(identifier: str) -> str:
    return '"{0}"'.format(identifier.replace('"', '""'))


def qualified_name(schema_name: str, object_name: str) -> str:
    return "{0}.{1}".format(quote_identifier(schema_name), quote_identifier(object_name))


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


def has_explicit_transaction_control(statements: List[str]) -> bool:
    return any(statement_head(statement) in TRANSACTION_CONTROL_HEADS for statement in statements)


def describe_columns(description: Any) -> List[Dict[str, Any]]:
    if not description:
        return []
    return [
        {
            "name": item.name,
            "typeCode": item.type_code,
        }
        for item in description
    ]


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


def serialize_pg_error(exc: Exception, statement: Optional[str] = None, statement_index: Optional[int] = None) -> Dict[str, Any]:
    diag = getattr(exc, "diag", None)
    position = getattr(diag, "statement_position", None) if diag else None
    line, column = line_column_from_position(statement or "", position)
    return {
        "message": getattr(diag, "message_primary", None) or str(exc),
        "detail": getattr(diag, "message_detail", None) if diag else None,
        "hint": getattr(diag, "message_hint", None) if diag else None,
        "context": getattr(diag, "context", None) if diag else None,
        "position": int(position) if position and str(position).isdigit() else None,
        "line": line,
        "column": column,
        "sqlstate": getattr(exc, "pgcode", None),
        "schema": getattr(diag, "schema_name", None) if diag else None,
        "table": getattr(diag, "table_name", None) if diag else None,
        "columnName": getattr(diag, "column_name", None) if diag else None,
        "constraint": getattr(diag, "constraint_name", None) if diag else None,
        "statementIndex": statement_index,
        "statement": statement,
    }


def build_statement_result(conn: Any, cur: Any, statement: str, statement_index: int, started: float, notices_start: int) -> Dict[str, Any]:
    columns = describe_columns(cur.description)
    rows: List[List[Any]] = []
    truncated = False
    if columns:
        rows, truncated = fetch_result_rows(cur, MAX_QUERY_ROWS)

    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    command_tag = cur.statusmessage or statement_head(statement)
    statement_type = command_tag.split()[0].upper() if command_tag else statement_head(statement)
    return {
        "index": statement_index,
        "statement": statement,
        "statementType": statement_type,
        "commandTag": command_tag,
        "rowCount": max(cur.rowcount, 0) if cur.rowcount is not None else None,
        "elapsedMs": elapsed_ms,
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
        "maxRows": MAX_QUERY_ROWS,
        "hasResultSet": bool(columns),
        "notices": conn.notices[notices_start:],
    }


def schema_payload() -> Dict[str, Any]:
    schema_map: Dict[str, Dict[str, Any]] = {}
    object_lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    kind_map = {
        "r": ("tables", "table"),
        "p": ("tables", "table"),
        "v": ("views", "view"),
        "m": ("materializedViews", "materialized_view"),
        "S": ("sequences", "sequence"),
    }

    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS object_name,
                    c.relkind,
                    COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0)::bigint AS row_estimate
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s
                  ON s.relid = c.oid
                WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%%'
                  AND n.nspname NOT LIKE 'pg_temp_%%'
                ORDER BY n.nspname, c.relkind, c.relname
                """
            )
            for row in cur.fetchall():
                schema_name = row["schema_name"]
                schema_entry = schema_map.setdefault(
                    schema_name,
                    {
                        "schema": schema_name,
                        "counts": {
                            "tables": 0,
                            "views": 0,
                            "materializedViews": 0,
                            "sequences": 0,
                            "functions": 0,
                        },
                        "objects": {
                            "tables": [],
                            "views": [],
                            "materializedViews": [],
                            "sequences": [],
                            "functions": [],
                        },
                    },
                )
                list_name, object_kind = kind_map[row["relkind"]]
                entry = {
                    "name": row["object_name"],
                    "schema": schema_name,
                    "kind": object_kind,
                    "rowEstimate": row["row_estimate"],
                    "columns": [],
                    "indexes": [],
                }
                schema_entry["objects"][list_name].append(entry)
                schema_entry["counts"][list_name] += 1
                object_lookup[(schema_name, row["object_name"], object_kind)] = entry

            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS object_name,
                    c.relkind,
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    a.attnotnull AS not_null,
                    pg_get_expr(d.adbin, d.adrelid) AS default_value,
                    a.attidentity <> '' AS is_identity,
                    a.attgenerated <> '' AS is_generated,
                    a.attnum AS ordinal_position
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                JOIN pg_attribute a
                  ON a.attrelid = c.oid
                 AND a.attnum > 0
                 AND NOT a.attisdropped
                LEFT JOIN pg_attrdef d
                  ON d.adrelid = a.attrelid
                 AND d.adnum = a.attnum
                WHERE c.relkind IN ('r', 'p', 'v', 'm')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%%'
                  AND n.nspname NOT LIKE 'pg_temp_%%'
                ORDER BY n.nspname, c.relname, a.attnum
                """
            )
            for row in cur.fetchall():
                object_kind = kind_map[row["relkind"]][1]
                entry = object_lookup.get((row["schema_name"], row["object_name"], object_kind))
                if entry is None:
                    continue
                entry["columns"].append(
                    {
                        "name": row["column_name"],
                        "dataType": row["data_type"],
                        "notNull": row["not_null"],
                        "default": row["default_value"],
                        "isIdentity": row["is_identity"],
                        "isGenerated": row["is_generated"],
                    }
                )

            cur.execute(
                """
                SELECT
                    schemaname AS schema_name,
                    tablename AS object_name,
                    indexname AS index_name,
                    indexdef
                FROM pg_indexes
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                  AND schemaname NOT LIKE 'pg_toast%%'
                  AND schemaname NOT LIKE 'pg_temp_%%'
                ORDER BY schemaname, tablename, indexname
                """
            )
            for row in cur.fetchall():
                entry = object_lookup.get((row["schema_name"], row["object_name"], "table"))
                if entry is None:
                    entry = object_lookup.get((row["schema_name"], row["object_name"], "materialized_view"))
                if entry is None:
                    continue
                entry["indexes"].append(
                    {
                        "name": row["index_name"],
                        "definition": row["indexdef"],
                    }
                )

            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    p.proname AS function_name,
                    pg_get_function_identity_arguments(p.oid) AS arguments,
                    pg_get_function_result(p.oid) AS returns
                FROM pg_proc p
                JOIN pg_namespace n
                  ON n.oid = p.pronamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%%'
                  AND n.nspname NOT LIKE 'pg_temp_%%'
                  AND p.prokind = 'f'
                ORDER BY n.nspname, p.proname, 3
                """
            )
            for row in cur.fetchall():
                schema_entry = schema_map.setdefault(
                    row["schema_name"],
                    {
                        "schema": row["schema_name"],
                        "counts": {
                            "tables": 0,
                            "views": 0,
                            "materializedViews": 0,
                            "sequences": 0,
                            "functions": 0,
                        },
                        "objects": {
                            "tables": [],
                            "views": [],
                            "materializedViews": [],
                            "sequences": [],
                            "functions": [],
                        },
                    },
                )
                schema_entry["objects"]["functions"].append(
                    {
                        "name": row["function_name"],
                        "schema": row["schema_name"],
                        "kind": "function",
                        "signature": row["arguments"],
                        "returns": row["returns"],
                    }
                )
                schema_entry["counts"]["functions"] += 1

    schemas = list(schema_map.values())
    total_objects = sum(
        entry["counts"]["tables"]
        + entry["counts"]["views"]
        + entry["counts"]["materializedViews"]
        + entry["counts"]["sequences"]
        + entry["counts"]["functions"]
        for entry in schemas
    )
    return {
        "context": context,
        "schemas": schemas,
        "schemaCount": len(schemas),
        "objectCount": total_objects,
    }


def relation_columns(conn: Any, schema_name: str, object_name: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                a.attnotnull AS not_null,
                pg_get_expr(d.adbin, d.adrelid) AS default_value,
                a.attidentity <> '' AS is_identity,
                a.attgenerated <> '' AS is_generated,
                a.attnum AS ordinal_position
            FROM pg_class c
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_attribute a
              ON a.attrelid = c.oid
             AND a.attnum > 0
             AND NOT a.attisdropped
            LEFT JOIN pg_attrdef d
              ON d.adrelid = a.attrelid
             AND d.adnum = a.attnum
            WHERE n.nspname = %s
              AND c.relname = %s
            ORDER BY a.attnum
            """,
            (schema_name, object_name),
        )
        return [
            {
                "name": row["column_name"],
                "dataType": row["data_type"],
                "notNull": row["not_null"],
                "default": row["default_value"],
                "isIdentity": row["is_identity"],
                "isGenerated": row["is_generated"],
            }
            for row in cur.fetchall()
        ]


def table_indexes(conn: Any, schema_name: str, object_name: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                indexname AS index_name,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s
              AND tablename = %s
            ORDER BY indexname
            """,
            (schema_name, object_name),
        )
        return [
            {
                "name": row["index_name"],
                "definition": row["indexdef"],
            }
            for row in cur.fetchall()
        ]


def load_object_details(schema_name: str, object_name: str, object_kind: str) -> Dict[str, Any]:
    if is_system_schema(schema_name):
        raise HTTPException(status_code=404, detail="Object not found.")

    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        if object_kind == "function":
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        p.oid,
                        p.proname,
                        pg_get_function_identity_arguments(p.oid) AS arguments,
                        pg_get_function_result(p.oid) AS returns,
                        pg_get_functiondef(p.oid) AS definition
                    FROM pg_proc p
                    JOIN pg_namespace n
                      ON n.oid = p.pronamespace
                    WHERE n.nspname = %s
                      AND p.proname = %s
                      AND p.prokind = 'f'
                    ORDER BY 3
                    """,
                    (schema_name, object_name),
                )
                overloads = cur.fetchall()
            if not overloads:
                raise HTTPException(status_code=404, detail="Function not found.")
            return {
                "context": context,
                "object": {
                    "schema": schema_name,
                    "name": object_name,
                    "kind": "function",
                    "overloads": [dict(row) for row in overloads],
                },
                "actions": {},
            }

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    c.oid,
                    c.relkind,
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
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
                """,
                (schema_name, object_name),
            )
            object_row = cur.fetchone()

        if not object_row:
            raise HTTPException(status_code=404, detail="Object not found.")

        relkind = object_row["relkind"]
        kind_map = {
            "r": "table",
            "p": "table",
            "v": "view",
            "m": "materialized_view",
            "S": "sequence",
        }
        actual_kind = kind_map[relkind]
        if object_kind != actual_kind:
            raise HTTPException(status_code=404, detail="Object not found.")

        columns = relation_columns(conn, schema_name, object_name) if actual_kind != "sequence" else []
        indexes = table_indexes(conn, schema_name, object_name) if actual_kind in {"table", "materialized_view"} else []
        definition = None
        sequence_state = None

        if actual_kind in {"view", "materialized_view"}:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_get_viewdef(%s, true)", (object_row["oid"],))
                row = cur.fetchone()
                definition = row[0] if row else None
        elif actual_kind == "sequence":
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        start_value,
                        minimum_value,
                        maximum_value,
                        increment,
                        cycle_option,
                        cache_size,
                        last_value
                    FROM pg_sequences
                    WHERE schemaname = %s
                      AND sequencename = %s
                    """,
                    (schema_name, object_name),
                )
                sequence_state = cur.fetchone()

        qualified = qualified_name(schema_name, object_name)
        actions = {
            "insertSelect": "SELECT *\nFROM {0}\nLIMIT 100;".format(qualified),
            "insertPreview": "SELECT *\nFROM {0}\nORDER BY 1\nLIMIT 100;".format(qualified),
            "insertExplain": "EXPLAIN ANALYZE\nSELECT *\nFROM {0}\nLIMIT 100;".format(qualified),
        }

        return {
            "context": context,
            "object": {
                "schema": schema_name,
                "name": object_name,
                "kind": actual_kind,
                "rowEstimate": object_row["row_estimate"],
                "totalBytes": object_row["total_bytes"],
                "totalSize": object_row["total_size"],
                "comment": object_row["comment"],
                "columns": columns,
                "indexes": indexes,
                "definition": definition,
                "sequence": dict(sequence_state) if sequence_state else None,
            },
            "actions": actions,
        }


def preview_relation(
    schema_name: str,
    object_name: str,
    limit: int,
    offset: int,
    order_by: Optional[str],
    order_dir: str,
) -> Dict[str, Any]:
    if is_system_schema(schema_name):
        raise HTTPException(status_code=404, detail="Relation not found.")

    started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relname = %s
                  AND c.relkind IN ('r', 'p', 'v', 'm')
                """,
                (schema_name, object_name),
            )
            relation_row = cur.fetchone()
        if not relation_row:
            raise HTTPException(status_code=404, detail="Preview is only available for tables and views.")

        columns = relation_columns(conn, schema_name, object_name)
        column_names = {column["name"] for column in columns}
        if order_by and order_by not in column_names:
            raise HTTPException(status_code=400, detail="Unknown sort column: {0}".format(order_by))

        relation_sql = pg_sql.SQL(".").join([pg_sql.Identifier(schema_name), pg_sql.Identifier(object_name)])
        query = pg_sql.SQL("SELECT * FROM {}").format(relation_sql)
        if order_by:
            direction_sql = pg_sql.SQL("DESC") if order_dir.lower() == "desc" else pg_sql.SQL("ASC")
            query += pg_sql.SQL(" ORDER BY {} {}").format(pg_sql.Identifier(order_by), direction_sql)
        query += pg_sql.SQL(" LIMIT %s OFFSET %s")

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
            cur.execute(query, (limit + 1, offset))
            result_columns = describe_columns(cur.description)
            rows, truncated = fetch_result_rows(cur, limit)

        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
        return {
            "context": context,
            "result": {
                "index": 1,
                "title": "{0}.{1}".format(schema_name, object_name),
                "statement": "preview",
                "statementType": "SELECT",
                "commandTag": "SELECT",
                "rowCount": len(rows),
                "elapsedMs": elapsed_ms,
                "columns": result_columns,
                "rows": rows,
                "truncated": truncated,
                "maxRows": limit,
                "hasResultSet": True,
                "source": {
                    "schema": schema_name,
                    "name": object_name,
                    "kind": "preview",
                    "orderBy": order_by,
                    "orderDir": order_dir.lower(),
                    "offset": offset,
                    "limit": limit,
                },
            },
        }


def execute_query(sql_text: str) -> Dict[str, Any]:
    statements = split_sql_script(sql_text)
    started = time.perf_counter()
    explicit_transactions = has_explicit_transaction_control(statements)
    results: List[Dict[str, Any]] = []

    with db_connection(readonly=False, autocommit=explicit_transactions) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
                cur.execute("SET lock_timeout = %s", (str(LOCK_TIMEOUT_MS),))
                cur.execute("SET idle_in_transaction_session_timeout = '5000'")

                for index, statement in enumerate(statements, start=1):
                    notices_start = len(conn.notices)
                    statement_started = time.perf_counter()
                    cur.execute(statement)
                    results.append(
                        build_statement_result(
                            conn=conn,
                            cur=cur,
                            statement=statement,
                            statement_index=index,
                            started=statement_started,
                            notices_start=notices_start,
                        )
                    )

            if not explicit_transactions:
                conn.commit()

            context = fetch_sql_context(conn)
        except Exception as exc:
            if conn.status != pg_extensions.STATUS_READY:
                conn.rollback()
            raise HTTPException(
                status_code=400,
                detail=serialize_pg_error(
                    exc,
                    statement=statements[len(results)] if len(results) < len(statements) else None,
                    statement_index=len(results) + 1 if len(results) < len(statements) else None,
                ),
            ) from exc

    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    return {
        "success": True,
        "statementCount": len(statements),
        "transactionMode": "explicit" if explicit_transactions else "script",
        "elapsedMs": elapsed_ms,
        "context": context,
        "results": results,
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


@app.get("/regression", include_in_schema=False)
def regression_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "regression.html")


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


@app.get("/api/regression/bootstrap")
def regression_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
    fast: int = Query(DEFAULT_REGRESSION_FAST_WINDOW, ge=MIN_ANALYSIS_WINDOW, le=MAX_WINDOW),
    slow: int = Query(DEFAULT_REGRESSION_SLOW_WINDOW, ge=MIN_ANALYSIS_WINDOW, le=MAX_WINDOW),
    series: str = Query("mid", pattern="^(ask|bid|mid)$"),
    persist: bool = Query(True),
) -> Dict[str, Any]:
    rows = fetch_bootstrap_rows(mode, id, window)
    return build_regression_response(
        mode=mode,
        series=series,
        window=window,
        fast_window_ticks=fast,
        slow_window_ticks=slow,
        rows=rows,
        requested_start_id=id,
        persist=persist,
    )


@app.get("/api/regression/next")
def regression_next(
    mode: str = Query("live", pattern="^(live|review)$"),
    afterId: int = Query(..., ge=0),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
    fast: int = Query(DEFAULT_REGRESSION_FAST_WINDOW, ge=MIN_ANALYSIS_WINDOW, le=MAX_WINDOW),
    slow: int = Query(DEFAULT_REGRESSION_SLOW_WINDOW, ge=MIN_ANALYSIS_WINDOW, le=MAX_WINDOW),
    series: str = Query("mid", pattern="^(ask|bid|mid)$"),
    limit: int = Query(DEFAULT_REGRESSION_STEP, ge=1, le=MAX_STREAM_BATCH),
    persist: bool = Query(True),
) -> Dict[str, Any]:
    new_rows = fetch_rows_after(afterId, limit)
    if not new_rows:
        return {
            "rows": [],
            "newRows": [],
            "rowCount": 0,
            "newRowCount": 0,
            "advanced": False,
            "lastId": afterId,
            "mode": mode,
            "window": window,
            "symbol": TICK_SYMBOL,
            "series": series,
            "persistence": {"requested": persist, "stored": False, "snapshotId": None, "error": None},
        }

    rows = fetch_window_ending_at(new_rows[-1]["id"], window)
    return build_regression_response(
        mode=mode,
        series=series,
        window=window,
        fast_window_ticks=fast,
        slow_window_ticks=slow,
        rows=rows,
        new_rows=new_rows,
        persist=persist,
        advanced_from_id=afterId,
    )


@app.get("/api/sql/schema")
def sql_schema(_: str = Depends(require_sql_admin)) -> Dict[str, Any]:
    return schema_payload()


@app.get("/api/sql/object")
def sql_object(
    schema: str = Query(..., min_length=1),
    name: str = Query(..., min_length=1),
    kind: str = Query(..., pattern="^(table|view|materialized_view|sequence|function)$"),
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
    orderDir: str = Query("asc", pattern="^(asc|desc)$"),
    _: str = Depends(require_sql_admin),
) -> Dict[str, Any]:
    return preview_relation(schema, name, limit, offset, orderBy, orderDir)


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest, _: str = Depends(require_sql_admin)) -> Dict[str, Any]:
    return execute_query(payload.sql)
