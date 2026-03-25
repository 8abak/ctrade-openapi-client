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
DEFAULT_REGRESSION_FAST_WINDOW = int(os.getenv("DATAVIS_REGRESSION_FAST_WINDOW", "240"))
DEFAULT_REGRESSION_SLOW_WINDOW = int(os.getenv("DATAVIS_REGRESSION_SLOW_WINDOW", "1200"))
DEFAULT_REGRESSION_STEP = int(os.getenv("DATAVIS_REGRESSION_STEP", "120"))
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


@app.get("/regression", include_in_schema=False)
def regression_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "regression.html")


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
def sql_schema() -> Dict[str, Any]:
    schemas = schema_payload()
    return {"schemas": schemas, "schemaCount": len(schemas)}


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest) -> Dict[str, Any]:
    return execute_query(payload.sql)
