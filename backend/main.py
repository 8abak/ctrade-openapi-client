# PATH: backend/main.py
# FastAPI backend (clean version)
# - Keeps only what sql.html, review.html, and live.html need.
# - SQL console: /api/sql/* and legacy /sqlvw/*.
# - Review window: /api/review/window
# - Live window:   /api/live_window

import os
import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Body, Query, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from backend.db import (
    get_conn,
    dict_cur,
    detect_ts_col,
    detect_mid_expr,
    detect_bid_ask,
)

VERSION = "2025.11.24.clean-v1"

app = FastAPI(title="cTrade backend (clean)")

# --------------------------- Static & CORS ----------------------------

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------- Utilities ------------------------------

def _jsonable(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, dict):
        return {k: _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    return o


def _ts_mid_cols(conn):
    """Detect timestamp column, mid expression, and bid/ask presence."""
    return detect_ts_col(conn), detect_mid_expr(conn), detect_bid_ask(conn)


def _table_exists(conn, name: str) -> bool:
    """Check if a table exists in public schema."""
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name   = %s
            """,
            (name,),
        )
        return cur.fetchone() is not None


def _ticks_has_kal(conn) -> bool:
    """Return True if ticks table has a 'kal' column."""
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = 'ticks'
              AND column_name  = 'kal'
            """
        )
        return cur.fetchone() is not None


# --------------------------- Basic / Status ---------------------------

@app.get("/")
def root():
    return {"ok": True, "version": VERSION}


@app.get("/version")
def get_version():
    return {"version": VERSION}


# -------------------------- SQL Console APIs --------------------------
# These are what sql.html / sql-core.js rely on.

# Legacy endpoints kept for compatibility with older tooling
@app.get("/sqlvw/tables")
def sqlvw_tables():
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname='public'
              AND tablename NOT LIKE 'pg_%'
            ORDER BY tablename
            """
        )
        return [r["tablename"] for r in cur.fetchall()]


@app.get("/sqlvw/query")
def sqlvw_query(query: str = Query(...)):
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(query)
        if cur.description:
            return _jsonable(cur.fetchall())
        return {"ok": True, "rowcount": cur.rowcount}


# Newer API used primarily by sql-core.js
@app.get("/api/sql/tables")
def api_sql_tables():
    """
    List visible tables in public schema, excluding ones explicitly hidden
    in meta.hidden_tables (if that table exists).
    """
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT t.table_name
            FROM information_schema.tables t
            LEFT JOIN meta.hidden_tables h
              ON h.table_schema = t.table_schema
             AND h.table_name   = t.table_name
            WHERE t.table_schema = 'public'
              AND t.table_type   = 'BASE TABLE'
              AND (h.table_name IS NULL)
            ORDER BY t.table_name
            """
        )
        return [r["table_name"] for r in cur.fetchall()]


@app.get("/api/sql")
def api_sql_get(q: str = ""):
    """
    Simple read-only SELECT endpoint via query string (?q=...).
    """
    q = (q or "").strip()
    if not q:
        return {"rows": []}
    if not q.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT is allowed here.")
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(q)
        return {"rows": _jsonable(cur.fetchall())}


@app.post("/api/sql")
def api_sql_post(sql: str = Body("", embed=True)):
    """
    Read-only SELECT endpoint via POST body { "sql": "SELECT ..." }.
    """
    stmt = (sql or "").strip()
    if not stmt:
        return {"rows": []}
    if not stmt.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT is allowed here.")
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(stmt)
        return {"rows": _jsonable(cur.fetchall())}


@app.post("/api/sql/exec")
def api_sql_exec(
    sql: str = Body("", embed=True),
    unsafe: Optional[bool] = Query(False),
    x_allow_write: Optional[str] = Header(None),
):
    """
    DDL/DML executor used by the "Execute" button in sql.html.

    Guard rails:
      - require unsafe=true query param
      - require header X-Allow-Write: yes
    Accepts multiple semicolon-separated statements and wraps them in a single
    transaction. Returns per-statement results/rowcounts.
    """
    stmt = (sql or "").strip()
    if not stmt:
        return {"ok": True, "results": []}

    if not unsafe or (x_allow_write or "").lower() != "yes":
        raise HTTPException(
            status_code=403,
            detail="Write access disabled. Use unsafe=true and X-Allow-Write: yes",
        )

    import sqlparse

    conn = get_conn()
    conn.autocommit = False
    results: List[Dict[str, Any]] = []
    try:
        with conn, dict_cur(conn) as cur:
            for part in [p.strip() for p in sqlparse.split(stmt) if p.strip()]:
                cur.execute(part)
                if cur.description:
                    rows = cur.fetchall()
                    results.append(
                        {"type": "resultset", "rows": _jsonable(rows)}
                    )
                else:
                    results.append(
                        {"type": "rowcount", "rowcount": cur.rowcount}
                    )
        return {"ok": True, "results": results}
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"{type(e).__name__}: {e}",
        )


# ----------------------------- Review API -----------------------------
# Used by index.html / chart-core.js (historical windows, not streaming).

@app.get("/api/review/window")
def api_review_window(
    from_id: int = Query(..., description="Starting tick id (inclusive)"),
    window: int = Query(5000, ge=100, le=50000),
):
    """
    Extended historical review window over ticks.

    Returns JSON:
      {
        "ticks":       [...],
        "segs":        [...],  # kalseg (old, if exists)
        "zones":       [...],  # zones  (old, if exists)

        "piv_hilo":    [...],  # local highs/lows
        "piv_swings":  [...],  # swing points
        "hhll":        [...],  # HH/HL/LH/LL pivots
        "zones_hhll":  [...]   # HH/LL zones
      }
    """
    to_id = from_id + window - 1

    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    has_kal = _ticks_has_kal(conn)

    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    kal_sel = ", kal" if has_kal else ""

    with dict_cur(conn) as cur:
        # ------------------------------------------------
        # 1) Ticks
        # ------------------------------------------------
        cur.execute(
            f"""
            SELECT id,
                   {ts_col}   AS ts,
                   {mid_expr} AS mid
                   {kal_sel}{bid_sel}{ask_sel}
            FROM ticks
            WHERE id BETWEEN %s AND %s
            ORDER BY id ASC
            """,
            (from_id, to_id),
        )
        ticks = cur.fetchall()

        if not ticks:
            # No data in this window: everything else will be empty too.
            return {
                "ticks":      [],
                "segs":       [],
                "zones":      [],
                "piv_hilo":   [],
                "piv_swings": [],
                "hhll":       [],
                "zones_hhll": [],
            }

        # we’ll need the time range for zones_hhll
        cur.execute(
            """
            SELECT MIN(timestamp) AS min_ts,
                   MAX(timestamp) AS max_ts
            FROM ticks
            WHERE id BETWEEN %s AND %s
            """,
            (from_id, to_id),
        )
        ts_row = cur.fetchone()
        tick_ts_min = ts_row["min_ts"]
        tick_ts_max = ts_row["max_ts"]

        # normalize ticks (floats + kal + spread + ts ISO)
        for r in ticks:
            if isinstance(r.get("mid"), Decimal):
                r["mid"] = float(r["mid"])

            if has_kal:
                if isinstance(r.get("kal"), Decimal):
                    r["kal"] = float(r["kal"])
            else:
                # mirror mid so frontend always has kal value
                r["kal"] = r.get("mid")

            if has_bid and isinstance(r.get("bid"), Decimal):
                r["bid"] = float(r["bid"])
            if has_ask and isinstance(r.get("ask"), Decimal):
                r["ask"] = float(r["ask"])

            r["spread"] = (
                (r.get("ask") - r.get("bid"))
                if (
                    has_bid
                    and has_ask
                    and r.get("ask") is not None
                    and r.get("bid") is not None
                )
                else None
            )

            ts = r.get("ts")
            if isinstance(ts, (datetime, date)):
                r["ts"] = ts.isoformat()

        # ------------------------------------------------
        # 2) Old kalseg segments (optional)
        # ------------------------------------------------
        if _table_exists(conn, "kalseg"):
            cur.execute(
                """
                SELECT id, start_id, end_id, direction
                FROM kalseg
                WHERE NOT (end_id < %s OR start_id > %s)
                ORDER BY start_id
                """,
                (from_id, to_id),
            )
            segs = cur.fetchall()
        else:
            segs = []

        # ------------------------------------------------
        # 3) Old zones (optional)
        # ------------------------------------------------
        if _table_exists(conn, "zones"):
            cur.execute(
                """
                SELECT id, start_id, end_id, direction, zone_type
                FROM zones
                WHERE NOT (end_id < %s OR start_id > %s)
                ORDER BY start_id
                """,
                (from_id, to_id),
            )
            zones = cur.fetchall()
        else:
            zones = []

        # ------------------------------------------------
        # 4) NEW: piv_hilo (raw local highs/lows)
        # ------------------------------------------------
        if _table_exists(conn, "piv_hilo"):
            cur.execute(
                """
                SELECT tick_id, ts, mid, ptype, win_left, win_right
                FROM piv_hilo
                WHERE tick_id BETWEEN %s AND %s
                ORDER BY tick_id
                """,
                (from_id, to_id),
            )
            piv_hilo = cur.fetchall()
        else:
            piv_hilo = []

        # ------------------------------------------------
        # 5) NEW: piv_swings
        # ------------------------------------------------
        if _table_exists(conn, "piv_swings"):
            cur.execute(
                """
                SELECT id, tick_id, ts, mid, stype
                FROM piv_swings
                WHERE tick_id BETWEEN %s AND %s
                ORDER BY tick_id
                """,
                (from_id, to_id),
            )
            piv_swings = cur.fetchall()
        else:
            piv_swings = []

        # ------------------------------------------------
        # 6) NEW: hhll_piv → hhll (HH/HL/LH/LL)
        # ------------------------------------------------
        if _table_exists(conn, "hhll_piv"):
            cur.execute(
                """
                SELECT id, tick_id, ts, mid, class_text
                FROM hhll_piv
                WHERE tick_id BETWEEN %s AND %s
                ORDER BY tick_id
                """,
                (from_id, to_id),
            )
            hhll = cur.fetchall()
        else:
            hhll = []

        # ------------------------------------------------
        # 7) NEW: zones_hhll – time/price rectangles on hhll
        # ------------------------------------------------
        if _table_exists(conn, "zones_hhll") and tick_ts_min and tick_ts_max:
            cur.execute(
                """
                SELECT id,
                       start_time,
                       end_time,
                       top_price,
                       bot_price,
                       state,
                       break_dir
                FROM zones_hhll
                WHERE end_time   >= %s
                  AND start_time <= %s
                ORDER BY start_time
                """,
                (tick_ts_min, tick_ts_max),
            )
            zones_hhll = cur.fetchall()
        else:
            zones_hhll = []

    return {
        "ticks":      _jsonable(ticks),
        "segs":       _jsonable(segs),
        "zones":      _jsonable(zones),
        "piv_hilo":   _jsonable(piv_hilo),
        "piv_swings": _jsonable(piv_swings),
        "hhll":       _jsonable(hhll),
        "zones_hhll": _jsonable(zones_hhll),
    }






# ------------------------------ Live API ------------------------------
# Used for the live chart (window-based, but we can layer streaming/polling on top).
# live-core.js will talk to /api/live_window.

@app.get("/api/live_window")
def api_live_window(
    limit: int = Query(5000, ge=500, le=20000),
    before_id: Optional[int] = Query(
        None,
        description="If set, window ends at/before this tick id.",
    ),
    after_id: Optional[int] = Query(
        None,
        description="If set, window starts at/after this tick id.",
    ),
):
    """
    Unified window API for live view.

    Modes:
      - default (no before_id/after_id): last N ticks
      - before_id=X: window of N ticks ending at/before X
      - after_id=Y:  window of N ticks starting at/after Y

    Response:
      {
        "ticks":    [{id, ts, mid, kal?, bid?, ask?, spread?}, ...],
        "segments": [{id, start_id, end_id, direction}, ...],
        "zones":    [{id, start_id, end_id, direction, zone_type}, ...]
      }
    """
    if before_id is not None and after_id is not None:
        raise HTTPException(
            status_code=400,
            detail="Use only one of before_id or after_id",
        )

    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    has_kal = _ticks_has_kal(conn)

    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    kal_sel = ", kal" if has_kal else ""

    with dict_cur(conn) as cur:
        if before_id is not None:
            # window ending at/before before_id
            cur.execute(
                f"""
                SELECT id,
                       {ts_col}   AS ts,
                       {mid_expr} AS mid
                       {kal_sel}{bid_sel}{ask_sel}
                FROM ticks
                WHERE id <= %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (before_id, limit),
            )
            tick_rows = list(reversed(cur.fetchall()))
        elif after_id is not None:
            # window starting at/after after_id
            cur.execute(
                f"""
                SELECT id,
                       {ts_col}   AS ts,
                       {mid_expr} AS mid
                       {kal_sel}{bid_sel}{ask_sel}
                FROM ticks
                WHERE id >= %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (after_id, limit),
            )
            tick_rows = cur.fetchall()
        else:
            # default: last N ticks
            cur.execute(
                f"""
                SELECT id,
                       {ts_col}   AS ts,
                       {mid_expr} AS mid
                       {kal_sel}{bid_sel}{ask_sel}
                FROM ticks
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            tick_rows = list(reversed(cur.fetchall()))

    if not tick_rows:
        return {"ticks": [], "segments": [], "zones": []}

    # normalize tick rows
    for r in tick_rows:
        if isinstance(r.get("mid"), Decimal):
            r["mid"] = float(r["mid"])
        if has_kal:
            if isinstance(r.get("kal"), Decimal):
                r["kal"] = float(r["kal"])
        else:
            # If no kal column, mirror mid so frontend still has a value.
            r["kal"] = r.get("mid")

        if has_bid and isinstance(r.get("bid"), Decimal):
            r["bid"] = float(r["bid"])
        if has_ask and isinstance(r.get("ask"), Decimal):
            r["ask"] = float(r["ask"])

        r["spread"] = (
            (r.get("ask") - r.get("bid"))
            if (
                has_bid
                and has_ask
                and r.get("ask") is not None
                and r.get("bid") is not None
            )
            else None
        )
        if isinstance(r.get("ts"), (datetime, date)):
            r["ts"] = r["ts"].isoformat()

    window_start = int(tick_rows[0]["id"])
    window_end = int(tick_rows[-1]["id"])

    segments: List[Dict[str, Any]] = []
    zones: List[Dict[str, Any]] = []

    # kalseg segments
    if _table_exists(conn, "kalseg"):
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT id, start_id, end_id, direction
                FROM kalseg
                WHERE end_id   >= %s
                  AND start_id <= %s
                ORDER BY start_id ASC
                """,
                (window_start, window_end),
            )
            segments = [dict(r) for r in cur.fetchall()]

    # zones
    if _table_exists(conn, "zones"):
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT id, start_id, end_id, direction, zone_type
                FROM zones
                WHERE end_id   >= %s
                  AND start_id <= %s
                ORDER BY start_id ASC
                """,
                (window_start, window_end),
            )
            zones = [dict(r) for r in cur.fetchall()]

    return {
        "ticks": _jsonable(tick_rows),
        "segments": _jsonable(segments),
        "zones": _jsonable(zones),
    }
