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

import psycopg2.extras

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


@app.get("/api/evals/window")
def api_evals_window(
    tick_from: int = Query(..., ge=1),
    tick_to: int = Query(..., ge=1),
    min_level: int = Query(1, ge=1),
    max_rows: int = Query(200_000, ge=1, le=1_000_000),
):
    """
    Return evals between tick_from and tick_to (inclusive),
    filtered by level >= min_level.

    max_rows is a safety cap to avoid returning too many rows.
    """
    if tick_to < tick_from:
        tick_from, tick_to = tick_to, tick_from

    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT
                id,
                tick_id,
                mid,
                timestamp,
                base_sign,
                level,
                signed_importance,
                promotion_path,
                computed_at
            FROM evals
            WHERE tick_id BETWEEN %s AND %s
              AND level >= %s
            ORDER BY tick_id, level
            LIMIT %s
            """,
            (tick_from, tick_to, min_level, max_rows),
        )
        rows = cur.fetchall()

    rows_json = _jsonable(rows)
    truncated = len(rows_json) >= max_rows

    return {
        "tick_from": tick_from,
        "tick_to": tick_to,
        "min_level": min_level,
        "max_rows": max_rows,
        "truncated": truncated,
        "evals": rows_json,
    }
    
    
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
# Used by review.html / review-core.js (no live stream, just windows).

@app.get("/api/review/window")
def api_review_window(
    from_id: int = Query(..., description="Starting tick id (inclusive)"),
    window: int = Query(5000, ge=100, le=50000),
):
    """
    Historical review window over ticks.

    Returns JSON:
      {
        "ticks":      [...],
        "segs":       [...],  # kalseg (old, if exists)
        "zones":      [...],  # zones  (old, if exists)

        "piv_hilo":   [...],  # local highs/lows (if piv_hilo exists)
        "piv_swings": [...],  # swing pivots      (if piv_swings exists)
        "hhll":       [...],  # HH/HL/LH/LL       (if hhll_piv exists)
        "zones_hhll": [...]   # HH/LL zones       (if zones_hhll exists)
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
        # Ticks
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
            # Nothing in this range – return empty but valid payload
            return {
                "ticks":      [],
                "segs":       [],
                "zones":      [],
                "piv_hilo":   [],
                "piv_swings": [],
                "hhll":       [],
                "zones_hhll": [],
            }

        # normalize ticks (floats + kal + spread + ts ISO)
        for r in ticks:
            if isinstance(r.get("mid"), Decimal):
                r["mid"] = float(r["mid"])

            if has_kal:
                if isinstance(r.get("kal"), Decimal):
                    r["kal"] = float(r["kal"])
            else:
                # mirror mid so frontend always has a kal value
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

        # For other tables we can safely use the tick id range
        window_start = int(ticks[0]["id"])
        window_end = int(ticks[-1]["id"])

        # ------------------------------------------------
        # Old kalseg segments (if present)
        # ------------------------------------------------
        if _table_exists(conn, "kalseg"):
            cur.execute(
                """
                SELECT id, start_id, end_id, direction
                FROM kalseg
                WHERE end_id   >= %s
                  AND start_id <= %s
                ORDER BY start_id
                """,
                (window_start, window_end),
            )
            segs = cur.fetchall()
        else:
            segs = []

        # ------------------------------------------------
        # Old zones (if present)
        # ------------------------------------------------
        if _table_exists(conn, "zones"):
            cur.execute(
                """
                SELECT id, start_id, end_id, direction, zone_type
                FROM zones
                WHERE end_id   >= %s
                  AND start_id <= %s
                ORDER BY start_id
                """,
                (window_start, window_end),
            )
            zones = cur.fetchall()
        else:
            zones = []

        # ------------------------------------------------
        # NEW: piv_hilo (raw local highs/lows) – only if table exists
        # ------------------------------------------------
        if _table_exists(conn, "piv_hilo"):
            cur.execute(
                """
                SELECT id,
                       tick_id,
                       ts,
                       mid,
                       ptype,
                       win_left,
                       win_right
                FROM piv_hilo
                WHERE tick_id BETWEEN %s AND %s
                ORDER BY tick_id
                """,
                (window_start, window_end),
            )
            piv_hilo = cur.fetchall()
        else:
            piv_hilo = []

        # ------------------------------------------------
        # NEW: piv_swings – swing pivots (uses ptype, not stype)
        # ------------------------------------------------
        if _table_exists(conn, "piv_swings"):
            cur.execute(
                """
                SELECT id,
                       pivot_id,
                       tick_id,
                       ts,
                       mid,
                       ptype,
                       swing_index
                FROM piv_swings
                WHERE tick_id BETWEEN %s AND %s
                ORDER BY tick_id
                """,
                (window_start, window_end),
            )
            piv_swings = cur.fetchall()
        else:
            piv_swings = []

        # ------------------------------------------------
        # NEW: hhll_piv → hhll (HH/HL/LH/LL)
        # ------------------------------------------------
        if _table_exists(conn, "hhll_piv"):
            cur.execute(
                """
                SELECT id,
                       swing_id,
                       tick_id,
                       ts,
                       mid,
                       ptype,
                       class,
                       class_text
                FROM hhll_piv
                WHERE tick_id BETWEEN %s AND %s
                ORDER BY tick_id
                """,
                (window_start, window_end),
            )
            hhll = cur.fetchall()
        else:
            hhll = []

        # ------------------------------------------------
        # NEW: zones_hhll – time/price rectangles built on hhll
        #      (filter using start_tick_id / end_tick_id)
        # ------------------------------------------------
        if _table_exists(conn, "zones_hhll"):
            cur.execute(
                """
                SELECT id,
                       start_tick_id,
                       end_tick_id,
                       start_time,
                       end_time,
                       top_price,
                       bot_price,
                       state,
                       break_dir
                FROM zones_hhll
                WHERE end_tick_id   >= %s
                  AND start_tick_id <= %s
                ORDER BY start_tick_id
                """,
                (window_start, window_end),
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


# -------------------------- Review segLines API -------------------------
# (append-only block; does not modify existing routes)

def _review_segm_meta(conn, segm_id: int) -> Dict[str, Any]:
    with dict_cur(conn) as cur:
        cur.execute(
            "SELECT (start_ts::date)::text AS date FROM public.segms WHERE id=%s",
            (int(segm_id),),
        )
        srow = cur.fetchone()
        if not srow:
            return {"error": "segm not found", "segm_id": int(segm_id)}

        cur.execute(
            """
            SELECT COUNT(*)::int AS num_ticks,
                   MIN(tick_id) AS tick_from,
                   MAX(tick_id) AS tick_to
            FROM public.segticks
            WHERE segm_id=%s
            """,
            (int(segm_id),),
        )
        tr = cur.fetchone() or {}

        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE is_active=true)::int AS num_lines_active,
                   MAX(max_abs_dist) FILTER (WHERE is_active=true) AS global_max_abs_dist
            FROM public.seglines
            WHERE segm_id=%s
            """,
            (int(segm_id),),
        )
        lr = cur.fetchone() or {}

    return {
        "segm_id": int(segm_id),
        "date": srow["date"],
        "num_ticks": int(tr["num_ticks"]) if tr.get("num_ticks") is not None else 0,
        "tick_from": int(tr["tick_from"]) if tr.get("tick_from") is not None else None,
        "tick_to": int(tr["tick_to"]) if tr.get("tick_to") is not None else None,
        "num_lines_active": int(lr["num_lines_active"]) if lr.get("num_lines_active") is not None else 0,
        "global_max_abs_dist": float(lr["global_max_abs_dist"]) if lr.get("global_max_abs_dist") is not None else None,
    }


@app.get("/api/review/default_segm")
def api_review_default_segm():
    """
    Prefer latest segm with no seglines. Fallback to latest segm.
    date is derived from segms.start_ts::date (segms has no 'date' column).
    """
    conn = get_conn()
    try:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT s.id AS segm_id,
                       (s.start_ts::date)::text AS date
                FROM public.segms s
                LEFT JOIN public.seglines l ON l.segm_id = s.id
                GROUP BY s.id
                HAVING COUNT(l.id) = 0
                ORDER BY s.id DESC
                LIMIT 1
                """
            )
            r = cur.fetchone()
            if r:
                return {"segm_id": int(r["segm_id"]), "date": r["date"], "has_segLines": False}

            cur.execute(
                """
                SELECT s.id AS segm_id,
                       (s.start_ts::date)::text AS date,
                       (COUNT(l.id) > 0) AS has_seglines
                FROM public.segms s
                LEFT JOIN public.seglines l ON l.segm_id = s.id
                GROUP BY s.id
                ORDER BY s.id DESC
                LIMIT 1
                """
            )
            r = cur.fetchone()
            if not r:
                return {"error": "no segms found"}
            return {"segm_id": int(r["segm_id"]), "date": r["date"], "has_segLines": bool(r["has_seglines"])}
    finally:
        conn.close()


@app.get("/api/review/segms")
def api_review_segms(limit: int = Query(200, ge=1, le=2000)):
    conn = get_conn()
    try:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                WITH tick_counts AS (
                  SELECT segm_id, COUNT(*)::int AS num_ticks
                  FROM public.segticks
                  GROUP BY segm_id
                ),
                line_stats AS (
                  SELECT segm_id,
                         COUNT(*) FILTER (WHERE is_active=true)::int AS num_lines_active,
                         MAX(max_abs_dist) FILTER (WHERE is_active=true) AS global_max_abs_dist
                  FROM public.seglines
                  GROUP BY segm_id
                )
                SELECT s.id AS segm_id,
                       (s.start_ts::date)::text AS date,
                       COALESCE(tc.num_ticks, 0) AS num_ticks,
                       COALESCE(ls.num_lines_active, 0) AS num_lines_active,
                       ls.global_max_abs_dist
                FROM public.segms s
                LEFT JOIN tick_counts tc ON tc.segm_id = s.id
                LEFT JOIN line_stats ls ON ls.segm_id = s.id
                ORDER BY s.id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append(
                {
                    "segm_id": int(r["segm_id"]),
                    "date": r["date"],
                    "num_ticks": int(r["num_ticks"]),
                    "num_lines_active": int(r["num_lines_active"]),
                    "global_max_abs_dist": float(r["global_max_abs_dist"]) if r["global_max_abs_dist"] is not None else None,
                }
            )
        return out
    finally:
        conn.close()


@app.get("/api/review/segm/{segm_id}/meta")
def api_review_segm_meta(segm_id: int):
    conn = get_conn()
    try:
        return _review_segm_meta(conn, int(segm_id))
    finally:
        conn.close()


@app.get("/api/review/segm/{segm_id}/ticks_sample")
def api_review_ticks_sample(
    segm_id: int,
    target_points: int = Query(5000, ge=100, le=50000),
):
    segm_id = int(segm_id)
    target = max(100, min(int(target_points), 50000))

    conn = get_conn()
    try:
        with dict_cur(conn) as cur:
            cur.execute(
                "SELECT COUNT(*)::int AS n FROM public.segticks WHERE segm_id=%s",
                (segm_id,),
            )
            n = int(cur.fetchone()["n"])
            if n <= 0:
                return {"segm_id": segm_id, "stride": 1, "points": []}

            stride = (n + target - 1) // target
            if stride < 1:
                stride = 1

            # NOTE: assumes ticks has timestamp, ask, bid, mid, kal columns (as in your existing stack)
            cur.execute(
                """
                WITH ordered AS (
                  SELECT t.id AS id,
                         t.timestamp AS ts,
                         t.ask, t.bid, t.mid, t.kal,
                         ROW_NUMBER() OVER (ORDER BY t.timestamp ASC, t.id ASC) AS rn
                  FROM public.segticks st
                  JOIN public.ticks t ON t.id = st.tick_id
                  WHERE st.segm_id=%s
                )
                SELECT id, ts, ask, bid, mid, kal
                FROM ordered
                WHERE ((rn - 1) %% %s) = 0
                ORDER BY ts ASC, id ASC
                """,
                (segm_id, int(stride)),
            )
            rows = cur.fetchall()

        pts = []
        for r in rows:
            pts.append(
                {
                    "id": int(r["id"]),
                    "ts": r["ts"].isoformat(),
                    "ask": float(r["ask"]) if r["ask"] is not None else None,
                    "bid": float(r["bid"]) if r["bid"] is not None else None,
                    "mid": float(r["mid"]) if r["mid"] is not None else None,
                    "kal": float(r["kal"]) if r["kal"] is not None else None,
                }
            )

        return {"segm_id": segm_id, "stride": int(stride), "points": pts}
    finally:
        conn.close()


@app.get("/api/review/segm/{segm_id}/lines")
def api_review_lines(segm_id: int):
    conn = get_conn()
    try:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT id, parent_id, depth, iteration,
                       start_ts, end_ts, start_price, end_price,
                       num_ticks, duration_ms, max_abs_dist
                FROM public.seglines
                WHERE segm_id=%s AND is_active=true
                ORDER BY max_abs_dist DESC NULLS LAST, id ASC
                """,
                (int(segm_id),),
            )
            rows = cur.fetchall()

        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r["id"]),
                    "parent_id": int(r["parent_id"]) if r["parent_id"] is not None else None,
                    "depth": int(r["depth"]),
                    "iteration": int(r["iteration"]),
                    "start_ts": r["start_ts"].isoformat(),
                    "end_ts": r["end_ts"].isoformat(),
                    "start_price": float(r["start_price"]),
                    "end_price": float(r["end_price"]),
                    "num_ticks": int(r["num_ticks"]) if r["num_ticks"] is not None else None,
                    "duration_ms": int(r["duration_ms"]) if r["duration_ms"] is not None else None,
                    "max_abs_dist": float(r["max_abs_dist"]) if r["max_abs_dist"] is not None else None,
                }
            )

        return {"segm_id": int(segm_id), "lines": out}
    finally:
        conn.close()


@app.post("/api/review/breakLine")
def api_review_breakline(payload: Dict[str, Any] = Body(...)):
    segm_id = payload.get("segm_id", None)
    segLine_id = payload.get("segLine_id", None)

    if segm_id is None:
        raise HTTPException(status_code=400, detail="segm_id required")

    segm_id = int(segm_id)
    segLine_id = int(segLine_id) if segLine_id is not None else None

    # Import locally to avoid changing top-of-file imports
    from jobs.breakLine import break_line

    result = break_line(segm_id=segm_id, segLine_id=segLine_id)
    if isinstance(result, dict) and "error" in result:
        return {"result": result, "meta": {"segm_id": segm_id}}

    conn = get_conn()
    try:
        meta = _review_segm_meta(conn, segm_id)
    finally:
        conn.close()

    return {"result": result, "meta": meta}


# ------------------------------ Journal API ------------------------------
# Adds a tiny append-only text journal under /src/journal/YYYY-MM-DD.txt
# nginx serves /src, so it becomes reachable at:
#   https://datavis.au/src/journal/YYYY-MM-DD.txt

@app.post("/api/journal/write")
def api_journal_write(payload: Dict[str, Any] = Body(...)):
    # local import so we don't disturb your existing import section
    from backend import journal as j

    event = payload.get("event", "event")
    segm_id = payload.get("segm_id", None)
    segline_id = payload.get("segline_id", None)
    details = payload.get("details", None)
    extra = payload.get("extra", None)

    line = j.format_event(
        event=str(event),
        segm_id=int(segm_id) if segm_id is not None else None,
        segline_id=int(segline_id) if segline_id is not None else None,
        details=str(details) if details is not None else None,
        extra=extra if isinstance(extra, dict) else None,
    )
    return j.append_line(line)


@app.get("/api/journal/today")
def api_journal_today(tail: int = Query(50, ge=1, le=500)):
    """
    Convenience endpoint (optional) – returns the last N lines of today's journal
    so frontend can show it without reading /src directly.
    """
    from backend import journal as j
    from datetime import datetime, timezone

    fname = datetime.now(timezone.utc).strftime("%Y-%m-%d") + ".txt"
    full_path = os.path.join(j.JOURNAL_DIR, fname)

    if not os.path.exists(full_path):
        return {"ok": True, "filename": fname, "url_path": f"/src/journal/{fname}", "lines": []}

    with open(full_path, "r", encoding="utf-8") as f:
        lines = [ln.rstrip("\n") for ln in f.readlines()]

    # tail last N lines
    if tail and len(lines) > tail:
        lines = lines[-tail:]

    return {"ok": True, "filename": fname, "url_path": f"/src/journal/{fname}", "lines": lines}
