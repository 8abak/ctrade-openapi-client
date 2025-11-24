# PATH: backend/main.py
# FastAPI app that preserves prior routes and adds the ones used by
# review-core.js and sql-core.js.

import os, json
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Body, Query, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Your project helpers (already in repo)
from backend.db import (
    get_conn, dict_cur, detect_ts_col, detect_mid_expr, detect_bid_ask, scalar
)

VERSION = "2025.09.02.unified-routes"

app = FastAPI(title="cTrade backend")

ZIG_TABLES = {"max": "max", "mid": "mid", "min": "min"}

# Mount frontend (useful if you ever open /frontend/*.html via the app)
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")

# Keep permissive CORS for local debugging / your hosted pages
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Try to include any existing router you had under /api (keeps old routes intact)
try:
    from zig_api import router as lview_router  # noqa: F401
    app.include_router(lview_router, prefix="/api")
except Exception:
    pass


# ----------------------------- Utilities -----------------------------

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
    # Detects the timestamp column and mid expression; also which of bid/ask exist.
    return detect_ts_col(conn), detect_mid_expr(conn), detect_bid_ask(conn)


# --------------------------- Basic / Legacy ---------------------------

@app.get("/")
def root():
    return {"ok": True, "version": VERSION}

@app.get("/version")
def get_version():
    return {"version": VERSION}

# Prior SQL viewer endpoints (keep)
@app.get("/sqlvw/tables")
def sqlvw_tables():
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname='public' AND tablename NOT LIKE 'pg_%'
            ORDER BY tablename
        """)
        return [r["tablename"] for r in cur.fetchall()]

@app.get("/sqlvw/query")
def sqlvw_query(query: str = Query(...)):
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(query)
        if cur.description:
            return _jsonable(cur.fetchall())
        return {"ok": True, "rowcount": cur.rowcount}


# ---------------------- New SQL console endpoints ---------------------

@app.get("/api/sql/tables")
def api_sql_tables():
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute("""
            SELECT t.table_name
            FROM information_schema.tables t
            LEFT JOIN meta.hidden_tables h
              ON h.table_schema = t.table_schema
             AND h.table_name   = t.table_name
            WHERE t.table_schema='public'
              AND t.table_type='BASE TABLE'
              AND h.table_name IS NULL          -- hide only what’s listed
            ORDER BY t.table_name
        """)
        return [r["table_name"] for r in cur.fetchall()]



@app.get("/api/sql")
def api_sql_get(q: str = ""):
    q = (q or "").strip()
    if not q:
        return {"rows": []}
    if not q.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT is allowed here.")
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(q)
        return {"rows": _jsonable(cur.fetchall())}



# --- ADD: helper to get latest tick (id + time + prices)
@app.get("/api/ticks/latest")
def api_ticks_latest():
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)

    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id,
                   {ts_col} AS ts,
                   {mid_expr} AS mid
                   {bid_sel}
                   {ask_sel}
            FROM ticks
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if not row:
        return {}

    # normalize numerics to float and timestamp to ISO (same style as your other tick routes)
    if isinstance(row.get("mid"), Decimal):
        row["mid"] = float(row["mid"])
    if has_bid and isinstance(row.get("bid"), Decimal):
        row["bid"] = float(row["bid"])
    if has_ask and isinstance(row.get("ask"), Decimal):
        row["ask"] = float(row["ask"])

    row["spread"] = (
        (row.get("ask") - row.get("bid"))
        if (has_bid and has_ask and row.get("ask") is not None and row.get("bid") is not None)
        else None
    )
    row["ts"] = row["ts"].isoformat()

    return row

# --- ADD: unified zigzag fetcher (min/mid/max) by id window
from typing import Optional
from fastapi import Query

def _zig_overlapping(conn, table: str, start_id: int, end_id: int):
    # A segment overlaps the window if it starts before the window ends AND ends after the window starts
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT id, start_id, end_id, start_price, end_price
        FROM {table}
        WHERE end_id >= %s AND start_id <= %s
        ORDER BY start_id ASC, end_id ASC
        """,
        (start_id, end_id),
    )
    rows = cur.fetchall()
    return [
        dict(id=r[0], start_id=r[1], end_id=r[2], start_price=r[3], end_price=r[4])
        for r in rows
    ]

@app.get("/api/zigzags/within")
def zigzags_within(start_id: int, end_id: int, kinds: str = "max,mid,min"):
    """
    Return overlapping segments for requested kinds (comma-separated: max,mid,min)
    within [start_id, end_id] (tick_id range).
    """
    ks = [k.strip() for k in kinds.split(",") if k.strip() in ZIG_TABLES]
    if not ks:
        raise HTTPException(400, "no valid kinds provided")
    if end_id < start_id:
        start_id, end_id = end_id, start_id
    out: Dict[str, List[dict]] = {}
    with get_conn() as conn:
        for k in ks:
            out[k] = _zig_overlapping(conn, ZIG_TABLES[k], start_id, end_id)
    return out

# (Optional but handy)
@app.get("/api/max/by_id")
def max_by_id(id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, start_id, end_id, start_price, end_price FROM max WHERE id=%s", (id,))
        r = cur.fetchone()
        if not r: raise HTTPException(404, "not found")
        return dict(id=r[0], start_id=r[1], end_id=r[2], start_price=r[3], end_price=r[4])

@app.get("/api/max/next")
def max_next(after_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, start_id, end_id, start_price, end_price FROM max WHERE id>%s ORDER BY id ASC LIMIT 1", (after_id,))
        r = cur.fetchone()
        if not r: raise HTTPException(404, "no next")
        return dict(id=r[0], start_id=r[1], end_id=r[2], start_price=r[3], end_price=r[4])

@app.get("/api/ticks/last_id")
def ticks_last_id():
    with get_conn() as conn:
        cur = conn.cursor(); cur.execute("SELECT max(id) FROM ticks")
        return {"last_id": cur.fetchone()[0]}



@app.get("/api/zigzag")
def api_zz(
    from_id: int = Query(..., alias="from_id"),
    to_id: int = Query(..., alias="to_id"),
    kind: Optional[str] = Query(None, pattern="^(min|mid|max)$")  # optional filter
):
    conn = get_conn()
    with dict_cur(conn) as cur:
        if kind in ("min", "mid", "max"):
            cur.execute(
                f"""
                SELECT start_id, end_id, start_ts, end_ts,
                       start_price::float AS start_price,
                       end_price::float   AS end_price,
                       dir,
                       span::float        AS span,
                       len,
                       '{kind}'           AS kind
                FROM {kind}
                WHERE NOT (end_id < %s OR start_id > %s)
                ORDER BY start_id
                """,
                (from_id, to_id),
            )
            rows = cur.fetchall()
        else:
            cur.execute(
                """
                SELECT start_id, end_id, start_ts, end_ts,
                       start_price::float, end_price::float,
                       dir, span::float, len, 'min' AS kind
                  FROM min WHERE NOT (end_id < %s OR start_id > %s)
                UNION ALL
                SELECT start_id, end_id, start_ts, end_ts,
                       start_price::float, end_price::float,
                       dir, span::float, len, 'mid' AS kind
                  FROM mid WHERE NOT (end_id < %s OR start_id > %s)
                UNION ALL
                SELECT start_id, end_id, start_ts, end_ts,
                       start_price::float, end_price::float,
                       dir, span::float, len, 'max' AS kind
                  FROM max WHERE NOT (end_id < %s OR start_id > %s)
                ORDER BY start_id
                """,
                (from_id, to_id, from_id, to_id, from_id, to_id),
            )
            rows = cur.fetchall()
    return rows


@app.post("/api/sql")
def api_sql_post(sql: str = Body("", embed=True)):
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
    x_allow_write: Optional[str] = Header(None)
):
    """
    Execute DDL/DML/PLpgSQL. By default blocked unless:
      - unsafe=true query param AND
      - X-Allow-Write: yes header is present.
    Also supports multi-statement batches separated by semicolons.
    Wraps everything in a transaction; returns per-statement rowcounts.
    """
    stmt = (sql or "").strip()
    if not stmt:
        return {"ok": True, "results": []}

    # Safety gates (you can loosen for your dev box)
    if not unsafe or (x_allow_write or "").lower() != "yes":
        raise HTTPException(403, detail="Write access disabled. Use unsafe=true and X-Allow-Write: yes")

    results = []
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn, dict_cur(conn) as cur:
            # crude splitter; Postgres also accepts DO $$...$$; this keeps $$ blocks intact
            import sqlparse
            for part in [p.strip() for p in sqlparse.split(stmt) if p.strip()]:
                cur.execute(part)
                if cur.description:
                    rows = cur.fetchall()
                    results.append({"type":"resultset","rows":_jsonable(rows)})
                else:
                    results.append({"type":"rowcount","rowcount": cur.rowcount})
        return {"ok": True, "results": results}
    except Exception as e:
        conn.rollback()
        raise HTTPException(400, detail=f"{type(e).__name__}: {e}")

# ----------------------- Tick data (kept/compat) ----------------------
# --- ADD: last-N ticks for live bootstrap (safe alias around /ticks/recent) ---
@app.get("/api/ticks/latestN")
def api_ticks_latestN(limit: int = 10000):
    limit = max(1, min(limit, 20000))
    return ticks_recent(limit)

# --- ADD: incremental ticks strictly AFTER a given id (for live polling) ---
@app.get("/api/ticks/after")
def api_ticks_after(since_id: int, limit: int = 5000):
    limit = max(1, min(limit, 20000))
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
            FROM ticks
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (since_id, limit),
        )
        rows = cur.fetchall()
    # normalize
    for r in rows:
        if isinstance(r.get("mid"), Decimal): r["mid"] = float(r["mid"])
        if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
        if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
        r["spread"] = (
            (r.get("ask") - r.get("bid"))
            if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None)
            else None
        )
        r["ts"] = r["ts"].isoformat()
    return rows

# --- ADD: generic label fetcher for min/mid/max as per-tick lines ---
def _table_exists(conn, name: str) -> bool:
    with dict_cur(conn) as cur:
        cur.execute(
            "select 1 from information_schema.tables where table_schema='public' and table_name=%s",
            (name,),
        )
        return cur.fetchone() is not None

@app.get("/api/labels/{name}/range")
def api_labels_range(
    name: str,
    start_id: int = Query(...),
    limit: int = Query(20000, ge=1, le=20000),
):
    """
    Returns rows for label tables (min|mid|max) as [{id, ts, value}].
    - Tolerates schema differences:
        * id column can be 'id' or 'tick_id'
        * value column can be 'value' or 'price' (or 'mid' as fallback)
        * timestamp column is auto-detected with detect_ts_col()
    - Returns [] if table doesn't exist.
    """
    name = name.lower()
    if name not in ("min", "mid", "max"):
        raise HTTPException(400, "name must be one of: min, mid, max")

    conn = get_conn()
    if not _table_exists(conn, name):
        return []  # gracefully handle empty/missing label tables

    ts_col, _, _ = _ts_mid_cols(conn)

    # Probe columns
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            """,
            (name,),
        )
        cols = {r["column_name"] for r in cur.fetchall()}
        id_col = (
            "tick_id" if "tick_id" in cols else
            ("start_id" if "start_id" in cols else
            ("id" if "id" in cols else None))
        )
        if not id_col:
            return []


    id_col = "tick_id" if "tick_id" in cols else ("id" if "id" in cols else None)
    if not id_col:
        # best-effort: bail out safely
        return []

    # value candidates in priority order
    val_col = (
        "value" if "value" in cols else
        ("start_price" if "start_price" in cols else
        ("price" if "price" in cols else
        ("mid" if "mid" in cols else None)))
    )
    if not val_col:
        return []


    # If label table lacks timestamp, we’ll derive ts by joining ticks
    has_ts = ts_col in cols

    with dict_cur(conn) as cur:
        if has_ts:
            cur.execute(
                f"""
                SELECT {id_col} AS id, {ts_col} AS ts, {val_col} AS value
                FROM {name}
                WHERE {id_col} >= %s
                ORDER BY {id_col} ASC
                LIMIT %s
                """,
                (start_id, limit),
            )
        else:
            # join ticks to get ts (safe if ticks is big; indexed on id)
            cur.execute(
                f"""
                SELECT t.id, t.{ts_col} AS ts, lb.{val_col} AS value
                FROM {name} lb
                JOIN ticks t ON t.id = lb.{id_col}
                WHERE lb.{id_col} >= %s
                ORDER BY lb.{id_col} ASC
                LIMIT %s
                """,
                (start_id, limit),
            )
        rows = cur.fetchall()

    # normalize
    for r in rows:
        if isinstance(r.get("value"), Decimal):
            r["value"] = float(r["value"])
        if isinstance(r.get("ts"), (datetime, date)):
            r["ts"] = r["ts"].isoformat()
    return rows

@app.get("/api/labels/{name}/prev")
def api_labels_prev(name: str, before_id: int = Query(...)):
    """
    Return the single most recent label row at or before `before_id`.
    Schema-tolerant over id_col (tick_id|start_id|id) and value col (value|start_price|price|mid).
    """
    name = name.lower()
    if name not in ("min", "mid", "max"):
        raise HTTPException(400, "name must be one of: min, mid, max")

    conn = get_conn()
    if not _table_exists(conn, name):
        return {}

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            """,
            (name,),
        )
        cols = {r["column_name"] for r in cur.fetchall()}

    id_col = (
        "tick_id" if "tick_id" in cols else
        ("start_id" if "start_id" in cols else
         ("id" if "id" in cols else None))
    )
    if not id_col:
        return {}

    val_col = (
        "value" if "value" in cols else
        ("start_price" if "start_price" in cols else
         ("price" if "price" in cols else
          ("mid" if "mid" in cols else None)))
    )
    if not val_col:
        return {}

    ts_col, _, _ = _ts_mid_cols(conn)
    has_ts = ts_col in cols

    with dict_cur(conn) as cur:
        if has_ts:
            cur.execute(
                f"""
                SELECT {id_col} AS id, {ts_col} AS ts, {val_col} AS value
                FROM {name}
                WHERE {id_col} <= %s
                ORDER BY {id_col} DESC
                LIMIT 1
                """,
                (before_id,),
            )
        else:
            cur.execute(
                f"""
                SELECT t.id, t.{ts_col} AS ts, lb.{val_col} AS value
                FROM {name} lb
                JOIN ticks t ON t.id = lb.{id_col}
                WHERE lb.{id_col} <= %s
                ORDER BY lb.{id_col} DESC
                LIMIT 1
                """,
                (before_id,),
            )
        row = cur.fetchone() or {}

    if row:
        if isinstance(row.get("value"), Decimal): row["value"] = float(row["value"])
        row["ts"] = row["ts"].isoformat() if isinstance(row.get("ts"), (datetime, date)) else row.get("ts")
    return row


@app.get("/ticks/lastid")
def ticks_lastid():
    conn = get_conn()
    ts_col, _, _ = _ts_mid_cols(conn)
    with dict_cur(conn) as cur:
        cur.execute("SELECT MAX(id) AS last_id FROM ticks")
        last_id = int(cur.fetchone()["last_id"] or 0)
        cur.execute(f"SELECT {ts_col} AS ts FROM ticks WHERE id=%s", (last_id,))
        r = cur.fetchone()
        ts = r["ts"].isoformat() if r and r["ts"] else None
        return {"lastId": last_id, "timestamp": ts}

@app.get("/ticks/recent")
def ticks_recent(limit: int = 2200):
    limit = max(1, min(limit, 10000))
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
            FROM ticks
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = list(reversed(cur.fetchall()))
    # normalize
    for r in rows:
        if isinstance(r.get("mid"), Decimal): r["mid"] = float(r["mid"])
        if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
        if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
        r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
        r["ts"] = r["ts"].isoformat()
    return rows

# ADD THIS near other helpers (after _ts_mid_cols is a good place)

def _ticks_has_kal(conn) -> bool:
    """
    Returns True if ticks table has a 'kal' column, False otherwise.
    """
    with dict_cur(conn) as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = 'ticks'
              AND column_name  = 'kal'
        """)
        return cur.fetchone() is not None

@app.get("/ticks/before/{tickid}")
def ticks_before(tickid: int, limit: int = 2000):
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
            FROM ticks
            WHERE id <= %s
            ORDER BY id DESC
            LIMIT %s
            """,
            (tickid, limit),
        )
        rows = list(reversed(cur.fetchall()))
    for r in rows:
        if isinstance(r.get("mid"), Decimal): r["mid"] = float(r["mid"])
        if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
        if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
        r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
        r["ts"] = r["ts"].isoformat()
    return rows

@app.get("/ticks/range")
def ticks_range(start: int, end: int, limit: int = 200000):
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
            FROM ticks
            WHERE id BETWEEN %s AND %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (start, end, limit),
        )
        rows = cur.fetchall()
    for r in rows:
        if isinstance(r.get("mid"), Decimal): r["mid"] = float(r["mid"])
        if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
        if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
        r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
        r["ts"] = r["ts"].isoformat()
    return rows

# convenience alias used by some charts
@app.get("/api/ticks")
def api_ticks(from_id: int, to_id: int):
    return ticks_range(from_id, to_id, 200000)

# --- HELPERS: tolerant column detection for label tables (min/mid/max) ---
def _label_cols(conn, table: str):
    with dict_cur(conn) as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
        """, (table,))
        cols = {r["column_name"] for r in cur.fetchall()}

    id_col = (
        "id" if "id" in cols else
        ("tick_id" if "tick_id" in cols else
         ("start_id" if "start_id" in cols else None))
    )
    start_id_col = "start_id" if "start_id" in cols else (id_col if id_col != "id" else None)
    end_id_col = (
        "end_id" if "end_id" in cols else
        ("stop_id" if "stop_id" in cols else None)
    )
    price_col = (
        "value" if "value" in cols else
        ("start_price" if "start_price" in cols else
         ("price" if "price" in cols else
          ("mid" if "mid" in cols else None)))
    )
    end_price_col = (
        "end_price" if "end_price" in cols else
        ("stop_price" if "stop_price" in cols else None)
    )

    ts_col, _, _ = _ts_mid_cols(conn)
    return {
        "id_col": id_col,
        "start_id_col": start_id_col,
        "end_id_col": end_id_col,
        "price_col": price_col,
        "end_price_col": end_price_col,
        "ts_col": ts_col,
        "has_ts": ts_col in cols,
    }

def _ticks_last_id(conn):
    with dict_cur(conn) as cur:
        cur.execute("SELECT max(id) AS last FROM ticks")
        r = cur.fetchone()
    return r["last"] or 0

def _row_to_float(v):
    return float(v) if isinstance(v, Decimal) else v

def _resolve_max_segment(conn, row, cols):
    """Given a 'max' row, compute a normalized segment {id,start_id,end_id,...}."""
    id_col = cols["id_col"]
    sid = row.get(cols["start_id_col"]) if cols["start_id_col"] else None
    if sid is None:
        # fall back: start_id equals row's id/tick_id
        sid = row.get(id_col)
    eid = row.get(cols["end_id_col"]) if cols["end_id_col"] else None
    price = row.get(cols["price_col"])
    eprice = row.get(cols["end_price_col"]) if cols["end_price_col"] else None

    # If end_id is missing, derive from next row or latest tick
    if eid is None:
        with dict_cur(conn) as cur:
            cur.execute(
                f"SELECT {cols['start_id_col'] or id_col} AS sid_next "
                f"FROM max WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT 1",
                (row[id_col],)
            )
            rn = cur.fetchone()
        if rn and rn.get("sid_next"):
            eid = rn["sid_next"] - 1
        else:
            eid = _ticks_last_id(conn)

    # Add timestamps from ticks
    ts_col = cols["ts_col"]
    with dict_cur(conn) as cur:
        cur.execute(f"SELECT {ts_col} AS ts FROM ticks WHERE id=%s", (sid,))
        srow = cur.fetchone()
        cur.execute(f"SELECT {ts_col} AS ts FROM ticks WHERE id=%s", (eid,))
        erow = cur.fetchone()

    return {
        "id": row[id_col],
        "start_id": sid,
        "end_id": eid,
        "start_price": _row_to_float(price),
        "end_price": _row_to_float(eprice) if eprice is not None else None,
        "start_ts": srow["ts"].isoformat() if srow and srow.get("ts") else None,
        "end_ts": erow["ts"].isoformat() if erow and erow.get("ts") else None,
    }

# --- NEW: get last max segment ---
@app.get("/api/maxline/last")
def api_maxline_last():
    conn = get_conn()
    if not _table_exists(conn, "max"):
        return {}
    cols = _label_cols(conn, "max")
    id_col = cols["id_col"]
    if not id_col:
        return {}
    with dict_cur(conn) as cur:
        cur.execute(f"SELECT * FROM max ORDER BY {id_col} DESC LIMIT 1")
        row = cur.fetchone()
    if not row:
        return {}
    return _resolve_max_segment(conn, row, cols)

# --- NEW: get max segment by its row id (not tick id) ---
@app.get("/api/maxline/by_id")
def api_maxline_by_id(id: int = Query(...)):
    conn = get_conn()
    if not _table_exists(conn, "max"):
        raise HTTPException(404, "max table not found")
    cols = _label_cols(conn, "max")
    id_col = cols["id_col"]
    if not id_col:
        raise HTTPException(400, "cannot detect id column on max")
    with dict_cur(conn) as cur:
        cur.execute(f"SELECT * FROM max WHERE {id_col}=%s", (id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"max id {id} not found")
    return _resolve_max_segment(conn, row, cols)

# --- NEW: next max row id after a given row id (for 'Load More') ---
@app.get("/api/maxline/next")
def api_maxline_next(after_id: int = Query(...)):
    conn = get_conn()
    if not _table_exists(conn, "max"):
        return {}
    cols = _label_cols(conn, "max")
    id_col = cols["id_col"]
    if not id_col:
        return {}
    with dict_cur(conn) as cur:
        cur.execute(
            f"SELECT {id_col} AS id FROM max WHERE {id_col} > %s ORDER BY {id_col} ASC LIMIT 1",
            (after_id,)
        )
        row = cur.fetchone()
    return {"id": row["id"]} if row else {}



# -------------------- Review: segments & per-segment ------------------

# list of segments for the left-side table
@app.get("/api/segms")
def api_segms(limit: int = 200):
    """
    Return a lightweight list of segments for the left pane.
    - Always returns ratio, but uses 0.0 as a neutral default (no fragile join).
    - Counts preds only if table exists; otherwise 0.
    """
    limit = max(1, min(limit, 2000))
    conn = get_conn()

    def _table_exists(name: str) -> bool:
        with dict_cur(conn) as cur:
            cur.execute(
                "select 1 from information_schema.tables "
                "where table_schema='public' and table_name=%s", (name,)
            )
            return cur.fetchone() is not None

    has_pred = _table_exists("pred")

    with dict_cur(conn) as cur:
        if has_pred:
            cur.execute(
                """
                SELECT s.id,
                       s.start_ts,
                       s.end_ts,
                       EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))::int AS dur_s,
                       COALESCE(p.cnt, 0) AS preds,
                       0.0::float AS ratio,     -- safe default
                       s.dir
                FROM segm s
                LEFT JOIN (
                   SELECT segm_id, COUNT(*) AS cnt
                   FROM pred
                   GROUP BY segm_id
                ) p ON p.segm_id = s.id
                ORDER BY s.id ASC
                LIMIT %s
                """,
                (limit,),
            )
        else:
            cur.execute(
                """
                SELECT s.id,
                       s.start_ts,
                       s.end_ts,
                       EXTRACT(EPOCH FROM (s.end_ts - s.start_ts))::int AS dur_s,
                       0::int AS preds,
                       0.0::float AS ratio,
                       s.dir
                FROM segm s
                ORDER BY s.id ASC
                LIMIT %s
                """,
                (limit,),
            )

        rows = cur.fetchall()

    for r in rows:
        r["start_ts"] = r["start_ts"].isoformat()
        r["end_ts"]   = r["end_ts"].isoformat()
        if isinstance(r.get("ratio"), Decimal):
            r["ratio"] = float(r["ratio"])
    return rows

# --- ADD: paged ticks for a segment (load from beginning in chunks) ---
@app.get("/api/segm/ticks")
def api_segm_ticks(id: int = Query(...), from_: Optional[int] = Query(None, alias="from"), limit: int = 2000):
    """
    Return up to {limit} ticks for segm id, starting at 'from' (tick id) going forward.
    If 'from' is omitted, starts from the segment's start_id.
    """
    limit = max(100, min(limit, 20000))
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""

    with dict_cur(conn) as cur:
        cur.execute("SELECT start_id, end_id FROM segm WHERE id=%s", (id,))
        sg = cur.fetchone()
        if not sg:
            raise HTTPException(404, "segm not found")
        start_id, end_id = int(sg["start_id"]), int(sg["end_id"])
        from_id = int(from_ or start_id)

        cur.execute(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
            FROM ticks
            WHERE id BETWEEN %s AND %s AND id >= %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (start_id, end_id, from_id, limit),
        )
        rows = cur.fetchall()
        for r in rows:
            if isinstance(r.get("mid"), Decimal): r["mid"] = float(r["mid"])
            if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
            if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
            r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
            r["ts"] = r["ts"].isoformat()
        return rows

# --- ADD: generic per-segment layer fetch for arbitrary tables ---------
@app.get("/api/segm/layers")
def api_segm_layers(id: int = Query(...), tables: str = Query(...)):
    """
    Fetch rows from requested tables for a given segment id.
    Heuristics:
      - If table has 'segm_id': filter WHERE segm_id = id
      - Else if it has start_id/end_id: filter by overlapping [start_id,end_id]
      - Else if it has timestamp columns a_ts/b_ts/start_ts/end_ts: filter by ts range
    Returns {'layers': {table: [rows...]}}
    """
    names = [t.strip() for t in (tables or "").split(",") if t.strip()]
    if not names:
        return {"layers": {}}

    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute("SELECT start_id, end_id, start_ts, end_ts FROM segm WHERE id=%s", (id,))
        sg = cur.fetchone()
        if not sg:
            raise HTTPException(404, "segm not found")

        def table_info(name: str):
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
            """, (name,))
            return [r["column_name"] for r in cur.fetchall()]

        layers = {}
        for t in names:
            cols = table_info(t)
            if not cols:
                layers[t] = []  # silently skip unknown
                continue

            # choose filter
            if "segm_id" in cols:
                q = f"SELECT * FROM {t} WHERE segm_id=%s ORDER BY 1"
                cur.execute(q, (id,))
            elif "start_id" in cols and "end_id" in cols:
                q = f"""
                    SELECT * FROM {t}
                    WHERE NOT (end_id < %s OR start_id > %s)
                    ORDER BY 1
                """
                cur.execute(q, (sg["start_id"], sg["end_id"]))
            elif any(c in cols for c in ("a_ts","b_ts","start_ts","end_ts")):
                q = f"""
                    SELECT * FROM {t}
                    WHERE COALESCE(start_ts, a_ts, b_ts, end_ts) BETWEEN %s AND %s
                    ORDER BY 1
                """
                cur.execute(q, (sg["start_ts"], sg["end_ts"]))
            else:
                # last resort: return empty to avoid dumping huge unrelated tables
                layers[t] = []
                continue

            rows = cur.fetchall()
            # best-effort JSON normalization of common numeric/time columns
            for r in rows:
                for k,v in list(r.items()):
                    if isinstance(v, Decimal): r[k] = float(v)
                    elif isinstance(v, (datetime, date)): r[k] = v.isoformat()
            layers[t] = rows

        return {"layers": layers}


# full data for a single segment id
@app.get("/api/segm")
def api_segm(id: int = Query(...)):
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""

    with dict_cur(conn) as cur:
        cur.execute("SELECT * FROM segm WHERE id=%s", (id,))
        seg = cur.fetchone()
        if not seg:
            return JSONResponse({"detail": "not found"}, status_code=404)

        # segment ticks
        cur.execute(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
            FROM ticks
            WHERE id BETWEEN %s AND %s
            ORDER BY id ASC
            """,
            (seg["start_id"], seg["end_id"]),
        )
        ticks = cur.fetchall()
        for r in ticks:
            if isinstance(r.get("mid"), Decimal): r["mid"] = float(r["mid"])
            if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
            if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
            r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
            r["ts"] = r["ts"].isoformat()

        # small moves
        cur.execute("SELECT * FROM smal WHERE segm_id=%s ORDER BY id ASC", (id,))
        sm = cur.fetchall()
        for r in sm:
            if r.get("a_ts"): r["a_ts"] = r["a_ts"].isoformat()
            if r.get("b_ts"): r["b_ts"] = r["b_ts"].isoformat()
            if isinstance(r.get("move"), Decimal): r["move"] = float(r["move"])

        # big moves
        cur.execute("SELECT * FROM bigm WHERE segm_id=%s ORDER BY id ASC", (id,))
        bm = cur.fetchall()
        for r in bm:
            if r.get("a_ts"): r["a_ts"] = r["a_ts"].isoformat()
            if r.get("b_ts"): r["b_ts"] = r["b_ts"].isoformat()
            if isinstance(r.get("move"), Decimal): r["move"] = float(r["move"])

        # levels
        cur.execute("SELECT * FROM level WHERE segm_id=%s ORDER BY id ASC", (id,))
        lv = cur.fetchall()
        for r in lv:
            if r.get("ts"): r["ts"] = r["ts"].isoformat()
            if r.get("used_at_ts"): r["used_at_ts"] = r["used_at_ts"].isoformat()
            if isinstance(r.get("price"), Decimal): r["price"] = float(r["price"])

        # predictions
        cur.execute("SELECT * FROM pred WHERE segm_id=%s ORDER BY id ASC", (id,))
        pd = cur.fetchall()
        for r in pd:
            if r.get("at_ts"): r["at_ts"] = r["at_ts"].isoformat()
            if r.get("resolved_at_ts"): r["resolved_at_ts"] = r["resolved_at_ts"].isoformat()
            if isinstance(r.get("goal_usd"), Decimal): r["goal_usd"] = float(r["goal_usd"])

    return {"segm": _jsonable(seg), "ticks": ticks, "smal": sm, "bigm": bm, "level": lv, "pred": pd}

# --- recent segments for review sidebar ---
@app.get("/api/segm/recent")
def api_segm_recent(limit: int = 200):
    q = """
    SELECT id, start_id, end_id, start_ts, end_ts, dir, span, len
    FROM segm
    ORDER BY id DESC
    LIMIT %s
    """
    with get_conn() as conn:  # use your existing conn helper
        with conn.cursor() as cur:
            cur.execute(q, (limit,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    # return as list of dicts
    return [dict(zip(cols, r)) for r in rows]



# ----------------------------- Live SSE ------------------------------

@app.get("/api/live")
def api_live():
    """Server-sent events for live ticks & predictions."""
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""

    def gen():
        last_tick_id = scalar(conn, "SELECT COALESCE(MAX(id),0) FROM ticks") or 0
        last_pred_id = scalar(conn, "SELECT COALESCE(MAX(id),0) FROM pred") or 0
        yield "event: hello\ndata: {}\n\n"

        import time as _t
        while True:
            # ticks
            with dict_cur(conn) as cur:
                cur.execute(
                    f"""
                    SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
                    FROM ticks
                    WHERE id > %s
                    ORDER BY id ASC
                    """,
                    (last_tick_id,),
                )
                for r in cur.fetchall():
                    last_tick_id = int(r["id"])
                    d = {
                        "type": "tick",
                        "id": last_tick_id,
                        "ts": r["ts"].isoformat(),
                        "mid": float(r["mid"]) if isinstance(r["mid"], Decimal) else r["mid"],
                    }
                    if has_bid: d["bid"] = float(r["bid"]) if isinstance(r["bid"], Decimal) else r["bid"]
                    if has_ask: d["ask"] = float(r["ask"]) if isinstance(r["ask"], Decimal) else r["ask"]
                    if has_bid and has_ask and d.get("ask") is not None and d.get("bid") is not None:
                        d["spread"] = d["ask"] - d["bid"]
                    yield f"event: tick\ndata: {json.dumps(_jsonable(d))}\n\n"

            # preds
            with dict_cur(conn) as cur:
                cur.execute("SELECT * FROM pred WHERE id > %s ORDER BY id ASC", (last_pred_id,))
                for p in cur.fetchall():
                    last_pred_id = int(p["id"])
                    yield f"event: pred\ndata: {json.dumps(_jsonable({'type':'pred', **p}))}\n\n"

            yield "event: ping\ndata: {}\n\n"
            _t.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ------------------------- Runner convenience -------------------------

from backend.runner import Runner

@app.post("/api/run")
def api_run():
    """Run the segment-by-segment pipeline until now."""
    return Runner().run_until_now()


# ------------------------------ Extras --------------------------------

@app.get("/movements")
def movements_page():
    return HTMLResponse("<h3>Movements Page</h3><p>Use /frontend/review.html for charts.</p>")
