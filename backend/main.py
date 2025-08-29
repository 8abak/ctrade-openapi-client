# PATH: backend/main.py
# FastAPI app: preserves existing endpoints and adds bigm in /api/segm.
import os
import json
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Any, Dict

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import psycopg2
import psycopg2.extras

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr, detect_bid_ask, scalar

app = FastAPI(title="cTrade backend")

# Static files (the Nginx root points here too; mounting is still useful for direct app access)
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET","POST","OPTIONS"],
    allow_headers=["*"],
)

VERSION = "2025.08.29.price-action-segments.bigm.v2"

def _to_jsonable(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, dict):
        return {k: _to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_to_jsonable(v) for v in o]
    return o

def _ts_mid_cols(conn):
    return detect_ts_col(conn), detect_mid_expr(conn), detect_bid_ask(conn)

# ----------------- Basic/legacy endpoints (kept) -----------------
@app.get("/")
def home():
    return {"ok": True}

@app.get("/version")
def get_version():
    return {"version": VERSION}

# SQL passthrough used by SQL UI
@app.get("/sqlvw/tables")
def get_all_table_names():
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute("""
          SELECT tablename FROM pg_tables
          WHERE schemaname='public' AND tablename NOT LIKE 'pg_%'
          ORDER BY tablename
        """)
        tabs = [r["tablename"] for r in cur.fetchall()]
    return tabs

@app.get("/sqlvw/query")
def run_sql_query(query: str):
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(query)
        if cur.description:
            rows = cur.fetchall()
            return _to_jsonable(rows)
        return {"ok": True, "rowcount": cur.rowcount}

# Ticks views (compat)
@app.get("/ticks/lastid")
def get_lastid():
    conn = get_conn()
    ts_col, mid_expr, _ = _ts_mid_cols(conn)
    with dict_cur(conn) as cur:
        cur.execute(f"SELECT MAX(id) AS last_id FROM ticks")
        last_id = int(cur.fetchone()["last_id"] or 0)
        cur.execute(f"SELECT {ts_col} AS ts FROM ticks WHERE id=%s", (last_id,))
        r = cur.fetchone()
        ts = r["ts"].isoformat() if r and r["ts"] else None
    return {"lastId": last_id, "timestamp": ts}

@app.get("/ticks/recent")
def get_recent_ticks(limit: int = 2200):
    limit = max(1, min(limit, 10000))
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute(f"""
          SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
          FROM ticks
          ORDER BY id DESC
          LIMIT %s
        """, (limit,))
        rows = list(reversed(cur.fetchall()))
        for r in rows:
            if isinstance(r["mid"], Decimal): r["mid"] = float(r["mid"])
            if has_bid and isinstance(r["bid"], Decimal): r["bid"] = float(r["bid"])
            if has_ask and isinstance(r["ask"], Decimal): r["ask"] = float(r["ask"])
            r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
            r["ts"] = r["ts"].isoformat()
    return rows

@app.get("/ticks/before/{tickid}")
def get_ticks_before(tickid: int, limit: int = 2000):
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute(f"""
          SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
          FROM ticks
          WHERE id <= %s
          ORDER BY id DESC
          LIMIT %s
        """, (tickid, limit))
        rows = list(reversed(cur.fetchall()))
        for r in rows:
            if isinstance(r["mid"], Decimal): r["mid"] = float(r["mid"])
            if has_bid and isinstance(r["bid"], Decimal): r["bid"] = float(r["bid"])
            if has_ask and isinstance(r["ask"], Decimal): r["ask"] = float(r["ask"])
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
        cur.execute(f"""
          SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
          FROM ticks
          WHERE id BETWEEN %s AND %s
          ORDER BY id ASC
          LIMIT %s
        """, (start, end, limit))
        rows = cur.fetchall()
        for r in rows:
            if isinstance(r["mid"], Decimal): r["mid"] = float(r["mid"])
            if has_bid and isinstance(r["bid"], Decimal): r["bid"] = float(r["bid"])
            if has_ask and isinstance(r["ask"], Decimal): r["ask"] = float(r["ask"])
            r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
            r["ts"] = r["ts"].isoformat()
    return rows

# Compatibility API for charts
@app.get("/api/ticks")
def api_ticks(from_id: int, to_id: int):
    return ticks_range(from_id, to_id, 200000)

# ----------------- New ML endpoints -----------------
from backend.runner import Runner

@app.post("/api/run")
def api_run():
    r = Runner().run_until_now()
    return r

@app.get("/api/outcome")
def api_outcome(limit: int = 50):
    limit = max(1, min(limit, 500))
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute("""
          SELECT o.*, s.start_id, s.end_id, s.start_ts, s.end_ts, s.dir, s.span, s.len
          FROM outcome o
          JOIN segm s ON s.id = o.segm_id
          ORDER BY o.id DESC
          LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        for r in rows:
            r["time"] = r["time"].isoformat()
            r["start_ts"] = r["start_ts"].isoformat()
            r["end_ts"] = r["end_ts"].isoformat()
            if isinstance(r["ratio"], Decimal): r["ratio"] = float(r["ratio"])
            if isinstance(r["span"], Decimal): r["span"] = float(r["span"])
    return rows

@app.get("/api/segm")
def api_segment(id: int):
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    with dict_cur(conn) as cur:
        cur.execute("SELECT * FROM segm WHERE id=%s", (id,))
        seg = cur.fetchone()
        if not seg:
            return JSONResponse({"detail":"not found"}, status_code=404)
        # ticks bounded to the segment
        cur.execute(f"""
          SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
          FROM ticks
          WHERE id BETWEEN %s AND %s
          ORDER BY id ASC
        """, (seg["start_id"], seg["end_id"]))
        ticks = cur.fetchall()
        for r in ticks:
            if isinstance(r["mid"], Decimal): r["mid"] = float(r["mid"])
            if has_bid and isinstance(r.get("bid"), Decimal): r["bid"] = float(r["bid"])
            if has_ask and isinstance(r.get("ask"), Decimal): r["ask"] = float(r["ask"])
            r["spread"] = (r.get("ask") - r.get("bid")) if (has_bid and has_ask and r.get("ask") is not None and r.get("bid") is not None) else None
            r["ts"] = r["ts"].isoformat()

        # small moves
        cur.execute("SELECT * FROM smal WHERE segm_id=%s ORDER BY id ASC", (id,))
        sm = cur.fetchall()
        for r in sm:
            r["a_ts"] = r["a_ts"].isoformat()
            r["b_ts"] = r["b_ts"].isoformat()
            if isinstance(r["move"], Decimal): r["move"] = float(r["move"])

        # big moves
        cur.execute("SELECT * FROM bigm WHERE segm_id=%s ORDER BY id ASC", (id,))
        bm = cur.fetchall()
        for r in bm:
            r["a_ts"] = r["a_ts"].isoformat()
            r["b_ts"] = r["b_ts"].isoformat()
            if isinstance(r["move"], Decimal): r["move"] = float(r["move"])

        # predictions
        cur.execute("SELECT * FROM pred WHERE segm_id=%s ORDER BY id ASC", (id,))
        pd = cur.fetchall()
        for r in pd:
            r["at_ts"] = r["at_ts"].isoformat()
            if r["resolved_at_ts"]:
                r["resolved_at_ts"] = r["resolved_at_ts"].isoformat()
            if isinstance(r.get("goal_usd"), Decimal):
                r["goal_usd"] = float(r["goal_usd"])

    return {"segm": _to_jsonable(seg), "ticks": ticks, "smal": sm, "bigm": bm, "pred": pd}

# SSE live: pushes new ticks and pred updates (tail segment)
@app.get("/api/live")
def api_live():
    conn = get_conn()
    ts_col, mid_expr, (has_bid, has_ask) = _ts_mid_cols(conn)
    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""

    def gen():
        last_tick_id = scalar(conn, "SELECT COALESCE(MAX(id),0) FROM ticks") or 0
        last_pred_id = scalar(conn, "SELECT COALESCE(MAX(id),0) FROM pred") or 0
        yield "event: hello\ndata: {}\n\n"
        while True:
            # ticks
            with dict_cur(conn) as cur:
                cur.execute(f"""
                   SELECT id, {ts_col} AS ts, {mid_expr} AS mid{bid_sel}{ask_sel}
                   FROM ticks
                   WHERE id > %s
                   ORDER BY id ASC
                """, (last_tick_id,))
                for r in cur.fetchall():
                    last_tick_id = int(r["id"])
                    mid = float(r["mid"]) if isinstance(r["mid"], Decimal) else r["mid"]
                    bid = float(r["bid"]) if (has_bid and isinstance(r.get("bid"), Decimal)) else (r.get("bid") if has_bid else None)
                    ask = float(r["ask"]) if (has_ask and isinstance(r.get("ask"), Decimal)) else (r.get("ask") if has_ask else None)
                    spread = (ask - bid) if (ask is not None and bid is not None) else None
                    data = {"type": "tick", "id": last_tick_id, "ts": r["ts"].isoformat(), "mid": mid, "bid": bid, "ask": ask, "spread": spread}
                    yield f"event: tick\ndata: {json.dumps(_to_jsonable(data))}\n\n"
            # preds
            with dict_cur(conn) as cur:
                cur.execute("""
                   SELECT * FROM pred
                   WHERE id > %s
                   ORDER BY id ASC
                """, (last_pred_id,))
                for p in cur.fetchall():
                    last_pred_id = int(p["id"])
                    yield f"event: pred\ndata: {json.dumps(_to_jsonable({'type':'pred', **p}))}\n\n"
            # gentle poll
            yield "event: ping\ndata: {}\n\n"
            import time as _t; _t.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")

# Convenience: step once + snapshot (compat shims)
@app.post("/walkforward/step")
def walkforward_step():
    n = Runner().run_until_now()
    return {"ok": True, **n}

@app.get("/walkforward/snapshot")
def walkforward_snapshot():
    return {"ok": True, "last_done_tick_id": scalar(get_conn(), "SELECT val FROM stat WHERE key='last_done_tick_id'")}

# Simple page (optional)
@app.get("/movements")
def movements_page():
    return HTMLResponse("<html><body><h3>Movements Page</h3><p>Use /frontend/review.html for charts.</p></body></html>")
