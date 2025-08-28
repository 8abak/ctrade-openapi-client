# PATH: backend/main.py
import os
import json
import math
import asyncio
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles

#----------------------------------------------------
# fixing decimal import
from decimal import Decimal

from .db import (
    q_dicts,
    exec_sql,
    tick_sql_fields,
    tick_ts_col,
    tick_mid_expr,
    last_tick_id,
)
from .runner import run_until_now

# ----------------------------------------------------
# App & CORS
# ----------------------------------------------------
app = FastAPI(title="cTrade backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def _ticks_select_base() -> str:
    return f"SELECT {tick_sql_fields()} FROM ticks"

def _normalize_tick_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Ensure both 'ts' and 'timestamp' keys for backward compatibility
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        if "ts" not in d and "timestamp" in d:
            d["ts"] = d["timestamp"]
        if "timestamp" not in d and "ts" in d:
            d["timestamp"] = d["ts"]
        # ensure floats for json
        if isinstance(d.get("mid"), (int, float)):
            pass
        else:
            try:
                d["mid"] = float(d["mid"])
            except Exception:
                pass
        out.append(d)
    return out

# ----------------------------------------------------
# Root
# ----------------------------------------------------
@app.get("/")
def home():
    return {"message": "API live. Try /ticks/recent, /api/outcome, /sqlvw/tables, /version"}

# ----------------------------------------------------
# SQL endpoints (kept compatible)
# ----------------------------------------------------
@app.post("/api/sql")
def sql_post(sql: str = Body(..., embed=True)):
    try:
        rows = q_dicts(sql)
        return {"rows": rows}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/sqlvw/tables")
def get_all_table_names():
    rows = q_dicts("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE'
        ORDER BY table_name
    """)
    return [r["table_name"] for r in rows]

@app.get("/sqlvw/query")
def run_sql_query(query: str = Query(...)):
    try:
        # If returns rows, return them; else return message
        rows = q_dicts(query)
        return rows
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# ----------------------------------------------------
# Legacy label discovery endpoints (kept; may be empty)
# ----------------------------------------------------
@app.get("/api/labels/available")
def get_label_tables():
    rows = q_dicts("""
        SELECT table_name
        FROM information_schema.columns
        WHERE column_name ILIKE 'tickid' AND table_schema='public'
    """)
    return sorted({r["table_name"] for r in rows})

@app.get("/api/labels/schema")
def labels_schema():
    q = """
    SELECT c.table_name, c.column_name
    FROM information_schema.columns c
    JOIN information_schema.columns k
      ON k.table_name = c.table_name
     AND k.column_name ILIKE 'tickid'
    WHERE c.table_schema='public'
    ORDER BY c.table_name, c.ordinal_position
    """
    out: Dict[str, Dict[str, Any]] = {}
    for row in q_dicts(q):
        tname = row["table_name"]
        cname = row["column_name"]
        if tname not in out:
            out[tname] = {"table": tname, "labels": []}
        low = cname.lower()
        if low not in ("id", "tickid") and not low.startswith("ts"):
            out[tname]["labels"].append(cname)
    return [v for v in out.values() if v["labels"]]

# ----------------------------------------------------
# Version
# ----------------------------------------------------
@app.get("/version")
def get_version():
    return {"version": "2025.08.28.price-action-segments"}

# ----------------------------------------------------
# Ticks (compatible shapes)
# ----------------------------------------------------
@app.get("/ticks/recent")
def get_recent_ticks(limit: int = Query(2200, le=10000)):
    rows = q_dicts(
        f"""
        SELECT * FROM (
           {_ticks_select_base()}
           ORDER BY id DESC
           LIMIT %s
        ) sub
        ORDER BY id ASC
        """,
        (limit,),
    )
    return _normalize_tick_rows(rows)

@app.get("/ticks/lastid")
def get_lastid():
    lid = last_tick_id()
    if lid is None:
        raise HTTPException(status_code=404, detail="No ticks")
    rows = q_dicts(f"SELECT id, {tick_ts_col()} as ts FROM ticks WHERE id=%s", (lid,))
    return {"lastId": lid, "timestamp": rows[0]["ts"] if rows else None}

@app.get("/ticks/before/{tickid}")
def get_ticks_before(tickid: int, limit: int = 2000):
    rows = q_dicts(
        f"""
        SELECT * FROM (
            {_ticks_select_base()}
            WHERE id < %s
            ORDER BY id DESC
            LIMIT %s
        ) x
        ORDER BY id ASC
        """,
        (tickid, limit),
    )
    return _normalize_tick_rows(rows)

@app.get("/ticks/range")
def ticks_range(start: int, end: int, limit: int = 200000):
    rows = q_dicts(
        f"""
        SELECT {_ticks_select_base().split('FROM')[0]}FROM ticks
        WHERE id BETWEEN %s AND %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (start, end, limit),
    )
    return _normalize_tick_rows(rows)

# New generic endpoint used by frontend
@app.get("/api/ticks")
def api_ticks(from_id: int = Query(...), to_id: int = Query(...)):
    rows = q_dicts(
        f"""
        {_ticks_select_base()}
        WHERE id BETWEEN %s AND %s
        ORDER BY id ASC
        """,
        (from_id, to_id),
    )
    return _normalize_tick_rows(rows)

# ----------------------------------------------------
# New: Price-Action Segments API
# ----------------------------------------------------
@app.post("/run")
def api_run():
    """
    Runs the pipeline from stat.last_done_tick_id+1 until now, segment-by-segment.
    Returns summary: {segments, from_tick, to_tick}
    """
    try:
        res = run_until_now()
        return res
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/outcome")
def api_outcome(limit: int = 50):
    rows = q_dicts(
        """
        SELECT o.id, o.time, o.duration, o.predictions, o.ratio,
               s.id AS segm_id, s.start_id, s.end_id, s.start_ts, s.end_ts, s.dir, s.span, s.len
        FROM outcome o
        JOIN segm s ON s.id = o.segm_id
        ORDER BY o.id DESC
        LIMIT %s
        """,
        (limit,),
    )
    return rows

@app.get("/segm")
def api_segment(id: int):
    seg = q_dicts("SELECT * FROM segm WHERE id=%s", (id,))
    if not seg:
        raise HTTPException(status_code=404, detail="segment not found")
    s = seg[0]
    ticks = q_dicts(
        f"""
        SELECT {tick_sql_fields()}
        FROM ticks
        WHERE id BETWEEN %s AND %s
        ORDER BY id ASC
        """,
        (s["start_id"], s["end_id"]),
    )
    ticks = _normalize_tick_rows(ticks)
    smals = q_dicts("SELECT * FROM smal WHERE segm_id=%s ORDER BY id ASC", (id,))
    preds = q_dicts("SELECT * FROM pred WHERE segm_id=%s ORDER BY id ASC", (id,))
    return {"segm": s, "ticks": ticks, "smal": smals, "pred": preds}

# ----------------------------------------------------
# SSE Live: ticks + pred updates (minimal)
# ----------------------------------------------------
def _to_jsonable(o):
    """Recursively convert Decimals to float and datetimes to ISO strings."""
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, dict):
        return {k: _to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_to_jsonable(v) for v in o]
    return o


async def _sse_generator():
    # Track last sent tick id and last sent resolved pred id
    last_sent_tick = last_tick_id() or 0
    last_pred_id = 0
    while True:
        try:
            # New ticks
            rows = q_dicts(
                f"""
                SELECT {tick_sql_fields()}
                FROM ticks
                WHERE id > %s
                ORDER BY id ASC
                LIMIT 2000
                """,
                (last_sent_tick,),
            )
            for r in rows:
                payload = {"type": "tick", "id": int(r["id"]), "ts": r["ts"].isoformat(), "mid": float(r["mid"])}
                yield f"event: tick\ndata: {json.dumps(_to_jsonable(payload))}\n\n"
                last_sent_tick = int(r["id"])

            # Recently resolved predictions (tail segment)
            preds = q_dicts(
                """
                SELECT id, segm_id, at_id, at_ts, dir, goal_usd, hit, resolved_at_id, resolved_at_ts
                FROM pred
                WHERE id > %s AND resolved_at_id IS NOT NULL
                ORDER BY id ASC
                LIMIT 1000
                """,
                (last_pred_id,),
            )
            for p in preds:
                pdata = dict(p)
                if isinstance(pdata.get("at_ts"), datetime):
                    pdata["at_ts"] = pdata["at_ts"].isoformat()
                if isinstance(pdata.get("resolved_at_ts"), datetime):
                    pdata["resolved_at_ts"] = pdata["resolved_at_ts"].isoformat()

                for k in ("id","segm_id","at_id","resolved_at_id"):
                    if k in pdata and pdata[k] is not None:
                        pdata[k] = int(pdata[k])
                if "goal_usd" in pdata and pdata["goal_usd"] is not None:
                    pdata["goal_usd"] = float(pdata["goal_usd"])


                yield f"event: pred\ndata: {json.dumps(_to_jsonable({'type':'pred', **pdata}))}\n\n"
                last_pred_id = max(last_pred_id, int(p["id"]))

            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            # Send error and keep trying
            yield f"event: error\ndata: {json.dumps({'message': str(e)})}\n\n"
            await asyncio.sleep(2.0)

@app.get("/api/live")
def api_live():
    return StreamingResponse(_sse_generator(), media_type="text/event-stream")

# ----------------------------------------------------
# Walk-forward compatibility shims
# ----------------------------------------------------
@app.post("/walkforward/step")
def walkforward_step():
    # Run at most one segment to mimic a 'step'
    res = run_until_now(max_segments=1)
    return {"ok": True, "journal": [f"processed {res['segments']} segment(s)"], "segments": res["segments"]}

@app.get("/walkforward/snapshot")
def walkforward_snapshot():
    # Provide a snapshot-like view from new schema
    segs = q_dicts("SELECT * FROM segm ORDER BY id DESC LIMIT 200")
    seg_ids = [s["id"] for s in segs]
    events = q_dicts("SELECT * FROM smal WHERE segm_id = ANY(%s) ORDER BY id ASC", (seg_ids,)) if seg_ids else []
    outcomes = q_dicts("SELECT * FROM outcome WHERE segm_id = ANY(%s)", (seg_ids,)) if seg_ids else []
    preds = q_dicts(
        """
        SELECT * FROM (
          SELECT *, ROW_NUMBER() OVER(PARTITION BY segm_id ORDER BY id DESC) AS rn
          FROM pred
          WHERE segm_id = ANY(%s)
        ) x WHERE rn=1
        """,
        (seg_ids,),
    ) if seg_ids else []
    return {"segments": segs, "events": events, "outcomes": outcomes, "predictions": preds}

# ----------------------------------------------------
# Static (preserve prior behavior)
# ----------------------------------------------------
public_dir = os.path.join(os.path.dirname(__file__), "..", "public")
if os.path.isdir(public_dir):
    app.mount("/public", StaticFiles(directory=public_dir, html=True), name="public")

@app.get("/movements")
def movements_page():
    file_path = os.path.join(public_dir, "movements.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"message": "movements.html not found."}
