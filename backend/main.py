# backend/main.py

import os
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy import text as sqtxt

# Your existing router under /api
from zig_api import router as lview_router

# === FIX: use relative imports (we run as package `backend`) ===
from .label_macro_segments import BuildOrExtendSegments
from .label_micro_events import DetectMicroEventsForLatestClosedSegment
from .compute_outcomes import ResolveOutcomes
from .train_predict import TrainAndPredict

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
app.include_router(lview_router, prefix="/api")

# ----------------------------------------------------
# DB
# ----------------------------------------------------
db_url = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(db_url)

# ----------------------------------------------------
# Models
# ----------------------------------------------------
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def q_all(sql: str, params: Dict[str, Any]):
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sqtxt(sql), params)]

# ----------------------------------------------------
# Root
# ----------------------------------------------------
@app.get("/")
def home():
    return {"message": "API live. Try /ticks/recent, /trends/day, /sqlvw/tables, /version"}

# ----------------------------------------------------
# Ticks
# ----------------------------------------------------
@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                ORDER BY timestamp ASC
                OFFSET :offset LIMIT :limit
            """),
            {"offset": offset, "limit": limit},
        ).mappings().all()
    return list(rows)

@app.post("/api/sql")
def sql_post(sql: str = Body(..., embed=True)):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            if result.returns_rows:
                return {"rows": [dict(r._mapping) for r in result]}
            return {"message": "Query executed successfully."}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(..., description="UTC timestamp in ISO format")):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                WHERE timestamp > :after
                ORDER BY timestamp ASC
                LIMIT 1000
            """),
            {"after": after},
        )
    return [dict(row._mapping) for row in result]

@app.get("/ticks/recent", response_model=List[Tick])
def get_recent_ticks(limit: int = Query(2200, le=5000)):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, timestamp, bid, ask, mid
                FROM (
                    SELECT id, timestamp, bid, ask, mid
                    FROM ticks
                    ORDER BY timestamp DESC
                    LIMIT :limit
                ) sub
                ORDER BY timestamp ASC
            """),
            {"limit": limit},
        ).mappings().all()
    return list(rows)

@app.get("/ticks/before/{tickid}", response_model=List[Tick])
def get_ticks_before(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                WHERE id < :tickid
                ORDER BY timestamp DESC
                LIMIT :limit
            """),
            {"tickid": tickid, "limit": limit},
        ).mappings().all()
    return list(reversed(rows))

@app.get("/ticks/lastid")
def get_lastid():
    with engine.connect() as conn:
        row = conn.execute(
            text("""SELECT id, timestamp FROM ticks ORDER BY id DESC LIMIT 1""")
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="No ticks")
    return {"lastId": row["id"], "timestamp": row["timestamp"]}

@app.get("/ticks/range", response_model=List[Tick])
def ticks_range(start: str, end: str, limit: int = 200000):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                WHERE timestamp >= :start AND timestamp <= :end
                ORDER BY timestamp ASC
                LIMIT :limit
            """),
            {"start": start, "end": end, "limit": limit},
        ).mappings().all()
    return list(rows)

# ----------------------------------------------------
# SQL viewer helpers
# ----------------------------------------------------
@app.get("/sqlvw/tables")
def get_all_table_names():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
        """)).all()
    return [r[0] for r in rows]

@app.get("/sqlvw/query")
def run_sql_query(query: str = Query(...)):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(query))
            if result.returns_rows:
                return [dict(row._mapping) for row in result]
            else:
                return {"message": "Query executed successfully."}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# Labels discovery (kept under /api/*)
@app.get("/api/labels/available")
def get_label_tables():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid' AND table_schema='public'
        """)).all()
    return sorted({r[0] for r in rows})

@app.get("/api/labels/schema")
def labels_schema():
    q = text("""
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.columns k
          ON k.table_name = c.table_name AND k.column_name ILIKE 'tickid'
        WHERE c.table_schema='public'
        ORDER BY c.table_name, c.ordinal_position
    """)
    out: Dict[str, Dict[str, Any]] = {}
    with engine.connect() as conn:
        for tname, cname in conn.execute(q):
            if tname not in out:
                out[tname] = {"table": tname, "labels": []}
            low = cname.lower()
            if low != "id" and low != "tickid" and not low.startswith("ts"):
                out[tname]["labels"].append(cname)
    return [v for v in out.values() if v["labels"]]

# ----------------------------------------------------
# Version
# ----------------------------------------------------
@app.get("/version")
def get_version():
    return {"version": "2025.08.24.walk-forward.bootstrap"}

# ----------------------------------------------------
# Walk-forward internals
# ----------------------------------------------------
def _do_walkforward_step() -> Dict[str, Any]:
    msum = BuildOrExtendSegments()
    esum = DetectMicroEventsForLatestClosedSegment()
    osum = ResolveOutcomes()
    psum = TrainAndPredict()
    snap = _do_walkforward_snapshot()
    return {
        "macro_segments": msum,
        "micro_events": esum,
        "outcomes": osum,
        "predictions": psum,
        "snapshot": snap,
    }

def _do_walkforward_snapshot() -> Dict[str, Any]:
    with engine.connect() as conn:
        segs = [dict(r._mapping) for r in conn.execute(text("""
            SELECT segment_id, start_ts, end_ts, direction, confidence,
                   start_price, end_price, start_tick_id, end_tick_id
            FROM macro_segments
            ORDER BY end_ts DESC
            LIMIT 40
        """))]

        seg_ids = [s["segment_id"] for s in segs[:5]] if segs else []
        events: List[Dict[str, Any]] = []
        if seg_ids:
            events = [dict(r._mapping) for r in conn.execute(text("""
                SELECT e.event_id, e.segment_id, e.tick_id, e.event_type, e.features,
                       t.timestamp AS event_ts, t.mid AS event_price
                FROM micro_events e
                JOIN ticks t ON t.id = e.tick_id
                WHERE e.segment_id = ANY(:seg_ids)
                ORDER BY e.event_id
            """), {"seg_ids": seg_ids})]

        outcomes: List[Dict[str, Any]] = []
        if events:
            eids = [e["event_id"] for e in events]
            outcomes = [dict(r._mapping) for r in conn.execute(text("""
                SELECT event_id, outcome, tp_hit_ts, sl_hit_ts, timeout_ts,
                       horizon_seconds, mfe, mae
                FROM outcomes
                WHERE event_id = ANY(:eids)
            """), {"eids": eids})]

        preds: List[Dict[str, Any]] = []
        if events:
            preds = [dict(r._mapping) for r in conn.execute(text("""
                SELECT DISTINCT ON (event_id)
                       event_id, model_version, p_tp, threshold, decided, predicted_at
                FROM predictions
                WHERE event_id = ANY(:eids)
                ORDER BY event_id, predicted_at DESC
            """), {"eids": [e["event_id"] for e in events]})]

        return {"segments": segs, "events": events, "outcomes": outcomes, "predictions": preds}

# Root paths
@app.post("/walkforward/step")
def walkforward_step_root():
    try:
        return _do_walkforward_step()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/walkforward/snapshot")
def walkforward_snapshot_root():
    try:
        return _do_walkforward_snapshot()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# /api mirrors (optional)
@app.post("/api/walkforward/step")
def walkforward_step_api():
    try:
        return _do_walkforward_step()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/walkforward/snapshot")
def walkforward_snapshot_api():
    try:
        return _do_walkforward_snapshot()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ----------------------------------------------------
# Trends (unchanged)
# ----------------------------------------------------
@app.get("/trends/recent")
def trends_recent(limit: int = 200):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, scale, direction, start_ts, end_ts, start_tickid, end_tickid,
                       start_price, end_price, magnitude, duration_sec, velocity
                FROM swings
                WHERE status=1
                ORDER BY end_ts DESC
                LIMIT :limit
            """),
            {"limit": limit},
        ).mappings().all()
    return list(rows)

@app.get("/trends/range")
def trends_range(start: str, end: str, scale: Optional[int] = None):
    q = """
        SELECT id, scale, direction, start_ts, end_ts, start_tickid, end_tickid,
               start_price, end_price, magnitude, duration_sec, velocity
        FROM swings
        WHERE status=1
          AND end_ts >= :a AND start_ts <= :b
    """
    params: Dict[str, Any] = {"a": start, "b": end}
    if scale in (1, 2):
        q += " AND scale=:scale"
        params["scale"] = scale
    q += " ORDER BY start_ts"
    with engine.connect() as conn:
        rows = conn.execute(text(q), params).mappings().all()
    return list(rows)

@app.get("/labels/{name}")
def get_labels_for_table(name: str):
    with engine.connect() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.columns
                    WHERE column_name ILIKE 'tickid' AND table_schema='public'
                """)
            )
        }
        if name not in tables:
            raise HTTPException(status_code=400, detail="Unknown label table")
        rows = conn.execute(text(f'SELECT tickid FROM "{name}" ORDER BY tickid ASC'))
        return [dict(row._mapping) for row in rows]

@app.get("/trends/day")
def trends_day(day: str, scale: Optional[int] = None):
    d = date.fromisoformat(day)
    a = f"{d.isoformat()}T00:00:00Z"
    b = f"{(d + timedelta(days=1)).isoformat()}T00:00:00Z"
    return trends_range(a, b, scale)

# ----------------------------------------------------
# Static
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
