# backend/main.py

import os
from datetime import datetime, timedelta, date
from typing import List, Optional
from typing import Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy import text as sqtxt
from fastapi import HTTPException

# ----- App & CORS -----
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----- DB -----
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(db_url)

# ----- Models -----
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

# ----- Ticks API (unchanged) -----
@app.get("/")
def home():
    return {"message": "API live. Try /ticks/recent, /trends/day, or /movements"}

@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            ORDER BY timestamp ASC
            OFFSET :offset LIMIT :limit
        """), {"offset": offset, "limit": limit}).mappings().all()
    return list(rows)

@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(..., description="UTC timestamp in ISO format")):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC
            LIMIT 1000
        """)
        result = conn.execute(query, {"after": after})
        ticks = [dict(row._mapping) for row in result]
    return ticks


@app.get("/ticks/recent", response_model=List[Tick])
def get_recent_ticks(limit: int = Query(2200, le=5000)):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM (
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                ORDER BY timestamp DESC
                LIMIT :limit
            ) sub
            ORDER BY timestamp ASC
        """), {"limit": limit}).mappings().all()
    return list(rows)

@app.get("/ticks/before/{tickid}", response_model=List[Tick])
def get_ticks_before(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE id < :tickid
            ORDER BY timestamp DESC
            LIMIT :limit
        """), {"tickid": tickid, "limit": limit}).mappings().all()
    return list(reversed(rows))

# ----- SQL Viewer helpers (unchanged) -----
@app.get("/sqlvw/tables")
def get_all_table_names():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_type='BASE TABLE'
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

# Labels discovery (unchanged path used by frontend)
@app.get("/api/labels/available")
def get_label_tables():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid' AND table_schema='public'
        """)).all()
    return sorted({r[0] for r in rows})

# ----- Version -----
@app.get("/version")
def get_version():
    return {"version": "2025.08.08.walk-forward.001"}

# ----- Trends API -----
@app.get("/trends/recent")
def trends_recent(limit: int = 200):
    with engine.connect() as conn:
        rows = conn.execute(text("""
          SELECT id, scale, direction, start_ts, end_ts,
                 start_tickid, end_tickid,
                 start_price, end_price, magnitude, duration_sec, velocity
          FROM swings
          WHERE status=1
          ORDER BY end_ts DESC
          LIMIT :limit
        """), {"limit": limit}).mappings().all()
    return list(rows)

@app.get("/trends/range")
def trends_range(start: str, end: str, scale: Optional[int] = None):
    q = """
      SELECT id, scale, direction, start_ts, end_ts,
             start_tickid, end_tickid,
             start_price, end_price, magnitude, duration_sec, velocity
      FROM swings
      WHERE status=1 AND end_ts >= :a AND start_ts <= :b
    """
    params = {"a": start, "b": end}
    if scale in (1, 2):
        q += " AND scale=:scale"
        params["scale"] = scale
    q += " ORDER BY start_ts"
    with engine.connect() as conn:
        rows = conn.execute(text(q), params).mappings().all()
    return list(rows)

@app.get("/labels/{name}")
def get_labels_for_table(name: str):
    # only allow names that appear in /labels/available
    with engine.connect() as conn:
        avail_query = text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid'
              AND table_schema = 'public'
        """)
        tables = {row[0] for row in conn.execute(avail_query)}
    if name not in tables:
        raise HTTPException(status_code=400, detail="Unknown label table")

    with engine.connect() as conn:
        # SELECT only tickid; frontend expects r.tickid
        result = conn.execute(text(f'SELECT tickid FROM "{name}" ORDER BY tickid ASC'))
        rows = [dict(row._mapping) for row in result]
    return rows


@app.get("/trends/day")
def trends_day(day: str, scale: Optional[int] = None):
    # day is YYYY-MM-DD (UTC)
    d = date.fromisoformat(day)
    a = f"{d.isoformat()}T00:00:00Z"
    b = f"{(d + timedelta(days=1)).isoformat()}T00:00:00Z"
    return trends_range(a, b, scale)

# ----- Static: movements visual -----
public_dir = os.path.join(os.path.dirname(__file__), "..", "public")
if os.path.isdir(public_dir):
    app.mount("/public", StaticFiles(directory=public_dir, html=True), name="public")

@app.get("/movements")
def movements_page():
    # convenience redirect to /public/movements.html
    file_path = os.path.join(public_dir, "movements.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"message": "movements.html not found. Put it under ./public/"}


#fetch zigzag
LEVEL_TABLE: Dict[str, str] = {
    "micro": "micro_trends",
    "medium": "medium_trends",
    "maxi": "maxi_trends",
}

def q_all(sql: str, params: Dict[str, Any]):
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sqtxt(sql), params)]

@app.get("/api/zigzag")
def zigzag(
    mode: str = Query("date", regex="^(date|id)$"),
    levels: str = "micro,medium,maxi",
    day: str | None = None,
    start_id: int | None = None,
    span_minutes: int = 60,
    cursor_ts: str | None = None,
    limit: int = 2000,
):
    lvls = [l for l in levels.split(",") if l in LEVEL_TABLE]
    if not lvls:
        lvls = ["micro", "medium", "maxi"]

    if mode == "date":
        if not day:
            return {"error": "missing day"}, 400
        where_more = "AND start_ts > :cursor_ts" if cursor_ts else ""
        params_more = {"day": day, "cursor_ts": cursor_ts, "limit": limit}
        segs, pts = {}, {}
        max_ts = None
        for lvl in lvls:
            tbl = LEVEL_TABLE[lvl]
            segs[lvl] = q_all(
                f"""
                SELECT start_ts, end_ts, start_price, end_price
                FROM {tbl}
                WHERE run_day = :day {where_more}
                ORDER BY start_ts
                LIMIT :limit
                """,
                params_more,
            )
            pts[lvl] = q_all(
                """
                SELECT ts, price, kind
                FROM zigzag_points
                WHERE level = :lvl AND run_day = :day
                ORDER BY ts
                """,
                {"lvl": lvl, "day": day},
            )
            if segs[lvl]:
                t = segs[lvl][-1]["end_ts"]
                max_ts = t if (max_ts is None or t > max_ts) else max_ts
        return {"segments": segs, "points": pts, "meta": {"cursor_ts": max_ts}}

    # mode == 'id'
    if not start_id:
        return {"error": "missing start_id"}, 400

    # find start_ts for this tick id
    row = q_all("SELECT timestamp FROM ticks WHERE id = :tid", {"tid": start_id})
    if not row:
        return {"error": "start_id not found"}, 404
    start_ts = row[0]["timestamp"]
    from datetime import timedelta
    end_ts = start_ts + timedelta(minutes=span_minutes)

    where_more = "AND start_ts > :cursor_ts" if cursor_ts else ""
    segs, pts = {}, {}
    max_ts = None
    for lvl in lvls:
        tbl = LEVEL_TABLE[lvl]
        segs[lvl] = q_all(
            f"""
            SELECT start_ts, end_ts, start_price, end_price
            FROM {tbl}
            WHERE start_ts >= :a AND start_ts < :b {where_more}
            ORDER BY start_ts
            LIMIT :limit
            """,
            {"a": start_ts, "b": end_ts, "cursor_ts": cursor_ts, "limit": limit},
        )
        pts[lvl] = q_all(
            """
            SELECT ts, price, kind
            FROM zigzag_points
            WHERE level = :lvl AND ts >= :a AND ts < :b
            ORDER BY ts
            """,
            {"lvl": lvl, "a": start_ts, "b": end_ts},
        )
        if segs[lvl]:
            t = segs[lvl][-1]["end_ts"]
            max_ts = t if (max_ts is None or t > max_ts) else max_ts

    return {"segments": segs, "points": pts, "meta": {"cursor_ts": max_ts}}

    