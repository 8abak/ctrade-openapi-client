# backend/main.py

import os
from datetime import datetime, timedelta, date
from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text

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
def get_latest_ticks(after: str = Query(..., description="UTC ISO timestamp")):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC
            LIMIT 1000
        """), {"after": after}).mappings().all()
    return list(rows)

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
