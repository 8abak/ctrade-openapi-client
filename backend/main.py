# backend/main.py

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine, text
import os
from datetime import datetime
from fastapi.responses import JSONResponse

# Initialize FastAPI
app = FastAPI()

# Allow cross-origin requests (frontend calling this backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to "https://www.datavis.au" later
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(db_url)

# Tick model
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

# Home route to check if API is live
@app.get("/")
def home():
    return {"message": "Tick API is live. Try /ticks or /ticks/latest."}

# Get ticks with offset (legacy)
@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            ORDER BY timestamp ASC
            OFFSET :offset LIMIT :limit
        """)
        result = conn.execute(query, {"offset": offset, "limit": limit})
        return [dict(row._mapping) for row in result]

# Get latest ticks after timestamp
@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(...)):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC
            LIMIT 1000
        """)
        result = conn.execute(query, {"after": after})
        return [dict(row._mapping) for row in result]

# Get recent N ticks
@app.get("/ticks/recent", response_model=List[Tick])
def get_recent_ticks(limit: int = Query(2200, le=5000)):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM (
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                ORDER BY timestamp DESC
                LIMIT :limit
            ) sub
            ORDER BY timestamp ASC
        """)
        result = conn.execute(query, {"limit": limit})
        return [dict(row._mapping) for row in result]

# Get ticks before a specific tick ID
@app.get("/ticks/before/{tickid}", response_model=List[Tick])
def get_ticks_before(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE id < :tickid
            ORDER BY timestamp DESC
            LIMIT :limit
        """)
        result = conn.execute(query, {"tickid": tickid, "limit": limit})
        return list(reversed([dict(row._mapping) for row in result]))

# Get ticks after a tick ID (used for Htick View scroll)
@app.get("/ticks/after-id/{tickid}", response_model=List[Tick])
def get_ticks_after_id(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE id > :tickid
            ORDER BY id ASC
            LIMIT :limit
        """)
        result = conn.execute(query, {"tickid": tickid, "limit": limit})
        return [dict(row._mapping) for row in result]

# ✅ NEW: Get ticks after a timestamp (used for main chart startup)
@app.get("/ticks/after/{timestamp}", response_model=List[Tick])
def get_ticks_after_timestamp(timestamp: str, limit: int = 5000):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp >= :timestamp
            ORDER BY timestamp ASC
            LIMIT :limit
        """)
        result = conn.execute(query, {"timestamp": timestamp, "limit": limit})
        return [dict(row._mapping) for row in result]

# ✅ NEW: Fetch all labels from a table like /labels/upmoves
@app.get("/labels/{name}")
def get_labels_by_name(name: str):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT * FROM {name}
                ORDER BY tickid ASC
            """))
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# ✅ NEW: Mirror route expected by frontend
@app.get("/api/labels/available")
def get_label_tables_api():
    return get_label_tables()

# Get label-related tables that contain tickid
@app.get("/labels/available")
def get_label_tables():
    with engine.connect() as conn:
        query = text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid'
              AND table_schema = 'public'
        """)
        result = conn.execute(query)
        return sorted({row[0] for row in result})

# SQL Table Browser (for SQL View)
@app.get("/sqlvw/tables")
def get_all_table_names():
    with engine.connect() as conn:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_type='BASE TABLE'
        """)
        result = conn.execute(query)
        return [row[0] for row in result]

@app.get("/sqlvw/query")
def run_sql_query(query: str = Query(...)):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(query))
            if result.returns_rows:
                return [dict(row._mapping) for row in result]
            return {"message": "Query executed successfully."}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# Version check
@app.get("/version")
def get_version():
    return {"version": "2025.07.05.002"}
