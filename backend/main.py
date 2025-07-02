# backend/main.py

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine, text
import os
from datetime import datetime
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta



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


# Get chunk of historical ticks (for scrolling/initial view)
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
        ticks = [dict(row._mapping) for row in result]
    return ticks


# Get new ticks after a timestamp (for live appending)
@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(..., description="UTC timestamp in ISO format")):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid  -- <-- FIXED: add id
            FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC
            LIMIT 1000
        """)
        result = conn.execute(query, {"after": after})
        ticks = [dict(row._mapping) for row in result]
    return ticks


# Get the latest N ticks in chronological order
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
        ticks = [dict(row._mapping) for row in result]
    return ticks

# Home route to check if API is live
@app.get("/")
def home():
    return {"message": "Tick API is live. Try /ticks or /ticks/latest."}

# get thicks before by ID
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
        ticks = [dict(row._mapping) for row in result]
    return list(reversed(ticks))  # Reverse to return in chronological order


# get all table names in the database
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
        tables = [row[0] for row in result]
    return tables

# Run a SQL query against the database
@app.get("/sqlvw/query")
def run_sql_query(query: str = Query(...)):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(query))
            # If it's a SELECT, return the rows
            if result.returns_rows:
                rows = [dict(row._mapping) for row in result]
                return rows
            else:
                return {"message": "Query executed successfully."}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

# to serve Support and Resisance zones
@app.get("/labels/supres")
def get_supres_zones():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT level_type, price_low, price_high, tickid_start, tickid_end
            FROM supRes
            WHERE confirmed = TRUE
        """))
        return [dict(row._mapping) for row in result]

# Get ticks starting from a specific timestamp
@app.get("/ticks/from", response_model=List[Tick])
def get_ticks_from(start: str = Query(..., description="UTC timestamp in ISO format"), limit: int = 2000):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp >= :start
            ORDER BY timestamp ASC
            LIMIT :limit
        """)
        result = conn.execute(query, {"start": start})
        ticks = [dict(row._mapping) for row in result]
    return ticks

# Get the first ticks of the current day
@app.get("/ticks/first-of-day", response_model=Tick)
def get_first_tick_of_day():
    try:
        today = datetime.utcnow().date()
        with engine.connect() as conn:
            query = text("""
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                WHERE timestamp >= CAST(:start AS timestamptz)
                ORDER BY timestamp ASC
                LIMIT 1
            """)
            result = conn.execute(query, {"start": str(today)})
            row = result.fetchone()
            if not row:
                return JSONResponse(status_code=404, content={"error": "No data today"})
            return dict(row._mapping)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})




# Get available tables based on labels.
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
        tables = sorted({row[0] for row in result})
    return tables


# Get the current version of the API
@app.get("/version")
def get_version():
    return {"version": "2025.07.02.2.010"}  # Manually update as needed
