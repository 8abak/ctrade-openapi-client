# backend/main.py

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine, text
import os
from datetime import datetime

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
    timestamp: datetime
    bid: float
    ask: float
    mid: float


# Get chunk of historical ticks (for scrolling/initial view)
@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        query = text("""
            SELECT timestamp, bid, ask, mid
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
            SELECT timestamp, bid, ask, mid
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
            SELECT timestamp, bid, ask, mid
            FROM (
                SELECT *
                FROM ticks
                ORDER BY timestamp DESC
                LIMIT :limit
            ) sub
            ORDER BY timestamp ASC
        """)
        result = conn.execute(query, {"limit": limit})
        ticks = [dict(row._mapping) for row in result]
    return ticks


@app.get("/")
def home():
    return {"message": "Tick API is live. Try /ticks or /ticks/latest."}


@app.get("/version")
def get_version():
    return {"version": "2025.06.28.01"}  # Manually update as needed

