# backend/main.py

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, text
import os
import datetime

app = FastAPI()

# Allow frontend from anywhere for now (we'll lock it later to datavis.au)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# PostgreSQL DB config
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(db_url)


class Tick(BaseModel):
    timestamp: str
    bid: float
    ask: float
    mid: float


@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        query = text("""
            SELECT timestamp, bid, ask, ROUND((bid + ask)/2, 2) AS mid
            FROM ticks
            ORDER BY timestamp ASC
            OFFSET :offset LIMIT :limit
        """)
        result = conn.execute(query, {"offset": offset, "limit": limit})
        ticks = [dict(row._mapping) for row in result]
    return ticks


@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(..., description="UTC timestamp in ISO format")):
    with engine.connect() as conn:
        query = text("""
            SELECT timestamp, bid, ask, ROUND((bid + ask)/2, 2) AS mid
            FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC
            LIMIT 1000
        """)
        result = conn.execute(query, {"after": after})
        ticks = [dict(row._mapping) for row in result]
    return ticks


@app.get("/")
def home():
    return {"message": "Tick API is live. Use /ticks or /ticks/latest."}
