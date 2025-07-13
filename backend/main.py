# backend/main.py

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine, text
import os
from datetime import datetime
from fastapi.responses import JSONResponse
from fastapi import WebSocket
import asyncio
import random
from datetime import timezone, timedelta
from fastapi import WebSocket, WebSocketDisconnect
from backend.wsmanager import connectedClients

# Initialize FastAPI test to see version 002
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

#get last id 
@app.get("/ticks/lastid")
def get_last_id():
    with engine.connect() as conn:
        query = text("SELECT id, timestamp FROM ticks ORDER BY id DESC LIMIT 1")
        result = conn.execute(query).fetchone()
        return {
            "lastId": result[0] if result else None,
            "timestamp": result[1].isoformat() if result else None
        }

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

# get ticks from ws
@app.post("/tickstream/push")
async def receive_tick(tick: Tick):
    for ws in list(connectedClients):
        try:
            print(f"✅ Broadcasting tick ID {tick.id} to {len(connectedClients)} clients", flush=True)
            await ws.send_json(tick.dict())
        except:
            connectedClients.remove(ws)
    return {"status": "ok"}

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


#get latest id
def get_latest_id():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id FROM ticks ORDER BY id DESC LIMIT 1")).fetchone()
        return result[0] if result else 0
        
        
#get the latest ids
@app.get("/ticks/latestid", response_model=List[Tick])
def get_latest_ticks_after_id(after_id: int = Query(...)):
    with engine.connect() as conn:
        query = text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE id > :after_id
            ORDER BY id ASC
            LIMIT 1000
        """)
        result = conn.execute(query, {"after_id": after_id})
        ticks = [dict(row._mapping) for row in result]
    return ticks

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

@app.websocket("/ws/ticks")
async def streamRealTickets(websocket: WebSocket):
    await websocket.accept()
    connectedClients.add(websocket)

    try:
        while True:
            await asyncio.sleep(3600)
    except WebSocketDisconnect:
        connectedClients.remove(websocket)

# Version check
@app.get("/version")
def get_version():
    import json
    with open("static/version.json") as f:
        return json.load(f)

