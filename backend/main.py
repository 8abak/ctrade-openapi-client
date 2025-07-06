# ✅ FINAL VERSION of main.py (backend) for live tick updates via WebSocket

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine, text
from datetime import datetime
from fastapi.responses import JSONResponse
import os
import json

# Initialize FastAPI
app = FastAPI()

# Allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Consider restricting in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(db_url)

# Tick Model
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

# Live tick clients (WebSocket)
connected_clients = []

@app.websocket("/ws/ticks")
async def websocket_ticks(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()  # Keep alive
    except WebSocketDisconnect:
        connected_clients.remove(websocket)

async def broadcast_tick(tick: dict):
    msg = json.dumps(tick)
    for client in connected_clients.copy():
        try:
            await client.send_text(msg)
        except:
            connected_clients.remove(client)

@app.get("/")
def home():
    return {"message": "Tick API live. Try /ticks or /ticks/latest."}

@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            ORDER BY timestamp ASC
            OFFSET :offset LIMIT :limit
        """), {"offset": offset, "limit": limit})
        return [dict(row._mapping) for row in result]

@app.get("/ticks/recent", response_model=List[Tick])
def get_recent_ticks(limit: int = Query(2200, le=5000)):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM (SELECT * FROM ticks ORDER BY timestamp DESC LIMIT :limit) sub
            ORDER BY timestamp ASC
        """), {"limit": limit})
        return [dict(row._mapping) for row in result]

@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(...)):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC
            LIMIT 1000
        """), {"after": after})
        return [dict(row._mapping) for row in result]

@app.get("/ticks/after/{timestamp}", response_model=List[Tick])
def get_ticks_after_timestamp(timestamp: str, limit: int = 5000):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE timestamp >= :timestamp
            ORDER BY timestamp ASC
            LIMIT :limit
        """), {"timestamp": timestamp, "limit": limit})
        return [dict(row._mapping) for row in result]

@app.get("/ticks/after-id/{tickid}", response_model=List[Tick])
def get_ticks_after_id(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE id > :tickid
            ORDER BY id ASC
            LIMIT :limit
        """), {"tickid": tickid, "limit": limit})
        return [dict(row._mapping) for row in result]

@app.get("/ticks/before/{tickid}", response_model=List[Tick])
def get_ticks_before(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid
            FROM ticks
            WHERE id < :tickid
            ORDER BY timestamp DESC
            LIMIT :limit
        """), {"tickid": tickid, "limit": limit})
        return list(reversed([dict(row._mapping) for row in result]))

@app.get("/labels/{name}")
def get_labels_by_name(name: str):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""SELECT * FROM {name} ORDER BY tickid ASC"""))
            return [dict(row._mapping) for row in result]
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/labels/available")
def get_label_tables():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid'
              AND table_schema = 'public'
        """))
        return sorted({row[0] for row in result})

@app.get("/api/labels/available")
def get_label_tables_api():
    return get_label_tables()

@app.get("/sqlvw/tables")
def get_all_table_names():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
        """))
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

@app.get("/version")
def get_version():
    return {"version": bver}
