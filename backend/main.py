from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Set
from sqlalchemy import create_engine, text
from datetime import datetime
from backend.wsmanager import pushTick
import asyncio
import os
import json

app = FastAPI()
connectedClients: Set[WebSocket] = set()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB setup
db_url = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(db_url)

# Models
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

@app.get("/")
def home():
    return {"message": "Tick API is live."}

@app.get("/ticks/lastid")
def get_last_id():
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id, timestamp FROM ticks ORDER BY id DESC LIMIT 1")).fetchone()
        return {
            "lastId": row[0] if row else None,
            "timestamp": row[1].isoformat() if row else None
        }

@app.get("/ticks", response_model=List[Tick])
def get_ticks(offset: int = 0, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            ORDER BY timestamp ASC OFFSET :offset LIMIT :limit
        """), {"offset": offset, "limit": limit})
        return [dict(r._mapping) for r in rows]

@app.post("/tickstream/push")
async def receive_tick(tick: Tick):
    print(f"📨 Received tick: {tick}", flush=True)
    await pushTick(tick.model_dump())
    return {"status": "ok"}

@app.get("/ticks/latest", response_model=List[Tick])
def get_latest_ticks(after: str = Query(...)):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            WHERE timestamp > :after
            ORDER BY timestamp ASC LIMIT 1000
        """), {"after": after})
        return [dict(r._mapping) for r in rows]

@app.get("/ticks/range", response_model=List[Tick])
def get_ticks_range(start: str, end: str):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            WHERE timestamp >= :start AND timestamp < :end
            ORDER BY timestamp ASC
        """), {"start": start, "end": end})
        return [dict(r._mapping) for r in rows]

@app.get("/ticks/recent", response_model=List[Tick])
def get_recent_ticks(limit: int = Query(2200, le=5000)):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM (
                SELECT id, timestamp, bid, ask, mid FROM ticks
                ORDER BY timestamp DESC LIMIT :limit
            ) sub ORDER BY timestamp ASC
        """), {"limit": limit})
        return [dict(r._mapping) for r in rows]

@app.get("/ticks/before/{tickid}", response_model=List[Tick])
def get_ticks_before(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            WHERE id < :tickid ORDER BY timestamp DESC LIMIT :limit
        """), {"tickid": tickid, "limit": limit})
        return list(reversed([dict(r._mapping) for r in rows]))

@app.get("/ticks/after-id/{tickid}", response_model=List[Tick])
def get_ticks_after_id(tickid: int, limit: int = 2000):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            WHERE id > :tickid ORDER BY id ASC LIMIT :limit
        """), {"tickid": tickid, "limit": limit})
        return [dict(r._mapping) for r in rows]

@app.get("/ticks/after/{timestamp}", response_model=List[Tick])
def get_ticks_after_timestamp(timestamp: str, limit: int = 5000):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            WHERE timestamp >= :timestamp ORDER BY timestamp ASC LIMIT :limit
        """), {"timestamp": timestamp, "limit": limit})
        return [dict(r._mapping) for r in rows]

@app.get("/labels/{name}")
def get_labels_by_name(name: str):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {name} ORDER BY tickid ASC"))
            return [dict(r._mapping) for r in result]
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/available")
def get_label_tables():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT t.table_name FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE t.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
            AND c.column_name ILIKE 'tickid'
        """))
        return sorted({row[0] for row in rows})

@app.get("/ticks/latestid", response_model=List[Tick])
def get_latest_ticks_after_id(after_id: int = Query(...)):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, bid, ask, mid FROM ticks
            WHERE id > :after_id ORDER BY id ASC LIMIT 1000
        """), {"after_id": after_id})
        return [dict(r._mapping) for r in rows]

@app.get("/sqlvw/tables")
def get_all_table_names():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
        """))
        return [row[0] for row in rows]

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

@app.post("/sqlvw/delete")
def drop_table(payload: dict = Body(...)):
    table = payload.get("table")
    if not table:
        return JSONResponse(status_code=400, content={"error": "Missing table name"})
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        return {"message": f"Table {table} deleted."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/sqlvw/create")
def create_standard_table(payload: dict = Body(...)):
    table = payload.get("table")
    mode = payload.get("mode")
    if not table or not mode:
        return JSONResponse(status_code=400, content={"error": "Missing table or mode"})
    try:
        with engine.begin() as conn:
            if mode == "tick":
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id SERIAL PRIMARY KEY,
                        tickId INTEGER,
                        content TEXT,
                        modelVersion TEXT,
                        confidenceScore REAL
                    )
                """))
            elif mode == "area":
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id SERIAL PRIMARY KEY,
                        startTime TIMESTAMP,
                        endTime TIMESTAMP,
                        minPrice REAL,
                        maxPrice REAL,
                        content TEXT,
                        modelVersion TEXT,
                        confidenceScore REAL
                    )
                """))
            else:
                return JSONResponse(status_code=400, content={"error": "Invalid mode"})
        return {"message": f"Table {table} created as {mode}."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/labels/assign")
def assign_labels(payload: dict = Body(...)):
    table = payload.get("table")
    ids = payload.get("ids", [])
    note = payload.get("note", None)

    if not table or not ids:
        return JSONResponse(status_code=400, content={"error": "Missing table or ids"})

    try:
        with engine.begin() as conn:
            for tickid in ids:
                if note:
                    conn.execute(text(f"INSERT INTO {table} (tickid, content) VALUES (:id, :note) ON CONFLICT DO NOTHING"), {"id": tickid, "note": note})
                else:
                    conn.execute(text(f"INSERT INTO {table} (tickid) VALUES (:id) ON CONFLICT DO NOTHING"), {"id": tickid})
        return {"status": "success", "inserted": len(ids)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.websocket("/ws/ticks")
async def streamRealTickets(websocket: WebSocket):
    await websocket.accept()
    connectedClients.add(websocket)
    print(f"🎯 WebSocket connected from {websocket.client.host}. Total: {len(connectedClients)}", flush=True)

    try:
        while True:
            await websocket.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        print("❌ WebSocket disconnected", flush=True)
        connectedClients.remove(websocket)

@app.get("/version")
def get_version():
    with open("static/version.json") as f:
        return json.load(f)
