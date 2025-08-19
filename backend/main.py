# backend/main.py
import os
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy import text as sqtxt
from zig_api import router as lview_router
#MLing imports
import subprocess, sys, json
from sqlalchemy import text as _sqltext
from ml.db import get_engine, latest_prediction, review_slice


# ---------  App & CORS ---------
app = FastAPI(title="cTrade backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten later
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(lview_router, prefix="/api")

# ---------- DB ----------
db_url = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(db_url)

# ---------- Models ----------
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float


# ---------- Small helpers ----------
def q_all(sql: str, params: Dict[str, Any]):
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sqtxt(sql), params)]


# ---------- Root ----------
@app.get("/")
def home():
    return {"message": "API live. Try /ticks/recent, /trends/day, /sqlvw/tables, /version"}


# ---------- Ticks ----------
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
                SELECT id, timestamp, bid, ask, mid FROM (
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


# ---------- SQL viewer helpers ----------
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


# Labels discovery (kept at /api/* because frontend pages use it)
@app.get("/api/labels/available")
def get_label_tables():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid' AND table_schema='public'
        """)).all()
    return sorted({r[0] for r in rows})


# ---------- Version ----------
@app.get("/version")
def get_version():
    return {"version": "2025.08.08.walk-forward.001"}


# ---------- Trends (unchanged) ----------
@app.get("/trends/recent")
def trends_recent(limit: int = 200):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, scale, direction, start_ts, end_ts,
                       start_tickid, end_tickid, start_price, end_price,
                       magnitude, duration_sec, velocity
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
        SELECT id, scale, direction, start_ts, end_ts,
               start_tickid, end_tickid, start_price, end_price,
               magnitude, duration_sec, velocity
        FROM swings
        WHERE status=1 AND end_ts >= :a AND start_ts <= :b
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
    # only allow names that appear in /labels/available
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
    d = date.fromisoformat(day)  # YYYY-MM-DD
    a = f"{d.isoformat()}T00:00:00Z"
    b = f"{(d + timedelta(days=1)).isoformat()}T00:00:00Z"
    return trends_range(a, b, scale)


# ---------- Static: movements visual ----------
public_dir = os.path.join(os.path.dirname(__file__), "..", "public")
if os.path.isdir(public_dir):
    app.mount("/public", StaticFiles(directory=public_dir, html=True), name="public")


@app.get("/movements")
def movements_page():
    file_path = os.path.join(public_dir, "movements.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"message": "movements.html not found. Put it under ./public/"}


# ---------- ZigZag ----------
LEVEL_TABLE: Dict[str, str] = {
    "micro": "micro_trends",
    "medium": "medium_trends",
    "maxi": "maxi_trends",
}

def _zigzag_impl(
    mode: str,
    levels: str,
    day: Optional[str],
    start_id: Optional[int],
    span_minutes: int,
    cursor_ts: Optional[str],
    limit: int,
):
    lvls = [l for l in levels.split(",") if l in LEVEL_TABLE]
    if not lvls:
        lvls = ["micro", "medium", "maxi"]

    # ----- mode: by date -----
    if mode == "date":
        if not day:
            return {"error": "missing day"}, 400

        where_more = "AND start_ts > :cursor_ts" if cursor_ts else ""
        params_more = {"day": day, "cursor_ts": cursor_ts, "limit": limit}

        segs: Dict[str, List[Dict[str, Any]]] = {}
        pts: Dict[str, List[Dict[str, Any]]] = {}
        max_ts: Optional[datetime] = None

        for lvl in lvls:
            tbl = LEVEL_TABLE[lvl]

            segs[lvl] = q_all(
                f"""
                SELECT id, start_tick_id, end_tick_id,
                    start_ts, end_ts, 
                    start_price, end_price,
                    direction, range_abs, duration_s, num_ticks
                FROM {tbl}
                WHERE run_day = :day {where_more}
                ORDER BY start_ts
                LIMIT :limit
                """,
                params_more,
            )

            pts[lvl] = q_all(
                """
                SELECT tick_id, ts, price, kind
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

    # ----- mode: by start tick id -----
    if not start_id:
        return {"error": "missing start_id"}, 400

    row = q_all("SELECT timestamp FROM ticks WHERE id = :tid", {"tid": start_id})
    if not row:
        return {"error": "start_id not found"}, 404

    start_ts: datetime = row[0]["timestamp"]
    end_ts: datetime = start_ts + timedelta(minutes=span_minutes)

    where_more = "AND start_ts > :cursor_ts" if cursor_ts else ""

    segs: Dict[str, List[Dict[str, Any]]] = {}
    pts: Dict[str, List[Dict[str, Any]]] = {}
    max_ts: Optional[datetime] = None

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


# Serve at root path to avoid nginx /api rewrite needs
@app.get("/zigzag")
def zigzag(
    mode: str = Query("date", regex="^(date|id)$"),
    levels: str = "micro,medium,maxi",
    day: Optional[str] = None,
    start_id: Optional[int] = None,
    span_minutes: int = 60,
    cursor_ts: Optional[str] = None,
    limit: int = 2000,
):
    return _zigzag_impl(
        mode=mode,
        levels=levels,
        day=day,
        start_id=start_id,
        span_minutes=span_minutes,
        cursor_ts=cursor_ts,
        limit=limit,
    )



#MLing routes
@app.post("/ml/run_step")
def ml_run_step(start: int = Query(...), block: int = Query(100000), algo: str = Query("sgd")):
    try:
        p = subprocess.run(
            [sys.executable, "-m", "jobs.run_step", "--start", str(start), "--block", str(block), "--algo", algo],
            capture_output=True, text=True, timeout=3600
        )
        if p.returncode != 0:
            return JSONResponse(status_code=500, content={"error": p.stderr.strip()})
        out = p.stdout.strip().splitlines()[-1]
        return json.loads(out)
    except subprocess.TimeoutExpired:
        return JSONResponse(status_code=500, content={"error": "run_step timeout"})

@app.get("/ml/review")
def ml_review(start: int = Query(...),
              offset: int = Query(0),
              limit: int = Query(10000)):
    # 200k window: train+test = [start .. start+199999]
    total = 200000
    bundle = review_slice(start, total, offset, limit)
    # try to attach run_id if exists
    eng = get_engine()
    with eng.begin() as conn:
        rr = conn.execute(_sqltext("""
            SELECT run_id, confirmed, model_id FROM walk_runs 
            WHERE train_start=:s AND train_end=:te LIMIT 1
        """), {"s": start, "te": start+100000-1}).mappings().first()
        if rr:
            bundle["run"] = {"run_id": rr["run_id"], "confirmed": rr["confirmed"], "model_id": rr["model_id"]}
    return bundle

@app.post("/ml/confirm")
def ml_confirm(run_id: str = Query(...)):
    p = subprocess.run([sys.executable, "-m", "jobs.confirm_run", "--run_id", run_id],
                       capture_output=True, text=True, timeout=60)
    if p.returncode != 0:
        return JSONResponse(status_code=500, content={"error": p.stderr.strip()})
    return {"ok": True, "run_id": run_id}

@app.get("/ml/predict/last")
def ml_predict_last():
    lp = latest_prediction()
    return lp or {}

@app.get("/ml/status")
def ml_status():
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(_sqltext("""
            SELECT run_id, train_start, train_end, test_start, test_end, model_id, confirmed, metrics, created_at
            FROM walk_runs ORDER BY created_at DESC LIMIT 20
        """)).mappings().all()
        out = []
        for r in rows:
            out.append({
                "run_id": r["run_id"],
                "train_range": [r["train_start"], r["train_end"]],
                "test_range": [r["test_start"], r["test_end"]],
                "model_id": r["model_id"],
                "confirmed": r["confirmed"],
                "metrics": r["metrics"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None
            })
        return out