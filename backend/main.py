# backend/main.py

import os
import traceback
from datetime import datetime, timedelta, date, time
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from sqlalchemy import create_engine, text
from sqlalchemy import text as sqtxt

# Mount existing router
from zig_api import router as lview_router  # keeps your /api/* endpoints

# Relative imports (your files in backend/)
from .label_macro_segments import BuildOrExtendSegments
from .label_micro_events import DetectMicroEventsForLatestClosedSegment
from .compute_outcomes import ResolveOutcomes
from .train_predict import TrainAndPredict

# ------------------------------ App & CORS ------------------------------
app = FastAPI(title="cTrade backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(lview_router, prefix="/api")

# ------------------------------ DB ------------------------------
db_url = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(db_url)

# ------------------------------ Models ------------------------------
class Tick(BaseModel):
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

# ------------------------------ Helpers ------------------------------
def q_all(sql: str, params: Dict[str, Any]):
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sqtxt(sql), params)]

# Timezone helper (no external deps): Sydney is UTC+10 / +11. We don’t
# need tz math to find the 06:00 boundary; compute in DB using tick timestamps.
SYD_OFFSET_HOURS = 10  # your DB stores local +10:00 timestamps already

def _get_last_progress_tick_id() -> int:
    """
    Anchor progress by the furthest tick we have labeled/predicted on.
    Prefer micro_events.tick_id, fall back to events.tick_id, else 0.
    """
    with engine.begin() as cx:
        row = cx.execute(text("""
            SELECT COALESCE(
                (SELECT MAX(tick_id) FROM micro_events),
                (SELECT MAX(tick_id) FROM events),
                0
            ) AS last_id
        """)).mappings().first()
        return int(row["last_id"] or 0)

def _get_tick_ts_by_id(tick_id: int) -> Optional[datetime]:
    with engine.connect() as cx:
        if tick_id <= 0:
            r = cx.execute(text("SELECT MIN(timestamp) AS ts FROM ticks")).mappings().first()
            return r["ts"]
        r = cx.execute(text("SELECT timestamp AS ts FROM ticks WHERE id = :i"), {"i": tick_id}).mappings().first()
        return r["ts"] if r else None

def _next_session_end_after(ts: datetime) -> datetime:
    """
    Trading day runs 08:00 → next day 06:00. Daily 1h gap (06–08).
    Return the next 06:00 (local) strictly AFTER `ts`. Weekends closed:
    the final weekly session ends Sat 06:00; next session starts Mon 08:00.
    For our purpose, the "end boundary after ts" during weekend resolves to
    the upcoming 06:00 that terminates the first post-weekend session.
    """
    # ts is already local (+10:00) per your data; normalize to local clock:
    local = ts  # stored with +10:00 in your DB

    # candidate end today at 06:00 local
    cand = datetime.combine(local.date(), time(6, 0, 0), tzinfo=local.tzinfo)
    if local >= cand:
        cand = cand + timedelta(days=1)

    # If this lands on Sunday 06:00, push to Monday 06:00 (no sessions on weekend)
    # Monday=0 ... Sunday=6
    if cand.weekday() == 6:  # Sunday
        cand = cand + timedelta(days=1)

    return cand

def _last_tick_id_before(end_ts: datetime) -> int:
    with engine.begin() as cx:
        r = cx.execute(
            text("SELECT COALESCE(MAX(id),0) AS last_id FROM ticks WHERE timestamp < :ts"),
            {"ts": end_ts},
        ).mappings().first()
        return int(r["last_id"] or 0)

def _do_walkforward_step() -> Dict[str, Any]:
    """
    Internal step runner (engineless calls) preserved for /api mirror.
    """
    journal: List[str] = []
    try:
        journal.append("Build/extend macro segments…")
        msum = BuildOrExtendSegments()
        journal.append(f"macro: {msum}")

        journal.append("Detect micro events for latest closed segment…")
        esum = DetectMicroEventsForLatestClosedSegment()
        journal.append(f"micro: {esum}")

        journal.append("Resolve outcomes for eligible events…")
        osum = ResolveOutcomes()
        journal.append(f"outcomes: {osum}")

        journal.append("Train & predict…")
        psum = TrainAndPredict()
        journal.append(f"predict: {psum}")

        journal.append("Snapshot…")
        snap = _do_walkforward_snapshot()

        return {
            "ok": True,
            "message": "Working",
            "macro_segments": msum,
            "micro_events": esum,
            "outcomes": osum,
            "predictions": psum,
            "snapshot": snap,
            "journal": journal,
        }
    except Exception as e:
        journal.append("ERROR during walk-forward step")
        journal.append(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e), "journal": journal},
        )

def _do_walkforward_snapshot() -> Dict[str, Any]:
    with engine.connect() as conn:
        segs = [dict(r._mapping) for r in conn.execute(text("""
            SELECT segment_id, start_ts, end_ts, direction, confidence,
                   start_price, end_price, start_tick_id, end_tick_id
            FROM macro_segments
            ORDER BY end_ts DESC
            LIMIT 200
        """))]
        seg_ids = [s["segment_id"] for s in segs] if segs else []

        events: List[Dict[str, Any]] = []
        if seg_ids:
            events = [dict(r._mapping) for r in conn.execute(text("""
                SELECT e.event_id, e.segment_id, e.tick_id, e.event_type, e.features,
                       t.timestamp AS event_ts, t.mid AS event_price
                FROM micro_events e
                JOIN ticks t ON t.id = e.tick_id
                WHERE e.segment_id = ANY(:seg_ids)
                ORDER BY e.event_id
            """), {"seg_ids": seg_ids})]

        outcomes: List[Dict[str, Any]] = []
        if events:
            eids = [e["event_id"] for e in events]
            outcomes = [dict(r._mapping) for r in conn.execute(text("""
                SELECT event_id, outcome, tp_hit_ts, sl_hit_ts, timeout_ts,
                       horizon_seconds, mfe, mae
                FROM outcomes
                WHERE event_id = ANY(:eids)
            """), {"eids": eids})]

        preds: List[Dict[str, Any]] = []
        if events:
            preds = [dict(r._mapping) for r in conn.execute(text("""
                SELECT DISTINCT ON (event_id)
                    event_id, model_version, p_tp, threshold, decided, predicted_at
                FROM predictions
                WHERE event_id = ANY(:eids)
                ORDER BY event_id, predicted_at DESC
            """), {"eids": [e["event_id"] for e in events]})]

        return {"segments": segs, "events": events, "outcomes": outcomes, "predictions": preds}

# ------------------------------ Root ------------------------------
@app.get("/")
def home():
    return {"message": "API live. Try /ticks/recent, /trends/day, /sqlvw/tables, /version"}

# ------------------------------ Ticks ------------------------------
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

@app.post("/api/sql")
def sql_post(sql: str = Body(..., embed=True)):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            if result.returns_rows:
                return {"rows": [dict(r._mapping) for r in result]}
            return {"message": "Query executed successfully."}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

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
                SELECT id, timestamp, bid, ask, mid
                FROM (
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
                WHERE id BETWEEN :start AND :end
                ORDER BY id ASC
                LIMIT :limit
            """),
            {"start": start, "end": end, "limit": limit},
        ).mappings().all()
        return list(rows)

# ------------------------------ SQL viewer helpers ------------------------------
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

# Labels discovery for frontend
@app.get("/api/labels/available")
def get_label_tables():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT table_name
            FROM information_schema.columns
            WHERE column_name ILIKE 'tickid' AND table_schema='public'
        """)).all()
        return sorted({r[0] for r in rows})

@app.get("/api/labels/schema")
def labels_schema():
    q = text("""
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.columns k
          ON k.table_name = c.table_name AND k.column_name ILIKE 'tickid'
        WHERE c.table_schema='public'
        ORDER BY c.table_name, c.ordinal_position
    """)
    out: Dict[str, Dict[str, Any]] = {}
    with engine.connect() as conn:
        for tname, cname in conn.execute(q):
            if tname not in out:
                out[tname] = {"table": tname, "labels": []}
            low = cname.lower()
            if low != "id" and low != "tickid" and not low.startswith("ts"):
                out[tname]["labels"].append(cname)
    return [v for v in out.values() if v["labels"]]

# ------------------------------ Version ------------------------------
@app.get("/version")
def get_version():
    return {"version": "2025.08.27.run-day+reports"}

# ------------------------------ Walk-forward snapshot & step ------------------------------
@app.post("/walkforward/step")
def walkforward_step():
    """
    Engine-aware step, matching your existing pipeline order:
    Build/extend macro -> Detect micro -> Resolve outcomes -> Train & predict.
    """
    journal: List[str] = []

    def j(msg): journal.append(msg)
    try:
        j("Build/extend macro segments…")
        msum = BuildOrExtendSegments(engine)
        j(f"macro: {msum}")

        j("Detect micro events for latest closed segment…")
        esum = DetectMicroEventsForLatestClosedSegment(engine)
        j(f"micro: {esum}")

        j("Resolve outcomes for eligible events…")
        osum = ResolveOutcomes(engine)
        j(f"outcomes: {osum}")

        j("Train & predict…")
        psum = TrainAndPredict(engine)
        j(f"predict: {psum}")

        j("Snapshot…")
        snap = _do_walkforward_snapshot()
        return {"ok": True, "journal": journal, **snap}
    except Exception as e:
        j(f"ERROR: {repr(e)}")
        return {"ok": False, "error": str(e), "journal": journal}

@app.get("/walkforward/snapshot")
def walkforward_snapshot_root():
    try:
        return _do_walkforward_snapshot()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# Mirrors under /api
@app.post("/api/walkforward/step")
def walkforward_step_api():
    return _do_walkforward_step()

@app.get("/api/walkforward/snapshot")
def walkforward_snapshot_api():
    try:
        return _do_walkforward_snapshot()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ------------------------------ NEW: Run whole trading day ------------------------------
def _run_until_tick(target_tick_id: int, hard_cap_iters: int = 2000) -> Dict[str, Any]:
    """
    Repeatedly execute your standard step until micro_events.tick_id advances
    to >= target_tick_id, or until progress stalls.
    """
    iters = 0
    last_progress = -1
    with engine.begin() as cx:
        r = cx.execute(text("SELECT COALESCE(MAX(tick_id),0) AS mx FROM micro_events")).mappings().first()
        last_progress = int(r["mx"] or 0)

    while iters < hard_cap_iters:
        # run one step with engine (your canonical order)
        BuildOrExtendSegments(engine)
        DetectMicroEventsForLatestClosedSegment(engine)
        ResolveOutcomes(engine)
        TrainAndPredict(engine)

        with engine.begin() as cx:
            r = cx.execute(text("SELECT COALESCE(MAX(tick_id),0) AS mx FROM micro_events")).mappings().first()
            now = int(r["mx"] or 0)

        iters += 1

        # reached the boundary
        if now >= target_tick_id:
            break

        # prevent infinite loops if nothing advances
        if now <= last_progress:
            break
        last_progress = now

    return {"iterations": iters, "final_micro_tick": last_progress}

@app.post("/walkforward/run_day")
def run_day():
    """
    From the last processed tick, run step-loop until the next 06:00 boundary.
    1h daily gap (06–08). Weekends closed.
    """
    start_tick_id = _get_last_progress_tick_id()
    start_ts = _get_tick_ts_by_id(start_tick_id)
    if start_ts is None:
        return {"ok": False, "reason": "No ticks available."}

    day_end_ts = _next_session_end_after(start_ts)
    target_tick_id = _last_tick_id_before(day_end_ts)

    loop = _run_until_tick(target_tick_id)
    snap = _do_walkforward_snapshot()

    return {
        "ok": True,
        "start_tick_id": start_tick_id,
        "start_ts": start_ts.isoformat(),
        "day_end_ts": day_end_ts.isoformat(),
        "target_tick_id": target_tick_id,
        **loop,
        "snapshot": snap,
    }

# ------------------------------ NEW: Reports ------------------------------
@app.get("/reports/models")
def report_models():
    """
    Per-model summary: n_preds, n_decided, avg_p_tp, n_correct, success_rate.
    Pure SQL with CASE; safe for your SQL console too.
    """
    sql = text("""
        SELECT
          p.model_version,
          COUNT(*) AS n_preds,
          SUM(CASE WHEN p.decided THEN 1 ELSE 0 END) AS n_decided,
          AVG(p.p_tp) AS avg_p_tp,
          SUM(CASE WHEN p.decided AND e.outcome='TP' THEN 1 ELSE 0 END) AS n_correct,
          ROUND(
            100.0 * SUM(CASE WHEN p.decided AND e.outcome='TP' THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN p.decided THEN 1 ELSE 0 END), 0), 2
          ) AS success_rate_pct
        FROM predictions p
        JOIN events e ON e.event_id = p.event_id
        GROUP BY p.model_version
        ORDER BY p.model_version
    """)
    with engine.begin() as cx:
        rows = cx.execute(sql).mappings().all()
    return {"ok": True, "rows": rows}

# ------------------------------ Static ------------------------------
public_dir = os.path.join(os.path.dirname(__file__), "..", "public")
if os.path.isdir(public_dir):
    app.mount("/public", StaticFiles(directory=public_dir, html=True), name="public")

@app.get("/movements")
def movements_page():
    file_path = os.path.join(public_dir, "movements.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"message": "movements.html not found."}
