# backend/main.py
import os
import traceback
from datetime import datetime, timedelta, time
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text

# === keep your existing imports for pipeline steps ===
# NOTE: these match your repo layout; if your originals are absolute, keep them absolute.
from label_macro_segments import BuildOrExtendSegments
from label_micro_events import DetectMicroEventsForLatestClosedSegment
from compute_outcomes import ResolveOutcomes
from train_predict import TrainAndPredict

# -------------------------------------------------------------------
# App + DB
# -------------------------------------------------------------------
app = FastAPI(title="cTrade backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(DB_URL)

# -------------------------------------------------------------------
# Small DB helpers
# -------------------------------------------------------------------
def q1(sql: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
    with engine.connect() as cx:
        r = cx.execute(text(sql), params or {}).mappings().first()
        return dict(r) if r else None

def qall(sql: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    with engine.connect() as cx:
        return [dict(r) for r in cx.execute(text(sql), params or {}).mappings().all()]

# -------------------------------------------------------------------
# Snapshot used by the review page
# -------------------------------------------------------------------
def snapshot() -> Dict[str, Any]:
    segs = qall("""
        SELECT segment_id, start_ts, end_ts, direction, confidence,
               start_price, end_price, start_tick_id, end_tick_id
        FROM macro_segments
        ORDER BY end_ts DESC
        LIMIT 200
    """)
    eids = []
    events = []
    if segs:
        events = qall("""
            SELECT e.event_id, e.segment_id, e.tick_id, e.event_type, e.features,
                   t.timestamp AS event_ts, t.mid AS event_price
            FROM micro_events e
            JOIN ticks t ON t.id = e.tick_id
            WHERE e.segment_id = ANY(:ids)
            ORDER BY e.event_id
        """, {"ids": [s["segment_id"] for s in segs]})
        eids = [e["event_id"] for e in events]

    outcomes = qall("""
        SELECT event_id, outcome, tp_hit_ts, sl_hit_ts, timeout_ts,
               horizon_seconds, mfe, mae
        FROM outcomes
        WHERE event_id = ANY(:eids)
    """, {"eids": eids}) if eids else []

    preds = qall("""
        SELECT DISTINCT ON (event_id)
            event_id, model_version, p_tp, threshold, decided, predicted_at
        FROM predictions
        WHERE event_id = ANY(:eids)
        ORDER BY event_id, predicted_at DESC
    """, {"eids": eids}) if eids else []

    return {"segments": segs, "events": events, "outcomes": outcomes, "predictions": preds}

# -------------------------------------------------------------------
# One normal walk-forward step (your canonical order)
# -------------------------------------------------------------------
@app.post("/walkforward/step")
def walkforward_step():
    journal = []
    try:
        journal.append("Build/extend macro segments…")
        m = BuildOrExtendSegments(engine)
        journal.append(f"macro: {m}")

        journal.append("Detect micro events for latest closed segment…")
        me = DetectMicroEventsForLatestClosedSegment(engine)
        journal.append(f"micro: {me}")

        journal.append("Resolve outcomes…")
        oc = ResolveOutcomes(engine)
        journal.append(f"outcomes: {oc}")

        journal.append("Train & predict…")
        pr = TrainAndPredict(engine)
        journal.append(f"predict: {pr}")

        snap = snapshot()
        return {"ok": True, "journal": journal, "snapshot": snap}
    except Exception as e:
        journal.append(traceback.format_exc())
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "journal": journal})

@app.get("/walkforward/snapshot")
def walkforward_snapshot():
    return snapshot()

# -------------------------------------------------------------------
# NEW: Run until next 06:00 Sydney boundary (1h daily gap 06–08; weekends closed)
# -------------------------------------------------------------------
def _last_progress_tick_id() -> int:
    # prefer micro event anchor; fall back to predictions/events anchor; else 0
    r = q1("SELECT COALESCE(MAX(tick_id),0) AS x FROM micro_events")
    if r and r["x"]:
        return int(r["x"])
    r = q1("SELECT COALESCE(MAX(e.tick_id),0) AS x FROM outcomes o JOIN micro_events e USING(event_id)")
    return int(r["x"] or 0)

def _tick_ts_by_id(tid: int) -> Optional[datetime]:
    if tid <= 0:
        r = q1("SELECT MIN(timestamp) AS ts FROM ticks")
        return r["ts"] if r else None
    r = q1("SELECT timestamp AS ts FROM ticks WHERE id=:i", {"i": tid})
    return r["ts"] if r else None

def _next_session_end_after(ts: datetime) -> datetime:
    # Timestamps in your DB already carry +10:00; just compute next local 06:00 strictly after ts.
    cand = datetime.combine(ts.date(), time(6, 0, 0), tzinfo=ts.tzinfo)
    if ts >= cand:
        cand = cand + timedelta(days=1)
    # If we hit Sunday 06:00, push to Monday 06:00 (no sessions on weekend)
    if cand.weekday() == 6:  # Sunday
        cand = cand + timedelta(days=1)
    return cand

def _last_tick_id_before(ts: datetime) -> int:
    r = q1("SELECT COALESCE(MAX(id),0) AS x FROM ticks WHERE timestamp < :ts", {"ts": ts})
    return int(r["x"] or 0)

def _progress_counters() -> Dict[str, int]:
    r1 = q1("SELECT COALESCE(COUNT(*),0) AS c FROM macro_segments")
    r2 = q1("SELECT COALESCE(COUNT(*),0) AS c FROM micro_events")
    r3 = q1("SELECT COALESCE(COUNT(*),0) AS c FROM outcomes")
    r4 = q1("SELECT COALESCE(COUNT(*),0) AS c FROM predictions")
    r5 = q1("SELECT COALESCE(MAX(id),0) AS mx FROM ticks")
    r6 = q1("SELECT COALESCE(MAX(tick_id),0) AS mx FROM micro_events")
    return {
        "macro_segments": int(r1["c"]),
        "micro_events": int(r2["c"]),
        "outcomes": int(r3["c"]),
        "predictions": int(r4["c"]),
        "ticks_max_id": int(r5["mx"]),
        "micro_max_tick": int(r6["mx"]),
    }

def _step_once(journal: List[str]):
    journal.append("Build/extend macro segments…")
    BuildOrExtendSegments(engine)

    journal.append("Detect micro events…")
    DetectMicroEventsForLatestClosedSegment(engine)

    journal.append("Resolve outcomes…")
    ResolveOutcomes(engine)

    journal.append("Train & predict…")
    TrainAndPredict(engine)

@app.post("/walkforward/run_day")
def run_whole_day():
    """
    From the last processed tick, loop the standard step until we reach the
    last tick before the next 06:00 boundary.
    """
    journal: List[str] = []
    before = _progress_counters()

    start_tid = _last_progress_tick_id()
    start_ts = _tick_ts_by_id(start_tid)
    if not start_ts:
        return {"ok": False, "reason": "No ticks in DB."}

    day_end_ts = _next_session_end_after(start_ts)
    target_tid = _last_tick_id_before(day_end_ts)

    journal.append(f"Start tick = {start_tid} @ {start_ts.isoformat()}")
    journal.append(f"Target boundary = {day_end_ts.isoformat()} (tick < boundary)")
    journal.append(f"Target tick id = {target_tid}")

    # loop while we make progress and haven't reached target
    last_seen = _last_progress_tick_id()
    iters = 0
    HARD_CAP = 2000

    while iters < HARD_CAP and last_seen < target_tid:
        _step_once(journal)
        now_seen = _last_progress_tick_id()
        journal.append(f"progress: {last_seen} → {now_seen}")
        if now_seen <= last_seen:
            journal.append("No further progress; breaking.")
            break
        last_seen = now_seen
        iters += 1

    after = _progress_counters()
    snap = snapshot()

    return {
        "ok": True,
        "iterations": iters,
        "start_tick_id": start_tid,
        "target_tick_id": target_tid,
        "start_ts": start_ts.isoformat(),
        "day_end_ts": day_end_ts.isoformat(),
        "before": before,
        "after": after,
        "journal": journal,
        "snapshot": snap,
    }

# -------------------------------------------------------------------
# Misc convenience routes you already had (kept minimal)
# -------------------------------------------------------------------
@app.get("/version")
def version():
    return {"version": "2025-08-27.whole-day+report"}

public_dir = os.path.join(os.path.dirname(__file__), "..", "public")
if os.path.isdir(public_dir):
    app.mount("/public", StaticFiles(directory=public_dir, html=True), name="public")

@app.get("/movements")
def movements_page():
    path = os.path.join(public_dir, "movements.html")
    if os.path.exists(path):
        return FileResponse(path)
    return {"message": "movements.html not found."}
