# backend/compute_outcomes.py
# Purpose: Resolve outcomes for unresolved micro_events: first-touch of +$2/-$1 within 60 min.
# Uses macro direction for TP side; price = mid at event tick.

import os
from datetime import timedelta
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text

def _GetEngine():
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
    )
    return create_engine(db_url)

def ResolveOutcomes(MaxMinutes:int = 60, UpUSD:float=2.0, DownUSD:float=1.0) -> Dict[str,Any]:
    eng = _GetEngine()
    done = 0
    with eng.begin() as conn:
        # unresolved events within any closed segment
        rows = conn.execute(text("""
            SELECT e.event_id, e.segment_id, e.tick_id, ms.direction
            FROM micro_events e
            JOIN macro_segments ms ON ms.segment_id = e.segment_id
            LEFT JOIN outcomes o ON o.event_id = e.event_id
            WHERE o.event_id IS NULL
        """)).mappings().all()

        for r in rows:
            eid = r["event_id"]; seg_dir = int(r["direction"]); tid = r["tick_id"]
            # Get event tick (ts, px)
            t0 = conn.execute(text("""
                SELECT timestamp, mid FROM ticks WHERE id = :tid
            """), {"tid": tid}).mappings().first()
            if not t0: 
                continue
            ts0, px0 = t0["timestamp"], float(t0["mid"])
            tp = px0 + (UpUSD * seg_dir)
            sl = px0 - (DownUSD * seg_dir)
            until = ts0 + timedelta(minutes=MaxMinutes)

            # forward scan (bounded)
            fwd = conn.execute(text("""
                SELECT timestamp, mid
                FROM ticks
                WHERE timestamp > :ts0 AND timestamp <= :until
                ORDER BY timestamp ASC
            """), {"ts0": ts0, "until": until}).mappings().all()
            if not fwd:
                continue

            tp_hit_ts = None
            sl_hit_ts = None
            mfe = -1e9
            mae = +1e9

            for row in fwd:
                px = float(row["mid"])
                # update extremes relative to entry
                mfe = max(mfe, (px - px0)*seg_dir)
                mae = min(mae, (px - px0)*seg_dir)

                if tp_hit_ts is None and ((px - tp) * seg_dir >= 0):
                    tp_hit_ts = row["timestamp"]
                    break
                if sl_hit_ts is None and ((px - sl) * seg_dir <= 0):
                    sl_hit_ts = row["timestamp"]
                    break

            outcome = "Timeout"; tp_ts = None; sl_ts = None; to_ts = until
            if tp_hit_ts and not sl_hit_ts:
                outcome = "TP"; tp_ts = tp_hit_ts; to_ts = None
            elif sl_hit_ts and not tp_hit_ts:
                outcome = "SL"; sl_ts = sl_hit_ts; to_ts = None
            elif tp_hit_ts and sl_hit_ts:
                # first-touch rule: whichever came earlier
                if tp_hit_ts <= sl_hit_ts:
                    outcome = "TP"; tp_ts = tp_hit_ts; sl_ts = None; to_ts = None
                else:
                    outcome = "SL"; sl_ts = sl_hit_ts; tp_ts = None; to_ts = None

            conn.execute(text("""
                INSERT INTO outcomes (event_id, outcome, tp_hit_ts, sl_hit_ts, timeout_ts, horizon_seconds, mfe, mae)
                VALUES (:eid, :out, :tp, :sl, :to, :hz, :mfe, :mae)
                ON CONFLICT (event_id) DO NOTHING
            """), {
                "eid": eid, "out": outcome,
                "tp": tp_ts, "sl": sl_ts, "to": to_ts,
                "hz": MaxMinutes*60,
                "mfe": round(mfe,5) if mfe>-1e9 else None,
                "mae": round(mae,5) if mae<+1e9 else None
            })
            done += 1

    return {"outcomes_resolved": done}
