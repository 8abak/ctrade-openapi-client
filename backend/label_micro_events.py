# backend/label_micro_events.py
# Purpose: Find micro-entry candidates in the most recent CLOSED macro segment:
#  - pullback_end: counter-move then fast-MA resumes in macro direction
#  - breakout: narrow consolidation (low std) -> break in macro direction
#  - retest_hold: breakout, then retest prior level and hold
# Stores compact features JSONB (FeatureVersion). Idempotent via unique index.

import os, math
from datetime import timedelta
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import create_engine, text

def _GetEngine():
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
    )
    return create_engine(db_url)

def _LastClosedSegment(conn) -> Optional[Dict[str,Any]]:
    row = conn.execute(text("""
        SELECT segment_id, start_ts, end_ts, direction, start_tick_id, end_tick_id, start_price, end_price
        FROM macro_segments
        ORDER BY end_ts DESC
        LIMIT 1
    """)).mappings().first()
    return dict(row) if row else None

def _FetchTicksRange(conn, a_id: int, b_id: int) -> List[Dict[str,Any]]:
    rows = conn.execute(text("""
        SELECT id, timestamp, mid
        FROM ticks
        WHERE id BETWEEN :a AND :b
        ORDER BY id ASC
    """), {"a": a_id, "b": b_id}).mappings().all()
    return [dict(r) for r in rows]

def _SMA(seq, n):
    out = []
    s = 0.0
    q = []
    for v in seq:
        q.append(v); s += v
        if len(q) > n: s -= q.pop(0)
        out.append(s / len(q))
    return out

def _RollingStd(seq, n):
    out = []
    q = []
    for v in seq:
        q.append(v)
        if len(q) > n: q.pop(0)
        m = sum(q)/len(q)
        var = sum((x-m)*(x-m) for x in q)/len(q)
        out.append(math.sqrt(var))
    return out

def _SessionBucket(ts) -> str:
    # Crude: bucket by UTC hour (tweak later)
    h = ts.hour
    if 21 <= h or h < 6:  return "Asia"
    if 6 <= h < 13:       return "Europe"
    return "US"

def _InsertEvent(conn, seg_id:int, tick_id:int, etype:str, features:Dict[str,Any]):
    conn.execute(text("""
        INSERT INTO micro_events (segment_id, tick_id, event_type, features)
        VALUES (:seg, :tid, :tp, CAST(:feat AS jsonb))
        ON CONFLICT DO NOTHING
    """), {"seg": seg_id, "tid": tick_id, "tp": etype, "feat": features})

def DetectMicroEventsForLatestClosedSegment(FeatureVersion: str = "v1") -> Dict[str,Any]:
    eng = _GetEngine()
    created = 0
    with eng.begin() as conn:
        seg = _LastClosedSegment(conn)
        if not seg:
            return {"segment_id": None, "events_added": 0}

        ticks = _FetchTicksRange(conn, seg["start_tick_id"], seg["end_tick_id"])
        if len(ticks) < 50:
            return {"segment_id": seg["segment_id"], "events_added": 0}

        prices = [float(t["mid"]) for t in ticks]
        fast = _SMA(prices, 20)     # ~short window
        slow = _SMA(prices, 100)    # ~longer window
        vol  = _RollingStd(prices, 60)

        # geometry helpers
        leg_dir = int(seg["direction"])
        start_px, end_px = float(seg["start_price"]), float(seg["end_price"])
        run = end_px - start_px if leg_dir == 1 else start_px - end_px
        run = max(1e-6, run)

        # 1) Pullback-end: fast below slow against trend, then crosses back with regained slope
        for i in range(120, len(prices)):
            prev_down = (leg_dir==1 and fast[i-5] < slow[i-5]) or (leg_dir==-1 and fast[i-5] > slow[i-5])
            cross_back = (leg_dir==1 and fast[i] >= slow[i]) or (leg_dir==-1 and fast[i] <= slow[i])
            slope_ok = (fast[i] - fast[i-5]) * leg_dir > 0
            if prev_down and cross_back and slope_ok:
                pos_in_leg = i/len(prices)
                feat = {
                    "FeatureVersion": FeatureVersion,
                    "Family": "pullback_end",
                    "FastSlope": (fast[i] - fast[i-5]),
                    "SlowGap":  abs(fast[i]-slow[i]),
                    "PosInLeg": round(pos_in_leg, 3),
                    "VolNow":   vol[i],
                    "Session":  _SessionBucket(ticks[i]["timestamp"])
                }
                _InsertEvent(conn, seg["segment_id"], ticks[i]["id"], "pullback_end", feat)
                created += 1

        # 2) Breakout: consolidation (low std) then price pushes in trend direction > threshold
        for i in range(120, len(prices)):
            window_std = sum(vol[max(0,i-60):i]) / max(1, len(vol[max(0,i-60):i]))
            if window_std < 0.25:  # tune later
                push = (prices[i] - prices[i-20]) * leg_dir
                if push > 0.8:      # tune later
                    feat = {
                        "FeatureVersion": FeatureVersion,
                        "Family": "breakout",
                        "Std60Avg": window_std,
                        "Push20":  push,
                        "Session": _SessionBucket(ticks[i]["timestamp"])
                    }
                    _InsertEvent(conn, seg["segment_id"], ticks[i]["id"], "breakout", feat)
                    created += 1

        # 3) Retest-hold: recent high/low broken then retested w/o violation
        swing_win = 60
        for i in range(2*swing_win, len(prices)):
            if leg_dir == 1:
                prior_hi = max(prices[i-swing_win:i])
                broke = prices[i-1] > prior_hi and prices[i] > prior_hi
                if broke:
                    # look forward a bit within the segment for retest
                    j = min(len(prices)-1, i+40)
                    if prices[j] >= prior_hi and min(prices[i:j+1]) >= prior_hi - 0.3:
                        feat = {
                            "FeatureVersion": FeatureVersion,
                            "Family": "retest_hold",
                            "Level": prior_hi,
                            "Lookahead": j - i,
                            "Session": _SessionBucket(ticks[i]["timestamp"])
                        }
                        _InsertEvent(conn, seg["segment_id"], ticks[i]["id"], "retest_hold", feat)
                        created += 1
            else:
                prior_lo = min(prices[i-swing_win:i])
                broke = prices[i-1] < prior_lo and prices[i] < prior_lo
                if broke:
                    j = min(len(prices)-1, i+40)
                    if prices[j] <= prior_lo and max(prices[i:j+1]) <= prior_lo + 0.3:
                        feat = {
                            "FeatureVersion": FeatureVersion,
                            "Family": "retest_hold",
                            "Level": prior_lo,
                            "Lookahead": j - i,
                            "Session": _SessionBucket(ticks[i]["timestamp"])
                        }
                        _InsertEvent(conn, seg["segment_id"], ticks[i]["id"], "retest_hold", feat)
                        created += 1

    return {"segment_id": seg["segment_id"], "events_added": created}
