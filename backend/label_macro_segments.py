# backend/label_macro_segments.py
# Purpose: Build/extend macro segments using $6 Renko/ZigZag logic.
# Deps: SQLAlchemy (engine), PostgreSQL, ticks(id, timestamp, mid).
# Confidence proxy: slow EMA slope magnitude (as "slow Kalman/VWAP" proxy).
# Idempotent: will not duplicate closed segments.

import os
from datetime import timedelta
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy import create_engine, text

RENKO_SIZE_USD_DEFAULT = 6.0

def _GetEngine():
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
    )
    return create_engine(db_url)

def _FetchTicksFrom(conn, after_tick_id: Optional[int], limit:int=200_000) -> List[Dict[str,Any]]:
    if after_tick_id is None:
        q = text("""
            SELECT id, timestamp, mid
            FROM ticks
            ORDER BY id ASC
            LIMIT :limit
        """)
        params = {"limit": limit}
    else:
        q = text("""
            SELECT id, timestamp, mid
            FROM ticks
            WHERE id > :after
            ORDER BY id ASC
            LIMIT :limit
        """)
        params = {"after": after_tick_id, "limit": limit}
    return [dict(r._mapping) for r in conn.execute(q, params)]

def _LastClosedSegment(conn) -> Optional[Dict[str,Any]]:
    row = conn.execute(text("""
        SELECT segment_id, start_ts, end_ts, direction, start_price, end_price, start_tick_id, end_tick_id
        FROM macro_segments
        ORDER BY end_ts DESC
        LIMIT 1
    """)).mappings().first()
    return dict(row) if row else None

def _Ema(values, alpha: float) -> List[float]:
    out = []
    s = None
    for v in values:
        s = v if s is None else (alpha*v + (1-alpha)*s)
        out.append(s)
    return out

def _ConfidenceFromSlowEma(prices: List[float], seconds: List[float]) -> float:
    # crude slope magnitude normalized by $ scale; guards for short lists
    if len(prices) < 5: return 0.0
    ema = _Ema(prices, alpha=0.02)  # very slow
    # slope via last vs middle
    y0, y1 = ema[max(0, len(ema)-5)], ema[-1]
    # seconds spacing approximate
    dt = max(1.0, seconds[-1] - seconds[max(0, len(seconds)-5)])
    slope = (y1 - y0) / dt
    # normalize around renko size per ~hour; simple clamp
    conf = min(1.0, max(0.0, abs(slope) * 3600.0 / 6.0))
    return float(conf)

def BuildOrExtendSegments(RenkoSizeUSD: float = RENKO_SIZE_USD_DEFAULT) -> Dict[str, Any]:
    """
    Walks forward from the end of the last closed segment using Renko($6) logic:
    - Start when price moves >= RenkoSizeUSD from seed.
    - Maintain extreme; when reversal >= RenkoSizeUSD, close prior segment.
    Confidence = slow EMA slope magnitude (0..1).
    Returns summary counts.
    """
    eng = _GetEngine()
    made = 0
    last_end_tick: Optional[int] = None

    with eng.begin() as conn:
        last = _LastClosedSegment(conn)
        if last:
            last_end_tick = last["end_tick_id"]

        ticks = _FetchTicksFrom(conn, last_end_tick, limit=500_000)
        if not ticks:
            return {"segments_added": 0}

        # Seed at first unseen tick
        seed_id  = ticks[0]["id"]
        seed_ts  = ticks[0]["timestamp"]
        seed_px  = float(ticks[0]["mid"])

        leg_dir  = 0   # +1 up, -1 dn
        leg_start_id, leg_start_ts, leg_start_px = seed_id, seed_ts, seed_px
        extreme_px = seed_px
        extreme_id = seed_id
        extreme_ts = seed_ts

        # if we continue from a prior closed leg, keep state empty; we only add fully confirmed legs
        for t in ticks[1:]:
            tid = t["id"]; ts = t["timestamp"]; px = float(t["mid"])

            if leg_dir == 0:
                # Wait for first box from seed
                if px >= seed_px + RenkoSizeUSD:
                    leg_dir = +1
                    leg_start_id, leg_start_ts, leg_start_px = seed_id, seed_ts, seed_px
                    extreme_px, extreme_id, extreme_ts = px, tid, ts
                elif px <= seed_px - RenkoSizeUSD:
                    leg_dir = -1
                    leg_start_id, leg_start_ts, leg_start_px = seed_id, seed_ts, seed_px
                    extreme_px, extreme_id, extreme_ts = px, tid, ts
                continue

            # Update extreme within current leg
            if leg_dir == +1 and px > extreme_px:
                extreme_px, extreme_id, extreme_ts = px, tid, ts
            if leg_dir == -1 and px < extreme_px:
                extreme_px, extreme_id, extreme_ts = px, tid, ts

            # Check reversal confirm
            if leg_dir == +1 and px <= extreme_px - RenkoSizeUSD:
                # Close UP leg from leg_start to extreme
                # Compute confidence on subseries
                sub = conn.execute(text("""
                    SELECT timestamp, mid FROM ticks
                    WHERE id BETWEEN :a AND :b
                    ORDER BY id ASC
                """), {"a": leg_start_id, "b": extreme_id}).mappings().all()
                secs = [(row["timestamp"] - sub[0]["timestamp"]).total_seconds() for row in sub]
                conf = _ConfidenceFromSlowEma([float(r["mid"]) for r in sub], secs)
                conn.execute(text("""
                    INSERT INTO macro_segments
                    (start_ts, end_ts, direction, start_price, end_price, confidence, start_tick_id, end_tick_id)
                    VALUES (:st, :et, :dir, :sp, :ep, :conf, :sid, :eid)
                    ON CONFLICT DO NOTHING
                """), {
                    "st": leg_start_ts, "et": extreme_ts, "dir": +1,
                    "sp": leg_start_px, "ep": extreme_px, "conf": conf,
                    "sid": leg_start_id, "eid": extreme_id
                })
                made += 1
                # New leg seeds from pivot extreme
                seed_id, seed_ts, seed_px = extreme_id, extreme_ts, extreme_px
                leg_dir = -1
                leg_start_id, leg_start_ts, leg_start_px = seed_id, seed_ts, seed_px
                extreme_px, extreme_id, extreme_ts = px, tid, ts

            elif leg_dir == -1 and px >= extreme_px + RenkoSizeUSD:
                # Close DOWN leg from leg_start to extreme
                sub = conn.execute(text("""
                    SELECT timestamp, mid FROM ticks
                    WHERE id BETWEEN :a AND :b
                    ORDER BY id ASC
                """), {"a": leg_start_id, "b": extreme_id}).mappings().all()
                secs = [(row["timestamp"] - sub[0]["timestamp"]).total_seconds() for row in sub]
                conf = _ConfidenceFromSlowEma([float(r["mid"]) for r in sub], secs)
                conn.execute(text("""
                    INSERT INTO macro_segments
                    (start_ts, end_ts, direction, start_price, end_price, confidence, start_tick_id, end_tick_id)
                    VALUES (:st, :et, :dir, :sp, :ep, :conf, :sid, :eid)
                    ON CONFLICT DO NOTHING
                """), {
                    "st": leg_start_ts, "et": extreme_ts, "dir": -1,
                    "sp": leg_start_px, "ep": extreme_px, "conf": conf,
                    "sid": leg_start_id, "eid": extreme_id
                })
                made += 1
                seed_id, seed_ts, seed_px = extreme_id, extreme_ts, extreme_px
                leg_dir = +1
                leg_start_id, leg_start_ts, leg_start_px = seed_id, seed_ts, seed_px
                extreme_px, extreme_id, extreme_ts = px, tid, ts

    return {"segments_added": made}
