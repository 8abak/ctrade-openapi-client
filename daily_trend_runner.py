#!/usr/bin/env python3
"""
Compute zigzag pivots (micro / medium / maxi) and store points + segments.

- micro: built from ticks
- medium: built from micro pivots (same rules)
- maxi: built from medium pivots (same rules)

Schema (relevant):
  ticks(id, "timestamp" timestamptz, bid, ask, mid)
  zigzag_points(level, tick_id, ts, price, kind, run_day)
  micro_trends / medium_trends / maxi_trends:
     (id, start_tick_id, end_tick_id, start_ts, end_ts,
      start_price, end_price, high_price, low_price,
      direction, range_abs GENERATED ALWAYS, duration_s, num_ticks, run_day)

IMPORTANT: Do NOT insert into range_abs (generated).
"""

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx

# -------- Timezone (Python 3.9+ with zoneinfo available) ----------
try:
    from zoneinfo import ZoneInfo
    SYD = ZoneInfo("Australia/Sydney")
except Exception:  # pragma: no cover
    from datetime import timezone
    SYD = timezone(timedelta(hours=10))  # fallback (no DST)

# ============================= DB =================================

def open_conn():
    """Open PostgreSQL connection via DATABASE_URL or simple creds."""
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        dbname="trading",
        user="babak",
        password="babak33044",
        host="localhost",
        port=5432,
    )

# =========================== Models ===============================

@dataclass
class Tick:
    id: int
    ts: datetime
    price: float

@dataclass
class Pivot:
    tick_id: int
    ts: datetime
    price: float
    kind: str  # "peak" or "trough"

@dataclass
class Segment:
    start_tick_id: int
    end_tick_id: int
    start_ts: datetime
    end_ts: datetime
    start_price: float
    end_price: float
    high_price: Optional[float]
    low_price: Optional[float]
    direction: int  # +1 up, -1 down
    duration_s: Optional[int]
    num_ticks: Optional[int]
    run_day: str

# ====================== Core ZigZag Logic =========================

def _zigzag_from_events(events: List[Tuple[int, datetime, float]], threshold: float) -> List[Pivot]:
    """
    Implements the exact rules described:

    1) Keep first tick as anchor.
    2) Wait for first exceed from anchor to set the candidate & direction.
    3) While moving same direction, replace candidate when making a new extreme.
    4) When price breaks threshold in the opposite direction, commit the old
       candidate (peak/trough), and set the breaker tick as the new candidate
       with flipped direction.
    5) Do NOT auto-commit the final candidate at the end of the window.

    Returns committed Pivot list.
    """
    pivots: List[Pivot] = []
    if not events:
        return pivots

    # Anchor: first tick in the window
    anchor_id, anchor_ts, anchor_px = events[0]
    cand_dir: Optional[int] = None  # +1 up, -1 down
    cand_id: Optional[int] = None
    cand_ts: Optional[datetime] = None
    cand_px: Optional[float] = None

    for tid, ts, px in events[1:]:
        if cand_dir is None:
            # Not yet started: look for initial exceed from anchor
            delta = px - anchor_px
            if abs(delta) >= threshold:
                cand_dir = 1 if delta > 0 else -1
                cand_id, cand_ts, cand_px = tid, ts, px
            continue

        # We have a candidate and a direction
        if cand_dir == 1:
            # Extending uptrend?
            if px >= cand_px:
                cand_id, cand_ts, cand_px = tid, ts, px
            # Opposite break (down) enough from the candidate?
            elif (cand_px - px) >= threshold:
                pivots.append(Pivot(cand_id, cand_ts, cand_px, "peak"))
                # Flip direction, new candidate is the breaker tick
                cand_dir = -1
                cand_id, cand_ts, cand_px = tid, ts, px
        else:
            # cand_dir == -1 (downtrend candidate)
            if px <= cand_px:
                cand_id, cand_ts, cand_px = tid, ts, px
            elif (px - cand_px) >= threshold:
                pivots.append(Pivot(cand_id, cand_ts, cand_px, "trough"))
                cand_dir = 1
                cand_id, cand_ts, cand_px = tid, ts, px

    # Do NOT commit last candidate without reversal (per your rule)
    return pivots

def pivots_from_ticks(cur, start_ts: datetime, end_ts: datetime, threshold: float) -> List[Pivot]:
    """Build micro pivots from ticks (prefer mid; fallback to (bid+ask)/2)."""
    cur.execute(
        """
        SELECT id, "timestamp",
               COALESCE(mid, (bid + ask)/2.0) AS price
        FROM ticks
        WHERE "timestamp" >= %s AND "timestamp" < %s
        ORDER BY "timestamp", id
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    events = [(int(r[0]), r[1], float(r[2])) for r in rows]
    return _zigzag_from_events(events, threshold)

def pivots_from_pivots(base_pivots: List[Pivot], threshold: float) -> List[Pivot]:
    """Build higher-level pivots (medium/maxi) from the lower-level pivots."""
    events = [(p.tick_id, p.ts, p.price) for p in base_pivots]
    return _zigzag_from_events(events, threshold)

# ===================== Segment Construction ======================

def compute_segment_stats(cur, a_tick: int, b_tick: int) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    """Count ticks and high/low between two tick IDs (inclusive)."""
    if a_tick is None or b_tick is None:
        return None, None, None
    lo, hi = sorted((a_tick, b_tick))
    cur.execute(
        """
        SELECT COUNT(*) AS n,
               MAX(COALESCE(mid, (bid + ask)/2.0)) AS hi,
               MIN(COALESCE(mid, (bid + ask)/2.0)) AS lo
        FROM ticks
        WHERE id >= %s AND id <= %s
        """,
        (lo, hi),
    )
    n, hi_px, lo_px = cur.fetchone()
    n = int(n or 0)
    hi_val = float(hi_px) if hi_px is not None else None
    lo_val = float(lo_px) if lo_px is not None else None
    return n, hi_val, lo_val

def segments_from_pivots(cur, pivots: List[Pivot], run_day: str) -> List[Segment]:
    """Create adjacent segments from pivots, filling stats from ticks."""
    segs: List[Segment] = []
    for i in range(len(pivots) - 1):
        a = pivots[i]
        b = pivots[i + 1]
        direction = 1 if (b.price >= a.price) else -1
        duration_s = int((b.ts - a.ts).total_seconds())
        num_ticks, hi_px, lo_px = compute_segment_stats(cur, a.tick_id, b.tick_id)
        segs.append(
            Segment(
                start_tick_id=a.tick_id,
                end_tick_id=b.tick_id,
                start_ts=a.ts,
                end_ts=b.ts,
                start_price=a.price,
                end_price=b.price,
                high_price=hi_px,
                low_price=lo_px,
                direction=direction,
                duration_s=duration_s,
                num_ticks=num_ticks,
                run_day=run_day,
            )
        )
    return segs

# ========================= Writers =================================

def insert_points(cur, level: str, run_day: str, points: List[Pivot]) -> None:
    """Insert zigzag points (tick_id is NOT NULL in schema)."""
    if not points:
        return
    sql = """
        INSERT INTO zigzag_points (level, tick_id, ts, price, kind, run_day)
        VALUES %s
    """
    rows = [(level, p.tick_id, p.ts, p.price, p.kind, run_day) for p in points]
    pgx.execute_values(cur, sql, rows, page_size=1000)

def insert_segments(cur, level: str, segs: List[Segment]) -> None:
    """Insert segments into the correct level table (NO range_abs)."""
    if not segs:
        return
    tbl = {"micro": "micro_trends", "medium": "medium_trends", "maxi": "maxi_trends"}[level]
    sql = f"""
        INSERT INTO {tbl} (
            start_tick_id, end_tick_id,
            start_ts, end_ts,
            start_price, end_price, high_price, low_price,
            direction, duration_s, num_ticks, run_day
        )
        VALUES %s
    """
    rows = [(
        s.start_tick_id, s.end_tick_id,
        s.start_ts, s.end_ts,
        s.start_price, s.end_price, s.high_price, s.low_price,
        s.direction, s.duration_s, s.num_ticks, s.run_day
    ) for s in segs]
    pgx.execute_values(cur, sql, rows, page_size=1000)

# ==================== Orchestration / CLI ==========================

def day_window_local(day_str: str) -> Tuple[datetime, datetime]:
    """08:00 local -> next day 07:00 (i.e., +23 hours)."""
    y, m, d = map(int, day_str.split("-"))
    start = datetime(y, m, d, 8, 0, 0, tzinfo=SYD)
    end = start + timedelta(hours=23)
    return start, end

def wipe_day(cur, day: str) -> None:
    """Optionally clear existing rows for a day before recomputing."""
    cur.execute("DELETE FROM zigzag_points WHERE run_day = %s", (day,))
    cur.execute("DELETE FROM micro_trends  WHERE run_day = %s", (day,))
    cur.execute("DELETE FROM medium_trends WHERE run_day = %s", (day,))
    cur.execute("DELETE FROM maxi_trends   WHERE run_day = %s", (day,))

def run_for_day(day: str, micro_thr: float, med_thr: float, maxi_thr: float, wipe: bool) -> None:
    start_ts, end_ts = day_window_local(day)
    print(f"[zig] {day}  window {start_ts.isoformat()} -> {end_ts.isoformat()}  "
          f"(thr micro={micro_thr}, med={med_thr}, maxi={maxi_thr})")

    conn = open_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if wipe:
                wipe_day(cur, day)

            # MICRO from ticks
            micro_pts = pivots_from_ticks(cur, start_ts, end_ts, micro_thr)
            insert_points(cur, "micro", day, micro_pts)
            micro_segs = segments_from_pivots(cur, micro_pts, day)
            insert_segments(cur, "micro", micro_segs)

            # MEDIUM from micro
            med_pts = pivots_from_pivots(micro_pts, med_thr)
            insert_points(cur, "medium", day, med_pts)
            med_segs = segments_from_pivots(cur, med_pts, day)
            insert_segments(cur, "medium", med_segs)

            # MAXI from medium
            maxi_pts = pivots_from_pivots(med_pts, maxi_thr)
            insert_points(cur, "maxi", day, maxi_pts)
            maxi_segs = segments_from_pivots(cur, maxi_pts, day)
            insert_segments(cur, "maxi", maxi_segs)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def main():
    ap = argparse.ArgumentParser(description="Compute/store zigzag pivots and segments for a local trading day.")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (Australia/Sydney)")
    ap.add_argument("--micro", type=float, required=True, help="Micro threshold (price units)")
    ap.add_argument("--medium", type=float, required=True, help="Medium threshold (price units)")
    ap.add_argument("--maxi", type=float, required=True, help="Maxi threshold (price units)")
    ap.add_argument("--wipe", action="store_true", help="Delete existing rows for this day first")
    args = ap.parse_args()
    run_for_day(args.day, args.micro, args.medium, args.maxi, args.wipe)

if __name__ == "__main__":
    main()
