#!/usr/bin/env python3
"""
Compute zigzag pivots (micro/medium/maxi) and store results.

- micro: computed from ticks
- medium: computed from micro pivots (same algorithm)
- maxi: computed from medium pivots (same algorithm)

Tables used:
  ticks(id, timestamp, ..., mid)
  zigzag_points(level, tick_id, ts, price, kind, run_day)
  micro_trends / medium_trends / maxi_trends:
     (id, start_tick_id, end_tick_id, start_ts, end_ts,
      start_price, end_price, high_price, low_price,
      direction, range_abs GENERATED ALWAYS, duration_s, num_ticks, run_day)

IMPORTANT: we NEVER insert into range_abs (generated column).
"""

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import psycopg2
import psycopg2.extras as pgx

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
    SYD = ZoneInfo("Australia/Sydney")
except Exception:
    # Fallback: +10:00 fixed (no DST). Only used if zoneinfo unavailable.
    from datetime import timezone
    SYD = timezone(timedelta(hours=10))


# --------------------------- DB Helpers -------------------------------------

def open_conn():
    """Open a PostgreSQL connection.

    If DATABASE_URL is present, use it; otherwise use your simple credentials.
    """
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


# --------------------------- Data Classes -----------------------------------

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


# --------------------------- Core ZigZag Logic ------------------------------

def _zigzag_from_events(
    events: List[Tuple[int, datetime, float]],
    threshold: float
) -> List[Pivot]:
    """
    events: list of (tick_id, ts, price) in time order.
    Implements the exact rules you listed.

    Returns committed pivots (candidate is only committed on an opposite break).
    """
    pivots: List[Pivot] = []
    if not events:
        return pivots

    # Anchor (the very first tick)
    anchor_id, anchor_ts, anchor_price = events[0]

    cand_dir: Optional[int] = None
    cand_id: Optional[int] = None
    cand_ts: Optional[datetime] = None
    cand_price: Optional[float] = None

    for (tid, ts, px) in events[1:]:
        if cand_dir is None:
            # Wait until first exceed from anchor
            delta = px - anchor_price
            if abs(delta) >= threshold:
                cand_dir = 1 if delta > 0 else -1
                cand_id, cand_ts, cand_price = tid, ts, px
            continue

        # If moving in the same direction as candidate, extend to new extreme
        if cand_dir == 1:
            if px >= cand_price:
                cand_id, cand_ts, cand_price = tid, ts, px
            # Opposite break? (down from candidate)
            elif (cand_price - px) >= threshold:
                pivots.append(Pivot(cand_id, cand_ts, cand_price, "peak"))
                # new candidate becomes the breaking tick, and direction flips
                cand_dir = -1
                cand_id, cand_ts, cand_price = tid, ts, px
                # Move the anchor to the committed pivot (per your rule 1-7)
                anchor_id, anchor_ts, anchor_price = cand_id, cand_ts, cand_price
        else:  # cand_dir == -1
            if px <= cand_price:
                cand_id, cand_ts, cand_price = tid, ts, px
            elif (px - cand_price) >= threshold:
                pivots.append(Pivot(cand_id, cand_ts, cand_price, "trough"))
                cand_dir = 1
                cand_id, cand_ts, cand_price = tid, ts, px
                anchor_id, anchor_ts, anchor_price = cand_id, cand_ts, cand_price

    # NOTE: classic zigzag does not commit the last candidate without reversal.
    # If you want to *also* store last candidate as a softer point, you could
    # uncomment below – but you said commit on reversal, so we keep it off.
    # if cand_dir is not None and cand_id is not None:
    #     kind = "peak" if cand_dir == 1 else "trough"
    #     pivots.append(Pivot(cand_id, cand_ts, cand_price, kind))

    return pivots


def pivots_from_ticks(cur, start_ts: datetime, end_ts: datetime, threshold: float) -> List[Pivot]:
    """Build micro pivots from ticks (uses mid; falls back to (bid+ask)/2)."""
    cur.execute(
        """
        SELECT id, "timestamp", 
               COALESCE(mid, (bid + ask)/2.0) AS price
        FROM ticks
        WHERE "timestamp" >= %s AND "timestamp" < %s
        ORDER BY id
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    events = [(r[0], r[1], float(r[2])) for r in rows]
    return _zigzag_from_events(events, threshold)


def pivots_from_pivots(base_pivots: List[Pivot], threshold: float) -> List[Pivot]:
    """Build med/max pivots from the previous level’s pivots."""
    events = [(p.tick_id, p.ts, p.price) for p in base_pivots]
    return _zigzag_from_events(events, threshold)


# --------------------------- Segment Builders -------------------------------

def compute_segment_stats(cur, a_tick: int, b_tick: int) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    """
    Count ticks and compute high/low between two tick IDs inclusive of end.
    """
    if a_tick is None or b_tick is None:
        return None, None, None

    lo = min(a_tick, b_tick)
    hi = max(a_tick, b_tick)
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
    return int(n or 0), (float(hi_px) if hi_px is not None else None), (float(lo_px) if lo_px is not None else None)


def segments_from_pivots(cur, pivots: List[Pivot], run_day: str) -> List[Segment]:
    """Create adjacent segments from pivots, filling stats from ticks."""
    segs: List[Segment] = []
    for i in range(len(pivots) - 1):
        a = pivots[i]
        b = pivots[i + 1]
        direction = 1 if b.price >= a.price else -1
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


# --------------------------- DB Writers -------------------------------------

# points (note: tick_id is required and NOT NULL)
def insert_points(cur, level, run_day, pts):
    # pts: list of dicts with keys: tick_id, ts, price, kind
    sql = """
        INSERT INTO zigzag_points (level, tick_id, ts, price, kind, run_day)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    rows = [
        (level, p["tick_id"], p["ts"], p["price"], p["kind"], run_day)
        for p in pts
    ]
    pgx.execute_values(cur, sql, rows, page_size=500)


# segments (NO range_abs!)
def insert_segments(cur, table_name, run_day, segs):
    """
    segs: list of dicts with keys:
      start_tick_id, end_tick_id, start_ts, end_ts,
      start_price, end_price, high_price, low_price,
      direction, duration_s, num_ticks
    """
    sql = f"""
        INSERT INTO {table_name} (
            start_tick_id, end_tick_id,
            start_ts, end_ts,
            start_price, end_price, high_price, low_price,
            direction, duration_s, num_ticks, run_day
        )
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    rows = [
        (
            s["start_tick_id"], s["end_tick_id"],
            s["start_ts"], s["end_ts"],
            s["start_price"], s["end_price"], s["high_price"], s["low_price"],
            s["direction"], s["duration_s"], s["num_ticks"], run_day
        )
        for s in segs
    ]
    pgx.execute_values(cur, sql, rows, page_size=500)


# --------------------------- Orchestration ----------------------------------

def day_window_local(day_str: str) -> Tuple[datetime, datetime]:
    """Return local 08:00 -> next-day 07:00 (Australia/Sydney)."""
    y, m, d = map(int, day_str.split("-"))
    start = datetime(y, m, d, 8, 0, 0, tzinfo=SYD)
    end = start + timedelta(hours=23, minutes=0) + timedelta(hours=23)  # wrong
    # Correction: we want 8:00 -> next-day 7:00 (i.e., +23 hours)
    end = start + timedelta(hours=23)
    return start, end


def run_for_day(day: str, micro_thr: float, med_thr: float, maxi_thr: float) -> None:
    start_ts, end_ts = day_window_local(day)
    print(f"[zig] {day}  window {start_ts.isoformat()} -> {end_ts.isoformat()}  "
          f"(thr micro={micro_thr}, med={med_thr}, maxi={maxi_thr})")

    conn = open_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # MICRO
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


# --------------------------- CLI --------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Compute/store zigzag pivots and segments for a day.")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (local Australia/Sydney trading day)")
    ap.add_argument("--micro", type=float, required=True, help="Micro threshold")
    ap.add_argument("--medium", type=float, required=True, help="Medium threshold")
    ap.add_argument("--maxi", type=float, required=True, help="Maxi threshold")
    args = ap.parse_args()

    run_for_day(args.day, args.micro, args.medium, args.maxi)


if __name__ == "__main__":
    main()
