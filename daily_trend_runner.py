#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZigZag daily runner.
- Micro: computed from raw ticks using absolute threshold
- Medium: computed from confirmed Micro pivots
- Maxi: computed from confirmed Medium pivots
Day window: local (Australia/Sydney) 08:00 -> next day 07:00
Python 3.9 compatible.
"""

import os
import sys
import argparse
from dataclasses import dataclass
from typing import List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Timezone handling (3.9) ---
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None

AUS_TZ_NAME = "Australia/Sydney"  # handles DST if system tzdata present


# ---------- Data shapes ----------

@dataclass
class Node:
    """A single data point that can become a pivot (always carries tick id)."""
    id: int
    ts: datetime
    price: float


@dataclass
class Segment:
    """Zigzag segment from a confirmed pivot to the next confirmed pivot."""
    start: Node
    end: Node


@dataclass
class Point:
    """A confirmed pivot."""
    ts: datetime
    price: float
    kind: str           # 'peak' or 'trough'
    tick_id: int        # non-null, always sourced from a Node


# ---------- DB helpers ----------

def get_engine() -> Engine:
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: env DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)
    # Typical: postgresql+psycopg2://user:pass@host:5432/dbname
    return create_engine(url, future=True)


# ---------- Windows & data fetch ----------

def day_window_local(day_yyyy_mm_dd: str) -> Tuple[datetime, datetime]:
    """
    Build local AU/Sydney 08:00 -> next day 07:00 window,
    returned as timezone-aware datetimes.
    """
    if ZoneInfo is None:
        # Fallback to fixed +10 if tzdata missing; still returns tz-aware
        tz = timezone(timedelta(hours=10))
    else:
        tz = ZoneInfo(AUS_TZ_NAME)

    d = datetime.strptime(day_yyyy_mm_dd, "%Y-%m-%d").replace(tzinfo=None)
    start = d.replace(hour=8, minute=0, second=0, microsecond=0)
    start = start.replace(tzinfo=tz)
    end = (start + timedelta(days=1)) - timedelta(hours=1)  # next day 07:00
    # safer/more explicit:
    end = start + timedelta(hours=23)
    return start, end


def fetch_ticks(engine: Engine, start: datetime, end: datetime) -> List[Node]:
    """
    Pull ticks in [start, end) ordered by id.
    Uses mid if present else (bid+ask)/2.0.
    """
    sql = text("""
        SELECT id, "timestamp" AS ts,
               COALESCE(mid, (bid + ask) / 2.0) AS px
        FROM ticks
        WHERE "timestamp" >= :a AND "timestamp" < :b
        ORDER BY id
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"a": start, "b": end}).mappings().all()
    return [Node(id=r["id"], ts=r["ts"], price=float(r["px"])) for r in rows]


# ---------- ZigZag core algorithm ----------

def build_zig(series: List[Node], threshold: float) -> Tuple[List[Segment], List[Point]]:
    """
    Implements the exact spec:
      - wait until price moves threshold from the initial anchor to set the first direction & candidate
      - keep updating candidate while moving further in same direction
      - confirm candidate as pivot only when price reverses threshold in opposite direction
      - on confirmation, start new candidate with the reversing tick
    Returns (segments, points). Points are confirmed pivots; segments connect successive pivots.
    """
    segs: List[Segment] = []
    pts: List[Point] = []

    if not series or threshold <= 0:
        return segs, pts

    anchor = series[0]               # first tick
    direction: Optional[str] = None  # 'up' or 'down'
    candidate: Optional[Node] = None
    last_pivot: Optional[Node] = None

    for n in series[1:]:
        if direction is None:
            diff = n.price - anchor.price
            if abs(diff) >= threshold:
                direction = "up" if diff > 0 else "down"
                candidate = n
            # else still waiting to establish direction
            continue

        # Direction is established; update candidate/extreme.
        if direction == "up":
            if candidate is None or n.price >= candidate.price:
                candidate = n
            # reversal check: drop from the extreme >= threshold
            if candidate.price - n.price >= threshold:
                # Confirm candidate as PEAK
                pivot = candidate
                pts.append(Point(ts=pivot.ts, price=pivot.price, kind="peak", tick_id=pivot.id))
                if last_pivot is not None:
                    segs.append(Segment(start=last_pivot, end=pivot))
                last_pivot = pivot
                # flip direction, start new candidate with current tick
                direction = "down"
                candidate = n
        else:  # direction == "down"
            if candidate is None or n.price <= candidate.price:
                candidate = n
            # reversal check: rise from extreme >= threshold
            if n.price - candidate.price >= threshold:
                # Confirm candidate as TROUGH
                pivot = candidate
                pts.append(Point(ts=pivot.ts, price=pivot.price, kind="trough", tick_id=pivot.id))
                if last_pivot is not None:
                    segs.append(Segment(start=last_pivot, end=pivot))
                last_pivot = pivot
                # flip direction
                direction = "up"
                candidate = n

    # Note: the last candidate is NOT confirmed unless a reversal occurred (spec).
    return segs, pts


# ---------- Writers (delete-then-insert for run_day) ----------

def wipe_day(engine: Engine, day: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM zigzag_points WHERE run_day = :d"), {"d": day})
        conn.execute(text("DELETE FROM micro_trends  WHERE run_day = :d"), {"d": day})
        conn.execute(text("DELETE FROM medium_trends WHERE run_day = :d"), {"d": day})
        conn.execute(text("DELETE FROM maxi_trends   WHERE run_day = :d"), {"d": day})


def write_points(engine: Engine, level: str, run_day: str, points: List[Point]) -> None:
    if not points:
        return
    sql = text("""
        INSERT INTO zigzag_points (level, tick_id, ts, price, kind, run_day)
        VALUES (:lvl, :tid, :ts, :px, :kind, :day)
    """)
    payload = [{"lvl": level, "tid": p.tick_id, "ts": p.ts, "px": p.price,
                "kind": p.kind, "day": run_day} for p in points]
    with engine.begin() as conn:
        conn.execute(sql, payload)


def write_segments(engine: Engine, table: str, run_day: str,
                   segs: List[Segment], *, count_ticks: bool) -> None:
    if not segs:
        return
    sql = text(f"""
        INSERT INTO {table}
          (start_tick_id, end_tick_id, start_ts, end_ts, start_price, end_price,
           direction, range_abs, duration_s, num_ticks, run_day)
        VALUES
          (:sid, :eid, :a_ts, :b_ts, :a_px, :b_px, :dir, :rng, :dur, :nticks, :day)
    """)
    rows = []
    for s in segs:
        direction = 1 if s.end.price >= s.start.price else -1
        rng = abs(s.end.price - s.start.price)
        dur = (s.end.ts - s.start.ts).total_seconds()
        nticks = (s.end.id - s.start.id) if count_ticks else 0
        rows.append({
            "sid": s.start.id, "eid": s.end.id,
            "a_ts": s.start.ts, "b_ts": s.end.ts,
            "a_px": s.start.price, "b_px": s.end.price,
            "dir": direction, "rng": rng, "dur": dur,
            "nticks": nticks, "day": run_day
        })
    with engine.begin() as conn:
        conn.execute(sql, rows)


# ---------- Orchestration ----------

def compute_for_day(day: str, micro_thr: float, med_thr: float, maxi_thr: float,
                    symbol: Optional[str] = None) -> None:
    """
    symbol is currently unused (pipeline is single-symbol). Add WHERE symbol=:sym
    in fetch if you want to filter.
    """
    engine = get_engine()
    start, end = day_window_local(day)

    print(f"[zig] {day}  window {start.isoformat()} -> {end.isoformat()}  "
          f"(thr micro={micro_thr}, med={med_thr}, maxi={maxi_thr})")

    # Clean slate for the run_day
    wipe_day(engine, day)

    # 1) MICRO from ticks
    ticks = fetch_ticks(engine, start, end)
    if not ticks:
        print("No ticks for window; nothing to compute.")
        return

    micro_segs, micro_pts = build_zig(ticks, micro_thr)

    # 2) MEDIUM from confirmed MICRO pivots
    micro_nodes = [Node(id=p.tick_id, ts=p.ts, price=p.price) for p in micro_pts]
    med_segs, med_pts = build_zig(micro_nodes, med_thr)

    # 3) MAXI from confirmed MEDIUM pivots
    med_nodes = [Node(id=p.tick_id, ts=p.ts, price=p.price) for p in med_pts]
    maxi_segs, maxi_pts = build_zig(med_nodes, maxi_thr)

    # Persist
    write_points(engine, "micro",  day, micro_pts)
    write_points(engine, "medium", day, med_pts)
    write_points(engine, "maxi",   day, maxi_pts)

    write_segments(engine, "micro_trends",  day, micro_segs, count_ticks=True)
    write_segments(engine, "medium_trends", day, med_segs,   count_ticks=False)
    write_segments(engine, "maxi_trends",   day, maxi_segs,  count_ticks=False)

    print(f"Done: micro pts={len(micro_pts)}, segs={len(micro_segs)} | "
          f"medium pts={len(med_pts)}, segs={len(med_segs)} | "
          f"maxi pts={len(maxi_pts)}, segs={len(maxi_segs)}")


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Compute ZigZag for a day.")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (local AU/Sydney)")
    ap.add_argument("--micro", type=float, required=True, help="micro absolute threshold")
    ap.add_argument("--medium", type=float, required=True, help="medium absolute threshold")
    ap.add_argument("--maxi", type=float, required=True, help="maxi absolute threshold")
    ap.add_argument("--symbol", default=None, help="optional symbol filter (not used by default)")
    args = ap.parse_args()

    compute_for_day(args.day, args.micro, args.medium, args.maxi, args.symbol)


if __name__ == "__main__":
    main()
