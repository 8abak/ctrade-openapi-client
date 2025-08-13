# daily_trend_runner.py
# Python 3.9+
import os
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Iterable

from sqlalchemy import create_engine, text

# ---------- DB ----------
DB_URL = os.getenv("DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
)
engine = create_engine(DB_URL, future=True)

# ---------- Config ----------
TZ_OFFSET_HOURS = int(os.getenv("ZIG_TZ_OFFSET_HOURS", "10"))  # +10 default
MICRO_THRESHOLD = float(os.getenv("ZIG_MICRO", "0.50"))        # absolute price units
MEDIUM_THRESHOLD = float(os.getenv("ZIG_MEDIUM", "1.00"))
MAXI_THRESHOLD = float(os.getenv("ZIG_MAXI", "3.00"))

# ---------- Data structures ----------
@dataclass
class Node:
    ts: datetime
    price: float
    id: Optional[int] = None  # tick id if available

@dataclass
class Segment:
    start: Node
    end: Node

@dataclass
class Point:
    ts: datetime
    price: float
    kind: str  # 'peak' or 'trough'

# ---------- Utilities ----------
def day_bounds(run_day: str) -> Tuple[datetime, datetime]:
    """08:00 local -> next-day 07:00 local (end exclusive)."""
    d = date.fromisoformat(run_day)
    tz = timezone(timedelta(hours=TZ_OFFSET_HOURS))
    start = datetime.combine(d, time(8, 0), tzinfo=tz)
    end_excl = start + timedelta(hours=23)  # up to 07:00 of next day (exclusive)
    return start, end_excl

def fetch_ticks(a: datetime, b: datetime) -> List[Node]:
    sql = """
        SELECT id, timestamp, COALESCE(mid, (bid+ask)/2.0) AS price
        FROM ticks
        WHERE timestamp >= :a AND timestamp < :b
        ORDER BY timestamp ASC, id ASC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"a": a, "b": b}).mappings().all()
    return [Node(ts=r["timestamp"], price=float(r["price"]), id=r["id"]) for r in rows]

def write_points(level: str, run_day: str, points: List[Point]) -> None:
    if not points:
        return
    sql = """
        INSERT INTO zigzag_points (level, ts, price, kind, run_day)
        VALUES (:level, :ts, :price, :kind, :day)
    """
    payload = [{"level": level, "ts": p.ts, "price": p.price, "kind": p.kind, "day": run_day}
               for p in points]
    with engine.begin() as conn:
        conn.execute(text(sql), payload)

def write_segments(table: str, run_day: str, segs: List[Segment]) -> None:
    if not segs:
        return
    sql = f"""
        INSERT INTO {table}
            (start_ts, end_ts, start_price, end_price,
             direction, range_abs, duration_s, num_ticks, run_day)
        VALUES
            (:a_ts, :b_ts, :a_px, :b_px, :dir, :rng, :dur, :nticks, :day)
    """
    rows = []
    for s in segs:
        direction = 1 if s.end.price >= s.start.price else -1
        rows.append({
            "a_ts": s.start.ts, "b_ts": s.end.ts,
            "a_px": s.start.price, "b_px": s.end.price,
            "dir": direction,
            "rng": abs(s.end.price - s.start.price),
            "dur": (s.end.ts - s.start.ts).total_seconds(),
            # tick count is only exact for micro; OK to approximate by None/0 at higher levels
            "nticks": (s.end.id - s.start.id) if (s.start.id and s.end.id) else 0,
            "day": run_day,
        })
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def clear_day(run_day: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM zigzag_points WHERE run_day = :d"), {"d": run_day})
        for t in ("micro_trends", "medium_trends", "maxi_trends"):
            conn.execute(text(f"DELETE FROM {t} WHERE run_day = :d"), {"d": run_day})

# ---------- Core zigzag (confirm-on-break) ----------
def build_zig(series: Iterable[Node], threshold: float) -> Tuple[List[Segment], List[Point]]:
    series = list(series)
    if not series:
        return [], []

    anchor = series[0]            # last confirmed turning point
    candidate: Optional[Node] = None
    cand_dir: int = 0             # 0=unknown, +1=up, -1=down

    segs: List[Segment] = []
    pts: List[Point] = []

    for x in series[1:]:
        diff = x.price - anchor.price

        # no direction yet: wait for first threshold exceed
        if cand_dir == 0:
            if abs(diff) >= threshold:
                cand_dir = 1 if diff > 0 else -1
                candidate = x
            continue

        if cand_dir == 1:
            # still going up? extend candidate
            if x.price >= candidate.price:
                candidate = x
            # opposite break confirmed?
            if (anchor.price - x.price) >= threshold:
                segs.append(Segment(anchor, candidate))
                pts.append(Point(ts=candidate.ts, price=candidate.price, kind="peak"))
                anchor = candidate
                cand_dir = -1
                candidate = x
        else:  # cand_dir == -1 (down)
            if x.price <= candidate.price:
                candidate = x
            if (x.price - anchor.price) >= threshold:
                segs.append(Segment(anchor, candidate))
                pts.append(Point(ts=candidate.ts, price=candidate.price, kind="trough"))
                anchor = candidate
                cand_dir = +1
                candidate = x

    # Do NOT finalize the last leg unless it was confirmed by the opposite break.
    return segs, pts

# ---------- Pipeline ----------
def compute_for_day(run_day: str,
                    micro_thr: float = MICRO_THRESHOLD,
                    medium_thr: float = MEDIUM_THRESHOLD,
                    maxi_thr: float = MAXI_THRESHOLD):
    a, b = day_bounds(run_day)
    print(f"[zig] {run_day}  window {a.isoformat()} -> {b.isoformat()}  (thr micro={micro_thr}, med={medium_thr}, maxi={maxi_thr})")

    # 1) micro from ticks
    ticks = fetch_ticks(a, b)
    micro_segs, micro_pts = build_zig(ticks, micro_thr)

    # 2) medium from confirmed micro points
    micro_nodes = [Node(ts=p.ts, price=p.price) for p in micro_pts]
    medium_segs, medium_pts = build_zig(micro_nodes, medium_thr)

    # 3) maxi from confirmed medium points
    medium_nodes = [Node(ts=p.ts, price=p.price) for p in medium_pts]
    maxi_segs, maxi_pts = build_zig(medium_nodes, maxi_thr)

    # write DB
    clear_day(run_day)
    write_points("micro", run_day, micro_pts)
    write_points("medium", run_day, medium_pts)
    write_points("maxi", run_day, maxi_pts)

    write_segments("micro_trends", run_day, micro_segs)
    write_segments("medium_trends", run_day, medium_segs)
    write_segments("maxi_trends", run_day, maxi_segs)

    print(f"[zig] {run_day}  micro: {len(micro_segs)} segs / {len(micro_pts)} pts ; "
          f"medium: {len(medium_segs)} / {len(medium_pts)} ; "
          f"maxi: {len(maxi_segs)} / {len(maxi_pts)}")

# ---------- CLI ----------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Rebuild zigzags for a day (08:00->next 07:00).")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (run_day)")
    ap.add_argument("--micro", type=float, default=MICRO_THRESHOLD)
    ap.add_argument("--medium", type=float, default=MEDIUM_THRESHOLD)
    ap.add_argument("--maxi", type=float, default=MAXI_THRESHOLD)
    args = ap.parse_args()
    compute_for_day(args.day, args.micro, args.medium, args.maxi)
