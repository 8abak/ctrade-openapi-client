#!/usr/bin/env python3
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

import psycopg2
import psycopg2.extras as pgx

# -------------------------
# CONFIG: keep it simple
# -------------------------
PG_DB = "trading"
PG_USER = "babak"
PG_PASS = "babak33044"
PG_HOST = "localhost"
PG_PORT = 5432

TICKS_TABLE = "ticks"              # columns: id, timestamp (timestamptz), mid (float), ...
POINTS_TABLE = "zigzag_points"     # columns: id, level, tick_id, ts, price, kind, run_day
TREND_TABLES = {                   # columns used below must exist
    "micro":  "micro_trends",
    "medium": "medium_trends",
    "maxi":   "maxi_trends",
}

# If your DB not in AEST but timestamps are stored with +10:00 tzinfo (as shown), this is fine
AEST = timezone(timedelta(hours=10))

@dataclass
class Item:
    """One stream element for zigzag algorithm."""
    tick_id: int
    ts: datetime
    price: float
    # idx is only used for micro to count ticks cheaply; optional for others
    idx: Optional[int] = None

@dataclass
class ZigPoint:
    level: str
    tick_id: int
    ts: datetime
    price: float
    kind: str      # "peak" or "trough"

@dataclass
class Segment:
    start_tick_id: int
    end_tick_id: int
    start_ts: datetime
    end_ts: datetime
    start_price: float
    end_price: float
    direction: int     # +1 up, -1 down
    range_abs: float
    duration_s: int
    num_ticks: int     # for medium/maxi we compute via COUNT(*)
    run_day: str


def connect():
    return psycopg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
        host=PG_HOST, port=PG_PORT
    )


def fetch_ticks(cur, start_ts: datetime, end_ts: datetime) -> List[Item]:
    cur.execute(
        f"""
        SELECT id, "timestamp", mid
        FROM {TICKS_TABLE}
        WHERE "timestamp" >= %s AND "timestamp" < %s
        ORDER BY id
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    items = [Item(tick_id=r[0], ts=r[1], price=float(r[2]), idx=i) for i, r in enumerate(rows)]
    return items


def zigzag_from_stream(items: List[Item], threshold: float, level: str) -> List[ZigPoint]:
    """
    Implements:
      - wait until |price - base| >= threshold to set a candidate (direction ±1)
      - update candidate while moving further in same direction
      - on reversal exceeding threshold, emit previous candidate as a point (peak/trough)
    Does NOT emit the trailing unfinished candidate.
    """
    out: List[ZigPoint] = []
    if not items:
        return out

    base = items[0]          # reference point for threshold check
    direction = 0            # +1 up, -1 down, 0 uninitialized
    candidate: Optional[Item] = None

    for it in items[1:]:
        delta = it.price - base.price
        if direction == 0:
            if delta >= threshold:
                direction = +1
                candidate = it
            elif delta <= -threshold:
                direction = -1
                candidate = it
            else:
                continue
        else:
            if direction == +1:
                # still rising? keep best peak
                if it.price >= candidate.price:
                    candidate = it
                # reversal big enough?
                elif (candidate.price - it.price) >= threshold:
                    # emit previous candidate as peak
                    out.append(ZigPoint(level, candidate.tick_id, candidate.ts, candidate.price, "peak"))
                    base = candidate
                    direction = -1
                    candidate = it
            else:  # direction == -1
                if it.price <= candidate.price:
                    candidate = it
                elif (it.price - candidate.price) >= threshold:
                    out.append(ZigPoint(level, candidate.tick_id, candidate.ts, candidate.price, "trough"))
                    base = candidate
                    direction = +1
                    candidate = it

    # trailing candidate is not confirmed; do not emit
    return out


def build_segments(level: str, pts: List[ZigPoint], source: str, cur) -> List[Segment]:
    """
    Turn alternating points into trend segments.
    For micro: num_ticks uses in-memory indexes (fast).
    For medium/maxi: count ticks from DB between start/end (accurate).
    """
    segs: List[Segment] = []
    for a, b in zip(pts, pts[1:]):
        start_tick_id = a.tick_id
        end_tick_id   = b.tick_id
        start_ts, end_ts = a.ts, b.ts
        start_price, end_price = a.price, b.price
        direction = +1 if end_price > start_price else -1
        range_abs = abs(end_price - start_price)
        duration_s = int((end_ts - start_ts).total_seconds())

        if source == "micro":
            # micro num_ticks: use tick_id index proxy from the source list
            # (we don't have idx in ZigPoint; so count via DB for consistency)
            cur.execute(
                f'SELECT COUNT(*) FROM {TICKS_TABLE} WHERE "timestamp" >= %s AND "timestamp" <= %s',
                (start_ts, end_ts)
            )
            num_ticks = cur.fetchone()[0]
        else:
            # always COUNT(*) for medium/maxi
            cur.execute(
                f'SELECT COUNT(*) FROM {TICKS_TABLE} WHERE "timestamp" >= %s AND "timestamp" <= %s',
                (start_ts, end_ts)
            )
            num_ticks = cur.fetchone()[0]

        segs.append(Segment(
            start_tick_id, end_tick_id, start_ts, end_ts,
            start_price, end_price, direction, range_abs, duration_s,
            num_ticks, run_day=start_ts.date().isoformat()
        ))
    return segs


def insert_points(cur, level: str, run_day: str, pts: List[ZigPoint]):
    if not pts:
        return
    payload = [
        (level, p.tick_id, p.ts, p.price, p.kind, run_day)
        for p in pts
    ]
    pgx.execute_values(
        cur,
        f"""
        INSERT INTO {POINTS_TABLE} (level, tick_id, ts, price, kind, run_day)
        VALUES %s
        """,
        payload,
        template="(%s,%s,%s,%s,%s,%s)"
    )


def insert_segments(cur, level: str, segs: List[Segment]):
    if not segs:
        return
    tbl = TREND_TABLES[level]
    payload = [
        (s.start_tick_id, s.end_tick_id, s.start_ts, s.end_ts,
         s.start_price, s.end_price, s.direction, s.range_abs,
         s.duration_s, s.num_ticks, s.run_day)
        for s in segs
    ]
    pgx.execute_values(
        cur,
        f"""
        INSERT INTO {tbl}
            (start_tick_id, end_tick_id, start_ts, end_ts,
             start_price, end_price, direction, range_abs,
             duration_s, num_ticks, run_day)
        VALUES %s
        """,
        payload,
        template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )


def delete_existing_for_day(cur, run_day: str):
    # points
    cur.execute(f"DELETE FROM {POINTS_TABLE} WHERE run_day = %s", (run_day,))
    # segments
    for lvl, tbl in TREND_TABLES.items():
        cur.execute(f"DELETE FROM {tbl} WHERE run_day = %s", (run_day,))


def main():
    ap = argparse.ArgumentParser(description="Compute micro/medium/maxi zigzags for a run day.")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (day rolls 08:00 → next day 07:00, UTC+10)")
    ap.add_argument("--micro", type=float, required=True, help="micro threshold (absolute)")
    ap.add_argument("--medium", type=float, required=True, help="medium threshold (absolute)")
    ap.add_argument("--maxi", type=float, required=True, help="maxi threshold (absolute)")
    args = ap.parse_args()

    day = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=AEST)
    start = day.replace(hour=8, minute=0, second=0, microsecond=0)
    end   = start + timedelta(hours=23)
    run_day = args.day

    print(f"[zig] {run_day}  window {start.isoformat()} -> {end.isoformat()}  "
          f"(thr micro={args.micro}, med={args.medium}, maxi={args.maxi})")

    con = connect()
    con.autocommit = False
    try:
        cur = con.cursor()

        # clean old rows for this day
        delete_existing_for_day(cur, run_day)

        # 1) micro from ticks
        tick_items = fetch_ticks(cur, start, end)
        if not tick_items:
            print("No ticks for this window.")
            con.commit()
            return

        micro_pts = zigzag_from_stream(tick_items, args.micro, "micro")
        micro_segs = build_segments("micro", micro_pts, "micro", cur)
        insert_points(cur, "micro", run_day, micro_pts)
        insert_segments(cur, "micro", micro_segs)
        print(f"micro: {len(micro_pts)} points, {len(micro_segs)} segments")

        # 2) medium from micro points (use their tick_id/ts/price as stream)
        micro_stream = [Item(p.tick_id, p.ts, p.price) for p in micro_pts]
        medium_pts = zigzag_from_stream(micro_stream, args.medium, "medium")
        medium_segs = build_segments("medium", medium_pts, "medium", cur)
        insert_points(cur, "medium", run_day, medium_pts)
        insert_segments(cur, "medium", medium_segs)
        print(f"medium: {len(medium_pts)} points, {len(medium_segs)} segments")

        # 3) maxi from medium points
        medium_stream = [Item(p.tick_id, p.ts, p.price) for p in medium_pts]
        maxi_pts = zigzag_from_stream(medium_stream, args.maxi, "maxi")
        maxi_segs = build_segments("maxi", maxi_pts, "maxi", cur)
        insert_points(cur, "maxi", run_day, maxi_pts)
        insert_segments(cur, "maxi", maxi_segs)
        print(f"maxi: {len(maxi_pts)} points, {len(maxi_segs)} segments")

        con.commit()
        print("Done.")
    except Exception as e:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
