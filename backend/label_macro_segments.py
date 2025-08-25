# backend/label_macro_segments.py
# Purpose: Build/extend macro segments using a $6 Renko/ZigZag rule.
# - Idempotent: if the next confirmed pivot already exists, does nothing.
# - Bounded: at most one new CLOSED segment is appended per call.

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

from sqlalchemy.engine import Engine
from sqlalchemy import text

RENKO_USD = float(__import__("os").environ.get("RENKO_USD", 6.0))


@dataclass
class TickRow:
    id: int
    ts: Any
    mid: float


def _latest_segment(conn) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        text(
            """
            SELECT segment_id, start_ts, end_ts, direction, start_price, end_price,
                   start_tick_id, end_tick_id, confidence
            FROM macro_segments
            ORDER BY end_ts DESC, segment_id DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    return dict(row) if row else None


def _first_tick(conn) -> Optional[TickRow]:
    r = conn.execute(
        text("SELECT id, timestamp, mid FROM ticks ORDER BY id ASC LIMIT 1")
    ).first()
    return TickRow(r[0], r[1], float(r[2])) if r else None


def _tick_by_id(conn, tick_id: int) -> Optional[TickRow]:
    r = conn.execute(
        text("SELECT id, timestamp, mid FROM ticks WHERE id=:i"), {"i": tick_id}
    ).first()
    return TickRow(r[0], r[1], float(r[2])) if r else None


def _scan_for_pivot(conn, start_tick_id: int, start_price: float) -> Optional[TickRow]:
    """
    Scan forward in batches until price has moved >= RENKO_USD from start_price.
    Returns the *pivot* tick (end). None if no further data.
    """
    batch = 20000
    last_id = start_tick_id
    while True:
        rows = conn.execute(
            text(
                """
                SELECT id, timestamp, mid
                FROM ticks
                WHERE id > :after
                ORDER BY id ASC
                LIMIT :lim
                """
            ),
            {"after": last_id, "lim": batch},
        ).fetchall()
        if not rows:
            return None
        for r in rows:
            rid, ts, mid = int(r[0]), r[1], float(r[2])
            if abs(mid - start_price) >= RENKO_USD:
                return TickRow(rid, ts, mid)
            last_id = rid


def _insert_segment(conn, start: TickRow, end: TickRow) -> int:
    direction = 1 if (end.mid - start.mid) > 0 else -1
    length_usd = abs(end.mid - start.mid)
    raw = max(0.0, min(1.0, length_usd / (RENKO_USD * 1.5)))
    confidence = 0.15 + 0.8 * raw

    exists = conn.execute(
        text(
            "SELECT segment_id FROM macro_segments WHERE start_tick_id=:a AND end_tick_id=:b"
        ),
        {"a": start.id, "b": end.id},
    ).first()
    if exists:
        return int(exists[0])

    seg_id = conn.execute(
        text(
            """
            INSERT INTO macro_segments
                (start_ts, end_ts, direction, start_price, end_price,
                 length_usd, confidence, start_tick_id, end_tick_id)
            VALUES
                (:s_ts, :e_ts, :dir, :s_p, :e_p, :len, :conf, :s_id, :e_id)
            RETURNING segment_id
            """
        ),
        {
            "s_ts": start.ts,
            "e_ts": end.ts,
            "dir": direction,
            "s_p": start.mid,
            "e_p": end.mid,
            "len": length_usd,
            "conf": confidence,
            "s_id": start.id,
            "e_id": end.id,
        },
    ).scalar_one()
    return int(seg_id)


def BuildOrExtendSegments(engine: Engine) -> Dict[str, Any]:
    """
    Append at most one CLOSED macro segment based on a $RENKO_USD Renko/ZigZag rule.
    If the table is empty, bootstrap from the earliest tick.
    """
    with engine.begin() as conn:
        last = _latest_segment(conn)
        if last is None:
            first = _first_tick(conn)
            if not first:
                return {"segments_added": 0, "last_segment_id": None}
            nxt = _scan_for_pivot(conn, first.id, first.mid)
            if not nxt:
                return {"segments_added": 0, "last_segment_id": None}
            seg_id = _insert_segment(conn, first, nxt)
            return {"segments_added": 1, "last_segment_id": seg_id}

        pivot_tick = _tick_by_id(conn, last["end_tick_id"])
        if not pivot_tick:
            return {"segments_added": 0, "last_segment_id": last["segment_id"]}

        nxt = _scan_for_pivot(conn, pivot_tick.id, pivot_tick.mid)
        if not nxt:
            return {"segments_added": 0, "last_segment_id": last["segment_id"]}

        seg_id = _insert_segment(conn, pivot_tick, nxt)
        return {"segments_added": 1, "last_segment_id": seg_id}
