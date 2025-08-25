# backend/label_macro_segments.py
# Purpose: Build/extend macro segments using a $6 Renko/ZigZag rule.
# - Idempotent: if the next confirmed pivot already exists, does nothing.
# - Bounded: at most one new CLOSED segment is appended per call.
# Deps: SQLAlchemy Core only. Works with ticks(id, timestamp, mid).

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from sqlalchemy import create_engine, text

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(DB_URL, pool_pre_ping=True)

RENKO_USD = 6.0  # size for macro legs


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
    Scan forward in small batches until price has moved >= RENKO_USD
    from start_price (either direction). Returns the *pivot* tick (end).
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
        # continue loop (bounded by end of data)


def _insert_segment(
    conn,
    start: TickRow,
    end: TickRow,
) -> int:
    direction = 1 if (end.mid - start.mid) > 0 else -1
    length_usd = abs(end.mid - start.mid)
    # confidence proxy: range (0.15..0.95) scaled by move size over renko size
    raw = max(0.0, min(1.0, length_usd / (RENKO_USD * 1.5)))
    confidence = 0.15 + 0.8 * raw

    # Idempotency guard: do we already have this exact pair?
    exists = conn.execute(
        text(
            """
            SELECT segment_id
            FROM macro_segments
            WHERE start_tick_id=:a AND end_tick_id=:b
            """
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


def BuildOrExtendSegments() -> Dict[str, Any]:
    """
    Append at most one CLOSED macro segment based on a $6 Renko/ZigZag rule.
    If the table is empty, bootstrap from the earliest tick.
    Returns: {"segments_added": int, "last_segment_id": Optional[int]}
    """
    with engine.begin() as conn:
        last = _latest_segment(conn)
        if last is None:
            # Bootstrap: pick very first tick as pivot A, scan for first pivot B
            first = _first_tick(conn)
            if not first:
                return {"segments_added": 0, "last_segment_id": None}
            nxt = _scan_for_pivot(conn, first.id, first.mid)
            if not nxt:
                return {"segments_added": 0, "last_segment_id": None}
            seg_id = _insert_segment(conn, first, nxt)
            return {"segments_added": 1, "last_segment_id": seg_id}

        # We have at least one closed segment; continue from its end pivot
        pivot_tick = _tick_by_id(conn, last["end_tick_id"])
        if not pivot_tick:
            return {"segments_added": 0, "last_segment_id": None}

        nxt = _scan_for_pivot(conn, pivot_tick.id, pivot_tick.mid)
        if not nxt:
            return {"segments_added": 0, "last_segment_id": last["segment_id"]}

        seg_id = _insert_segment(conn, pivot_tick, nxt)
        return {"segments_added": 1, "last_segment_id": seg_id}
