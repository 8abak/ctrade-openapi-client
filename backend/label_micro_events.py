# backend/label_micro_events.py
# Purpose: Detect candidate micro events for the most recent CLOSED macro segment.
# Minimal v1 heuristic, idempotent. Writes into micro_events(features as JSONB).
# Families: pullback_end, breakout, retest_hold.

from __future__ import annotations

import os
import json
from typing import Dict, Any, Optional, List

from sqlalchemy import create_engine, text

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(DB_URL, pool_pre_ping=True)

FEATURES_VERSION = "v1"


def _latest_closed_segment(conn) -> Optional[Dict[str, Any]]:
    r = conn.execute(
        text(
            """
            SELECT segment_id, start_tick_id, end_tick_id, direction
            FROM macro_segments
            WHERE end_ts IS NOT NULL
            ORDER BY end_ts DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    return dict(r) if r else None


def _segment_has_events(conn, seg_id: int) -> bool:
    r = conn.execute(
        text("SELECT 1 FROM micro_events WHERE segment_id=:s LIMIT 1"), {"s": seg_id}
    ).first()
    return bool(r)


def _ticks_slice(conn, a: int, b: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE id BETWEEN :a AND :b
            ORDER BY id
            """
        ),
        {"a": a, "b": b},
    ).mappings().all()
    return [dict(r) for r in rows]


def _insert_event(conn, seg_id: int, tick_id: int, etype: str, feats: Dict[str, Any]):
    conn.execute(
        text(
            """
            INSERT INTO micro_events (segment_id, tick_id, event_type, features)
            VALUES (:s, :t, :e, :f)
            ON CONFLICT DO NOTHING
            """
        ),
        {"s": seg_id, "t": tick_id, "e": etype, "f": json.dumps(feats)},
    )


def DetectMicroEventsForLatestClosedSegment() -> Dict[str, Any]:
    with engine.begin() as conn:
        seg = _latest_closed_segment(conn)
        if not seg:
            return {"events_added": 0}
        if _segment_has_events(conn, seg["segment_id"]):
            return {"events_added": 0}

        ticks = _ticks_slice(conn, seg["start_tick_id"], seg["end_tick_id"])
        if len(ticks) < 50:
            return {"events_added": 0}

        # Heuristic: pick 3 positions within the segment window, then refine a bit.
        n = len(ticks)
        idx_a = max(10, n // 3)
        idx_b = max(20, 2 * n // 3)
        idx_c = max(30, int(n * 0.8))

        def nearest_local_turn(i: int, look: int = 8) -> int:
            i0 = max(look, min(n - look - 1, i))
            win = ticks[i0 - look : i0 + look + 1]
            mids = [w["mid"] for w in win]
            if seg["direction"] > 0:
                # local pullback low
                j = mids.index(min(mids))
            else:
                # local pullback high
                j = mids.index(max(mids))
            return (i0 - look) + j

        ia = nearest_local_turn(idx_a)
        ib = nearest_local_turn(idx_b)
        ic = nearest_local_turn(idx_c)

        added = 0
        for (i, etype) in [(ia, "pullback_end"), (ib, "breakout"), (ic, "retest_hold")]:
            t = ticks[i]
            feats = {
                "fv": FEATURES_VERSION,
                "pos_in_segment": round(i / n, 4),
                "seg_dir": seg["direction"],
            }
            _insert_event(conn, seg["segment_id"], t["id"], etype, feats)
            added += 1

        return {"events_added": added}
