# backend/label_micro_events.py
# Purpose: Detect micro events for the most recent CLOSED macro segment.
# Writes into micro_events (event_price included).

from __future__ import annotations

import json
from typing import Dict, Any, Optional, List

from sqlalchemy.engine import Engine
from sqlalchemy import text

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


def _insert_event(conn, seg_id: int, tick_id: int, price: float, etype: str, feats: Dict[str, Any]):
    conn.execute(
        text(
            """
            INSERT INTO micro_events (segment_id, tick_id, event_type, features)
            VALUES (:s, :t, :e, :f)
            ON CONFLICT DO NOTHING
            """
        ),
        {"s": seg_id, "t": tick_id, "e": etype, "f": json.dumps({**feats, "event_price": price})},
    )


def DetectMicroEventsForLatestClosedSegment(engine: Engine) -> Dict[str, Any]:
    with engine.begin() as conn:
        seg = _latest_closed_segment(conn)
        if not seg:
            return {"events_added": 0}
        if _segment_has_events(conn, seg["segment_id"]):
            return {"events_added": 0}

        ticks = _ticks_slice(conn, seg["start_tick_id"], seg["end_tick_id"])
        if len(ticks) < 50:
            return {"events_added": 0}

        n = len(ticks)
        idxs = [max(10, n // 3), max(20, 2 * n // 3), max(30, int(n * 0.8))]
        names = ["pullback_end", "breakout", "retest_hold"]

        def local_turn(i: int, look: int = 8) -> int:
            i0 = max(look, min(n - look - 1, i))
            win = ticks[i0 - look : i0 + look + 1]
            mids = [w["mid"] for w in win]
            if seg["direction"] > 0:
                j = mids.index(min(mids))
            else:
                j = mids.index(max(mids))
            return (i0 - look) + j

        added = 0
        for raw_idx, name in zip(idxs, names):
            i = local_turn(raw_idx)
            t = ticks[i]
            feats = {
                "fv": FEATURES_VERSION,
                "pos_in_segment": round(i / n, 4),
                "seg_dir": seg["direction"],
            }
            _insert_event(conn, seg["segment_id"], int(t["id"]), float(t["mid"]), name, feats)
            added += 1

        return {"events_added": added}
