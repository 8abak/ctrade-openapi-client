# backend/compute_outcomes.py
# Purpose: Resolve TP/SL/Timeout for micro events that don't have outcomes yet.

from __future__ import annotations

from datetime import timedelta
from sqlalchemy.engine import Engine
from sqlalchemy import text

TP = 2.0
SL = 1.0
HORIZON_SEC = 60 * 60


def _eligible_events(conn):
    return conn.execute(
        text(
            """
            SELECT e.event_id, e.tick_id, e.features,
                   t.timestamp AS ts, t.mid AS price,
                   s.direction
            FROM micro_events e
            JOIN ticks t ON t.id = e.tick_id
            JOIN macro_segments s ON s.segment_id = e.segment_id
            LEFT JOIN outcomes o ON o.event_id = e.event_id
            WHERE o.event_id IS NULL
            ORDER BY e.event_id
            """
        )
    ).mappings().all()


def _forward_ticks(conn, after_tick_id: int, until_ts):
    return conn.execute(
        text(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE id > :after AND timestamp <= :until
            ORDER BY id ASC
            """
        ),
        {"after": after_tick_id, "until": until_ts},
    ).mappings().all()


def ResolveOutcomes(engine: Engine):
    with engine.begin() as conn:
        rows = _eligible_events(conn)
        resolved = 0
        for r in rows:
            start_price = float(r["price"])
            direction = int(r["direction"])
            tp = start_price + (TP if direction > 0 else -TP)
            sl = start_price - (SL if direction > 0 else -SL)
            until_ts = r["ts"] + timedelta(seconds=HORIZON_SEC)

            winner = None
            tp_ts = None
            sl_ts = None
            for f in _forward_ticks(conn, r["tick_id"], until_ts):
                p = float(f["mid"])
                if direction > 0:
                    if p >= tp:
                        winner = "TP"; tp_ts = f["timestamp"]; break
                    if p <= sl:
                        winner = "SL"; sl_ts = f["timestamp"]; break
                else:
                    if p <= tp:
                        winner = "TP"; tp_ts = f["timestamp"]; break
                    if p >= sl:
                        winner = "SL"; sl_ts = f["timestamp"]; break

            outcome = winner if winner else "Timeout"
            conn.execute(
                text(
                    """
                    INSERT INTO outcomes
                        (event_id, outcome, tp_hit_ts, sl_hit_ts, timeout_ts,
                         horizon_seconds, mfe, mae)
                    VALUES
                        (:eid, :outc, :tp, :sl, :tout, :hz, :mfe, :mae)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                ),
                {
                    "eid": r["event_id"],
                    "outc": outcome,
                    "tp": tp_ts,
                    "sl": sl_ts,
                    "tout": None if winner else until_ts,
                    "hz": HORIZON_SEC,
                    "mfe": None,
                    "mae": None,
                },
            )
            resolved += 1

        return {"outcomes_resolved": resolved}
