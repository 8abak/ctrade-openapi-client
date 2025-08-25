# backend/train_predict.py
# Purpose: Train a tiny tabular classifier and write predictions for events
# in the NEXT segment (after the training window). Idempotent & bounded.

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Dict, Any, List

from sqlalchemy import create_engine, text

# Use sklearn (already widely available). If not installed, this gracefully falls back.
try:
    from sklearn.ensemble import GradientBoostingClassifier
except Exception:  # pragma: no cover
    GradientBoostingClassifier = None  # type: ignore

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(DB_URL, pool_pre_ping=True)


def _resolved_events(conn):
    # Join outcomes to know the label
    return conn.execute(
        text(
            """
            SELECT e.event_id, e.features, s.direction, o.outcome
            FROM micro_events e
            JOIN outcomes o ON o.event_id = e.event_id
            JOIN macro_segments s ON s.segment_id = e.segment_id
            ORDER BY e.event_id
            """
        )
    ).mappings().all()


def _latest_segment_id(conn):
    r = conn.execute(
        text(
            """
            SELECT segment_id
            FROM macro_segments
            ORDER BY end_ts DESC
            LIMIT 1
            """
        )
    ).first()
    return int(r[0]) if r else None


def _events_for_segment(conn, seg_id: int):
    return conn.execute(
        text(
            """
            SELECT e.event_id, e.features, s.direction
            FROM micro_events e
            JOIN macro_segments s ON s.segment_id = e.segment_id
            WHERE e.segment_id=:sid
            """
        ),
        {"sid": seg_id},
    ).mappings().all()


def _to_xy(rows):
    X, y = [], []
    for r in rows:
        feats = r["features"] or {}
        if isinstance(feats, str):
            try:
                feats = json.loads(feats)
            except Exception:
                feats = {}
        fv = [
            float(feats.get("pos_in_segment", 0)),
            float(feats.get("seg_dir", 0)),
        ]
        X.append(fv)
        y.append(1 if r["outcome"] == "TP" else 0)
    return X, y


def _predict_X(rows):
    X = []
    meta = []
    for r in rows:
        feats = r["features"] or {}
        if isinstance(feats, str):
            try:
                feats = json.loads(feats)
            except Exception:
                feats = {}
        X.append([float(feats.get("pos_in_segment", 0)), float(feats.get("seg_dir", 0))])
        meta.append({"event_id": r["event_id"]})
    return X, meta


def TrainAndPredict() -> Dict[str, Any]:
    with engine.begin() as conn:
        train_rows = _resolved_events(conn)
        if not train_rows or len(train_rows) < 10 or GradientBoostingClassifier is None:
            # Not enough to train yet or sklearn missing
            return {"trained": False, "written": 0, "threshold": 0.0}

        X, y = _to_xy(train_rows)
        clf = GradientBoostingClassifier(random_state=42)
        clf.fit(X, y)

        # Choose τ that maximizes EV under +2/-1 (simple sweep)
        import numpy as np

        proba = clf.predict_proba(X)[:, 1]
        grid = np.linspace(0.2, 0.8, 25)
        best_tau, best_ev = 0.5, -1e9
        for t in grid:
            long = proba >= t
            if long.sum() == 0:
                continue
            ev = (2.0 * (y * long)).sum() - (1.0 * ((1 - np.array(y)) * long)).sum()
            if ev > best_ev:
                best_ev, best_tau = ev, float(t)

        # Predict for the most recent CLOSED segment's events
        seg_id = _latest_segment_id(conn)
        if seg_id is None:
            return {"trained": True, "written": 0, "threshold": best_tau}

        rows = _events_for_segment(conn, seg_id)
        if not rows:
            return {"trained": True, "written": 0, "threshold": best_tau}

        Xp, meta = _predict_X(rows)
        p = clf.predict_proba(Xp)[:, 1]
        now = datetime.utcnow().isoformat() + "Z"
        written = 0
        for (m, prob) in zip(meta, p):
            conn.execute(
                text(
                    """
                    INSERT INTO predictions
                        (event_id, model_version, p_tp, threshold, decided, predicted_at)
                    VALUES (:eid, :mv, :p, :t, :d, :ts)
                    """
                ),
                {
                    "eid": m["event_id"],
                    "mv": f"gb-{now[:10]}-{len(train_rows)}",
                    "p": float(prob),
                    "t": best_tau,
                    "d": bool(prob >= best_tau),
                    "ts": now,
                },
            )
            written += 1

        return {"trained": True, "written": written, "threshold": best_tau}
