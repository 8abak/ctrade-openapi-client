"""
ml/snowball.py

Online "snowball" trainer that tries to predict **the direction of the NEXT zone**
from the behaviour of the CURRENT zone.

- Input rows:   zone_i features (direction, volatility, duration, etc.)
- Target label: direction of zone_{i+1}  in {-1, 0, 1}
    -1 : next zone is down
     0 : next zone is neutral/flat/undefined
     1 : next zone is up

Training is strictly walk-forward:
    1) Warm-up on the very first (current, next) pair.
    2) For every subsequent zone_i:
         • Predict next_dir for zone_{i+1} using the model trained so far.
         • Store prediction + probability vector in snowball_prediction.
         • Compare with the true label and update a running accuracy.
         • Then do a partial_fit() on this (x_i, y_i) pair to learn from it.

The run level metrics are stored in snowball_run.

This file is intentionally self-contained and only relies on `backend.db`
for the Postgres connection and tick/price detection helpers.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
from sklearn.linear_model import SGDClassifier

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Multi-class labels for "direction of NEXT zone"
CLASSES = np.array([-1, 0, 1], dtype=int)

# Reporting cadence (how often to print running accuracy).
REPORT_EVERY = 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def dir_to_int(raw) -> int:
    """
    Normalise various direction encodings to -1 / 0 / 1.
    Accepts:
        -1, 0, 1
        'up', 'u', 'long', 'buy'
        'down', 'dn', 'd', 'short', 'sell'
    Anything else -> 0
    """
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        if raw > 0:
            return 1
        if raw < 0:
            return -1
        return 0
    s = str(raw).strip().lower()
    if s in ("1", "up", "u", "long", "buy"):
        return 1
    if s in ("-1", "dn", "down", "d", "short", "sell"):
        return -1
    return 0


@dataclass
class ZonePair:
    """
    One training row:
        current zone i  -> features
        next zone i+1   -> label (direction)
    """
    zone_id: int
    next_zone_id: int
    start_id: int
    end_id: int
    dir_curr: int
    dir_next: int


def fetch_zone_pairs(conn, limit: int) -> List[ZonePair]:
    """
    Fetch zones and build (current, next) pairs.

    We order by zones.id and pair each zone with the next zone in time.
    The very last zone has no "next" and is therefore ignored.

    Returns at most (limit - 1) pairs.
    """
    sql = """
        SELECT id, start_id, end_id, direction
        FROM zones
        WHERE start_id IS NOT NULL
          AND end_id   IS NOT NULL
        ORDER BY id
        LIMIT %s
    """
    with dict_cur(conn) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    pairs: List[ZonePair] = []
    for i in range(len(rows) - 1):
        cur_z = rows[i]
        nxt_z = rows[i + 1]

        dir_curr = dir_to_int(cur_z["direction"])
        dir_next = dir_to_int(nxt_z["direction"])

        # If we have absolutely no idea about the next direction, skip.
        # (You can relax this later if you want a 3-way model including "0".)
        if dir_next == 0:
            continue

        pairs.append(
            ZonePair(
                zone_id=cur_z["id"],
                next_zone_id=nxt_z["id"],
                start_id=cur_z["start_id"],
                end_id=cur_z["end_id"],
                dir_curr=dir_curr,
                dir_next=dir_next,
            )
        )

    return pairs


def fetch_zone_stats(conn, ts_col: str, mid_expr: str, start_id: int, end_id: int) -> Dict[str, float]:
    """
    Aggregate tick-level behaviour for a single zone.

    We deliberately keep this fairly small and interpretable:
        - n_ticks         (# of ticks in the zone)
        - duration_sec    (wall-clock duration)
        - p_min / p_max   (extremes)
        - p_mean          (average price)
        - p_range         (max-min)
        - p_std           (stddev of price)
        - drift           (close - open)
        - speed           (drift / duration_sec)
        - range_per_tick  (p_range / n_ticks)
    """
    sql = f"""
        WITH base AS (
            SELECT
                id,
                {ts_col}  AS ts,
                {mid_expr}::double precision AS mid
            FROM ticks
            WHERE id BETWEEN %s AND %s
            ORDER BY id
        ),
        agg AS (
            SELECT
                COUNT(*)               AS n_ticks,
                MIN(mid)               AS p_min,
                MAX(mid)               AS p_max,
                AVG(mid)               AS p_mean,
                STDDEV_POP(mid)        AS p_std,
                MAX(mid) - MIN(mid)    AS p_range,
                EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) AS duration_sec,
                ARRAY_AGG(mid ORDER BY id) AS arr
            FROM base
        )
        SELECT
            n_ticks,
            p_min,
            p_max,
            p_mean,
            COALESCE(p_std, 0.0)      AS p_std,
            p_range,
            COALESCE(duration_sec, 0) AS duration_sec,
            arr[1]                    AS p_open,
            arr[array_length(arr, 1)] AS p_close
        FROM agg
    """
    with dict_cur(conn) as cur:
        cur.execute(sql, (start_id, end_id))
        row = cur.fetchone()

    if not row or row["n_ticks"] is None or row["n_ticks"] == 0:
        # Completely missing zone data – return neutral defaults.
        return {
            "n_ticks": 0.0,
            "duration_sec": 0.0,
            "p_min": 0.0,
            "p_max": 0.0,
            "p_mean": 0.0,
            "p_range": 0.0,
            "p_std": 0.0,
            "drift": 0.0,
            "speed": 0.0,
            "range_per_tick": 0.0,
        }

    n_ticks = float(row["n_ticks"])
    duration = float(row["duration_sec"] or 0.0)
    p_min = float(row["p_min"])
    p_max = float(row["p_max"])
    p_mean = float(row["p_mean"])
    p_range = float(row["p_range"] or 0.0)
    p_std = float(row["p_std"] or 0.0)
    p_open = float(row["p_open"])
    p_close = float(row["p_close"])

    drift = p_close - p_open
    # Avoid division by zero
    speed = drift / duration if duration > 0 else 0.0
    range_per_tick = p_range / n_ticks if n_ticks > 0 else 0.0

    return {
        "n_ticks": n_ticks,
        "duration_sec": duration,
        "p_min": p_min,
        "p_max": p_max,
        "p_mean": p_mean,
        "p_range": p_range,
        "p_std": p_std,
        "drift": drift,
        "speed": speed,
        "range_per_tick": range_per_tick,
    }


def build_feature_matrix(conn, limit: int):
    """
    Build X, y, meta for the first `limit` zones.

    X: features from CURRENT zone i
    y: direction of NEXT zone i+1 (-1 / 1)
    meta: list of dicts with ids etc. for later inspection.
    """
    ts_col = detect_ts_col(conn)
    mid_expr = detect_mid_expr(conn)

    pairs = fetch_zone_pairs(conn, limit)
    if not pairs:
        raise RuntimeError("No zone pairs available to train on.")

    X: List[List[float]] = []
    y: List[int] = []
    meta: List[Dict] = []

    for p in pairs:
        stats = fetch_zone_stats(conn, ts_col, mid_expr, p.start_id, p.end_id)

        features = [
            float(p.dir_curr),              # direction of current zone
            stats["n_ticks"],
            stats["duration_sec"],
            stats["p_range"],
            stats["p_std"],
            stats["drift"],
            stats["speed"],
            stats["range_per_tick"],
        ]

        X.append(features)
        y.append(p.dir_next)
        meta.append(
            {
                "zone_id": p.zone_id,
                "next_zone_id": p.next_zone_id,
                "start_id": p.start_id,
                "end_id": p.end_id,
                "dir_curr": p.dir_curr,
            }
        )

    return np.array(X, dtype=float), np.array(y, dtype=int), meta


# ---------------------------------------------------------------------------
# DB schema for runs & predictions
# ---------------------------------------------------------------------------


def ensure_tables(conn) -> None:
    """
    Create snowball_run and snowball_prediction if they don't exist.

    Probabilities are stored in DOUBLE PRECISION so we keep the true
    confidence levels (no more 0/1 truncation).
    """
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS snowball_run (
                run_id         TEXT PRIMARY KEY,
                algo           TEXT NOT NULL,
                label_target   TEXT NOT NULL,
                total_samples  INTEGER NOT NULL,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at    TIMESTAMPTZ,
                final_accuracy DOUBLE PRECISION
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS snowball_prediction (
                id             BIGSERIAL PRIMARY KEY,
                run_id         TEXT NOT NULL REFERENCES snowball_run(run_id) ON DELETE CASCADE,
                step_index     INTEGER NOT NULL,
                zone_id        INTEGER NOT NULL,
                next_zone_id   INTEGER NOT NULL,
                start_id       INTEGER NOT NULL,
                end_id         INTEGER NOT NULL,
                dir_curr       INTEGER NOT NULL,
                true_label     INTEGER NOT NULL,
                pred_label     INTEGER NOT NULL,
                proba_down     DOUBLE PRECISION,
                proba_none     DOUBLE PRECISION,
                proba_up       DOUBLE PRECISION,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )


def insert_prediction(
    conn,
    run_id: str,
    step_index: int,
    meta_row: Dict,
    true_label: int,
    pred_label: int,
    proba_vec: np.ndarray,
) -> None:
    """
    Store one online prediction.

    proba_vec is an array of length len(CLASSES) with probabilities
    in the same order as CLASSES.
    """
    # Helper to grab probability by label value
    label_to_idx = {int(lbl): i for i, lbl in enumerate(CLASSES)}

    def p(label: int) -> float:
        idx = label_to_idx[label]
        if idx < 0 or idx >= len(proba_vec):
            return 0.0
        return float(proba_vec[idx])

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO snowball_prediction (
                run_id,
                step_index,
                zone_id,
                next_zone_id,
                start_id,
                end_id,
                dir_curr,
                true_label,
                pred_label,
                proba_down,
                proba_none,
                proba_up
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                step_index,
                meta_row["zone_id"],
                meta_row["next_zone_id"],
                meta_row["start_id"],
                meta_row["end_id"],
                meta_row["dir_curr"],
                int(true_label),
                int(pred_label),
                p(-1),
                p(0),
                p(1),
            ),
        )


# ---------------------------------------------------------------------------
# Online training loop (SGD snowball)
# ---------------------------------------------------------------------------


def _predict_proba_any(clf: SGDClassifier, x_row: np.ndarray) -> np.ndarray:
    """
    Return probability vector for a single row, even if the model
    doesn't implement predict_proba (fallback using softmax).
    """
    if hasattr(clf, "predict_proba"):
        proba = clf.predict_proba(x_row)[0]
        return np.asarray(proba, dtype=float)

    # Fallback: use decision_function scores and softmax.
    scores = clf.decision_function(x_row)
    scores = np.atleast_1d(scores)
    # For binary, decision_function returns shape (n_samples,),
    # so we manually expand to 2-class; here we have 3 classes,
    # but SGDClassifier with log_loss *should* give predict_proba.
    # This is defensive code.
    if scores.ndim == 1:
        scores = np.vstack([-scores, scores]).T
    scores = scores[0]
    scores = scores - np.max(scores)
    exp = np.exp(scores)
    prob = exp / exp.sum()
    return prob.astype(float)


def snowball_train(limit: int) -> None:
    """
    Main entrypoint.

    1) Build dataset (X, y, meta) for the first `limit` zones.
    2) Create a new SGD run row.
    3) Warm-up on the very first example so the model has initial weights.
    4) For every subsequent example:
         - predict,
         - store prediction + probabilities,
         - update running accuracy,
         - learn via partial_fit.
    """
    conn = get_conn()
    ensure_tables(conn)

    X, y, meta = build_feature_matrix(conn, limit)
    n_samples = len(y)
    if n_samples < 2:
        print("Not enough samples to do online snowball training (need at least 2).")
        return

    run_id = f"sgd-{uuid.uuid4().hex[:8]}"
    algo_name = "SGDClassifier(log_loss)"

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO snowball_run (run_id, algo, label_target, total_samples)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, algo_name, "next_zone_direction", n_samples),
        )

    print(f"Total zone pairs with features: {n_samples}")
    print(f"Run id: {run_id}")

    # Model: linear, online, with probabilistic output
    clf = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=1e-4,
        learning_rate="optimal",
        max_iter=1,
        tol=None,
        random_state=42,
    )

    # Warm-up on the very first sample so we have a model to start from.
    clf.partial_fit(X[0:1], y[0:1], classes=CLASSES)

    correct = 0
    total = 0

    # Start online loop from the *second* sample
    for idx in range(1, n_samples):
        x_row = X[idx : idx + 1]
        true_label = int(y[idx])

        # Predict with the current model (no knowledge of this label yet).
        pred_label = int(clf.predict(x_row)[0])
        proba_vec = _predict_proba_any(clf, x_row)

        total += 1
        if pred_label == true_label:
            correct += 1

        acc = correct / total if total else 0.0
        if total % REPORT_EVERY == 0 or idx == n_samples - 1:
            print(f"Up to zone {idx}: accuracy = {acc:.3f}")

        # Persist this prediction
        insert_prediction(conn, run_id, idx, meta[idx], true_label, pred_label, proba_vec)

        # Now learn from this example
        clf.partial_fit(x_row, [true_label])

    # Final accuracy bookkeeping
    final_acc = correct / total if total else 0.0
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE snowball_run
            SET finished_at = now(),
                final_accuracy = %s
            WHERE run_id = %s
            """,
            (final_acc, run_id),
        )

    # Persist the final model to disk so we can later load it for live trading.
    model_dir = os.path.join(os.path.dirname(__file__), "model_store")
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"{run_id}_sgd.joblib")

    try:
        from joblib import dump as joblib_dump

        joblib_dump(clf, model_path)
        print(f"Saved final SGD model to {model_path}")
    except Exception as exc:  # pragma: no cover - best effort
        print(f"Warning: could not save model to {model_path}: {exc}", file=sys.stderr)

    print(f"Snowball training complete. Final accuracy = {final_acc:.3f}")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Zone-level SGD snowball trainer")
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of zones to use (we train on up to limit-1 zone pairs).",
    )
    args = parser.parse_args(argv)
    snowball_train(args.limit)


if __name__ == "__main__":
    main()
