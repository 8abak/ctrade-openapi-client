"""
ml/snowball.py

Online "snowball" trainer that tries to predict **the direction of the CURRENT zone**
from the personalities of the PREVIOUS zones.

Flow (offline backtest, streaming style):

    zones[0], zones[1], zones[2], ... ordered by id

    - For the very first few zones we only build their personalities.
    - Once we have `memory_depth` previous personalities:

        For zone_i:
            1) Build a feature vector from personalities of zones
               [i-memory_depth, ..., i-1].
            2) Use the current model to predict direction of zone_i.
            3) Store prediction + probabilities + meta in snowball_prediction.
            4) Compare with true direction of zone_i and update running accuracy.
            5) Train (partial_fit) on this (features, true_dir) pair.
            6) Build personality for zone_i and add it to the history.

The model is strictly walk-forward: at each step it only sees information
from zones up to i-1 when making a prediction for zone_i.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from typing import Dict, List

import numpy as np
from sklearn.linear_model import SGDClassifier

from backend.db import get_conn, dict_cur
from jobs.buildZonePersonality import build_zone_personality

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Multi-class labels for direction
CLASSES = np.array([-1, 0, 1], dtype=int)

# How many previous zones to use as "memory"
DEFAULT_MEMORY_DEPTH = 3

# Reporting cadence (how often to print running accuracy).
REPORT_EVERY = 100

# Which fields we use from zone_personality for each zone
PERSONALITY_FEATURES = [
    "dir_zone",
    "net_move",
    "abs_move",
    "full_range",
    "body_range_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "duration_sec",
    "n_ticks",
    "speed",
    "noise_ratio",
    "pos_of_extreme",
    "delay_frac_0_2",
    "delay_frac_0_4",
    "n_swings",
    "swing_dir_changes",
    "avg_swing_range",
    "max_swing_range",
]


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


def fetch_zones(conn, limit: int) -> List[Dict]:
    """
    Fetch zones in id order, limited to `limit`.
    We need id, start_id, end_id, direction for each.
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
    return rows


def fetch_zone_personality(conn, zone_id: int) -> Dict:
    """
    Ensure zone_personality exists for this zone_id, then fetch the row.
    """
    # First try to fetch
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT
                id,
                dir_zone,
                net_move,
                abs_move,
                full_range,
                body_range_ratio,
                upper_wick_ratio,
                lower_wick_ratio,
                duration_sec,
                n_ticks,
                speed,
                noise_ratio,
                pos_of_extreme,
                delay_frac_0_2,
                delay_frac_0_4,
                n_swings,
                swing_dir_changes,
                avg_swing_range,
                max_swing_range
            FROM zone_personality
            WHERE id = %s
            """,
            (zone_id,),
        )
        row = cur.fetchone()

    if row:
        return row

    # Not present -> build it using the jobs helper (separate connection).
    build_zone_personality(zone_id)

    # Fetch again
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT
                id,
                dir_zone,
                net_move,
                abs_move,
                full_range,
                body_range_ratio,
                upper_wick_ratio,
                lower_wick_ratio,
                duration_sec,
                n_ticks,
                speed,
                noise_ratio,
                pos_of_extreme,
                delay_frac_0_2,
                delay_frac_0_4,
                n_swings,
                swing_dir_changes,
                avg_swing_range,
                max_swing_range
            FROM zone_personality
            WHERE id = %s
            """,
            (zone_id,),
        )
        row = cur.fetchone()

    if not row:
        raise RuntimeError(f"zone_personality not found for zone {zone_id} even after build.")

    return row


def features_from_history(history: List[Dict]) -> List[float]:
    """
    Build a flat feature vector from a list of zone_personality rows,
    in chronological order (oldest first).
    """
    feats: List[float] = []
    for zp in history:
        for key in PERSONALITY_FEATURES:
            val = zp.get(key)
            if val is None:
                feats.append(0.0)
            else:
                # dir_zone can be SMALLINT, others are numeric
                try:
                    feats.append(float(val))
                except Exception:
                    feats.append(0.0)
    return feats


# ---------------------------------------------------------------------------
# DB schema for runs & predictions (unchanged)
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
# Online training loop (SGD snowball with zone personalities)
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
    if scores.ndim == 1:
        scores = np.vstack([-scores, scores]).T
    scores = scores[0]
    scores = scores - np.max(scores)
    exp = np.exp(scores)
    prob = exp / exp.sum()
    return prob.astype(float)


def snowball_train(limit: int, memory_depth: int) -> None:
    """
    Main entrypoint.

    - Fetch up to `limit` zones in order.
    - Walk through them once in chronological order.
    - Use previous `memory_depth` zone personalities to predict
      direction of the current zone.
    - After prediction, build current zone's personality and learn from it.
    """
    if memory_depth <= 0:
        raise ValueError("memory_depth must be >= 1")

    conn = get_conn()
    ensure_tables(conn)

    zones = fetch_zones(conn, limit)
    n_zones = len(zones)
    if n_zones <= memory_depth:
        print(f"Not enough zones: have {n_zones}, need > memory_depth ({memory_depth}).")
        return

    total_samples = max(0, n_zones - memory_depth)

    run_id = f"zp-sgd-{uuid.uuid4().hex[:8]}"
    algo_name = f"SGDClassifier(log_loss) with memory_depth={memory_depth}"

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO snowball_run (run_id, algo, label_target, total_samples)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, algo_name, "curr_zone_direction_from_prev_personalities", total_samples),
        )

    print(f"Total zones fetched: {n_zones}")
    print(f"Will train on up to {total_samples} zones (after warm-up).")
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

    history: List[Dict] = []  # list of zone_personality dicts
    correct = 0
    total = 0
    step_index = 0
    model_initialized = False

    for idx, z in enumerate(zones):
        zone_id = z["id"]
        true_dir = dir_to_int(z["direction"])

        # Only predict once we have enough history
        if len(history) >= memory_depth:
            window = history[-memory_depth:]
            feat_vec = features_from_history(window)
            x_row = np.asarray(feat_vec, dtype=float).reshape(1, -1)

            if not model_initialized:
                # Warm-up: first sample only trains, no prediction recorded.
                clf.partial_fit(x_row, [true_dir], classes=CLASSES)
                model_initialized = True
                print(f"Warm-up on zone_id={zone_id}, direction={true_dir}")
            else:
                # Predict with the current model (it has only seen zones < idx)
                pred_label = int(clf.predict(x_row)[0])
                proba_vec = _predict_proba_any(clf, x_row)

                total += 1
                if pred_label == true_dir:
                    correct += 1
                step_index += 1

                acc = correct / total if total else 0.0
                if step_index % REPORT_EVERY == 0 or idx == n_zones - 1:
                    print(
                        f"Up to global index {idx} (zone_id={zone_id}): "
                        f"running accuracy = {acc:.3f}"
                    )

                # Meta row: we treat the last zone in history as the "current"
                # zone that led to this prediction, and zone_id as the predicted one.
                last_zone = zones[idx - 1]
                meta_row = {
                    "zone_id": last_zone["id"],         # base zone (most recent previous)
                    "next_zone_id": zone_id,           # zone whose direction we're predicting
                    "start_id": z["start_id"],
                    "end_id": z["end_id"],
                    "dir_curr": dir_to_int(last_zone["direction"]),
                }

                # Persist this prediction
                insert_prediction(
                    conn,
                    run_id,
                    step_index,
                    meta_row,
                    true_label=true_dir,
                    pred_label=pred_label,
                    proba_vec=proba_vec,
                )

                # Learn from this example
                clf.partial_fit(x_row, [true_dir])

        # After prediction and learning, build personality for the current zone
        # and extend our history.
        zp = fetch_zone_personality(conn, zone_id)
        history.append(zp)

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
    parser = argparse.ArgumentParser(description="Zone-personality SGD snowball trainer")
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of zones to use (we walk on them once in id order).",
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=DEFAULT_MEMORY_DEPTH,
        help="How many previous zone personalities to use as features.",
    )
    args = parser.parse_args(argv)
    snowball_train(args.limit, args.memory)


if __name__ == "__main__":
    main()
