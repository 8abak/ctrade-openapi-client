"""
ml/snowball.py

New snowball trainer:

- At each step it predicts the **direction of the CURRENT zone (zone_i)**
  using the personalities of the previous `memory_depth` zones:
      [zone_{i-memory_depth}, ..., zone_{i-1}]

- After making the prediction, it:
    1) Stores prediction + probabilities in snowball_prediction.
    2) Compares with the true direction of zone_i and updates running accuracy.
    3) Calls build_zone_personality(zone_i) so the zone's own personality
       becomes available for future steps.
    4) Trains the model on this example (features from previous zones,
       label = direction of zone_i).

- DB tables used:
    zones              (existing)
    zone_personality   (we just built)
    snowball_run       (created if not exists)
    snowball_prediction (created if not exists)

The schema of snowball_run/snowball_prediction is kept compatible
with your previous version so review.html can still read them.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from sklearn.linear_model import SGDClassifier

# ---------------------------------------------------------------------------
# Make project root importable and load build_zone_personality from jobs/
# ---------------------------------------------------------------------------

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import importlib.util

_build_path = os.path.join(ROOT_DIR, "jobs", "buildZonePersonality.py")
_spec = importlib.util.spec_from_file_location("buildZonePersonality", _build_path)
_build_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_build_module)  # type: ignore
build_zone_personality = _build_module.build_zone_personality  # noqa: E305


# ---------------------------------------------------------------------------
# DB config / helpers
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
    "host": "localhost",
    "port": 5432,
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


@contextmanager
def dict_cur(conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Labels, features, small helpers
# ---------------------------------------------------------------------------

CLASSES = np.array([-1, 0, 1], dtype=int)
DEFAULT_MEMORY_DEPTH = 3
REPORT_EVERY = 100

# Which columns from zone_personality we use for each zone
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


def dir_to_int(raw) -> int:
    """Normalise direction into -1 / 0 / 1."""
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
class ZoneMeta:
    id: int
    start_id: int
    end_id: int
    direction: int


# ---------------------------------------------------------------------------
# Fetch zones and personalities
# ---------------------------------------------------------------------------


def fetch_zones(conn, limit: int) -> List[ZoneMeta]:
    """
    Fetch zones in id order, up to `limit`.
    Only keep the columns we need.
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

    result: List[ZoneMeta] = []
    for r in rows:
        result.append(
            ZoneMeta(
                id=int(r["id"]),
                start_id=int(r["start_id"]),
                end_id=int(r["end_id"]),
                direction=dir_to_int(r["direction"]),
            )
        )
    return result


def fetch_zone_personality(conn, zone_id: int) -> Dict:
    """
    Ensure zone_personality exists for given zone, then return it as dict.
    """
    # Try to fetch
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

    # Not found: build it via jobs/buildZonePersonality.py
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
        raise RuntimeError(f"zone_personality not found for zone {zone_id} even after build()")

    return row


def features_from_history(history: List[Dict]) -> List[float]:
    """
    Build a flat feature vector from a list of zone_personality dicts,
    oldest first.
    """
    feats: List[float] = []
    for zp in history:
        for key in PERSONALITY_FEATURES:
            val = zp.get(key)
            if val is None:
                feats.append(0.0)
            else:
                try:
                    feats.append(float(val))
                except Exception:
                    feats.append(0.0)
    return feats


# ---------------------------------------------------------------------------
# DB schema for snowball_run / snowball_prediction
# ---------------------------------------------------------------------------


def ensure_tables(conn) -> None:
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
    base_zone: ZoneMeta,
    target_zone: ZoneMeta,
    true_label: int,
    pred_label: int,
    proba_vec: np.ndarray,
) -> None:
    # Map label -> index in probability vector (CLASSES order)
    label_to_idx = {int(lbl): i for i, lbl in enumerate(CLASSES)}

    def p(label: int) -> float:
        idx = label_to_idx[label]
        if 0 <= idx < len(proba_vec):
            return float(proba_vec[idx])
        return 0.0

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
                base_zone.id,
                target_zone.id,
                target_zone.start_id,
                target_zone.end_id,
                base_zone.direction,
                int(true_label),
                int(pred_label),
                p(-1),
                p(0),
                p(1),
            ),
        )


# ---------------------------------------------------------------------------
# Model helpers
# ---------------------------------------------------------------------------


def _predict_proba_any(clf: SGDClassifier, x_row: np.ndarray) -> np.ndarray:
    """
    Get probability vector for a single row.
    If predict_proba isn't available, approximate with softmax(decision_function).
    """
    if hasattr(clf, "predict_proba"):
        return np.asarray(clf.predict_proba(x_row)[0], dtype=float)

    scores = clf.decision_function(x_row)
    scores = np.atleast_1d(scores)
    if scores.ndim == 1:
        # binary case → convert to 2-class scores
        scores = np.vstack([-scores, scores]).T
    scores = scores[0]
    scores = scores - np.max(scores)
    exp = np.exp(scores)
    prob = exp / exp.sum()
    return prob.astype(float)


# ---------------------------------------------------------------------------
# Main snowball training loop
# ---------------------------------------------------------------------------


def snowball_train(limit: int, memory_depth: int) -> None:
    if memory_depth < 1:
        raise ValueError("memory_depth must be >= 1")

    conn = get_conn()
    ensure_tables(conn)

    zones = fetch_zones(conn, limit)
    n_zones = len(zones)
    if n_zones <= memory_depth:
        print(f"Not enough zones: have {n_zones}, need > memory_depth ({memory_depth})")
        return

    total_samples = max(0, n_zones - memory_depth)
    run_id = f"zp-sgd-{uuid.uuid4().hex[:8]}"
    algo_desc = f"SGD(log_loss) using previous {memory_depth} zone_personality rows"

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO snowball_run (run_id, algo, label_target, total_samples)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, algo_desc, "direction(zone_i) from previous zone_personalities", total_samples),
        )

    print(f"Snowball run id: {run_id}")
    print(f"Zones fetched: {n_zones}, training samples (after warm-up): {total_samples}")

    clf = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=1e-4,
        learning_rate="optimal",
        max_iter=1,
        tol=None,
        random_state=42,
    )

    history: List[Dict] = []  # zone_personality rows
    correct = 0
    total = 0
    step_index = 0
    model_initialized = False

    for idx, zone in enumerate(zones):
        zone_id = zone.id
        true_dir = zone.direction

        # We can only predict after we have enough previous personalities.
        if len(history) >= memory_depth:
            # Features built from the previous `memory_depth` zones.
            window = history[-memory_depth:]
            feat_vec = features_from_history(window)
            x_row = np.asarray(feat_vec, dtype=float).reshape(1, -1)

            if not model_initialized:
                # First sample: only initialise model via partial_fit.
                clf.partial_fit(x_row, [true_dir], classes=CLASSES)
                model_initialized = True
                print(f"Warm-up on zone_id={zone_id}, dir={true_dir}")
            else:
                # Predict direction for CURRENT zone using previous personalities.
                pred_label = int(clf.predict(x_row)[0])
                proba_vec = _predict_proba_any(clf, x_row)

                total += 1
                if pred_label == true_dir:
                    correct += 1
                step_index += 1

                acc = correct / total if total else 0.0
                if step_index % REPORT_EVERY == 0 or idx == n_zones - 1:
                    print(
                        f"Index {idx} (zone_id={zone_id}): "
                        f"running accuracy = {acc:.3f}"
                    )

                # For compatibility with old schema:
                # base_zone = last previous zone, target_zone = current zone.
                base_zone = zones[idx - 1]

                insert_prediction(
                    conn=conn,
                    run_id=run_id,
                    step_index=step_index,
                    base_zone=base_zone,
                    target_zone=zone,
                    true_label=true_dir,
                    pred_label=pred_label,
                    proba_vec=proba_vec,
                )

                # Learn from this sample
                clf.partial_fit(x_row, [true_dir])

        # After prediction+learning: ensure personality for the CURRENT zone exists
        # and push it into history for future steps.
        zp = fetch_zone_personality(conn, zone_id)
        history.append(zp)

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

    # Save model for later reuse
    model_dir = os.path.join(os.path.dirname(__file__), "model_store")
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"{run_id}_sgd.joblib")

    try:
        from joblib import dump as joblib_dump

        joblib_dump(clf, model_path)
        print(f"Saved model to {model_path}")
    except Exception as exc:
        print(f"Warning: could not save model to {model_path}: {exc}", file=sys.stderr)

    print(f"Snowball finished. Final accuracy = {final_acc:.3f}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Zone-personality snowball trainer")
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of zones to walk through.",
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=DEFAULT_MEMORY_DEPTH,
        help="How many previous zone personalities to use.",
    )
    args = parser.parse_args(argv)
    snowball_train(args.limit, args.memory)


if __name__ == "__main__":
    main()
