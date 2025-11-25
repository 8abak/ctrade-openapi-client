# PATH: ml/kalseg_snowball.py
"""
Persistent snowball training over kalseg segments using kalseg_outcome.

Key ideas
---------
* Work in chronological order over kalseg.id.
* CHUNK_SIZE = 1 => one segment per learning step.
* Chunk 0: used only as initial training (no prediction logged).
* For each subsequent chunk:
    - predict label for that segment
    - log prediction to kalseg_prediction
    - compute accuracy for that chunk and record in kalseg_run_stats
    - update the model:
        * if accuracy < ACCURACY_THRESHOLD:
              reset model and retrain from scratch on ALL data seen so far
        * else:
              gently extend ensemble with a few more trees (warm_start)

Persistence
-----------
We persist a single "brain":

    ml/model_store/kalseg_gb.pkl         (sklearn model)
    ml/model_store/kalseg_gb_meta.json   (metadata)

Meta fields:
    trained_index         : how many segments (from the start) are in the
                            training set the model has seen so far.
    n_estimators_current  : current size of the ensemble (trees).

On each new run:
    - If model+meta exist:
        * load classifier + meta
        * rebuild features from DB
        * reconstruct X_train, y_train from the first `trained_index` segments
        * continue snowball from the first unseen segment
    - If not:
        * start from scratch, using chunk 0 as initial training only
"""

import argparse
import json
import os
from pathlib import Path
import uuid
from typing import List, Tuple

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report
import joblib

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr


# ----------------- Snowball configuration -----------------

CHUNK_SIZE = 1  # one segment per chunk

# Below this accuracy we treat the chunk as "badly missed"
ACCURACY_THRESHOLD = 0.75

# Base size of the ensemble and small incremental growth
BASE_ESTIMATORS = 200
SMALL_DELTA_ESTIMATORS = 20

# Model persistence paths
MODEL_DIR = Path(__file__).resolve().parent / "model_store"
MODEL_PATH = MODEL_DIR / "kalseg_gb.pkl"
META_PATH = MODEL_DIR / "kalseg_gb_meta.json"


# ----------------- DB helpers -----------------


def ensure_prediction_tables(conn):
    """
    Ensure kalseg_prediction, kalseg_run_stats tables and a summary view exist.
    """
    ddl_prediction = """
    CREATE TABLE IF NOT EXISTS kalseg_prediction (
        run_id      text    NOT NULL,
        chunk_index integer NOT NULL,
        seg_id      integer NOT NULL,
        start_id    integer NOT NULL,
        dir_kalseg  integer NOT NULL,
        true_label  integer NOT NULL,
        pred_label  integer NOT NULL,
        proba_down  double precision NOT NULL,
        proba_none  double precision NOT NULL,
        proba_up    double precision NOT NULL,
        PRIMARY KEY (run_id, seg_id)
    );
    CREATE INDEX IF NOT EXISTS idx_kalseg_pred_run_chunk
        ON kalseg_prediction(run_id, chunk_index);
    """

    ddl_run_stats = """
    CREATE TABLE IF NOT EXISTS kalseg_run_stats (
        run_id      text    NOT NULL,
        chunk_index integer NOT NULL,
        n           integer NOT NULL,
        n_correct   integer NOT NULL,
        accuracy    double precision NOT NULL,
        PRIMARY KEY (run_id, chunk_index)
    );
    """

    ddl_view = """
    CREATE OR REPLACE VIEW kalseg_run_summary AS
    SELECT
        run_id,
        MIN(chunk_index)                      AS first_chunk,
        MAX(chunk_index)                      AS last_chunk,
        SUM(n)                                AS total_segments,
        SUM(n_correct)                        AS total_correct,
        ROUND(100.0 * SUM(n_correct)
              / GREATEST(SUM(n), 1), 2)       AS accuracy_pct
    FROM kalseg_run_stats
    GROUP BY run_id
    ORDER BY run_id;
    """

    with conn.cursor() as cur:
        cur.execute(ddl_prediction)
        cur.execute(ddl_run_stats)
        cur.execute(ddl_view)
    conn.commit()


def fetch_labeled_segments(conn, limit: int):
    """
    Fetch joined kalseg + kalseg_outcome for the first N segments
    where outcome is already computed.
    """
    sql = """
    SELECT
        k.id            AS seg_id,
        k.start_id      AS start_id,
        k.end_id        AS end_id,
        k.direction     AS direction_raw,
        o.dir_kalseg    AS dir_kalseg,
        o.final_label   AS final_label
    FROM kalseg k
    JOIN kalseg_outcome o
      ON o.seg_id = k.id
    WHERE k.id IN (
        SELECT id FROM kalseg ORDER BY id ASC LIMIT %(limit)s
    )
    ORDER BY k.id ASC;
    """
    with dict_cur(conn) as cur:
        cur.execute(sql, {"limit": limit})
        return cur.fetchall()


def fetch_segment_stats(conn, seg_id: int, start_id: int, end_id: int):
    """
    Extract segment-level features using auto-detected columns,
    using correct Postgres window/aggregation handling.
    """

    ts_col = detect_ts_col(conn)         # resolves to "timestamp"
    mid_expr = detect_mid_expr(conn)     # resolves to "mid"

    sql = f"""
    WITH base AS (
        SELECT
            id,
            {ts_col} AS ts,
            COALESCE(kal, {mid_expr}) AS price
        FROM ticks
        WHERE id BETWEEN %s AND %s
        ORDER BY id
    ),
    enriched AS (
        SELECT
            *,
            first_value(price) OVER (ORDER BY id ASC) AS p_start,
            last_value(price)  OVER (ORDER BY id ASC) AS p_end
        FROM base
    )
    SELECT
        count(*)                       AS n,
        min(ts)                        AS ts_min,
        max(ts)                        AS ts_max,
        min(price)                     AS p_min,
        max(price)                     AS p_max,
        stddev_pop(price)              AS p_std,
        max(p_start)                   AS p_start,
        max(p_end)                     AS p_end
    FROM enriched;
    """

    with dict_cur(conn) as cur:
        cur.execute(sql, (start_id, end_id))
        row = cur.fetchone()

    if not row or row["n"] == 0:
        return None

    length_ticks = int(row["n"])
    ts_min = row["ts_min"]
    ts_max = row["ts_max"]

    duration_secs = (
        (ts_max - ts_min).total_seconds()
        if ts_min is not None and ts_max is not None
        else 0.0
    )

    p_start = float(row["p_start"])
    p_end = float(row["p_end"])
    change = p_end - p_start
    abs_change = abs(change)
    p_min = float(row["p_min"])
    p_max = float(row["p_max"])
    p_std = float(row["p_std"]) if row["p_std"] is not None else 0.0

    return {
        "length_ticks": length_ticks,
        "duration_secs": duration_secs,
        "price_change": change,
        "abs_change": abs_change,
        "p_min": p_min,
        "p_max": p_max,
        "p_std": p_std,
    }


def build_feature_matrix(conn, seg_rows: List[dict]):
    """
    Build X, y, meta list from seg_rows.
    Also injects simple previous-seg context (dir, label).
    """
    X = []
    y = []
    meta = []

    prev_dir = 0
    prev_label = 0

    for row in seg_rows:
        seg_id = int(row["seg_id"])
        start_id = int(row["start_id"])
        end_id = int(row["end_id"])
        dir_kalseg = int(row["dir_kalseg"])
        final_label = int(row["final_label"])

        stats = fetch_segment_stats(conn, seg_id, start_id, end_id)
        if stats is None:
            continue

        feats = [
            dir_kalseg,
            stats["length_ticks"],
            stats["duration_secs"],
            stats["price_change"],
            stats["abs_change"],
            stats["p_min"],
            stats["p_max"],
            stats["p_std"],
            prev_dir,
            prev_label,
        ]

        X.append(feats)
        y.append(final_label)
        meta.append(
            {
                "seg_id": seg_id,
                "start_id": start_id,
                "end_id": end_id,
                "dir_kalseg": dir_kalseg,
            }
        )

        prev_dir = dir_kalseg
        prev_label = final_label

    return np.array(X, dtype=float), np.array(y, dtype=int), meta


def insert_predictions(
    conn,
    run_id: str,
    chunk_index: int,
    meta_chunk: List[dict],
    y_true_chunk: np.ndarray,
    proba_chunk: np.ndarray,
    y_pred_chunk: np.ndarray,
    classes: np.ndarray,
):
    """
    Insert prediction rows into kalseg_prediction.

    We always store three probability columns:
        proba_down  = P(label=-1)
        proba_none  = P(label=0)
        proba_up    = P(label=+1)
    """

    class_to_idx = {int(c): i for i, c in enumerate(classes)}

    def get_prob(probs, cls):
        idx = class_to_idx.get(cls)
        return float(probs[idx]) if idx is not None else 0.0

    sql = """
    INSERT INTO kalseg_prediction (
        run_id, chunk_index, seg_id, start_id, dir_kalseg,
        true_label, pred_label,
        proba_down, proba_none, proba_up
    ) VALUES (
        %(run_id)s, %(chunk_index)s, %(seg_id)s, %(start_id)s, %(dir_kalseg)s,
        %(true_label)s, %(pred_label)s,
        %(proba_down)s, %(proba_none)s, %(proba_up)s
    )
    ON CONFLICT (run_id, seg_id) DO UPDATE
       SET chunk_index = EXCLUDED.chunk_index,
           true_label  = EXCLUDED.true_label,
           pred_label  = EXCLUDED.pred_label,
           proba_down  = EXCLUDED.proba_down,
           proba_none  = EXCLUDED.proba_none,
           proba_up    = EXCLUDED.proba_up;
    """

    with dict_cur(conn) as cur:
        for i, m in enumerate(meta_chunk):
            probs = proba_chunk[i]
            cur.execute(
                sql,
                {
                    "run_id": run_id,
                    "chunk_index": chunk_index,
                    "seg_id": m["seg_id"],
                    "start_id": m["start_id"],
                    "dir_kalseg": m["dir_kalseg"],
                    "true_label": int(y_true_chunk[i]),
                    "pred_label": int(y_pred_chunk[i]),
                    "proba_down": get_prob(probs, -1),
                    "proba_none": get_prob(probs, 0),
                    "proba_up": get_prob(probs, 1),
                },
            )
    conn.commit()


def record_chunk_stats(
    conn,
    run_id: str,
    chunk_index: int,
    y_true_chunk: np.ndarray,
    y_pred_chunk: np.ndarray,
) -> float:
    """
    Store per-chunk accuracy in kalseg_run_stats and return accuracy.
    """
    n = int(len(y_true_chunk))
    n_correct = int(np.sum(y_true_chunk == y_pred_chunk))
    accuracy = (n_correct / n) if n > 0 else 0.0

    ddl = """
    CREATE TABLE IF NOT EXISTS kalseg_run_stats (
        run_id      text    NOT NULL,
        chunk_index integer NOT NULL,
        n           integer NOT NULL,
        n_correct   integer NOT NULL,
        accuracy    double precision NOT NULL,
        PRIMARY KEY (run_id, chunk_index)
    );
    """
    sql = """
    INSERT INTO kalseg_run_stats (
        run_id, chunk_index, n, n_correct, accuracy
    ) VALUES (
        %(run_id)s, %(chunk_index)s, %(n)s, %(n_correct)s, %(accuracy)s
    )
    ON CONFLICT (run_id, chunk_index) DO UPDATE
       SET n         = EXCLUDED.n,
           n_correct = EXCLUDED.n_correct,
           accuracy  = EXCLUDED.accuracy;
    """

    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(
            sql,
            {
                "run_id": run_id,
                "chunk_index": chunk_index,
                "n": n,
                "n_correct": n_correct,
                "accuracy": accuracy,
            },
        )
    conn.commit()
    return accuracy


# ----------------- Persistence helpers -----------------


def load_persistent_model() -> Tuple[GradientBoostingClassifier, dict]:
    """
    Load model + meta if they exist, otherwise return (None, default_meta).
    """
    MODEL_DIR.mkdir(exist_ok=True)

    if MODEL_PATH.exists() and META_PATH.exists():
        clf = joblib.load(MODEL_PATH)
        with META_PATH.open("r") as f:
            meta = json.load(f)
        return clf, meta

    default_meta = {
        "trained_index": 0,
        "n_estimators_current": BASE_ESTIMATORS,
    }
    return None, default_meta


def save_persistent_model(clf: GradientBoostingClassifier, meta: dict):
    MODEL_DIR.mkdir(exist_ok=True)
    joblib.dump(clf, MODEL_PATH)
    with META_PATH.open("w") as f:
        json.dump(meta, f)


# ----------------- Main snowball procedure -----------------


def snowball_train(limit_segments: int):
    conn = get_conn()
    ensure_prediction_tables(conn)

    rows = fetch_labeled_segments(conn, limit_segments)
    if not rows:
        print("No labeled segments found; did you run build_kalseg_outcome?")
        return

    X_all, y_all, meta_all = build_feature_matrix(conn, rows)
    n = X_all.shape[0]
    if n == 0:
        print("No features built (maybe no ticks in these segments).")
        return

    num_chunks = (n + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"Total segments with features: {n} (chunks: {num_chunks})")

    # Load or initialise persistent model
    clf, meta = load_persistent_model()
    trained_index = int(meta.get("trained_index", 0))
    n_estimators_current = int(meta.get("n_estimators_current", BASE_ESTIMATORS))

    # Clamp trained_index to current data size
    if trained_index > n:
        trained_index = n

    # Prepare classifier
    if clf is None:
        clf = GradientBoostingClassifier(
            n_estimators=BASE_ESTIMATORS,
            learning_rate=0.05,
            max_depth=3,
            subsample=0.9,
            random_state=42,
            warm_start=True,
        )
        n_estimators_current = BASE_ESTIMATORS
        X_train = None
        y_train = None
        start_chunk = 0
        print("No existing model found: starting from scratch.")
    else:
        # Rebuild X_train, y_train from DB features
        X_train = X_all[:trained_index]
        y_train = y_all[:trained_index]
        start_chunk = trained_index // CHUNK_SIZE
        print(
            f"Loaded existing model: trained_index={trained_index}, "
            f"start_chunk={start_chunk}, n_estimators={n_estimators_current}"
        )

    # If no training yet, use chunk 0 purely as initial training
    if trained_index == 0:
        init_end = min(CHUNK_SIZE, n)
        if init_end == 0:
            print("Nothing to train on.")
            return

        X_init = X_all[:init_end]
        y_init = y_all[:init_end]
        clf.fit(X_init, y_init)

        X_train = X_init.copy()
        y_train = y_init.copy()
        trained_index = init_end
        meta["trained_index"] = trained_index
        meta["n_estimators_current"] = n_estimators_current
        start_chunk = trained_index // CHUNK_SIZE

        print(f"Initial training on {len(y_init)} segments (chunk 0).")

    # Run id for this evaluation run
    run_id = f"run-{uuid.uuid4().hex[:10]}"
    print(f"Run id: {run_id}")

    # Snowball from first unseen chunk
    for chunk_idx in range(start_chunk, num_chunks):
        start = chunk_idx * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, n)

        if start >= end:
            continue

        X_chunk = X_all[start:end]
        y_chunk = y_all[start:end]
        meta_chunk = meta_all[start:end]

        # 1) Predict for this chunk
        proba = clf.predict_proba(X_chunk)
        y_pred = clf.predict(X_chunk)

        print(f"\nChunk {chunk_idx}: {len(y_chunk)} segments")
        print(classification_report(y_chunk, y_pred, digits=3))

        # 2) Store predictions
        insert_predictions(
            conn,
            run_id=run_id,
            chunk_index=chunk_idx,
            meta_chunk=meta_chunk,
            y_true_chunk=y_chunk,
            proba_chunk=proba,
            y_pred_chunk=y_pred,
            classes=clf.classes_,
        )

        # 3) Record accuracy for this chunk
        acc = record_chunk_stats(
            conn,
            run_id=run_id,
            chunk_index=chunk_idx,
            y_true_chunk=y_chunk,
            y_pred_chunk=y_pred,
        )
        print(f"Chunk {chunk_idx}: accuracy = {acc:.3f}")

        # 4) Extend training set to include this chunk
        X_train = np.vstack([X_train, X_chunk]) if X_train is not None else X_chunk
        y_train = (
            np.concatenate([y_train, y_chunk]) if y_train is not None else y_chunk
        )
        trained_index = X_train.shape[0]

        # 5) Decide how strongly to update the model
        if acc < ACCURACY_THRESHOLD:
            # Big reset: new model, retrain from scratch on all training data so far
            print(
                f"Chunk {chunk_idx}: accuracy below {ACCURACY_THRESHOLD:.2f}, "
                "resetting model and retraining from scratch."
            )
            clf = GradientBoostingClassifier(
                n_estimators=BASE_ESTIMATORS,
                learning_rate=0.05,
                max_depth=3,
                subsample=0.9,
                random_state=42,
                warm_start=True,
            )
            n_estimators_current = BASE_ESTIMATORS
            clf.fit(X_train, y_train)
        else:
            # Gentle extension: increase number of trees with warm_start
            n_estimators_current += SMALL_DELTA_ESTIMATORS
            clf.n_estimators = n_estimators_current
            print(
                f"Chunk {chunk_idx}: accuracy OK, "
                f"extending ensemble to {n_estimators_current} trees."
            )
            clf.fit(X_train, y_train)

        # Update meta after each chunk
        meta["trained_index"] = trained_index
        meta["n_estimators_current"] = n_estimators_current

    # Save persistent model & meta at the end
    save_persistent_model(clf, meta)

    print("\nSnowball training complete.")
    print(
        f"Final trained_index={meta['trained_index']}, "
        f"n_estimators_current={meta['n_estimators_current']}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of earliest kalseg segments to consider.",
    )
    args = parser.parse_args()
    snowball_train(args.limit)
