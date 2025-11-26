# PATH: ml/snowball.py
"""
Incremental "snowball" training with SGDClassifier on kalseg segments.

- Uses kalseg_outcome as labels (same labels as before: -1, 0, +1).
- Reads the first N labeled segments (ordered by kalseg.id ASC).
- Builds one feature vector per segment (same features as old kalseg_snowball).
- Trains an SGDClassifier online, **one segment at a time**:
    * First segment: only used to initialise the model (no prediction stored).
    * From segment 2 onwards:
        - predict label with current model
        - log prediction + probabilities into snowball_prediction
        - update running accuracy (in memory)
        - partial_fit on this segment (online learning step)

Metadata:
    - snowball_run: one row per run (run_id, limits, model, accuracy, duration, etc.)
    - snowball_prediction: one row per (run_id, segment) with per-class probabilities.

Later, this script can be turned into a daemon/service that keeps running and
consumes new segments as they are created.
"""

import argparse
import uuid
import time
from pathlib import Path
from typing import List, Tuple

import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import classification_report, accuracy_score
import joblib

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr

# --- configuration ---------------------------------------------------------

# We conceptually keep CHUNK_SIZE to align with previous queries,
# but here "chunk" == "one segment".
CHUNK_SIZE = 1

# Label universe we care about
CLASSES = np.array([-1, 0, 1], dtype=int)

# Where to store models
MODEL_DIR = Path(__file__).resolve().parent / "model_store"


# --- helpers to fetch segments & build features ---------------------------

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
    Extract segment-level features using auto-detected columns.
    """
    ts_col = detect_ts_col(conn)         # e.g. "timestamp"
    mid_expr = detect_mid_expr(conn)     # e.g. "mid"

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
        count(*)                    AS n_ticks,
        min(price)                  AS p_min,
        max(price)                  AS p_max,
        avg(price)                  AS p_mean,
        stddev_pop(price)           AS p_std,
        min(ts)                     AS ts_start,
        max(ts)                     AS ts_end,
        avg(price - p_start)        AS avg_offset_from_start,
        avg(p_end - price)          AS avg_offset_to_end
    FROM enriched;
    """
    with dict_cur(conn) as cur:
        cur.execute(sql, (start_id, end_id))
        row = cur.fetchone()
        if not row or row["n_ticks"] is None or row["n_ticks"] == 0:
            return None
        return row


def build_feature_matrix(conn, seg_rows: List[dict]) -> Tuple[np.ndarray, np.ndarray, List[dict]]:
    """
    Build X, y, meta from seg_rows.
    Includes simple previous-segment context (previous direction & previous label).
    """
    X: List[List[float]] = []
    y: List[int] = []
    meta: List[dict] = []

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

        n_ticks = float(stats["n_ticks"])
        p_min = float(stats["p_min"])
        p_max = float(stats["p_max"])
        p_mean = float(stats["p_mean"])
        p_std = float(stats["p_std"] or 0.0)
        avg_offset_from_start = float(stats["avg_offset_from_start"] or 0.0)
        avg_offset_to_end = float(stats["avg_offset_to_end"] or 0.0)

        # Direction encoded as -1 / 0 / +1
        dir_raw = int(row["dir_kalseg"])
        dir_encoded = -1 if dir_raw < 0 else (1 if dir_raw > 0 else 0)

        # Feature vector (keep it small + robust)
        feats = [
            n_ticks,
            p_min,
            p_max,
            p_mean,
            p_std,
            avg_offset_from_start,
            avg_offset_to_end,
            dir_encoded,
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

    X_arr = np.array(X, dtype=float)
    y_arr = np.array(y, dtype=int)
    return X_arr, y_arr, meta


# --- DB schema helpers ----------------------------------------------------

def ensure_tables(conn):
    """
    Create snowball_run and snowball_prediction tables if they don't exist.
    """
    sql_run = """
    CREATE TABLE IF NOT EXISTS snowball_run (
        id               SERIAL PRIMARY KEY,
        run_id           TEXT UNIQUE NOT NULL,
        started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at      TIMESTAMPTZ,
        limit_segments   INTEGER,
        chunk_size       INTEGER,
        model_name       TEXT,
        feature_set      TEXT,
        total_segments   INTEGER,
        correct_segments INTEGER,
        accuracy_pct     NUMERIC(6,2),
        duration_sec     NUMERIC,
        model_path       TEXT,
        notes            TEXT
    );
    """

    sql_pred = """
    CREATE TABLE IF NOT EXISTS snowball_prediction (
        id           BIGSERIAL PRIMARY KEY,
        run_id       TEXT NOT NULL,
        chunk_index  INTEGER NOT NULL,   -- here: segment index (0-based)
        seg_id       INTEGER NOT NULL,
        start_id     INTEGER NOT NULL,
        dir_kalseg   INTEGER NOT NULL,
        true_label   INTEGER NOT NULL,
        pred_label   INTEGER NOT NULL,
        proba_down   DOUBLE PRECISION,
        proba_none   DOUBLE PRECISION,
        proba_up     DOUBLE PRECISION,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    with dict_cur(conn) as cur:
        cur.execute(sql_run)
        cur.execute(sql_pred)
    conn.commit()


def start_run(conn, limit_segments: int, feature_desc: str) -> str:
    """
    Insert a row into snowball_run and return the run_id.
    """
    run_id = "sgd-" + uuid.uuid4().hex[:10]

    with dict_cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO snowball_run (
                run_id, started_at, limit_segments, chunk_size,
                model_name, feature_set
            ) VALUES (
                %(run_id)s, NOW(), %(limit_segments)s, %(chunk_size)s,
                %(model_name)s, %(feature_set)s
            );
            """,
            {
                "run_id": run_id,
                "limit_segments": limit_segments,
                "chunk_size": CHUNK_SIZE,
                "model_name": "SGDClassifier(log_loss)",
                "feature_set": feature_desc,
            },
        )
    conn.commit()
    return run_id


def finalize_run(
    conn,
    run_id: str,
    total_segments: int,
    correct_segments: int,
    start_time: float,
    model_path: str,
    notes: str = "",
):
    duration_sec = time.time() - start_time
    accuracy_pct = (
        100.0 * correct_segments / total_segments if total_segments > 0 else None
    )

    with dict_cur(conn) as cur:
        cur.execute(
            """
            UPDATE snowball_run
            SET finished_at      = NOW(),
                total_segments   = %(total)s,
                correct_segments = %(correct)s,
                accuracy_pct     = %(acc)s,
                duration_sec     = %(dur)s,
                model_path       = %(model_path)s,
                notes            = %(notes)s
            WHERE run_id = %(run_id)s;
            """,
            {
                "run_id": run_id,
                "total": total_segments,
                "correct": correct_segments,
                "acc": accuracy_pct,
                "dur": duration_sec,
                "model_path": model_path,
                "notes": notes,
            },
        )
    conn.commit()


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
    Insert prediction rows into snowball_prediction, handling the fact
    that SGDClassifier.predict_proba may only output probabilities for
    the classes seen so far.

    We always store three columns:
        proba_down = P(label = -1)
        proba_none = P(label =  0)
        proba_up   = P(label = +1)
    """

    def get_prob(vec, label):
        for cls, p in zip(classes, vec):
            if cls == label:
                return float(p)
        return 0.0

    with dict_cur(conn) as cur:
        for i, meta in enumerate(meta_chunk):
            probs = proba_chunk[i]

            cur.execute(
                """
                INSERT INTO snowball_prediction (
                    run_id,
                    chunk_index,
                    seg_id,
                    start_id,
                    dir_kalseg,
                    true_label,
                    pred_label,
                    proba_down,
                    proba_none,
                    proba_up
                ) VALUES (
                    %(run_id)s,
                    %(chunk_index)s,
                    %(seg_id)s,
                    %(start_id)s,
                    %(dir_kalseg)s,
                    %(true_label)s,
                    %(pred_label)s,
                    %(proba_down)s,
                    %(proba_none)s,
                    %(proba_up)s
                );
                """,
                {
                    "run_id": run_id,
                    "chunk_index": chunk_index,
                    "seg_id": meta["seg_id"],
                    "start_id": meta["start_id"],
                    "dir_kalseg": meta["dir_kalseg"],
                    "true_label": int(y_true_chunk[i]),
                    "pred_label": int(y_pred_chunk[i]),
                    "proba_down": get_prob(probs, -1),
                    "proba_none": get_prob(probs, 0),
                    "proba_up": get_prob(probs, 1),
                },
            )
    conn.commit()


# --- main snowball training loop -----------------------------------------

def snowball_train(limit_segments: int):
    conn = get_conn()
    ensure_tables(conn)

    # 1) get the first N labeled segments
    rows = fetch_labeled_segments(conn, limit_segments)
    if not rows:
        print("No labeled segments found; did you run build_kalseg_outcome?")
        return

    # 2) build feature matrix
    X_all, y_all, meta_all = build_feature_matrix(conn, rows)
    n = X_all.shape[0]
    if n == 0:
        print("No features built (maybe no ticks in these segments).")
        return

    print(f"Total segments with features: {n}")

    feature_desc = (
        "n_ticks, p_min, p_max, p_mean, p_std, "
        "avg_offset_from_start, avg_offset_to_end, "
        "dir_encoded, prev_dir, prev_label"
    )

    run_id = start_run(conn, limit_segments, feature_desc)
    print(f"Run id: {run_id}")

    # Make sure model directory exists
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = str(MODEL_DIR / f"{run_id}_sgd.joblib")

    start_time = time.time()

    # 3) initialise SGD model
    clf = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=1e-4,
        learning_rate="optimal",
        random_state=42,
    )

    # First segment: initialise model, no prediction stored.
    X0 = X_all[0:1]
    y0 = y_all[0:1]
    clf.partial_fit(X0, y0, classes=CLASSES)
    correct = 0
    total_predicted = 0

    # 4) process remaining segments one by one
    for idx in range(1, n):
        X_seg = X_all[idx:idx + 1]
        y_seg = y_all[idx:idx + 1]
        meta_seg = [meta_all[idx]]

        # Predict with current model
        y_pred = clf.predict(X_seg)
        if hasattr(clf, "predict_proba"):
            proba = clf.predict_proba(X_seg)
        else:
            # fallback: decision_function -> pseudo probs
            decision = clf.decision_function(X_seg)
            # softmax-like normalisation over 3 classes
            exp = np.exp(decision - np.max(decision))
            proba = (exp / exp.sum(axis=1, keepdims=True))

        # Store prediction in DB
        insert_predictions(
            conn,
            run_id,
            chunk_index=idx,   # chunk_index is effectively "segment index"
            meta_chunk=meta_seg,
            y_true_chunk=y_seg,
            proba_chunk=proba,
            y_pred_chunk=y_pred,
            classes=CLASSES,
        )

        # Update running accuracy
        total_predicted += 1
        if int(y_pred[0]) == int(y_seg[0]):
            correct += 1

        if total_predicted % 100 == 0:
            acc = correct / total_predicted
            print(f"Up to segment {idx}: accuracy = {acc:.3f}")

        # Online update
        clf.partial_fit(X_seg, y_seg)

    # 5) final stats and model persistence
    joblib.dump(clf, model_path)
    print(f"Saved final SGD model to {model_path}")

    # Final evaluation on all predicted segments (1..n-1)
    y_true_all = y_all[1:]
    # Re-run model on all features for a clean report
    y_pred_all = clf.predict(X_all[1:])
    report = classification_report(y_true_all, y_pred_all)
    print("\nFinal classification report (on segments 1..N):")
    print(report)

    correct_final = int((y_true_all == y_pred_all).sum())
    finalize_run(
        conn,
        run_id=run_id,
        total_segments=int(y_true_all.shape[0]),
        correct_segments=correct_final,
        start_time=start_time,
        model_path=model_path,
        notes=report,
    )

    print("\nSnowball training complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    snowball_train(args.limit)
