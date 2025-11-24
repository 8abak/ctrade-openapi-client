# PATH: ml/kalseg_snowball.py
"""
Snowball training over kalseg segments using kalseg_outcome as labels.

- Uses first N segments (default 5000).
- Chunks of CHUNK_SIZE (default 5).
- Chunk 0 is used only for initial training.
- For chunk k>=1:
    * predict labels with current model
    * log predictions to kalseg_prediction
    * add these 5 segments to training set
    * retrain model from scratch

We model 3 classes:
    -1 : "eventually significant DOWN regime"
     0 : "no strong regime within horizon"
    +1 : "eventually significant UP regime"
"""

import argparse
import uuid
from typing import List, Tuple

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr


CHUNK_SIZE = 5


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

    ts_col = detect_ts_col(conn)         # will resolve to "timestamp"
    mid_expr = detect_mid_expr(conn)     # will resolve to "mid"

    # 1) base_stream: raw ticks
    # 2) enriched: add first/last price using window functions
    # 3) stats: aggregate summary
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
        max(p_start)                   AS p_start,  -- same for all rows
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
        if ts_min is not None and ts_max is not None else 0.0
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
    meta = []  # each: dict with seg_id, start_id,...

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

        # feature vector
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

        # update context for next segment
        prev_dir = dir_kalseg
        prev_label = final_label

    X = np.array(X, dtype=float)
    y = np.array(y, dtype=int)
    return X, y, meta


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
    Insert prediction rows into kalseg_prediction, handling the fact
    that scikit-learn may only give probabilities for classes that
    have been seen in training so far.

    We always store three columns:
        proba_down  = P(label = -1)
        proba_none  = P(label =  0)
        proba_up    = P(label = +1)

    If a class hasn't been seen yet by the model, its probability is 0.0.
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
        for i, meta in enumerate(meta_chunk):
            probs = proba_chunk[i]
            cur.execute(
                sql,
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
                    "proba_up":   get_prob(probs, 1),
                },
            )
    conn.commit()



def snowball_train(limit_segments: int):
    conn = get_conn()

    rows = fetch_labeled_segments(conn, limit_segments)
    if not rows:
        print("No labeled segments found; did you run build_kalseg_outcome?")
        return

    # Build base feature matrix
    X_all, y_all, meta_all = build_feature_matrix(conn, rows)
    n = X_all.shape[0]
    if n == 0:
        print("No features built (maybe no ticks in these segments).")
        return

    print(f"Total segments with features: {n}")

    # Align chunks by index
    num_chunks = (n + CHUNK_SIZE - 1) // CHUNK_SIZE
    run_id = f"run-{uuid.uuid4().hex[:10]}"
    print(f"Run id: {run_id}, chunks: {num_chunks}, chunk size: {CHUNK_SIZE}")

    # Classifier: 3-class Gradient Boosting
    clf = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=3,
        subsample=0.9,
        random_state=42,
    )

    # Chunk 0: initial training
    first_end = min(CHUNK_SIZE, n)
    X_train = X_all[:first_end]
    y_train = y_all[:first_end]
    meta_train = meta_all[:first_end]

    clf.fit(X_train, y_train)
    print(f"Initial training on {len(y_train)} segments (chunk 0).")

    # Walk forward chunk by chunk
    for chunk_idx in range(1, num_chunks):
        start = chunk_idx * CHUNK_SIZE
        end = min((chunk_idx + 1) * CHUNK_SIZE, n)

        X_chunk = X_all[start:end]
        y_chunk = y_all[start:end]
        meta_chunk = meta_all[start:end]

        if len(y_chunk) == 0:
            continue

        # 1) Predict
        proba = clf.predict_proba(X_chunk)
        y_pred = clf.predict(X_chunk)

        print(f"\nChunk {chunk_idx}: {len(y_chunk)} segments")
        print(classification_report(y_chunk, y_pred, digits=3))

        # 2) Log predictions
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

        # 3) Extend training set and retrain
        X_train = np.vstack([X_train, X_chunk])
        y_train = np.concatenate([y_train, y_chunk])
        clf.fit(X_train, y_train)
        print(f"Retrained model on {len(y_train)} segments (up to chunk {chunk_idx}).")

    print("\nSnowball training complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    snowball_train(args.limit)
