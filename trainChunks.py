#!/usr/bin/env python
"""
Stage 5: Online forward-learning over Kalman chunks.

- Loads data from kal_break_train (subset by MAX_ROWS)
- Sorts by id_start
- Groups rows into chunks by kal_grp_start (KAL_CHUNK_SIZE distinct kal_grps per chunk)
- Uses an online SGDClassifier with partial_fit
- Features are globally standardized once via StandardScaler BEFORE chunking
- Chunk 0: train only (bootstrap)
- Chunks 1..N:
    * predict on chunk -> metrics
    * insert metrics into kal_break_online_chunks
    * update model with partial_fit on that chunk
"""

import os
import psycopg2
import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import (
    accuracy_score,
    roc_auc_score,
    precision_recall_fscore_support,
)

# ---------------------------
# CONFIG
# ---------------------------

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # <-- change this
DB_HOST = "localhost"
DB_PORT = 5432

# Use same subset as before for now
MAX_ROWS = 500_000

# Chunk size in terms of distinct kal_grp_start values
KAL_CHUNK_SIZE = 5

MODEL_NAME = "stage5_online_sgd_v1"

FEATURE_COLS = [
    "mic_dm", "mic_dt", "mic_v",
    "gap_flag", "gap_dir", "gap_sz",
    "vel_cat", "vel_pos", "vel_len",
    "kal_cat", "kal_pos", "kal_len", "kal_chg", "kal_val",
    "mom_cat", "mom_pos", "mom_len",
    "vol_cat", "vol_pos", "vol_len", "vol_val",
]


# ---------------------------
# MAIN
# ---------------------------

def main():
    # 1) Connect to DB
    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cur = conn.cursor()

    # 2) Load data
    print("Loading data from kal_break_train...")

    base_query = """
        SELECT
            vel_grp,
            id_start,
            kal_grp_start,
            label,

            mic_dm,
            mic_dt,
            mic_v,

            gap_flag,
            gap_dir,
            gap_sz,

            vel_cat,
            vel_pos,
            vel_len,

            kal_cat,
            kal_pos,
            kal_len,
            kal_chg,
            kal_val,

            mom_cat,
            mom_pos,
            mom_len,

            vol_cat,
            vol_pos,
            vol_len,
            vol_val
        FROM kal_break_train
        ORDER BY id_start
    """

    if MAX_ROWS is not None:
        query = base_query + f" LIMIT {int(MAX_ROWS)}"
        print(f"  -> Using first {MAX_ROWS} rows.")
    else:
        query = base_query
        print("  -> Using ALL rows.")

    df = pd.read_sql_query(query, conn)
    print(f"Loaded {len(df):,} rows.")

    # Convert boolean to int
    if "gap_flag" in df.columns:
        df["gap_flag"] = df["gap_flag"].astype(int)

    # Fill NULLs
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)

    # 3) Global scaling (one-time, before chunking)
    print("Fitting global StandardScaler on all rows...")
    scaler = StandardScaler()
    X_all_raw = df[FEATURE_COLS].values
    X_all = scaler.fit_transform(X_all_raw)
    y_all = df["label"].astype(int).values

    # 4) Compute global class weights manually for imbalance handling
    classes = np.array([0, 1], dtype=int)
    class_counts = np.bincount(y_all, minlength=2).astype(float)
    total = class_counts.sum()
    class_freq = class_counts / total
    # Inverse-frequency weights (rare class gets higher weight)
    eps = 1e-8
    class_weight = 1.0 / np.maximum(class_freq, eps)
    # Optional normalization (not strictly needed, but keeps numbers reasonable)
    class_weight = class_weight / class_weight.mean()

    print("Global class counts:", class_counts)
    print("Global class freq  :", class_freq)
    print("Global class weight:", class_weight)

    # 5) Prepare Kalman chunks
    unique_kal = df["kal_grp_start"].dropna().unique()
    unique_kal.sort()
    total_kal = len(unique_kal)
    print(f"Found {total_kal} distinct kal_grp_start values in this subset.")

    kal_chunks = []
    for start_idx in range(0, total_kal, KAL_CHUNK_SIZE):
        kal_chunk = unique_kal[start_idx:start_idx + KAL_CHUNK_SIZE]
        kal_chunks.append(kal_chunk)

    print(f"Total chunks: {len(kal_chunks)} (each ~{KAL_CHUNK_SIZE} kal_grps)")

    # 6) Initialize online model (fresh, no built-in class_weight)
    model = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        max_iter=1,
        tol=None,
        n_jobs=-1,
        random_state=42,
    )

    n_seen_total = 0

    # 7) Walk forward over chunks
    for chunk_id, kal_chunk in enumerate(kal_chunks):
        kal_min = int(kal_chunk.min())
        kal_max = int(kal_chunk.max())
        kal_count = int(len(kal_chunk))

        df_chunk = df[df["kal_grp_start"].isin(kal_chunk)]

        if df_chunk.empty:
            continue

        idx = df_chunk.index.to_numpy()
        X_chunk = X_all[idx]
        y_chunk = y_all[idx]

        n_samples = len(df_chunk)
        n_break = int((y_chunk == 1).sum())
        n_continue = int((y_chunk == 0).sum())

        print(
            f"Chunk {chunk_id}: kal_grp {kal_min}-{kal_max} "
            f"(groups={kal_count}, samples={n_samples}, "
            f"break={n_break}, cont={n_continue})"
        )

        # Compute sample weights for this chunk from global class_weight
        sample_weight = class_weight[y_chunk]

        # CHUNK 0: bootstrap training only (no evaluation)
        if chunk_id == 0:
            print("  -> Bootstrapping model with initial chunk (no evaluation).")
            model.partial_fit(X_chunk, y_chunk, classes=classes, sample_weight=sample_weight)
            n_seen_total += n_samples
            continue

        # For all later chunks:
        # 1) Evaluate with current model (trained on all previous chunks)
        y_pred = model.predict(X_chunk)

        # ROC AUC
        roc = None
        try:
            if hasattr(model, "predict_proba"):
                y_score = model.predict_proba(X_chunk)[:, 1]
                roc = roc_auc_score(y_chunk, y_score)
            elif hasattr(model, "decision_function"):
                y_dec = model.decision_function(X_chunk)
                roc = roc_auc_score(y_chunk, y_dec)
        except Exception:
            roc = None

        acc = accuracy_score(y_chunk, y_pred)

        prec, rec, f1, _ = precision_recall_fscore_support(
            y_chunk, y_pred, labels=[0, 1], zero_division=0
        )

        precision_0, precision_1 = float(prec[0]), float(prec[1])
        recall_0, recall_1 = float(rec[0]), float(rec[1])
        f1_0, f1_1 = float(f1[0]), float(f1[1])

        print(
            f"  -> Eval before update: acc={acc:.4f}, "
            f"roc_auc={roc if roc is not None else 'NA'}"
        )

        # 2) Insert metrics into kal_break_online_chunks
        insert_sql = """
            INSERT INTO kal_break_online_chunks (
                chunk_id,
                kal_grp_min,
                kal_grp_max,
                kal_grp_count,
                n_samples,
                n_break,
                n_continue,
                accuracy,
                roc_auc,
                precision_0,
                recall_0,
                f1_0,
                precision_1,
                recall_1,
                f1_1,
                n_samples_seen_before,
                model_name
            )
            VALUES (
                %(chunk_id)s,
                %(kal_grp_min)s,
                %(kal_grp_max)s,
                %(kal_grp_count)s,
                %(n_samples)s,
                %(n_break)s,
                %(n_continue)s,
                %(accuracy)s,
                %(roc_auc)s,
                %(precision_0)s,
                %(recall_0)s,
                %(f1_0)s,
                %(precision_1)s,
                %(recall_1)s,
                %(f1_1)s,
                %(n_seen_before)s,
                %(model_name)s
            )
            ON CONFLICT (chunk_id) DO UPDATE SET
                kal_grp_min = EXCLUDED.kal_grp_min,
                kal_grp_max = EXCLUDED.kal_grp_max,
                kal_grp_count = EXCLUDED.kal_grp_count,
                n_samples = EXCLUDED.n_samples,
                n_break = EXCLUDED.n_break,
                n_continue = EXCLUDED.n_continue,
                accuracy = EXCLUDED.accuracy,
                roc_auc = EXCLUDED.roc_auc,
                precision_0 = EXCLUDED.precision_0,
                recall_0 = EXCLUDED.recall_0,
                f1_0 = EXCLUDED.f1_0,
                precision_1 = EXCLUDED.precision_1,
                recall_1 = EXCLUDED.recall_1,
                f1_1 = EXCLUDED.f1_1,
                n_samples_seen_before = EXCLUDED.n_samples_seen_before,
                model_name = EXCLUDED.model_name;
        """

        cur.execute(insert_sql, {
            "chunk_id": chunk_id,
            "kal_grp_min": kal_min,
            "kal_grp_max": kal_max,
            "kal_grp_count": kal_count,
            "n_samples": n_samples,
            "n_break": n_break,
            "n_continue": n_continue,
            "accuracy": float(acc),
            "roc_auc": float(roc) if roc is not None else None,
            "precision_0": precision_0,
            "recall_0": recall_0,
            "f1_0": f1_0,
            "precision_1": precision_1,
            "recall_1": recall_1,
            "f1_1": f1_1,
            "n_seen_before": int(n_seen_total),
            "model_name": MODEL_NAME,
        })

        # 3) Update model with this chunk (learning from its actual outcomes)
        model.partial_fit(X_chunk, y_chunk, sample_weight=sample_weight)
        n_seen_total += n_samples

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nFinished online progression over {len(kal_chunks)} chunks.")
    print("Metrics stored in kal_break_online_chunks.")


if __name__ == "__main__":
    main()
