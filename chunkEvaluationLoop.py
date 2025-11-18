#!/usr/bin/env python
"""
Stage 4: Chunked evaluation loop for Kalman break model.

- Loads data from kal_break_train (same subset as Stage 3)
- Loads trained model from Stage 3
- Groups by kal_grp_start into chunks of size K kalman groups
- For each chunk:
    - computes predictions and metrics
    - inserts a row into kal_break_eval_chunks
"""

import os
import psycopg2
import pandas as pd
from sklearn.metrics import (
    classification_report,
    accuracy_score,
    roc_auc_score,
    precision_recall_fscore_support,
)
import joblib

# ---------------------------
# CONFIG
# ---------------------------

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # <-- change
DB_HOST = "localhost"
DB_PORT = 5432

# Use the same max rows as training, so we evaluate on same universe
MAX_ROWS = 500_000

# Chunk size in terms of DISTINCT kal_grp_start values
KAL_CHUNK_SIZE = 5

MODEL_DIR = "models"
MODEL_PATH = os.path.join(MODEL_DIR, "kal_break_stage3_baseline.pkl")
MODEL_NAME = "stage3_baseline_sgd"

# ---------------------------
# FEATURES
# ---------------------------

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

    # Fill any NULLs
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)

    # 3) Load model
    print(f"Loading model from {MODEL_PATH} ...")
    model = joblib.load(MODEL_PATH)

    # 4) Prepare chunking by kal_grp_start
    unique_kal = df["kal_grp_start"].dropna().unique()
    unique_kal.sort()
    total_kal = len(unique_kal)
    print(f"Found {total_kal} distinct kal_grp_start values in this subset.")

    # 5) Loop over chunks
    chunk_id = 0
    for start_idx in range(0, total_kal, KAL_CHUNK_SIZE):
        kal_chunk = unique_kal[start_idx:start_idx + KAL_CHUNK_SIZE]
        kal_min = int(kal_chunk.min())
        kal_max = int(kal_chunk.max())
        kal_count = len(kal_chunk)

        df_chunk = df[df["kal_grp_start"].isin(kal_chunk)]

        if df_chunk.empty:
            continue

        X_chunk = df_chunk[FEATURE_COLS]
        y_chunk = df_chunk["label"]

        # Predictions
        y_pred = model.predict(X_chunk)

        # Try probabilities or decision function for ROC AUC
        try:
            y_score = model.predict_proba(X_chunk)[:, 1]
        except Exception:
            y_score = model.decision_function(X_chunk)

        # Metrics
        acc = accuracy_score(y_chunk, y_pred)
        try:
            roc = roc_auc_score(y_chunk, y_score)
        except ValueError:
            # If only one class present in y_chunk, ROC AUC is undefined
            roc = None

        # precision_recall_fscore_support returns arrays for [class0, class1]
        prec, rec, f1, _ = precision_recall_fscore_support(
            y_chunk, y_pred, labels=[0, 1], zero_division=0
        )

        precision_0, precision_1 = float(prec[0]), float(prec[1])
        recall_0, recall_1 = float(rec[0]), float(rec[1])
        f1_0, f1_1 = float(f1[0]), float(f1[1])

        n_samples = int(len(df_chunk))
        n_break = int((y_chunk == 1).sum())
        n_continue = int((y_chunk == 0).sum())

        print(
            f"Chunk {chunk_id}: kal_grp {kal_min}-{kal_max} "
            f"(groups={kal_count}, samples={n_samples}) "
            f"acc={acc:.4f}, roc_auc={roc if roc is not None else 'NA'}"
        )

        # 6) Insert into kal_break_eval_chunks
        insert_sql = """
            INSERT INTO kal_break_eval_chunks (
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
            "model_name": MODEL_NAME,
        })

        chunk_id += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Finished. Wrote metrics for {chunk_id} chunks to kal_break_eval_chunks.")


if __name__ == "__main__":
    main()
