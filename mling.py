#!/usr/bin/env python
"""
Stage 3: Baseline training pipeline for Kalman break prediction.

- Reads features from kal_break_train
- Time-based train/test split
- Trains a baseline SGDClassifier
- Prints metrics
- Saves model to models/kal_break_stage3_baseline.pkl
"""

import os
import psycopg2
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import classification_report, roc_auc_score, accuracy_score
import joblib

# ---------------------------
# CONFIG
# ---------------------------

# ⚠️ Set your database password here
DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # <-- change this
DB_HOST = "localhost"
DB_PORT = 5432

# Limit rows for memory / speed. Set to None to use all 9.9M rows.
MAX_ROWS = 500_000  # e.g. 500k for first run; set to None later if you want full data

MODEL_DIR = "models"
MODEL_PATH = os.path.join(MODEL_DIR, "kal_break_stage3_baseline.pkl")


# ---------------------------
# MAIN
# ---------------------------

def main():
    # 1) Connect to Postgres
    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    # 2) Load data from kal_break_train
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
        print(f"  -> Using first {MAX_ROWS} rows (for speed).")
    else:
        query = base_query
        print("  -> Using ALL rows.")

    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Loaded {len(df):,} rows.")

    # 3) Basic cleaning / feature selection
    feature_cols = [
        "mic_dm", "mic_dt", "mic_v",
        "gap_flag", "gap_dir", "gap_sz",
        "vel_cat", "vel_pos", "vel_len",
        "kal_cat", "kal_pos", "kal_len", "kal_chg", "kal_val",
        "mom_cat", "mom_pos", "mom_len",
        "vol_cat", "vol_pos", "vol_len", "vol_val",
    ]

    target_col = "label"

    # Convert boolean to int (gap_flag)
    if "gap_flag" in df.columns:
        df["gap_flag"] = df["gap_flag"].astype(int)

    # Handle any NULLs by filling with 0 for now
    df[feature_cols] = df[feature_cols].fillna(0)

    X = df[feature_cols]
    y = df[target_col]

    print("Class balance (0=CONTINUE, 1=BREAK):")
    print(y.value_counts(normalize=False).rename("count"))
    print(y.value_counts(normalize=True).rename("ratio"))

    # 4) Time-based train/test split (80% earliest, 20% latest by id_start)
    n = len(df)
    split_idx = int(n * 0.8)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]

    X_train = train[feature_cols]
    y_train = train[target_col]

    X_test = test[feature_cols]
    y_test = test[target_col]

    print(f"Train size: {len(X_train):,} / Test size: {len(X_test):,}")

    # 5) Build model pipeline
    # SGDClassifier is online-friendly and can scale to large data.
    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", SGDClassifier(
            loss="log_loss",
            penalty="l2",
            class_weight="balanced",  # handle imbalance
            max_iter=10,
            tol=1e-3,
            n_jobs=-1,
            random_state=42,
        )),
    ])

    # 6) Train
    print("Training model...")
    model.fit(X_train, y_train)

    # 7) Evaluate
    print("Evaluating on test set...")
    y_pred = model.predict(X_test)

    try:
        y_proba = model.predict_proba(X_test)[:, 1]
        roc = roc_auc_score(y_test, y_proba)
    except Exception:
        # Some linear models may not expose predict_proba; fall back
        y_dec = model.decision_function(X_test)
        roc = roc_auc_score(y_test, y_dec)

    acc = accuracy_score(y_test, y_pred)
    print(f"\nAccuracy: {acc:.4f}")
    print(f"ROC AUC: {roc:.4f}\n")

    print("Classification report:")
    print(classification_report(y_test, y_pred, digits=4))

    # 8) Save model
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    print(f"\nModel saved to: {MODEL_PATH}")


if __name__ == "__main__":
    main()
