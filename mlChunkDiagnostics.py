#!/usr/bin/env python
"""
Stage 6A: Diagnostics for a single Kalman chunk.

Given a chunk_id from kal_break_chunk_comparison / kal_break_hard_chunks:
- Finds its kal_grp_min / kal_grp_max
- Pulls all rows from kal_break_train within that kal range
- Prints:
    * class balance
    * basic label stats
    * feature means per label (0 vs 1)
"""

import psycopg2
import pandas as pd

# ---------------------------
# CONFIG
# ---------------------------

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # <- change
DB_HOST = "localhost"
DB_PORT = 5432

# Choose a hard chunk to inspect
CHUNK_ID = 2  # you can change this to any chunk_id from kal_break_hard_chunks

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
    print(f"Connecting to Postgres (chunk_id = {CHUNK_ID})...")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    # 1) Fetch chunk metadata (kal range, metrics)
    meta_query = """
        SELECT
            chunk_id,
            kal_grp_min,
            kal_grp_max,
            kal_grp_count,
            n_samples,
            n_break,
            n_continue,
            acc_offline,
            acc_online,
            roc_offline,
            roc_online,
            recall1_offline,
            recall1_online,
            f1_1_offline,
            f1_1_online,
            offline_model_name,
            online_model_name
        FROM kal_break_chunk_comparison
        WHERE chunk_id = %s;
    """

    meta_df = pd.read_sql_query(meta_query, conn, params=(CHUNK_ID,))
    if meta_df.empty:
        print(f"No chunk_comparison row found for chunk_id={CHUNK_ID}")
        conn.close()
        return

    meta = meta_df.iloc[0]
    print("\n=== Chunk metadata ===")
    print(meta.to_string())

    kal_min = int(meta["kal_grp_min"])
    kal_max = int(meta["kal_grp_max"])

    # 2) Pull all rows from kal_break_train in this Kalman range
    data_query = """
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
        WHERE kal_grp_start BETWEEN %s AND %s
        ORDER BY id_start;
    """

    df = pd.read_sql_query(data_query, conn, params=(kal_min, kal_max))
    conn.close()

    print(f"\nLoaded {len(df)} rows for kal_grp_start in [{kal_min}, {kal_max}]")

    if df.empty:
        print("No rows found for this chunk.")
        return

    # 3) Class balance
    print("\n=== Class balance in this chunk (label) ===")
    print(df["label"].value_counts(dropna=False).rename("count"))
    print(df["label"].value_counts(normalize=True).rename("ratio"))

    # 4) Quick feature means per label
    print("\n=== Feature means per label (0=CONTINUE, 1=BREAK) ===")
    group_means = df.groupby("label")[FEATURE_COLS].mean()
    print(group_means)

    # 5) Optional: key categorical distributions (vel_cat, kal_cat, vol_cat, mom_cat)
    print("\n=== vel_cat distribution by label ===")
    print(df.groupby("label")["vel_cat"].value_counts(normalize=True).rename("ratio"))

    print("\n=== kal_cat distribution by label ===")
    print(df.groupby("label")["kal_cat"].value_counts(normalize=True).rename("ratio"))

    print("\n=== vol_cat distribution by label ===")
    print(df.groupby("label")["vol_cat"].value_counts(normalize=True).rename("ratio"))

    print("\n=== mom_cat distribution by label ===")
    print(df.groupby("label")["mom_cat"].value_counts(normalize=True).rename("ratio"))


if __name__ == "__main__":
    main()
