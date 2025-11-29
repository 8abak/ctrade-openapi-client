#!/usr/bin/env python
# ml/buildPfeat.py
#
# Build pattern-window feature table pfeat from pwin + swg.
#
# Each row in pfeat:
#   - summarizes all swings inside one window (from pwin)
#   - attaches the "next" swing as a label (next_ret, next_dir, etc.)

import sys
import math
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
}

# Big move threshold for label_bigmove (in absolute return units)
BIGMOVE_RET_THRESHOLD = 1.0

# How many windows to process before commit + progress print
WINDOW_BATCH_SIZE = 200


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def build_pfeat():
    conn = get_conn()
    conn.autocommit = False

    cur = conn.cursor()

    print("Truncating pfeat...")
    cur.execute("TRUNCATE pfeat;")
    conn.commit()

    # Read all windows from pwin
    read_cur = conn.cursor()
    read_cur.execute(
        """
        SELECT
            id,
            start_swg_id,
            end_swg_id
        FROM pwin
        ORDER BY id ASC
        """
    )

    insert_cur = conn.cursor()

    total_windows = 0
    while True:
        rows = read_cur.fetchmany(WINDOW_BATCH_SIZE)
        if not rows:
            break

        for pwin_id, start_swg_id, end_swg_id in rows:
            total_windows += 1

            # Aggregate swings inside this window
            agg_sql = """
                SELECT
                    COUNT(*) AS n_swings,
                    SUM(CASE WHEN dir = 1 THEN 1 ELSE 0 END) AS up_count,
                    SUM(CASE WHEN dir = -1 THEN 1 ELSE 0 END) AS dn_count,
                    SUM(ret) AS window_ret,
                    SUM(ret_abs) AS window_ret_abs,
                    SUM(dur_sec) AS window_dur_sec,
                    AVG(imp) AS mean_imp,
                    MAX(imp) AS max_imp,
                    AVG(vol_mean) AS mean_vol,
                    MAX(vol_max) AS max_vol,
                    AVG(lin_r2) AS mean_lin_r2
                FROM swg
                WHERE id BETWEEN %s AND %s
            """
            insert_cur.execute(agg_sql, (start_swg_id, end_swg_id))
            agg_row = insert_cur.fetchone()
            if agg_row is None or agg_row[0] is None or agg_row[0] == 0:
                # no swings in range (should not happen, but be safe)
                continue

            (
                n_swings,
                up_count,
                dn_count,
                window_ret,
                window_ret_abs,
                window_dur_sec,
                mean_imp,
                max_imp,
                mean_vol,
                max_vol,
                mean_lin_r2,
            ) = agg_row

            # First swing in window (for start tick/time)
            insert_cur.execute(
                """
                SELECT start_tick_id, start_ts
                FROM swg
                WHERE id = %s
                """,
                (start_swg_id,),
            )
            row_first = insert_cur.fetchone()
            if not row_first:
                continue
            start_tick_id, start_ts = row_first

            # Last swing in window (for end tick/time)
            insert_cur.execute(
                """
                SELECT end_tick_id, end_ts
                FROM swg
                WHERE id = %s
                """,
                (end_swg_id,),
            )
            row_last = insert_cur.fetchone()
            if not row_last:
                continue
            end_tick_id, end_ts = row_last

            # What happened after this window?
            # Take the very next swing by id after end_swg_id
            insert_cur.execute(
                """
                SELECT id, ret, dir
                FROM swg
                WHERE id > %s
                ORDER BY id ASC
                LIMIT 1
                """,
                (end_swg_id,),
            )
            next_row = insert_cur.fetchone()
            if next_row:
                next_swg_id, next_ret, next_dir = next_row
                if next_ret is None:
                    label_dir = 0
                    label_bigmove = False
                else:
                    if next_ret > 0.0:
                        label_dir = 1
                    elif next_ret < 0.0:
                        label_dir = -1
                    else:
                        label_dir = 0
                    label_bigmove = abs(next_ret) >= BIGMOVE_RET_THRESHOLD
            else:
                next_swg_id = None
                next_ret = None
                next_dir = None
                label_dir = None
                label_bigmove = None

            insert_sql = """
                INSERT INTO pfeat (
                    pwin_id,
                    start_tick_id, end_tick_id,
                    start_ts, end_ts,
                    n_swings, up_count, dn_count,
                    window_ret, window_ret_abs, window_dur_sec,
                    mean_imp, max_imp,
                    mean_vol, max_vol,
                    mean_lin_r2,
                    next_swg_id, next_ret, next_dir,
                    label_dir, label_bigmove
                )
                VALUES (
                    %s,%s,%s,
                    %s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s,
                    %s,%s,
                    %s,
                    %s,%s,%s,
                    %s,%s
                )
            """
            insert_cur.execute(
                insert_sql,
                (
                    pwin_id,
                    start_tick_id,
                    end_tick_id,
                    start_ts,
                    end_ts,
                    n_swings,
                    up_count,
                    dn_count,
                    window_ret,
                    window_ret_abs,
                    window_dur_sec,
                    mean_imp,
                    max_imp,
                    mean_vol,
                    max_vol,
                    mean_lin_r2,
                    next_swg_id,
                    next_ret,
                    next_dir,
                    label_dir,
                    label_bigmove,
                ),
            )

        conn.commit()
        print("Processed %d windows into pfeat so far..." % total_windows)
        sys.stdout.flush()

    read_cur.close()
    insert_cur.close()
    conn.close()

    print("Done. Total pfeat rows inserted: %d" % total_windows)
    sys.stdout.flush()


if __name__ == "__main__":
    try:
        build_pfeat()
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
