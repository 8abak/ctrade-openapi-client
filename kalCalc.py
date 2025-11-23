#!/usr/bin/env python3
"""
kalCalc.py

Fill ticks.kal with a simple 1-D Kalman smoother.

- Streams through ticks in ID order.
- Resets Kalman whenever a time gap > GAP_SECONDS is detected.
- Logs each segment end.
- Uses two DB connections:
    * read_conn : holds one long transaction for the named cursor
    * write_conn: commits each batch safely
"""

import psycopg2
from psycopg2.extras import execute_batch
from datetime import timedelta

# ---------------- CONFIG ----------------

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"
DB_HOST = "localhost"
DB_PORT = 5432

SYMBOL_FILTER = "XAUUSD"    # or None
BATCH_SIZE = 20000
GAP_SECONDS = 300           # 5 minutes

Q = 0.01
R = 1.0

# ----------------------------------------


def main():

    # --------- READ CONNECTION (transaction open for whole job) ----------
    read_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    read_conn.autocommit = False     # IMPORTANT
    read_cur = read_conn.cursor(name="kal_stream")

    if SYMBOL_FILTER:
        read_cur.execute(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE symbol = %s
            ORDER BY id
            """,
            (SYMBOL_FILTER,)
        )
    else:
        read_cur.execute(
            "SELECT id, timestamp, mid FROM ticks ORDER BY id"
        )

    # --------- WRITE CONNECTION (free commits) ----------
    write_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    write_conn.autocommit = False
    write_cur = write_conn.cursor()

    kal_prev = None
    P_prev = 1.0

    prev_ts = None
    prev_id = None
    segment_start_ts = None
    segment_index = 0

    gap_delta = timedelta(seconds=GAP_SECONDS)

    total_rows = 0

    while True:
        rows = read_cur.fetchmany(BATCH_SIZE)
        if not rows:
            if segment_start_ts is not None:
                segment_index += 1
                print(f"[Segment {segment_index}] finished. "
                      f"start_ts={segment_start_ts}, end_ts={prev_ts}, last_id={prev_id}")
            break

        updates = []

        for tick_id, ts, mid in rows:

            # -------- Gap detection / segment boundaries --------
            if prev_ts is None:
                segment_index += 1
                segment_start_ts = ts
                kal_prev = None
                P_prev = 1.0
            else:
                if ts - prev_ts > gap_delta:
                    print(f"[Segment {segment_index}] finished. "
                          f"start_ts={segment_start_ts}, end_ts={prev_ts}, last_id={prev_id}")
                    segment_index += 1
                    segment_start_ts = ts
                    kal_prev = None
                    P_prev = 1.0

            # -------- Kalman filter -------
            if mid is None:
                kal = kal_prev
                P = P_prev
            else:
                if kal_prev is None:
                    kal = mid
                    P = 1.0
                else:
                    kal_prior = kal_prev
                    P_prior = P_prev + Q

                    K = P_prior / (P_prior + R)
                    kal = kal_prior + K * (mid - kal_prior)
                    P = (1 - K) * P_prior

                kal_prev = kal
                P_prev = P

            updates.append((kal, tick_id))

            prev_ts = ts
            prev_id = tick_id
            total_rows += 1

        # -------- Write batch --------
        execute_batch(
            write_cur,
            "UPDATE ticks SET kal = %s WHERE id = %s",
            updates,
            page_size=BATCH_SIZE,
        )
        write_conn.commit()

        print(f"Updated {total_rows} rows so far...")

    # -------- Cleanup --------
    write_cur.close()
    write_conn.close()

    read_cur.close()
    read_conn.commit()   # end the long cursor transaction
    read_conn.close()

    print("All done. Total rows updated:", total_rows)


if __name__ == "__main__":
    main()
