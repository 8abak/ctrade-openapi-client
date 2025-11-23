#!/usr/bin/env python3
"""
kalCalc.py

Fill ticks.kal with a simple 1-D Kalman smoother.

- Processes the table in id order.
- Resets the Kalman state whenever there is a time gap > GapSeconds.
- Logs when each "family" (continuous segment) finishes:
    [Segment N] finished. start_ts=..., end_ts=..., last_id=...

Designed to be gentle on a small t3 instance:
- Streams rows with a server-side cursor (low memory).
- Uses a separate connection for updates so we can commit each batch.
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

# Process only one symbol; set to None to process all symbols
SYMBOL_FILTER = "XAUUSD"   # or None

# How many rows to process per batch (tweak if needed)
BATCH_SIZE = 20000

# Time gap threshold (seconds) to start a new Kalman segment
GAP_SECONDS = 300          # 5 minutes; change if you want different gaps

# Simple, fixed Kalman parameters (tune later if needed)
# These are variances, not standard deviations.
Q = 0.01   # process noise – how much "true price" can drift between ticks
R = 1.0    # measurement noise – how noisy each tick is

# ----------------------------------------


def main():
    # Connection A (READ): autocommit so server-side cursor stays valid
    read_conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    read_conn.autocommit = True

    # Connection B (WRITE): transactional, we commit after each batch
    write_conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    write_conn.autocommit = False

    # Server-side cursor on READ connection
    read_cur = read_conn.cursor(name="kal_stream")

    if SYMBOL_FILTER:
        read_cur.execute(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE symbol = %s
            ORDER BY id
            """,
            (SYMBOL_FILTER,),
        )
    else:
        read_cur.execute(
            """
            SELECT id, timestamp, mid
            FROM ticks
            ORDER BY id
            """
        )

    write_cur = write_conn.cursor()

    # Kalman state for current segment
    kal_prev = None
    P_prev = 1.0

    # For gap detection and logging segments
    prev_ts = None
    prev_id = None
    segment_start_ts = None
    segment_index = 0

    gap_delta = timedelta(seconds=GAP_SECONDS)
    total_rows = 0

    while True:
        rows = read_cur.fetchmany(BATCH_SIZE)
        if not rows:
            # End of data – close last segment if it exists
            if segment_start_ts is not None:
                segment_index += 1
                print(
                    f"[Segment {segment_index}] finished. "
                    f"start_ts={segment_start_ts}, end_ts={prev_ts}, last_id={prev_id}"
                )
            break

        updates = []

        for tick_id, ts, mid in rows:
            # Start or continue segments based on time gaps
            if prev_ts is None:
                # very first row overall
                segment_index += 1
                segment_start_ts = ts
                kal_prev = None
                P_prev = 1.0
            else:
                # check for time gap
                if ts is not None and prev_ts is not None:
                    if ts - prev_ts > gap_delta:
                        # previous segment just finished
                        print(
                            f"[Segment {segment_index}] finished. "
                            f"start_ts={segment_start_ts}, end_ts={prev_ts}, last_id={prev_id}"
                        )
                        # start a new segment
                        segment_index += 1
                        segment_start_ts = ts
                        kal_prev = None
                        P_prev = 1.0

            # --- Kalman calculation for this tick ---
            if mid is None:
                # no measurement – carry forward previous value if any
                kal = kal_prev
                P = P_prev
            else:
                if kal_prev is None:
                    # first point in a segment: initialise state at measurement
                    kal = mid
                    P = 1.0
                else:
                    # Predict
                    kal_prior = kal_prev
                    P_prior = P_prev + Q

                    # Update
                    K = P_prior / (P_prior + R)
                    kal = kal_prior + K * (mid - kal_prior)
                    P = (1.0 - K) * P_prior

                kal_prev = kal
                P_prev = P

            updates.append((kal, tick_id))

            prev_ts = ts
            prev_id = tick_id
            total_rows += 1

        # Batch update via WRITE connection
        execute_batch(
            write_cur,
            "UPDATE ticks SET kal = %s WHERE id = %s",
            updates,
            page_size=BATCH_SIZE,
        )
        write_conn.commit()

        print(f"Updated {total_rows} rows so far...")

    # Clean up
    read_cur.close()
    read_conn.close()
    write_cur.close()
    write_conn.close()

    print("All done. Total rows updated:", total_rows)


if __name__ == "__main__":
    main()
