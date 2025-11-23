#!/usr/bin/env python3
"""
Fill ticks.kal with a simple 1-D Kalman smoother.

- Processes the table in time order (via id).
- Resets the Kalman state whenever there is a time gap > GapSeconds.
- Logs when each "family" (continuous segment) finishes:
  "Segment N (start_ts=...) finished."

Tune the CONFIG section for your DB and thresholds.
"""

import psycopg2
from psycopg2.extras import execute_batch
from datetime import timedelta

# ---------------- CONFIG ----------------

DbHost = "localhost"
DbPort = 5432
DbName = "trading"
DbUser = "babak"
DbPassword = "babak33044"         # or read from env

SymbolFilter = "XAUUSD"  # set to None to process ALL symbols

BatchSize = 20000        # how many rows to stream at once
GapSeconds = 300         # time gap threshold in seconds (5 min as example)

# Simple, fixed Kalman parameters (tune later if needed)
# These are variances, not std devs.
Q = 0.01   # process noise – how much "true price" can drift between ticks
R = 1.0    # measurement noise – how noisy each tick is

# ----------------------------------------


def main():
    conn = psycopg2.connect(
        host=DbHost,
        port=DbPort,
        dbname=DbName,
        user=DbUser,
        password=DbPassword,
    )
    conn.autocommit = False

    # Server-side cursor to avoid loading 17M rows into memory
    read_cur = conn.cursor(name="kalman_stream")

    if SymbolFilter:
        read_cur.execute(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE symbol = %s
            ORDER BY id
            """,
            (SymbolFilter,),
        )
    else:
        read_cur.execute(
            """
            SELECT id, timestamp, mid
            FROM ticks
            ORDER BY id
            """
        )

    update_cur = conn.cursor()

    total_rows = 0

    # Kalman state for the current "family"
    kal_prev = None
    P_prev = 1.0

    # For gap detection / segment logs
    prev_ts = None
    prev_id = None
    segment_index = 0
    segment_start_ts = None

    gap_delta = timedelta(seconds=GapSeconds)

    while True:
        rows = read_cur.fetchmany(BatchSize)
        if not rows:
            # end of data – close last segment if there was one
            if segment_start_ts is not None:
                segment_index += 1
                print(
                    f"[Segment {segment_index}] finished. "
                    f"start_ts={segment_start_ts}, end_ts={prev_ts}, last_id={prev_id}"
                )
            break

        updates = []

        for tick_id, ts, mid in rows:
            # Determine if this is the start of a new segment
            if prev_ts is None:
                # very first row
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
                # no measurement – keep previous value if we have one
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

            # we still write kal even if it's None; DB will store NULL
            updates.append((kal, tick_id))

            prev_ts = ts
            prev_id = tick_id
            total_rows += 1

        # Batch update for this chunk
        execute_batch(
            update_cur,
            "UPDATE ticks SET kal = %s WHERE id = %s",
            updates,
            page_size=BatchSize,
        )
        conn.commit()

        print(f"Updated {total_rows} rows so far...")

    read_cur.close()
    update_cur.close()
    conn.close()
    print("All done. Total rows updated:", total_rows)


if __name__ == "__main__":
    main()
