#!/usr/bin/env python3
"""
build_kalseg.py

Create monotone Kalman segments and store them in kalseg.

- Reads ticks in ID order, using a server-side cursor (streaming).
- Respects time gaps: breaks segments if gap > GAP_SECONDS.
- Splits segments when the Kalman direction (up/down) truly flips,
  ignoring tiny wiggles smaller than KAL_EPS.
- Inserts one row per segment into kalseg.
"""

import psycopg2
from psycopg2.extras import execute_batch
from datetime import timedelta

# --------------- CONFIG -----------------

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"
DB_HOST = "localhost"
DB_PORT = 5432

SYMBOL_FILTER = "XAUUSD"   # or None to segment all symbols

BATCH_SIZE = 20000         # rows fetched from ticks each time
SEGMENT_BATCH_SIZE = 5000  # how many segments to insert per batch

GAP_SECONDS = 300          # break segment if time gap > this (5 min)
KAL_EPS = 0.02             # ignore direction changes smaller than this kal delta

# ----------------------------------------


def create_kalseg_table():
    """Create kalseg table if needed and truncate it."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kalseg (
            id                BIGSERIAL PRIMARY KEY,
            symbol            TEXT NOT NULL,
            start_id          BIGINT NOT NULL,
            end_id            BIGINT NOT NULL,
            direction         SMALLINT NOT NULL,      -- -1 = down, 0 = flat, 1 = up
            start_ts          TIMESTAMPTZ NOT NULL,
            end_ts            TIMESTAMPTZ NOT NULL,
            start_kal         DOUBLE PRECISION NOT NULL,
            end_kal           DOUBLE PRECISION NOT NULL,
            kal_delta         DOUBLE PRECISION NOT NULL,
            tick_count        INTEGER NOT NULL,
            duration_seconds  DOUBLE PRECISION NOT NULL,
            avg_speed         DOUBLE PRECISION NOT NULL
        );
        """
    )

    # wipe old data
    cur.execute("TRUNCATE TABLE kalseg;")

    conn.commit()
    cur.close()
    conn.close()


def main():
    create_kalseg_table()

    # ---------- READ CONNECTION ----------
    read_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    read_conn.autocommit = False
    read_cur = read_conn.cursor(name="kalseg_stream")

    if SYMBOL_FILTER:
        read_cur.execute(
            """
            SELECT id, symbol, timestamp, kal
            FROM ticks
            WHERE kal IS NOT NULL
              AND symbol = %s
            ORDER BY id
            """,
            (SYMBOL_FILTER,),
        )
    else:
        read_cur.execute(
            """
            SELECT id, symbol, timestamp, kal
            FROM ticks
            WHERE kal IS NOT NULL
            ORDER BY id
            """
        )

    # ---------- WRITE CONNECTION ----------
    write_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    write_conn.autocommit = False
    write_cur = write_conn.cursor()

    gap_delta = timedelta(seconds=GAP_SECONDS)

    # current segment state
    seg_symbol = None
    seg_start_id = None
    seg_start_ts = None
    seg_start_kal = None
    seg_last_id = None
    seg_last_ts = None
    seg_last_kal = None
    seg_direction = None     # -1, 0, 1, or None
    seg_tick_count = 0

    prev_ts = None
    prev_kal = None
    prev_id = None

    total_ticks = 0
    segments_buffer = []
    total_segments = 0

    def close_segment():
        nonlocal seg_start_id, seg_start_ts, seg_start_kal
        nonlocal seg_last_id, seg_last_ts, seg_last_kal
        nonlocal seg_direction, seg_tick_count
        nonlocal segments_buffer, total_segments, write_cur, write_conn

        if seg_start_id is None or seg_last_id is None:
            return

        kal_delta = seg_last_kal - seg_start_kal
        if seg_last_ts and seg_start_ts:
            dur = (seg_last_ts - seg_start_ts).total_seconds()
        else:
            dur = 0.0
        if dur <= 0:
            avg_speed = 0.0
        else:
            avg_speed = kal_delta / dur

        # store segment in buffer
        segments_buffer.append(
            (
                seg_symbol,
                seg_start_id,
                seg_last_id,
                int(seg_direction if seg_direction is not None else 0),
                seg_start_ts,
                seg_last_ts,
                seg_start_kal,
                seg_last_kal,
                kal_delta,
                seg_tick_count,
                dur,
                avg_speed,
            )
        )
        total_segments += 1

        # flush buffer if big enough
        if len(segments_buffer) >= SEGMENT_BATCH_SIZE:
            execute_batch(
                write_cur,
                """
                INSERT INTO kalseg (
                    symbol, start_id, end_id, direction,
                    start_ts, end_ts,
                    start_kal, end_kal, kal_delta,
                    tick_count, duration_seconds, avg_speed
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                segments_buffer,
                page_size=SEGMENT_BATCH_SIZE,
            )
            write_conn.commit()
            segments_buffer = []
            print(f"Inserted {total_segments} segments so far...")

        # reset current segment
        seg_start_id = None
        seg_start_ts = None
        seg_start_kal = None
        seg_last_id = None
        seg_last_ts = None
        seg_last_kal = None
        seg_direction = None
        seg_tick_count = 0

    while True:
        rows = read_cur.fetchmany(BATCH_SIZE)
        if not rows:
            # end; close last segment
            close_segment()
            break

        for tick_id, symbol, ts, kal in rows:
            total_ticks += 1

            if prev_ts is None:
                # first tick overall -> start new segment
                seg_symbol = symbol
                seg_start_id = tick_id
                seg_start_ts = ts
                seg_start_kal = kal
                seg_last_id = tick_id
                seg_last_ts = ts
                seg_last_kal = kal
                seg_direction = 0
                seg_tick_count = 1

                prev_ts = ts
                prev_kal = kal
                prev_id = tick_id
                continue

            # ---- check time gap ----
            if ts - prev_ts > gap_delta or symbol != seg_symbol:
                # close previous segment at prev tick
                seg_last_id = prev_id
                seg_last_ts = prev_ts
                seg_last_kal = prev_kal
                close_segment()

                # start new segment at current tick
                seg_symbol = symbol
                seg_start_id = tick_id
                seg_start_ts = ts
                seg_start_kal = kal
                seg_last_id = tick_id
                seg_last_ts = ts
                seg_last_kal = kal
                seg_direction = 0
                seg_tick_count = 1

                prev_ts = ts
                prev_kal = kal
                prev_id = tick_id
                continue

            # ---- direction based on kal ----
            delta_kal = kal - prev_kal
            if abs(delta_kal) < KAL_EPS:
                step_dir = 0
            else:
                step_dir = 1 if delta_kal > 0 else -1

            if seg_direction in (None, 0):
                # segment direction not yet firmly set
                if step_dir != 0:
                    seg_direction = step_dir
            else:
                # if strong reversal, close segment and start new at pivot
                if step_dir != 0 and step_dir != seg_direction:
                    # close current segment at prev tick (pivot point)
                    seg_last_id = prev_id
                    seg_last_ts = prev_ts
                    seg_last_kal = prev_kal
                    close_segment()

                    # start new segment at pivot
                    seg_symbol = symbol
                    seg_start_id = prev_id
                    seg_start_ts = prev_ts
                    seg_start_kal = prev_kal
                    seg_last_id = tick_id
                    seg_last_ts = ts
                    seg_last_kal = kal
                    seg_direction = step_dir
                    seg_tick_count = 2  # pivot + current

                    prev_ts = ts
                    prev_kal = kal
                    prev_id = tick_id
                    continue

            # extend current segment
            seg_last_id = tick_id
            seg_last_ts = ts
            seg_last_kal = kal
            seg_tick_count += 1

            prev_ts = ts
            prev_kal = kal
            prev_id = tick_id

        if total_ticks % 200000 == 0:
            print(f"Processed {total_ticks} ticks so far...")

    # flush remaining segments in buffer
    if segments_buffer:
        execute_batch(
            write_cur,
            """
            INSERT INTO kalseg (
                symbol, start_id, end_id, direction,
                start_ts, end_ts,
                start_kal, end_kal, kal_delta,
                tick_count, duration_seconds, avg_speed
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            segments_buffer,
            page_size=SEGMENT_BATCH_SIZE,
        )
        write_conn.commit()

    # cleanup
    write_cur.close()
    write_conn.close()

    read_cur.close()
    read_conn.commit()
    read_conn.close()

    print(f"Done. Total ticks processed: {total_ticks}, total segments: {total_segments}")


if __name__ == "__main__":
    main()