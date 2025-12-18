#!/usr/bin/env python3
"""
buildSegticks.py

Populate segticks in a SAFE, CHUNKED way.
Also creates the FIRST segLine for each segm.

This script is:
- low memory
- resumable
- deterministic
"""

import psycopg2
from psycopg2.extras import execute_batch
from math import copysign

CHUNK_SIZE = 10_000

DSN = "dbname=trading user=babak password=babak33044 host=localhost"

def linear_y(x, x1, y1, x2, y2):
    if x2 == x1:
        return y1
    return y1 + (x - x1) * (y2 - y1) / (x2 - x1)

def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    cur = conn.cursor()

    # 1. Find segms that need segticks
    cur.execute("""
        SELECT s.id, s.start_tick_id, s.end_tick_id
        FROM segms s
        LEFT JOIN seglines l ON l.segm_id = s.id
        WHERE l.id IS NULL
        ORDER BY s.id
    """)
    segms = cur.fetchall()

    print(f"[buildSegticks] segms to process: {len(segms)}")

    for segm_id, start_tid, end_tid in segms:
        print(f"\n[segm {segm_id}] building root segLine")

        # 2. Get endpoints
        cur.execute("""
            SELECT tick_id, kal
            FROM ticks
            WHERE tick_id IN (%s, %s)
            ORDER BY tick_id
        """, (start_tid, end_tid))
        rows = cur.fetchall()

        if len(rows) != 2:
            print(f"  !! missing endpoint ticks, skipping segm {segm_id}")
            conn.rollback()
            continue

        (t1, y1), (t2, y2) = rows

        # 3. Insert root segLine
        cur.execute("""
            INSERT INTO seglines
                (segm_id, depth, iteration,
                 start_tick_id, end_tick_id,
                 start_price, end_price)
            VALUES (%s, 0, 0, %s, %s, %s, %s)
            RETURNING id
        """, (segm_id, t1, t2, y1, y2))
        segline_id = cur.fetchone()[0]

        conn.commit()
        print(f"  segLine {segline_id} created")

        # 4. Stream ticks in chunks
        last_tid = start_tid

        while True:
            cur.execute("""
                SELECT tick_id, kal
                FROM ticks
                WHERE tick_id >= %s
                  AND tick_id <= %s
                ORDER BY tick_id
                LIMIT %s
            """, (last_tid, end_tid, CHUNK_SIZE))

            ticks = cur.fetchall()
            if not ticks:
                break

            rows_to_insert = []

            for tick_id, kal in ticks:
                y_hat = linear_y(tick_id, t1, y1, t2, y2)
                dist = kal - y_hat

                rows_to_insert.append((
                    segm_id,
                    tick_id,
                    segline_id,
                    dist
                ))

            execute_batch(cur, """
                INSERT INTO segticks
                    (segm_id, tick_id, segline_id, dist)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tick_id) DO NOTHING
            """, rows_to_insert, page_size=1000)

            conn.commit()

            last_tid = ticks[-1][0] + 1
            print(f"    ticks up to {last_tid}")

        print(f"[segm {segm_id}] done")

    cur.close()
    conn.close()
    print("\n[buildSegticks] all done")

if __name__ == "__main__":
    main()
