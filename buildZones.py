#!/usr/bin/env python3
"""
buildZones.py

Create higher-level zones by fusing neighbouring Kalman segments (kalseg)
according to Option B:

- Same direction  -> always fused.
- Direction flip  -> fused only if BOTH segments are small magnitude
                     (|kal_delta| < SMALL_MOVE).
- Large time gaps -> always start a new zone.

Writes results into the "zones" table.
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

SYMBOL_FILTER = "XAUUSD"     # or None to use all symbols

SEG_BATCH_SIZE = 20000       # how many kalseg rows to fetch each time
ZONE_BATCH_SIZE = 2000       # how many zones to insert per batch

GAP_SECONDS_ZONE = 600       # break zone if gap between segments > this (10 min)

SMALL_MOVE = 1.0             # |kal_delta| < 1    -> small segment
MEDIUM_MOVE = 3.0            # 1 <= |kal_delta| < 3 -> medium
# >= MEDIUM_MOVE             -> big

# ----------------------------------------


def mag_zone(abs_move: float) -> int:
    """
    Classify move magnitude into 1/2/3.
    1: small   (< SMALL_MOVE)
    2: medium  (< MEDIUM_MOVE)
    3: big     (>= MEDIUM_MOVE)
    """
    if abs_move < SMALL_MOVE:
        return 1
    elif abs_move < MEDIUM_MOVE:
        return 2
    else:
        return 3


def sign_with_zero(x: float, eps: float = 1e-9) -> int:
    if x > eps:
        return 1
    elif x < -eps:
        return -1
    else:
        return 0


def main():
    # ---------- READ CONNECTION (kalseg) ----------
    read_conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    read_conn.autocommit = False
    read_cur = read_conn.cursor(name="zones_stream")

    if SYMBOL_FILTER:
        read_cur.execute(
            """
            SELECT id, symbol, start_id, end_id, direction,
                   start_ts, end_ts, kal_delta, duration_seconds
            FROM kalseg
            WHERE symbol = %s
            ORDER BY start_id
            """,
            (SYMBOL_FILTER,),
        )
    else:
        read_cur.execute(
            """
            SELECT id, symbol, start_id, end_id, direction,
                   start_ts, end_ts, kal_delta, duration_seconds
            FROM kalseg
            ORDER BY start_id
            """
        )

    # ---------- WRITE CONNECTION (zones) ----------
    write_conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    write_conn.autocommit = False
    write_cur = write_conn.cursor()

    gap_delta = timedelta(seconds=GAP_SECONDS_ZONE)

    # ----- current zone state -----
    zone_symbol = None
    zone_start_seg_id = None
    zone_end_seg_id = None
    zone_start_id = None
    zone_end_id = None
    zone_start_ts = None
    zone_end_ts = None

    zone_seg_count = 0
    zone_total_kal_delta = 0.0
    zone_total_duration = 0.0
    zone_max_abs_kal_delta = 0.0
    zone_up_seg_count = 0
    zone_dn_seg_count = 0
    zone_flip_count = 0
    zone_small_seg_count = 0
    zone_medium_seg_count = 0
    zone_big_seg_count = 0

    # last segment info inside current zone
    last_seg_direction = None
    last_seg_mag_zone = None
    last_seg_end_ts = None

    zones_buffer = []
    total_zones = 0
    total_segments_read = 0

    def close_zone():
        nonlocal zone_symbol, zone_start_seg_id, zone_end_seg_id
        nonlocal zone_start_id, zone_end_id, zone_start_ts, zone_end_ts
        nonlocal zone_seg_count, zone_total_kal_delta, zone_total_duration
        nonlocal zone_max_abs_kal_delta, zone_up_seg_count, zone_dn_seg_count
        nonlocal zone_flip_count, zone_small_seg_count, zone_medium_seg_count
        nonlocal zone_big_seg_count, zones_buffer, total_zones

        if zone_seg_count == 0 or zone_start_seg_id is None:
            return

        zone_dir = sign_with_zero(zone_total_kal_delta)

        zones_buffer.append(
            (
                zone_symbol,
                zone_start_seg_id,
                zone_end_seg_id,
                zone_start_id,
                zone_end_id,
                zone_start_ts,
                zone_end_ts,
                zone_seg_count,
                zone_dir,
                zone_total_kal_delta,
                zone_total_duration,
                zone_max_abs_kal_delta,
                zone_up_seg_count,
                zone_dn_seg_count,
                zone_flip_count,
                zone_small_seg_count,
                zone_medium_seg_count,
                zone_big_seg_count,
            )
        )
        total_zones += 1

        if len(zones_buffer) >= ZONE_BATCH_SIZE:
            execute_batch(
                write_cur,
                """
                INSERT INTO zones (
                    symbol,
                    start_seg_id, end_seg_id,
                    start_id, end_id,
                    start_ts, end_ts,
                    seg_count, direction,
                    total_kal_delta, total_duration_seconds,
                    max_abs_kal_delta,
                    up_seg_count, dn_seg_count, flip_count,
                    small_seg_count, medium_seg_count, big_seg_count
                )
                VALUES (
                    %s,%s,%s,
                    %s,%s,
                    %s,%s,
                    %s,%s,
                    %s,%s,
                    %s,
                    %s,%s,%s,
                    %s,%s,%s
                )
                """,
                zones_buffer,
                page_size=ZONE_BATCH_SIZE,
            )
            write_conn.commit()
            zones_buffer.clear()
            print(f"Inserted {total_zones} zones so far...")

        # reset zone state
        reset_zone_state()

    def reset_zone_state():
        nonlocal zone_symbol, zone_start_seg_id, zone_end_seg_id
        nonlocal zone_start_id, zone_end_id, zone_start_ts, zone_end_ts
        nonlocal zone_seg_count, zone_total_kal_delta, zone_total_duration
        nonlocal zone_max_abs_kal_delta, zone_up_seg_count, zone_dn_seg_count
        nonlocal zone_flip_count, zone_small_seg_count, zone_medium_seg_count
        nonlocal zone_big_seg_count, last_seg_direction, last_seg_mag_zone
        nonlocal last_seg_end_ts

        zone_symbol = None
        zone_start_seg_id = None
        zone_end_seg_id = None
        zone_start_id = None
        zone_end_id = None
        zone_start_ts = None
        zone_end_ts = None

        zone_seg_count = 0
        zone_total_kal_delta = 0.0
        zone_total_duration = 0.0
        zone_max_abs_kal_delta = 0.0
        zone_up_seg_count = 0
        zone_dn_seg_count = 0
        zone_flip_count = 0
        zone_small_seg_count = 0
        zone_medium_seg_count = 0
        zone_big_seg_count = 0

        last_seg_direction = None
        last_seg_mag_zone = None
        last_seg_end_ts = None

    reset_zone_state()

    # ------------- main streaming loop -------------
    while True:
        rows = read_cur.fetchmany(SEG_BATCH_SIZE)
        if not rows:
            close_zone()
            break

        for seg_id, symbol, start_id, end_id, direction, start_ts, end_ts, kal_delta, duration_seconds in rows:
            total_segments_read += 1

            abs_move = abs(kal_delta)
            mz = mag_zone(abs_move)

            # time gap or symbol change always breaks the zone
            if (
                zone_seg_count > 0
                and (
                    symbol != zone_symbol
                    or (last_seg_end_ts is not None and start_ts - last_seg_end_ts > gap_delta)
                )
            ):
                close_zone()

            if zone_seg_count == 0:
                # start a new zone with this segment
                zone_symbol = symbol
                zone_start_seg_id = seg_id
                zone_end_seg_id = seg_id
                zone_start_id = start_id
                zone_end_id = end_id
                zone_start_ts = start_ts
                zone_end_ts = end_ts

                zone_seg_count = 1
                zone_total_kal_delta = kal_delta
                zone_total_duration = duration_seconds
                zone_max_abs_kal_delta = abs_move

                if direction == 1:
                    zone_up_seg_count = 1
                    zone_dn_seg_count = 0
                elif direction == -1:
                    zone_up_seg_count = 0
                    zone_dn_seg_count = 1
                else:
                    zone_up_seg_count = 0
                    zone_dn_seg_count = 0

                if mz == 1:
                    zone_small_seg_count = 1
                elif mz == 2:
                    zone_medium_seg_count = 1
                else:
                    zone_big_seg_count = 1

                zone_flip_count = 0
                last_seg_direction = direction
                last_seg_mag_zone = mz
                last_seg_end_ts = end_ts
                continue

            # decide whether to fuse or start new zone
            start_new_zone = False

            # direction or small flip logic
            if direction == 0 or last_seg_direction == 0:
                # flat segments never break zone by themselves
                pass
            else:
                if direction == last_seg_direction:
                    # same direction -> always fuse
                    pass
                else:
                    # direction flipped
                    if last_seg_mag_zone == 1 and mz == 1:
                        # both small -> choppy, stay in zone but count flip
                        zone_flip_count += 1
                    else:
                        # real opposite move -> start new zone
                        start_new_zone = True

            if start_new_zone:
                # close current zone, start new one with this segment
                close_zone()

                zone_symbol = symbol
                zone_start_seg_id = seg_id
                zone_end_seg_id = seg_id
                zone_start_id = start_id
                zone_end_id = end_id
                zone_start_ts = start_ts
                zone_end_ts = end_ts

                zone_seg_count = 1
                zone_total_kal_delta = kal_delta
                zone_total_duration = duration_seconds
                zone_max_abs_kal_delta = abs_move

                zone_up_seg_count = 1 if direction == 1 else 0
                zone_dn_seg_count = 1 if direction == -1 else 0
                zone_flip_count = 0

                zone_small_seg_count = 1 if mz == 1 else 0
                zone_medium_seg_count = 1 if mz == 2 else 0
                zone_big_seg_count = 1 if mz == 3 else 0

                last_seg_direction = direction
                last_seg_mag_zone = mz
                last_seg_end_ts = end_ts
                continue

            # ----- fuse this segment into current zone -----
            zone_end_seg_id = seg_id
            zone_end_id = end_id
            zone_end_ts = end_ts

            zone_seg_count += 1
            zone_total_kal_delta += kal_delta
            zone_total_duration += duration_seconds
            if abs_move > zone_max_abs_kal_delta:
                zone_max_abs_kal_delta = abs_move

            if direction == 1:
                zone_up_seg_count += 1
            elif direction == -1:
                zone_dn_seg_count += 1

            if mz == 1:
                zone_small_seg_count += 1
            elif mz == 2:
                zone_medium_seg_count += 1
            else:
                zone_big_seg_count += 1

            last_seg_direction = direction
            last_seg_mag_zone = mz
            last_seg_end_ts = end_ts

        if total_segments_read % 50000 == 0:
            print(f"Processed {total_segments_read} kalseg rows so far...")

    # flush remaining zones
    if zones_buffer:
        execute_batch(
            write_cur,
            """
            INSERT INTO zones (
                symbol,
                start_seg_id, end_seg_id,
                start_id, end_id,
                start_ts, end_ts,
                seg_count, direction,
                total_kal_delta, total_duration_seconds,
                max_abs_kal_delta,
                up_seg_count, dn_seg_count, flip_count,
                small_seg_count, medium_seg_count, big_seg_count
            )
            VALUES (
                %s,%s,%s,
                %s,%s,
                %s,%s,
                %s,%s,
                %s,%s,
                %s,
                %s,%s,%s,
                %s,%s,%s
            )
            """,
            zones_buffer,
            page_size=ZONE_BATCH_SIZE,
        )
        write_conn.commit()

    write_cur.close()
    write_conn.close()

    read_cur.close()
    read_conn.commit()
    read_conn.close()

    print(f"Done. Total kalseg rows processed: {total_segments_read}, total zones: {total_zones}")


if __name__ == "__main__":
    main()