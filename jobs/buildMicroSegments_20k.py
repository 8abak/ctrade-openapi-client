#!/usr/bin/env python3
import psycopg2
from statistics import pstdev

DBName = "trading"
DBUser = "babak"
DBPassword = "babak33044"
DBHost = "localhost"
DBPort = 5432

MaxTickId = 20000  # only first 20k for now

def GetConnection():
    return psycopg2.connect(
        dbname=DBName,
        user=DBUser,
        password=DBPassword,
        host=DBHost,
        port=DBPort,
    )

def SignWithEps(x, eps=1e-6):
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0

def Main():
    conn = GetConnection()
    cur = conn.cursor()

    print("Cleaning old micro segments for id <= 20000...")
    cur.execute(
        """
        DELETE FROM micro_feat
        WHERE seg_id IN (
            SELECT id FROM micro_seg
            WHERE start_tick_id <= %s
        );
        """,
        (MaxTickId,),
    )
    cur.execute(
        """
        DELETE FROM micro_seg
        WHERE start_tick_id <= %s;
        """,
        (MaxTickId,),
    )
    conn.commit()

    print("Loading first 20k ticks...")
    cur.execute(
        """
        SELECT id, timestamp, mid, spread, kal_fast_resid
        FROM ticks
        WHERE id <= %s
        ORDER BY id;
        """,
        (MaxTickId,),
    )
    rows = cur.fetchall()
    if not rows:
        print("No ticks found up to id 20000.")
        cur.close()
        conn.close()
        return

    print(f"Loaded {len(rows)} ticks. Building micro segments...")

    segments = []
    seg_start_idx = 0
    seg_sign = SignWithEps(rows[0][4])

    for i in range(1, len(rows)):
        resid = rows[i][4]
        this_sign = SignWithEps(resid)

        # close segment when sign actually flips (ignore 0 noise)
        if this_sign != seg_sign and this_sign != 0 and seg_sign != 0:
            segments.append((seg_start_idx, i - 1, seg_sign))
            seg_start_idx = i
            seg_sign = this_sign

    # last segment
    segments.append((seg_start_idx, len(rows) - 1, seg_sign))

    print(f"Identified {len(segments)} micro segments.")

    for seg_start_idx, seg_end_idx, seg_sign in segments:
        seg_rows = rows[seg_start_idx : seg_end_idx + 1]

        first = seg_rows[0]
        last  = seg_rows[-1]

        start_tick_id = first[0]
        end_tick_id   = last[0]
        start_time    = first[1]
        end_time      = last[1]
        start_mid     = first[2]
        end_mid       = last[2]

        mids    = [r[2] for r in seg_rows]
        spreads = [r[3] for r in seg_rows]
        resids  = [r[4] for r in seg_rows]

        high_mid = max(mids)
        low_mid  = min(mids)
        tick_count = len(seg_rows)

        delta = end_mid - start_mid
        if   delta > 0: direction = 1
        elif delta < 0: direction = -1
        else:           direction = 0

        # Insert micro_seg
        cur.execute(
            """
            INSERT INTO micro_seg
                (start_tick_id, end_tick_id,
                 start_time, end_time,
                 start_mid, end_mid,
                 high_mid, low_mid,
                 direction, tick_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id;
            """,
            (
                start_tick_id,
                end_tick_id,
                start_time,
                end_time,
                start_mid,
                end_mid,
                high_mid,
                low_mid,
                direction,
                tick_count,
            ),
        )
        seg_id = cur.fetchone()[0]

        duration_sec = (end_time - start_time).total_seconds()
        avg_spread = sum(spreads) / tick_count
        max_spread = max(spreads)
        avg_resid  = sum(resids) / tick_count
        max_resid  = max(resids, key=abs)  # max abs residual
        std_resid  = pstdev(resids) if tick_count > 1 else 0.0

        # Insert micro_feat
        cur.execute(
            """
            INSERT INTO micro_feat
                (seg_id,
                 duration_sec,
                 avg_spread, max_spread,
                 avg_kal_fast_resid,
                 max_kal_fast_resid,
                 std_kal_fast_resid)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
            """,
            (
                seg_id,
                duration_sec,
                avg_spread,
                max_spread,
                avg_resid,
                max_resid,
                std_resid,
            ),
        )

    conn.commit()
    print("✅ Micro segments and features created for first 20k ticks.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    Main()
