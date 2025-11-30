#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import execute_values

DBName = "trading"
DBUser = "babak"
DBPassword = "babak33044"
DBHost = "localhost"
DBPort = 5432

BatchSize = 5000  # small, safe chunk size

def GetConnection():
    return psycopg2.connect(
        dbname=DBName,
        user=DBUser,
        password=DBPassword,
        host=DBHost,
        port=DBPort,
    )

def Main():
    conn = GetConnection()
    cur = conn.cursor()

    # Current stats
    cur.execute("SELECT COUNT(*), MAX(id) FROM ticks;")
    totalTicks, maxIdTicks = cur.fetchone()

    cur.execute("SELECT COUNT(*) FROM ticks_rebuild;")
    totalRebuild = cur.fetchone()[0]

    print(f"ticks:         count={totalTicks}, max_id={maxIdTicks}")
    print(f"ticks_rebuild: count={totalRebuild}")

    if totalRebuild >= totalTicks:
        print("✅ ticks_rebuild already has all rows (or more). Nothing to do.")
        cur.close()
        conn.close()
        return

    newTicks = totalTicks - totalRebuild
    boundaryId = maxIdTicks - newTicks

    print(f"Missing {newTicks} rows.")
    print(f"Assuming rows with id > {boundaryId} in ticks are not yet in ticks_rebuild.")

    lastId = boundaryId
    copied = 0

    while True:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask,
                   kal, mid, spread,
                   kal_fast, kal_slow,
                   kal_fast_resid, kal_slow_resid
            FROM ticks
            WHERE id > %s
            ORDER BY id
            LIMIT %s;
            """,
            (lastId, BatchSize),
        )
        rows = cur.fetchall()
        if not rows:
            break

        lastId = rows[-1][0]

        values = [
            (
                r[1],  # symbol
                r[2],  # timestamp
                r[3],  # bid
                r[4],  # ask
                r[5],  # kal
                r[6],  # mid
                r[7],  # spread
                r[8],  # kal_fast
                r[9],  # kal_slow
                r[10], # kal_fast_resid
                r[11], # kal_slow_resid
            )
            for r in rows
        ]

        execute_values(
            cur,
            """
            INSERT INTO ticks_rebuild
                (symbol, timestamp, bid, ask,
                 kal, mid, spread,
                 kal_fast, kal_slow,
                 kal_fast_resid, kal_slow_resid)
            VALUES %s;
            """,
            values,
        )
        conn.commit()

        copied += len(rows)
        print(f"Copied {copied}/{newTicks} missing rows so far (up to old id {lastId})...")

    print("✅ Finished appending missing rows to ticks_rebuild.")

    # Final sanity
    cur.execute("SELECT COUNT(*) FROM ticks_rebuild;")
    newTotal = cur.fetchone()[0]
    print(f"ticks_rebuild final count = {newTotal}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    Main()
