#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import execute_values

DBName = "trading"
DBUser = "babak"
DBPassword = "babak33044"
DBHost = "localhost"
DBPort = 5432

BatchSize = 10000  # adjust if you like

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

    print("Dropping ticks_rebuild if exists...")
    cur.execute("DROP TABLE IF EXISTS ticks_rebuild;")
    conn.commit()

    print("Creating ticks_rebuild...")
    # Adjust types if your schema is slightly different
    cur.execute(
        """
        CREATE TABLE ticks_rebuild (
            id               bigserial PRIMARY KEY,
            symbol           text,
            timestamp        timestamptz,
            bid              double precision,
            ask              double precision,
            kal              double precision,
            mid              double precision,
            spread           double precision,
            kal_fast         double precision,
            kal_slow         double precision,
            kal_fast_resid   double precision,
            kal_slow_resid   double precision
        );
        """
    )
    conn.commit()

    # Find the smallest id to start
    cur.execute("SELECT MIN(id), MAX(id) FROM ticks;")
    row = cur.fetchone()
    if row is None or row[0] is None:
        print("No rows in ticks; nothing to do.")
        cur.close()
        conn.close()
        return

    minId, maxId = row
    print(f"Rebuilding IDs from {minId} to {maxId}...")

    lastId = minId - 1
    totalCopied = 0

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

        totalCopied += len(rows)
        print(f"Copied {totalCopied} rows so far (up to old id {lastId})...")

    print("Finished copying. Swapping tables...")

    cur.execute("ALTER TABLE ticks RENAME TO ticks_old;")
    cur.execute("ALTER TABLE ticks_rebuild RENAME TO ticks;")
    conn.commit()

    print("✅ Rebuild complete.")
    print("Old table kept as 'ticks_old'. Drop it manually when you are happy.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    Main()
