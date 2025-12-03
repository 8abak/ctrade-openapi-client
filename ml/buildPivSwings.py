#!/usr/bin/env python3
import psycopg2

DBKwargs = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)

def Main():
    Conn = psycopg2.connect(**DBKwargs)
    Cur = Conn.cursor()

    print("Truncating piv_swings...")
    Cur.execute("TRUNCATE TABLE piv_swings CASCADE;")
    Conn.commit()

    print("Loading piv_hilo ordered by time...")
    Cur.execute("""
        SELECT id, tick_id, ts, mid, ptype
        FROM piv_hilo
        ORDER BY ts, id
    """)
    Rows = Cur.fetchall()
    print(f"Loaded {len(Rows)} raw pivots.")

    if not Rows:
        print("No pivots found, exiting.")
        return

    Swings = []   # list of (pivot_id, tick_id, ts, mid, ptype, swing_index)

    CurrentExtreme = Rows[0]
    SwingIndex = 1

    for Row in Rows[1:]:
        PivotId, TickId, Ts, Mid, PType = Row
        CurId, CurTickId, CurTs, CurMid, CurPType = CurrentExtreme

        if PType == CurPType:
            # Same type: keep the more extreme one
            if PType == 1:   # highs -> keep highest
                if Mid >= CurMid:
                    CurrentExtreme = Row
            else:            # lows -> keep lowest
                if Mid <= CurMid:
                    CurrentExtreme = Row
        else:
            # Type flipped: finalize current extreme as a swing pivot
            CurId, CurTickId, CurTs, CurMid, CurPType = CurrentExtreme
            Swings.append((CurId, CurTickId, CurTs, CurMid, CurPType, SwingIndex))
            SwingIndex += 1
            CurrentExtreme = Row

    # Final one
    CurId, CurTickId, CurTs, CurMid, CurPType = CurrentExtreme
    Swings.append((CurId, CurTickId, CurTs, CurMid, CurPType, SwingIndex))

    print(f"Built {len(Swings)} swing pivots. Inserting into piv_swings...")

    InsertSql = """
        INSERT INTO piv_swings (pivot_id, tick_id, ts, mid, ptype, swing_index)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    for ChunkStart in range(0, len(Swings), 1000):
        Chunk = Swings[ChunkStart:ChunkStart + 1000]
        Cur.executemany(InsertSql, Chunk)
        Conn.commit()
        print(f"Inserted {ChunkStart + len(Chunk)} / {len(Swings)} swings...")

    Cur.close()
    Conn.close()
    print("Done building piv_swings.")

if __name__ == "__main__":
    Main()
