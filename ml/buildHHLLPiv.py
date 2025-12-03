#!/usr/bin/env python3
import psycopg2

DBKwargs = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)

def Main():
    Conn = psycopg2.connect(**DBKwargs)
    Cur = Conn.cursor()

    print("Truncating hhll_piv...")
    Cur.execute("TRUNCATE TABLE hhll_piv CASCADE;")
    Conn.commit()

    print("Loading piv_swings ordered by swing_index...")
    Cur.execute("""
        SELECT id, tick_id, ts, mid, ptype, swing_index
        FROM piv_swings
        ORDER BY swing_index
    """)
    Rows = Cur.fetchall()
    print(f"Loaded {len(Rows)} swing pivots.")

    if not Rows:
        print("No swings found, exiting.")
        return

    LastHighMid = None
    LastLowMid = None

    Inserts = []

    for Row in Rows:
        SwingId, TickId, Ts, Mid, PType, SwingIndex = Row
        ClassVal = 0
        ClassText = ""

        if PType == 1:
            # High
            if LastHighMid is None:
                ClassVal = 2
                ClassText = "HH"
            else:
                if Mid > LastHighMid:
                    ClassVal = 2
                    ClassText = "HH"
                else:
                    ClassVal = -1
                    ClassText = "LH"
            LastHighMid = Mid

        else:
            # Low
            if LastLowMid is None:
                ClassVal = -2
                ClassText = "LL"
            else:
                if Mid < LastLowMid:
                    ClassVal = -2
                    ClassText = "LL"
                else:
                    ClassVal = 1
                    ClassText = "HL"
            LastLowMid = Mid

        Inserts.append((SwingId, TickId, Ts, Mid, PType, ClassVal, ClassText))

    print("Inserting into hhll_piv...")
    InsertSql = """
        INSERT INTO hhll_piv
        (swing_id, tick_id, ts, mid, ptype, class, class_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for ChunkStart in range(0, len(Inserts), 1000):
        Chunk = Inserts[ChunkStart:ChunkStart + 1000]
        Cur.executemany(InsertSql, Chunk)
        Conn.commit()
        print(f"Inserted {ChunkStart + len(Chunk)} / {len(Inserts)} hhll pivots...")

    Cur.close()
    Conn.close()
    print("Done building hhll_piv.")

if __name__ == "__main__":
    Main()
