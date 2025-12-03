#!/usr/bin/env python3
import psycopg2
from decimal import Decimal

DBKwargs = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)

def Main():
    Conn = psycopg2.connect(**DBKwargs)
    Cur = Conn.cursor()

    print("Truncating zones_hhll...")
    Cur.execute("TRUNCATE TABLE zones_hhll CASCADE;")
    Conn.commit()

    print("Loading HH/LL pivots...")
    Cur.execute("""
        SELECT id, tick_id, ts, mid, ptype, class, class_text
        FROM hhll_piv
        WHERE class_text IN ('HH','LL')
        ORDER BY ts, id
    """)
    Pivots = Cur.fetchall()
    print(f"Loaded {len(Pivots)} HH/LL pivots.")

    if len(Pivots) < 2:
        print("Not enough pivots to build zones.")
        return

    LastHHPivot = None
    LastLLPivot = None
    Zones = []

    for Pivot in Pivots:
        PivId, TickId, Ts, Mid, PType, ClassVal, ClassText = Pivot

        if ClassText == "HH":
            LastHHPivot = Pivot
        elif ClassText == "LL":
            LastLLPivot = Pivot

        if LastHHPivot is not None and LastLLPivot is not None:
            # Build a zone whenever we have both
            HId, HTickId, HTs, HMid, _, _, _ = LastHHPivot
            LId, LTickId, LTs, LMid, _, _, _ = LastLLPivot

            TopPrice = max(HMid, LMid)
            BotPrice = min(HMid, LMid)

            # Zone starts from later of the two pivot times
            if HTs >= LTs:
                StartTime = HTs
                StartTick = HTickId
            else:
                StartTime = LTs
                StartTick = LTickId

            # For this first pass we just end zone at the next pivot time
            EndTime = Ts
            EndTick = TickId

            if EndTick <= StartTick:
                continue  # ignore degenerate zones

            NTicks = EndTick - StartTick + 1

            Zones.append((
                StartTick,
                EndTick,
                StartTime,
                EndTime,
                TopPrice,
                BotPrice,
                HId,
                LId,
                NTicks,
                None,          # break_dir
                None,          # break_tick_id
                None,          # break_time
                "finished",    # state
                None,          # activate_time
                None,          # invalidate_time
                None           # invalidate_tick
            ))

    print(f"Built {len(Zones)} basic zones. Inserting into zones_hhll...")

    InsertSql = """
        INSERT INTO zones_hhll (
            start_tick_id, end_tick_id,
            start_time, end_time,
            top_price, bot_price,
            top_pivot_id, bot_pivot_id,
            n_ticks,
            break_dir, break_tick_id, break_time,
            state, activate_time, invalidate_time, invalidate_tick
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    Cur = Conn.cursor()
    for ChunkStart in range(0, len(Zones), 500):
        Chunk = Zones[ChunkStart:ChunkStart + 500]
        Cur.executemany(InsertSql, Chunk)
        Conn.commit()
        print(f"Inserted {ChunkStart + len(Chunk)} / {len(Zones)} zones...")

    Cur.close()
    Conn.close()
    print("Done building zones_hhll.")

if __name__ == "__main__":
    Main()
