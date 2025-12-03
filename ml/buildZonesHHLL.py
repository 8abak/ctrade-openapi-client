#!/usr/bin/env python3
import psycopg2
from decimal import Decimal

DBKwargs = dict(
    dbname="trading",
    user="babak",
    password="babak33044",
    host="localhost",
    port=5432,
)

def Main():
    Conn = psycopg2.connect(**DBKwargs)
    Cur = Conn.cursor()

    print("Truncating zones_hhll...")
    Cur.execute("TRUNCATE TABLE zones_hhll CASCADE;")
    Conn.commit()

    print("Loading HH/LL pivots from hhll_piv...")
    Cur.execute(
        """
        SELECT id, tick_id, ts, mid, ptype, class, class_text
        FROM hhll_piv
        WHERE class_text IN ('HH','LL')
        ORDER BY ts, id
        """
    )
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
            # New higher high pivot
            LastHHPivot = Pivot
            if LastLLPivot is not None:
                # Build a zone between this HH and the latest LL
                Zones.append(BuildZone(LastHHPivot, LastLLPivot))
        elif ClassText == "LL":
            # New lower low pivot
            LastLLPivot = Pivot
            if LastHHPivot is not None:
                # Build a zone between this LL and the latest HH
                Zones.append(BuildZone(LastHHPivot, LastLLPivot))

    # Filter out any degenerate zones (same start/end tick)
    Zones = [z for z in Zones if z[0] < z[1]]

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

    ChunkSize = 500
    for Start in range(0, len(Zones), ChunkSize):
        Chunk = Zones[Start:Start + ChunkSize]
        Cur.executemany(InsertSql, Chunk)
        Conn.commit()
        print(f"Inserted {Start + len(Chunk)} / {len(Zones)} zones...")

    Cur.close()
    Conn.close()
    print("Done building zones_hhll.")


def BuildZone(HHPivot, LLPivot):
    """
    Build a basic zone rectangle from one HH pivot and one LL pivot.
    Returns a tuple matching the INSERT columns:
      (start_tick_id, end_tick_id,
       start_time, end_time,
       top_price, bot_price,
       top_pivot_id, bot_pivot_id,
       n_ticks,
       break_dir, break_tick_id, break_time,
       state, activate_time, invalidate_time, invalidate_tick)
    """
    HId, HTickId, HTs, HMid, _, _, _ = HHPivot
    LId, LTickId, LTs, LMid, _, _, _ = LLPivot

    # Horizontal span: from earlier pivot to later pivot
    if HTs <= LTs:
        StartTime = HTs
        StartTick = HTickId
        EndTime = LTs
        EndTick = LTickId
    else:
        StartTime = LTs
        StartTick = LTickId
        EndTime = HTs
        EndTick = HTickId

    TopPrice = max(HMid, LMid)
    BotPrice = min(HMid, LMid)

    NTicks = max(1, EndTick - StartTick + 1)

    # For now we don’t model breakout direction or life-cycle here
    BreakDir = None
    BreakTickId = None
    BreakTime = None

    State = "finished"     # will refine later to forming/active/invalidated
    ActivateTime = None
    InvalidateTime = None
    InvalidateTick = None

    return (
        StartTick,
        EndTick,
        StartTime,
        EndTime,
        TopPrice,
        BotPrice,
        HId,
        LId,
        NTicks,
        BreakDir,
        BreakTickId,
        BreakTime,
        State,
        ActivateTime,
        InvalidateTime,
        InvalidateTick,
    )


if __name__ == "__main__":
    Main()
