import psycopg2
import numpy as np
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

DB_CONFIG = {
    'dbname': 'trading',
    'user': 'babak',
    'password': 'babak33044',
    'host': 'localhost',
    'port': '5432'
}

def get_tick_by_id(tickid):
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM ticks WHERE id = %s", (tickid,))
            return cur.fetchone()

def get_ticks_around(tickid, before=250, after=250):
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM ticks
                WHERE id BETWEEN %s AND %s
                ORDER BY id ASC
            """, (tickid - before, tickid + after))
            return cur.fetchall()

def get_zigzag_context(tickid, count=7):
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM zigzag_predictions
                WHERE end_tick_id BETWEEN %s AND %s
                ORDER BY end_tick_id ASC
            """, (tickid - 100, tickid + 100))
            zigs = cur.fetchall()
            index = next((i for i, z in enumerate(zigs) if z['end_tick_id'] == tickid), None)
            if index is not None:
                context = zigs[max(0, index-count):index+count+1]
                return [f"{z['label']}:id{z['end_tick_id']}" for z in context]
            return []

def atr(ticks, period):
    if len(ticks) < period:
        return 0.0
    mids = [tick['mid'] for tick in ticks[-period:]]
    diffs = [abs(mids[i] - mids[i-1]) for i in range(1, len(mids))]
    return np.mean(diffs)

def stddev(ticks):
    mids = [tick['mid'] for tick in ticks]
    return np.std(mids)

def slope(ticks):
    if len(ticks) < 2:
        return 0.0
    x = np.arange(len(ticks))
    y = np.array([tick['mid'] for tick in ticks])
    a, _ = np.polyfit(x, y, 1)
    return float(a)

def insert_training_record(tickid, label, tick_window, zigzag_context, atr140, atr15, std_before, std_after, slope_val, tod):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO zigzag_training_data
                (tickid, label, tick_window, zigzag_context, atr140, atr15, stddev_before, stddev_after, slope, time_of_day)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (tickid, label, tick_window, zigzag_context, atr140, atr15, std_before, std_after, slope_val, tod))
            print(f"✅ Training record added for tick {tickid}")

def process_zig(zig):
    tickid = zig['end_tick_id']
    label = 1 if zig['label'] == 'bz' else 0
    ticks = get_ticks_around(tickid, before=250, after=250)
    tick_window = [tick['mid'] for tick in ticks]
    atr140_val = atr(ticks, 140)
    atr15_val = atr(ticks, 15)
    std_before = stddev(ticks[:200])
    std_after = stddev(ticks[-200:])
    slope_val = slope(ticks)
    tod = get_tick_by_id(tickid)['timestamp'].hour + get_tick_by_id(tickid)['timestamp'].minute / 60
    context = get_zigzag_context(tickid, count=7)

    insert_training_record(
        tickid, label, tick_window, context,
        atr140_val, atr15_val, std_before, std_after, slope_val, tod
    )

if __name__ == '__main__':
    # Example usage
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM zigzag_predictions ORDER BY end_tick_id ASC LIMIT 1")
            row = cur.fetchone()
            if row:
                process_zig(row)
            else:
                print("No zigzag prediction entries yet.")
