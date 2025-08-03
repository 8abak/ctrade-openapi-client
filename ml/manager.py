import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from . import gatherer, trainer


DB_CONFIG = {
    'dbname': 'trading',
    'user': 'babak',
    'password': 'babak33044',
    'host': 'localhost',
    'port': '5432'
}

ZIG_THRESHOLD_SZ = 0.5
ZIG_THRESHOLD_BZ = 3.0

def get_last_tick():
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(MAX(end_tick_id), 0) AS last_tick_id FROM zigzag_predictions;
            """)
            return cur.fetchone()['last_tick_id']

def get_next_ticks(start_id, limit=1000):
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, mid FROM ticks
                WHERE id > %s ORDER BY id ASC LIMIT %s;
            """, (start_id, limit))
            return cur.fetchall()

def distance(p1, p2):
    return abs(p1 - p2)

def store_zig(start, end, label):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO zigzag_predictions
                (start_tick_id, end_tick_id, start_time, end_time, up_price, dn_price, label)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                start['id'], end['id'],
                start['timestamp'], end['timestamp'],
                max(start['mid'], end['mid']),
                min(start['mid'], end['mid']),
                label
            ))
            print(f"📌 Added {label.upper()} from {start['id']} to {end['id']}, Δ={distance(start['mid'], end['mid']):.2f}")

def find_next_confirmed_zig(ticks, threshold):
    if not ticks:
        return None

    base = ticks[0]
    extremum = base
    direction = None

    for i in range(1, len(ticks)):
        tick = ticks[i]
        delta = tick['mid'] - base['mid']

        if direction is None:
            direction = 'up' if delta > 0 else 'down'
            extremum = tick
        elif (direction == 'up' and tick['mid'] > extremum['mid']) or (direction == 'down' and tick['mid'] < extremum['mid']):
            extremum = tick
        elif (direction == 'up' and tick['mid'] < extremum['mid'] - threshold) or \
             (direction == 'down' and tick['mid'] > extremum['mid'] + threshold):
            return base, extremum

    return None

def next_zig():
    last_id = get_last_tick()
    buffer = []
    current_id = last_id

    while True:
        new_ticks = get_next_ticks(current_id, limit=1000)
        if not new_ticks:
            print("No more ticks to process.")
            return None

        buffer.extend(new_ticks)
        sz = find_next_confirmed_zig(buffer, ZIG_THRESHOLD_SZ)
        if sz:
            start, end = sz
            store_zig(start, end, 'sz')
            return {
                'label': 'sz',
                'end_tick_id': end['id'],
                'timestamp': str(end['timestamp'])
            }

        bz = find_next_confirmed_zig(buffer, ZIG_THRESHOLD_BZ)
        if bz:
            start, end = bz
            store_zig(start, end, 'bz')
            return {
                'label': 'bz',
                'end_tick_id': end['id'],
                'timestamp': str(end['timestamp'])
            }

        current_id = new_ticks[-1]['id']

class Manager:
    def __init__(self, mode='bootstrap', limit=30):
        self.mode = mode  # 'bootstrap' or 'predict'
        self.limit = limit  # how many BZs to process initially
        self.sz_buffer = []  # stores recent SZs for context
        self.bz_counter = 0

    def run(self):
        print(f"📌 Starting manager in mode: {self.mode}, target BZs: {self.limit}")
        while self.bz_counter < self.limit:
            if self.mode == 'bootstrap':
                self.handle_bootstrap_cycle()
            time.sleep(1)
        print("✅ Finished bootstrap run.")

    def handle_bootstrap_cycle(self):
        zig = next_zig()
        if not zig:
            print("⏳ No new zigzag found. Waiting...")
            return

        if zig['label'] == 'sz':
            self.sz_buffer.append(zig)
            print(f"📥 Collected SZ tick {zig['end_tick_id']}")
        elif zig['label'] == 'bz':
            print(f"🔥 BZ tick {zig['end_tick_id']} confirmed. Triggering data gatherer + trainer.")
            self.sz_buffer.append(zig)
            gatherer.process_zig(zig)
            trainer.train()
            self.sz_buffer = []  # reset after training
            self.bz_counter += 1

if __name__ == '__main__':
    mgr = Manager(mode='bootstrap', limit=30)
    mgr.run()
