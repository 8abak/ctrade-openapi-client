import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from cTrader.ml import gatherer, trainer

DB_CONFIG = {
    'dbname': 'trading',
    'user': 'babak',
    'password': 'babak33044',
    'host': 'localhost',
    'port': '5432'
}

ZIG_THRESHOLD_SZ = 0.5
ZIG_THRESHOLD_BZ = 3.0

def get_tick(start_id):
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, mid FROM ticks
                WHERE id = %s
            """, (start_id,))
            return cur.fetchone()

def get_next_tick(current_id):
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, mid FROM ticks
                WHERE id > %s ORDER BY id ASC LIMIT 1;
            """, (current_id,))
            return cur.fetchone()

def store_zig(tick, level, direction):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO zigzag_pivots (tickid, timestamp, mid, level, direction)
                VALUES (%s, %s, %s, %s, %s)
            """, (tick['id'], tick['timestamp'], tick['mid'], level, direction))
            print(f"📌 Stored {level.upper()} at tick {tick['id']} with direction {direction}, price={tick['mid']}")

class Manager:
    def __init__(self, mode='bootstrap', limit=30):
        self.mode = mode
        self.limit = limit
        self.bz_counter = 0
        self.prev_tick = get_tick(1)
        store_zig(self.prev_tick, 'sz', 'up')
        store_zig(self.prev_tick, 'bz', 'up')

    def run(self):
        print(f"📌 Starting manager in mode: {self.mode}, target BZs: {self.limit}")
        while self.bz_counter < self.limit:
            self.handle_cycle()
            time.sleep(0.5)
        print("✅ Finished bootstrap run.")

    def handle_cycle(self):
        next_tick = get_next_tick(self.prev_tick['id'])
        if not next_tick:
            print("⏳ No more ticks available.")
            return

        delta = next_tick['mid'] - self.prev_tick['mid']
        direction = 'up' if delta > 0 else 'dn'
        level = None
        if abs(delta) >= ZIG_THRESHOLD_BZ:
            level = 'bz'
        elif abs(delta) >= ZIG_THRESHOLD_SZ:
            level = 'sz'

        if level:
            store_zig(next_tick, level, direction)
            if level == 'bz':
                print(f"🔥 BZ tick {next_tick['id']} confirmed. Triggering data gatherer + trainer.")
                gatherer.process_zig({
                    'label': 'bz',
                    'tick_id': next_tick['id'],
                    'timestamp': str(next_tick['timestamp'])
                })
                trainer.train()
                self.bz_counter += 1

        self.prev_tick = next_tick

if __name__ == '__main__':
    mgr = Manager(mode='bootstrap', limit=30)
    mgr.run()
