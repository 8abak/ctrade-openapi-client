import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from ml import gatherer, trainer

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


def get_last_pivot_tick():
    with psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tickid, timestamp, mid FROM zigzag_pivots
                ORDER BY tickid DESC LIMIT 1;
            """)
            return cur.fetchone()


def store_zig(tick, level, direction):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO zigzag_pivots (tickid, timestamp, mid, level, direction, price)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (tick['id'], tick['timestamp'], tick['mid'], level, direction, tick['mid']))
            print(f"📌 Stored {level.upper()} at tick {tick['id']} with direction {direction}, price={tick['mid']}")


class Manager:
    def __init__(self, mode='bootstrap', limit=31):
        self.mode = mode
        self.limit = limit
        self.bz_counter = 0

        # Determine start tick
        last_pivot = get_last_pivot_tick()
        if last_pivot:
            self.prev_tick = get_tick(last_pivot['tickid'])
            self.last_bz_tick = self.prev_tick
            self.extreme_tick = self.prev_tick
            print(f"🔁 Resuming from tick {self.prev_tick['id']}")
        else:
            self.prev_tick = get_tick(1)
            self.last_bz_tick = self.prev_tick
            self.extreme_tick = self.prev_tick
            store_zig(self.prev_tick, 'sz', 'up')
            store_zig(self.prev_tick, 'bz', 'up')
            self.bz_counter = 1
            print(f"🆕 Starting from tick 1")

        self.trend = None

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

        next_price = next_tick['mid']
        extreme_price = self.extreme_tick['mid']

        if self.trend == 'up':
            if next_price > extreme_price:
                self.extreme_tick = next_tick
            elif extreme_price - next_price >= ZIG_THRESHOLD_SZ:
                delta = self.extreme_tick['mid'] - self.last_bz_tick['mid']
                level = 'bz' if delta >= ZIG_THRESHOLD_BZ else 'sz'
                store_zig(self.extreme_tick, level, 'up')
                if level == 'bz':
                    self.last_bz_tick = self.extreme_tick
                    gatherer.process_zig({
                        'label': 'bz',
                        'end_tick_id': self.extreme_tick['id'],
                        'timestamp': str(self.extreme_tick['timestamp'])
                    })
                    trainer.train()
                    self.bz_counter += 1
                self.prev_tick = self.extreme_tick
                self.extreme_tick = next_tick
                self.trend = 'dn'

        elif self.trend == 'dn':
            if next_price < extreme_price:
                self.extreme_tick = next_tick
            elif next_price - extreme_price >= ZIG_THRESHOLD_SZ:
                delta = self.last_bz_tick['mid'] - self.extreme_tick['mid']
                level = 'bz' if delta >= ZIG_THRESHOLD_BZ else 'sz'
                store_zig(self.extreme_tick, level, 'dn')
                if level == 'bz':
                    self.last_bz_tick = self.extreme_tick
                    gatherer.process_zig({
                        'label': 'bz',
                        'tick_id': self.extreme_tick['id'],
                        'timestamp': str(self.extreme_tick['timestamp'])
                    })
                    trainer.train()
                    self.bz_counter += 1
                self.prev_tick = self.extreme_tick
                self.extreme_tick = next_tick
                self.trend = 'up'

        else:
            if next_price > self.prev_tick['mid']:
                self.trend = 'up'
                self.extreme_tick = next_tick
            elif next_price < self.prev_tick['mid']:
                self.trend = 'dn'
                self.extreme_tick = next_tick

        self.prev_tick = next_tick


if __name__ == '__main__':
    mgr = Manager(mode='bootstrap', limit=31)
    mgr.run()
