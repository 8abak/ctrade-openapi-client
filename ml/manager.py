import time
from . import Crawler as crawler
from . import Gatherer as gatherer
from . import Trainer as trainer

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
        # Try to get the next zig from the crawler
        zig = crawler.next_zig()
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
