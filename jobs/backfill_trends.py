import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from detectors.peak_detector import detect_and_store
from detectors.swing_builder import build_swings

ENGINE = create_engine(...)

def get_range():
    with ENGINE.connect() as conn:
        r = conn.execute(text("SELECT min(timestamp), max(timestamp) FROM ticks")).fetchone()
        return r[0], r[1]

def run_backfill(batch_hours=12):
    a, b = get_range()
    cur = a
    while cur < b:
        nxt = min(cur + timedelta(hours=batch_hours), b)
        print(f"[BACKFILL] peaks {cur} -> {nxt}")
        detect_and_store(cur, nxt)
        cur = nxt

    print("[BACKFILL] build small swings...")
    build_swings(scale=1, min_magnitude=0.0)
    print("[BACKFILL] build big swings...")
    build_swings(scale=2, min_magnitude=3.0)

if __name__ == "__main__":
    run_backfill(batch_hours=12)   # 12h window: safe on memory
