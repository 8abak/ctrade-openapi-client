# workers/trend_worker.py
import os, time
from datetime import datetime, timedelta, timezone
from detectors.peak_detector import detect_and_store
from detectors.swing_builder import build_swings

def loop():
    while True:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=2)  # short window
        try:
            detect_and_store(start, end)
            build_swings(scale=1, min_magnitude=0.0)
            build_swings(scale=2, min_magnitude=3.0)
        except Exception as e:
            print("[TREND_WORKER] error:", e, flush=True)
        time.sleep(5)  # tune as needed

if __name__ == "__main__":
    loop()
