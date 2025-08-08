# jobs/walk_forward.py
import os
import argparse
from datetime import datetime, timedelta, timezone, date
from sqlalchemy import create_engine, text

from detectors.peak_detector import detect_and_store
from detectors.swing_builder import build_swings

ENGINE = create_engine(os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"))

def _day_bounds(d: date):
    a = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    b = a + timedelta(days=1)
    return a, b

def _first_day():
    with ENGINE.connect() as conn:
        return conn.execute(text("SELECT date(min(timestamp)) FROM ticks")).scalar()

def _last_day():
    with ENGINE.connect() as conn:
        return conn.execute(text("SELECT date(max(timestamp)) FROM ticks")).scalar()

def process_day(d: date):
    a, b = _day_bounds(d)
    print(f"[WALK] Detect peaks {a} → {b}")
    s_small, s_big = detect_and_store(a, b)

    print(f"[WALK] Build small swings...")
    n_small = build_swings(scale=1, min_magnitude=0.0)
    print(f"[WALK] Build big swings (>=3.0)...")
    n_big   = build_swings(scale=2, min_magnitude=3.0)

    with ENGINE.begin() as conn:
        conn.execute(text("""
          INSERT INTO daily_runs (run_date, status, small_swings, big_swings, note)
          VALUES (:d, 'done', :s, :b, :note)
          ON CONFLICT (run_date) DO UPDATE
          SET status='done', small_swings=:s, big_swings=:b, note=:note
        """), {"d": d.isoformat(), "s": n_small, "b": n_big,
               "note": f"peaks small={s_small}, big={s_big}"})
    print(f"[WALK] {d} small={n_small}, big={n_big}")

def run(days: int = 1, start: str | None = None):
    if start:
        cur = date.fromisoformat(start)
    else:
        cur = _first_day()
    if cur is None:
        raise SystemExit("No ticks found.")

    end = cur + timedelta(days=days-1)
    last = _last_day()
    if last and end > last:
        end = last

    while cur <= end:
        process_day(cur)
        cur += timedelta(days=1)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="YYYY-MM-DD (UTC). If omitted, earliest ticks date.")
    p.add_argument("--days", type=int, default=1)
    args = p.parse_args()
    run(days=args.days, start=args.start)
