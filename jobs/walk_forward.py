# jobs/walk_forward.py
import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DEFAULT_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
)

# ---------- DB setup ----------

DDL_PREDICTIONS_SMALL = """
CREATE TABLE IF NOT EXISTS predictions_small (
    id           BIGSERIAL PRIMARY KEY,
    timestamp    TIMESTAMPTZ NOT NULL,
    tickid       BIGINT       NOT NULL,
    direction    SMALLINT     NOT NULL,  -- -1 down, 0 flat, +1 up
    confidence   REAL         NOT NULL DEFAULT 0,
    model_tag    TEXT         NOT NULL DEFAULT 'v0',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_predictions_small_ts   ON predictions_small (timestamp);
CREATE INDEX IF NOT EXISTS idx_predictions_small_tick ON predictions_small (tickid);
"""

DDL_PREDICTIONS_BIG = """
CREATE TABLE IF NOT EXISTS predictions_big (
    id           BIGSERIAL PRIMARY KEY,
    timestamp    TIMESTAMPTZ NOT NULL,
    tickid       BIGINT       NOT NULL,
    direction    SMALLINT     NOT NULL,  -- -1 down, 0 flat, +1 up
    confidence   REAL         NOT NULL DEFAULT 0,
    model_tag    TEXT         NOT NULL DEFAULT 'v0',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_predictions_big_ts   ON predictions_big (timestamp);
CREATE INDEX IF NOT EXISTS idx_predictions_big_tick ON predictions_big (tickid);
"""

def get_engine(db_url: str = DEFAULT_DB_URL) -> Engine:
    return create_engine(db_url, future=True)

def ensure_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL_PREDICTIONS_SMALL)
        conn.exec_driver_sql(DDL_PREDICTIONS_BIG)

# ---------- helpers ----------

def parse_yyyy_mm_dd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")

def day_bounds_utc(day: datetime):
    start = datetime(day.year, day.month, day.day)
    end = start + timedelta(days=1)
    return start, end

# ---------- core steps (stubs to be upgraded with your detectors) ----------

def train_one_day(engine: Engine, day: datetime) -> None:
    """
    Placeholder for training on a single day.
    In our flow this will:
      - pull ticks of `day`
      - (optionally) build features + labels from your trends tables
      - fit/update model(s)
    """
    start, end = day_bounds_utc(day)
    # No-op for now: just log.
    with engine.begin() as conn:
        conn.execute(
            text("SELECT 1")  # keeps the transaction happy; real training happens in code
        )

def predict_next_day(engine: Engine, train_day: datetime, model_tag: str = "v0") -> None:
    """
    Placeholder for inference on the *next* day after `train_day`.
    For now, inserts a dumb baseline (direction=0, confidence=0) so you can
    see the pipeline writing rows and wire up the UI.
    We'll replace this with the real SciPy find_peaks based detectors shortly.
    """
    predict_day = train_day + timedelta(days=1)
    start, end = day_bounds_utc(predict_day)

    # Fetch tick ids & timestamps for the prediction day
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id AS tickid, timestamp
                FROM ticks
                WHERE timestamp >= :start AND timestamp < :end
                ORDER BY timestamp ASC
            """),
            {"start": start, "end": end}
        ).fetchall()

        if not rows:
            print(f"[predict] No ticks found for {predict_day.date()}, skipping.")
            return

        # Trivial “flat” predictions so the pipeline runs end-to-end.
        # Replace this block with your real small/big detectors.
        small_payload = [
            {
                "tickid": r.tickid,
                "timestamp": r.timestamp,
                "direction": 0,
                "confidence": 0.0,
                "model_tag": model_tag,
            }
            for r in rows
        ]
        big_payload = [
            {
                "tickid": r.tickid,
                "timestamp": r.timestamp,
                "direction": 0,
                "confidence": 0.0,
                "model_tag": model_tag,
            }
            for r in rows
        ]

        # Insert in chunks to keep memory/statement size sane
        def batched(iterable, n=2000):
            for i in range(0, len(iterable), n):
                yield iterable[i:i+n]

        for batch in batched(small_payload):
            conn.execute(
                text("""
                    INSERT INTO predictions_small (timestamp, tickid, direction, confidence, model_tag)
                    VALUES (:timestamp, :tickid, :direction, :confidence, :model_tag)
                """),
                batch
            )

        for batch in batched(big_payload):
            conn.execute(
                text("""
                    INSERT INTO predictions_big (timestamp, tickid, direction, confidence, model_tag)
                    VALUES (:timestamp, :tickid, :direction, :confidence, :model_tag)
                """),
                batch
            )

    print(f"[predict] Wrote baseline predictions for {predict_day.date()} "
          f"({len(rows)} ticks) to predictions_small/big.")

# ---------- runner ----------

def run(days: int = 1, start: Optional[str] = None, model_tag: str = "v0") -> None:
    """
    Walk-forward loop:
      Day D: train on D
      Day D+1: write predictions for D+1
    Repeat for `days`.
    """
    if start is None:
        print("ERROR: --start YYYY-MM-DD is required.", file=sys.stderr)
        sys.exit(2)

    start_day = parse_yyyy_mm_dd(start)
    engine = get_engine()
    ensure_tables(engine)

    for i in range(days):
        day_i = start_day + timedelta(days=i)
        print(f"\n=== Walk-forward step {i+1}/{days} | train on {day_i.date()} ===")
        train_one_day(engine, day_i)
        predict_next_day(engine, day_i, model_tag=model_tag)

# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(description="Day-by-day walk-forward trainer/predictor.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (UTC)")
    parser.add_argument("--days", type=int, default=1, help="How many steps to run")
    parser.add_argument("--model-tag", default="v0", help="Tag stored with predictions")

    args = parser.parse_args()
    run(days=args.days, start=args.start, model_tag=args.model_tag)

if __name__ == "__main__":
    main()
