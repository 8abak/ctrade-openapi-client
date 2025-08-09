# jobs/walk_forward.py
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOG_FMT = "[%(asctime)s] %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("walk_forward")

# --------- DB helpers ---------

def get_engine() -> Engine:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
    )
    return create_engine(url, future=True)

def ensure_prediction_tables(engine: Engine) -> None:
    """Create predictions_small and predictions_big if they don't exist (Postgres)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS predictions_small (
        id           BIGSERIAL PRIMARY KEY,
        tickid       BIGINT NOT NULL REFERENCES ticks(id) ON DELETE CASCADE,
        timestamp    TIMESTAMPTZ NOT NULL,
        model        TEXT NOT NULL,
        proba_up     DOUBLE PRECISION,
        proba_down   DOUBLE PRECISION,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_small_tickid ON predictions_small(tickid);
    CREATE INDEX IF NOT EXISTS idx_predictions_small_ts ON predictions_small(timestamp);

    CREATE TABLE IF NOT EXISTS predictions_big (
        id           BIGSERIAL PRIMARY KEY,
        tickid       BIGINT NOT NULL REFERENCES ticks(id) ON DELETE CASCADE,
        timestamp    TIMESTAMPTZ NOT NULL,
        model        TEXT NOT NULL,
        proba_up     DOUBLE PRECISION,
        proba_down   DOUBLE PRECISION,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_big_tickid ON predictions_big(tickid);
    CREATE INDEX IF NOT EXISTS idx_predictions_big_ts ON predictions_big(timestamp);
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
    log.info("Ensured predictions_small & predictions_big exist.")

# --------- your ML hooks (stubbed) ---------

def train_for_day(day_start: datetime, day_end: datetime) -> None:
    """
    Plug your training code here (load features for [day_start, day_end), fit model, save).
    """
    log.info("Training on window %s -> %s", day_start.isoformat(), day_end.isoformat())

def predict_for_day(engine: Engine, day_start: datetime, day_end: datetime) -> None:
    """
    Plug your predict code here. Below is just a placeholder that writes one row
    per table so you can verify the job ran end-to-end.
    """
    # Get any tick in window (purely as a demo)
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT id, timestamp
                FROM ticks
                WHERE timestamp >= :start AND timestamp < :end
                ORDER BY timestamp ASC
                LIMIT 1
            """),
            {"start": day_start, "end": day_end},
        ).fetchone()

        if not row:
            log.warning("No ticks found in %s -> %s; skipping write.", day_start, day_end)
            return

        tickid, ts = int(row[0]), row[1]
        payload = {
            "tickid": tickid,
            "timestamp": ts,
            "model": "demo_v1",
            "proba_up": 0.5,
            "proba_down": 0.5,
        }

        conn.execute(
            text("""
                INSERT INTO predictions_small (tickid, timestamp, model, proba_up, proba_down)
                VALUES (:tickid, :timestamp, :model, :proba_up, :proba_down)
                ON CONFLICT (tickid) DO NOTHING
            """),
            payload,
        )
        conn.execute(
            text("""
                INSERT INTO predictions_big (tickid, timestamp, model, proba_up, proba_down)
                VALUES (:tickid, :timestamp, :model, :proba_up, :proba_down)
                ON CONFLICT (tickid) DO NOTHING
            """),
            payload,
        )
    log.info("Wrote demo predictions for %s", ts.isoformat())

# --------- main runner ---------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Walk-forward training/prediction loop (Py3.9)")
    p.add_argument("--start", type=str, default=None,
                   help="Start date (YYYY-MM-DD). Defaults to latest tick day.")
    p.add_argument("--days", type=int, default=1, help="Number of days to run.")
    return p.parse_args()

def resolve_start(engine: Engine, start_str: Optional[str]) -> datetime:
    if start_str:
        return datetime.strptime(start_str, "%Y-%m-%d")
    # Fallback: detect latest tick date
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(timestamp) FROM ticks")).fetchone()
        if row and row[0]:
            dt = row[0]
            return datetime(dt.year, dt.month, dt.day)
    # If no ticks, default to today
    today = datetime.utcnow()
    return datetime(today.year, today.month, today.day)

def run(days: int = 1, start: Optional[str] = None) -> None:
    engine = get_engine()
    ensure_prediction_tables(engine)

    day0 = resolve_start(engine, start)
    for i in range(days):
        day_start = day0 + timedelta(days=i)
        day_end = day_start + timedelta(days=1)

        log.info("=== Day %s (%s -> %s) ===", i + 1, day_start.date(), day_end.date())
        train_for_day(day_start, day_end)
        predict_for_day(engine, day_start, day_end)

def main() -> None:
    args = parse_args()
    run(days=args.days, start=args.start)

if __name__ == "__main__":
    main()
