# jobs/walk_forward.py
import argparse
import os
from datetime import datetime, timedelta
from typing import Optional, Sequence
from sqlalchemy import text

from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
# ----------------------------
engine = create_engine(DB_URL)


def process_predictions(ticks, table_name: str, model_name: str) -> None:
    print(f"Generating predictions for {len(ticks)} ticks into {table_name}")

    payload = []
    for t in ticks:
        pred = 1 if (t.mid or 0) >= 0 else 0
        payload.append({
            "tickid": t.id,
            "timestamp": t.timestamp,
            "model": model_name,
            "proba_up": None,       # or your probability value
            "proba_down": None,     # or your probability value
            "prediction": pred,
        })

    sql = f"""
        INSERT INTO {table_name}
            (tickid, timestamp, model, proba_up, proba_down, prediction)
        VALUES
            (:tickid, :timestamp, :model, :proba_up, :proba_down, :prediction)
        ON CONFLICT (tickid) DO UPDATE
        SET timestamp = EXCLUDED.timestamp,
            model = EXCLUDED.model,
            proba_up = EXCLUDED.proba_up,
            proba_down = EXCLUDED.proba_down,
            prediction = EXCLUDED.prediction
    """

    with engine.begin() as conn:
        conn.execute(text(sql), payload)

    print(f"Upserted {len(payload)} rows into {table_name}")


def run(days: int = 1, start: Optional[str] = None, model: str = "both") -> None:
    """
    Run walk-forward predictions for a range of days.

    :param days: Number of days to process
    :param start: Start date as YYYY-MM-DD (string) or None
    :param model: 'sz' (small), 'bz' (big), or 'both'
    """
    if start is None:
        start_date = datetime.utcnow().date()
    else:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()

    for day_offset in range(days):
        day_start = start_date + timedelta(days=day_offset)
        day_end = day_start + timedelta(days=1)

        print(f"Processing {day_start} to {day_end} for model={model}")

        # Get ticks for this day
        with engine.connect() as conn:
            ticks = conn.execute(
                text(
                    """
                    SELECT id, timestamp, bid, ask, mid
                    FROM ticks
                    WHERE timestamp >= :start AND timestamp < :end
                    ORDER BY timestamp ASC
                    """
                ),
                {"start": day_start, "end": day_end},
            ).fetchall()

        if not ticks:
            print(f"No ticks found for {day_start}")
            continue

        # Run predictions
        if model in ("sz", "both"):
            process_predictions(ticks, "predictions_small", "sz")
        if model in ("bz", "both"):
            process_predictions(ticks, "predictions_big", "bz")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Walk-forward prediction runner")
    parser.add_argument("--days", type=int, default=1, help="Number of days to process")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument(
        "--model",
        type=str,
        default="both",
        choices=["sz", "bz", "both"],  # sz = small zig, bz = big zig
        help="Which model(s) to run",
    )
    args = parser.parse_args()
    run(days=args.days, start=args.start, model=args.model)
