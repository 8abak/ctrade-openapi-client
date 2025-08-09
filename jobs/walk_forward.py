# jobs/walk_forward.py
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from typing import Optional

# ---------- CONFIG ----------
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
)
# ----------------------------

engine = create_engine(DB_URL)


def run(days: int = 1, start: Optional[str] = None, model: str = "both"):
    """
    Run walk-forward predictions for a range of days.
    :param days: Number of days to process
    :param start: Start date as YYYY-MM-DD (string) or None
    :param model: 'small', 'big', or 'both'
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
            ticks = conn.execute(text("""
                SELECT id, timestamp, bid, ask, mid
                FROM ticks
                WHERE timestamp >= :start AND timestamp < :end
                ORDER BY timestamp ASC
            """), {"start": day_start, "end": day_end}).fetchall()

        if not ticks:
            print(f"No ticks found for {day_start}")
            continue

        # Run predictions
        if model in ("small", "both"):
            process_predictions(ticks, "predictions_small")

        if model in ("big", "both"):
            process_predictions(ticks, "predictions_big")


        if __name__ == "__main__":
            parser = argparse.ArgumentParser()
            parser.add_argument("--days", type=int, default=1, help="Number of days to process")
            parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
            parser.add_argument("--model", choices=["both", "bz", "sz"], default="both")
            args = parser.parse_args()


            run(days=args.days, start=args.start, model=args.model)


def process_predictions(ticks, table_name):
    """
    Dummy prediction logic — replace with your actual model call.
    Writes predictions to an existing table.
    """
    print(f"Generating predictions for {len(ticks)} ticks into {table_name}")
    results = []
    for tick in ticks:
        tick_id = tick.id
        pred_value = 1 if tick.mid > 0 else 0  # dummy logic
        results.append((tick_id, pred_value))

    # Insert into the table
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {table_name} (tickid, prediction)
                VALUES (:tickid, :prediction)
            """),
            [{"tickid": tid, "prediction": pred} for tid, pred in results]
        )
    print(f"Inserted {len(results)} rows into {table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Walk-forward prediction runner")
    parser.add_argument("--days", type=int, default=1, help="Number of days to process")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--model", type=str, default="both", choices=["small", "big", "both"], help="Which model to run")
    args = parser.parse_args()

    run(days=args.days, start=args.start, model=args.model)
