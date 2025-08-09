# jobs/walk_forward.py
"""
Walk-forward runner (Python 3.9 compatible).

- Iterates over trading days starting from --start for --days
- Produces predictions for the requested model(s)
- Inserts predictions into EXISTING tables (no DDL here)

Usage:
  python -m jobs.walk_forward --start 2025-06-17 --days 1 --model both
"""

import os
import sys
import argparse
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

DEFAULT_MODEL = "both"  # small | big | both
ENV_DB_URL = "DATABASE_URL"

TABLE_SMALL = "predictions_small"
TABLE_BIG = "predictions_big"

# Adjust these to your real column names. These are the columns this
# script will attempt to insert. Make sure they exist in your tables.
# If your schema is different, update both the COLUMNS_* and the
# INSERT SQL below accordingly.
COLUMNS_COMMON = [
    "tickid",        # int
    "timestamp",     # timestamptz or timestamp
    "symbol",        # text (optional; remove if you don't store symbol)
    "side",          # text or smallint, e.g. 'buy'/'sell' or 1/-1
    "score",         # float (model score)
    "prob_buy",      # float
    "prob_sell",     # float
    "meta",          # json/jsonb or text (optional diagnostics)
]

# If small/big tables have different columns, you can split these.
COLUMNS_SMALL = COLUMNS_COMMON
COLUMNS_BIG = COLUMNS_COMMON


# -------------------------------------------------------------------
# DB helpers
# -------------------------------------------------------------------

def get_engine() -> Engine:
    db_url = os.getenv(ENV_DB_URL)
    if not db_url:
        print(f"ERROR: {ENV_DB_URL} is not set", file=sys.stderr)
        sys.exit(2)
    return create_engine(db_url, pool_pre_ping=True)


def chunked(iterable: Iterable[Dict], size: int = 1000) -> Iterable[List[Dict]]:
    buf: List[Dict] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# -------------------------------------------------------------------
# Prediction producers (replace with your real logic)
# -------------------------------------------------------------------

def produce_predictions_small(for_day: date) -> List[Dict]:
    """
    Return a list[dict] where keys match COLUMNS_SMALL.

    IMPORTANT: Replace this stub with your actual small-model scoring.
    """
    # Example dummy row (remove this and implement your pipeline)
    # Return [] if no data for the day.
    return []


def produce_predictions_big(for_day: date) -> List[Dict]:
    """
    Return a list[dict] where keys match COLUMNS_BIG.

    IMPORTANT: Replace this stub with your actual big-model scoring.
    """
    # Example dummy row (remove this and implement your pipeline)
    return []


# -------------------------------------------------------------------
# Insert helpers
# -------------------------------------------------------------------

def insert_rows(engine: Engine, table: str, columns: List[str], rows: List[Dict]) -> int:
    if not rows:
        return 0

    cols_sql = ", ".join(columns)
    params_sql = ", ".join([f":{c}" for c in columns])
    sql = text(f"INSERT INTO {table} ({cols_sql}) VALUES ({params_sql})")

    inserted = 0
    with engine.begin() as conn:
        for batch in chunked(rows, size=2000):
            conn.execute(sql, batch)
            inserted += len(batch)
    return inserted


# -------------------------------------------------------------------
# Core
# -------------------------------------------------------------------

def run(days: int = 1,
        start: Optional[str] = None,
        model: str = DEFAULT_MODEL,
        dry_run: bool = False) -> None:
    """
    Run walk-forward scoring/insertion.

    :param days: number of days to process (>=1)
    :param start: 'YYYY-MM-DD' inclusive start date
    :param model: 'small' | 'big' | 'both'
    :param dry_run: if True, do everything except DB inserts
    """
    if not start:
        print("ERROR: --start is required (YYYY-MM-DD)", file=sys.stderr)
        sys.exit(2)

    try:
        start_day = datetime.strptime(start, "%Y-%m-%d").date()
    except ValueError:
        print("ERROR: --start must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    if days < 1:
        print("ERROR: --days must be >= 1", file=sys.stderr)
        sys.exit(2)

    model = model.lower().strip()
    if model not in ("small", "big", "both"):
        print("ERROR: --model must be one of: small | big | both", file=sys.stderr)
        sys.exit(2)

    engine = get_engine()
    print(f"[walk_forward] start={start_day} days={days} model={model} dry_run={dry_run}")

    day = start_day
    for i in range(days):
        print(f"\n=== Day {i+1}/{days}: {day} ===")

        # SMALL
        if model in ("small", "both"):
            print("  -> Producing small-model predictions…")
            rows_small = produce_predictions_small(day)
            print(f"     small rows: {len(rows_small)}")
            if not dry_run and rows_small:
                inserted_small = insert_rows(engine, TABLE_SMALL, COLUMNS_SMALL, rows_small)
                print(f"     inserted into {TABLE_SMALL}: {inserted_small}")

        # BIG
        if model in ("big", "both"):
            print("  -> Producing big-model predictions…")
            rows_big = produce_predictions_big(day)
            print(f"     big rows: {len(rows_big)}")
            if not dry_run and rows_big:
                inserted_big = insert_rows(engine, TABLE_BIG, COLUMNS_BIG, rows_big)
                print(f"     inserted into {TABLE_BIG}: {inserted_big}")

        day = day + timedelta(days=1)

    print("\n[walk_forward] done.")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Walk-forward runner (no table creation).")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--days", type=int, default=1, help="Number of days to process (default: 1)")
    p.add_argument("--model", choices=["small", "big", "both"], default=DEFAULT_MODEL,
                   help="Which model(s) to run (default: both)")
    p.add_argument("--dry-run", action="store_true", help="Do everything except inserts")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    run(days=args.days, start=args.start, model=args.model, dry_run=args.dry_run)
