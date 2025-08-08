# jobs/walk_forward.py
# Python 3.9 compatible walk-forward day-by-day runner.
#
# Calibrates thresholds from day D (small/big), then applies SciPy peak/trough
# detection on day D+1 to produce trend states (-1, 0, +1) and writes them to:
#   predictions_small, predictions_big
#
# Environment:
#   DATABASE_URL (same as backend/main.py), or default to local trading DB.

from __future__ import annotations

import os
import argparse
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from sqlalchemy import create_engine, text


# ------------------------------
# DB
# ------------------------------
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
engine = create_engine(DB_URL)


CREATE_PRED_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    tickid      BIGINT PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    state       SMALLINT NOT NULL,             -- -1 down, 0 flat, +1 up
    day         DATE NOT NULL,
    source      TEXT NOT NULL DEFAULT 'walk_forward',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO {table} (tickid, timestamp, state, day, source)
VALUES (:tickid, :timestamp, :state, :day, :source)
ON CONFLICT (tickid) DO UPDATE SET
  state = EXCLUDED.state,
  day = EXCLUDED.day,
  source = EXCLUDED.source,
  created_at = NOW();
"""

# ------------------------------
# Utilities
# ------------------------------
def day_bounds(d: date) -> Tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def fetch_ticks(start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    """
    returns DataFrame: ['id','timestamp','mid'] between [start_ts, end_ts)
    """
    with engine.connect() as conn:
        q = text(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE timestamp >= :s AND timestamp < :e
            ORDER BY timestamp ASC
            """
        )
        df = pd.read_sql(q, conn, params={"s": start_ts, "e": end_ts})
    return df


def ensure_tables(small_table: str, big_table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(CREATE_PRED_TABLE_SQL.format(table=small_table)))
        conn.execute(text(CREATE_PRED_TABLE_SQL.format(table=big_table)))


def ema(series: pd.Series, span: int = 20) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def detect_trend_states(df: pd.DataFrame,
                        small_prom: float,
                        big_prom: float,
                        smooth_span: int = 20) -> Dict[str, np.ndarray]:
    """
    Given ticks df(id,timestamp,mid), return per-tick trend state arrays:
      - small_state: -1/0/+1 via smaller moves
      - big_state:   -1/0/+1 via bigger ($) moves
    Approach:
      1) smooth mid with EMA
      2) find peaks & troughs with different prominence thresholds
      3) turn alternating extrema into segments; each segment labeled by sign of slope
         and magnitude threshold (green/up=+1, yellow/flat=0, red/down=-1)
    """
    if df.empty:
        return {"small": np.array([]), "big": np.array([])}

    px = df["mid"].astype(float).values
    sm = ema(pd.Series(px), span=smooth_span).values

    # Helper to compute states for a given prominence
    def states_for_prom(prom_usd: float) -> np.ndarray:
        # Peaks (highs)
        peak_idx, _ = find_peaks(sm, prominence=prom_usd)
        # Troughs (invert to find lows)
        trough_idx, _ = find_peaks(-sm, prominence=prom_usd)

        all_extrema = np.sort(np.concatenate([peak_idx, trough_idx]))
        if all_extrema.size == 0:
            return np.zeros_like(sm, dtype=np.int8)

        # Build segments between alternating extrema; label each segment by slope & magnitude
        state = np.zeros_like(sm, dtype=np.int8)

        prev = all_extrema[0]
        prev_val = sm[prev]
        for i in range(1, len(all_extrema)):
            cur = all_extrema[i]
            cur_val = sm[cur]
            slope = cur_val - prev_val

            # magnitude condition: require abs move >= prom_usd to consider it trending
            if abs(slope) >= prom_usd:
                seg_state = 1 if slope > 0 else -1
            else:
                seg_state = 0

            state[prev:cur] = seg_state
            prev, prev_val = cur, cur_val

        # After last extrema -> extend its state to the end
        state[prev:] = state[prev - 1] if prev > 0 else 0

        return state

    small_state = states_for_prom(small_prom)
    big_state = states_for_prom(big_prom)

    return {"small": small_state, "big": big_state}


def calibrate_from_day(df: pd.DataFrame) -> Tuple[float, float]:
    """
    Calibrate noise-based small/big prominence from a single day.
    We use intraday absolute dollar returns to set:
       small_prom = max(0.20, 1.5 * median_abs_return)
       big_prom   = max(3.00, 3.0 * median_abs_return)
    This keeps 'big' at least ~$3 as requested and small above noise.
    """
    if len(df) < 3:
        # fall back to defaults
        return 0.20, 3.00

    r = np.abs(np.diff(df["mid"].astype(float).values))
    med = float(np.median(r)) if r.size else 0.05
    small_prom = max(0.20, 1.5 * med)
    big_prom = max(3.00, 3.0 * med)
    return small_prom, big_prom


def upsert_predictions(table: str, day_d1: date, df: pd.DataFrame, states: np.ndarray, source: str) -> int:
    """
    Write (tickid, timestamp, state, day, source) for D+1 rows into table.
    Returns number of rows written.
    """
    if df.empty or states.size == 0:
        return 0

    payload = []
    for tickid, ts, st in zip(df["id"].values, df["timestamp"].values, states.astype(int)):
        payload.append({
            "tickid": int(tickid),
            "timestamp": pd.Timestamp(ts).to_pydatetime(),
            "state": int(st),
            "day": day_d1,
            "source": source
        })

    with engine.begin() as conn:
        for chunk_start in range(0, len(payload), 1000):
            chunk = payload[chunk_start:chunk_start + 1000]
            conn.execute(text(UPSERT_SQL.format(table=table)), chunk)

    return len(payload)


# ------------------------------
# Walk-forward loop
# ------------------------------
def first_tick_day() -> Optional[date]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MIN(date(timestamp)) FROM ticks")).fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


def last_tick_day() -> Optional[date]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(date(timestamp)) FROM ticks")).fetchone()
        if not row or row[0] is None:
            return None
        return row[0]


def process_day(d: date,
                small_table: str,
                big_table: str,
                smooth_span: int,
                force_small: Optional[float],
                force_big: Optional[float]) -> None:
    """
    Train on day d (calibrate thresholds), predict on d+1 (write predictions).
    """
    d0s, d0e = day_bounds(d)
    d1 = d + timedelta(days=1)
    d1s, d1e = day_bounds(d1)

    df_train = fetch_ticks(d0s, d0e)
    df_pred = fetch_ticks(d1s, d1e)

    if df_train.empty or df_pred.empty:
        print(f"[{d}] skip: train={len(df_train)} pred={len(df_pred)}")
        return

    if force_small is not None and force_big is not None:
        small_prom, big_prom = float(force_small), float(force_big)
    else:
        small_prom, big_prom = calibrate_from_day(df_train)

    # Predict states on D+1
    states = detect_trend_states(df_pred, small_prom, big_prom, smooth_span=smooth_span)
    ensure_tables(small_table, big_table)

    n1 = upsert_predictions(small_table, d1, df_pred, states["small"], source=f"wf_small_{small_prom:.2f}")
    n2 = upsert_predictions(big_table, d1, df_pred, states["big"],   source=f"wf_big_{big_prom:.2f}")

    print(f"[{d}] trained(small={small_prom:.2f}, big={big_prom:.2f}) -> "
          f"pred {d1}: small rows={n1}, big rows={n2}")


def run(days: int = 1,
        start: Optional[str] = None,
        small_table: str = "predictions_small",
        big_table: str = "predictions_big",
        smooth_span: int = 20,
        small_prom: Optional[float] = None,
        big_prom: Optional[float] = None) -> None:
    """
    Walk forward for N days.
      - If start provided, begin there; else start at first tick day.
      - For each D in window, calibrate on D, predict D+1.
      - Writes to predictions_small/big.
    """
    if start:
        cur = date.fromisoformat(start)
    else:
        cur = first_tick_day()

    if cur is None:
        raise SystemExit("No ticks found to determine start day.")

    last = last_tick_day()
    if last is None:
        raise SystemExit("No ticks found to determine last day.")

    # We can only predict up to last-1 (need D+1 available)
    max_predictable = last - timedelta(days=1)
    end = min(cur + timedelta(days=days - 1), max_predictable)

    if end < cur:
        print(f"Nothing to do: start={cur}, last={last}")
        return

    print(f"Walk-forward: start={cur} end={end}  (predicts up to {end + timedelta(days=1)})")
    while cur <= end:
        process_day(
            cur,
            small_table=small_table,
            big_table=big_table,
            smooth_span=smooth_span,
            force_small=small_prom,
            force_big=big_prom
        )
        cur += timedelta(days=1)


# ------------------------------
# CLI
# ------------------------------
def main():
    ap = argparse.ArgumentParser(description="Day-by-day walk-forward with dual peak detectors.")
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD start day to train on")
    ap.add_argument("--days", type=int, default=1, help="number of training days (each predicts the next day)")
    ap.add_argument("--small-table", type=str, default="predictions_small")
    ap.add_argument("--big-table", type=str, default="predictions_big")
    ap.add_argument("--smooth-span", type=int, default=20, help="EMA span for smoothing prior to peaks")
    ap.add_argument("--small-prom", type=float, default=None, help="override small prominence ($). If omitted, auto-calibrate")
    ap.add_argument("--big-prom", type=float, default=None, help="override big prominence ($). If omitted, auto-calibrate")
    args = ap.parse_args()

    run(
        days=args.days,
        start=args.start,
        small_table=args.small_table,
        big_table=args.big_table,
        smooth_span=args.smooth_span,
        small_prom=args.small_prom,
        big_prom=args.big_prom
    )


if __name__ == "__main__":
    main()
