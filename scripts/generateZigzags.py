import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import argparse

# ---- CONFIG ----
DATABASE_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(DATABASE_URL)

# ---- ZIGZAG ENGINE ----
def calculate_atr(series: pd.Series, window: int = 14) -> float:
    diffs = np.abs(series.diff())
    return diffs.rolling(window=window).mean().iloc[-1]

def zigzag_engine(df, mode, threshold, atr_value=None):
    pivots = []
    last_pivot_price = df["mid"].iloc[0]
    direction = None

    for i in range(1, len(df)):
        price = df["mid"].iloc[i]
        price_change = price - last_pivot_price

        if mode == "abs":
            move_enough = abs(price_change) >= threshold
        elif mode == "pct":
            move_enough = abs(price_change) / last_pivot_price >= threshold
        elif mode == "atr":
            move_enough = abs(price_change) >= atr_value * threshold
        else:
            raise ValueError("Invalid mode")

        if not move_enough:
            continue

        new_dir = "up" if price_change > 0 else "dn"
        if direction is None or new_dir != direction:
            direction = new_dir
            last_pivot_price = price
            pivots.append({
                "tickid": int(df["tickid"].iloc[i]),
                "timestamp": df["timestamp"].iloc[i],
                "price": price,
                "direction": direction
            })

    return pivots

# ---- MAIN PROCESS ----
def run_for_date(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start = date.replace(hour=8, minute=0, second=0)
    end = start + timedelta(days=1)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id AS tickid, timestamp, mid
            FROM ticks
            WHERE timestamp >= :start AND timestamp < :end
            ORDER BY timestamp ASC
        """), {"start": start, "end": end})
        rows = result.fetchall()

    if not rows:
        print(f"No data found for {date_str}")
        return

    df = pd.DataFrame(rows, columns=["tickid", "timestamp", "mid"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    atr_val = calculate_atr(df["mid"])

    configs = [
        ("zAtr0.5", "atr", 0.5),
        ("zAtr1.0", "atr", 1.0),
        ("zAtr2.0", "atr", 2.0),
        ("zAbs0.5", "abs", 0.5),
        ("zAbs1.0", "abs", 1.0),
        ("zAbs3.0", "abs", 3.0),
        ("zAbs10.0", "abs", 10.0),
        ("zPct0.1", "pct", 0.001),
        ("zPct0.3", "pct", 0.003),
        ("zPct0.5", "pct", 0.005),
        ("zPct1.0", "pct", 0.01),
        ("zPct5.0", "pct", 0.05),
        ("zPct10.0", "pct", 0.10)
    ]

    all_pivots = []
    for levelName, mode, threshold in configs:
        pivots = zigzag_engine(df, mode, threshold, atr_val)
        for p in pivots:
            p["level"] = levelName
        all_pivots.extend(pivots)

    if all_pivots:
        pivot_df = pd.DataFrame(all_pivots)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM zigzag_pivots WHERE timestamp >= :start AND timestamp < :end"), {"start": start, "end": end})
            pivot_df.to_sql("zigzag_pivots", engine, index=False, if_exists="append")
            print(f"Inserted {len(pivot_df)} pivots for {date_str}")
    else:
        print("No pivots generated.")

# ---- ENTRY ----
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    run_for_date(args.date)
