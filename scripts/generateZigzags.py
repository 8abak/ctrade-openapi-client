import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import argparse

# ---- CONFIG ----
DATABASE_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(DATABASE_URL)

# ---- ZIGZAG ENGINE (Correct First Extreme After Reversal) ----
def zigzag_engine(df, threshold):
    pivots = []
    i = 0
    n = len(df)

    # Find the first pivot: either first high or low that moved at least $threshold from the starting point
    while i < n:
        start_price = df["mid"].iloc[i]
        j = i + 1
        while j < n and abs(df["mid"].iloc[j] - start_price) < threshold:
            j += 1
        if j >= n:
            return pivots
        direction = "up" if df["mid"].iloc[j] > start_price else "dn"
        range_df = df.iloc[i:j+1]
        pivot_idx = range_df["mid"].idxmin() if direction == "up" else range_df["mid"].idxmax()
        pivots.append({
            "tickid": int(df["tickid"].loc[pivot_idx]),
            "timestamp": df["timestamp"].loc[pivot_idx],
            "price": df["mid"].loc[pivot_idx],
            "direction": direction
        })
        i = df.index.get_loc(pivot_idx)
        break

    # Alternate pivots after that
    while i < n:
        last_dir = pivots[-1]["direction"]
        last_price = pivots[-1]["price"]
        j = i + 1
        while j < n:
            price = df["mid"].iloc[j]
            move = price - last_price if last_dir == "dn" else last_price - price
            if move >= threshold:
                range_df = df.iloc[i:j+1]
                if last_dir == "dn":
                    pivot_idx = range_df["mid"].idxmax()
                    direction = "up"
                else:
                    pivot_idx = range_df["mid"].idxmin()
                    direction = "dn"
                pivots.append({
                    "tickid": int(df["tickid"].loc[pivot_idx]),
                    "timestamp": df["timestamp"].loc[pivot_idx],
                    "price": df["mid"].loc[pivot_idx],
                    "direction": direction
                })
                i = df.index.get_loc(pivot_idx)
                break
            j += 1
        else:
            break

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

    levelName = "zAbs1.0"
    threshold = 1.0
    pivots = zigzag_engine(df, threshold)
    for p in pivots:
        p["level"] = levelName

    if pivots:
        pivot_df = pd.DataFrame(pivots)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM zigzag_pivots WHERE timestamp >= :start AND timestamp < :end"), {"start": start, "end": end})
            pivot_df.to_sql("zigzag_pivots", engine, index=False, if_exists="append")
            print(f"Inserted {len(pivot_df)} zAbs1.0 pivots for {date_str}")
    else:
        print("No pivots generated.")

# ---- ENTRY ----
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    run_for_date(args.date)
