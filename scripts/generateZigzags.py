import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import argparse

# ---- CONFIG ----
DATABASE_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(DATABASE_URL)

# ---- ZIGZAG ENGINE (Correct High/Low Snap) ----
def zigzag_engine(df, threshold):
    pivots = []
    last_pivot_idx = 0
    last_pivot_price = df["mid"].iloc[0]
    direction = None

    for i in range(1, len(df)):
        price = df["mid"].iloc[i]
        price_change = price - last_pivot_price

        if direction is None:
            if abs(price_change) >= threshold:
                direction = "up" if price_change > 0 else "dn"
                pivot_idx = df["mid"].iloc[:i+1].idxmin() if direction == "up" else df["mid"].iloc[:i+1].idxmax()
                pivots.append({
                    "tickid": int(df["tickid"].iloc[pivot_idx]),
                    "timestamp": df["timestamp"].iloc[pivot_idx],
                    "price": df["mid"].iloc[pivot_idx],
                    "direction": direction
                })
                last_pivot_idx = pivot_idx
                last_pivot_price = df["mid"].iloc[pivot_idx]
        else:
            if direction == "up":
                highest = df["mid"].iloc[last_pivot_idx:i+1].idxmax()
                max_move = df["mid"].iloc[highest] - last_pivot_price
                if max_move >= threshold:
                    pivots.append({
                        "tickid": int(df["tickid"].iloc[highest]),
                        "timestamp": df["timestamp"].iloc[highest],
                        "price": df["mid"].iloc[highest],
                        "direction": "up"
                    })
                    last_pivot_idx = highest
                    last_pivot_price = df["mid"].iloc[highest]
                    direction = "dn"
            else:
                lowest = df["mid"].iloc[last_pivot_idx:i+1].idxmin()
                max_move = last_pivot_price - df["mid"].iloc[lowest]
                if max_move >= threshold:
                    pivots.append({
                        "tickid": int(df["tickid"].iloc[lowest]),
                        "timestamp": df["timestamp"].iloc[lowest],
                        "price": df["mid"].iloc[lowest],
                        "direction": "dn"
                    })
                    last_pivot_idx = lowest
                    last_pivot_price = df["mid"].iloc[lowest]
                    direction = "up"

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
