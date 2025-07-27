import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import argparse

# ---- CONFIG ----
DATABASE_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(DATABASE_URL)

# ---- ZIGZAG ENGINE (Track extreme until reversal over threshold) ----
def zigzag_engine(df, threshold):
    pivots = []
    n = len(df)
    i = 0

    # Start with first price
    extreme_idx = i
    extreme_price = df["mid"].iloc[i]
    direction = None

    for j in range(i + 1, n):
        price = df["mid"].iloc[j]

        if direction is None:
            move = price - extreme_price
            if abs(move) >= threshold:
                direction = "up" if move > 0 else "dn"
                extreme_idx = j
                extreme_price = price
        else:
            if direction == "up":
                if price > extreme_price:
                    extreme_price = price
                    extreme_idx = j
                elif extreme_price - price >= threshold:
                    pivots.append({
                        "tickid": int(df["tickid"].iloc[extreme_idx]),
                        "timestamp": df["timestamp"].iloc[extreme_idx],
                        "price": extreme_price,
                        "direction": "up"
                    })
                    direction = "dn"
                    extreme_idx = j
                    extreme_price = price
            elif direction == "dn":
                if price < extreme_price:
                    extreme_price = price
                    extreme_idx = j
                elif price - extreme_price >= threshold:
                    pivots.append({
                        "tickid": int(df["tickid"].iloc[extreme_idx]),
                        "timestamp": df["timestamp"].iloc[extreme_idx],
                        "price": extreme_price,
                        "direction": "dn"
                    })
                    direction = "up"
                    extreme_idx = j
                    extreme_price = price

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

    # ATR estimate from past 14 bars
    atr_val = np.abs(df["mid"].diff()).rolling(14).mean().iloc[-1]

    configs = [
        ("zAtr0.5", atr_val * 0.5),
        ("zAtr1.0", atr_val * 1.0),
        ("zAtr2.0", atr_val * 2.0),
        ("zAbs0.5", 0.5),
        ("zAbs1.0", 1.0),
        ("zAbs3.0", 3.0),
        ("zAbs10.0", 10.0),
        ("zPct0.1", df["mid"].iloc[-1] * 0.001),
        ("zPct0.3", df["mid"].iloc[-1] * 0.003),
        ("zPct0.5", df["mid"].iloc[-1] * 0.005),
        ("zPct1.0", df["mid"].iloc[-1] * 0.01),
        ("zPct5.0", df["mid"].iloc[-1] * 0.05),
        ("zPct10.0", df["mid"].iloc[-1] * 0.10),
    ]

    all_pivots = []
    for levelName, threshold in configs:
        pivots = zigzag_engine(df, threshold)
        for p in pivots:
            p["level"] = levelName
        all_pivots.extend(pivots)

    if all_pivots:
        pivot_df = pd.DataFrame(all_pivots)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM zigzag_pivots WHERE timestamp >= :start AND timestamp < :end"), {"start": start, "end": end})
            pivot_df.to_sql("zigzag_pivots", engine, index=False, if_exists="append")
            print(f"Inserted {len(pivot_df)} total pivots for {date_str} across all levels")
    else:
        print("No pivots generated.")

# ---- ENTRY ----
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    run_for_date(args.date)
