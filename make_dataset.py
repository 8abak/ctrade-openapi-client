# make_dataset.py

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from tqdm import tqdm

# ---- CONFIG ----
DATABASE_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(DATABASE_URL)
WINDOW_SIZE = 250  # Total tick window: ±250 → 500

# ---- LOAD ALL DATA ----
with engine.connect() as conn:
    ticks_df = pd.read_sql("SELECT id AS tickid, timestamp, mid FROM ticks ORDER BY tickid ASC", conn)
    pivots_df = pd.read_sql("SELECT * FROM zigzag_pivots ORDER BY tickid ASC", conn)

# ---- Extract zAtr0.5 candidates and zAbs3.0 for labels ----
zatr = pivots_df[pivots_df["level"] == "zAtr0.5"]
zabs = set(zip(
    pivots_df[pivots_df["level"] == "zAbs3.0"]["tickid"],
    pivots_df[pivots_df["level"] == "zAbs3.0"]["direction"]
))

# ---- Row Processor ----
def process_pivot(pivot_row):
    tickid = pivot_row.tickid
    label = int((tickid, pivot_row.direction) in zabs)
    
    idx = ticks_df.index[ticks_df["tickid"] == tickid].tolist()
    if not idx: return None
    idx = idx[0]

    if idx < WINDOW_SIZE or idx + WINDOW_SIZE >= len(ticks_df):
        return None

    # Tick window (deltas)
    tick_window = ticks_df["mid"].iloc[idx - WINDOW_SIZE: idx + WINDOW_SIZE].diff().fillna(0).tolist()

    # Std dev before/after
    stddev_before = ticks_df["mid"].iloc[idx - 200:idx].std()
    stddev_after = ticks_df["mid"].iloc[idx:idx + 200].std()

    # Slope
    y = ticks_df["mid"].iloc[idx - 100:idx].values
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]

    # ATR(140)
    atr140 = ticks_df["mid"].diff().abs().rolling(140).mean().iloc[idx]

    # Time of day (in minutes)
    t = pivot_row.timestamp.time()
    time_of_day = t.hour * 60 + t.minute + t.second / 60

    # Zigzag context: 20 before + 30 after
    piv_idx = pivots_df.index[pivots_df["tickid"] == tickid].tolist()
    if not piv_idx: return None
    piv_idx = piv_idx[0]
    context = pivots_df.iloc[max(0, piv_idx - 20): piv_idx + 30]
    zigzag_context = [f"{row.direction}:{row.level}" for _, row in context.iterrows()]

    # ATR(15) of zigzag context
    prices = context["price"].values
    atr15 = np.mean(np.abs(np.diff(prices))) if len(prices) > 1 else 0

    return {
        "tickid": int(tickid),
        "label": label,
        "tick_window": tick_window,
        "zigzag_context": zigzag_context,
        "atr140": float(atr140),
        "atr15": float(atr15),
        "stddev_before": float(stddev_before),
        "stddev_after": float(stddev_after),
        "slope": float(slope),
        "time_of_day": float(time_of_day)
    }

# ---- Process All ----
rows = []
for _, row in tqdm(zatr.iterrows(), total=len(zatr)):
    out = process_pivot(row)
    if out:
        rows.append(out)

# ---- Insert into DB ----
with engine.begin() as conn:
    conn.execute(text("DELETE FROM zigzag_training_data"))  # clear old
    for row in rows:
        conn.execute(text("""
            INSERT INTO zigzag_training_data (
                tickid, label, tick_window, zigzag_context,
                atr140, atr15, stddev_before, stddev_after,
                slope, time_of_day
            ) VALUES (
                :tickid, :label, :tick_window, :zigzag_context,
                :atr140, :atr15, :stddev_before, :stddev_after,
                :slope, :time_of_day
            )
        """), row)

print(f"✅ Inserted {len(rows)} rows into zigzag_training_data")
