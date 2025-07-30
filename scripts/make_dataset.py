import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from tqdm import tqdm

# Configuration
DATABASE_URL = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(DATABASE_URL)
WINDOW_SIZE = 250  # ticks before and after (500 total)

# --- Step 1: Load ticks and pivots ---
with engine.connect() as conn:
    ticks_df = pd.read_sql("SELECT id AS tickid, timestamp, mid FROM ticks ORDER BY tickid ASC", conn)
    pivots_df = pd.read_sql("SELECT * FROM zigzag_pivots ORDER BY tickid ASC", conn)

# --- Step 2: Filter zAtr0.5 and zAbs3.0 ---
zatr_df = pivots_df[pivots_df["level"] == "zAtr0.5"].copy()
zabs_df = pivots_df[pivots_df["level"] == "zAbs3.0"].copy()

# --- Step 3: Mark each zAtr0.5 tick as positive if reused in zAbs3.0 ---
zabs_set = set(zip(zabs_df["tickid"], zabs_df["direction"]))
zatr_df["label"] = zatr_df.apply(lambda r: 1 if (r["tickid"], r["direction"]) in zabs_set else 0, axis=1)

# --- Step 4: Create training rows ---
def compute_row(pivot):
    tickid = pivot["tickid"]
    timestamp = pivot["timestamp"]
    direction = pivot["direction"]
    label = pivot["label"]

    # Get index in ticks
    idx = ticks_df.index[ticks_df["tickid"] == tickid].tolist()
    if not idx:
        return None
    idx = idx[0]

    if idx < WINDOW_SIZE or idx + WINDOW_SIZE >= len(ticks_df):
        return None  # skip edge cases

    # Tick window
    window_ticks = ticks_df.iloc[idx - WINDOW_SIZE: idx + WINDOW_SIZE]
    tick_window = window_ticks["mid"].diff().fillna(0).values.tolist()

    # ATR(140)
    atr140 = ticks_df["mid"].diff().abs().rolling(140).mean().iloc[idx]

    # Std dev before/after
    stddev_before = ticks_df["mid"].iloc[idx - 200:idx].std()
    stddev_after = ticks_df["mid"].iloc[idx:idx + 200].std()

    # Slope (price vs index)
    y = ticks_df["mid"].iloc[idx - 100:idx].values
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]

    # Time of day
    t = timestamp.time()
    time_of_day = t.hour * 60 + t.minute + t.second / 60

    # Zigzag context (20 before + 30 after from pivots)
    piv_idx = pivots_df.index[pivots_df["tickid"] == tickid].tolist()
    if not piv_idx:
        return None
    piv_idx = piv_idx[0]
    context_rows = pivots_df.iloc[max(0, piv_idx - 20): piv_idx + 30]
    zigzag_context = [
        f"{row['direction']}:{row['level']}" for _, row in context_rows.iterrows()
    ]

    # ATR(15) over context
    context_prices = context_rows["price"].values
    context_atr = np.abs(np.diff(context_prices)).mean() if len(context_prices) > 1 else 0

    return {
        "tickid": int(tickid),
        "label": int(label),
        "tick_window": tick_window,
        "zigzag_context": zigzag_context,
        "atr140": float(atr140),
        "atr15": float(context_atr),
        "stddev_before": float(stddev_before),
        "stddev_after": float(stddev_after),
        "slope": float(slope),
        "time_of_day": float(time_of_day)
    }

# --- Step 5: Generate rows and insert into DB ---
rows = []
for _, pivot in tqdm(zatr_df.iterrows(), total=len(zatr_df)):
    row = compute_row(pivot)
    if row:
        rows.append(row)

df_final = pd.DataFrame(rows)

with engine.begin() as conn:
    conn.execute(text("DELETE FROM zigzag_training_data"))
    for _, row in df_final.iterrows():
        conn.execute(text("""
            INSERT INTO zigzag_training_data (
                tickid, label, tick_window, zigzag_context, atr140, atr15,
                stddev_before, stddev_after, slope, time_of_day
            ) VALUES (
                :tickid, :label, :tick_window, :zigzag_context, :atr140, :atr15,
                :stddev_before, :stddev_after, :slope, :time_of_day
            )
        """), {
            "tickid": row["tickid"],
            "label": row["label"],
            "tick_window": row["tick_window"],
            "zigzag_context": row["zigzag_context"],
            "atr140": row["atr140"],
            "atr15": row["atr15"],
            "stddev_before": row["stddev_before"],
            "stddev_after": row["stddev_after"],
            "slope": row["slope"],
            "time_of_day": row["time_of_day"]
        })

print("✅ Dataset inserted into zigzag_training_data")
