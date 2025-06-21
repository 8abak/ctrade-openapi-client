import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text

# Database connection
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# Step 1: Get the latest toTime from brickan
with engine.connect() as conn:
    result = conn.execute(text("SELECT MAX(toTime) FROM brickan"))
    latest_time = result.scalar()

if latest_time is None:
    latest_time = "1970-01-01T00:00:00"  # Start from beginning if table is empty

# Step 2: Load next 100,000 ticks after latest_time
query = f"""
    SELECT timestamp, bid, ask, mid
    FROM ticks
    WHERE symbol = 'XAUUSD'
    AND timestamp > '{latest_time}'
    ORDER BY timestamp ASC
    LIMIT 100000
"""
df = pd.read_sql(query, engine)
df["timestamp"] = pd.to_datetime(df["timestamp"])

if df.empty:
    print("✅ No new ticks to process.")
    exit()

# Step 3: Filter data
startTime = df["timestamp"].min()
endTime = df["timestamp"].max()
df["spread"] = df["ask"] - df["bid"]
df = df.sort_values("timestamp")
df["timeDiff"] = df["timestamp"].diff().dt.total_seconds()
filtered = df[df["timeDiff"] <= 1].copy()
filtered["delta"] = filtered["mid"].diff().abs()
filtered = filtered.dropna()
mid_prices = filtered["mid"].values

# Step 4: Brick builder
def build_renko(mid_prices, brick_size):
    bricks = []
    last = None
    for price in mid_prices:
        if last is None:
            last = price
            bricks.append(price)
            continue
        diff = price - last
        steps = int(diff / brick_size)
        for _ in range(abs(steps)):
            last += brick_size * np.sign(steps)
            bricks.append(last)
    return bricks

# Step 5: Analyzer
def analyze_bricks(brick_prices):
    directions = np.sign(np.diff(brick_prices))
    if len(directions) == 0:
        return 0, 0, 0, 0, 0, 0, 0

    pivotCount = 0
    zigzagCount = 0
    spike_lengths = []
    zigzag_lengths = []

    cur = 1
    while cur < len(directions):
        if directions[cur] != directions[cur - 1]:
            pivotCount += 1
            if cur >= 2 and directions[cur - 2] == directions[cur]:
                zigzagCount += 1
        cur += 1

    # Spike
    current_dir = directions[0]
    current_len = 1
    for d in directions[1:]:
        if d == current_dir:
            current_len += 1
        else:
            spike_lengths.append(current_len)
            current_len = 1
            current_dir = d
    spike_lengths.append(current_len)

    # Zigzag
    zigzag_streak = 1
    for i in range(1, len(directions)):
        if directions[i] != directions[i - 1]:
            zigzag_streak += 1
        else:
            if zigzag_streak >= 3:
                zigzag_lengths.append(zigzag_streak)
            zigzag_streak = 1
    if zigzag_streak >= 3:
        zigzag_lengths.append(zigzag_streak)

    return (
        pivotCount,
        zigzagCount,
        len(spike_lengths),
        round(len(spike_lengths) / len(brick_prices), 4),
        round(len(zigzag_lengths) / len(brick_prices), 4),
        max(spike_lengths) if spike_lengths else 0,
        max(zigzag_lengths) if zigzag_lengths else 0
    )

# Step 6: Run over brick sizes and insert
conn = psycopg2.connect(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)
cur = conn.cursor()

for brickSize in np.round(np.arange(0.4, 6.1, 0.1), 2):
    bricks = build_renko(mid_prices, brickSize)
    pivot, zigzag, spike, spikeR, zigzagR, maxS, maxZ = analyze_bricks(bricks)

    cur.execute("""
        INSERT INTO brickan (
            brickSize, timestamp, toTime, brickCount,
            pivotCount, zigzagCount, spikeCount,
            spikeRatio, zigzagRatio, maxSpikeLength, maxZigzagLength
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        float(brickSize), startTime, endTime, len(bricks),
        pivot, zigzag, spike,
        spikeR, zigzagR, maxS, maxZ
    ))

conn.commit()
cur.close()
conn.close()
print(f"✅ Brick analysis stored from {startTime} to {endTime} in 'brickan'.")
