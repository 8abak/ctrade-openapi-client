import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Database connection
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# Load first 100k ticks
query = """
    SELECT timestamp, bid, ask, mid
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp ASC
    LIMIT 100000
"""
df = pd.read_sql(query, engine)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Calculate spread and filter by time difference
df["spread"] = df["ask"] - df["bid"]
df = df.sort_values("timestamp")
df["timeDiff"] = df["timestamp"].diff().dt.total_seconds()
filtered = df[df["timeDiff"] <= 1].copy()
filtered["delta"] = filtered["mid"].diff().abs()
filtered = filtered.dropna()

# Renko logic
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

# Analyze renko bricks
def analyze_bricks(brick_prices):
    directions = np.sign(np.diff(brick_prices))
    if len(directions) == 0:
        return 0, 0, 0, 0, 0, 0, 0

    spikeCount = 0
    zigzagCount = 0
    pivotCount = 0
    maxSpike = 0
    maxZigzag = 0
    cur = 1
    while cur < len(directions):
        if directions[cur] != directions[cur - 1]:
            pivotCount += 1
            if cur >= 2 and directions[cur - 2] == directions[cur]:
                zigzagCount += 1
        cur += 1

    current_dir = directions[0]
    current_len = 1
    spike_lengths = []
    zigzag_lengths = []
    for d in directions[1:]:
        if d == current_dir:
            current_len += 1
        else:
            spike_lengths.append(current_len)
            current_len = 1
            current_dir = d
    spike_lengths.append(current_len)

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

# Run analysis for multiple brick sizes
results = []
startTime = filtered["timestamp"].min()
endTime = filtered["timestamp"].max()
mid_prices = filtered["mid"].values

for brickSize in np.round(np.arange(0.4, 6.1, 0.1), 2):
    bricks = build_renko(mid_prices, brickSize)
    pivot, zigzag, spike, spikeR, zigzagR, maxS, maxZ = analyze_bricks(bricks)
    results.append({
        "brickSize": brickSize,
        "brickCount": len(bricks),
        "pivotCount": pivot,
        "zigzagCount": zigzag,
        "spikeCount": spike,
        "spikeRatio": spikeR,
        "zigzagRatio": zigzagR,
        "maxSpikeLength": maxS,
        "maxZigzagLength": maxZ,
        "fromTime": startTime,
        "toTime": endTime
    })


# Save to DB
result_df = pd.DataFrame(results)
# Save each row to the brickan table
import psycopg2

conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="babak33044",
    host="localhost",
    port=5432
)
cur = conn.cursor()

for row in results:
    cur.execute("""
    INSERT INTO brickan (
        brickSize, fromTime, toTime, brickCount,
        pivotCount, zigzagCount, spikeCount,
        spikeRatio, zigzagRatio, maxSpikeLength, maxZigzagLength
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (brickSize) DO NOTHING
""", (
    float(row["brickSize"]),
    row["fromTime"],
    row["toTime"],
    int(row["brickCount"]),
    int(row["pivotCount"]),
    int(row["zigzagCount"]),
    int(row["spikeCount"]),
    float(row["spikeRatio"]),
    float(row["zigzagRatio"]),
    int(row["maxSpikeLength"]),
    int(row["maxZigzagLength"])
    ))


conn.commit()
cur.close()
conn.close()

print("✅ All records inserted into 'brickan'.")
