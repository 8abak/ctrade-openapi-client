import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Connect to PostgreSQL
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# Load 100,000 cleaned ticks
query = """
    SELECT timestamp, bid, ask, mid
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp ASC
    LIMIT 100000
"""
df = pd.read_sql(query, engine)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp")
df["timeDiff"] = df["timestamp"].diff().dt.total_seconds()
df["delta"] = df["mid"].diff().abs()

# Filter out dead-time jumps
df = df[df["timeDiff"] <= 1].copy()

# Define candidate brick sizes (only > spread)
brickSizes = [0.25, 0.3, 0.5, 1.0, 1.5, 2.0]

def simulate_bricks(midSeries, timeSeries, brickSize):
    bricks = []
    directions = []
    lastBrick = None
    startTime = None
    results = []

    for i in range(len(midSeries)):
        price = midSeries.iloc[i]
        time = timeSeries.iloc[i]

        if lastBrick is None:
            lastBrick = price
            startTime = time
            continue

        diff = price - lastBrick
        steps = int(diff / brickSize)

        if steps != 0:
            for _ in range(abs(steps)):
                direction = "up" if steps > 0 else "down"
                endTime = time
                bricks.append({
                    "start": startTime,
                    "end": endTime,
                    "durationSec": (endTime - startTime).total_seconds(),
                    "startPrice": lastBrick,
                    "endPrice": lastBrick + brickSize * (1 if steps > 0 else -1),
                    "direction": direction
                })
                lastBrick += brickSize * (1 if steps > 0 else -1)
                directions.append(direction)
                startTime = endTime

    return pd.DataFrame(bricks)

# Analyze each brick size
records = []
for size in brickSizes:
    brickDf = simulate_bricks(df["mid"], df["timestamp"], size)

    total = len(brickDf)
    zigzags = 0
    trends = 0
    maxRun = 0
    lastDir = None
    currentRun = 0

    for d in brickDf["direction"]:
        if d == lastDir:
            currentRun += 1
        else:
            if currentRun in [2, 3, 4]:
                zigzags += 1
            elif currentRun >= 5:
                trends += 1
                maxRun = max(maxRun, currentRun)
            currentRun = 1
        lastDir = d

    # Final run
    if currentRun in [2, 3, 4]:
        zigzags += 1
    elif currentRun >= 5:
        trends += 1
        maxRun = max(maxRun, currentRun)

    records.append({
        "brickSize": size,
        "brickCount": total,
        "zigzagCount": zigzags,
        "spikeCount": trends,
        "spikeRatio": round(trends / total, 4) if total else 0,
        "zigzagRatio": round(zigzags / total, 4) if total else 0,
        "maxSpikeLength": maxRun
    })

resultDf = pd.DataFrame(records)
print("\nRenko Brick Size Evaluation:")
print(resultDf.sort_values("spikeRatio", ascending=False).to_string(index=False))


# Report the time of the first and 100,000th tick
first_time = df["timestamp"].iloc[0]
last_time = df["timestamp"].iloc[-1]
duration = last_time - first_time

print(f"\nFirst tick time:  {first_time}")
print(f"Last tick time:   {last_time}")
print(f"Time span:        {duration} (HH:MM:SS)")
