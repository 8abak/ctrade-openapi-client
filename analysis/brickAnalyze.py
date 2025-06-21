import pandas as pd
from sqlalchemy import create_engine

# Connect to PostgreSQL
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# Load and clean tick data
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
df = df[df["timeDiff"] <= 1].copy()

# Candidate brick sizes
brickSizes = [0.25, 0.3, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]

# Renko brick builder
def simulate_bricks(midSeries, timeSeries, brickSize):
    bricks = []
    lastBrick = None
    startTime = None

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
                bricks.append({
                    "start": startTime,
                    "end": time,
                    "durationSec": (time - startTime).total_seconds(),
                    "startPrice": lastBrick,
                    "endPrice": lastBrick + brickSize * (1 if steps > 0 else -1),
                    "direction": direction
                })
                lastBrick += brickSize * (1 if steps > 0 else -1)
                startTime = time

    return pd.DataFrame(bricks)

# Evaluate each brick size
records = []
for size in brickSizes:
    brickDf = simulate_bricks(df["mid"], df["timestamp"], size)
    total = len(brickDf)

    if total == 0:
        continue

    # Run analysis
    zigzags = 0
    spikes = 0
    maxSpikeRun = 0
    maxZigzagRun = 0

    lastDir = None
    currentSpike = 0
    currentZigzag = 0

    for d in brickDf["direction"]:
        if lastDir is None:
            currentSpike = 1
            currentZigzag = 1
        elif d == lastDir:
            currentSpike += 1
            maxZigzagRun = max(maxZigzagRun, currentZigzag)
            if currentZigzag >= 2:
                zigzags += 1
            currentZigzag = 1
        else:
            currentZigzag += 1
            if currentSpike >= 2:
                spikes += 1
                maxSpikeRun = max(maxSpikeRun, currentSpike)
            currentSpike = 1
        lastDir = d

    # Final sequence
    maxZigzagRun = max(maxZigzagRun, currentZigzag)
    if currentZigzag >= 2:
        zigzags += 1
    if currentSpike >= 2:
        spikes += 1
        maxSpikeRun = max(maxSpikeRun, currentSpike)

    records.append({
        "brickSize": size,
        "brickCount": total,
        "zigzagCount": zigzags,
        "spikeCount": spikes,
        "spikeRatio": round(spikes / total, 4),
        "zigzagRatio": round(zigzags / total, 4),
        "maxSpikeLength": maxSpikeRun,
        "maxZigzagLength": maxZigzagRun
    })

# Show results
resultDf = pd.DataFrame(records)
print("\nRenko Brick Size Evaluation:")
print(resultDf.sort_values("spikeRatio", ascending=False).to_string(index=False))
