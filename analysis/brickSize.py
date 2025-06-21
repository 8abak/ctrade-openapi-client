import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Connect to PostgreSQL
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# Load 100,000 ticks (ordered by timestamp)
query = """
    SELECT timestamp, bid, ask, mid
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp ASC
    LIMIT 100000
"""
df = pd.read_sql(query, engine)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Calculate spread
df["spread"] = df["ask"] - df["bid"]
print("Descriptive statistics of spread:")
print(df["spread"].describe())

# Sort and compute time between ticks
df = df.sort_values("timestamp")
df["timeDiff"] = df["timestamp"].diff().dt.total_seconds()

# Compute mid-price delta
df["delta"] = df["mid"].diff().abs()

# Filter out tick pairs with timeDiff > 1s (dead periods)
filtered = df[df["timeDiff"] <= 1].copy()

# Show clean delta stats
print("\nDescriptive statistics of filtered tick-to-tick movement:")
print(filtered["delta"].describe())

# Plot clean delta histogram
plt.figure(figsize=(10, 5))
sns.histplot(filtered["delta"].dropna(), bins=100, kde=True)
plt.title("Cleaned Tick-to-Tick Price Change (XAUUSD)")
plt.xlabel("Absolute ΔPrice")
plt.ylabel("Frequency")
plt.grid(True)
plt.tight_layout()
plt.show()
