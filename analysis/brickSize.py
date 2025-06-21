import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Connect to PostgreSQL
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# Load a large chunk of ticks
query = """
    SELECT timestamp, bid, ask, mid
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp ASC
    LIMIT 100000
"""
df = pd.read_sql(query, engine)
df["timestamp"] = pd.to_datetime(df["timestamp"])

#calculate spread for better understanding of price movement
df["spread"] = df["ask"] - df["bid"]
spreadStats = df["spread"].describe()
print("Descriptive statistics of spread:")
print(spreadStats)

#Sort by timestamp to ensure order
df = df.sort_values("timestamp")

#Compute time difference between ticks
df["timeDiff"] = df["timestamp"].diff().dt.total_seconds()

#Only keep rows where the time gap between ticks is short (e.g., less than 10 seconds)
filtered= df[df["timeDiff"] <= 10].copy()

#Compute price movement
filtered["delta"] = filtered["mid"].diff().abs()

# Step 2: Show distribution stats
print("Descriptive statistics of tick-to-tick movement:")
print(filtered["delta"].describe())

# Step 3: Plot histogram
plt.figure(figsize=(10, 5))
sns.histplot(filtered["delta"].dropna(), bins=100, kde=True)
plt.title("Tick-to-Tick Price Change (XAUUSD)")
plt.xlabel("Absolute ΔPrice")
plt.ylabel("Frequency")
plt.grid(True)
plt.tight_layout()
plt.show()
