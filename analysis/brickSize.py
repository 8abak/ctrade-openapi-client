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

# Step 1: Calculate tick-to-tick change
df["delta"] = df["mid"].diff().abs()

# Step 2: Show distribution stats
print("Descriptive statistics of tick-to-tick movement:")
print(df["delta"].describe())

# Step 3: Plot histogram
plt.figure(figsize=(10, 5))
sns.histplot(df["delta"].dropna(), bins=100, kde=True)
plt.title("Tick-to-Tick Price Change (XAUUSD)")
plt.xlabel("Absolute ΔPrice")
plt.ylabel("Frequency")
plt.grid(True)
plt.tight_layout()
plt.show()
