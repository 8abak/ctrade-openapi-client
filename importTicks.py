# importTicks.py

import psycopg2
import pandas as pd
from datetime import datetime

# Load CSV
df = pd.read_csv("samples/ConsoleSample/ticks.csv")

# Load CSV
df = pd.read_csv("samples/ConsoleSample/ticks.csv")

# Replace 0.0 with NaN to allow forward-fill
df["bid"] = df["bid"].replace(0.0, pd.NA)
df["ask"] = df["ask"].replace(0.0, pd.NA)

# Forward-fill missing values using last valid tick
df["bid"] = df["bid"].ffill()
df["ask"] = df["ask"].ffill()

# Convert timestamp (ms) to UTC datetime
df["timestamp"] = df["timestamp"].apply(lambda x: datetime.utcfromtimestamp(x / 1000.0))

# Connect to DB
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb3304",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# Insert rows
for _, row in df.iterrows():
    cur.execute(
        """
        INSERT INTO ticks (symbol, timestamp, bid, ask)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
        """,
        ('XAUUSD', row['timestamp'], row['bid'], row['ask'])
    )

conn.commit()
cur.close()
conn.close()

print(f"✅ Inserted {len(df)} clean tick rows into database.")
