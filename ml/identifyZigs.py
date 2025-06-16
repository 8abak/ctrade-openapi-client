import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# === DB CONNECTION ===
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)

# Or use SQLAlchemy for easier update
engine = create_engine("postgresql+psycopg2://babak:BB@bb33044@localhost:5432/trading")

# === LOAD DATA ===
df = pd.read_sql("SELECT * FROM pivotIdentification ORDER BY timestamp ASC", conn)
df['mid'] = df['mid'].astype(float)

# === CALCULATE ROLLING ADX-LIKE THRESHOLD ===
df['mid_diff'] = df['mid'].diff()
df['up_move'] = df['mid_diff'].where(df['mid_diff'] > 0, 0)
df['dn_move'] = -df['mid_diff'].where(df['mid_diff'] < 0, 0)

period = 9000  # ~30 minutes of ticks (if ~5 per sec)
df['plus_di'] = 100 * df['up_move'].rolling(period).mean() / df['mid'].rolling(period).mean()
df['minus_di'] = 100 * df['dn_move'].rolling(period).mean() / df['mid'].rolling(period).mean()
df['adx'] = (df['plus_di'] - df['minus_di']).abs()

# === ZIGZAG DETECTION ===
pivots = []
last_pivot_idx = None
last_pivot_price = None
last_direction = None

for i in range(period, len(df)):
    price = df.at[i, 'mid']
    threshold = df.at[i, 'adx']
    if pd.isna(threshold):
        continue

    if last_pivot_price is None:
        # First pivot (start point)
        last_pivot_price = price
        last_pivot_idx = i
        df.at[i, 'pivot_type'] = 'PIVOT_LOW'
        continue

    price_change = price - last_pivot_price
    if last_direction in [None, 'dnZig'] and price_change > threshold:
        # New upward pivot
        df.at[i, 'pivot_type'] = 'PIVOT_HIGH'
        df.loc[last_pivot_idx:i, 'zigzag_direction'] = 'upZig'
        last_pivot_price = price
        last_pivot_idx = i
        last_direction = 'upZig'

    elif last_direction in [None, 'upZig'] and -price_change > threshold:
        # New downward pivot
        df.at[i, 'pivot_type'] = 'PIVOT_LOW'
        df.loc[last_pivot_idx:i, 'zigzag_direction'] = 'dnZig'
        last_pivot_price = price
        last_pivot_idx = i
        last_direction = 'dnZig'

# === SAVE BACK TO DATABASE ===
df_to_update = df[['id', 'pivot_type', 'zigzag_direction']].dropna(subset=['pivot_type', 'zigzag_direction'])

with engine.begin() as connection:
    for _, row in df_to_update.iterrows():
        connection.execute(
            f"""
            UPDATE pivotIdentification
            SET pivot_type = %s,
                zigzag_direction = %s
            WHERE id = %s
            """,
            (row['pivot_type'], row['zigzag_direction'], int(row['id']))
        )

print("✅ Zigzag labeling complete.")
