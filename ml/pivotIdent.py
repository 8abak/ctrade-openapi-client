import pandas as pd
import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="babak33044",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# Load data
df = pd.read_sql("SELECT * FROM ticks ORDER BY timestamp ASC LIMIT 20000", conn)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['mid'] = (df['bid'] + df['ask']) / 2

# Pivot identification logic
pivots = []
i = 1

while i < len(df) - 1:
    prevMid = df['mid'].iloc[i - 1]
    currMid = df['mid'].iloc[i]
    nextMid = df['mid'].iloc[i + 1]

    if currMid > prevMid and currMid > nextMid:
        pivots.append({'type': 'high', 'i': i, 'timestamp': df['timestamp'].iloc[i], 'price': float(round(currMid, 2))})
    elif currMid < prevMid and currMid < nextMid:
        pivots.append({'type': 'low', 'i': i, 'timestamp': df['timestamp'].iloc[i], 'price': float(round(currMid, 2))})
    i += 1

# Structural enforcement
finalPivots = []
for j in range(1, len(pivots)):
    prev = pivots[j - 1]
    curr = pivots[j]

    if prev['type'] == curr['type']:
        subset = df.iloc[prev['i']:curr['i']]

        if curr['type'] == 'high':
            idx = subset['mid'].idxmin()
            finalPivots.append({
                'type': 'low',
                'timestamp': df['timestamp'].loc[idx],
                'price': float(round(df['mid'].loc[idx], 2))
            })
        else:
            idx = subset['mid'].idxmax()
            finalPivots.append({
                'type': 'high',
                'timestamp': df['timestamp'].loc[idx],
                'price': float(round(df['mid'].loc[idx], 2))
            })

    finalPivots.append(curr)

# Save to database
cur.execute("DELETE FROM pivots;")
for p in finalPivots:
    cur.execute("""
        INSERT INTO pivots (timestamp, price, pivot_type)
        VALUES (%s, %s, %s)
    """, (p['timestamp'], p['price'], p['type']))
conn.commit()
cur.close()
conn.close()

print(f"✅ Inserted {len(finalPivots)} structured pivots.")
