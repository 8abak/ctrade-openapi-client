import psycopg2
import pandas as pd

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# Load ticks
df = pd.read_sql("SELECT * FROM ticks ORDER BY timestamp ASC", conn)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Parameters
pivotLookback = 10
encounterRange = 0.05
confirmWindow = 50
minZoneGap = 0.3

supportLevels = []
resistanceLevels = []

def isPivotHigh(index):
    if index < pivotLookback or index + pivotLookback >= len(df):
        return False
    return all(df['ask'].iloc[index] > df['ask'].iloc[index - i] and df['ask'].iloc[index] > df['ask'].iloc[index + i] for i in range(1, pivotLookback + 1))

def isPivotLow(index):
    if index < pivotLookback or index + pivotLookback >= len(df):
        return False
    return all(df['bid'].iloc[index] < df['bid'].iloc[index - i] and df['bid'].iloc[index] < df['bid'].iloc[index + i] for i in range(1, pivotLookback + 1))

for i in range(pivotLookback, len(df) - confirmWindow):
    row = df.iloc[i]

    # Check for Support (Pivot Low)
    if isPivotLow(i):
        price = round(float(row['bid']), 2)
        if not any(abs(price - lvl) < minZoneGap for lvl in supportLevels):
            supportLevels.append(price)
            cur.execute("""
                INSERT INTO sr_zones (type, price, start_time, end_time)
                VALUES ('support', %s, %s, %s) RETURNING id
            """, (price, row['timestamp'], row['timestamp']))
            zoneId = cur.fetchone()[0]
            conn.commit()

            # Watch next ticks for encounter
            future = df.iloc[i+1:i+1+confirmWindow]
            for j in range(len(future)):
                touchPrice = float(future.iloc[j]['bid'])
                if abs(touchPrice - price) <= encounterRange:
                    futureSlice = df.iloc[i+j:i+j+confirmWindow]
                    outcome = 'reacted' if futureSlice['bid'].max() > price + encounterRange else \
                              'broken' if futureSlice['bid'].min() < price - encounterRange else 'unclear'
                    if outcome != 'unclear':
                        cur.execute("""
                            INSERT INTO sr_mob_events (zone_id, timestamp, outcome, confirmation_window_secs, price_at_touch, price_after)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            zoneId,
                            future.iloc[j]['timestamp'],
                            outcome,
                            confirmWindow,
                            touchPrice,
                            float(futureSlice['bid'].iloc[-1])
                        ))
                        conn.commit()
                        break

    # Check for Resistance (Pivot High)
    if isPivotHigh(i):
        price = round(float(row['ask']), 2)
        if not any(abs(price - lvl) < minZoneGap for lvl in resistanceLevels):
            resistanceLevels.append(price)
            cur.execute("""
                INSERT INTO sr_zones (type, price, start_time, end_time)
                VALUES ('resistance', %s, %s, %s) RETURNING id
            """, (price, row['timestamp'], row['timestamp']))
            zoneId = cur.fetchone()[0]
            conn.commit()

            # Watch next ticks for encounter
            future = df.iloc[i+1:i+1+confirmWindow]
            for j in range(len(future)):
                touchPrice = float(future.iloc[j]['ask'])
                if abs(touchPrice - price) <= encounterRange:
                    futureSlice = df.iloc[i+j:i+j+confirmWindow]
                    outcome = 'reacted' if futureSlice['ask'].min() < price - encounterRange else \
                              'broken' if futureSlice['ask'].max() > price + encounterRange else 'unclear'
                    if outcome != 'unclear':
                        cur.execute("""
                            INSERT INTO sr_mob_events (zone_id, timestamp, outcome, confirmation_window_secs, price_at_touch, price_after)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            zoneId,
                            future.iloc[j]['timestamp'],
                            outcome,
                            confirmWindow,
                            touchPrice,
                            float(futureSlice['ask'].iloc[-1])
                        ))
                        conn.commit()
                        break

cur.close()
conn.close()
