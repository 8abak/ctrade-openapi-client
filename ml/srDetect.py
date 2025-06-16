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

# Parameters
windowSize = 200
encounterRange = 0.05
confirmWindow = 50
minZoneGap = 0.3  # Minimum distance to accept a new zone

lastSupport = None
lastResistance = None

for i in range(0, len(df) - windowSize - confirmWindow, windowSize):
    window = df.iloc[i:i+windowSize]

    # Detect support zone from min bid
    minBid = float(window['bid'].min())
    minBidIdx = window['bid'].idxmin()
    minTime = df.loc[minBidIdx]['timestamp']

    if lastSupport is None or abs(minBid - lastSupport) > minZoneGap:
        lastSupport = float(minBid)
        cur.execute("""
            INSERT INTO sr_zones (type, price, start_time, end_time)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, ('support', float(minBid), window.iloc[0]['timestamp'], window.iloc[-1]['timestamp']))
        supportId = cur.fetchone()[0]
        conn.commit()

        # Check for encounter
        future = df.iloc[i+windowSize:i+windowSize+confirmWindow]
        for j in range(len(future)):
            price = float(future.iloc[j]['bid'])
            if abs(price - minBid) <= encounterRange:
                futureSlice = future.iloc[j:j+10]
                rebound = futureSlice['bid'].max() > minBid + encounterRange
                breakdown = futureSlice['bid'].min() < minBid - encounterRange
                outcome = 'reacted' if rebound else 'broken' if breakdown else 'unclear'

                cur.execute("""
                    INSERT INTO sr_mob_events (zone_id, timestamp, outcome, confirmation_window_secs, price_at_touch, price_after)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    supportId,
                    future.iloc[j]['timestamp'],
                    outcome,
                    confirmWindow,
                    price,
                    float(futureSlice['bid'].iloc[-1])
                ))
                conn.commit()
                break

    # Detect resistance zone from max ask
    maxAsk = float(window['ask'].max())
    maxAskIdx = window['ask'].idxmax()
    maxTime = df.loc[maxAskIdx]['timestamp']

    if lastResistance is None or abs(maxAsk - lastResistance) > minZoneGap:
        lastResistance = float(maxAsk)
        cur.execute("""
            INSERT INTO sr_zones (type, price, start_time, end_time)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, ('resistance', float(maxAsk), window.iloc[0]['timestamp'], window.iloc[-1]['timestamp']))
        resistanceId = cur.fetchone()[0]
        conn.commit()

        # Check for encounter
        future = df.iloc[i+windowSize:i+windowSize+confirmWindow]
        for j in range(len(future)):
            price = float(future.iloc[j]['ask'])
            if abs(price - maxAsk) <= encounterRange:
                futureSlice = future.iloc[j:j+10]
                rejection = futureSlice['ask'].min() < maxAsk - encounterRange
                breakout = futureSlice['ask'].max() > maxAsk + encounterRange
                outcome = 'reacted' if rejection else 'broken' if breakout else 'unclear'

                cur.execute("""
                    INSERT INTO sr_mob_events (zone_id, timestamp, outcome, confirmation_window_secs, price_at_touch, price_after)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    resistanceId,
                    future.iloc[j]['timestamp'],
                    outcome,
                    confirmWindow,
                    price,
                    float(futureSlice['ask'].iloc[-1])
                ))
                conn.commit()
                break

cur.close()
conn.close()
