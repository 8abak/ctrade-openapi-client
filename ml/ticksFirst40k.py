import psycopg2
import pandas as pd

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)

# Query to fetch the first 40,000 ticks for XAUUSD
query = """
    SELECT timestamp, bid, ask
    from ticks
    WHERE symbol = 'XAUUSD'
    order by timestamp asc
    LIMIT 40000
"""
df=pd.read_sql_query(query, conn)
conn.close()

# Add mid price column
df['mid'] = (df['bid'] + df['ask']) / 2

# save for analysis
df.to_csv("first40kTicks.csv", index=False)
print("First 40,000 ticks saved to first40kTicks.csv")