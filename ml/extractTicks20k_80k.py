import psycopg2
import pandas as pd

# Connect to the database
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)

# Query and load ticks
query = """
    SELECT *
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp
    OFFSET 20000
    LIMIT 60000
"""
df = pd.read_sql_query(query, conn)
conn.close()

# Save result
df.to_csv("ml/data/ticks20k_80k.csv", index=False)
print("✅ Extracted 20k–80k ticks from database and saved to ml/data/ticks20k_80k.csv")
