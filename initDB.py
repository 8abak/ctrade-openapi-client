# initDb.py

import psycopg2

conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb3304",
    host="localhost",
    port=5432
)

cur = conn.cursor()

createTable = """
CREATE TABLE IF NOT EXISTS ticks (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    bid DOUBLE PRECISION NOT NULL,
    ask DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION,
    CONSTRAINT unique_tick UNIQUE(symbol, timestamp)
);
"""

cur.execute(createTable)
conn.commit()
cur.close()
conn.close()

print("✅ ticks table created.")
