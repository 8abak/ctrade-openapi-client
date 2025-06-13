# tickStream.py

import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb3304",
    host="localhost",
    port=5432
)

st.set_page_config(layout="wide")
st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, limit=None, key="db_autorefresh")

query = """
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp DESC
    LIMIT 50
"""

df = pd.read_sql(query, conn)
df = df.sort_values("timestamp")

st.dataframe(df, use_container_width=True)
