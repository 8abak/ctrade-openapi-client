# tickStream.py

import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ✅ Set page config FIRST
st.set_page_config(layout="wide", page_title="Gold Live Stream")


# Auto-refresh every 1 second
st_autorefresh(interval=1000, limit=None, key="db_autorefresh")

st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")


# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)

# Query last 100 ticks
query = """
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp DESC
    LIMIT 100
"""
df = pd.read_sql(query, conn)
conn.close()

# Sort by ascending time for charting
df = df.sort_values("timestamp")

# Optional: convert timestamp to shorter strings
df["timestamp"] = df["timestamp"].dt.strftime('%H:%M:%S')

# Display line chart
st.line_chart(df.set_index("timestamp")[["bid", "ask"]])
