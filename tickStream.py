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
import altair as alt
# Create Altair line chart
yMin = df[["bid", "ask"]].min().min() - 0.1
yMax = df[["bid", "ask"]].max().max() + 0.1

chart = alt.Chart(df).transform_fold(
    ['bid', 'ask'], as +['type', 'value'    ]
).mark_line().encode(
    x='timestamp:T',
    y=alt.Y('value:Q',scale=alt.Scale(domain=[yMin, yMax])),
    color='type:N',
).properties(width=800, height=400)


# Display line chart
st.altair_chart(chart, use_container_width=True)

