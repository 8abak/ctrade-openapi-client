# tickStream.py

import psycopg2
import pandas as pd
import streamlit as st
import altair as alt
from streamlit_autorefresh import st_autorefresh

# Auto-refresh every 1 second
st_autorefresh(interval=1000, limit=None, key="db_autorefresh")

# Streamlit layout
st.set_page_config(layout="wide")
st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)

# Query the latest 100 ticks
query = """
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp DESC
    LIMIT 100
"""
df = pd.read_sql(query, conn)
conn.close()

# Sort in ascending time for clean chart
df = df.sort_values("timestamp")

# Format time for x-axis readability
df["timestamp"] = df["timestamp"].dt.strftime('%H:%M:%S')

# Calculate dynamic y-axis limits with slight padding
yMin = df[["bid", "ask"]].min().min() - 0.1
yMax = df[["bid", "ask"]].max().max() + 0.1

# Build Altair chart for full axis control
chart = alt.Chart(df).transform_fold(
    ['bid', 'ask'], as_=['type', 'value']
).mark_line().encode(
    x='timestamp:T',
    y=alt.Y('value:Q', scale=alt.Scale(domain=[yMin, yMax])),
    color='type:N'
).properties(
    width=1000,
    height=400
)

# Show chart
st.altair_chart(chart, use_container_width=True)
