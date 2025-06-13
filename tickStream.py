# tickStream.py

import streamlit as st
st.set_page_config(layout="wide")  # MUST be the first Streamlit command

import psycopg2
import pandas as pd
import altair as alt
from streamlit_autorefresh import st_autorefresh

# Auto-refresh every 1 second
st_autorefresh(interval=1000, limit=None, key="db_autorefresh")

# Title and caption
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

# Query latest 100 rows
query = """
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp DESC
    LIMIT 100
"""
df = pd.read_sql(query, conn)
conn.close()

# Sort by ascending timestamp for plotting
df = df.sort_values("timestamp")
df["timestamp"] = df["timestamp"].dt.strftime('%H:%M:%S')

# Dynamically determine y-axis range
yMin = df[["bid", "ask"]].min().min() - 0.1
yMax = df[["bid", "ask"]].max().max() + 0.1

# Altair chart for live bid/ask line chart
chart = alt.Chart(df).transform_fold(
    ['bid', 'ask'], as_=['type', 'value']
).mark_line().encode(
    x=alt.X('timestamp:T', title='Time'),
    y=alt.Y('value:Q', title='Price', scale=alt.Scale(domain=[yMin, yMax])),
    color='type:N'
).properties(
    width=1000,
    height=400
)

st.altair_chart(chart, use_container_width=True)
