# tickStream.py

import streamlit as st
st.set_page_config(layout="wide")  # Must be first Streamlit command

import psycopg2
import pandas as pd
import altair as alt
from streamlit_autorefresh import st_autorefresh

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

query = """
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY timestamp DESC
    LIMIT 100
"""
df = pd.read_sql(query, conn)
conn.close()

# Sort and keep original datetime format
df = df.sort_values("timestamp")

# Altair needs long format
df_melted = df.melt(id_vars=["timestamp"], value_vars=["bid", "ask"], var_name="type", value_name="price")

# Calculate y-axis bounds
yMin = df_melted["price"].min() - 0.1
yMax = df_melted["price"].max() + 0.1

# Create Altair chart
chart = alt.Chart(df_melted).mark_line().encode(
    x=alt.X("timestamp:T", title="Time"),
    y=alt.Y("price:Q", title="Price", scale=alt.Scale(domain=[yMin, yMax])),
    color=alt.Color("type:N", title="Type")
).properties(
    width=1000,
    height=400
)

st.altair_chart(chart, use_container_width=True)
