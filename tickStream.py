# tickStream.py

import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import altair as alt

# This must be the very first Streamlit command
st.set_page_config(page_title="Gold Live Stream", layout="wide")

# Auto-refresh every 1 second
st_autorefresh(interval=1000, limit=None, key="db_autorefresh")

st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

# ✅ Allow user to select how many ticks to load
numTicks = st.slider("How many recent ticks to display?", min_value=100, max_value=2000, value=100, step=100)

# PostgreSQL connection
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
    LIMIT %s
"""
df = pd.read_sql(query, conn, params=(numTicks,))
conn.close()

df = df.sort_values("timestamp")
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["timestamp"] = df["timestamp"].dt.strftime('%H:%M:%S')

# Melt for Altair (so bid and ask are separate lines)
df_melted = df.melt(id_vars="timestamp", value_vars=["bid", "ask"], var_name="type", value_name="price")

# ✅ Interactive zoom selection
zoom = alt.selection_interval(bind='scales', encodings=["x", "y"])

chart = alt.Chart(df_melted).mark_line().encode(
    x=alt.X("timestamp:T", title="Time"),
    y=alt.Y("price:Q", title="Price"),
    color=alt.Color("type:N", title="Type")
).properties(
    width=1000,
    height=400
).add_selection(
    zoom
)

st.altair_chart(chart, use_container_width=True)
