# tickStream.py

import psycopg2
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go

st.set_page_config(page_title="Gold Live Stream", layout="wide")
st_autorefresh(interval=1000, limit=None, key="refresh")

st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

numTicks = st.slider("Number of ticks to display", min_value=100, max_value=2000, value=100, step=100)

# Connect to database
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

# Create Plotly chart
fig = go.Figure()

fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid"))
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask"))

fig.update_layout(
    title="Live XAUUSD Tick Chart",
    xaxis_title="Time",
    yaxis_title="Price",
    xaxis_rangeslider_visible=True,
    template="plotly_white",
    height=500
)

st.plotly_chart(fig, use_container_width=True)
