import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

# Streamlit UI setup (must be first)
st.set_page_config(page_title="Gold Live Stream", layout="wide")
st_autorefresh(interval=1000, key="auto_refresh")

st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

# PostgreSQL connection configuration
conn = psycopg2.connect(
    dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432
)

# Fetch entire history so zooming out reveals more data
query = """
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY id
"""

df = pd.read_sql(query, conn)
conn.close()

if df.empty:
    st.warning("No tick data found.")
else:
    df = df.sort_values("timestamp")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid"))
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask"))

    # Default view: last 500 ticks when available
    start_idx = max(len(df) - 500, 0)

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=True,
        xaxis_range=[df["timestamp"].iloc[start_idx], df["timestamp"].iloc[-1]],
        height=600,
        uirevision="window",
    )

    st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})
