import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objs as go
from streamlit_autorefresh import st_autorefresh

# ---------------------- UI SETUP ----------------------
st.set_page_config(layout="wide")
st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("📉 Scroll left to load more XAUUSD data automatically.")

# Refresh every 2 seconds
st_autorefresh(interval=2000, key="tick_autorefresh")

# ------------------ Session State Init ----------------
if "windowSize" not in st.session_state:
    st.session_state.windowSize = 5000

# ------------------ Fetch Data ------------------------
def fetchTicks(limit):
    conn = psycopg2.connect(
        dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432
    )
    query = f"""
        SELECT * FROM public.ticks
        WHERE symbol = 'XAUUSD'
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df.sort_values("timestamp")

df = fetchTicks(st.session_state.windowSize)

# ------------------ Plot Chart ------------------------
fig = go.Figure()
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid", line=dict(color="blue")))
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask", line=dict(color="red")))

fig.update_layout(
    xaxis_title="Time",
    yaxis_title="Price",
    xaxis=dict(type="date", rangeslider_visible=True),
    uirevision="keep"
)

st.plotly_chart(fig, use_container_width=True)
