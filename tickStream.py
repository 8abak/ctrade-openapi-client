import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objs as go
from streamlit_plotly_events import plotly_events
from streamlit_autorefresh import st_autorefresh

# ---------------------- UI SETUP ----------------------
st.set_page_config(layout="wide")
st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("📉 Scroll left or zoom out to load more XAUUSD data.")

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

# Convert bid/ask to numeric
df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
df.dropna(subset=["bid", "ask"], inplace=True)

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

selected_events = plotly_events(
    fig,
    events=["relayout"],
    override_plotly_events=True,
    config={"scrollZoom": True},
    override_height=600,
    override_width="100%",
    key="zoom"
)

# ------------------ Detect Scroll Left ------------------------
if selected_events and isinstance(selected_events[0], dict):
    event = selected_events[0]
    if "xaxis.range[0]" in event:
        zoomStartTime = pd.to_datetime(event["xaxis.range[0]"])
        earliestTimestamp = df["timestamp"].min()
        if zoomStartTime <= earliestTimestamp + pd.Timedelta(minutes=2):
            st.session_state.windowSize = int(st.session_state.windowSize * 1.2)
            st.experimental_rerun()
