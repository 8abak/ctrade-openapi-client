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

# Use plotly_events to detect zoom
zoom_event = plotly_events(
    fig,
    click_event=False,
    select_event=False,
    override_height=600,
    override_width="100%",
    key="zoom"
)

# ------------------ Detect Zoom Scroll to Left ------------------------
if zoom_event and isinstance(zoom_event[0], dict) and "xaxis.range[0]" in zoom_event[0]:
    zoomStartTime = pd.to_datetime(zoom_event[0]["xaxis.range[0]"])
    earliestTimestamp = df["timestamp"].min()

    if zoomStartTime <= earliestTimestamp + pd.Timedelta(minutes=2):
        st.session_state.windowSize = int(st.session_state.windowSize * 1.2)
        st.experimental_rerun()
