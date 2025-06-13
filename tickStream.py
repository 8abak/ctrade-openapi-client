import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from streamlit_plotly_events import plotly_events

# Streamlit UI setup (must be first)
st.set_page_config(page_title="Gold Live Stream", layout="wide")
st_autorefresh(interval=1000, key="auto_refresh")

st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

# PostgreSQL connection configuration
conn = psycopg2.connect(
    dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432
)

# Initialize session state to manage loaded range
if "start_index" not in st.session_state:
    st.session_state.start_index = -550  # Load 500 + 10% buffer initially

# Count total rows
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM ticks WHERE symbol = 'XAUUSD'")
    total_rows = cur.fetchone()[0]

# Query the window with a 10% buffer
window_size = 500
buffer = int(window_size * 0.1)
start = max(total_rows + st.session_state.start_index, 0)
limit = window_size + buffer

query = f"""
    SELECT timestamp, bid, ask
    FROM ticks
    WHERE symbol = 'XAUUSD'
    ORDER BY id
    OFFSET {start}
    LIMIT {limit}
"""

# Load and process data
df = pd.read_sql(query, conn)
conn.close()

if df.empty:
    st.warning("No tick data found.")
else:
    df = df.sort_values("timestamp")
    df["timestamp"] = df["timestamp"].dt.strftime("%H:%M:%S")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid"))
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask"))

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=True,
        height=600,
        uirevision="window",
    )

    config = {"scrollZoom": True}
    events = plotly_events(
        fig,
        events=["relayout"],
        config=config,
        key="tick_chart",
        override_height=600,
    )

    # When the user zooms out using the mouse wheel, attempt to load more data
    if events and any("range" in e for e in events):
        if st.session_state.start_index - window_size >= -total_rows:
            st.session_state.start_index -= window_size
        else:
            st.session_state.start_index = -total_rows
        st.experimental_rerun()
