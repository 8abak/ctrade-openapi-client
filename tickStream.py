import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from streamlit_plotly_events import plotly_events

# The chart uses Plotly events to detect zooming. The dependency is optional so
# the app still works even if it's not installed.
try:
    from streamlit_plotly_events import plotly_events
except ModuleNotFoundError:
    plotly_events = None

# Streamlit UI setup (must be first)
st.set_page_config(page_title="Gold Live Stream", layout="wide")
st_autorefresh(interval=1000, key="auto_refresh")

st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("Streaming XAUUSD data directly from database")

# PostgreSQL connection configuration
conn = psycopg2.connect(
    dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432
)

# Manage session state for how many rows to load
WINDOW = 500
if "rows_loaded" not in st.session_state:
    st.session_state.rows_loaded = WINDOW

# Determine total number of rows
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM ticks WHERE symbol = 'XAUUSD'")
    total_rows = cur.fetchone()[0]


def load_rows(num_rows: int) -> pd.DataFrame:
    start = max(total_rows - num_rows, 0)
    query = f"""
        SELECT timestamp, bid, ask
        FROM ticks
        WHERE symbol = 'XAUUSD'
        ORDER BY id
        OFFSET {start}
        LIMIT {num_rows}
    """
    return pd.read_sql(query, conn)


# Load current window
df = load_rows(st.session_state.rows_loaded)
conn.close()

if df.empty:
    st.warning("No tick data found.")
else:
    df = df.sort_values("timestamp")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid"))
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask"))

    # Show the last WINDOW ticks by default
    start_idx = max(len(df) - WINDOW, 0)

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=True,
        xaxis_range=[df["timestamp"].iloc[start_idx], df["timestamp"].iloc[-1]],
        height=600,
        uirevision="stream",
    )

    if plotly_events:
        events = plotly_events(
            fig,
            events=["relayout"],
            key="tick_chart",
            config={"scrollZoom": True},
        )
        if plotly_events:
            events = plotly_events(
                fig,
                events=["relayout"],
                key="tick_chart",
                config={"scrollZoom": True},
            )
            if events and "xaxis.range[0]" in events[0]:
                # When the visible range starts before our loaded data, double the
                # number of rows fetched (up to the total available) and rerun the
                # app so the plot expands accordingly.
                range_start = pd.to_datetime(events[0]["xaxis.range[0]"])
                earliest = df["timestamp"].iloc[0]
                if range_start < earliest and st.session_state.rows_loaded < total_rows:
                    st.session_state.rows_loaded = min(
                        st.session_state.rows_loaded * 2, total_rows
                    )
                    st.experimental_rerun()
            st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})
        else:
            st.warning("streamlit-plotly-events not installed, zoom-based loading disabled")
            st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})
