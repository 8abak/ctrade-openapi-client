import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objs as go
from streamlit_autorefresh import st_autorefresh
from streamlit_js_eval import streamlit_js_eval  # ✅ Main addition

# Setup
st.set_page_config(layout="wide")
st.title("📈 Live Tick Stream from PostgreSQL")
st.caption("Zoom out on chart to load more data automatically.")

# Autorefresh every second
st_autorefresh(interval=1000, key="tick_autorefresh")

# PostgreSQL connection
def fetchTicks(limit=500):
    conn = psycopg2.connect(
        dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432
    )
    query = f"""
        SELECT * FROM ticks
        WHERE symbol = 'XAUUSD'
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df.sort_values("timestamp")

# Setup window size
if "windowSize" not in st.session_state:
    st.session_state.windowSize = 500

# Fetch data
df = fetchTicks(limit=st.session_state.windowSize)

# Build plotly figure
fig = go.Figure()
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid", line=dict(color="blue")))
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask", line=dict(color="red")))

fig.update_layout(xaxis_title="Time", yaxis_title="Price", uirevision=True)

# Inject zoom detector
zoom_event = streamlit_js_eval(
    js_expressions="window.PlotlyZoomRange", key="zoomEval"
)

# Show chart
st.plotly_chart(fig, use_container_width=True)

# Check if zoom occurred (based on custom JS)
if isinstance(zoom_event, dict):
    if "xaxis.range[0]" in zoom_event:
        # Detected zoom → load more data
        st.session_state.windowSize = int(st.session_state.windowSize * 1.2)
        st.experimental_rerun()
