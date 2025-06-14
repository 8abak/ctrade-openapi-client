import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objs as go
from streamlit_autorefresh import st_autorefresh
from streamlit_js_eval import streamlit_js_eval

# ---------------------- UI SETUP ----------------------
st.set_page_config(layout="wide")
st.title("📡 Live Tick Stream from PostgreSQL")
st.caption("📉 Zoom out with your mouse to automatically load more XAUUSD data.")

# Refresh every second
st_autorefresh(interval=2000, key="tick_autorefresh")

# ------------------ Session State Init ----------------
if "windowSize" not in st.session_state:
    st.session_state.windowSize = 500

# ------------------ Fetch Data ------------------------
def fetchTicks(limit=5000):
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

df = fetchTicks(limit=st.session_state.windowSize)

# ------------------ Plot Chart ------------------------
fig = go.Figure()
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bid"], mode="lines", name="bid", line=dict(color="blue")))
fig.add_trace(go.Scatter(x=df["timestamp"], y=df["ask"], mode="lines", name="ask", line=dict(color="red")))

fig.update_layout(
    xaxis_title="Time",
    yaxis_title="Price",
    xaxis=dict(type="date", rangeslider_visible=True),
    uirevision="constant"
)

st.plotly_chart(fig, use_container_width=True)

# ------------------ Inject JS for Zoom Detection ------------------------
st.markdown("""
<script>
document.addEventListener("DOMContentLoaded", function () {
    const plotlyChart = document.querySelector('[id^="root"] .js-plotly-plot');
    if (plotlyChart && !plotlyChart.zoomHooked) {
        plotlyChart.on('plotly_relayout', function(eventData) {
            window.PlotlyZoomRange = eventData;
        });
        plotlyChart.zoomHooked = true;
    }
});
</script>
""", unsafe_allow_html=True)

# ------------------ Check for Zoom & Rerun ------------------------
zoom_event = streamlit_js_eval(js_expressions="window.PlotlyZoomRange", key="zoomEval")

if isinstance(zoom_event, dict) and "xaxis.range[0]" in zoom_event:
    st.session_state.windowSize = int(st.session_state.windowSize * 1.2)
    st.experimental_rerun()
