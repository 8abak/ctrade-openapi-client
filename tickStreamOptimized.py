
import psycopg2
import pandas as pd
import streamlit as st
import plotly.graph_objs as go
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide")
st.title("📡 Live Tick Stream (XAUUSD)")
st.caption("Zoom or scroll left to load more data.")

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, key="tick_autorefresh")

# ------------------- Session State -------------------
if "windowSize" not in st.session_state:
    st.session_state.windowSize = 1000
if "offsetTimestamp" not in st.session_state:
    st.session_state.offsetTimestamp = None

# ------------------- DB Query -------------------
def fetchTicks(limit=1000, before=None):
    conn = psycopg2.connect(dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432)
    cur = conn.cursor()
    if before:
        query = '''
            SELECT * FROM ticks
            WHERE symbol = %s AND timestamp < %s
            ORDER BY timestamp DESC
            LIMIT %s
        '''
        cur.execute(query, ('XAUUSD', before, limit))
    else:
        query = '''
            SELECT * FROM ticks
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        '''
        cur.execute(query, ('XAUUSD', limit))
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['symbol', 'timestamp', 'bid', 'ask'])
    conn.close()
    return df.sort_values('timestamp')

# ------------------- Load Data -------------------
df = fetchTicks(limit=st.session_state.windowSize, before=st.session_state.offsetTimestamp)
if not df.empty:
    st.session_state.offsetTimestamp = df['timestamp'].min()

# ------------------- Plot Chart -------------------
fig = go.Figure()
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['ask'], mode='lines', name='Ask'))
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['bid'], mode='lines', name='Bid'))

fig.update_layout(
    hovermode="x unified",
    xaxis=dict(rangeslider=dict(visible=True)),
    template="plotly_dark"
)

st.plotly_chart(fig, use_container_width=True)

# ------------------- Load More Button -------------------
if st.button("⬅️ Load More History"):
    st.session_state.windowSize += 1000
    st.experimental_rerun()
