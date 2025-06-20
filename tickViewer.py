import streamlit as st

# ✅ Must be first Streamlit command
st.set_page_config(layout="wide")

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

# ✅ Hide header and spacing
st.markdown("""
    <style>
        .block-container { padding-top: 0rem; }
        header, .st-emotion-cache-z5fcl4 { display: none; }
        button[kind="secondary"] {
            padding: 0.25rem 0.5rem;
            font-size: 0.75rem;
        }
    </style>
""", unsafe_allow_html=True)

# --- Database ---
db_uri = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(db_uri)

# --- Get tick count ---
totalTicks = pd.read_sql("SELECT COUNT(*) FROM ticks", engine).iloc[0, 0]
defaultStart = max(0, totalTicks - 10000)
defaultEnd = totalTicks

# --- Init session state ---
if "tickRange" not in st.session_state:
    st.session_state.tickRange = (defaultStart, defaultEnd)
if "tickSliderMoved" not in st.session_state:
    st.session_state.tickSliderMoved = False

# --- Zoom & Shift Buttons ---
st.markdown("#### Navigation")

navCols = st.columns([1, 1, 1, 1, 1])
with navCols[0]:
    if st.button("⏪ -100"):
        s, e = st.session_state.tickRange
        s = max(0, s - 100)
        e = max(s + 1, e - 100)
        st.session_state.tickRange = (s, e)
        st.rerun()
with navCols[1]:
    if st.button("🔍 In"):
        s, e = st.session_state.tickRange
        c = (s + e) // 2
        w = max(100, (e - s) // 2)
        s = max(0, c - w // 2)
        e = min(totalTicks, s + w)
        st.session_state.tickRange = (s, e)
        st.rerun()
with navCols[2]:
    if st.button("🔍 Out"):
        s, e = st.session_state.tickRange
        c = (s + e) // 2
        w = min(totalTicks, (e - s) * 2)
        s = max(0, c - w // 2)
        e = min(totalTicks, s + w)
        st.session_state.tickRange = (s, e)
        st.rerun()
with navCols[3]:
    if st.button("+100 ⏩"):
        s, e = st.session_state.tickRange
        e = min(totalTicks, e + 100)
        s = max(0, e - (e - s))
        st.session_state.tickRange = (s, e)
        st.rerun()
with navCols[4]:
    if st.button("🔄 Reset"):
        st.session_state.tickRange = (max(0, totalTicks - 10000), totalTicks)
        st.session_state.tickSliderMoved = False
        st.rerun()

# --- Slider ---
tickRange = st.slider(
    "Tick Range....",
    0, totalTicks,
    st.session_state.tickRange,
    step=1000,
    key="tickSlider"
)
if tickRange != st.session_state.tickRange:
    st.session_state.tickRange = tickRange
    st.session_state.tickSliderMoved = True

startTick, endTick = st.session_state.tickRange

# --- Sidebar Options ---
st.sidebar.subheader("🧩 Display Options")
st.sidebar.markdown("**Tick**")
tickCols = st.sidebar.columns(2)
showTickChart = tickCols[0].checkbox("Chart", value=True, key="tickChart")
showTickTable = tickCols[1].checkbox("Table", value=False, key="tickTable")

st.sidebar.markdown("**Pivot**")
pivotCols = st.sidebar.columns(2)
showPivotChart = pivotCols[0].checkbox("Chart", value=True, key="pivotChart")
showPivotTable = pivotCols[1].checkbox("Table", value=False, key="pivotTable")

# --- Load Ticks ---
queryTicks = f"""
    SELECT * FROM ticks
    ORDER BY timestamp
    OFFSET {startTick}
    LIMIT {endTick - startTick}
"""
df = pd.read_sql(queryTicks, engine)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['mid'] = (df['bid'] + df['ask']) / 2

# --- Load Pivots ---
minTime = df['timestamp'].min()
maxTime = df['timestamp'].max()
pivots = pd.read_sql("""
    SELECT * FROM pivots
    WHERE timestamp BETWEEN %s AND %s
""", engine, params=(minTime, maxTime))
pivots['timestamp'] = pd.to_datetime(pivots['timestamp'])

# --- Chart ---
fig = go.Figure()

if showTickChart:
    dfMidThin = df.iloc[::5]
    fig.add_trace(go.Scatter(
        x=dfMidThin['timestamp'], y=dfMidThin['mid'],
        mode='lines', name='Mid Price', line=dict(color='black', width=1)
    ))

if showPivotChart:
    for _, p in pivots.iterrows():
        marker = 'triangle-up' if p['pivot_type'] == 'high' else 'triangle-down'
        color = 'blue' if p['pivot_type'] == 'high' else 'orange'
        fig.add_trace(go.Scatter(
            x=[p['timestamp']], y=[p['price']],
            mode='markers+text',
            marker=dict(symbol=marker, color=color, size=12),
            text=[p['pivot_type']],
            textposition='top center',
            name=f"{p['pivot_type']} @ {p['price']:.2f}"
        ))

fig.update_layout(
    height=600,
    margin=dict(l=20, r=0, t=0, b=20),
    showlegend=False
)
st.plotly_chart(fig, use_container_width=True)

# --- Tables ---
if showTickTable:
    st.subheader("Tick Table")
    st.dataframe(df)

if showPivotTable:
    st.subheader("Pivot Table")
    st.dataframe(pivots)

engine.dispose()
