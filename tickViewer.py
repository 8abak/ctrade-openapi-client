import streamlit as st

# ✅ ABSOLUTELY FIRST Streamlit command
st.set_page_config(layout="wide")

# ✅ Now safe to import everything else
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

# ✅ CSS to remove top padding and Streamlit header
st.markdown("""
    <style>
        .block-container {
            padding-top: 0rem;
        }
        header, .st-emotion-cache-z5fcl4 {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)

# DB setup
db_uri = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(db_uri)

# Streamlit config
st.markdown("<h1 style='margin-bottom: 0;'>📍 Pivot Viewer</h1>", unsafe_allow_html=True)

# Get total tick count
totalTicks = pd.read_sql("SELECT COUNT(*) FROM ticks", engine).iloc[0, 0]
defaultStart = max(0, totalTicks - 10000)
defaultEnd = totalTicks

# Only set default once when app first runs
if "tickRange" not in st.session_state or not st.session_state.get("tickSliderMoved", False):
    st.session_state.tickRange = (defaultStart, defaultEnd)

# Horizontal slider + Jump to Latest button
col1, col2 = st.columns([4, 1])
with col1:
    tickRange = st.slider("Tick Index Range", 0, totalTicks, st.session_state.tickRange, step=100, key="tickSlider")
with col2:
    if st.button("🔄"):
        st.session_state.tickRange = (max(0, totalTicks - 10000), totalTicks)
        st.rerun()

startTick, endTick = st.session_state.tickRange

# Sidebar checkbox layout
st.sidebar.subheader("🧩 Display Options")

# Tick display options
st.sidebar.markdown("**Tick**")
tickCols = st.sidebar.columns(2)
showTickChart = tickCols[0].checkbox("Chart", value=True, key="tickChart")
showTickTable = tickCols[1].checkbox("Table", value=False, key="tickTable")

# Pivot display options
st.sidebar.markdown("**Pivot**")
pivotCols = st.sidebar.columns(2)
showPivotChart = pivotCols[0].checkbox("Chart", value=True, key="pivotChart")
showPivotTable = pivotCols[1].checkbox("Table", value=False, key="pivotTable")

# Load ticks
queryTicks = f"""
    SELECT * FROM ticks
    ORDER BY timestamp
    OFFSET {startTick}
    LIMIT {endTick - startTick}
"""
df = pd.read_sql(queryTicks, engine)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['mid'] = (df['bid'] + df['ask']) / 2

# Load pivots
minTime = df['timestamp'].min()
maxTime = df['timestamp'].max()

pivots = pd.read_sql("""
    SELECT * FROM pivots
    WHERE timestamp BETWEEN %s AND %s
""", engine, params=(minTime, maxTime))
pivots['timestamp'] = pd.to_datetime(pivots['timestamp'])

# Plot chart
fig = go.Figure()

# Mid price (optional)
if showTickChart:
    dfMidThin = df.iloc[::5]
    fig.add_trace(go.Scatter(
        x=dfMidThin['timestamp'], y=dfMidThin['mid'], mode='lines',
        name='Mid Price', line=dict(color='black', width=1)
    ))

# Pivots
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

# Tables
if showTickTable:
    st.subheader("Tick Table")
    st.dataframe(df)

if showPivotTable:
    st.subheader("Pivot Table")
    st.dataframe(pivots)

engine.dispose()
