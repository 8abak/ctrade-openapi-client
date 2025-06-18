import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from sqlalchemy import create_engine

# DB setup
db_uri = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(db_uri)

# Streamlit config
st.set_page_config(layout="wide")
st.title("📍 Pivot Viewer")

# Slider on top instead of sidebar inputs
maxIndex = 100000  # adjust based on your total tick count
tickRange = st.slider("Select Tick Range", 0, maxIndex, (0, 1000), step=100)
startTick, endTick = tickRange

# Sidebar checkboxes
st.sidebar.subheader("Pivot Display Options")
showPivotChart = st.sidebar.checkbox("Show Pivots in Chart", value=True)
showPivotTable = st.sidebar.checkbox("Show Pivots Table", value=False)
showTickTable = st.sidebar.checkbox("Show Ticks Table", value=False)

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

# Mid price
fig.add_trace(go.Scatter(
    x=df['timestamp'], y=df['mid'], mode='lines',
    name='Mid Price', line=dict(color='black')
))

# Pivots (conditionally show)
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
    title="Structured Pivots Only",
    margin=dict(l=20, r=0, t=40, b=20)
)
st.plotly_chart(fig, use_container_width=True)

# Tables
if showTickTable:
    st.subheader("Ticks Table")
    st.dataframe(df)

if showPivotTable:
    st.subheader("Pivots Table")
    st.dataframe(pivots)

engine.dispose()
