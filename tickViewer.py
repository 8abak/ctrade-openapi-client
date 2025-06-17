import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from sqlalchemy import create_engine

# Streamlit setup
st.set_page_config(layout="wide")
st.title("📊 Support/Resistance MOB Viewer")

# Database connection via SQLAlchemy
db_uri = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"  # ← or use %40 if needed
engine = create_engine(db_uri)

# User inputs
st.sidebar.header("Display Options")
startTick = st.sidebar.number_input("Start Tick Index", min_value=0, value=0, step=100)
endTick = st.sidebar.number_input("End Tick Index", min_value=startTick + 1, value=startTick + 1000, step=100)

# Load ticks
queryTicks = f"""
    SELECT * FROM ticks
    ORDER BY timestamp
    OFFSET {startTick}
    LIMIT {endTick - startTick}
"""
df = pd.read_sql(queryTicks, engine)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Load support/resistance zones
minTime = df['timestamp'].min()
maxTime = df['timestamp'].max()

zones = pd.read_sql("""
    SELECT * FROM sr_zones
    WHERE start_time <= %s AND end_time >= %s
""", engine, params=(maxTime, minTime))

# Load events
events = pd.read_sql("""
    SELECT e.*, z.type AS zone_type, z.price AS zone_price
    FROM sr_mob_events e
    JOIN sr_zones z ON e.zone_id = z.id
    WHERE e.timestamp BETWEEN %s AND %s
""", engine, params=(minTime, maxTime))
events['timestamp'] = pd.to_datetime(events['timestamp'])

# Plot setup
fig = go.Figure()

# Mid price
fig.add_trace(go.Scatter(
    x=df['timestamp'], y=df['mid'], mode='lines',
    name='Mid Price', line=dict(color='black')
))

# Zones with start and break-end logic
for _, zone in zones.iterrows():
    color = 'green' if zone['type'] == 'support' else 'red'
    broken_events = events[(events['zone_id'] == zone['id']) & (events['outcome'] == 'broken')]
    endTime = broken_events['timestamp'].min() if not broken_events.empty else maxTime

    fig.add_trace(go.Scatter(
        x=[zone['start_time'], endTime],
        y=[zone['price'], zone['price']],
        mode='lines',
        name=f"{zone['type'].capitalize()} @ {zone['price']:.2f}",
        line=dict(color=color, dash='dot')
    ))

# Event markers
color_map = {'reacted': 'blue', 'broken': 'orange', 'unclear': 'gray'}
for _, row in events.iterrows():
    fig.add_trace(go.Scatter(
        x=[row['timestamp']], y=[row['price_at_touch']],
        mode='markers+text', name=row['outcome'],
        marker=dict(size=10, color=color_map.get(row['outcome'], 'black'), symbol='x'),
        text=[row['outcome']], textposition='top center'
    ))

fig.update_layout(height=600, title="Tick Stream with S/R Zones and MOB Events")
st.plotly_chart(fig, use_container_width=True)

# Optional tables
st.sidebar.markdown("---")
if st.sidebar.checkbox("Show Ticks Table"):
    st.dataframe(df)

if st.sidebar.checkbox("Show S/R Zones Table"):
    st.dataframe(zones)

if st.sidebar.checkbox("Show MOB Events Table"):
    st.dataframe(events)

engine.dispose()
