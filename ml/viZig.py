import pandas as pd
import psycopg2
import plotly.graph_objects as go
import streamlit as st

# === Load data ===
conn = psycopg2.connect(
    dbname="trading",
    user="babak",
    password="BB@bb33044",
    host="localhost",
    port=5432
)

st.set_page_config(layout="wide")
st.title("🔁 Zigzag Pivot Structure Viewer")

query = """
    SELECT timestamp, mid, pivot_type, zigzag_direction
    FROM pivotIdentification
    ORDER BY timestamp ASC
"""
df = pd.read_sql(query, conn)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# === Plot mid price ===
fig = go.Figure()
fig.add_trace(go.Scatter(x=df['timestamp'], y=df['mid'], mode='lines', name='Mid'))

# === Highlight pivots ===
pivot_highs = df[df['pivot_type'] == 'PIVOT_HIGH']
pivot_lows = df[df['pivot_type'] == 'PIVOT_LOW']

fig.add_trace(go.Scatter(
    x=pivot_highs['timestamp'],
    y=pivot_highs['mid'],
    mode='markers',
    name='PIVOT_HIGH',
    marker=dict(color='red', size=8, symbol='triangle-up')
))

fig.add_trace(go.Scatter(
    x=pivot_lows['timestamp'],
    y=pivot_lows['mid'],
    mode='markers',
    name='PIVOT_LOW',
    marker=dict(color='blue', size=8, symbol='triangle-down')
))

# === Show chart ===
fig.update_layout(height=600, margin=dict(l=20, r=20, t=40, b=20))
st.plotly_chart(fig, use_container_width=True)
