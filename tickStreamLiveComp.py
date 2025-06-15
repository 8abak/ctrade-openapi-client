import streamlit as st
import json
import pandas as pd
import psycopg2
from streamlit_plotly_events import live_append_chart

# Fetch last N ticks from PostgreSQL
def fetch_recent_ticks(limit=100):
    conn = psycopg2.connect(dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432)
    df = pd.read_sql_query(
        f"SELECT timestamp, ask FROM ticks WHERE symbol = 'XAUUSD' ORDER BY timestamp DESC LIMIT {limit}",
        conn
    )
    conn.close()
    return df.sort_values("timestamp")

df = fetch_recent_ticks(100)
df.rename(columns={"ask": "price"}, inplace=True)

# Convert to Plotly format
data = [{
    "x": df["timestamp"].astype(str).tolist(),
    "y": df["price"].tolist(),
    "type": "scatter",
    "mode": "lines",
    "name": "XAUUSD Ask"
}]

# Render
live_append_chart(chart_data=json.dumps(data), override_height=600)
