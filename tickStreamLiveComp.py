import streamlit as st
import json
import pandas as pd
from datetime import datetime
from streamlit_plotly_events import live_append_chart

# Simulated initial data (replace with DB fetch if needed)
df = pd.DataFrame({
    "timestamp": [datetime.utcnow()],
    "price": [2350.00]
})

# Convert to Plotly-compatible JSON
data = [{
    "x": df["timestamp"].astype(str).tolist(),
    "y": df["price"].tolist(),
    "type": "scatter",
    "mode": "lines",
    "name": "XAUUSD Ask"
}]

# Render the chart using your custom component
live_append_chart(chart_data=json.dumps(data), override_height=600)
