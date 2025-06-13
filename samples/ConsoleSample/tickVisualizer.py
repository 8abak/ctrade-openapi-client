import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, key="tickRefresh")

# Load and show latest tick data
st.title("📈 Live Tick Stream")

try:
    df = pd.read_csv("ticks.csv")
    
    # Ensure timestamp is datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')

    st.write(f"Total ticks: {len(df)}")
    
    # Select last N points for a smoother chart
    latestTicks = df.tail(50)

    # Stream chart
    st.line_chart(
        latestTicks.set_index("timestamp")[["bid", "ask"]]
    )

except FileNotFoundError:
    st.warning("ticks.csv not found. Waiting for data...")
except Exception as e:
    st.error(f"Error reading ticks: {e}")
