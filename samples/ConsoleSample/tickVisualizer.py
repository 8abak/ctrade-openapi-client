import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Live Tick Visualizer", layout="wide")

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, key="tickRefresh")

st.title("📈 Live Tick Stream")

try:
    df = pd.read_csv("ticks.csv")

    # Convert 'datetime' to datetime type
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Sort and select last 50 ticks
    df = df.sort_values(by='datetime')
    latestTicks = df.tail(50)

    st.write(f"Total ticks: {len(df)}")

    # Line chart with datetime as index
    st.line_chart(
        latestTicks.set_index("datetime")[["bid", "ask"]]
    )

except FileNotFoundError:
    st.warning("ticks.csv not found. Waiting for data...")
except Exception as e:
    st.error(f"Error reading ticks: {e}")
