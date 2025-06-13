import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

# Set page configuration
st.set_page_config(page_title="Live Tick Visualizer", layout="wide")

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, key="tick_autorefresh")

st.title("Live Tick Stream")

# Load and display the latest tick data
try:
    df = pd.read_csv("ticks.csv")
    st.write(f"Total ticks: {len(df)}")
    st.dataframe(df.tail(10))
except FileNotFoundError:
    st.warning("ticks.csv not found. Waiting for data...")
