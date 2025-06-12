#!/usr/bin/env python3

import os
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

csvFile = "ticks.csv"

# Set page layout
st.set_page_config(layout="wide")
st.title("📈 Live Tick Data Stream for GOLD")
st.caption("Streaming from ticks.csv and updating in real time.")

# Auto-refresh every 1000 ms (1 second)
st_autorefresh(interval=1000, limit=None, key="tick_autorefresh")

# Load data
if os.path.exists(csvFile):
    df = pd.read_csv(csvFile)
    # Use the modern Streamlit data editor API
    st.data_editor(df.tail(30), use_container_width=True)
else:
    st.warning("No data yet. Please wait for tick data to be generated.")
