#!/usr/bin/env python3

import json
import os
import csv
from datetime import datetime
import streamlit as st
import pandas as pd
import time

csvFile = "ticks.csv"

# UI
st.set_page_config(layout="wide")
st.title("📈 Live Tick Data Stream for XAUUSD")
st.caption("Streaming from ticks.csv and updating in real time.")
placeholder = st.empty()

@st.cache_data(ttl=1.0, show_spinner=False)
def load_data():
    if os.path.exists(csvFile):
        return pd.read_csv(csvFile)
    return pd.DataFrame(columns=["timestamp", "datetime", "symbolId", "bid", "ask"])

# Auto-refresh using Streamlit's built-in rerun after a delay
while True:
    df = load_data()
    placeholder.dataframe(df.tail(30), use_container_width=True)
    time.sleep(1)
    st._rerun()
