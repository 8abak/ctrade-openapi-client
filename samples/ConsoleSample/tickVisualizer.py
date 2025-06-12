#!/usr/bin/env python3

import json
import os
import csv
from datetime import datetime
import streamlit as st
import pandas as pd

csvFile = "ticks.csv"

# UI
st.set_page_config(layout="wide")
st.title("📈 Live Tick Data Stream for XAUUSD")
st.caption("Streaming from ticks.csv and updating in real time.")
placeholder = st.empty()

@st.cache_data(ttl=0.0, show_spinner=False)
def load_data():
    if os.path.exists(csvFile):
        return pd.read_csv(csvFile)
    return pd.DataFrame(columns=["timestamp", "datetime", "symbolId", "bid", "ask"])

# Auto-refresh
while True:
    df = load_data()
    placeholder.dataframe(df.tail(30), use_container_width=True)
    st.experimental_rerun()
