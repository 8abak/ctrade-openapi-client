#!/usr/bin/env python3

import pandas as pd
import time
import streamlit as st
import altair as alt

st.set_page_config(page_title="Live Tick Visualizer", layout="wide")

CSV_PATH = "ticks.csv"
REFRESH_INTERVAL = 5  # seconds

st.title("📈 Live Tick Data Viewer - XAUUSD")
st.markdown("Updates every 5 seconds.")

# Placeholder
placeholder = st.empty()

while True:
    try:
        df = pd.read_csv(CSV_PATH)
        df["datetime"] = pd.to_datetime(df["datetime"])

        latest_ticks = df.sort_values("timestamp", ascending=False).head(100)

        with placeholder.container():
            st.write("### 🔄 Last 100 Ticks")
            st.dataframe(latest_ticks, use_container_width=True)

            st.write("### 📊 Bid vs Ask Over Time")
            chart_data = latest_ticks.sort_values("datetime")
            line_chart = alt.Chart(chart_data).mark_line().encode(
                x="datetime:T",
                y=alt.Y("value:Q", title="Price"),
                color="type:N"
            ).transform_fold(
                ["bid", "ask"], as_=["type", "value"]
            ).properties(height=400)

            st.altair_chart(line_chart, use_container_width=True)

        time.sleep(REFRESH_INTERVAL)

    except Exception as e:
        st.error(f"Error reading or processing file: {e}")
        time.sleep(REFRESH_INTERVAL)
