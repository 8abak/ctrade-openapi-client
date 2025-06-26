import streamlit as st
import pandas as pd
from streamlit_echarts import st_echarts
from sqlalchemy import create_engine
import pytz

# --- Page config ---
st.set_page_config(layout="wide")

# --- Database connection ---
db_uri = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(db_uri)

# --- Load real tick data ---
query = """
    SELECT timestamp, bid, ask
    FROM ticks
    ORDER BY timestamp ASC
"""
df = pd.read_sql(query, engine)
df["mid"] = ((df["bid"] + df["ask"]) / 2).round(2)

# Convert timestamps to Sydney timezone
sydney = pytz.timezone("Australia/Sydney")
df["timestamp"] = pd.to_datetime(df["timestamp"])
if df["timestamp"].dt.tz is None:
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
df["timestamp"] = df["timestamp"].dt.tz_convert(sydney).dt.strftime("%Y-%m-%d %H:%M:%S")

tick_data = df[["timestamp", "mid"]].copy()

# Simulated market depth (Level 2) data
last_price = tick_data['mid'].iloc[-1]
ask_depth = pd.DataFrame({
    "price": [round(last_price + i * 0.1, 2) for i in range(1, 11)],
    "volume": [v for v in range(2, 12)],
    "side": ["ask"] * 10
})
bid_depth = pd.DataFrame({
    "price": [round(last_price - i * 0.1, 2) for i in range(1, 11)],
    "volume": [v for v in range(10, 0, -1)],
    "side": ["bid"] * 10
})
depth_df = pd.concat([ask_depth, bid_depth], ignore_index=True)

# --- Prepare Tick Series ---
tick_series = [[row['timestamp'], row['mid']] for _, row in tick_data.iterrows()]

# --- Prepare Depth Bars (plotted at the end of chart range) ---
depth_series = [
    {
        "name": f"{row['side'].capitalize()} @{row['price']}",
        "type": "bar",
        "xAxisIndex": 1,
        "yAxisIndex": 1,
        "data": [row['volume'] if row['side'] == 'bid' else -row['volume']],
        "barWidth": '40%',
        "itemStyle": {
            "color": "#ffa07a" if row['side'] == 'ask' else "#90ee90"
        },
        "stack": row['side']
    }
    for _, row in depth_df.iterrows()
]

# --- Chart Options ---
echart_options = {
    "tooltip": {
        "trigger": "axis",
        "formatter": "function(params) { return `${params[0].axisValue}<br/>Price: ${params[0].data[1].toFixed(2)}`; }"
    },
    "dataZoom": [
        {"type": "inside", "xAxisIndex": [0, 1]},
        {"type": "slider", "xAxisIndex": [0, 1], "bottom": 10, "height": 40}
    ],
    "grid": [
        {"left": "5%", "right": "5%", "height": "60%"},
        {"left": "5%", "right": "5%", "top": "75%", "height": "20%"}
    ],
    "xAxis": [
        {"type": "category", "gridIndex": 0, "name": "Timestamp", "data": tick_data['timestamp'].tolist()},
        {"type": "category", "gridIndex": 1, "data": [str(p) for p in depth_df['price']]}
    ],
    "yAxis": [
        {"type": "value", "scale": True, "gridIndex": 0, "name": "Price"},
        {"type": "value", "gridIndex": 1, "name": "Depth Volume"}
    ],
    "series": [
        {
            "name": "Mid Price",
            "type": "line",
            "symbol": "circle",
            "symbolSize": 3,
            "showSymbol": False,
            "data": tick_series,
            "lineStyle": {"width": 1},
            "xAxisIndex": 0,
            "yAxisIndex": 0
        }
    ] + depth_series
}

# --- Render Chart ---
st_echarts(options=echart_options, height="700px")
