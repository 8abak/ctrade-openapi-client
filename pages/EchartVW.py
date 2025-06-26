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
    ORDER BY timestamp DESC
    LIMIT 2000
"""
df = pd.read_sql(query, engine)
df = df.sort_values(by="timestamp").reset_index(drop=True)
df["mid"] = ((df["bid"] + df["ask"]) / 2).round(2)

# Convert timestamps to Sydney timezone
sydney = pytz.timezone("Australia/Sydney")
df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC").dt.tz_convert(sydney).dt.strftime("%Y-%m-%d %H:%M:%S")

tick_data = df[["timestamp", "mid"]].copy()
tick_data["tick_index"] = tick_data.index

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
tick_series = [[i, row['mid']] for i, row in tick_data.iterrows()]
tooltips = [
    f"{row['mid']:.2f}<br/>{row['timestamp']}"
    for _, row in tick_data.iterrows()
]

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
        "formatter": {
            "function": """
                function (params) {
                    const idx = params[0].dataIndex;
                    const val = params[0].value[1].toFixed(2);
                    const ts = tickTimestamps[idx];
                    return `Tick ${idx}<br/>${ts}<br/>Price: ${val}`;
                }
            """
        }
    },
    "dataZoom": [
        {"type": "inside"},
        {"type": "slider"}
    ],
    "grid": [
        {"left": "5%", "right": "5%", "height": "60%"},
        {"left": "5%", "right": "5%", "top": "75%", "height": "20%"}
    ],
    "xAxis": [
        {"type": "value", "gridIndex": 0, "name": "Tick"},
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

# --- Inject JS timestamps array ---
tick_timestamps_js = "const tickTimestamps = [" + ",".join([f'\"{ts}\"' for ts in tick_data['timestamp']]) + "];"
st.components.v1.html(f"<script>{tick_timestamps_js}</script>", height=0)

# --- Render Chart ---
st_echarts(options=echart_options, height="700px")
