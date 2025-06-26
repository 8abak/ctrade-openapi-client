import streamlit as st
import pandas as pd
from streamlit_echarts import st_echarts

# --- Page config ---
st.set_page_config(layout="wide")

# --- Simulated Data ---
num_ticks = 2000
mid_prices = pd.Series([2350 + (i * 0.01) + (0.5 - i % 10) * 0.1 for i in range(num_ticks)])
tick_data = pd.DataFrame({
    "tick_index": list(range(num_ticks)),
    "mid": mid_prices.round(2)
})

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
tick_series = [[i, p] for i, p in enumerate(tick_data['mid'])]

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
    "tooltip": {"trigger": "axis"},
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

# --- Render Chart ---
st_echarts(options=echart_options, height="700px")
