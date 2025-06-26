import streamlit as st
import pandas as pd
from streamlit_echarts import st_echarts, JsCode
from sqlalchemy import create_engine
import pytz

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Live Tick Chart", initial_sidebar_state="collapsed")

# --- DB Config ---
db_uri = "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
engine = create_engine(db_uri)

# --- Constants ---
CHUNK_SIZE = 2000
VISIBLE_WINDOW = 1800
AUTO_REFRESH_SEC = 3  # Polling every few seconds

# --- Get Current Total Ticks ---
totalTicks = pd.read_sql("SELECT COUNT(*) FROM ticks", engine).iloc[0, 0]

# --- Session State Initialization ---
if "offset" not in st.session_state:
    st.session_state.offset = max(0, totalTicks - CHUNK_SIZE)
if "lastMaxTimestamp" not in st.session_state:
    st.session_state.lastMaxTimestamp = None

# --- Auto Refresh every X seconds ---
st.experimental_rerun = st.experimental_rerun if st.session_state.get("autoRefresh") else None
st_autorefresh(interval=AUTO_REFRESH_SEC * 1000, key="chartRefresh")

# --- Load Chunk ---
offset = st.session_state.offset
query = f"""
    SELECT timestamp, bid, ask
    FROM ticks
    ORDER BY timestamp ASC
    OFFSET {offset}
    LIMIT {CHUNK_SIZE}
"""
df = pd.read_sql(query, engine)
df["mid"] = ((df["bid"] + df["ask"]) / 2).round(2)

# --- Timezone Handling ---
sydney = pytz.timezone("Australia/Sydney")
df["timestamp"] = pd.to_datetime(df["timestamp"])
if df["timestamp"].dt.tz is None:
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
df["timestamp"] = df["timestamp"].dt.tz_convert(sydney).dt.strftime("%Y-%m-%d %H:%M:%S")

# --- Keep Visible Portion ---
df = df.tail(VISIBLE_WINDOW).reset_index(drop=True)
tickData = df[["timestamp", "mid"]]

# --- Auto-scroll right when new data is added ---
newMax = df["timestamp"].iloc[-1]
if st.session_state.lastMaxTimestamp and newMax > st.session_state.lastMaxTimestamp:
    scrollToRight = True
else:
    scrollToRight = False
st.session_state.lastMaxTimestamp = newMax

# --- Level 2 (Simulated) ---
lastPrice = tickData['mid'].iloc[-1]
askDepth = pd.DataFrame({
    "price": [round(lastPrice + i * 0.1, 2) for i in range(1, 11)],
    "volume": [v for v in range(2, 12)],
    "side": ["ask"] * 10
})
bidDepth = pd.DataFrame({
    "price": [round(lastPrice - i * 0.1, 2) for i in range(1, 11)],
    "volume": [v for v in range(10, 0, -1)],
    "side": ["bid"] * 10
})
depthDf = pd.concat([askDepth, bidDepth], ignore_index=True)

# --- Tick Series ---
tickSeries = [[row["timestamp"], row["mid"]] for _, row in tickData.iterrows()]

# --- Depth Series ---
depthSeries = [
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
    for _, row in depthDf.iterrows()
]

# --- Chart Config ---
echartOptions = {
    "darkMode": True,
    "tooltip": {
        "trigger": "axis",
        "formatter": JsCode("""
            function(params) {
                const val = params[0].value[1].toFixed(2);
                const ts = params[0].value[0];
                return ts + '<br/>Price: ' + val;
            }
        """)
    },
    "dataZoom": [
        {"type": "inside", "xAxisIndex": [0, 1]},
        {"type": "slider", "xAxisIndex": [0, 1], "bottom": 10, "height": 40}
    ],
    "grid": [
        {"left": "5%", "right": "5%", "top": 20, "height": "60%"},
        {"left": "5%", "right": "5%", "top": "70%", "height": "20%"}
    ],
    "xAxis": [
        {"type": "category", "gridIndex": 0, "data": tickData["timestamp"].tolist()},
        {"type": "category", "gridIndex": 1, "data": [str(p) for p in depthDf["price"]]}
    ],
    "yAxis": [
        {"type": "value", "scale": True, "gridIndex": 0},
        {"type": "value", "gridIndex": 1}
    ],
    "series": [
        {
            "name": "Mid Price",
            "type": "line",
            "symbol": "circle",
            "symbolSize": 3,
            "showSymbol": False,
            "data": tickSeries,
            "lineStyle": {"width": 1},
            "xAxisIndex": 0,
            "yAxisIndex": 0
        }
    ] + depthSeries
}

# --- Scroll Detection for Backfill ---
st.markdown("⬅️ Scroll to left end to load more")
event = st_echarts(options=echartOptions, height="700px", width="100%", key="tickChart", events=["datazoom"])

if event and "dataZoom" in event:
    zoomStart = event["dataZoom"][0]["start"]
    if zoomStart <= 1 and st.session_state.offset > 0:
        st.session_state.offset = max(0, st.session_state.offset - CHUNK_SIZE)
        st.rerun()
