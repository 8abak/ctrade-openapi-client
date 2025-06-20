import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.title("📉 Renko Chart Viewer")

# ✅ PostgreSQL connection
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# ✅ Load last 10,000 ticks
df = pd.read_sql("SELECT * FROM ticks WHERE symbol = 'XAUUSD' ORDER BY timestamp DESC LIMIT 10000", engine)
df = df.sort_values("timestamp")
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["mid"] = (df["bid"] + df["ask"]) / 2

# ✅ Renko calculation
brickSize = st.sidebar.slider("Brick Size ($)", min_value=0.1, max_value=5.0, value=1.0, step=0.1)

renko = []
lastBrick = None

for price in df["mid"]:
    if lastBrick is None:
        lastBrick = price
        renko.append(price)
        continue

    diff = price - lastBrick
    steps = int(diff / brickSize)

    if steps != 0:
        for i in range(abs(steps)):
            lastBrick += brickSize * (1 if steps > 0 else -1)
            renko.append(lastBrick)

# ✅ Create pseudo-time axis
renko_df = pd.DataFrame({
    "index": range(len(renko)),
    "price": renko
})

# ✅ Plot
fig = go.Figure()
fig.add_trace(go.Scatter(x=renko_df["index"], y=renko_df["price"],
                         mode="lines+markers", line=dict(width=2), marker=dict(size=4)))

fig.update_layout(
    title=f"Renko Chart — {len(renko)} Bricks | Brick Size: ${brickSize}",
    xaxis_title="Renko Brick Index",
    yaxis_title="Price",
    height=600
)

st.plotly_chart(fig, use_container_width=True)
engine.dispose()
