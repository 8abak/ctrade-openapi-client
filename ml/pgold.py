
import pandas as pd
import plotly.graph_objs as go
import streamlit as st

# Load model output and associated timestamps
proba_df = pd.read_csv("ml/data/predicted_probabilities.csv")
features_df = pd.read_csv("ml/data/features.csv")

# Use matching time window (last 20k rows of features = 20k test rows)
test_df = features_df.iloc[20000:40000].copy()
test_df = test_df.reset_index(drop=True)
proba_df = proba_df.reset_index(drop=True)

# Merge predictions with actual price data
merged = test_df[['timestamp', 'mid']].copy()
merged['prob_up'] = proba_df['prob_up']
merged['prob_down'] = proba_df['prob_down']
merged['true_label'] = proba_df['true_label']

# Assign color based on confidence
def get_color(p_up):
    if p_up > 0.7:
        return 'rgba(0,200,0,0.4)'  # green
    elif p_up < 0.3:
        return 'rgba(200,0,0,0.4)'  # red
    else:
        return 'rgba(200,200,0,0.2)'  # yellow

merged['color'] = merged['prob_up'].apply(get_color)

# Plot price line
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=merged['timestamp'],
    y=merged['mid'],
    mode='lines',
    name='Mid Price',
    line=dict(color='white')
))

# Add prediction "cloud"
fig.add_trace(go.Scatter(
    x=merged['timestamp'],
    y=merged['mid'],
    mode='markers',
    marker=dict(
        color=merged['color'],
        size=6,
    ),
    name='Prediction Cloud'
))

fig.update_layout(
    title="📊 Model Prediction Confidence vs. Real Price",
    xaxis_title="Timestamp",
    yaxis_title="Price (Mid)",
    template="plotly_dark",
    hovermode="x unified",
    height=700
)

# Streamlit layout
st.set_page_config(layout="wide")
st.title("🧠 Prediction Cloud vs Real Market - XAUUSD")
st.plotly_chart(fig, use_container_width=True)
