import pandas as pd
import numpy as np

def engineer_features(df):
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')

    # Price-based features
    df['spread'] = df['ask'] - df['bid']
    df['delta'] = df['mid'].diff()
    df['momentum'] = df['delta'].rolling(window=10).sum()
    df['vwap'] = df['mid'].expanding().mean()

    # Moving averages
    df['ma_20'] = df['mid'].rolling(window=20).mean()
    df['ma_50'] = df['mid'].rolling(window=50).mean()

    # MACD
    ema12 = df['mid'].ewm(span=12, adjust=False).mean()
    ema26 = df['mid'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema12 - ema26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    # Volatility
    df['volatility'] = df['mid'].rolling(window=20).std()

    # Time-based features
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    df['day_of_week'] = df['timestamp'].dt.dayofweek

    # Market session features
    df['is_sydney'] = df['hour'].between(21, 6, inclusive='left')
    df['is_tokyo'] = df['hour'].between(0, 9, inclusive='left')
    df['is_london'] = df['hour'].between(7, 16, inclusive='left')
    df['is_chicago'] = df['hour'].between(13, 22, inclusive='left')

    # ACD Strategy Features
    acd_window = pd.Timedelta(minutes=15)
    A_VALUE = 1.0 / 3
    C_VALUE = 1.0 / 3

    df['session_date'] = df['timestamp'].dt.date
    df['session_hour'] = df['timestamp'].dt.floor('h')  # lowercase 'h' for future-proofing

    or_highs, or_lows = [], []
    a_up_levels, c_down_levels = [], []
    in_or_range, above_a_up, below_c_down = [], [], []
    or_start_time = []

    for date, group in df.groupby('session_date'):
        session_df = group.sort_values('timestamp')
        if session_df.empty:
            continue

        or_start = session_df['timestamp'].iloc[0]
        or_end = or_start + acd_window
        or_range = session_df[(session_df['timestamp'] >= or_start) & (session_df['timestamp'] <= or_end)]

        high = or_range['mid'].max()
        low = or_range['mid'].min()
        a_up = high + A_VALUE
        c_down = low - C_VALUE

        for i, row in session_df.iterrows():
            price = row['mid']
            or_highs.append(high)
            or_lows.append(low)
            a_up_levels.append(a_up)
            c_down_levels.append(c_down)
            in_or_range.append(int(low <= price <= high))
            above_a_up.append(int(price > a_up))
            below_c_down.append(int(price < c_down))
            or_start_time.append(or_start)

    df = df.iloc[:len(or_highs)].copy()
    df['or_high'] = or_highs
    df['or_low'] = or_lows
    df['a_up_level'] = a_up_levels
    df['c_down_level'] = c_down_levels
    df['in_or_range'] = in_or_range
    df['above_a_up'] = above_a_up
    df['below_c_down'] = below_c_down
    df['or_minutes_elapsed'] = (df['timestamp'] - pd.to_datetime(or_start_time)).dt.total_seconds() / 60

    df.dropna(inplace=True)
    return df

# Standalone script execution
if __name__ == "__main__":
    df = pd.read_csv("ml/data/labeledTicks.csv")
    df = engineer_features(df)
    df.to_csv("ml/data/features.csv", index=False)
    print("✅ Features with ACD saved to ml/data/features.csv")
