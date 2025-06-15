
import pandas as pd

# Load the extracted tick data
df = pd.read_csv("ml/data/first40kTicks.csv")

# Initialize target column
df["target"] = None

# Set the trigger distance
TARGET_MOVE = 1.0

# Iterate and label each tick
for i in range(len(df) - 1):
    base_price = df.loc[i, "mid"]
    sub_df = df.iloc[i+1:i+1000]  # look ahead up to 1000 ticks

    hit_up = sub_df[sub_df["mid"] >= base_price + TARGET_MOVE]
    hit_down = sub_df[sub_df["mid"] <= base_price - TARGET_MOVE]

    if hit_up.empty and hit_down.empty:
        continue  # undecided
    elif not hit_up.empty and (hit_down.empty or hit_up.index[0] < hit_down.index[0]):
        df.loc[i, "target"] = 1  # price hit $1 up first
    else:
        df.loc[i, "target"] = -1  # price hit $1 down first

# Drop rows without target
df.dropna(subset=["target"], inplace=True)
df["target"] = df["target"].astype(int)

# Save the labeled data
df.to_csv("ml/data/labeledTicks.csv", index=False)
print("✅ Labeled data saved to ml/data/labeledTicks.csv")
