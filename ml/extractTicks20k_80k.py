import pandas as pd

# Load the full tick file
df = pd.read_csv("ticks.csv")

# Ensure it's sorted by timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')

# Extract ticks from 20k to 80k
df_20k_80k = df.iloc[20000:80000]

# Save it
df_20k_80k.to_csv("ml/data/ticks20k_80k.csv", index=False)
print("✅ Extracted ticks from 20k to 80k.")
