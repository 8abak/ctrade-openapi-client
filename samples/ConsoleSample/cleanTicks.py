import pandas as pd

# Load CSV
df = pd.read_csv("ticks.csv")

# Convert bid/ask to numeric just in case
df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
df["ask"] = pd.to_numeric(df["ask"], errors="coerce")

# Replace zeros with NaN to enable forward-fill
df["bid"].replace(0.0, pd.NA, inplace=True)
df["ask"].replace(0.0, pd.NA, inplace=True)

# Forward-fill and backfill
df["bid"] = df["bid"].ffill().bfill()
df["ask"] = df["ask"].ffill().bfill()

# Save cleaned version
df.to_csv("ticks_cleaned.csv", index=False)

print("✅ Zeroes replaced with previous non-zero values.")
