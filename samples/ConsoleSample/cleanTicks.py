import pandas as pd

# Load the CSV
df = pd.read_csv("ticks.csv")

# Replace 0.0 in bid/ask with NaN temporarily
df["bid"] = df["bid"].replace(0.0, pd.NA)
df["ask"] = df["ask"].replace(0.0, pd.NA)

# Forward-fill from previous valid value
df["bid"] = df["bid"].fillna(method="ffill")
df["ask"] = df["ask"].fillna(method="ffill")

# Optional: Fill initial NaNs (if the file starts with 0.0s)
df["bid"] = df["bid"].fillna(method="bfill")
df["ask"] = df["ask"].fillna(method="bfill")

# Save back if needed
df.to_csv("ticks_cleaned.csv", index=False)

print("Zeroes replaced with previous non-zero values.")
