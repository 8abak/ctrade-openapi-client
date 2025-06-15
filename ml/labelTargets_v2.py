import pandas as pd

def label_targets(df, target_move=1.0, lookahead=1000):
    df = df.copy()
    df["target"] = None

    for i in range(len(df) - 1):
        base_price = df.loc[i, "mid"]
        sub_df = df.iloc[i+1:i+1+lookahead]

        hit_up = sub_df[sub_df["mid"] >= base_price + target_move]
        hit_down = sub_df[sub_df["mid"] <= base_price - target_move]

        if hit_up.empty and hit_down.empty:
            continue
        elif not hit_up.empty and (hit_down.empty or hit_up.index[0] < hit_down.index[0]):
            df.loc[i, "target"] = 1
        else:
            df.loc[i, "target"] = -1

    df.dropna(subset=["target"], inplace=True)
    df["target"] = df["target"].astype(int)
    return df

# Optional: standalone execution for testing
if __name__ == "__main__":
    df = pd.read_csv("ml/data/first40kTicks.csv")
    labeled_df = label_targets(df)
    labeled_df.to_csv("ml/data/labeledTicks.csv", index=False)
    print("✅ Labeled data saved to ml/data/labeledTicks.csv")
