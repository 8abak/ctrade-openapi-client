import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib

# --- Load full 20k–80k ticks ---
df = pd.read_csv("ml/data/ticks20k_80k.csv")

# --- Convert timestamps properly ---
df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')

# --- Helper: compute mid price ---
df['mid'] = (df['bid'] + df['ask']) / 2

# --- Relabel 40k–60k for evaluation ---
target_window = df.iloc[20000:40000].copy()  # this is ticks 40k–60k
future_mid = df['mid'].iloc[20000 + 1000:40000 + 1000].reset_index(drop=True)
target = []

for i, current_price in enumerate(target_window['mid'].reset_index(drop=True)):
    try:
        future_price = future_mid[i]
        if future_price >= current_price + 1:
            target.append(1)
        elif future_price <= current_price - 1:
            target.append(-1)
        else:
            target.append(0)
    except:
        target.append(0)

target_window['target'] = target

# --- Append to labeledTicks.csv ---
target_window[['timestamp', 'bid', 'ask', 'mid', 'target']].to_csv(
    "ml/data/labeledTicks.csv", mode='a', header=False, index=False
)

# --- Train on 20k–40k (ticks 20k–40k) ---
train = df.iloc[0:20000].copy()
train['mid'] = (train['bid'] + train['ask']) / 2

# Feature example: momentum, spread, price delta
train['spread'] = train['ask'] - train['bid']
train['pd'] = train['mid'].diff()
train['momentum'] = train['mid'].diff(10)

# Target labeling for train
future_train = train['mid'].shift(-1000)
train['target'] = np.where(future_train >= train['mid'] + 1, 1,
                    np.where(future_train <= train['mid'] - 1, -1, 0))
train.dropna(inplace=True)

# Train features
X_train = train[['spread', 'pd', 'momentum']]
y_train = train['target']

model = RandomForestClassifier(n_estimators=100, random_state=42)

print("✅ X_train shape:", X_train.shape)
print("✅ y_train shape:", y_train.shape)
print("📊 Features used:", X_train.columns.tolist())
print("🔍 Preview of X_train:\n", X_train.head())

model.fit(X_train, y_train)

# --- Predict on 40k–60k ---
predict_set = target_window[['spread', 'pd', 'momentum']].copy()
predict_set = predict_set.fillna(0)
y_pred = model.predict(predict_set)
y_prob = model.predict_proba(predict_set)

# --- Append predictions ---
pred_df = target_window[['timestamp']].copy()
pred_df['predicted'] = y_pred
pred_df['proba_up'] = y_prob[:, 1]
pred_df['proba_down'] = y_prob[:, 0]

pred_df.to_csv("ml/data/predicted_probabilities.csv", mode='a', header=False, index=False)

# --- Label targets for 60k–80k for future training ---
next_window = df.iloc[40000:60000].copy()
future_mid_next = df['mid'].iloc[40000 + 1000:60000 + 1000].reset_index(drop=True)
target_next = []

for i, current_price in enumerate(next_window['mid'].reset_index(drop=True)):
    try:
        future_price = future_mid_next[i]
        if future_price >= current_price + 1:
            target_next.append(1)
        elif future_price <= current_price - 1:
            target_next.append(-1)
        else:
            target_next.append(0)
    except:
        target_next.append(0)

next_window['target'] = target_next

next_window[['timestamp', 'bid', 'ask', 'mid', 'target']].to_csv(
    "ml/data/labeledTicks.csv", mode='a', header=False, index=False
)

print("✅ Extended labeling and predictions completed.")
