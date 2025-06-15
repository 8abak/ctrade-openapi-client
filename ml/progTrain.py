import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from labelTargets_v2 import label_targets
from featureEngineering import engineer_features
import os

# Load full tick dataset (up to whatever number is available)
df = pd.read_csv("ml/data/ticks20k_80k.csv")
df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

# Label the entire dataset once
labeled_df = label_targets(df.copy())

# Initialize result report list
report = []
all_predictions = []

# Iterate in windows of 10k ticks, always training on the previous 10k and predicting the next
for start in range(0, len(labeled_df) - 20000 + 1, 10000):
    train_df = labeled_df.iloc[start:start+10000].copy()
    test_df = labeled_df.iloc[start+10000:start+20000].copy()

    # Drop nulls created during labeling
    train_df.dropna(inplace=True)
    test_df.dropna(inplace=True)

    # Feature engineering
    train_feat = engineer_features(train_df.copy())
    test_feat = engineer_features(test_df.copy())

    drop_cols = ['timestamp', 'bid', 'ask', 'mid', 'target', 'session_date']
    X_train = train_feat.drop(columns=drop_cols, errors='ignore')
    y_train = train_feat['target']
    X_test = test_feat.drop(columns=drop_cols, errors='ignore')
    y_test = test_feat['target']

    # Skip window if not enough data
    if X_train.empty or X_test.empty:
        continue

    # Train and predict
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    # Evaluate
    correct = np.sum(y_pred == y_test)
    total = len(y_test)
    accuracy = correct / total if total > 0 else 0
    
    # Store detailed predictions for later visualization
    test_df = test_df.iloc[:len(y_pred)].copy()
    test_df['prediction'] = y_pred
    test_df['correct'] = (y_pred == y_test.values[:len(y_pred)]).astype(int)
    all_predictions.append(test_df[['timestamp', 'prediction', 'target', 'correct']])

    # Log results
    report.append({
        "window": f"{start+10000}-{start+20000}",
        "total_guesses": total,
        "correct_guesses": correct,
        "accuracy": round(accuracy, 4)
    })

# Final result dataframe
report_df = pd.DataFrame(report)
report_df.to_csv("ml/data/guessReport.csv", index=False)

# Combine all detailed predictions and save
all_pred_df = pd.concat(all_predictions)
all_pred_df.to_csv("ml/data/detailedPredictions.csv", index=False)

print("✅ Report saved to ml/data/guessReport.csv")
print("✅ Detailed predictions saved to ml/data/detailedPredictions.csv")
