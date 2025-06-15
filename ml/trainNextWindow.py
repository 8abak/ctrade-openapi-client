
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

# -------------------------------
# Step 1: Load engineered features
# -------------------------------
df = pd.read_csv("ml/data/features.csv")

# -------------------------------
# Step 2: Define features and target
# -------------------------------
drop_cols = ['timestamp', 'bid', 'ask', 'mid', 'target', 'session_date', 'session_hour']
feature_cols = [col for col in df.columns if col not in drop_cols]
X = df[feature_cols]
y = df['target']

# -------------------------------
# Step 3: Train on 20k–40k, Test on 40k–60k
# -------------------------------
X_train = X.iloc[20000:40000]
y_train = y.iloc[20000:40000]
X_test = X.iloc[40000:60000]
y_test = y.iloc[40000:60000]

# -------------------------------
# Step 4: Train the model
# -------------------------------
model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
model.fit(X_train, y_train)

# -------------------------------
# Step 5: Evaluate predictions
# -------------------------------
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)

print("✅ Model Evaluation (Window 2):")
print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))
print("\nClassification Report:")
print(classification_report(y_test, y_pred, digits=4))

# -------------------------------
# Step 6: Save prediction probabilities
# -------------------------------
proba_df = pd.DataFrame(y_proba, columns=['prob_down', 'prob_up'])
proba_df['true_label'] = y_test.values
proba_df.to_csv("ml/data/predicted_probabilities_2.csv", index=False)
print("✅ Saved to ml/data/predicted_probabilities_2.csv")
