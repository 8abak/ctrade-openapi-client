# train/train_small.py
# Trains a small-trend classifier from ml_features_tick ⨝ ml_labels_small
# Writes predictions into predictions_small (upsert)

import os, json, pathlib, datetime as dt
import numpy as np, pandas as pd
from sqlalchemy import create_engine, text

# --------- config ----------
DATABASE_URL = os.getenv("DATABASE_URL","postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

ART_DIR = pathlib.Path("train/artifacts/small")
ART_DIR.mkdir(parents=True, exist_ok=True)

MODEL_NAME = "small_trend_baseline"
MODEL_VER  = dt.datetime.utcnow().strftime("v%Y%m%d")

def q(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execmany(sql, rows):
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def ensure_predictions_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS predictions_small (
      tickid     BIGINT PRIMARY KEY,
      timestamp  TIMESTAMPTZ NOT NULL,
      bid        DOUBLE PRECISION,
      ask        DOUBLE PRECISION,
      mid        DOUBLE PRECISION,
      prob_rev   DOUBLE PRECISION NOT NULL,
      decision   SMALLINT NOT NULL,
      model_name TEXT,
      model_ver  TEXT,
      day_key    DATE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def load_training(day: str):
    sql = """
    SELECT f.tickid, f.timestamp, f.bid, f.ask, f.mid, f.spread, f.vwap_dist,
           f.mom_5, f.mom_20, f.ma_fast, f.ma_slow, f.atr_s, f.atr_m,
           f.session_id, f.micro_state, f.maxi_state, f.day_key,
           l.s_next_hold
    FROM ml_features_tick f
    JOIN ml_labels_small l ON l.tickid = f.tickid
    WHERE f.day_key = :d
    """
    return q(sql, {"d": day})

def pick_X_y(df):
    y = df["s_next_hold"].astype(int).values
    drop = {"tickid","timestamp","day_key","s_next_hold"}
    X = df.drop(columns=[c for c in drop if c in df.columns], errors="ignore")
    # Safety: coerce to float (tree models will be fine)
    X = X.astype(float)
    return X.values, y, X.columns.tolist()

def get_model():
    # Try LightGBM → XGBoost → sklearn HistGB → RandomForest → LogisticRegression
    try:
        import lightgbm as lgb
        return "lightgbm", lgb.LGBMClassifier(n_estimators=300, max_depth=-1, learning_rate=0.05,
                                              subsample=0.8, colsample_bytree=0.8, random_state=17)
    except Exception:
        pass
    try:
        import xgboost as xgb
        return "xgboost", xgb.XGBClassifier(n_estimators=400, max_depth=6, learning_rate=0.05,
                                            subsample=0.8, colsample_bytree=0.8, random_state=17,
                                            eval_metric="logloss", n_jobs=4)
    except Exception:
        pass
    from sklearn.experimental import enable_hist_gradient_boosting  # noqa
    from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    try:
        return "sk_hgb", HistGradientBoostingClassifier(max_depth=8, learning_rate=0.06,
                                                        max_bins=255, random_state=17)
    except Exception:
        try:
            return "sk_rf", RandomForestClassifier(n_estimators=300, max_depth=12,
                                                   min_samples_leaf=5, random_state=17, n_jobs=-1)
        except Exception:
            return "sk_logit", LogisticRegression(max_iter=200)

def main():
    import argparse, joblib
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD (use 2025-06-17 for first run)")
    p.add_argument("--threshold", type=float, default=0.5, help="decision threshold for prob_rev")
    args = p.parse_args()

    df = load_training(args.date)
    if df.empty:
        print(f"No training rows for {args.date}."); return

    X, y, feat_names = pick_X_y(df)
    model_tag, model = get_model()
    print(f"Training model={model_tag} on {X.shape[0]} rows, {X.shape[1]} features...")

    model.fit(X, y)

    # Metrics (quick)
    try:
        from sklearn.metrics import roc_auc_score, accuracy_score
        proba = getattr(model, "predict_proba", None)
        if proba:
            p1 = model.predict_proba(X)[:,1]
            print("Train ROC-AUC:", round(roc_auc_score(y, p1), 4))
        yhat = model.predict(X)
        print("Train ACC:", round(accuracy_score(y, yhat), 4))
    except Exception:
        pass

    # Save artifacts
    outdir = ART_DIR / args.date
    outdir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, outdir / "model.pkl")
    (outdir / "features.json").write_text(json.dumps({"features": feat_names}, indent=2))
    (outdir / "model_card.json").write_text(json.dumps({
        "model_name": MODEL_NAME, "model_ver": MODEL_VER, "impl": model_tag,
        "train_date": args.date, "n_rows": int(X.shape[0]), "n_features": int(X.shape[1])
    }, indent=2))
    print(f"Artifacts saved to {outdir}")

    # Write predictions into predictions_small
    ensure_predictions_table()
    if getattr(model, "predict_proba", None):
        proba = model.predict_proba(X)[:,1]
    else:
        # fall back to decision function if no proba
        if hasattr(model, "decision_function"):
            import scipy.special as sps
            proba = sps.expit(model.decision_function(X))
        else:
            proba = model.predict(X).astype(float)
    decision = (proba >= args.threshold).astype(int)

    rows = []
    for i, r in df.iterrows():
        rows.append({
            "tickid": int(r["tickid"]),
            "timestamp": r["timestamp"],
            "bid": float(r["bid"]),
            "ask": float(r["ask"]),
            "mid": float(r["mid"]),
            "prob_rev": float(proba[i]),
            "decision": int(decision[i]),
            "model_name": MODEL_NAME,
            "model_ver": MODEL_VER,
            "day_key": r["day_key"]
        })

    execmany("""
      INSERT INTO predictions_small
      (tickid,timestamp,bid,ask,mid,prob_rev,decision,model_name,model_ver,day_key)
      VALUES (:tickid,:timestamp,:bid,:ask,:mid,:prob_rev,:decision,:model_name,:model_ver,:day_key)
      ON CONFLICT (tickid) DO UPDATE SET
        timestamp=EXCLUDED.timestamp, bid=EXCLUDED.bid, ask=EXCLUDED.ask, mid=EXCLUDED.mid,
        prob_rev=EXCLUDED.prob_rev, decision=EXCLUDED.decision,
        model_name=EXCLUDED.model_name, model_ver=EXCLUDED.model_ver, day_key=EXCLUDED.day_key
    """, rows)
    print(f"predictions_small upserted: {len(rows)}")

if __name__ == "__main__":
    main()
