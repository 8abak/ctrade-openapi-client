# jobs/train_online.py
import pandas as pd
from sqlalchemy import create_engine, text
import os
from ml.feat_events import build_event_features
from ml.learn import OnlineMoveModel

PG_DSN = os.environ.get("PG_DSN", "postgresql+psycopg2://postgres@localhost/ctrade")
THRESHOLDS = [2,3,4,5]

def fetch_kalman(engine) -> pd.DataFrame:
    return pd.read_sql("SELECT tickid, level FROM kalman_states ORDER BY tickid", engine)

def fetch_raw(engine) -> pd.DataFrame:
    # mid from ticks; if another mid source exists, use that
    return pd.read_sql("SELECT id AS tickid, mid FROM ticks ORDER BY id", engine)

def fetch_resolved(engine, T: int) -> pd.DataFrame:
    sql = text("""
      SELECT * FROM move_labels
      WHERE threshold_usd=:T AND is_open=FALSE
        AND p_up IS NOT NULL  -- already predicted; now use outcome to train
      ORDER BY tickid_start
    """)
    return pd.read_sql(sql, engine, params={'T':T})

def fetch_to_predict(engine, T: int) -> pd.DataFrame:
    # New starts without prediction yet (is_open TRUE) – we will predict and leave open
    sql = text("""
      SELECT * FROM move_labels
      WHERE threshold_usd=:T AND p_up IS NULL
      ORDER BY tickid_start
    """)
    return pd.read_sql(sql, engine, params={'T':T})

def main():
    eng = create_engine(PG_DSN)
    k = fetch_kalman(eng)
    r = fetch_raw(eng)

    for T in THRESHOLDS:
        model = OnlineMoveModel(T)

        # 1) Predict new opens
        to_pred = fetch_to_predict(eng, T)
        if not to_pred.empty:
            feats = build_event_features(k, r, to_pred['tickid_start'].tolist())
            probs = model.predict_proba(feats.drop(columns=['tickid']))
            # classes order ['dn','nt','up']; take p_up
            p_up = probs[:, 2]
            with eng.begin() as conn:
                for (tickid, p) in zip(feats['tickid'], p_up):
                    conn.execute(text("""
                        UPDATE move_labels
                        SET p_up=:p, dir_guess=CASE WHEN :p >= 0.5 THEN 'up' ELSE 'dn' END
                        WHERE tickid_start=:t AND threshold_usd=:T
                    """), dict(p=float(p), t=int(tickid), T=T))
                    # also store a point for plotting if you like a continuous line
                    conn.execute(text("""
                        INSERT INTO predictions (tickid, p_up, model_id)
                        VALUES (:t, :p, :m)
                        ON CONFLICT (tickid) DO UPDATE SET p_up=EXCLUDED.p_up, model_id=EXCLUDED.model_id
                    """), dict(t=int(tickid), p=float(p), m=f"move_{T}"))

        # 2) Train on resolved ones
        resolved = fetch_resolved(eng, T)
        if not resolved.empty:
            feats = build_event_features(k, r, resolved['tickid_start'].tolist())
            y = resolved['outcome'].tolist()  # 'up','dn','nt'
            model.partial_fit(feats.drop(columns=['tickid']), y)
            model.save()

    print("online train/predict complete")

if __name__ == "__main__":
    main()
