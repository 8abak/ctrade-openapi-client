from sqlalchemy import create_engine, text
from ml_config import DATABASE_URL, NO_RETURN_PROBA
import pandas as pd, numpy as np, os, joblib
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.utils.class_weight import compute_class_weight

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
MODEL_PATH = "models/no_return.pkl"
FEATURES = ["pos_ratio","progress_norm","ret1","ret5","vol20","drawdown","seconds_since"]

def _load(zig_id):
    q = text("""SELECT tickid, """ + ",".join(FEATURES) + """, target_no_return
                FROM tick_features WHERE zig_id=:z ORDER BY tickid""")
    with engine.begin() as c:
        return pd.read_sql(q, c, params={"z": zig_id})

def _model():
    os.makedirs("models", exist_ok=True)
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH)
    m = Pipeline([
        ("sc", StandardScaler(with_mean=False)),
        ("clf", SGDClassifier(loss="log", max_iter=1))  # acts like online
    ])
    joblib.dump(m, MODEL_PATH)
    return m

def train_on_zig(zig_id):
    df = _load(zig_id)
    if df.empty:
        return
    X = df[FEATURES].fillna(0.0).to_numpy()
    y = df["target_no_return"].astype(int).to_numpy()

    m = _model()
    cw = compute_class_weight("balanced", classes=np.array([0,1]), y=y)
    m.named_steps["clf"].class_weight = {0: cw[0], 1: cw[1]}
    m.fit(X, y)  # single pass
    joblib.dump(m, MODEL_PATH)

    proba = m.predict_proba(X)[:,1]
    first = np.argmax(proba >= NO_RETURN_PROBA) if (proba >= NO_RETURN_PROBA).any() else None

    with engine.begin() as c:
        upd = text("""
            UPDATE tick_features
            SET pred_proba=:p, pred_is_earliest=:e
            WHERE zig_id=:z AND tickid=:tid
        """)
        for i, (tid, p) in enumerate(zip(df["tickid"], proba)):
            c.execute(upd, {
                "p": float(p),
                "e": bool(first is not None and i >= first),
                "z": zig_id,
                "tid": int(tid)
            })
