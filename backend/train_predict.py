# backend/train_predict.py
# Purpose: Train a lightweight tabular classifier to estimate P(TP first).
# Chooses threshold tau to maximize EV with +$2 / -$1 payoff on a validation split.
# Writes predictions(event_id, model_version, p_tp, threshold, decided).

import os, json, math, random
from datetime import datetime
from typing import Dict, Any, List, Tuple
from sqlalchemy import create_engine, text

def _GetEngine():
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
    )
    return create_engine(db_url)

def _LoadTrainData(conn):
    rows = conn.execute(text("""
        SELECT e.event_id, e.features, o.outcome
        FROM micro_events e
        JOIN outcomes o ON o.event_id = e.event_id
    """)).mappings().all()
    X, y, ids = [], [], []
    for r in rows:
        feats = dict(r["features"])
        # Flatten a safe subset of features
        x = [
            float(feats.get("FastSlope", 0.0)),
            float(feats.get("SlowGap", 0.0)),
            float(feats.get("Std60Avg", 0.0)),
            float(feats.get("Push20", 0.0)),
            float(feats.get("PosInLeg", feats.get("Position", 0.0))),
        ]
        # Session as simple one-hot-ish buckets (Asia, Europe, US)
        sess = feats.get("Session", "US")
        sess_vec = [1.0 if sess=="Asia" else 0.0, 1.0 if sess=="Europe" else 0.0, 1.0 if sess=="US" else 0.0]
        x += sess_vec
        # Target: TP=1; SL/Timeout=0
        tgt = 1 if r["outcome"]=="TP" else 0
        X.append(x); y.append(tgt); ids.append(int(r["event_id"]))
    return X, y, ids

def _Sigmoid(z): return 1.0/(1.0+math.exp(-z))

class _TinyGBLike:
    """
    Minimal fallback learner (1-layer logistic on standardized features).
    We try real libs first; if unavailable, this runs without extra deps.
    """
    def __init__(self): self.w=None; self.mu=None; self.sd=None
    def fit(self, X, y, iters=400, lr=0.05):
        import statistics
        n = len(X); d = len(X[0]) if n else 0
        self.mu = [statistics.mean([x[j] for x in X]) for j in range(d)]
        self.sd = [max(1e-6, statistics.pstdev([x[j] for x in X])) for j in range(d)]
        Z = [[(x[j]-self.mu[j])/self.sd[j] for j in range(d)] for x in X]
        self.w = [0.0]* (d+1) # + bias
        for _ in range(iters):
            for i in range(n):
                z = sum(self.w[j]*Z[i][j] for j in range(d)) + self.w[-1]
                p = _Sigmoid(z)
                g = (p - y[i])
                for j in range(d):
                    self.w[j] -= lr * g * Z[i][j]
                self.w[-1] -= lr * g
    def predict_proba(self, X):
        d = len(self.mu)
        Z = [[(x[j]-self.mu[j])/self.sd[j] for j in range(d)] for x in X]
        return [_Sigmoid(sum(self.w[j]*z[j] for j in range(d)) + self.w[-1]) for z in Z]

def _TryModel():
    # Try LightGBM, then XGBoost, fallback to tiny logistic
    try:
        import lightgbm as lgb
        def train(X, y):
            import numpy as np
            dtrain = lgb.Dataset(np.array(X), label=np.array(y))
            params = {"objective":"binary", "metric":"binary_logloss", "verbosity":-1, "num_leaves":31}
            m = lgb.train(params, dtrain, num_boost_round=80)
            return lambda X2: m.predict(np.array(X2))
        return train
    except Exception:
        try:
            import xgboost as xgb
            def train(X, y):
                import numpy as np
                dtrain = xgb.DMatrix(np.array(X), label=np.array(y))
                params = {"objective":"binary:logistic", "eval_metric":"logloss", "verbosity":0}
                m = xgb.train(params, dtrain, num_boost_round=120)
                return lambda X2: m.predict(xgb.DMatrix(np.array(X2)))
            return train
        except Exception:
            def train(X, y):
                mdl = _TinyGBLike(); mdl.fit(X,y)
                return lambda X2: mdl.predict_proba(X2)
            return train

def _ChooseTau(probs: List[float], y: List[int]) -> float:
    # sweep taus and maximize expected value: EV = 2*TPs - 1*FPs over chosen set
    if not probs: return 0.5
    pairs = sorted([(p, yy) for p,yy in zip(probs, y)], key=lambda z:z[0])
    best_tau, best_ev = 0.5, -1e9
    for k in range(1, 20):
        tau = 0.45 + 0.03*k  # 0.48..1.0
        chosen = [(p,yy) for p,yy in pairs if p >= tau]
        if not chosen: continue
        tps = sum(1 for _,yy in chosen if yy==1)
        fps = sum(1 for _,yy in chosen if yy==0)
        ev = 2.0*tps - 1.0*fps
        if ev > best_ev:
            best_ev, best_tau = ev, tau
    return float(best_tau)

def TrainAndPredict(ModelVersionPrefix:str="wf-v1") -> Dict[str,Any]:
    eng = _GetEngine()
    with eng.begin() as conn:
        X, y, ids = _LoadTrainData(conn)
        if len(X) < 30:
            return {"trained": False, "reason": "insufficient labeled events", "written": 0}

        # simple split: last 20% as validation
        cut = int(0.8*len(X))
        Xtr, ytr = X[:cut], y[:cut]
        Xva, yva = X[cut:], y[cut:]

        train_fn = _TryModel()
        pred_tr = train_fn(Xtr, ytr)
        va_probs = pred_tr(Xva)
        tau = _ChooseTau(va_probs, yva)

        # model id
        model_version = f"{ModelVersionPrefix}-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"

        # Predict for events in the MOST RECENT closed segment that do not have predictions for this model
        seg = conn.execute(text("""
            SELECT segment_id FROM macro_segments ORDER BY end_ts DESC LIMIT 1
        """)).scalar()
        if not seg:
            return {"trained": True, "model_version": model_version, "threshold": tau, "written": 0}

        rows = conn.execute(text("""
            SELECT e.event_id, e.features
            FROM micro_events e
            LEFT JOIN predictions p ON p.event_id = e.event_id AND p.model_version = :mv
            WHERE e.segment_id = :seg AND p.event_id IS NULL
            ORDER BY e.event_id
        """), {"mv": model_version, "seg": seg}).mappings().all()
        if not rows:
            return {"trained": True, "model_version": model_version, "threshold": tau, "written": 0}

        Xnew, eids = [], []
        for r in rows:
            feats = dict(r["features"])
            x = [
                float(feats.get("FastSlope", 0.0)),
                float(feats.get("SlowGap", 0.0)),
                float(feats.get("Std60Avg", 0.0)),
                float(feats.get("Push20", 0.0)),
                float(feats.get("PosInLeg", feats.get("Position", 0.0))),
            ]
            sess = feats.get("Session","US")
            sess_vec = [1.0 if sess=="Asia" else 0.0, 1.0 if sess=="Europe" else 0.0, 1.0 if sess=="US" else 0.0]
            x += sess_vec
            Xnew.append(x); eids.append(int(r["event_id"]))

        probs = pred_tr(Xnew)
        wrote = 0
        for eid, p in zip(eids, probs):
            conn.execute(text("""
                INSERT INTO predictions (event_id, model_version, p_tp, threshold, decided)
                VALUES (:eid, :mv, :p, :tau, :decided)
                ON CONFLICT (event_id, model_version) DO NOTHING
            """), {"eid": eid, "mv": model_version, "p": float(p), "tau": float(tau), "decided": bool(p >= tau)})
            wrote += 1

    return {"trained": True, "model_version": model_version, "threshold": tau, "written": wrote}
