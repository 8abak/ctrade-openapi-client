# ml/train_sgd.py  — REPLACE ENTIRE FILE
import io, json, time, uuid
from typing import Tuple
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import Pipeline
import joblib
from sqlalchemy import text

from .db import db_conn, save_model_blob

FEATURES = [
    "level","slope","residual","vol_ewstd","vol_ewstd_long","r50","r200","r1000",
    "rsi","stoch_k","stoch_d","hilbert_amp","hilbert_phase","vwap_dist","r2_lin","tod_bucket"
]

def _load_xy(start: int, end: int) -> Tuple[np.ndarray, np.ndarray]:
    sql = f"""
      SELECT f.tickid, {", ".join("f."+c for c in FEATURES)}, l.direction, l.is_segment_start
      FROM ml_features f
      JOIN trend_labels l ON l.tickid = f.tickid
      WHERE f.tickid BETWEEN :s AND :e
      ORDER BY f.tickid
    """
    with db_conn() as conn:
        rows = conn.execute(text(sql), {"s": int(start), "e": int(end)}).fetchall()
    if not rows:
        raise RuntimeError("No training rows found in range.")
    X, y = [], []
    for row in rows:
        # Only use segment starts for training (causal segment beginnings)
        if not row[-1]:
            continue
        feats = [0.0 if v is None else float(v) for v in row[1:1+len(FEATURES)]]
        X.append(feats)
        y.append(int(row[1+len(FEATURES)]))
    X = np.array(X, dtype=float)
    y = np.array(y, dtype=int)  # values in {-1,0,1}
    # Map to classes 0,1,2 for sklearn
    y_map = {-1:0, 0:1, 1:2}
    y_mc = np.vectorize(y_map.get)(y)
    return X, y_mc

def train_and_calibrate(start: int, end: int) -> str:
    t0 = time.time()
    X, y = _load_xy(start, end)
    n = X.shape[0]
    if n < 100:
        raise RuntimeError("Too few samples for training.")
    k = max(1, int(n*0.9))  # holdout = last 10%
    Xtr, ytr = X[:k], y[:k]
    Xho, yho = X[k:], y[k:]

    base = SGDClassifier(
        loss="log_loss",
        class_weight="balanced",
        early_stopping=True,
        n_iter_no_change=5,
        max_iter=2000,
        random_state=42
    )
    pipe = Pipeline([("scaler", StandardScaler()), ("clf", base)])
    pipe.fit(Xtr, ytr)

    calib = CalibratedClassifierCV(pipe, method="isotonic", cv="prefit")
    calib.fit(Xho, yho)

    model_id = f"sgd-{start}-{end}-{uuid.uuid4().hex[:8]}"
    buf = io.BytesIO()
    joblib.dump(
        {"model": calib, "features": FEATURES, "y_map": {-1:0,0:1,1:2}, "y_inv": {0:-1,1:0,2:1}},
        buf
    )
    save_model_blob(
        model_id, "sgd", buf.getvalue(),
        notes=f"SGD isotonic; trained on {start}-{end}",
        extra_params={"train_range":[start,end]}
    )
    print(json.dumps({"model_id": model_id, "train_sec": time.time()-t0}))
    return model_id
