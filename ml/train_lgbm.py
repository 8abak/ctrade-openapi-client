# ml/train_lgbm.py
import io, json, time, uuid
import numpy as np
from typing import Tuple
from lightgbm import LGBMClassifier
from sklearn.calibration import CalibratedClassifierCV
import joblib

from .db import db_conn, save_model_blob
from .train_sgd import FEATURES

def _load_xy(start: int, end: int) -> Tuple[np.ndarray, np.ndarray]:
    sql = """
      SELECT f.tickid, f.{cols}, l.direction, l.is_segment_start
      FROM ml_features f
      JOIN trend_labels l ON l.tickid=f.tickid
      WHERE f.tickid BETWEEN :s AND :e
      ORDER BY f.tickid
    """.format(cols=",".join(FEATURES))
    with db_conn() as conn:
        rows = conn.exec_driver_sql(sql, {"s": int(start), "e": int(end)}).fetchall()
    if not rows:
        raise RuntimeError("No training rows found in range.")
    X = []
    y = []
    for row in rows:
        if not row[-1]:
            continue
        feats = []
        for v in row[1:1+len(FEATURES)]:
            feats.append(0.0 if v is None else float(v))
        X.append(feats)
        y.append(int(row[1+len(FEATURES)]))
    X = np.array(X, dtype=float)
    y_map = {-1:0,0:1,1:2}
    y = np.vectorize(y_map.get)(np.array(y, dtype=int))
    return X, y

def train_and_calibrate(start: int, end: int) -> str:
    X, y = _load_xy(start, end)
    n = X.shape[0]
    k = max(1, int(n*0.9))
    Xtr, ytr = X[:k], y[:k]
    Xho, yho = X[k:], y[k:]

    clf = LGBMClassifier(
        objective="multiclass",
        num_class=3,
        num_leaves=31,
        n_estimators=600,
        feature_fraction=0.8,
        bagging_fraction=0.8,
        learning_rate=0.05,
        random_state=42
    )
    clf.fit(Xtr, ytr)
    calib = CalibratedClassifierCV(clf, method="isotonic", cv="prefit")
    calib.fit(Xho, yho)

    model_id = f"lgbm-{start}-{end}-{uuid.uuid4().hex[:8]}"
    buf = io.BytesIO()
    joblib.dump({"model": calib, "features": FEATURES, "y_map": {-1:0,0:1,1:2}, "y_inv": {0:-1,1:0,2:1}}, buf)
    save_model_blob(model_id, "lgbm", buf.getvalue(), notes=f"LightGBM isotonic; trained on {start}-{end}", extra_params={"train_range":[start,end]})
    print(json.dumps({"model_id": model_id}))
    return model_id
