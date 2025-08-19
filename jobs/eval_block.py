# jobs/eval_block.py
import argparse, json, time, io
import numpy as np
import joblib
from sklearn.metrics import f1_score, precision_score

from ml.db import db_conn, upsert_many, load_model_blob
from ml.survival import survival_curve_up, compact_curve
from ml.train_sgd import FEATURES

def _load_XY(start: int, end: int):
    sql = """
      SELECT f.tickid, f.{cols}, l.direction, l.is_segment_start, extract(epoch from f.timestamp) as ts, COALESCE(NULLIF(f.level, NULL), 0.0) as lvl
      FROM ml_features f
      LEFT JOIN trend_labels l ON l.tickid=f.tickid
      WHERE f.tickid BETWEEN :s AND :e
      ORDER BY f.tickid
    """.format(cols=",".join(FEATURES))
    with db_conn() as conn:
        rows = conn.exec_driver_sql(sql, {"s": int(start), "e": int(end)}).fetchall()
    X=[]; y=[]; tickids=[]; is_start=[]; ts=[]; lvl=[]
    for row in rows:
        tickids.append(int(row[0]))
        feats = [0.0 if v is None else float(v) for v in row[1:1+len(FEATURES)]]
        X.append(feats)
        y.append(int(row[1+len(FEATURES)]) if row[1+len(FEATURES)] is not None else 0)
        is_start.append(bool(row[2+len(FEATURES)] if row[2+len(FEATURES)] is not None else False))
        ts.append(float(row[3+len(FEATURES)]))
        lvl.append(float(row[4+len(FEATURES)]))
    y_map = {-1:0,0:1,1:2}
    y_mc = np.vectorize(y_map.get)(np.array(y, dtype=int))
    return np.array(X, dtype=float), y_mc, tickids, np.array(is_start, dtype=bool), np.array(ts, dtype=float), np.array(lvl, dtype=float)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--model", type=str, default="latest")  # 'latest' or a model_id
    ap.add_argument("--algo", type=str, choices=["sgd","lgbm"], default="sgd")
    args = ap.parse_args()

    t0 = time.time()
    # Load model
    if args.model == "latest":
        model_id, blob = load_model_blob(algo=args.algo)
    else:
        model_id, blob = load_model_blob(model_id=args.model)
    model = joblib.load(io.BytesIO(blob))["model"]
    mapping = joblib.load(io.BytesIO(blob)).get("y_inv", {0:-1,1:0,2:1})

    # Load data
    X, y_true, tickids, is_start, ts, lvl = _load_XY(args.start, args.end)
    if X.size == 0:
        raise RuntimeError("No test features found.")

    proba = model.predict_proba(X)
    # Ensure columns are in order [class 0,1,2] => [-1,0,1]
    p_dn = proba[:,0]; p_neu = proba[:,1]; p_up = proba[:,2]
    decided = np.argmax(proba, axis=1)  # 0/1/2 -> -1/0/1
    decided_label = np.vectorize(mapping.get)(decided)

    pred_rows = []
    for i, tid in enumerate(tickids):
        pred_rows.append({
            "tickid": int(tid),
            "model_id": model_id,
            "p_up": float(p_up[i]),
            "p_neu": float(p_neu[i]),
            "p_dn": float(p_dn[i]),
            "s_curve": None,  # set below for starts
            "decided_label": int(decided_label[i])
        })
    # Survival curve aggregated over test window for up continuation
    ds, S = survival_curve_up(lvl, step=0.10, maxd=5.0, horizon=10000)
    sc = compact_curve(ds, S)
    # Only attach s_curve to segment starts for payload size
    for i, row in enumerate(pred_rows):
        if is_start[i] and decided_label[i] == 1:  # Up starts by label AND predicted up is not required; still add generic curve
            row["s_curve"] = sc

    upsert_many("predictions", pred_rows)

    # Metrics
    f1 = f1_score(y_true, decided, average="macro")
    # Precision@UpStart: among labeled Up-start ticks, how often we predict Up
    mask_up_start = (is_start) & (y_true == 2)
    if mask_up_start.sum() > 0:
        prec_up = precision_score((y_true[mask_up_start]==2).astype(int), (decided[mask_up_start]==2).astype(int))
    else:
        prec_up = 0.0
    metrics = {
        "F1_macro": f1,
        "Precision_at_UpStart": prec_up,
        "S_curve_p($2)": float(S[int(2.0/0.10)] if len(S) > int(2.0/0.10) else S[-1]),
        "S_curve_p($3)": float(S[int(3.0/0.10)] if len(S) > int(3.0/0.10) else S[-1]),
        "S_curve_p($5)": float(S[int(5.0/0.10)] if len(S) > int(5.0/0.10) else S[-1]),
        "eval_sec": time.time() - t0
    }
    print(json.dumps({"model_id": model_id, "metrics": metrics}))

if __name__ == "__main__":
    main()
