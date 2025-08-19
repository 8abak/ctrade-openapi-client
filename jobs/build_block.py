# jobs/build_block.py
import argparse, json, time
from ml.db import fetch_ticks
from ml.kalman import run_kalman, persist_kalman
from ml.features import build_features, persist_features
from ml.labeler import label_trend, persist_labels

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    args = ap.parse_args()

    t0 = time.time()
    ticks = fetch_ticks(args.start, args.end)
    kal = run_kalman(ticks)
    n_k = persist_kalman(kal)

    feats = build_features(kal)
    n_f = persist_features(feats)

    labels = label_trend(feats)
    n_l = persist_labels(labels)

    print(json.dumps({
        "built_range": [args.start, args.end],
        "kalman": n_k, "features": n_f, "labels": n_l,
        "sec": time.time()-t0
    }))

if __name__ == "__main__":
    main()
