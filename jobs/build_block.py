# jobs/build_block.py — REPLACE ENTIRE FILE
import argparse
import json
import time

from ml.kalman import run_kalman
from ml.features import build_features_range
from ml.labeler import build_labels_range

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    args = ap.parse_args()

    t0 = time.time()
    n_kal = run_kalman(args.start, args.end)
    t1 = time.time()
    n_feat = build_features_range(args.start, args.end)
    t2 = time.time()
    n_lab = build_labels_range(args.start, args.end)
    t3 = time.time()

    print(json.dumps({
        "range": [args.start, args.end],
        "kalman_rows": n_kal,
        "features_rows": n_feat,
        "labels_rows": n_lab,
        "timings_sec": {
            "kalman": round(t1 - t0, 3),
            "features": round(t2 - t1, 3),
            "labels": round(t3 - t2, 3),
            "total": round(t3 - t0, 3),
        }
    }))

if __name__ == "__main__":
    main()
