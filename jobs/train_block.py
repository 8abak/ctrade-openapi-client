# jobs/train_block.py
import argparse, json
from ml.train_sgd import train_and_calibrate as train_sgd
from ml.train_lgbm import train_and_calibrate as train_lgbm

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--algo", type=str, choices=["sgd","lgbm"], default="sgd")
    args = ap.parse_args()

    if args.algo == "sgd":
        model_id = train_sgd(args.start, args.end)
    else:
        model_id = train_lgbm(args.start, args.end)

    print(json.dumps({"model_id": model_id}))

if __name__ == "__main__":
    main()

