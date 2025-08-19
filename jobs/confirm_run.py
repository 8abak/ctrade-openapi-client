# jobs/confirm_run.py
import argparse, json
from ml.db import mark_run_confirmed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_id", required=True)
    args = ap.parse_args()
    mark_run_confirmed(args.run_id)
    print(json.dumps({"ok": True, "run_id": args.run_id}))

if __name__ == "__main__":
    main()
