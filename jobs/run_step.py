# jobs/run_step.py
import argparse, json, subprocess, sys, time, uuid
from ml.db import upsert_walk_run

def run_cmd(mod: str, args: list, timeout: int=1800):
    cmd = [sys.executable, "-m", mod] + args
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"{mod} failed: {p.stderr}")
    # expect JSON on stdout
    out = p.stdout.strip().splitlines()[-1]
    return json.loads(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--block", type=int, default=100000)
    ap.add_argument("--algo", type=str, choices=["sgd","lgbm"], default="sgd")
    args = ap.parse_args()

    train_start = int(args.start)
    train_end   = train_start + args.block - 1
    test_start  = train_end + 1
    test_end    = test_start + args.block - 1

    # Build features/labels for train and test windows
    b1 = run_cmd("jobs.build_block", ["--start", str(train_start), "--end", str(train_end)])
    b2 = run_cmd("jobs.build_block", ["--start", str(test_start), "--end", str(test_end)])

    # Train
    tinfo = run_cmd("jobs.train_block", ["--start", str(train_start), "--end", str(train_end), "--algo", args.algo])
    model_id = tinfo["model_id"]

    # Evaluate
    einfo = run_cmd("jobs.eval_block", ["--start", str(test_start), "--end", str(test_end), "--model", model_id, "--algo", args.algo])

    run_id = f"run-{args.algo}-{train_start}-{train_end}-{uuid.uuid4().hex[:6]}"
    run_row = {
        "run_id": run_id,
        "train_start": train_start,
        "train_end": train_end,
        "test_start": test_start,
        "test_end": test_end,
        "model_id": model_id,
        "metrics": json.dumps(einfo["metrics"]),
        "confirmed": False
    }
    upsert_walk_run(run_row)

    print(json.dumps({
        "run_id": run_id,
        "train_range": [train_start, train_end],
        "test_range": [test_start, test_end],
        "model_id": model_id,
        "metrics": einfo["metrics"],
        "ready_for_review": True
    }))

if __name__ == "__main__":
    main()
