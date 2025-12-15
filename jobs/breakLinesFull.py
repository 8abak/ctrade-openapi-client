# PATH: backend/jobs/breakLinesFull.py

"""
Break segLines repeatedly until global_max_abs_dist < threshold.

This uses jobs/breakLine.py as the single source of truth.
It will:
- create the initial root segline if none exist yet (init mode)
- otherwise split the worst active line (highest max_abs_dist)
- after each step, distances + max_abs_dist are updated by break_line()

Usage:
  python -m backend.jobs.breakLinesFull --segm 117 --threshold 3.0
or:
  python backend/jobs/breakLinesFull.py --segm 117 --threshold 3.0
"""

import argparse
from typing import Optional

from backend.jobs.breakLine import break_line


def run(segm_id: int, threshold: float, max_steps: Optional[int] = None) -> None:
    step = 0
    while True:
        step += 1
        out = break_line(segm_id=segm_id, segLine_id=None)  # None => init or split-worst

        if "error" in out:
            raise SystemExit(f"[breakLinesFull] ERROR: {out}")

        action = out.get("action")
        global_max = out.get("global_max_abs_dist")

        print(
            f"[breakLinesFull] step={step} segm_id={segm_id} action={action} "
            f"num_active={out.get('num_lines_active')} global_max_abs_dist={global_max}"
        )

        # stop conditions
        if global_max is None:
            print("[breakLinesFull] ✓ global_max_abs_dist is None (no dists?). Done.")
            break

        if float(global_max) < float(threshold):
            print(f"[breakLinesFull] ✓ Done. global_max_abs_dist={global_max} < threshold={threshold}")
            break

        if max_steps is not None and step >= int(max_steps):
            print(f"[breakLinesFull] Stop: reached max_steps={max_steps}")
            break


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm", type=int, required=True, help="segms.id to process")
    ap.add_argument("--threshold", type=float, default=3.0, help="stop when global max abs dist < threshold")
    ap.add_argument("--max-steps", type=int, default=None, help="optional safety limit")
    args = ap.parse_args()

    run(segm_id=args.segm, threshold=args.threshold, max_steps=args.max_steps)
