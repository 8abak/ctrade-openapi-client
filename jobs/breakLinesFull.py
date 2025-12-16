# jobs/breakLinesFull.py
"""
Break segLines repeatedly until global_max_abs_dist < threshold.

Journal behaviour requested:
- breakLinesFull: write "started" once at the beginning and "finished" once at the end.
- It calls break_line() internally many times, but we do NOT re-journal "started" per step here.
"""

import argparse
from typing import Optional

from jobs.breakLine import break_line

# Optional journal (works in your repo: backend/jobs/journal.py)
try:
    from backend.jobs.journal import write_journal  # type: ignore
except Exception:  # pragma: no cover
    write_journal = None  # type: ignore


def _journal(msg: str) -> None:
    if write_journal is not None:
        try:
            write_journal(msg)
            return
        except Exception:
            pass
    # No hard failure if journal isn't available.
    print(f"[journal-fallback] {msg}")


def run(segm_id: int, threshold: float, max_steps: Optional[int] = None, *, price_source: str = "mid") -> None:
    _journal(f"breakLinesFull started segm_id={segm_id} threshold={threshold} max_steps={max_steps} price_source={price_source}")

    step = 0
    last_out = None
    try:
        while True:
            step += 1

            out = break_line(segm_id=segm_id, segLine_id=None, price_source=price_source)
            last_out = out

            if not isinstance(out, dict):
                raise SystemExit(f"[breakLinesFull] ERROR: unexpected return type: {type(out)}")

            if "error" in out:
                raise SystemExit(f"[breakLinesFull] ERROR: {out}")

            action = out.get("action")
            global_max = out.get("global_max_abs_dist")
            num_active = out.get("num_lines_active")

            print(
                f"[breakLinesFull] step={step} segm_id={segm_id} action={action} "
                f"active={num_active} global_max_abs_dist={global_max}"
            )

            # stop conditions
            if global_max is None:
                print("[breakLinesFull] ✓ Done (global_max_abs_dist is None).")
                break

            if float(global_max) < float(threshold):
                print(f"[breakLinesFull] ✓ Done. global_max_abs_dist={global_max} < threshold={threshold}")
                break

            if max_steps is not None and step >= int(max_steps):
                print(f"[breakLinesFull] Stop: reached max_steps={max_steps}")
                break

    finally:
        # Always record finish (success or fail)
        ok = True
        err = None
        if isinstance(last_out, dict) and "error" in last_out:
            ok = False
            err = str(last_out.get("error"))
        _journal(f"breakLinesFull finished segm_id={segm_id} ok={ok} err={err}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm", type=int, required=True, help="segms.id to process")
    ap.add_argument("--threshold", type=float, default=3.0, help="stop when global max abs dist < threshold")
    ap.add_argument("--max-steps", type=int, default=None, help="optional safety limit")
    ap.add_argument("--price-source", type=str, default="mid", help="mid|kal (default mid)")
    args = ap.parse_args()

    run(segm_id=args.segm, threshold=args.threshold, max_steps=args.max_steps, price_source=args.price_source)
