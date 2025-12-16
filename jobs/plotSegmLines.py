# jobs/plotSegmLines.py
# Purpose: export a PNG that shows one segm's price series (kal by default) with active segLines overlaid.

import argparse
import os
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # server-safe (no GUI)
import matplotlib.pyplot as plt

from backend.db import get_conn, review_ticks_sample, review_active_lines


def _parse_iso(ts: str) -> datetime:
    # db.py returns ISO strings via datetime.isoformat() (no 'Z')
    return datetime.fromisoformat(ts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, required=True)
    ap.add_argument("--target-points", type=int, default=50000, help="downsample target (max 50000)")
    ap.add_argument("--series", choices=["kal", "mid", "bid", "ask"], default="kal")
    ap.add_argument("--out", default="", help="output png path (default: ./publicResults/segm_<id>_lines.png)")
    args = ap.parse_args()

    segm_id = int(args.segm_id)
    target_points = int(args.target_points)

    conn = get_conn()
    try:
        tick_pack = review_ticks_sample(conn, segm_id=segm_id, target_points=target_points)
        line_pack = review_active_lines(conn, segm_id=segm_id)
    finally:
        conn.close()

    points = tick_pack.get("points") or []
    lines = line_pack.get("lines") or []

    if not points:
        raise SystemExit(f"No ticks returned for segm_id={segm_id}")

    xs = [_parse_iso(p["ts"]) for p in points]
    ys = [p.get(args.series) for p in points]

    # Drop None values
    xs2, ys2 = [], []
    for x, y in zip(xs, ys):
        if y is None:
            continue
        xs2.append(x)
        ys2.append(float(y))

    out_path = args.out.strip() or os.path.join("publicResults", f"segm_{segm_id}_lines.png")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    plt.figure(figsize=(20, 8))
    plt.plot(xs2, ys2, linewidth=0.8)
    plt.title(f"segm {segm_id}  ({args.series})  points={len(xs2)}  stride={tick_pack.get('stride')}  lines={len(lines)}")
    plt.xlabel("time")
    plt.ylabel(args.series)

    # Overlay segLines
    for ln in lines:
        x0 = _parse_iso(ln["start_ts"])
        x1 = _parse_iso(ln["end_ts"])
        y0 = float(ln["start_price"])
        y1 = float(ln["end_price"])
        plt.plot([x0, x1], [y0, y1], linewidth=2.2)

    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
