from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jobs.tune_week import _evaluate, _grid, _sort_key


ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "runtime" / "configs" / "live_strategy.json"
RPT_DIR = ROOT / "runtime" / "reports"


def main() -> None:
    p = argparse.ArgumentParser(description="Nightly tuner + safe config promotion.")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--gap-sec", type=int, default=3300)
    p.add_argument("--end", default=None, help="YYYY-MM-DD trading day (default UTC today)")
    args = p.parse_args()

    end_day = datetime.fromisoformat(args.end).date() if args.end else datetime.now(timezone.utc).date()
    start_day = end_day - timedelta(days=max(1, args.days) - 1)

    gap_sec = max(1, args.gap_sec)
    results = [_evaluate(args.symbol, start_day, args.days, gap_sec, cfg) for cfg in _grid()]
    ranked = sorted(results, key=_sort_key)
    best = ranked[0] if ranked else None

    promote = False
    reason = "no candidate"
    if best:
        unstable = float(best["profit_var"]) > 5.0
        positive = float(best["total_profit"]) > 0.0
        if best["passes"] and positive and not unstable:
            promote = True
            reason = "best passed; positive profit; acceptable variance"
        else:
            reason = (
                f"kept previous config (passes={best['passes']}, "
                f"positive_profit={positive}, unstable={unstable})"
            )

    prev_cfg = None
    if CFG_PATH.exists():
        prev_cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))

    if promote and best:
        CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CFG_PATH.write_text(json.dumps(best["cfg"], indent=2), encoding="utf-8")

    RPT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    report_path = RPT_DIR / f"nightly_{stamp}.json"
    report = {
        "symbol": args.symbol,
        "days": args.days,
        "gap_sec": gap_sec,
        "start_day": start_day.isoformat(),
        "end_day": end_day.isoformat(),
        "promoted": promote,
        "reason": reason,
        "best": best,
        "previous_config": prev_cfg,
        "active_config": best["cfg"] if (promote and best) else prev_cfg,
    }
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"promoted": promote, "reason": reason, "report": str(report_path)}, indent=2))


if __name__ == "__main__":
    main()
