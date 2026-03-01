from __future__ import annotations

import argparse
import itertools
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from jobs.backtest_week import run_backtest
from jobs.strategy_core import StrategyConfig, profit_variance


ROOT = Path(__file__).resolve().parents[1]
CFG_DIR = ROOT / "runtime" / "configs"
RPT_DIR = ROOT / "runtime" / "reports"


def _parse_day(v: str) -> date:
    return date.fromisoformat(v)


def _daterange_end(days: int, end_day: date) -> date:
    return end_day - timedelta(days=max(1, days) - 1)


def _grid() -> List[StrategyConfig]:
    s_candidates = [0.0005, 0.0010, 0.0015]
    out: List[StrategyConfig] = []
    for n, r2_min, s_min, spread_mult, sigma_mult, cooldown_sec in itertools.product(
        [40, 60, 80],
        [0.82, 0.86, 0.90, 0.94],
        s_candidates,
        [1.5, 2.0, 2.5],
        [1.0, 1.5, 2.0],
        [30, 60, 90, 120],
    ):
        out.append(
            StrategyConfig(
                n=n,
                r2_min=r2_min,
                s_min=s_min,
                spread_mult=spread_mult,
                sigma_mult=sigma_mult,
                cooldown_sec=cooldown_sec,
            )
        )
    return out


def _evaluate(symbol: str, start_day: date, days: int, cfg: StrategyConfig) -> Dict[str, Any]:
    rows = run_backtest(symbol=symbol, start_day=start_day, days=days, cfg=cfg, outdb=False)
    in_range_days = sum(1 for r in rows if 4 <= int(r["trades_count"]) <= 20)
    max_hold_ok = all(int(r["max_hold_sec"]) <= 300 for r in rows)
    total_profit = sum(float(r["total_profit"]) for r in rows)
    stopouts = sum(int(r["stopouts_count"]) for r in rows)
    score = {
        "cfg": cfg.to_json(),
        "rows": rows,
        "in_range_days": in_range_days,
        "max_hold_ok": max_hold_ok,
        "total_profit": total_profit,
        "profit_var": profit_variance(rows),
        "stopouts_count": stopouts,
        "passes": in_range_days >= min(5, len(rows)) and max_hold_ok,
    }
    return score


def _sort_key(res: Dict[str, Any]) -> tuple:
    # passing configs first, then ranking:
    # 1) highest total_profit, 2) lower variance, 3) lower stopouts_count
    return (
        0 if res["passes"] else 1,
        -float(res["total_profit"]),
        float(res["profit_var"]),
        int(res["stopouts_count"]),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Grid-search tuner over the last N trading days.")
    p.add_argument("--symbol", required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--end", default=None, help="End trading day YYYY-MM-DD (default: UTC today)")
    p.add_argument("--write_backtest", type=int, choices=[0, 1], default=0)
    args = p.parse_args()

    end_day = _parse_day(args.end) if args.end else datetime.now(timezone.utc).date()
    start_day = _daterange_end(args.days, end_day)

    results: List[Dict[str, Any]] = []
    for i, cfg in enumerate(_grid(), start=1):
        res = _evaluate(args.symbol, start_day, args.days, cfg)
        results.append(res)
        print(
            f"[{i}] pass={res['passes']} pnl={res['total_profit']:.3f} "
            f"var={res['profit_var']:.6f} in_range_days={res['in_range_days']}"
        )

    ranked = sorted(results, key=_sort_key)
    best = ranked[0] if ranked else None
    promoted = bool(best and best["passes"])

    CFG_DIR.mkdir(parents=True, exist_ok=True)
    RPT_DIR.mkdir(parents=True, exist_ok=True)

    if promoted:
        cfg_path = CFG_DIR / "live_strategy.json"
        cfg_path.write_text(json.dumps(best["cfg"], indent=2), encoding="utf-8")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    rpt_path = RPT_DIR / f"tune_{stamp}.json"
    report = {
        "symbol": args.symbol,
        "days": args.days,
        "start_day": start_day.isoformat(),
        "end_day": end_day.isoformat(),
        "passes_rule": ">=5 days with 4..20 trades AND max_hold_sec<=300 on all days",
        "promoted": promoted,
        "best": best,
        "top10": ranked[:10],
    }
    rpt_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    if args.write_backtest and best:
        run_backtest(
            symbol=args.symbol,
            start_day=start_day,
            days=args.days,
            cfg=StrategyConfig(**best["cfg"]),
            outdb=True,
        )

    print(json.dumps({"promoted": promoted, "report": str(rpt_path)}, indent=2))


if __name__ == "__main__":
    main()
