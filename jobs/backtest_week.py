from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List

from backend.db import fetch_ticks_for_range, get_conn, upsert_backtest_row
from jobs.strategy_core import (
    AUS_TZ,
    StrategyConfig,
    run_backtest_ticks,
    session_label_for_start,
    split_sessions_by_gap,
    summarize_trades,
)


def _parse_day(v: str) -> date:
    return date.fromisoformat(v)


def run_backtest(
    *,
    symbol: str,
    start_day: date,
    days: int,
    gap_sec: int,
    cfg: StrategyConfig,
    outdb: bool,
) -> List[Dict]:
    rows: List[Dict] = []
    conn = get_conn()
    try:
        start_local = datetime.combine(start_day, time.min, tzinfo=AUS_TZ)
        end_local = start_local + timedelta(days=max(1, days))
        range_start = start_local.astimezone(timezone.utc)
        range_end = end_local.astimezone(timezone.utc)
        ticks = fetch_ticks_for_range(conn, symbol, range_start, range_end)
        sessions = split_sessions_by_gap(ticks, gap_sec=gap_sec)

        for sess in sessions:
            session_start_ts = sess["start_ts"]
            session_end_ts = sess["end_ts"]
            session_ticks = sess["ticks"]
            trading_day = session_label_for_start(session_start_ts)
            trades = run_backtest_ticks(session_ticks, cfg)

            row = summarize_trades(
                trades=trades,
                trading_day=trading_day,
                session_start_ts=session_start_ts,
                session_end_ts=session_end_ts,
                symbol=symbol,
                config=cfg,
                notes=f"ticks={len(session_ticks)};gap_sec={int(gap_sec)}",
            )
            rows.append(row)

            if outdb:
                upsert_backtest_row(conn, **row)

            print(
                f"{trading_day} {session_start_ts.isoformat()}..{session_end_ts.isoformat()} "
                f"trades={row['trades_count']} wins={row['wins_count']} "
                f"losses={row['losses_count']} pnl={row['total_profit']:.3f} "
                f"hold_max={row['max_hold_sec']}"
            )
    finally:
        conn.close()
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Run deterministic weekly backtest over ticks.")
    p.add_argument("--symbol", required=True, help="e.g. XAUUSD")
    p.add_argument("--start", required=True, help="YYYY-MM-DD trading day label")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--gap-sec", type=int, default=3300)
    p.add_argument("--outdb", type=int, choices=[0, 1], default=1)

    # tunable strategy params
    p.add_argument("--n", type=int, default=60)
    p.add_argument("--r2_min", type=float, default=0.86)
    p.add_argument("--s_min", type=float, default=0.014)
    p.add_argument("--spread_mult", type=float, default=2.0)
    p.add_argument("--sigma_mult", type=float, default=1.5)
    p.add_argument("--cooldown_sec", type=int, default=60)
    p.add_argument("--buffer_min", type=float, default=0.05)

    args = p.parse_args()
    cfg = StrategyConfig(
        n=args.n,
        r2_min=args.r2_min,
        s_min=args.s_min,
        spread_mult=args.spread_mult,
        sigma_mult=args.sigma_mult,
        cooldown_sec=args.cooldown_sec,
        buffer_min=args.buffer_min,
    )

    rows = run_backtest(
        symbol=args.symbol,
        start_day=_parse_day(args.start),
        days=max(1, args.days),
        gap_sec=max(1, args.gap_sec),
        cfg=cfg,
        outdb=bool(args.outdb),
    )
    total_pnl = sum(float(r["total_profit"]) for r in rows)
    print(json.dumps({"sessions": len(rows), "symbol": args.symbol, "total_profit": total_pnl}, indent=2))


if __name__ == "__main__":
    main()
