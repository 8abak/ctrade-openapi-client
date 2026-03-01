from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from backend.db import dict_cur, get_conn
from jobs.strategy_core import StrategyConfig, StrategyEngine


ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "runtime" / "configs" / "live_strategy.json"


class Executor:
    def place_order(self, side: str, qty: float, price: float, sl: float, tp: float) -> None:
        raise NotImplementedError

    def close_position(self, side: str, qty: float, price: float, reason: str) -> None:
        raise NotImplementedError


class PaperExecutor(Executor):
    def place_order(self, side: str, qty: float, price: float, sl: float, tp: float) -> None:
        print(f"[PAPER] OPEN side={side} qty={qty} entry={price:.5f} sl={sl:.5f} tp={tp:.5f}")

    def close_position(self, side: str, qty: float, price: float, reason: str) -> None:
        print(f"[PAPER] CLOSE side={side} qty={qty} price={price:.5f} reason={reason}")


class LiveExecutor(Executor):
    def __init__(self) -> None:
        raise RuntimeError(
            "LiveExecutor is intentionally disabled by default. "
            "Integrate a verified execution adapter before mode=live."
        )

    def place_order(self, side: str, qty: float, price: float, sl: float, tp: float) -> None:
        pass

    def close_position(self, side: str, qty: float, price: float, reason: str) -> None:
        pass


def load_cfg() -> StrategyConfig:
    if not CFG_PATH.exists():
        print(f"Config not found at {CFG_PATH}; using defaults.")
        return StrategyConfig()
    raw = json.loads(CFG_PATH.read_text(encoding="utf-8"))
    return StrategyConfig(**raw)


def fetch_new_ticks(conn, symbol: str, last_id: int, limit: int = 5000) -> List[Dict[str, Any]]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread, kal, k2
            FROM public.ticks
            WHERE symbol=%s
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, int(last_id), int(limit)),
        )
        rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "symbol": r["symbol"],
                "timestamp": r["timestamp"],
                "bid": float(r["bid"]) if r["bid"] is not None else None,
                "ask": float(r["ask"]) if r["ask"] is not None else None,
                "mid": float(r["mid"]) if r["mid"] is not None else None,
                "spread": float(r["spread"]) if r["spread"] is not None else None,
                "kal": float(r["kal"]) if r["kal"] is not None else None,
                "k2": float(r["k2"]) if r["k2"] is not None else None,
            }
        )
    return out


def fetch_last_id(conn, symbol: str) -> int:
    with dict_cur(conn) as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) AS mx FROM public.ticks WHERE symbol=%s", (symbol,))
        row = cur.fetchone()
    return int(row["mx"]) if row and row["mx"] is not None else 0


def main() -> None:
    p = argparse.ArgumentParser(description="Safe live robot skeleton (paper by default).")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--mode", choices=["paper", "live"], default="paper")
    p.add_argument("--qty", type=float, default=1.0)
    p.add_argument("--poll_sec", type=float, default=2.0)
    p.add_argument("--max_loops", type=int, default=0, help="0 = infinite")
    args = p.parse_args()

    cfg = load_cfg()
    executor: Executor = PaperExecutor() if args.mode == "paper" else LiveExecutor()
    engine = StrategyEngine(cfg)

    conn = get_conn()
    pnl_sum = 0.0
    loops = 0
    last_id = fetch_last_id(conn, args.symbol)
    print(f"Live robot started mode={args.mode} symbol={args.symbol} from last_id={last_id}")

    try:
        while True:
            loops += 1
            ticks = fetch_new_ticks(conn, args.symbol, last_id, limit=2000)
            if ticks:
                for t in ticks:
                    evt = engine.process_tick(t)
                    if evt["opened"] is not None:
                        o = evt["opened"]
                        executor.place_order(o.side, args.qty, o.entry_price, o.sl_price, o.tp_price)
                        print(f"[OPEN] id={o.tick_id} ts={o.ts.isoformat()} side={o.side}")
                    if evt["closed"] is not None:
                        c = evt["closed"]
                        executor.close_position(c.side, args.qty, c.exit_price, c.exit_reason)
                        pnl_sum += c.pnl
                        print(
                            f"[CLOSE] id={c.exit_tick_id} ts={c.exit_ts.isoformat()} "
                            f"reason={c.exit_reason} hold={c.hold_sec}s pnl={c.pnl:.5f} total={pnl_sum:.5f}"
                        )
                last_id = int(ticks[-1]["id"])

            if args.max_loops > 0 and loops >= args.max_loops:
                break
            time.sleep(max(0.2, args.poll_sec))
    finally:
        conn.close()
        print(f"Stopped. cumulative_pnl={pnl_sum:.5f}")


if __name__ == "__main__":
    main()
