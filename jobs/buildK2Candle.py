from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import psycopg2.extras

from backend.db import get_conn, dict_cur


@dataclass
class TickRow:
    id: int
    symbol: str
    timestamp: Any
    mid: Optional[float]
    spread: Optional[float]
    k2: Optional[float]


@dataclass
class CandleRow:
    symbol: str
    start_tick_id: int
    end_tick_id: int
    start_ts: Any
    end_ts: Any
    o: float
    h: float
    l: float
    c: float
    k2o: Optional[float]
    k2c: Optional[float]
    direction: int
    tick_count: int
    algo_version: str
    params: Dict[str, Any]


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return f
    except Exception:
        return None


def fetch_ticks(conn, symbol: str, hours: int) -> List[TickRow]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, mid, spread, k2
            FROM ticks
            WHERE symbol = %s
              AND timestamp >= now() - make_interval(hours => %s)
            ORDER BY id ASC
            """,
            (symbol, int(hours)),
        )
        rows = cur.fetchall()

    out: List[TickRow] = []
    for r in rows:
        out.append(
            TickRow(
                id=int(r["id"]),
                symbol=r["symbol"],
                timestamp=r["timestamp"],
                mid=_to_float(r.get("mid")),
                spread=_to_float(r.get("spread")),
                k2=_to_float(r.get("k2")),
            )
        )
    return out


def compute_dirs(ticks: List[TickRow]) -> List[int]:
    dirs: List[int] = []
    prev_basis: Optional[float] = None
    prev_nonzero_dir = 0

    for t in ticks:
        basis = t.k2 if t.k2 is not None else t.mid
        if basis is None:
            dirs.append(prev_nonzero_dir)
            continue

        if prev_basis is None:
            dirs.append(prev_nonzero_dir)
            prev_basis = basis
            continue

        delta = basis - prev_basis
        if delta > 0:
            d = 1
        elif delta < 0:
            d = -1
        else:
            d = prev_nonzero_dir

        dirs.append(d)
        if d != 0:
            prev_nonzero_dir = d
        prev_basis = basis

    return dirs


def make_candle(
    ticks: List[TickRow],
    start_idx: int,
    end_idx: int,
    direction: int,
    symbol: str,
    params: Dict[str, Any],
) -> Optional[CandleRow]:
    if start_idx < 0 or end_idx < start_idx or end_idx >= len(ticks):
        return None

    subset = ticks[start_idx : end_idx + 1]
    mids = [t.mid for t in subset if t.mid is not None]
    if not mids:
        return None

    return CandleRow(
        symbol=symbol,
        start_tick_id=subset[0].id,
        end_tick_id=subset[-1].id,
        start_ts=subset[0].timestamp,
        end_ts=subset[-1].timestamp,
        o=float(mids[0]),
        h=float(max(mids)),
        l=float(min(mids)),
        c=float(mids[-1]),
        k2o=subset[0].k2,
        k2c=subset[-1].k2,
        direction=1 if direction >= 0 else -1,
        tick_count=int(end_idx - start_idx + 1),
        algo_version="k2flip_v1",
        params=params,
    )


def build_candles(
    ticks: List[TickRow],
    min_flip_ticks: int,
    min_k2_move: float,
    min_mid_move: float,
    params: Dict[str, Any],
) -> List[CandleRow]:
    if not ticks:
        return []

    dirs = compute_dirs(ticks)
    candles: List[CandleRow] = []

    current_start = 0
    current_dir = 1
    for d in dirs:
        if d != 0:
            current_dir = d
            break

    candidate = None  # {start_idx, dir, run_count, k2_start, mid_start}

    for i in range(1, len(ticks)):
        d = dirs[i]
        if d == 0 or d == current_dir:
            candidate = None
            continue

        if candidate is None or candidate["dir"] != d:
            candidate = {
                "start_idx": i,
                "dir": d,
                "run_count": 1,
                "k2_start": ticks[i].k2,
                "mid_start": ticks[i].mid,
            }
        else:
            candidate["run_count"] += 1

        if candidate["run_count"] < min_flip_ticks:
            continue

        k2_now = ticks[i].k2
        k2_start = candidate["k2_start"]
        if k2_now is None or k2_start is None:
            k2_move_ok = True
        else:
            k2_move_ok = abs(k2_now - k2_start) >= min_k2_move

        mid_now = ticks[i].mid
        mid_start = candidate["mid_start"]
        if mid_now is None or mid_start is None:
            mid_move_ok = True
        else:
            mid_move_ok = abs(mid_now - mid_start) >= min_mid_move

        if not (k2_move_ok and mid_move_ok):
            continue

        # We close the previous candle at (t-1), and open the new one at t.
        close_idx = i - 1
        c = make_candle(ticks, current_start, close_idx, current_dir, ticks[0].symbol, params)
        if c is not None:
            candles.append(c)

        current_start = i
        current_dir = d
        candidate = None

    tail = make_candle(ticks, current_start, len(ticks) - 1, current_dir, ticks[0].symbol, params)
    if tail is not None:
        candles.append(tail)

    return candles


def delete_recent(conn, symbol: str, hours: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM k2_candles
            WHERE symbol = %s
              AND end_ts >= now() - make_interval(hours => %s)
            """,
            (symbol, int(hours)),
        )


def insert_candles(conn, candles: List[CandleRow]) -> None:
    if not candles:
        return

    rows = [
        (
            c.symbol,
            c.start_tick_id,
            c.end_tick_id,
            c.start_ts,
            c.end_ts,
            c.o,
            c.h,
            c.l,
            c.c,
            c.k2o,
            c.k2c,
            c.direction,
            c.tick_count,
            c.algo_version,
            json.dumps(c.params, separators=(",", ":")),
        )
        for c in candles
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO k2_candles (
                symbol, start_tick_id, end_tick_id, start_ts, end_ts,
                open, high, low, close,
                k2_open, k2_close, dir, tick_count,
                algo_version, params
            )
            VALUES %s
            """,
            rows,
            page_size=min(1000, len(rows)),
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build K2 flip candles for recent hours")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--hours", type=int, default=2)
    p.add_argument("--min_flip_ticks", type=int, default=3)
    p.add_argument("--min_k2_move", type=float, default=0.05)
    p.add_argument("--min_mid_move", type=float, default=0.10)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    symbol = (args.symbol or "XAUUSD").strip() or "XAUUSD"
    hours = max(1, int(args.hours))
    min_flip_ticks = max(1, int(args.min_flip_ticks))
    min_k2_move = max(0.0, float(args.min_k2_move))
    min_mid_move = max(0.0, float(args.min_mid_move))

    params = {
        "hours": hours,
        "min_flip_ticks": min_flip_ticks,
        "min_k2_move": min_k2_move,
        "min_mid_move": min_mid_move,
        "close_rule": "close_at_tminus1",
        "direction_source": "k2_then_mid",
    }

    conn = get_conn()
    conn.autocommit = False
    try:
        ticks = fetch_ticks(conn, symbol=symbol, hours=hours)
        delete_recent(conn, symbol=symbol, hours=hours)

        if not ticks:
            conn.commit()
            print(f"[buildK2Candle] symbol={symbol} ticks=0 candles=0")
            return

        candles = build_candles(
            ticks=ticks,
            min_flip_ticks=min_flip_ticks,
            min_k2_move=min_k2_move,
            min_mid_move=min_mid_move,
            params=params,
        )

        insert_candles(conn, candles)
        conn.commit()

        print(f"[buildK2Candle] symbol={symbol} ticks={len(ticks)} candles={len(candles)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
