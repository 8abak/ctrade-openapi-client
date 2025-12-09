# jobs/buildConfirmSpots.py
#
# Build "confirmation spot" labels from evals (L5+) and ticks.
#
# Usage example (from repo root):
#   python -m jobs.buildConfirmSpots \
#       --symbol XAUUSD \
#       --start 2025-07-01T08:00:00Z \
#       --end   2025-07-01T22:00:00Z \
#       --out-dir train/confirm_spots
#
# Summary of behaviour:
#   1) Loads eval ticks from DB for a symbol + time range.
#   2) Finds L5+ pivot anchors that are local extrema in +/- W_loc ticks.
#   3) Around each pivot, detects a confirmation pattern where:
#        - L1 / H1 are chosen among level >= 2 ticks.
#        - Confirmation tick is also a level >= 2 tick.
#   4) Computes trade outcome aiming to capture at least a fraction
#      (target_frac_of_wave) of the max favourable move after confirmation.
#   5) Saves all confirmations to CSV and prints basic stats.

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

# Adjust this import / function name if your DB helper is different.
from backend import db as dbmod  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ConfirmConfig:
    # Local extremum window: number of ticks before/after pivot.
    # This is your "tick window before and after around 400 ticks".
    W_loc: int = 400

    # Search horizons (number of ticks) for each stage
    N1: int = 200   # first drop / first push after pivot
    N2: int = 200   # retest high/low after L1/H1
    N3: int = 400   # confirmation horizon after retest

    # Wave scanning horizon after confirmation (ticks)
    # Used both to measure max favourable excursion and to search for exit.
    N_wave: int = 800

    # Target fraction of the move we want to capture.
    # 0.5 = aim to get at least half of the move after confirmation.
    target_frac_of_wave: float = 0.5

    # Price thresholds (all in price units)
    drop_min: float = 0.5       # min H0->L1 (short) or L0->H1 (long)
    bounce_min: float = 0.3     # min L1->H1 (short) or H1->L1 (long)
    small_buffer: float = 0.1   # H1 lower than H0 (short), or L1 higher than L0 (long)
    break_buffer: float = 0.0   # amount beyond L1/H1 for a "real" break (0.0 = at L1)

    SL_buffer: float = 0.5      # stop distance beyond H0/L0

    # Trading costs (spread + fees) in price units
    cost_per_trade: float = 0.1

    # Minimum eval level to treat as "level two" structure.
    # If you want exactly level == 2, change this logic in code where used.
    min_struct_level: int = 2


@dataclass
class EvalTick:
    idx: int           # index in in-memory list
    tick_id: int
    ts: datetime
    mid: float
    level: int


# ---------------------------------------------------------------------------
# DB loading
# ---------------------------------------------------------------------------

def load_eval_ticks(
    conn,
    symbol: Optional[str],
    start_ts: datetime,
    end_ts: datetime,
) -> List[EvalTick]:
    """
    Load eval ticks joined with ticks for a given symbol and time window.

    Uses evals.mid, evals.level, evals.timestamp and ticks.symbol.
    """

    sql = """
        SELECT e.tick_id,
               e.timestamp,
               e.mid::double precision,
               e.level
        FROM evals e
        JOIN ticks t ON e.tick_id = t.id
        WHERE (%(symbol)s IS NULL OR t.symbol = %(symbol)s)
          AND e.timestamp BETWEEN %(start)s AND %(end)s
        ORDER BY e.tick_id
    """

    params = {"symbol": symbol, "start": start_ts, "end": end_ts}
    cur = conn.cursor()
    cur.execute(sql, params)

    rows = cur.fetchall()
    eval_ticks: List[EvalTick] = []

    for i, (tick_id, ts, mid, level) in enumerate(rows):
        # Ensure timezone-aware UTC
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        eval_ticks.append(
            EvalTick(
                idx=i,
                tick_id=int(tick_id),
                ts=ts,
                mid=float(mid),
                level=int(level),
            )
        )

    return eval_ticks


# ---------------------------------------------------------------------------
# Step 1 – Pivot detection (L5+ local extrema in +/- W_loc ticks)
# ---------------------------------------------------------------------------

def find_pivots(eval_ticks: List[EvalTick], cfg: ConfirmConfig) -> List[Dict[str, Any]]:
    pivots: List[Dict[str, Any]] = []
    W = cfg.W_loc
    n = len(eval_ticks)

    for i, et in enumerate(eval_ticks):
        # Only consider level >= 5 as pivot anchors
        if et.level < 5:
            continue

        lo = max(0, i - W)
        hi = min(n - 1, i + W)

        window = eval_ticks[lo : hi + 1]
        if not window:
            continue

        mids = [w.mid for w in window]
        mid_i = et.mid
        max_mid = max(mids)
        min_mid = min(mids)

        # Allow exact equality for flat tops/bottoms
        is_local_high = abs(mid_i - max_mid) < 1e-12
        is_local_low = abs(mid_i - min_mid) < 1e-12

        if not (is_local_high or is_local_low):
            continue

        # Resolve flat case arbitrarily; you can refine later
        if is_local_high and not is_local_low:
            ptype = "high"
        elif is_local_low and not is_local_high:
            ptype = "low"
        else:
            # Flat zone around this tick: default to "high"
            ptype = "high"

        pivots.append(
            {
                "pivot_idx": i,
                "pivot_tick_id": et.tick_id,
                "pivot_time": et.ts,
                "pivot_price": et.mid,
                "pivot_eval_level": et.level,
                "pivot_type": ptype,
            }
        )

    return pivots


# ---------------------------------------------------------------------------
# Step 2 – Confirmation pattern detection (using level>=2 for L1/H1/confirm)
# ---------------------------------------------------------------------------

def detect_confirmation_for_pivot(
    eval_ticks: List[EvalTick],
    pivot: Dict[str, Any],
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    pidx = pivot["pivot_idx"]
    ptype = pivot["pivot_type"]

    if ptype == "high":
        return _detect_short(eval_ticks, pidx, cfg)
    else:
        return _detect_long(eval_ticks, pidx, cfg)


def _detect_short(
    eval_ticks: List[EvalTick],
    pivot_idx: int,
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    """
    Short setup around a high pivot, using level>=2 structure:

    H0 = pivot high (L5+)
    L1 = first level>=2 low after H0 (within N1 ticks)
    H1 = level>=2 retest high after L1 (within N2 ticks), lower than H0
    confirm = first level>=2 tick after H1 with price <= L1 - break_buffer (within N3 ticks)
    """

    n = len(eval_ticks)
    if pivot_idx >= n - 2:
        return None

    H0 = eval_ticks[pivot_idx].mid
    min_struct_level = cfg.min_struct_level

    # 1) First drop low L1 (among level>=2 ticks)
    j_start = pivot_idx + 1
    j_end = min(n, pivot_idx + 1 + cfg.N1)
    if j_start >= j_end:
        return None

    j_candidates = [et for et in eval_ticks[j_start:j_end] if et.level >= min_struct_level]
    if not j_candidates:
        return None

    L1 = min(j_candidates, key=lambda et: et.mid)
    L1_idx = L1.idx

    if H0 - L1.mid < cfg.drop_min:
        return None

    # 2) Retest high H1 (lower high, level>=2)
    k_start = L1_idx + 1
    k_end = min(n, L1_idx + 1 + cfg.N2)
    if k_start >= k_end:
        return None

    k_candidates = [et for et in eval_ticks[k_start:k_end] if et.level >= min_struct_level]
    if not k_candidates:
        return None

    H1 = max(k_candidates, key=lambda et: et.mid)
    H1_idx = H1.idx

    # Real bounce
    if H1.mid - L1.mid < cfg.bounce_min:
        return None

    # Lower high than pivot
    if H1.mid > H0 - cfg.small_buffer:
        return None

    # 3) Confirmation: break of L1 by a level>=2 tick
    m_start = H1_idx + 1
    m_end = min(n, H1_idx + 1 + cfg.N3)
    if m_start >= m_end:
        return None

    confirm_idx: Optional[int] = None
    for m in range(m_start, m_end):
        et = eval_ticks[m]
        if et.level >= min_struct_level and et.mid <= L1.mid - cfg.break_buffer:
            confirm_idx = m
            break

    if confirm_idx is None:
        return None

    stop_price = H0 + cfg.SL_buffer

    return {
        "pivot_idx": pivot_idx,
        "L1_idx": L1_idx,
        "H1_idx": H1_idx,
        "confirm_idx": confirm_idx,
        "side": "short",
        "stop_price": stop_price,
    }


def _detect_long(
    eval_ticks: List[EvalTick],
    pivot_idx: int,
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    """
    Long setup around a low pivot (mirrored), using level>=2 structure:

    L0 = pivot low (L5+)
    H1 = first level>=2 high after L0 (within N1 ticks)
    L1 = level>=2 retest low after H1 (within N2 ticks), higher than L0
    confirm = first level>=2 tick after L1 with price >= L1 + break_buffer (within N3 ticks)
    """

    n = len(eval_ticks)
    if pivot_idx >= n - 2:
        return None

    L0 = eval_ticks[pivot_idx].mid
    min_struct_level = cfg.min_struct_level

    # 1) First push high H1 (among level>=2 ticks)
    j_start = pivot_idx + 1
    j_end = min(n, pivot_idx + 1 + cfg.N1)
    if j_start >= j_end:
        return None

    j_candidates = [et for et in eval_ticks[j_start:j_end] if et.level >= min_struct_level]
    if not j_candidates:
        return None

    H1 = max(j_candidates, key=lambda et: et.mid)
    H1_idx = H1.idx

    if H1.mid - L0 < cfg.drop_min:
        return None

    # 2) Retest low L1 (higher low, level>=2)
    k_start = H1_idx + 1
    k_end = min(n, H1_idx + 1 + cfg.N2)
    if k_start >= k_end:
        return None

    k_candidates = [et for et in eval_ticks[k_start:k_end] if et.level >= min_struct_level]
    if not k_candidates:
        return None

    L1 = min(k_candidates, key=lambda et: et.mid)
    L1_idx = L1.idx

    # Real bounce
    if H1.mid - L1.mid < cfg.bounce_min:
        return None

    # Higher low than pivot
    if L1.mid < L0 + cfg.small_buffer:
        return None

    # 3) Confirmation: break above L1 by a level>=2 tick
    m_start = L1_idx + 1
    m_end = min(n, L1_idx + 1 + cfg.N3)
    if m_start >= m_end:
        return None

    confirm_idx: Optional[int] = None
    for m in range(m_start, m_end):
        et = eval_ticks[m]
        if et.level >= min_struct_level and et.mid >= L1.mid + cfg.break_buffer:
            confirm_idx = m
            break

    if confirm_idx is None:
        return None

    stop_price = L0 - cfg.SL_buffer

    return {
        "pivot_idx": pivot_idx,
        "L1_idx": L1_idx,
        "H1_idx": H1_idx,
        "confirm_idx": confirm_idx,
        "side": "long",
        "stop_price": stop_price,
    }


# ---------------------------------------------------------------------------
# Step 3 – Trade outcome metrics (half-wave style)
# ---------------------------------------------------------------------------

def compute_trade_metrics(
    eval_ticks: List[EvalTick],
    confirm_idx: int,
    side: str,
    stop_price: float,
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    """
    After confirmation, look ahead up to N_wave ticks:

      - First pass: compute max favourable move (MFE_base) in that window.
      - If MFE_base <= 0: price never moves in our favour; exit at earliest of
        stop or the last tick in the window.
      - Else:
        * target_move = target_frac_of_wave * MFE_base
        * simulate path:
            - if stop hit first -> exit at stop
            - else exit at first tick where favourable move >= target_move
        * if neither hit by end of window -> exit at last tick in window.

    MFE/MAE are computed over the actual trade path from confirm to exit.
    """

    confirm = eval_ticks[confirm_idx]
    n = len(eval_ticks)
    direction = -1 if side == "short" else 1

    start = confirm_idx + 1
    end = min(n, confirm_idx + 1 + cfg.N_wave)
    if start >= end:
        return None

    confirm_price = confirm.mid

    # -------- Pass 1: measure max favourable move over the whole window --------
    max_fav = 0.0
    for i in range(start, end):
        px = eval_ticks[i].mid
        move = direction * (px - confirm_price)
        if move > max_fav:
            max_fav = move

    # If never moves in our favour: loser trade, exit at earliest stop or window end
    if max_fav <= 0.0:
        exit_idx: Optional[int] = None
        stop_hit = False

        mfe = 0.0
        mae = 0.0

        for i in range(start, end):
            px = eval_ticks[i].mid
            move = direction * (px - confirm_price)
            if move > mfe:
                mfe = move
            if move < mae:
                mae = move

            if not stop_hit:
                if side == "short" and px >= stop_price:
                    stop_hit = True
                    exit_idx = i
                    exit_price = stop_price
                    break
                elif side == "long" and px <= stop_price:
                    stop_hit = True
                    exit_idx = i
                    exit_price = stop_price
                    break

        if exit_idx is None:
            exit_idx = end - 1
            exit_price = eval_ticks[exit_idx].mid

        raw_return = direction * (exit_price - confirm_price)
        net_return = raw_return - cfg.cost_per_trade

        return {
            "confirm_time": confirm.ts,
            "confirm_price": confirm_price,
            "exit_time": eval_ticks[exit_idx].ts,
            "exit_price": exit_price,
            "raw_return": raw_return,
            "net_return": net_return,
            "MFE": mfe,
            "MAE": mae,
            "stop_hit": stop_hit,
        }

    # -------- Pass 2: trade path with target at target_frac_of_wave * max_fav ----
    target_move = cfg.target_frac_of_wave * max_fav

    mfe = 0.0
    mae = 0.0
    stop_hit = False
    exit_idx: Optional[int] = None
    exit_price: float

    for i in range(start, end):
        px = eval_ticks[i].mid
        move = direction * (px - confirm_price)
        if move > mfe:
            mfe = move
        if move < mae:
            mae = move

        # Check stop first
        if not stop_hit:
            if side == "short" and px >= stop_price:
                stop_hit = True
                exit_idx = i
                exit_price = stop_price
                break
            elif side == "long" and px <= stop_price:
                stop_hit = True
                exit_idx = i
                exit_price = stop_price
                break

        # Then check if we reached the target fraction of the wave
        if move >= target_move and exit_idx is None:
            exit_idx = i
            exit_price = px
            break

    if exit_idx is None:
        # Neither stop nor target hit: exit at last tick in window
        exit_idx = end - 1
        exit_price = eval_ticks[exit_idx].mid

    raw_return = direction * (exit_price - confirm_price)
    net_return = raw_return - cfg.cost_per_trade

    return {
        "confirm_time": confirm.ts,
        "confirm_price": confirm_price,
        "exit_time": eval_ticks[exit_idx].ts,
        "exit_price": exit_price,
        "raw_return": raw_return,
        "net_return": net_return,
        "MFE": mfe,
        "MAE": mae,
        "stop_hit": stop_hit,
    }


# ---------------------------------------------------------------------------
# Step 4 – Save results
# ---------------------------------------------------------------------------

def save_confirmations(rows: List[Dict[str, Any]], out_dir: Path, label: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not rows:
        print("No confirmations to save.")
        return

    csv_path = out_dir / f"confirm_spots_{label}.csv"

    fieldnames = list(rows[0].keys())
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} confirmations to {csv_path}")


# ---------------------------------------------------------------------------
# Step 5 – Basic stats
# ---------------------------------------------------------------------------

def print_stats(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("No confirmations found.")
        return

    from collections import Counter
    import statistics

    net = [float(r["net_return"]) for r in rows]
    sides = [r["side"] for r in rows]
    wins = [v for v in net if v > 0]

    print(f"Total confirmations: {len(rows)}")
    side_counts = Counter(sides)
    print("By side:", dict(side_counts))

    win_rate = len(wins) / len(rows) if rows else 0.0
    print(f"Win rate (net_return > 0): {win_rate:.3f}")

    print(f"Mean net_return: {statistics.mean(net):.6f}")
    print(f"Median net_return: {statistics.median(net):.6f}")

    # Simple histogram
    bins = 10
    mn, mx = min(net), max(net)
    if mn == mx:
        print("All net_return identical (no histogram).")
        return

    width = (mx - mn) / bins
    hist = [0] * bins
    for v in net:
        idx = int((v - mn) / width)
        if idx >= bins:
            idx = bins - 1
        hist[idx] += 1

    print("Net return histogram:")
    for i, count in enumerate(hist):
        lo = mn + i * width
        hi = lo + width
        print(f"[{lo:.4f}, {hi:.4f}): {count}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_iso8601(s: str) -> datetime:
    # Accept "Z" suffix by normalizing to +00:00
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=None, help="Symbol (e.g. XAUUSD). Optional.")
    parser.add_argument("--start", required=True, help="Start ISO-8601, e.g. 2025-07-01T08:00:00Z")
    parser.add_argument("--end", required=True, help="End ISO-8601, e.g. 2025-07-01T22:00:00Z")
    parser.add_argument("--out-dir", default="train/confirm_spots")
    args = parser.parse_args()

    start_ts = parse_iso8601(args.start)
    end_ts = parse_iso8601(args.end)

    cfg = ConfirmConfig()

    # Get DB connection (adjust if your helper is different)
    conn = dbmod.get_conn()  # type: ignore

    print(f"Loading eval ticks for symbol={args.symbol} "
          f"between {start_ts.isoformat()} and {end_ts.isoformat()} ...")

    eval_ticks = load_eval_ticks(conn, args.symbol, start_ts, end_ts)
    print(f"Loaded {len(eval_ticks)} eval ticks.")

    if not eval_ticks:
        print("No data in this window, exiting.")
        return

    pivots = find_pivots(eval_ticks, cfg)
    print(f"Found {len(pivots)} pivot anchors (L5+ local extrema).")

    rows: List[Dict[str, Any]] = []

    for pivot in pivots:
        conf = detect_confirmation_for_pivot(eval_ticks, pivot, cfg)
        if conf is None:
            continue

        trade = compute_trade_metrics(
            eval_ticks,
            conf["confirm_idx"],
            conf["side"],
            conf["stop_price"],
            cfg,
        )
        if trade is None:
            continue

        pivot_idx = pivot["pivot_idx"]
        L1_idx = conf["L1_idx"]
        H1_idx = conf["H1_idx"]
        confirm_idx = conf["confirm_idx"]

        et_pivot = eval_ticks[pivot_idx]
        et_L1 = eval_ticks[L1_idx]
        et_H1 = eval_ticks[H1_idx]
        et_conf = eval_ticks[confirm_idx]

        row: Dict[str, Any] = {
            # Pivot info
            "pivot_type": pivot["pivot_type"],
            "pivot_tick_id": et_pivot.tick_id,
            "pivot_time": et_pivot.ts.isoformat(),
            "pivot_price": et_pivot.mid,
            "pivot_eval_level": et_pivot.level,
            # Pattern legs
            "L1_tick_id": et_L1.tick_id,
            "L1_time": et_L1.ts.isoformat(),
            "L1_price": et_L1.mid,
            "H1_tick_id": et_H1.tick_id,
            "H1_time": et_H1.ts.isoformat(),
            "H1_price": et_H1.mid,
            # Confirmation
            "confirm_tick_id": et_conf.tick_id,
            "confirm_time": et_conf.ts.isoformat(),
            "confirm_price": et_conf.mid,
            "side": conf["side"],
            "stop_price": conf["stop_price"],
        }

        # Trade metrics
        row.update(
            {
                "exit_time": trade["exit_time"].isoformat(),
                "exit_price": trade["exit_price"],
                "raw_return": trade["raw_return"],
                "net_return": trade["net_return"],
                "MFE": trade["MFE"],
                "MAE": trade["MAE"],
                "stop_hit": trade["stop_hit"],
            }
        )

        rows.append(row)

    print_stats(rows)

    label = f"{args.symbol or 'ALL'}_{args.start}_{args.end}".replace(":", "-")
    save_confirmations(rows, Path(args.out_dir), label)


if __name__ == "__main__":
    main()