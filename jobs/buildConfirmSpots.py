# jobs/buildConfirmSpots.py
#
# Build "confirmation spot" labels from evals (L5+) and ticks, pivot by pivot.
#
# Usage example (from repo root):
#
#   # Process tags (L5+ pivots) 1..300
#   python -m jobs.buildConfirmSpots \
#       --symbol XAUUSD \
#       --start-tag 1 \
#       --num-tags 300 \
#       --out-dir train/confirm_spots_tags
#
# Behaviour:
#   1) Fetch ordered L5+ pivots ("tags") for the symbol.
#   2) For each pivot in [start_tag, start_tag + num_tags):
#        - Find 3 previous L2+ evals and 3 next L4+ evals to define [start_ts, end_ts].
#        - Load eval ticks only in [start_ts, end_ts].
#        - Classify pivot as local high/low in that window.
#        - Detect confirmation pattern (L2 structure, half-wave exit).
#        - Append one row to CSV, with durations from pivot:
#            dur_pivot_to_L1_sec, dur_pivot_to_H1_sec,
#            dur_pivot_to_confirm_sec, dur_pivot_to_exit_sec.
#   3) Never hold more than one pivot-window in memory.

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

from collections import Counter

# Adjust this import / function name if your DB helper is different.
from backend import db as dbmod  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ConfirmConfig:
    # Local extremum window (in ticks) around pivot, inside the small segment.
    W_loc: int = 400

    # Search horizons (ticks) for pattern stages INSIDE the small segment
    N1: int = 200   # first drop / first push after pivot
    N2: int = 200   # retest high/low after L1/H1
    N3: int = 400   # confirmation horizon after retest

    # Wave scanning horizon after confirmation (ticks)
    N_wave: int = 800

    # Target fraction of maximal favourable move after confirmation
    target_frac_of_wave: float = 0.5

    # Price thresholds (all in price units)
    drop_min: float = 0.5       # min H0->L1 (short) or L0->H1 (long)
    bounce_min: float = 0.3     # min L1->H1 (short) or H1->L1 (long)
    small_buffer: float = 0.1   # H1 lower than H0 (short), or L1 higher than L0 (long)
    break_buffer: float = 0.0   # confirmation trigger at L1 (or slightly beyond)

    SL_buffer: float = 0.5      # stop distance beyond H0/L0

    # Trading costs (spread + fees) in price units
    cost_per_trade: float = 0.1

    # Minimum eval level to treat as "L2 structure".
    min_struct_level: int = 2


@dataclass
class EvalTick:
    idx: int           # index in in-memory list (segment-local)
    tick_id: int
    ts: datetime
    mid: float
    level: int


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_l5_pivots(
    conn,
    symbol: str,
    start_tag: int,
    num_tags: int,
) -> List[Dict[str, Any]]:
    """
    Fetch L5+ eval rows (pivots/tags) for a symbol, ordered by time,
    and return only the pivots in [start_tag, start_tag + num_tags).
    Tags are 1-based over all L5+ pivots for that symbol.
    """

    if start_tag < 1:
        start_tag = 1

    offset = start_tag - 1
    limit = num_tags

    sql = """
        SELECT e.tick_id,
               e.timestamp,
               e.mid::double precision,
               e.level
        FROM evals e
        JOIN ticks t ON e.tick_id = t.id
        WHERE t.symbol = %(symbol)s
          AND e.level >= 5
        ORDER BY e.timestamp
        OFFSET %(offset)s
        LIMIT %(limit)s
    """

    cur = conn.cursor()
    cur.execute(sql, {"symbol": symbol, "offset": offset, "limit": limit})
    rows = cur.fetchall()

    pivots: List[Dict[str, Any]] = []
    # tag_index is the global tag index (1-based) for this symbol
    for i, (tick_id, ts, mid, level) in enumerate(rows, start=start_tag):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        pivots.append(
            {
                "tag_index": i,         # global L5+ tag index (1-based)
                "pivot_tick_id": int(tick_id),
                "pivot_time": ts,
                "pivot_price": float(mid),
                "pivot_eval_level": int(level),
            }
        )

    return pivots


def get_window_bounds_for_pivot(
    conn,
    symbol: str,
    pivot_time: datetime,
) -> Optional[Dict[str, datetime]]:
    """
    For a given pivot_time, find:
      - start_ts: time of the 3rd previous L2+ eval (level >= 2),
      - end_ts: time of the 3rd next   L4+ eval (level >= 4).

    If there aren't enough points, we use what we have; if we have none on
    one side, we return None (skip this pivot).
    """

    cur = conn.cursor()

    # 3 previous L2+ (level >= 2), ordered DESC
    sql_prev = """
        SELECT e.timestamp
        FROM evals e
        JOIN ticks t ON e.tick_id = t.id
        WHERE t.symbol = %(symbol)s
          AND e.level >= 2
          AND e.timestamp <= %(pivot_time)s
        ORDER BY e.timestamp DESC
        LIMIT 3
    """
    cur.execute(sql_prev, {"symbol": symbol, "pivot_time": pivot_time})
    prev_rows = cur.fetchall()

    # 3 next L4+ (level >= 4), ordered ASC
    sql_next = """
        SELECT e.timestamp
        FROM evals e
        JOIN ticks t ON e.tick_id = t.id
        WHERE t.symbol = %(symbol)s
          AND e.level >= 4
          AND e.timestamp >= %(pivot_time)s
        ORDER BY e.timestamp ASC
        LIMIT 3
    """
    cur.execute(sql_next, {"symbol": symbol, "pivot_time": pivot_time})
    next_rows = cur.fetchall()

    if not prev_rows or not next_rows:
        # Not enough structure around this pivot; skip.
        return None

    # earliest of the previous 3, latest of the next 3
    start_ts = min(ts for (ts,) in prev_rows)
    end_ts = max(ts for (ts,) in next_rows)

    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=timezone.utc)
    if end_ts.tzinfo is None:
        end_ts = end_ts.replace(tzinfo=timezone.utc)

    return {"start_ts": start_ts, "end_ts": end_ts}


def load_segment_eval_ticks(
    conn,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
) -> List[EvalTick]:
    """
    Load eval ticks for a symbol in [start_ts, end_ts], ordered by tick_id.
    """

    sql = """
        SELECT e.tick_id,
               e.timestamp,
               e.mid::double precision,
               e.level
        FROM evals e
        JOIN ticks t ON e.tick_id = t.id
        WHERE t.symbol = %(symbol)s
          AND e.timestamp BETWEEN %(start)s AND %(end)s
        ORDER BY e.tick_id
    """

    cur = conn.cursor()
    cur.execute(sql, {"symbol": symbol, "start": start_ts, "end": end_ts})
    rows = cur.fetchall()

    eval_ticks: List[EvalTick] = []
    for i, (tick_id, ts, mid, level) in enumerate(rows):
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
# Pivot classification (local high/low inside the small segment)
# ---------------------------------------------------------------------------

def classify_pivot_type(
    eval_ticks: List[EvalTick],
    pivot_idx: int,
    cfg: ConfirmConfig,
) -> Optional[str]:
    """
    Determine if pivot_idx is a local high or local low in +/- W_loc ticks.
    Returns "high", "low", or None (if neither).
    """

    n = len(eval_ticks)
    if pivot_idx < 0 or pivot_idx >= n:
        return None

    W = cfg.W_loc
    lo = max(0, pivot_idx - W)
    hi = min(n - 1, pivot_idx + W)
    window = eval_ticks[lo : hi + 1]
    if not window:
        return None

    mids = [et.mid for et in window]
    mid_i = eval_ticks[pivot_idx].mid
    max_mid = max(mids)
    min_mid = min(mids)

    is_high = abs(mid_i - max_mid) < 1e-12
    is_low = abs(mid_i - min_mid) < 1e-12

    if not (is_high or is_low):
        return None

    if is_high and not is_low:
        return "high"
    if is_low and not is_high:
        return "low"

    # Flat plateau: default to high for now
    return "high"


# ---------------------------------------------------------------------------
# Step 2 – Confirmation pattern detection (using level>=2 for L1/H1/confirm)
# ---------------------------------------------------------------------------

def detect_confirmation_for_pivot(
    eval_ticks: List[EvalTick],
    pivot_idx: int,
    pivot_type: str,
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    if pivot_type == "high":
        return _detect_short(eval_ticks, pivot_idx, cfg)
    elif pivot_type == "low":
        return _detect_long(eval_ticks, pivot_idx, cfg)
    else:
        return None


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
# Step 3 – Trade outcome metrics (half-wave style, segment-local)
# ---------------------------------------------------------------------------

def compute_trade_metrics(
    eval_ticks: List[EvalTick],
    confirm_idx: int,
    side: str,
    stop_price: float,
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    """
    After confirmation, look ahead up to N_wave ticks INSIDE THIS SEGMENT:

      - Pass 1: compute max favourable move (MFE_base) in that segment window.
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
# Stats printing (streaming)
# ---------------------------------------------------------------------------

def print_stats_from_stream(net_values: List[float], side_counts: Counter) -> None:
    if not net_values:
        print("No confirmations found.")
        return

    import statistics

    print(f"Total confirmations: {len(net_values)}")
    print("By side:", dict(side_counts))

    wins = [v for v in net_values if v > 0]
    win_rate = len(wins) / len(net_values) if net_values else 0.0
    print(f"Win rate (net_return > 0): {win_rate:.3f}")

    print(f"Mean net_return: {statistics.mean(net_values):.6f}")
    print(f"Median net_return: {statistics.median(net_values):.6f}")

    # Simple histogram
    bins = 10
    mn, mx = min(net_values), max(net_values)
    if mn == mx:
        print("All net_return identical (no histogram).")
        return

    width = (mx - mn) / bins
    hist = [0] * bins
    for v in net_values:
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

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="Symbol (e.g. XAUUSD).")
    parser.add_argument("--start-tag", type=int, default=1,
                        help="1-based index of first L5+ pivot/tag to process.")
    parser.add_argument("--num-tags", type=int, default=300,
                        help="Number of L5+ pivots/tags to process.")
    parser.add_argument("--out-dir", default="train/confirm_spots_tags")
    args = parser.parse_args()

    cfg = ConfirmConfig()

    conn = dbmod.get_conn()  # type: ignore

    print(f"Fetching L5+ pivots for symbol={args.symbol}, "
          f"start_tag={args.start_tag}, num_tags={args.num_tags} ...")

    pivots = get_l5_pivots(conn, args.symbol, args.start_tag, args.num_tags)
    if not pivots:
        print("No L5+ pivots found in this tag range, exiting.")
        return

    print(f"Loaded {len(pivots)} L5+ pivots for processing.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    label = f"{args.symbol}_tags_{args.start_tag}_{args.start_tag + args.num_tags - 1}"
    label = label.replace(":", "-")
    csv_path = out_dir / f"confirm_spots_{label}.csv"

    fieldnames = [
        "tag_index",                 # global L5+ tag index (1-based)
        "pivot_type",                # high/low
        "pivot_tick_id",
        "pivot_time",               # actual datetime of the pivot/tag
        "pivot_price",
        "pivot_eval_level",
        "side",
        "stop_price",
        # prices of legs
        "L1_price",
        "H1_price",
        "confirm_price",
        "exit_price",
        # durations from pivot (in seconds)
        "dur_pivot_to_L1_sec",
        "dur_pivot_to_H1_sec",
        "dur_pivot_to_confirm_sec",
        "dur_pivot_to_exit_sec",
        # trade metrics
        "raw_return",
        "net_return",
        "MFE",
        "MAE",
        "stop_hit",
    ]

    net_values: List[float] = []
    side_counts: Counter = Counter()

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for pivot in pivots:
            tag_index = pivot["tag_index"]
            pivot_time = pivot["pivot_time"]
            pivot_tick_id = pivot["pivot_tick_id"]

            bounds = get_window_bounds_for_pivot(conn, args.symbol, pivot_time)
            if bounds is None:
                # Not enough surrounding structure; skip pivot
                continue

            start_ts = bounds["start_ts"]
            end_ts = bounds["end_ts"]

            eval_ticks = load_segment_eval_ticks(conn, args.symbol, start_ts, end_ts)
            if not eval_ticks:
                continue

            # find pivot_idx in this segment
            pivot_idx = None
            for et in eval_ticks:
                if et.tick_id == pivot_tick_id:
                    pivot_idx = et.idx
                    break

            if pivot_idx is None:
                # Pivot not in this segment? (shouldn't happen, but be safe)
                continue

            pivot_type = classify_pivot_type(eval_ticks, pivot_idx, cfg)
            if pivot_type is None:
                continue

            conf = detect_confirmation_for_pivot(eval_ticks, pivot_idx, pivot_type, cfg)
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

            et_pivot = eval_ticks[pivot_idx]
            et_L1 = eval_ticks[conf["L1_idx"]]
            et_H1 = eval_ticks[conf["H1_idx"]]
            et_conf = eval_ticks[conf["confirm_idx"]]

            # Durations from pivot
            dur_pivot_to_L1 = (et_L1.ts - et_pivot.ts).total_seconds()
            dur_pivot_to_H1 = (et_H1.ts - et_pivot.ts).total_seconds()
            dur_pivot_to_confirm = (trade["confirm_time"] - et_pivot.ts).total_seconds()
            dur_pivot_to_exit = (trade["exit_time"] - et_pivot.ts).total_seconds()

            row: Dict[str, Any] = {
                "tag_index": tag_index,
                "pivot_type": pivot_type,
                "pivot_tick_id": et_pivot.tick_id,
                "pivot_time": et_pivot.ts.isoformat(),
                "pivot_price": et_pivot.mid,
                "pivot_eval_level": et_pivot.level,
                "side": conf["side"],
                "stop_price": conf["stop_price"],
                "L1_price": et_L1.mid,
                "H1_price": et_H1.mid,
                "confirm_price": et_conf.mid,
                "exit_price": trade["exit_price"],
                "dur_pivot_to_L1_sec": dur_pivot_to_L1,
                "dur_pivot_to_H1_sec": dur_pivot_to_H1,
                "dur_pivot_to_confirm_sec": dur_pivot_to_confirm,
                "dur_pivot_to_exit_sec": dur_pivot_to_exit,
                "raw_return": trade["raw_return"],
                "net_return": trade["net_return"],
                "MFE": trade["MFE"],
                "MAE": trade["MAE"],
                "stop_hit": trade["stop_hit"],
            }

            writer.writerow(row)

            net_values.append(float(trade["net_return"]))
            side_counts[conf["side"]] += 1

            # Optional: light progress logging
            print(f"Processed tag {tag_index}, side={conf['side']}, net={trade['net_return']:.3f}")

    print_stats_from_stream(net_values, side_counts)
    print(f"Wrote {len(net_values)} confirmations to {csv_path}")


if __name__ == "__main__":
    main()