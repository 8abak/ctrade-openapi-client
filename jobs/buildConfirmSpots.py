# jobs/buildConfirmSpots.py
#
# Build "confirmation spot" labels from evals (L5+) and ticks, pivot by pivot.
#
# Usage example (from repo root):
#
#   python -m jobs.buildConfirmSpots \
#       --symbol XAUUSD \
#       --start-tag 1 \
#       --num-tags 300 \
#       --out-dir train/confirm_spots_tags
#
# Behaviour:
#   1) Fetch ordered L5+ pivots ("tags") for the symbol.
#   2) For each pivot in [start_tag, start_tag + num_tags):
#        - Find small time window around pivot using nearby L2+/L4+ evals.
#        - Load eval ticks in that window, including Kalman value.
#        - Classify pivot as local high/low (using mid).
#        - Detect confirmation using Kalman pattern:
#              high pivot  -> short via H0 -> L1 -> H1 -> break-of-L1
#              low  pivot  -> long  via L0 -> H1 -> L1 -> break-of-L1
#        - Simulate a trade after confirmation (on mid prices).
#        - Write exactly one CSV row per pivot that gets a valid confirmation.
#
#   CSV schema and semantics are kept identical to the previous implementation.

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
from collections import Counter

from backend import db as dbmod  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ConfirmConfig:
    # Local extremum window (in ticks) around pivot inside the segment.
    W_loc: int = 400

    # Search horizons (ticks) for pattern stages INSIDE the small segment.
    N1: int = 200   # first drop / first push after pivot
    N2: int = 200   # retest high/low after L1/H1
    N3: int = 400   # confirmation horizon after retest

    # Wave scanning horizon after confirmation (ticks).
    N_wave: int = 800

    # Target fraction of maximal favourable move after confirmation.
    target_frac_of_wave: float = 0.5

    # Price thresholds (in price units), applied to Kalman moves.
    drop_min: float = 0.5       # min H0->L1 (short) or L0->H1 (long) in Kalman
    bounce_min: float = 0.3     # min L1->H1 (short) or H1->L1 (long) in Kalman
    small_buffer: float = 0.1   # H1 lower than H0 / L1 higher than L0 in Kalman
    break_buffer: float = 0.0   # confirmation trigger relative to L1 in Kalman

    # Stop distance beyond H0/L0 (still on mid, not Kalman).
    SL_buffer: float = 0.5

    # Trading costs (spread + fees) in price units.
    cost_per_trade: float = 0.1

    # Kept for compatibility; no longer used inside the Kalman pattern.
    min_struct_level: int = 2


@dataclass
class EvalTick:
    idx: int           # index in in-memory list (segment-local)
    tick_id: int
    ts: datetime
    mid: float         # raw mid price
    level: int         # eval level (0..8)
    kal: float         # Kalman-smoothed mid for this tick


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
    and return the slices [start_tag, start_tag + num_tags) as a list
    of dicts with tag_index (global 1-based).
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
    for i, (tick_id, ts, mid, level) in enumerate(rows, start=start_tag):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        pivots.append(
            {
                "tag_index": i,
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
    If there aren’t enough points on one side, return None.
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
        return None

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
    Includes Kalman mid from ticks.kal.
    """

    sql = """
        SELECT e.tick_id,
               e.timestamp,
               e.mid::double precision,
               e.level,
               t.kal::double precision
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
    for i, (tick_id, ts, mid, level, kal) in enumerate(rows):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        eval_ticks.append(
            EvalTick(
                idx=i,
                tick_id=int(tick_id),
                ts=ts,
                mid=float(mid),
                level=int(level),
                kal=float(kal),
            )
        )

    return eval_ticks


# ---------------------------------------------------------------------------
# Pivot classification (local high/low on mid)
# ---------------------------------------------------------------------------

def classify_pivot_type(
    eval_ticks: List[EvalTick],
    pivot_idx: int,
    cfg: ConfirmConfig,
) -> Optional[str]:
    """
    Determine if pivot_idx is a local high or local low in +/- W_loc ticks.
    Returns "high", "low", or None.
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
    pivot_mid = eval_ticks[pivot_idx].mid
    max_mid = max(mids)
    min_mid = min(mids)

    is_high = abs(pivot_mid - max_mid) < 1e-12
    is_low = abs(pivot_mid - min_mid) < 1e-12

    if is_high and not is_low:
        return "high"
    if is_low and not is_high:
        return "low"
    if is_high and is_low:
        # flat plateau: default to high
        return "high"
    return None


# ---------------------------------------------------------------------------
# Confirmation detection (Kalman L1/H1 + break-of-L1)
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
    High pivot → short:

      H0 (pivot, Kalman H0_kal)
      L1 = first Kalman low after pivot within N1
      H1 = Kalman lower high after L1 within N2
      confirm = first Kalman break below L1 - break_buffer within N3

    All pattern conditions use Kalman values.
    Stops are still placed using mid prices.
    """

    n = len(eval_ticks)
    if pivot_idx >= n - 2:
        return None

    pivot = eval_ticks[pivot_idx]
    H0_mid = pivot.mid
    H0_kal = pivot.kal

    # 1) First Kalman low L1 after pivot, within N1 ticks
    j_start = pivot_idx + 1
    j_end = min(n, pivot_idx + 1 + cfg.N1)
    if j_start >= j_end:
        return None

    L1 = min(eval_ticks[j_start:j_end], key=lambda et: et.kal)
    L1_idx = L1.idx

    # Require a meaningful drop from pivot (Kalman)
    if H0_kal - L1.kal < cfg.drop_min:
        return None

    # 2) Retest high H1 (lower high), within N2 ticks after L1
    k_start = L1_idx + 1
    k_end = min(n, L1_idx + 1 + cfg.N2)
    if k_start >= k_end:
        return None

    H1 = max(eval_ticks[k_start:k_end], key=lambda et: et.kal)
    H1_idx = H1.idx

    # Real bounce in Kalman
    if H1.kal - L1.kal < cfg.bounce_min:
        return None

    # Lower high than H0 in Kalman space
    if H1.kal > H0_kal - cfg.small_buffer:
        return None

    # 3) Confirmation: Kalman breaks below L1 within N3 ticks after H1
    m_start = H1_idx + 1
    m_end = min(n, H1_idx + 1 + cfg.N3)
    if m_start >= m_end:
        return None

    confirm_idx: Optional[int] = None
    for m in range(m_start, m_end):
        et = eval_ticks[m]
        if et.kal <= L1.kal - cfg.break_buffer:
            confirm_idx = m
            break

    if confirm_idx is None:
        return None

    stop_price = H0_mid + cfg.SL_buffer

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
    Low pivot → long (mirror of short):

      L0 (pivot, Kalman L0_kal)
      H1 = first Kalman high after pivot within N1
      L1 = Kalman higher low after H1 within N2
      confirm = first Kalman break above L1 + break_buffer within N3
    """

    n = len(eval_ticks)
    if pivot_idx >= n - 2:
        return None

    pivot = eval_ticks[pivot_idx]
    L0_mid = pivot.mid
    L0_kal = pivot.kal

    # 1) First Kalman high H1 after pivot, within N1 ticks
    j_start = pivot_idx + 1
    j_end = min(n, pivot_idx + 1 + cfg.N1)
    if j_start >= j_end:
        return None

    H1 = max(eval_ticks[j_start:j_end], key=lambda et: et.kal)
    H1_idx = H1.idx

    # Require a meaningful push up from pivot (Kalman)
    if H1.kal - L0_kal < cfg.drop_min:
        return None

    # 2) Retest low L1 (higher low), within N2 ticks after H1
    k_start = H1_idx + 1
    k_end = min(n, H1_idx + 1 + cfg.N2)
    if k_start >= k_end:
        return None

    L1 = min(eval_ticks[k_start:k_end], key=lambda et: et.kal)
    L1_idx = L1.idx

    # Real pullback in Kalman
    if H1.kal - L1.kal < cfg.bounce_min:
        return None

    # Higher low than pivot in Kalman space
    if L1.kal < L0_kal + cfg.small_buffer:
        return None

    # 3) Confirmation: Kalman breaks above L1 within N3 ticks after L1
    m_start = L1_idx + 1
    m_end = min(n, L1_idx + 1 + cfg.N3)
    if m_start >= m_end:
        return None

    confirm_idx: Optional[int] = None
    for m in range(m_start, m_end):
        et = eval_ticks[m]
        if et.kal >= L1.kal + cfg.break_buffer:
            confirm_idx = m
            break

    if confirm_idx is None:
        return None

    stop_price = L0_mid - cfg.SL_buffer

    return {
        "pivot_idx": pivot_idx,
        "L1_idx": L1_idx,
        "H1_idx": H1_idx,
        "confirm_idx": confirm_idx,
        "side": "long",
        "stop_price": stop_price,
    }


# ---------------------------------------------------------------------------
# Trade simulation (unchanged semantics, on mid prices)
# ---------------------------------------------------------------------------

def compute_trade_metrics(
    eval_ticks: List[EvalTick],
    confirm_idx: int,
    side: str,
    stop_price: float,
    cfg: ConfirmConfig,
) -> Optional[Dict[str, Any]]:
    """
    After confirmation, look ahead up to N_wave ticks in this segment.

    - Pass 1: measure max favourable move (MFE_base) from confirm to end/window.
    - If MFE_base <= 0: exit at earliest of stop or end-of-window.
    - Else:
        target_move = target_frac_of_wave * MFE_base
        walk forward:
          * stop hit → exit at stop
          * else first time favourable move >= target_move → exit there
          * if neither by end-of-window → exit at last tick.

    All prices here use mid (not Kalman).
    """

    confirm = eval_ticks[confirm_idx]
    n = len(eval_ticks)
    direction = -1 if side == "short" else 1

    start = confirm_idx + 1
    end = min(n, confirm_idx + 1 + cfg.N_wave)
    if start >= end:
        return None

    confirm_price = confirm.mid

    # ----- Pass 1: max favourable move -----
    max_fav = 0.0
    for i in range(start, end):
        px = eval_ticks[i].mid
        move = direction * (px - confirm_price)
        if move > max_fav:
            max_fav = move

    # No favourable move at all → loser; exit at stop or end
    if max_fav <= 0.0:
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

            if not stop_hit:
                if side == "short" and px >= stop_price:
                    stop_hit = True
                    exit_idx = i
                    exit_price = stop_price
                    break
                if side == "long" and px <= stop_price:
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

    # ----- Pass 2: target fraction of wave -----
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
            if side == "long" and px <= stop_price:
                stop_hit = True
                exit_idx = i
                exit_price = stop_price
                break

        # Then check target
        if move >= target_move and exit_idx is None:
            exit_idx = i
            exit_price = px
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


# ---------------------------------------------------------------------------
# Stats printing
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
        "tag_index",
        "pivot_type",
        "pivot_tick_id",
        "pivot_time",
        "pivot_price",
        "pivot_eval_level",
        "side",
        "stop_price",
        "L1_price",
        "H1_price",
        "confirm_price",
        "exit_price",
        "dur_pivot_to_L1_sec",
        "dur_pivot_to_H1_sec",
        "dur_pivot_to_confirm_sec",
        "dur_pivot_to_exit_sec",
        "raw_return",
        "net_return",
        "MFE",
        "MAE",
        "stop_hit",
    ]

    net_values: List[float] = []
    side_counts: Counter = Counter()

    print(f"Writing output to {csv_path}")

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for pivot in pivots:
            tag_index = pivot["tag_index"]
            pivot_time = pivot["pivot_time"]
            pivot_tick_id = pivot["pivot_tick_id"]

            try:
                bounds = get_window_bounds_for_pivot(conn, args.symbol, pivot_time)
                if bounds is None:
                    print(f"Tag {tag_index}: skipped (no window bounds)")
                    continue

                start_ts = bounds["start_ts"]
                end_ts = bounds["end_ts"]

                eval_ticks = load_segment_eval_ticks(conn, args.symbol, start_ts, end_ts)
                if not eval_ticks:
                    print(f"Tag {tag_index}: skipped (no eval_ticks in window)")
                    continue

                # locate pivot in this segment
                pivot_idx = None
                for et in eval_ticks:
                    if et.tick_id == pivot_tick_id:
                        pivot_idx = et.idx
                        break

                if pivot_idx is None:
                    print(f"Tag {tag_index}: skipped (pivot tick_id not in segment)")
                    continue

                pivot_type = classify_pivot_type(eval_ticks, pivot_idx, cfg)
                if pivot_type is None:
                    print(f"Tag {tag_index}: skipped (cannot classify pivot type)")
                    continue

                conf = detect_confirmation_for_pivot(eval_ticks, pivot_idx, pivot_type, cfg)
                if conf is None:
                    print(f"Tag {tag_index}: skipped (no confirmation pattern)")
                    continue

                trade = compute_trade_metrics(
                    eval_ticks,
                    conf["confirm_idx"],
                    conf["side"],
                    conf["stop_price"],
                    cfg,
                )
                if trade is None:
                    print(f"Tag {tag_index}: skipped (no trade metrics)")
                    continue

                et_pivot = eval_ticks[pivot_idx]
                et_L1 = eval_ticks[conf["L1_idx"]]
                et_H1 = eval_ticks[conf["H1_idx"]]
                et_conf = eval_ticks[conf["confirm_idx"]]

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
                f.flush()  # make sure rows hit disk promptly

                net_values.append(float(trade["net_return"]))
                side_counts[conf["side"]] += 1

                print(f"Processed tag {tag_index}, side={conf['side']}, "
                      f"net={trade['net_return']:.3f}")

            except Exception as e:
                import traceback
                print(f"ERROR on tag {tag_index}: {e}")
                traceback.print_exc()

    print_stats_from_stream(net_values, side_counts)
    print(f"Wrote {len(net_values)} confirmations to {csv_path}")


if __name__ == "__main__":
    main()