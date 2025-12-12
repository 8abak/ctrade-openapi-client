# jobs/buildTags.py

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from backend import db as dbmod


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class TagConfig:
    # USER RULE: window is -500 to +2000 ticks around pivot
    pre_ticks: int = 30
    post_ticks: int = 500

    # local extrema detection on Kalman (span=2 => 5-tick window)
    local_span: int = 2

    # pivot direction classification (kept as-is)
    classify_N: int = 80

    cost_per_trade: float = 0.1


@dataclass
class TagTick:
    idx: int        # index inside window array
    tick_id: int
    ts: datetime
    mid: float
    kal: float


# ---------------------------------------------------------------------------
# Helpers: local extrema on Kalman
# ---------------------------------------------------------------------------

def is_local_min_kal(ticks: List[TagTick], i: int, span: int) -> bool:
    """True if ticks[i].kal is strictly less than all neighbours in [i-span .. i+span]."""
    if i - span < 0 or i + span >= len(ticks):
        return False
    k0 = ticks[i].kal
    for j in range(i - span, i + span + 1):
        if j == i:
            continue
        if ticks[j].kal <= k0:
            return False
    return True


def is_local_max_kal(ticks: List[TagTick], i: int, span: int) -> bool:
    """True if ticks[i].kal is strictly greater than all neighbours in [i-span .. i+span]."""
    if i - span < 0 or i + span >= len(ticks):
        return False
    k0 = ticks[i].kal
    for j in range(i - span, i + span + 1):
        if j == i:
            continue
        if ticks[j].kal >= k0:
            return False
    return True


def seconds(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds()


# ---------------------------------------------------------------------------
# Pivot selection (L5+ tags from evals)
# ---------------------------------------------------------------------------

def get_l5_pivots(conn, symbol: str, start_tag: int, num_tags: int):
    """
    Fetch L5+ pivots from evals joined to ticks, and assign a global 1-based tag_index.

    L5+ means evals.level >= 5 for the given symbol.
    Ordering is by evals.timestamp, evals.id.
    """
    sql = """
        WITH l5 AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY e.timestamp, e.id) AS tag_index,
                e.tick_id,
                e.timestamp AS pivot_time,
                e.mid       AS pivot_mid
            FROM evals e
            JOIN ticks t ON t.id = e.tick_id
            WHERE t.symbol = %(symbol)s
              AND e.level >= 5
        )
        SELECT tag_index, tick_id, pivot_time, pivot_mid
        FROM l5
        WHERE tag_index BETWEEN %(start_tag)s AND %(end_tag)s
        ORDER BY tag_index
    """
    end_tag = start_tag + num_tags - 1
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "symbol": symbol,
                "start_tag": start_tag,
                "end_tag": end_tag,
            },
        )
        rows = cur.fetchall()

    pivots = []
    for tag_index, tick_id, pivot_time, pivot_mid in rows:
        pivots.append(
            {
                "tag_index": int(tag_index),
                "tick_id": int(tick_id),
                "pivot_time": pivot_time,
                "pivot_mid": float(pivot_mid),
            }
        )
    return pivots


# ---------------------------------------------------------------------------
# Tick window loading
# ---------------------------------------------------------------------------

def load_tick_window(
    conn,
    symbol: str,
    pivot_tick_id: int,
    cfg: TagConfig,
) -> Tuple[List[TagTick], Optional[int], bool]:
    """
    Load ticks around pivot_tick_id:

    - prev: <= pivot_id ordered DESC, LIMIT pre+1 (includes pivot)
    - next: > pivot_id ordered ASC, LIMIT post

    Returns:
        ticks: concatenated window [prev_reversed, next]
        pivot_idx: index of pivot_tick_id inside ticks (or None if not found)
        full_window: True if we have at least pre_ticks before and post_ticks after
    """
    with conn.cursor() as cur:
        # previous ticks including pivot
        cur.execute(
            """
            SELECT id, timestamp, mid::double precision, kal::double precision
            FROM ticks
            WHERE symbol = %(symbol)s
              AND id <= %(pivot_id)s
            ORDER BY id DESC
            LIMIT %(limit)s
            """,
            {
                "symbol": symbol,
                "pivot_id": pivot_tick_id,
                "limit": cfg.pre_ticks + 1,
            },
        )
        prev_rows = cur.fetchall()

        # next ticks
        cur.execute(
            """
            SELECT id, timestamp, mid::double precision, kal::double precision
            FROM ticks
            WHERE symbol = %(symbol)s
              AND id > %(pivot_id)s
            ORDER BY id ASC
            LIMIT %(limit)s
            """,
            {
                "symbol": symbol,
                "pivot_id": pivot_tick_id,
                "limit": cfg.post_ticks,
            },
        )
        next_rows = cur.fetchall()

    prev_rows = list(reversed(prev_rows))  # chronological
    ticks: List[TagTick] = []
    idx = 0

    for tick_id, ts, mid, kal in prev_rows:
        ticks.append(TagTick(idx=idx, tick_id=int(tick_id), ts=ts, mid=float(mid), kal=float(kal)))
        idx += 1

    for tick_id, ts, mid, kal in next_rows:
        ticks.append(TagTick(idx=idx, tick_id=int(tick_id), ts=ts, mid=float(mid), kal=float(kal)))
        idx += 1

    pivot_idx = None
    for i, t in enumerate(ticks):
        if t.tick_id == pivot_tick_id:
            pivot_idx = i
            break

    full_window = (
        len(prev_rows) >= cfg.pre_ticks + 1 and
        len(next_rows) >= cfg.post_ticks
    )

    return ticks, pivot_idx, full_window


# ---------------------------------------------------------------------------
# Pivot direction classification
# ---------------------------------------------------------------------------

def classify_pivot_type(
    ticks: List[TagTick],
    pivot_idx: int,
    cfg: TagConfig,
) -> Optional[str]:
    """
    Classify pivot as "high" (short) vs "low" (long) via average mid before/after.

    If price goes down after pivot → "high"; else → "low".
    """
    N = cfg.classify_N
    before = ticks[max(0, pivot_idx - N): pivot_idx]
    after = ticks[pivot_idx + 1: min(len(ticks), pivot_idx + 1 + N)]

    if not before or not after:
        return None

    avg_before = sum(t.mid for t in before) / len(before)
    avg_after = sum(t.mid for t in after) / len(after)

    return "high" if avg_after < avg_before else "low"


# ---------------------------------------------------------------------------
# Wave detection: EXACT USER RULES (Kalman-based)
# ---------------------------------------------------------------------------

def detect_wave_points(
    ticks: List[TagTick],
    pivot_idx: int,
    pivot_type: Optional[str],
    cfg: TagConfig,
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    USER RULE:
      - From pivot, move forward to find first turning point on Kalman.
        * If pivot_type == "high" (short): first turning point = first local MIN (L1)
        * If pivot_type == "low"  (long):  first turning point = first local MAX (H1)
      - Then find second turning point (opposite of first):
        * high: second = first local MAX after L1 (H1)
        * low:  second = first local MIN after H1 (L1)
      - Confirm point: AFTER the second turning point, wait for Kalman to pass the first turning
        level:
        * high: after H1, first tick where kal <= kal(L1)
        * low:  after L1, first tick where kal >= kal(H1)

    Returns indices (L1_idx, H1_idx, conf_idx) or Nones.
    """
    if pivot_type not in ("high", "low"):
        return None, None, None

    span = cfg.local_span
    L1_idx: Optional[int] = None
    H1_idx: Optional[int] = None
    conf_idx: Optional[int] = None

    if pivot_type == "high":
        # First turning point (opposite of pivot): first local MIN after pivot => L1
        for i in range(pivot_idx + 1, len(ticks)):
            if is_local_min_kal(ticks, i, span):
                L1_idx = i
                break
        if L1_idx is None:
            return None, None, None

        # Second turning point (same direction as pivot): first local MAX after L1 => H1
        for j in range(L1_idx + 1, len(ticks)):
            if is_local_max_kal(ticks, j, span):
                H1_idx = j
                break
        if H1_idx is None:
            return L1_idx, None, None

        # Confirm: after H1, first tick where kal <= kal(L1)
        L1_kal = ticks[L1_idx].kal
        for k in range(H1_idx + 1, len(ticks)):
            if ticks[k].kal <= L1_kal:
                conf_idx = k
                break

        return L1_idx, H1_idx, conf_idx

    else:  # pivot_type == "low"
        # First turning point (opposite of pivot): first local MAX after pivot => H1
        for i in range(pivot_idx + 1, len(ticks)):
            if is_local_max_kal(ticks, i, span):
                H1_idx = i
                break
        if H1_idx is None:
            return None, None, None

        # Second turning point (same direction as pivot): first local MIN after H1 => L1
        for j in range(H1_idx + 1, len(ticks)):
            if is_local_min_kal(ticks, j, span):
                L1_idx = j
                break
        if L1_idx is None:
            return None, H1_idx, None

        # Confirm: after L1, first tick where kal >= kal(H1)
        H1_kal = ticks[H1_idx].kal
        for k in range(L1_idx + 1, len(ticks)):
            if ticks[k].kal >= H1_kal:
                conf_idx = k
                break

        return L1_idx, H1_idx, conf_idx


# ---------------------------------------------------------------------------
# Trade simulation: EXACT USER RULES
# ---------------------------------------------------------------------------

def simulate_trade(
    ticks: List[TagTick],
    pivot_idx: int,
    conf_idx: int,
    pivot_type: Optional[str],
    cfg: TagConfig,
) -> Tuple[
    Optional[int],   # close_idx
    Optional[int],   # tp_idx
    Optional[int],   # sl_idx
    Optional[float], # take_price
    Optional[float], # stop_price
    Optional[float], # gnet
    Optional[float], # net
    Optional[str],   # side
    Optional[bool],  # stop_hit
]:
    """
    USER RULE:
      - Enter at confirm point (conf_idx, entry_price = ticks[conf_idx].mid)
      - distance = |entry - pivot|
      - stop_loss distance = 1.1 * distance (10% extra) in adverse direction
      - take_profit distance = 1.0 * distance in favorable direction
      - walk forward until TP or SL hit; record tick id/time by index
    """
    if pivot_type not in ("high", "low"):
        return None, None, None, None, None, None, None, "", None
    if conf_idx is None or conf_idx >= len(ticks) - 1:
        return None, None, None, None, None, None, None, "", None

    pivot = ticks[pivot_idx]
    conf = ticks[conf_idx]

    entry = conf.mid
    distance = abs(entry - pivot.mid)

    direction = -1 if pivot_type == "high" else +1
    side = "short" if direction == -1 else "long"

    if distance == 0:
        # Degenerate: no SL/TP levels; exit at last tick
        close_idx = len(ticks) - 1
        exit_mid = ticks[close_idx].mid
        gnet = direction * (exit_mid - entry)
        net = gnet - cfg.cost_per_trade
        return close_idx, None, None, None, None, gnet, net, side, False

    if pivot_type == "high":
        stop_price = entry + 1.1 * distance
        take_price = entry - 1.0 * distance
    else:
        stop_price = entry - 1.1 * distance
        take_price = entry + 1.0 * distance

    close_idx: Optional[int] = None
    tp_idx: Optional[int] = None
    sl_idx: Optional[int] = None
    stop_hit = None

    for i in range(conf_idx + 1, len(ticks)):
        mid = ticks[i].mid
        if pivot_type == "high":
            if mid >= stop_price:
                close_idx = i
                sl_idx = i
                stop_hit = True
                break
            if mid <= take_price:
                close_idx = i
                tp_idx = i
                stop_hit = False
                break
        else:
            if mid <= stop_price:
                close_idx = i
                sl_idx = i
                stop_hit = True
                break
            if mid >= take_price:
                close_idx = i
                tp_idx = i
                stop_hit = False
                break

    if close_idx is None:
        close_idx = len(ticks) - 1
        stop_hit = False

    exit_mid = ticks[close_idx].mid
    gnet = direction * (exit_mid - entry)
    net = gnet - cfg.cost_per_trade

    return close_idx, tp_idx, sl_idx, take_price, stop_price, gnet, net, side, stop_hit


# ---------------------------------------------------------------------------
# CSV writing
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "row",          # 1..N rows in this file
    "tag",          # global tag_index from get_l5_pivots
    "id",           # tick id of pivot

    "L1",           # tick id of first turning point (kal local min/max depending on pivot_type)
    "H1",           # tick id of second turning point
    "conf",         # tick id of confirm (kal passes first turning level)
    "tp",           # tick id where TP hit (if any)
    "sl",           # tick id where SL hit (if any)
    "close",        # tick id of exit (tp/sl/last)

    "price_piv",
    "price_L1",
    "price_H1",
    "price_conf",
    "take_price",   # TP level price
    "stop_price",   # SL level price
    "price_close",

    "date",         # pivot date (YYYY-MM-DD)
    "time",         # pivot time (HH:MM:SS)

    "t_L1",
    "t_H1",
    "t_conf",
    "t_tp",
    "t_sl",
    "t_close",

    "stop_hit",     # true/false
    "net",
    "gnet",
    "side",
]


def write_tags_csv(
    conn,
    symbol: str,
    start_tag: int,
    num_tags: int,
    out_dir: Path,
    cfg: TagConfig,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    label = f"{symbol}_tags_{start_tag}_{start_tag + num_tags - 1}".replace(":", "-")
    csv_path = out_dir / f"tags_{label}.csv"

    pivots = get_l5_pivots(conn, symbol, start_tag, num_tags)
    logger.info("Fetched %d pivots (requested %d)", len(pivots), num_tags)

    row_counter = 0

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        f.flush()

        for pivot in pivots:
            row_counter += 1

            tag_index = pivot["tag_index"]
            pivot_tick_id = pivot["tick_id"]
            pivot_time = pivot["pivot_time"]
            pivot_mid = pivot["pivot_mid"]

            ticks, pivot_idx, full_window = load_tick_window(
                conn,
                symbol=symbol,
                pivot_tick_id=pivot_tick_id,
                cfg=cfg,
            )

            # Defaults: pivot-only
            L1_idx = H1_idx = conf_idx = None
            close_idx = tp_idx = sl_idx = None
            pivot_type: Optional[str] = None
            take_price = stop_price = None
            net = gnet = None
            side = ""
            stop_hit: Optional[bool] = None

            if pivot_idx is not None and full_window and ticks:
                pivot_ts = ticks[pivot_idx].ts or pivot_time

                pivot_type = classify_pivot_type(ticks, pivot_idx, cfg)

                L1_idx, H1_idx, conf_idx = detect_wave_points(ticks, pivot_idx, pivot_type, cfg)

                if conf_idx is not None and conf_idx < len(ticks) - 1 and pivot_type is not None:
                    (
                        close_idx,
                        tp_idx,
                        sl_idx,
                        take_price,
                        stop_price,
                        gnet,
                        net,
                        side,
                        stop_hit,
                    ) = simulate_trade(
                        ticks=ticks,
                        pivot_idx=pivot_idx,
                        conf_idx=conf_idx,
                        pivot_type=pivot_type,
                        cfg=cfg,
                    )
            else:
                pivot_ts = pivot_time

            pivot_date_str = pivot_ts.date().isoformat()
            pivot_time_str = pivot_ts.strftime("%H:%M:%S")

            def price(idx: Optional[int]) -> str:
                return f"{ticks[idx].mid:.6f}" if (idx is not None and 0 <= idx < len(ticks)) else ""

            def tick_id(idx: Optional[int]) -> str:
                return str(ticks[idx].tick_id) if (idx is not None and 0 <= idx < len(ticks)) else ""

            def td(idx: Optional[int]) -> str:
                if idx is None or not (0 <= idx < len(ticks)):
                    return ""
                return f"{seconds(pivot_ts, ticks[idx].ts):.6f}"

            row = {
                "row": row_counter,
                "tag": tag_index,
                "id": pivot_tick_id,

                "L1": tick_id(L1_idx),
                "H1": tick_id(H1_idx),
                "conf": tick_id(conf_idx),
                "tp": tick_id(tp_idx),
                "sl": tick_id(sl_idx),
                "close": tick_id(close_idx),

                "price_piv": f"{pivot_mid:.6f}",
                "price_L1": price(L1_idx),
                "price_H1": price(H1_idx),
                "price_conf": price(conf_idx),
                "take_price": f"{take_price:.6f}" if take_price is not None else "",
                "stop_price": f"{stop_price:.6f}" if stop_price is not None else "",
                "price_close": price(close_idx),

                "date": pivot_date_str,
                "time": pivot_time_str,

                "t_L1": td(L1_idx),
                "t_H1": td(H1_idx),
                "t_conf": td(conf_idx),
                "t_tp": td(tp_idx),
                "t_sl": td(sl_idx),
                "t_close": td(close_idx),

                "stop_hit": "true" if stop_hit else ("false" if stop_hit is not None else ""),
                "net": f"{net:.6f}" if net is not None else "",
                "gnet": f"{gnet:.6f}" if gnet is not None else "",
                "side": side,
            }

            writer.writerow(row)
            f.flush()

            if not full_window:
                logger.warning(
                    "Tag %s (tick %s) has incomplete window: wrote pivot-only row",
                    tag_index,
                    pivot_tick_id,
                )
            elif pivot_idx is None:
                logger.warning(
                    "Tag %s (tick %s) not found in window ticks: wrote pivot-only row",
                    tag_index,
                    pivot_tick_id,
                )

    logger.info("Finished writing %d rows to %s", row_counter, csv_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build tag windows around +5 pivots and simulate SL/TP trades based on Kalman turning points.",
    )
    p.add_argument("--symbol", required=True, help="Symbol to process (e.g. XAUUSD)")
    p.add_argument("--start-tag", type=int, required=True, help="1-based index of first L5+ pivot")
    p.add_argument("--num-tags", type=int, required=True, help="How many pivots to process")
    p.add_argument("--out-dir", default="train/tags", help="Output directory (default: train/tags)")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [buildTags] %(message)s",
    )

    args = parse_args()
    cfg = TagConfig()

    conn = dbmod.get_conn()
    try:
        write_tags_csv(
            conn=conn,
            symbol=args.symbol,
            start_tag=args.start_tag,
            num_tags=args.num_tags,
            out_dir=Path(args.out_dir),
            cfg=cfg,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
