"""
ml.resolution

Streaming, low-pressure 3-level resolution builder based purely on
local highs/lows, writing directly into the DB.

Levels
------

1) Micro (from ticks → hhll_piv)
   - Sliding window of 21 ticks (10 left, 10 right + center).
   - If the center tick is the strict maximum in that window → HIGH.
   - If the center tick is the strict minimum in that window → LOW.
   - Ticks are streamed in small batches; only a small overlap is kept
     in memory, so this can run over full history.

   Inserts into: hhll_piv(tick_id, ts, mid, ptype)

2) Middle (from Micro → piv_hilo)
   - Take full Micro list (in memory; much smaller than ticks).
   - Split into highs and lows.
   - For each type separately, use a 7-pivot window (3 left, 3 right):
       - center is strict max among 7 -> middle HIGH
       - center is strict min among 7 -> middle LOW
   - Merge highs + lows, enforce alternation (H/L/H/L...).

   Inserts into: piv_hilo(tick_id, ts, mid, ptype, win_left, win_right)

3) Macro (from Middle → piv_swings)
   - Same algorithm on Middle pivots to generate Macro pivots.

   Inserts into: piv_swings(tick_id, ts, mid, ptype, swing_index)

Journaling
----------

Every `--log-every N` pivots per level, we log a line:

    TIMESTAMP [INFO] micro pivot #200: tick_id=123456 ts=... price=...

So the log itself becomes a simple journal of progress.

CLI
---

    python -m ml.resolution --symbol XAUUSD \
        --batch-size 5000 --log-every 200 --log-level INFO

Run in background gently:

    nohup nice -n 10 python -m ml.resolution --symbol XAUUSD \
        --batch-size 5000 --log-every 200 --log-level INFO \
        > resolution.log 2>&1 &

If your tables have extra NOT NULL columns, adjust the INSERT column
lists in:

    _insert_micro_batch
    _insert_middle_batch
    _insert_macro_batch
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from backend.db import get_conn, dict_cur


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Window radii
MICRO_TICK_RADIUS = 10   # 2*10 + 1 = 21 ticks, center is the 11th
MIDDLE_PIVOT_RADIUS = 3  # 2*3 + 1 = 7 pivots
MACRO_PIVOT_RADIUS = 3   # same as middle


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TickPoint:
    id: int
    ts: object   # datetime in practice
    price: float


@dataclass
class Pivot:
    """Generic pivot representation."""
    seq_id: int          # running sequence id per level (1-based)
    tick_id: int
    ts: object
    price: float
    ptype: int           # +1 = HIGH, -1 = LOW

    @property
    def is_high(self) -> bool:
        return self.ptype > 0

    @property
    def is_low(self) -> bool:
        return self.ptype < 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_strict_max(values: Sequence[float], center_idx: int, radius: int) -> bool:
    """Check if values[center_idx] is strict max in its 2*radius+1 window."""
    v = values[center_idx]
    left = max(0, center_idx - radius)
    right = min(len(values) - 1, center_idx + radius)
    for j in range(left, right + 1):
        if j == center_idx:
            continue
        if values[j] >= v:
            return False
    return True


def _is_strict_min(values: Sequence[float], center_idx: int, radius: int) -> bool:
    """Check if values[center_idx] is strict min in its 2*radius+1 window."""
    v = values[center_idx]
    left = max(0, center_idx - radius)
    right = min(len(values) - 1, center_idx + radius)
    for j in range(left, right + 1):
        if j == center_idx:
            continue
        if values[j] <= v:
            return False
    return True


def _enforce_alternation(pivots: List[Pivot]) -> List[Pivot]:
    """
    Ensure HIGH/LOW alternation by removing weaker duplicates of same type.
    Assumes pivots are sorted by time / tick_id.
    """
    if not pivots:
        return pivots

    result: List[Pivot] = [pivots[0]]
    seq_counter = 1
    result[0].seq_id = seq_counter

    for p in pivots[1:]:
        last = result[-1]
        if p.ptype == last.ptype:
            # Same type (HH or LL). Keep the more extreme.
            if p.is_high:
                # keep higher high
                if p.price > last.price:
                    seq_counter += 1
                    p.seq_id = seq_counter
                    result[-1] = p
            else:
                # keep lower low
                if p.price < last.price:
                    seq_counter += 1
                    p.seq_id = seq_counter
                    result[-1] = p
        else:
            seq_counter += 1
            p.seq_id = seq_counter
            result.append(p)

    return result


# ---------------------------------------------------------------------------
# DB access
# ---------------------------------------------------------------------------

def stream_ticks(
    conn,
    symbol: str,
    tick_from: Optional[int],
    tick_to: Optional[int],
    batch_size: int,
) -> Iterable[List[TickPoint]]:
    """
    Generator yielding ticks in batches, ordered by id.

    Always fetches "id > last_id" to avoid OFFSET scans.
    """
    cur = dict_cur(conn)
    last_id: Optional[int] = None

    while True:
        params = [symbol]
        sql = """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE symbol = %s
        """
        if tick_from is not None:
            sql += " AND id >= %s"
            params.append(tick_from)
        if tick_to is not None:
            sql += " AND id <= %s"
            params.append(tick_to)
        if last_id is not None:
            sql += " AND id > %s"
            params.append(last_id)
        sql += " ORDER BY id LIMIT %s"
        params.append(batch_size)

        cur.execute(sql, params)
        rows = cur.fetchall()
        if not rows:
            break

        ticks = [TickPoint(id=r["id"], ts=r["timestamp"], price=float(r["mid"])) for r in rows]
        last_id = ticks[-1].id
        yield ticks

        # very small sleep to avoid hammering DB
        time.sleep(0.01)


# ---------------------------------------------------------------------------
# Level 1: Micro pivots from streaming ticks (writes to hhll_piv)
# ---------------------------------------------------------------------------

def build_micro_pivots_streaming(
    conn,
    symbol: str,
    tick_from: Optional[int],
    tick_to: Optional[int],
    batch_size: int,
    log_every: int,
    insert_batch_size: int,
) -> List[Pivot]:
    """
    Stream ticks in small batches, keeping only a small overlap in memory.

    For each tick that has MICRO_TICK_RADIUS neighbours on both sides
    (i.e. full 21-tick window), check:
      - strict maximum  -> HIGH
      - strict minimum  -> LOW

    Writes to hhll_piv as pivots are found, in batched INSERTs.

    Returns a list of Micro pivots (in memory).
    """
    radius = MICRO_TICK_RADIUS
    buffer: List[TickPoint] = []   # tail + current batch
    micro: List[Pivot] = []
    seq_counter = 0

    insert_rows: List[Tuple[int, object, float, int]] = []  # tick_id, ts, mid, ptype

    for batch in stream_ticks(conn, symbol, tick_from, tick_to, batch_size):
        buffer = buffer + batch
        prices = [t.price for t in buffer]

        # We leave last 2*radius ticks unprocessed to keep enough context
        # for the next batch.
        max_process_idx = max(-1, len(buffer) - 2 * radius - 1)

        for idx in range(radius, max_process_idx + 1):
            if not (idx - radius >= 0 and idx + radius < len(buffer)):
                continue

            price = prices[idx]
            is_high = _is_strict_max(prices, idx, radius)
            is_low = _is_strict_min(prices, idx, radius)

            if not (is_high or is_low):
                continue

            ptype = +1 if is_high else -1
            center = buffer[idx]
            seq_counter += 1

            pivot = Pivot(
                seq_id=seq_counter,
                tick_id=center.id,
                ts=center.ts,
                price=price,
                ptype=ptype,
            )
            micro.append(pivot)

            # Stage for DB insert
            insert_rows.append((center.id, center.ts, price, ptype))

            if seq_counter % log_every == 0:
                logging.info(
                    "micro pivot #%d: tick_id=%d ts=%s price=%.5f",
                    seq_counter,
                    center.id,
                    center.ts,
                    center.price,
                )

            if len(insert_rows) >= insert_batch_size:
                _insert_micro_batch(conn, insert_rows)
                insert_rows.clear()

        # Keep only last 2*radius ticks as overlap for next batch
        if len(buffer) > 2 * radius:
            buffer = buffer[-2 * radius :]

    # Flush remaining inserts
    if insert_rows:
        _insert_micro_batch(conn, insert_rows)
        insert_rows.clear()

    # Final alternation cleanup (does NOT touch DB rows, only in-memory list)
    micro.sort(key=lambda p: (p.ts, p.tick_id))
    micro = _enforce_alternation(micro)

    logging.info("micro pivots done for %s: count=%d", symbol, len(micro))
    return micro


# ---------------------------------------------------------------------------
# Level 2 and 3: generic local-extrema on pivots (with DB inserts)
# ---------------------------------------------------------------------------

def _build_level_from_pivots(
    base_level_name: str,
    level_name: str,
    pivots: List[Pivot],
    radius: int,
    log_every: int,
) -> List[Pivot]:
    """
    Generic builder for Middle/Macro levels:

    - Split input pivots into highs and lows.
    - For each group (highs, lows), use a sliding window of
      2*radius+1 SAME-TYPE pivots and mark center as pivot if it is
      strict max/min.
    - Merge highs + lows, sort by time, enforce alternation.
    """
    if not pivots:
        logging.info("%s: no input pivots; returning empty list", level_name)
        return []

    # Sort by time / tick_id just in case
    pivots.sort(key=lambda p: (p.ts, p.tick_id))

    highs = [p for p in pivots if p.is_high]
    lows = [p for p in pivots if p.is_low]

    def select_extrema(input_pivots: List[Pivot], is_high: bool) -> List[Pivot]:
        if not input_pivots:
            return []

        values = [p.price for p in input_pivots]
        selected: List[Pivot] = []
        n = len(input_pivots)
        seq_counter = 0

        for idx in range(radius, n - radius):
            if is_high:
                if not _is_strict_max(values, idx, radius):
                    continue
            else:
                if not _is_strict_min(values, idx, radius):
                    continue

            base_p = input_pivots[idx]
            seq_counter += 1
            p = Pivot(
                seq_id=seq_counter,   # temporary; re-assigned after merge
                tick_id=base_p.tick_id,
                ts=base_p.ts,
                price=base_p.price,
                ptype=base_p.ptype,
            )
            selected.append(p)

        return selected

    level_highs = select_extrema(highs, is_high=True)
    level_lows = select_extrema(lows, is_high=False)

    combined: List[Pivot] = level_highs + level_lows
    combined.sort(key=lambda p: (p.ts, p.tick_id))

    # enforce alternation and assign seq_ids
    combined = _enforce_alternation(combined)

    # Logging
    for p in combined:
        if p.seq_id % log_every == 0:
            logging.info(
                "%s pivot #%d: tick_id=%d ts=%s price=%.5f",
                level_name,
                p.seq_id,
                p.tick_id,
                p.ts,
                p.price,
            )

    logging.info(
        "%s pivots from %s: in=%d out=%d",
        level_name,
        base_level_name,
        len(pivots),
        len(combined),
    )
    return combined


def build_middle_pivots(
    micro: List[Pivot],
    log_every: int,
) -> List[Pivot]:
    return _build_level_from_pivots(
        base_level_name="micro",
        level_name="middle",
        pivots=micro,
        radius=MIDDLE_PIVOT_RADIUS,
        log_every=log_every,
    )


def build_macro_pivots(
    middle: List[Pivot],
    log_every: int,
) -> List[Pivot]:
    return _build_level_from_pivots(
        base_level_name="middle",
        level_name="macro",
        pivots=middle,
        radius=MACRO_PIVOT_RADIUS,
        log_every=log_every,
    )


# ---------------------------------------------------------------------------
# DB insert helpers for each level
# ---------------------------------------------------------------------------

def _insert_micro_batch(conn, rows: List[Tuple[int, object, float, int]]) -> None:
    """
    Insert a batch of Micro pivots into hhll_piv.

    Assumes table has at least: tick_id, ts, mid, ptype.
    Adjust column list if your schema differs.
    """
    if not rows:
        return
    sql = """
        INSERT INTO hhll_piv (tick_id, ts, mid, ptype)
        VALUES (%s, %s, %s, %s)
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()


def _insert_middle_batch(conn, pivots: List[Pivot]) -> None:
    """
    Insert Middle pivots into piv_hilo.

    Assumes table has at least: tick_id, ts, mid, ptype, win_left, win_right.
    win_left / win_right are set to the peer radius (3).
    """
    if not pivots:
        return
    sql = """
        INSERT INTO piv_hilo (tick_id, ts, mid, ptype, win_left, win_right)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    params = [
        (p.tick_id, p.ts, p.price, p.ptype, MIDDLE_PIVOT_RADIUS, MIDDLE_PIVOT_RADIUS)
        for p in pivots
    ]
    cur = conn.cursor()
    cur.executemany(sql, params)
    conn.commit()


def _insert_macro_batch(conn, pivots: List[Pivot]) -> None:
    """
    Insert Macro pivots into piv_swings.

    Assumes table has at least: tick_id, ts, mid, ptype, swing_index.

    Note: we *do not* set pivot_id here because the schema/constraints
    for piv_swings are not fully known in this context. If pivot_id is
    NOT NULL or has FK constraints, adjust this function accordingly.
    """
    if not pivots:
        return
    sql = """
        INSERT INTO piv_swings (tick_id, ts, mid, ptype, swing_index)
        VALUES (%s, %s, %s, %s, %s)
    """
    params = [
        (p.tick_id, p.ts, p.price, p.ptype, p.seq_id)
        for p in pivots
    ]
    cur = conn.cursor()
    cur.executemany(sql, params)
    conn.commit()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Streaming, low-pressure resolution builder")
    p.add_argument("--symbol", required=True, help="Instrument symbol, e.g. XAUUSD")
    p.add_argument("--from-tick", type=int, default=None, help="Lower bound on tick id (inclusive)")
    p.add_argument("--to-tick", type=int, default=None, help="Upper bound on tick id (inclusive)")
    p.add_argument("--batch-size", type=int, default=5000, help="Tick batch size per DB fetch")
    p.add_argument("--insert-batch-size", type=int, default=500, help="Micro insert batch size")
    p.add_argument("--log-every", type=int, default=200, help="Log every N pivots per level")
    p.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    conn = get_conn()

    logging.info(
        "Starting resolution build for %s (ticks [%s, %s], batch_size=%d)",
        args.symbol,
        args.from_tick,
        args.to_tick,
        args.batch_size,
    )

    # Level 1: Micro (writes to hhll_piv)
    micro = build_micro_pivots_streaming(
        conn=conn,
        symbol=args.symbol,
        tick_from=args.from_tick,
        tick_to=args.to_tick,
        batch_size=args.batch_size,
        log_every=args.log_every,
        insert_batch_size=args.insert_batch_size,
    )

    # Level 2: Middle (writes to piv_hilo)
    middle = build_middle_pivots(micro, log_every=args.log_every)
    _insert_middle_batch(conn, middle)

    # Level 3: Macro (writes to piv_swings)
    macro = build_macro_pivots(middle, log_every=args.log_every)
    _insert_macro_batch(conn, macro)

    logging.info(
        "Finished all levels for %s: micro=%d, middle=%d, macro=%d",
        args.symbol,
        len(micro),
        len(middle),
        len(macro),
    )


if __name__ == "__main__":
    main()
