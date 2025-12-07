"""
ml.resolution

Streaming, low-pressure 3-level resolution builder based purely on
local highs/lows, aligned to the actual schema:

Levels
------

Micro  (level 1)  → piv_hilo
    - Built directly from ticks.
    - Sliding window of 21 ticks (10 left, 10 right + center).
    - If the center tick is strict maximum in that window → HIGH (ptype = +1).
    - If the center tick is strict minimum in that window → LOW  (ptype = -1).
    - Inserts into piv_hilo(tick_id, ts, mid, ptype, win_left, win_right).

Middle (level 2)  → piv_swings
    - Built from piv_hilo (micro pivots).
    - Work separately on highs and lows:
        - For each type, use window of 7 pivots (3 left, 3 right + center).
        - Center is strict max/min among its same-type peers.
    - Merge highs + lows, sort by time, enforce HIGH/LOW alternation.
    - Inserts into piv_swings(pivot_id, tick_id, ts, mid, ptype, swing_index),
      where pivot_id = piv_hilo.id of the selected micro pivot.

Macro  (level 3)  → hhll_piv
    - Built from piv_swings (middle swings).
    - Same 7-pivot window logic on swings.
    - Inserts into hhll_piv(swing_id, tick_id, ts, mid, ptype, class, class_text),
      where swing_id = piv_swings.id of the selected swing.
    - class is set to 0, class_text to 'AUTO' for now.

Journaling
----------

Every `--log-every N` pivots per level, we log:

    <timestamp> [INFO] micro pivot #<seq>: tick_id=<id> ts=<ts> price=<mid>
    <timestamp> [INFO] middle pivot #...
    <timestamp> [INFO] macro pivot #...

This acts as a simple journal.

Usage
-----

Run from project root:

    python -m ml.resolution --symbol XAUUSD

Recommended gentle background run:

    nohup nice -n 10 python -m ml.resolution \
        --symbol XAUUSD \
        --batch-size 5000 \
        --insert-batch-size 500 \
        --log-every 200 \
        --log-level INFO \
        > resolution.log 2>&1 &

IMPORTANT
---------

- This script currently assumes that you either:
    - start from empty piv_hilo / piv_swings / hhll_piv
      (TRUNCATE beforehand per symbol), or
    - accept that re-running will append more pivots.
- It does not try to be incremental / idempotent yet; that can be added later.
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

# Micro: 21-tick window (10 left, 10 right + center)
MICRO_TICK_RADIUS = 10   # 2*10 + 1 = 21

# Middle / Macro: 7-pivot window (3 left, 3 right + center)
MIDDLE_PIVOT_RADIUS = 3  # 2*3 + 1 = 7
MACRO_PIVOT_RADIUS = 3

# hhll_piv class placeholders
HHLL_CLASS_DEFAULT = 0
HHLL_CLASS_TEXT_DEFAULT = "AUTO"


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
    """
    Generic pivot representation.

    src_id:
        - For middle level: underlying piv_hilo.id
        - For macro level : underlying piv_swings.id
        - For micro level (direct from ticks): None
    """
    seq_id: int
    tick_id: int
    ts: object
    price: float
    ptype: int           # +1 = HIGH, -1 = LOW
    src_id: Optional[int] = None

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

    Reassigns seq_id as 1..N in order.
    """
    if not pivots:
        return pivots

    pivots.sort(key=lambda p: (p.ts, p.tick_id))
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

        # tiny sleep to avoid hammering DB
        time.sleep(0.01)


def fetch_piv_hilo_for_symbol(conn, symbol: str) -> List[Pivot]:
    """
    Load all piv_hilo rows for a symbol, as micro-level pivots.

    src_id = piv_hilo.id
    """
    sql = """
        SELECT h.id AS hid, h.tick_id, h.ts, h.mid::float AS price, h.ptype
        FROM piv_hilo h
        JOIN ticks t ON t.id = h.tick_id
        WHERE t.symbol = %s
        ORDER BY h.id
    """
    cur = dict_cur(conn)
    cur.execute(sql, (symbol,))
    rows = cur.fetchall()
    pivots: List[Pivot] = []
    for idx, r in enumerate(rows, start=1):
        pivots.append(
            Pivot(
                seq_id=idx,
                tick_id=r["tick_id"],
                ts=r["ts"],
                price=float(r["price"]),
                ptype=int(r["ptype"]),
                src_id=int(r["hid"]),
            )
        )
    return pivots


def fetch_piv_swings_for_symbol(conn, symbol: str) -> List[Pivot]:
    """
    Load all piv_swings rows for a symbol, as middle-level pivots.

    src_id = piv_swings.id
    """
    sql = """
        SELECT s.id AS sid, s.tick_id, s.ts, s.mid::float AS price, s.ptype
        FROM piv_swings s
        JOIN ticks t ON t.id = s.tick_id
        WHERE t.symbol = %s
        ORDER BY s.id
    """
    cur = dict_cur(conn)
    cur.execute(sql, (symbol,))
    rows = cur.fetchall()
    pivots: List[Pivot] = []
    for idx, r in enumerate(rows, start=1):
        pivots.append(
            Pivot(
                seq_id=idx,
                tick_id=r["tick_id"],
                ts=r["ts"],
                price=float(r["price"]),
                ptype=int(r["ptype"]),
                src_id=int(r["sid"]),
            )
        )
    return pivots


# ---------------------------------------------------------------------------
# Level 1: Micro pivots from streaming ticks → piv_hilo
# ---------------------------------------------------------------------------

def build_micro_pivots_streaming(
    conn,
    symbol: str,
    tick_from: Optional[int],
    tick_to: Optional[int],
    batch_size: int,
    log_every: int,
    insert_batch_size: int,
) -> int:
    """
    Stream ticks, detect local highs/lows with a 21-tick window,
    and insert into piv_hilo.

    Returns number of inserted micro pivots.
    """
    radius = MICRO_TICK_RADIUS
    buffer: List[TickPoint] = []   # tail + current batch
    seq_counter = 0
    inserted_count = 0

    insert_rows: List[Tuple[int, object, float, int, int, int]] = []

    for batch in stream_ticks(conn, symbol, tick_from, tick_to, batch_size):
        buffer = buffer + batch
        prices = [t.price for t in buffer]

        # Leave last 2*radius ticks as overlap for the next batch
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

            if seq_counter % log_every == 0:
                logging.info(
                    "micro pivot #%d: tick_id=%d ts=%s price=%.5f",
                    seq_counter,
                    center.id,
                    center.ts,
                    center.price,
                )

            # stage row for piv_hilo
            insert_rows.append(
                (
                    center.id,    # tick_id
                    center.ts,    # ts
                    price,        # mid
                    ptype,        # ptype
                    radius,       # win_left
                    radius,       # win_right
                )
            )

            if len(insert_rows) >= insert_batch_size:
                _insert_piv_hilo_batch(conn, insert_rows)
                inserted_count += len(insert_rows)
                insert_rows.clear()

        # keep only last 2*radius ticks for overlap
        if len(buffer) > 2 * radius:
            buffer = buffer[-2 * radius :]

    # flush remaining
    if insert_rows:
        _insert_piv_hilo_batch(conn, insert_rows)
        inserted_count += len(insert_rows)
        insert_rows.clear()

    logging.info("micro pivots inserted for %s: count=%d", symbol, inserted_count)
    return inserted_count


def _insert_piv_hilo_batch(conn, rows: List[Tuple[int, object, float, int, int, int]]) -> None:
    """
    Insert batch into piv_hilo:

        (tick_id, ts, mid, ptype, win_left, win_right)
    """
    if not rows:
        return
    sql = """
        INSERT INTO piv_hilo (tick_id, ts, mid, ptype, win_left, win_right)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()


# ---------------------------------------------------------------------------
# Generic builder for Middle / Macro levels
# ---------------------------------------------------------------------------

def _build_level_from_pivots(
    base_level_name: str,
    level_name: str,
    pivots: List[Pivot],
    radius: int,
    log_every: int,
) -> List[Pivot]:
    """
    Generic builder:

      - Split pivots into highs and lows.
      - For each group (same type), use a 7-pivot window (radius=3).
      - Center is strict max/min in window among same-type peers.
      - Merge highs + lows, enforce alternation.

    src_id is preserved from input pivot.
    """
    if not pivots:
        logging.info("%s: no input pivots; returning empty list", level_name)
        return []

    pivots.sort(key=lambda p: (p.ts, p.tick_id))

    highs = [p for p in pivots if p.is_high]
    lows = [p for p in pivots if p.is_low]

    def select_extrema(input_pivots: List[Pivot], is_high: bool) -> List[Pivot]:
        if not input_pivots:
            return []
        values = [p.price for p in input_pivots]
        selected: List[Pivot] = []
        n = len(input_pivots)

        for idx in range(radius, n - radius):
            if is_high:
                if not _is_strict_max(values, idx, radius):
                    continue
            else:
                if not _is_strict_min(values, idx, radius):
                    continue

            base_p = input_pivots[idx]
            # preserve src_id (underlying DB id)
            p = Pivot(
                seq_id=0,  # will be assigned in _enforce_alternation
                tick_id=base_p.tick_id,
                ts=base_p.ts,
                price=base_p.price,
                ptype=base_p.ptype,
                src_id=base_p.src_id,
            )
            selected.append(p)

        return selected

    level_highs = select_extrema(highs, is_high=True)
    level_lows = select_extrema(lows, is_high=False)

    combined: List[Pivot] = level_highs + level_lows
    combined = _enforce_alternation(combined)

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


# ---------------------------------------------------------------------------
# Middle level: from piv_hilo → piv_swings
# ---------------------------------------------------------------------------

def build_middle_pivots_and_insert(
    conn,
    symbol: str,
    log_every: int,
) -> int:
    """
    Build middle-level swings from micro pivots (piv_hilo) and insert into
    piv_swings.

    For each selected middle pivot p:

        pivot_id    = p.src_id  (piv_hilo.id)
        tick_id     = p.tick_id
        ts, mid     = p.ts, p.price
        ptype       = p.ptype
        swing_index = p.seq_id

    Returns number of inserted middle pivots.
    """
    micro_pivots = fetch_piv_hilo_for_symbol(conn, symbol)
    middle = _build_level_from_pivots(
        base_level_name="micro",
        level_name="middle",
        pivots=micro_pivots,
        radius=MIDDLE_PIVOT_RADIUS,
        log_every=log_every,
    )
    if not middle:
        return 0

    sql = """
        INSERT INTO piv_swings (pivot_id, tick_id, ts, mid, ptype, swing_index)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    params = [
        (p.src_id, p.tick_id, p.ts, p.price, p.ptype, p.seq_id)
        for p in middle
    ]
    cur = conn.cursor()
    cur.executemany(sql, params)
    conn.commit()

    logging.info("middle pivots inserted for %s: count=%d", symbol, len(middle))
    return len(middle)


# ---------------------------------------------------------------------------
# Macro level: from piv_swings → hhll_piv
# ---------------------------------------------------------------------------

def build_macro_pivots_and_insert(
    conn,
    symbol: str,
    log_every: int,
) -> int:
    """
    Build macro-level HH/LL pivots from middle swings (piv_swings)
    and insert into hhll_piv.

    For each selected macro pivot p:

        swing_id   = p.src_id  (piv_swings.id)
        tick_id    = p.tick_id
        ts, mid    = p.ts, p.price
        ptype      = p.ptype
        class      = HHLL_CLASS_DEFAULT
        class_text = HHLL_CLASS_TEXT_DEFAULT

    Returns number of inserted macro pivots.
    """
    middle_swings = fetch_piv_swings_for_symbol(conn, symbol)
    macro = _build_level_from_pivots(
        base_level_name="middle",
        level_name="macro",
        pivots=middle_swings,
        radius=MACRO_PIVOT_RADIUS,
        log_every=log_every,
    )
    if not macro:
        return 0

    sql = """
        INSERT INTO hhll_piv (swing_id, tick_id, ts, mid, ptype, class, class_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = [
        (
            p.src_id,          # swing_id
            p.tick_id,
            p.ts,
            p.price,
            p.ptype,
            HHLL_CLASS_DEFAULT,
            HHLL_CLASS_TEXT_DEFAULT,
        )
        for p in macro
    ]
    cur = conn.cursor()
    cur.executemany(sql, params)
    conn.commit()

    logging.info("macro pivots inserted for %s: count=%d", symbol, len(macro))
    return len(macro)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Streaming high/low resolution builder")
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

    # Level 1: Micro (ticks → piv_hilo)
    micro_count = build_micro_pivots_streaming(
        conn=conn,
        symbol=args.symbol,
        tick_from=args.from_tick,
        tick_to=args.to_tick,
        batch_size=args.batch_size,
        log_every=args.log_every,
        insert_batch_size=args.insert_batch_size,
    )

    # Level 2: Middle (piv_hilo → piv_swings)
    middle_count = build_middle_pivots_and_insert(
        conn=conn,
        symbol=args.symbol,
        log_every=args.log_every,
    )

    # Level 3: Macro (piv_swings → hhll_piv)
    macro_count = build_macro_pivots_and_insert(
        conn=conn,
        symbol=args.symbol,
        log_every=args.log_every,
    )

    logging.info(
        "Finished all levels for %s: micro=%d, middle=%d, macro=%d",
        args.symbol,
        micro_count,
        middle_count,
        macro_count,
    )


if __name__ == "__main__":
    main()
