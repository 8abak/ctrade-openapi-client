# ml/resolution.py

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

from backend.db import get_conn, dict_cur

# ---------------------------------------------------------------------------
# Configuration for peer windows
# ---------------------------------------------------------------------------

# Micro: how many *ticks on each side* to consider as peers
MICRO_TICK_PEERS = 5          # → window size = 2*5 + 1 = 11

# Middle: how many *micro pivots of same type* on each side
MIDDLE_MICRO_PEERS = 3

# Macro: how many *middle pivots of same type* on each side
MACRO_MIDDLE_PEERS = 3


# ---------------------------------------------------------------------------
# Basic data structures
# ---------------------------------------------------------------------------

@dataclass
class TickPoint:
    id: int
    ts: object  # datetime
    price: float


@dataclass
class Pivot:
    id: Optional[int]         # DB id if exists, None for in-memory-only
    tick_id: int
    ts: object
    price: float
    ptype: int                # +1 = HIGH, -1 = LOW

    @property
    def is_high(self) -> bool:
        return self.ptype > 0

    @property
    def is_low(self) -> bool:
        return self.ptype < 0


@dataclass
class AggregatedPivot(Pivot):
    """
    Pivot at a coarser resolution with a range of underlying ids.
    For Middle level: source_* are hhll_piv.id ranges.
    For Macro level: source_* are piv_hilo.id ranges (and we can also
    derive min/max hhll_piv.id).
    """
    source_min_id: int
    source_max_id: int
    # For Macro we may also carry hhll ranges in memory if desired.


# ---------------------------------------------------------------------------
# Core helpers: local extrema selection
# ---------------------------------------------------------------------------

def _local_max_indices(values: Sequence[float], peer_radius: int) -> List[int]:
    """
    Return indices i such that values[i] is strictly greater than all
    values in the window [i-peer_radius, i+peer_radius] (excluding i).
    """
    n = len(values)
    result: List[int] = []
    for i in range(n):
        left = max(0, i - peer_radius)
        right = min(n - 1, i + peer_radius)
        v = values[i]
        is_max = True
        for j in range(left, right + 1):
            if j == i:
                continue
            if values[j] >= v:
                is_max = False
                break
        if is_max:
            result.append(i)
    return result


def _local_min_indices(values: Sequence[float], peer_radius: int) -> List[int]:
    """
    Return indices i such that values[i] is strictly smaller than all
    values in the window [i-peer_radius, i+peer_radius] (excluding i).
    """
    n = len(values)
    result: List[int] = []
    for i in range(n):
        left = max(0, i - peer_radius)
        right = min(n - 1, i + peer_radius)
        v = values[i]
        is_min = True
        for j in range(left, right + 1):
            if j == i:
                continue
            if values[j] <= v:
                is_min = False
                break
        if is_min:
            result.append(i)
    return result


def _enforce_alternation(pivots: List[Pivot]) -> List[Pivot]:
    """
    Ensure HIGH/LOW alternation by removing weaker duplicates of same type.
    Assumes pivots are sorted by time/id.
    """
    if not pivots:
        return pivots

    result: List[Pivot] = [pivots[0]]
    for p in pivots[1:]:
        last = result[-1]
        if p.ptype == last.ptype:
            # Same type: keep the more extreme
            if p.is_high:
                # keep the higher high
                if p.price > last.price:
                    result[-1] = p
            else:
                # keep the lower low
                if p.price < last.price:
                    result[-1] = p
        else:
            result.append(p)
    return result


# ---------------------------------------------------------------------------
# DB fetch helpers (adapt column names to actual schema)
# ---------------------------------------------------------------------------

def fetch_ticks(
    conn,
    symbol: str,
    tick_from: Optional[int] = None,
    tick_to: Optional[int] = None,
) -> List[TickPoint]:
    """
    Load ticks for a symbol. Adjust column names (timestamp/mid) to your schema.
    """
    sql = """
        SELECT id, timestamp, mid
        FROM ticks
        WHERE symbol = %s
    """
    params: List[object] = [symbol]
    if tick_from is not None:
        sql += " AND id >= %s"
        params.append(tick_from)
    if tick_to is not None:
        sql += " AND id <= %s"
        params.append(tick_to)
    sql += " ORDER BY id"

    cur = dict_cur(conn)
    cur.execute(sql, params)
    rows = cur.fetchall()
    return [TickPoint(id=r["id"], ts=r["timestamp"], price=float(r["mid"])) for r in rows]


def fetch_micro_from_db(
    conn,
    symbol: str,
    micro_from_id: Optional[int] = None,
    micro_to_id: Optional[int] = None,
) -> List[Pivot]:
    """
    Load existing Micro pivots (hhll_piv) by joining via ticks to filter by symbol.
    Adjust column names (tick_id, ts, mid, ptype) to your schema.
    """
    sql = """
        SELECT h.id, h.tick_id, h.ts, h.mid::float AS price, h.ptype
        FROM hhll_piv h
        JOIN ticks t ON t.id = h.tick_id
        WHERE t.symbol = %s
    """
    params: List[object] = [symbol]
    if micro_from_id is not None:
        sql += " AND h.id >= %s"
        params.append(micro_from_id)
    if micro_to_id is not None:
        sql += " AND h.id <= %s"
        params.append(micro_to_id)
    sql += " ORDER BY h.id"

    cur = dict_cur(conn)
    cur.execute(sql, params)
    rows = cur.fetchall()
    return [
        Pivot(
            id=r["id"],
            tick_id=r["tick_id"],
            ts=r["ts"],
            price=float(r["price"]),
            ptype=int(r["ptype"]),
        )
        for r in rows
    ]


def fetch_middle_from_db(
    conn,
    symbol: str,
    middle_from_id: Optional[int] = None,
    middle_to_id: Optional[int] = None,
) -> List[Pivot]:
    """
    Load existing Middle pivots (piv_hilo). Adjust names to schema.
    """
    sql = """
        SELECT p.id, p.tick_id, p.ts, p.mid::float AS price, p.ptype
        FROM piv_hilo p
        JOIN ticks t ON t.id = p.tick_id
        WHERE t.symbol = %s
    """
    params: List[object] = [symbol]
    if middle_from_id is not None:
        sql += " AND p.id >= %s"
        params.append(middle_from_id)
    if middle_to_id is not None:
        sql += " AND p.id <= %s"
        params.append(middle_to_id)
    sql += " ORDER BY p.id"

    cur = dict_cur(conn)
    cur.execute(sql, params)
    rows = cur.fetchall()
    return [
        Pivot(
            id=r["id"],
            tick_id=r["tick_id"],
            ts=r["ts"],
            price=float(r["price"]),
            ptype=int(r["ptype"]),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Micro level: ticks → in-memory Micro pivots
# ---------------------------------------------------------------------------

def build_micro_pivots_from_ticks(
    conn,
    symbol: str,
    tick_from: Optional[int] = None,
    tick_to: Optional[int] = None,
) -> List[Pivot]:
    """
    New Micro algorithm:
      - Look at tick series.
      - Micro highs: each tick whose price is higher than all its
        MICRO_TICK_PEERS ticks on each side.
      - Micro lows: each tick whose price is lower than all its peers.
      - Enforce alternation.
    Returns in-memory Pivot objects (no DB writes here).
    """
    ticks = fetch_ticks(conn, symbol, tick_from, tick_to)
    if not ticks:
        logging.info("No ticks for %s in [%s, %s]", symbol, tick_from, tick_to)
        return []

    prices = [t.price for t in ticks]
    high_idx = _local_max_indices(prices, MICRO_TICK_PEERS)
    low_idx = _local_min_indices(prices, MICRO_TICK_PEERS)

    pivots: List[Pivot] = []
    for i in high_idx:
        t = ticks[i]
        pivots.append(Pivot(
            id=None,
            tick_id=t.id,
            ts=t.ts,
            price=t.price,
            ptype=+1,
        ))
    for i in low_idx:
        t = ticks[i]
        pivots.append(Pivot(
            id=None,
            tick_id=t.id,
            ts=t.ts,
            price=t.price,
            ptype=-1,
        ))

    # Sort by time / id and enforce HIGH/LOW alternation
    pivots.sort(key=lambda p: (p.ts, p.tick_id))
    pivots = _enforce_alternation(pivots)

    logging.info(
        "Micro (in-memory) for %s: ticks=%d → micro_pivots=%d",
        symbol, len(ticks), len(pivots),
    )
    return pivots


# ---------------------------------------------------------------------------
# Middle level: Micro pivots → Middle pivots (peer-based)
# ---------------------------------------------------------------------------

def _select_from_same_type_peers(
    pivots: List[Pivot],
    peer_radius: int,
    is_high: bool,
) -> List[AggregatedPivot]:
    """
    Given only highs or only lows (sorted by time), select those that are
    local extremes among their same-type peers.
    Returns AggregatedPivot with source_min_id/source_max_id representing
    min/max *id* of underlying pivots in the peer window.
    """
    if not pivots:
        return []

    prices = [p.price for p in pivots]
    if is_high:
        idx = _local_max_indices(prices, peer_radius)
    else:
        idx = _local_min_indices(prices, peer_radius)

    agg: List[AggregatedPivot] = []
    n = len(pivots)
    for k in idx:
        left = max(0, k - peer_radius)
        right = min(n - 1, k + peer_radius)
        window = pivots[left:right+1]
        min_id = min(p.id for p in window if p.id is not None)
        max_id = max(p.id for p in window if p.id is not None)
        p = pivots[k]
        agg.append(
            AggregatedPivot(
                id=p.id,
                tick_id=p.tick_id,
                ts=p.ts,
                price=p.price,
                ptype=p.ptype,
                source_min_id=min_id,
                source_max_id=max_id,
            )
        )
    return agg


def build_middle_from_micro(
    conn,
    symbol: str,
    micro_from_id: Optional[int] = None,
    micro_to_id: Optional[int] = None,
) -> List[AggregatedPivot]:
    """
    New Middle algorithm:
      - Take Micro pivots from hhll_piv (DB).
      - Among Micro highs, select those that are higher than their
        MIDDLE_MICRO_PEERS same-type Micro highs.
      - Same for lows.
      - Enforce alternation.
    Returns in-memory AggregatedPivot list.
    """
    micro = fetch_micro_from_db(conn, symbol, micro_from_id, micro_to_id)
    if not micro:
        logging.info("No micro pivots for %s", symbol)
        return []

    micro.sort(key=lambda p: (p.ts, p.id))

    micro_highs = [p for p in micro if p.is_high]
    micro_lows = [p for p in micro if p.is_low]

    mid_highs = _select_from_same_type_peers(micro_highs, MIDDLE_MICRO_PEERS, is_high=True)
    mid_lows = _select_from_same_type_peers(micro_lows, MIDDLE_MICRO_PEERS, is_high=False)

    middle: List[AggregatedPivot] = mid_highs + mid_lows
    middle.sort(key=lambda p: (p.ts, p.id))
    middle = [_as_agg(p) for p in _enforce_alternation(middle)]

    logging.info(
        "Middle (in-memory) for %s: micro=%d → middle=%d",
        symbol, len(micro), len(middle),
    )
    return middle


def _as_agg(p: Pivot) -> AggregatedPivot:
    if isinstance(p, AggregatedPivot):
        return p
    # If no source range was set (rare case), default to [id,id]
    assert p.id is not None, "Pivot must have id to be aggregated"
    return AggregatedPivot(
        id=p.id,
        tick_id=p.tick_id,
        ts=p.ts,
        price=p.price,
        ptype=p.ptype,
        source_min_id=p.id,
        source_max_id=p.id,
    )


# ---------------------------------------------------------------------------
# Macro level: Middle pivots → Macro pivots (peer-based)
# ---------------------------------------------------------------------------

def build_macro_from_middle(
    conn,
    symbol: str,
    middle_from_id: Optional[int] = None,
    middle_to_id: Optional[int] = None,
) -> List[AggregatedPivot]:
    """
    New Macro algorithm:
      - Take Middle pivots from piv_hilo (DB).
      - Among Middle highs, select those that are higher than their
        MACRO_MIDDLE_PEERS same-type Middle highs.
      - Same for lows.
      - Enforce alternation.
    Returns AggregatedPivot list with source_min_id/source_max_id in
    terms of piv_hilo.id.
    """
    middle = fetch_middle_from_db(conn, symbol, middle_from_id, middle_to_id)
    if not middle:
        logging.info("No middle pivots for %s", symbol)
        return []

    middle.sort(key=lambda p: (p.ts, p.id))

    mid_highs = [p for p in middle if p.is_high]
    mid_lows = [p for p in middle if p.is_low]

    mac_highs = _select_from_same_type_peers(mid_highs, MACRO_MIDDLE_PEERS, is_high=True)
    mac_lows = _select_from_same_type_peers(mid_lows, MACRO_MIDDLE_PEERS, is_high=False)

    macro: List[AggregatedPivot] = mac_highs + mac_lows
    macro.sort(key=lambda p: (p.ts, p.id))
    macro = [_as_agg(p) for p in _enforce_alternation(macro)]

    logging.info(
        "Macro (in-memory) for %s: middle=%d → macro=%d",
        symbol, len(middle), len(macro),
    )
    return macro


# ---------------------------------------------------------------------------
# CLI (analysis only – DB writes can be added later)
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="High/low based 3-level resolution")
    p.add_argument("--symbol", required=True)
    p.add_argument("--from-tick", type=int, default=None)
    p.add_argument("--to-tick", type=int, default=None)
    p.add_argument(
        "--levels",
        default="all",
        help="Comma-separated subset of {micro,middle,macro,all}",
    )
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
    levels = {l.strip().lower() for l in args.levels.split(",")}
    if "all" in levels:
        levels = {"micro", "middle", "macro"}

    conn = get_conn()

    if "micro" in levels:
        _ = build_micro_pivots_from_ticks(
            conn,
            args.symbol,
            args.from_tick,
            args.to_tick,
        )

    if "middle" in levels:
        _ = build_middle_from_micro(conn, args.symbol)

    if "macro" in levels:
        _ = build_macro_from_middle(conn, args.symbol)


if __name__ == "__main__":
    main()
