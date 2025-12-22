# jobs/buildZigPivots.py
# Compute zig pivots per segm using strict 21-tick local extrema and alternating pivots.

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import psycopg2.extras

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr


WINDOW_SIZE = 21
HALF_WINDOW = WINDOW_SIZE // 2


@dataclass
class TickRow:
    id: int
    ts: object
    price: float


def _qualify_mid_expr(expr: str) -> str:
    if expr == "price":
        return "t.price"
    if expr == "mid":
        return "t.mid"
    if expr == "(bid+ask)/2.0":
        return "(t.bid+t.ask)/2.0"
    return expr


def _load_segment_ticks(conn, segm_id: int) -> List[TickRow]:
    ts_col = detect_ts_col(conn)
    mid_expr = _qualify_mid_expr(detect_mid_expr(conn))

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT t.id AS tick_id,
                   t.{ts_col} AS ts,
                   {mid_expr} AS price
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE st.segm_id=%s
            ORDER BY t.{ts_col} ASC, t.id ASC
            """,
            (int(segm_id),),
        )
        rows = cur.fetchall()

    out: List[TickRow] = []
    for r in rows:
        if r["price"] is None:
            continue
        out.append(TickRow(id=int(r["tick_id"]), ts=r["ts"], price=float(r["price"])))
    return out


def _detect_local_extreme(prices: List[float], i: int, half_window: int) -> Optional[str]:
    p = prices[i]
    is_high = True
    is_low = True

    for j in range(i - half_window, i + half_window + 1):
        if j == i:
            continue
        q = prices[j]
        if p <= q:
            is_high = False
        if p >= q:
            is_low = False
        if not is_high and not is_low:
            return None

    if is_high:
        return "high"
    if is_low:
        return "low"
    return None


def _pivot_from_tick(tick: TickRow, direction: str) -> Dict[str, object]:
    return {
        "tick_id": tick.id,
        "ts": tick.ts,
        "price": tick.price,
        "direction": direction,
    }


def compute_zig_pivots(ticks: List[TickRow], window_size: int = WINDOW_SIZE) -> List[Dict[str, object]]:
    if len(ticks) < window_size:
        # Short segments: no 21-tick centers, so return no pivots.
        return []

    half = window_size // 2
    prices = [t.price for t in ticks]

    pivots: List[Dict[str, object]] = []
    current_type: Optional[str] = None
    current_idx: Optional[int] = None

    first_idx: Optional[int] = None
    for i in range(half, len(ticks) - half):
        ctype = _detect_local_extreme(prices, i, half)
        if ctype is None:
            continue
        cprice = prices[i]
        if ctype == "low" and cprice < prices[0]:
            pivots.append(_pivot_from_tick(ticks[0], "high"))
            pivots.append(_pivot_from_tick(ticks[i], "low"))
            current_type = "low"
            current_idx = i
            first_idx = i
            break
        if ctype == "high" and cprice > prices[0]:
            pivots.append(_pivot_from_tick(ticks[0], "low"))
            pivots.append(_pivot_from_tick(ticks[i], "high"))
            current_type = "high"
            current_idx = i
            first_idx = i
            break

    if not pivots or current_type is None or current_idx is None or first_idx is None:
        return []

    for i in range(first_idx + 1, len(ticks) - half):
        ctype = _detect_local_extreme(prices, i, half)
        if ctype is None:
            continue
        cprice = prices[i]

        if current_type == "high":
            if ctype == "high":
                if cprice >= prices[current_idx]:
                    pivots[-1] = _pivot_from_tick(ticks[i], "high")
                    current_idx = i
            else:  # low candidate
                pivots.append(_pivot_from_tick(ticks[i], "low"))
                current_type = "low"
                current_idx = i
        else:  # current_type == "low"
            if ctype == "low":
                if cprice <= prices[current_idx]:
                    pivots[-1] = _pivot_from_tick(ticks[i], "low")
                    current_idx = i
            else:  # high candidate
                pivots.append(_pivot_from_tick(ticks[i], "high"))
                current_type = "high"
                current_idx = i

    for idx, p in enumerate(pivots):
        p["pivot_index"] = idx

    return pivots


def _build_pivot_rows(segm_id: int, pivots: Iterable[Dict[str, object]]) -> List[tuple]:
    rows = []
    for p in pivots:
        rows.append(
            (
                int(segm_id),
                int(p["tick_id"]),
                p["ts"],
                float(p["price"]),
                str(p["direction"]),
                int(p["pivot_index"]),
            )
        )
    return rows


def clear_zig_pivots(conn, segm_id: int) -> None:
    with dict_cur(conn) as cur:
        cur.execute("DELETE FROM public.zig_pivots WHERE segm_id=%s", (int(segm_id),))


def save_zig_pivots(conn, segm_id: int, pivots: List[Dict[str, object]]) -> int:
    clear_zig_pivots(conn, segm_id)
    if not pivots:
        return 0

    rows = _build_pivot_rows(segm_id, pivots)
    with dict_cur(conn) as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.zig_pivots (
                segm_id, tick_id, ts, price, direction, pivot_index
            )
            VALUES %s
            """,
            rows,
            page_size=5000,
        )
    return len(rows)


def recompute_zig_pivots_for_segment(conn, segm_id: int) -> int:
    ticks = _load_segment_ticks(conn, segm_id)
    pivots = compute_zig_pivots(ticks)
    return save_zig_pivots(conn, segm_id, pivots)


def list_segment_ids(conn) -> List[int]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT segm_id
            FROM public.segticks
            ORDER BY segm_id ASC
            """
        )
        return [int(r["segm_id"]) for r in cur.fetchall()]


def _validate_pivots(pivots: List[Dict[str, object]]) -> Optional[str]:
    if not pivots:
        return None

    last_ts = None
    last_dir = None
    for p in pivots:
        ts = p.get("ts")
        if last_ts is not None and ts is not None and ts < last_ts:
            return "pivot timestamps not in chronological order"
        last_ts = ts

        direction = p.get("direction")
        if last_dir is not None and direction == last_dir:
            return "pivot directions do not alternate"
        last_dir = direction

    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, help="Segment id to recompute")
    ap.add_argument("--all", action="store_true", help="Recompute all segments")
    ap.add_argument("--validate", action="store_true", help="Run basic ordering/alternation checks")
    args = ap.parse_args()

    if not args.all and not args.segm_id:
        raise SystemExit("Use --segm-id or --all")

    conn = get_conn()
    try:
        if args.all:
            segm_ids = list_segment_ids(conn)
        else:
            segm_ids = [int(args.segm_id)]

        for segm_id in segm_ids:
            n = recompute_zig_pivots_for_segment(conn, segm_id)
            print(f"[zig_pivots] segm_id={segm_id} pivots={n}")

            if args.validate:
                with dict_cur(conn) as cur:
                    cur.execute(
                        """
                        SELECT tick_id, ts, price, direction, pivot_index
                        FROM public.zig_pivots
                        WHERE segm_id=%s
                        ORDER BY pivot_index ASC
                        """,
                        (segm_id,),
                    )
                    rows = cur.fetchall()
                err = _validate_pivots(rows)
                if err:
                    raise RuntimeError(f"[zig_pivots] segm_id={segm_id} invalid: {err}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
