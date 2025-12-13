from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur


# =========================
# Data structures
# =========================

@dataclass
class TickRow:
    id: int
    ts: datetime
    kal: float


@dataclass
class SegmentFit:
    i0: int
    i1: int
    slope: float
    intercept: float
    sse: float


# =========================
# Utilities
# =========================

def _stddev_pop(xs: List[float]) -> float:
    n = len(xs)
    if n <= 1:
        return 0.0
    mu = sum(xs) / n
    return math.sqrt(sum((x - mu) ** 2 for x in xs) / n)


# =========================
# Prefix sums for O(1) fit cost
# =========================

class PrefixSums:
    def __init__(self, t: List[float], y: List[float]) -> None:
        n = len(t)
        self.t = t
        self.y = y
        self.S_t = [0.0] * (n + 1)
        self.S_y = [0.0] * (n + 1)
        self.S_tt = [0.0] * (n + 1)
        self.S_ty = [0.0] * (n + 1)
        self.S_yy = [0.0] * (n + 1)

        for i in range(n):
            ti, yi = t[i], y[i]
            self.S_t[i + 1] = self.S_t[i] + ti
            self.S_y[i + 1] = self.S_y[i] + yi
            self.S_tt[i + 1] = self.S_tt[i] + ti * ti
            self.S_ty[i + 1] = self.S_ty[i] + ti * yi
            self.S_yy[i + 1] = self.S_yy[i] + yi * yi

    def fit_cost(self, i0: int, i1: int) -> SegmentFit:
        n = i1 - i0 + 1
        S_t = self.S_t[i1 + 1] - self.S_t[i0]
        S_y = self.S_y[i1 + 1] - self.S_y[i0]
        S_tt = self.S_tt[i1 + 1] - self.S_tt[i0]
        S_ty = self.S_ty[i1 + 1] - self.S_ty[i0]
        S_yy = self.S_yy[i1 + 1] - self.S_yy[i0]

        den = n * S_tt - S_t * S_t
        if abs(den) < 1e-18:
            a = 0.0
            b = S_y / n
        else:
            a = (n * S_ty - S_t * S_y) / den
            b = (S_y - a * S_t) / n

        sse = (
            S_yy
            - 2 * a * S_ty
            - 2 * b * S_y
            + a * a * S_tt
            + 2 * a * b * S_t
            + b * b * n
        )
        return SegmentFit(i0, i1, a, b, max(0.0, sse))


# =========================
# FIXED PELT implementation
# =========================

def pelt(prefix: PrefixSums, penalty: float, min_len: int = 2) -> List[int]:
    n = len(prefix.t)
    if n == 0:
        return []

    F = [0.0] * (n + 1)
    prev = [-1] * (n + 1)
    R = [0]

    F[0] = -penalty

    def seg_cost(s: int, t_excl: int) -> float:
        if t_excl - s < min_len:
            return float("inf")
        return prefix.fit_cost(s, t_excl - 1).sse

    for t_excl in range(1, n + 1):
        best_val = float("inf")
        best_s = -1

        for s in R:
            v = F[s] + seg_cost(s, t_excl) + penalty
            if v < best_val:
                best_val = v
                best_s = s

        F[t_excl] = best_val
        prev[t_excl] = best_s

        # --- FIXED pruning ---
        new_R = []
        for s in R:
            if t_excl - s < min_len:
                new_R.append(s)
                continue
            if F[s] + seg_cost(s, t_excl) <= F[t_excl] + penalty:
                new_R.append(s)

        new_R.append(t_excl)
        R = new_R

    ends: List[int] = []
    t = n
    while t > 0:
        s = prev[t]
        if s < 0:
            break
        ends.append(t - 1)
        t = s

    return list(reversed(ends))


# =========================
# L2 refinement with adaptive penalty
# =========================

def refine_segment(parent_segment_id: int, c2_init: float, run_id: Optional[str]) -> None:
    conn = get_conn()
    conn.autocommit = False

    with conn, dict_cur(conn) as cur:
        cur.execute("""
            SELECT symbol, start_tick_id, end_tick_id, start_ts
            FROM segms
            WHERE id=%s
        """, (parent_segment_id,))
        p = cur.fetchone()
        if not p:
            raise RuntimeError("parent segment not found")

        symbol = p["symbol"]
        start_tick_id = p["start_tick_id"]
        end_tick_id = p["end_tick_id"]
        parent_start_ts = p["start_ts"]

        cur.execute("""
            SELECT id, timestamp AS ts, kal
            FROM ticks
            WHERE symbol=%s AND id BETWEEN %s AND %s
            ORDER BY timestamp ASC, id ASC
        """, (symbol, start_tick_id, end_tick_id))

        ticks = [TickRow(int(r["id"]), r["ts"], float(r["kal"]))
                 for r in cur.fetchall() if r["kal"] is not None]

        if len(ticks) < 5:
            print(f"[refineL2] parent={parent_segment_id} too few ticks")
            return

        if run_id:
            cur.execute("DELETE FROM segticks_l2 WHERE parent_segment_id=%s AND run_id=%s",
                        (parent_segment_id, run_id))
            cur.execute("DELETE FROM segms_l2 WHERE parent_segment_id=%s AND run_id=%s",
                        (parent_segment_id, run_id))

        t = [(x.ts - parent_start_ts).total_seconds() for x in ticks]
        y = [x.kal for x in ticks]

        sigma = _stddev_pop(y)
        pref = PrefixSums(t, y)

        # --- adaptive search ---
        target_min, target_max = 5, 40
        max_iters = 12
        c2 = c2_init
        c2_min, c2_max = 1e-6, 1e3

        best = None

        for _ in range(max_iters):
            c2 = max(min(c2, c2_max), c2_min)
            lam = c2 * sigma * sigma
            ends = pelt(pref, lam, min_len=2)
            if not ends:
                ends = [len(ticks) - 1]

            K = len(ends)
            if best is None or (
                K >= target_min and K <= target_max and
                (best[0] < target_min or K < best[0])
            ):
                best = (K, c2, ends)

            if target_min <= K <= target_max:
                break

            c2 = c2 / 2 if K < target_min else c2 * 2

        K, best_c2, ends = best

        print(
            f"[refineL2] parent={parent_segment_id} symbol={symbol} "
            f"ticks={len(ticks)} sigma={sigma:.6g} best_c2={best_c2:.6g} "
            f"K={K} target=[{target_min},{target_max}] run_id={run_id}"
        )

        # --- write segments ---
        s0 = 0
        for local_idx, e in enumerate(ends):
            i0, i1 = s0, e
            s0 = e + 1

            fit = pref.fit_cost(i0, i1)
            st, en = ticks[i0], ticks[i1]

            dur_ticks = i1 - i0 + 1
            dur_seconds = (en.ts - st.ts).total_seconds()
            price_change = fit.slope * (t[i1] - t[i0])
            mse = fit.sse / dur_ticks

            cur.execute("""
                INSERT INTO segms_l2 (
                    symbol, parent_segment_id, local_seg_index,
                    start_tick_id, end_tick_id,
                    start_ts, end_ts,
                    t_axis_type,
                    slope, intercept,
                    duration_ticks, duration_seconds,
                    price_change, mse, run_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,'seconds',%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                symbol, parent_segment_id, local_idx,
                st.id, en.id,
                st.ts, en.ts,
                fit.slope, fit.intercept,
                dur_ticks, dur_seconds,
                price_change, mse, run_id
            ))

            l2_id = cur.fetchone()["id"]

            rows = []
            denom = t[i1] - t[i0]
            for k in range(i0, i1 + 1):
                pos = 0.0 if denom <= 0 else (t[k] - t[i0]) / denom
                rows.append((
                    symbol, ticks[k].id, l2_id, parent_segment_id,
                    pos, fit.slope, price_change, dur_seconds, run_id
                ))

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO segticks_l2 (
                    symbol, tick_id, l2_segment_id, parent_segment_id,
                    seg_pos, seg_slope, seg_price_change, seg_duration_seconds, run_id
                ) VALUES %s
                """,
                rows
            )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parent-segment-id", type=int, required=True)
    ap.add_argument("--c2", type=float, default=0.5)
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    refine_segment(args.parent_segment_id, args.c2, args.run_id)


if __name__ == "__main__":
    main()
