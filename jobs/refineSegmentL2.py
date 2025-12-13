# jobs/refineSegmentL2.py
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur


@dataclass
class TickRow:
    id: int
    ts: object  # datetime with tz
    kal: float


@dataclass
class SegmentFit:
    i0: int
    i1: int
    slope: float
    intercept: float
    sse: float


def _stddev_pop(xs: List[float]) -> float:
    n = len(xs)
    if n <= 1:
        return 0.0
    mu = sum(xs) / n
    v = sum((x - mu) ** 2 for x in xs) / n
    return math.sqrt(v)


class PrefixSums:
    """
    Prefix sums enabling O(1) SSE for linear regression on any [i0,i1].
    """

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
            ti = float(t[i])
            yi = float(y[i])
            self.S_t[i + 1] = self.S_t[i] + ti
            self.S_y[i + 1] = self.S_y[i] + yi
            self.S_tt[i + 1] = self.S_tt[i] + ti * ti
            self.S_ty[i + 1] = self.S_ty[i] + ti * yi
            self.S_yy[i + 1] = self.S_yy[i] + yi * yi

    def _seg_sums(self, i0: int, i1: int) -> Tuple[int, float, float, float, float, float]:
        n = i1 - i0 + 1
        S_t = self.S_t[i1 + 1] - self.S_t[i0]
        S_y = self.S_y[i1 + 1] - self.S_y[i0]
        S_tt = self.S_tt[i1 + 1] - self.S_tt[i0]
        S_ty = self.S_ty[i1 + 1] - self.S_ty[i0]
        S_yy = self.S_yy[i1 + 1] - self.S_yy[i0]
        return n, S_t, S_y, S_tt, S_ty, S_yy

    def fit_cost(self, i0: int, i1: int) -> SegmentFit:
        """
        Fit y = a*t + b on indices [i0, i1] inclusive, return SSE.
        """
        n, S_t, S_y, S_tt, S_ty, S_yy = self._seg_sums(i0, i1)

        den = n * S_tt - S_t * S_t
        if abs(den) < 1e-18:
            a = 0.0
            b = (S_y / n) if n else 0.0
        else:
            a = (n * S_ty - S_t * S_y) / den
            b = (S_y - a * S_t) / n

        sse = (
            S_yy
            - 2.0 * a * S_ty
            - 2.0 * b * S_y
            + (a * a) * S_tt
            + 2.0 * a * b * S_t
            + (b * b) * n
        )
        if sse < 0 and sse > -1e-9:
            sse = 0.0
        return SegmentFit(i0, i1, float(a), float(b), float(max(0.0, sse)))


def pelt(prefix: PrefixSums, penalty: float, min_len: int = 2) -> List[int]:
    """
    Penalized DP with PELT-style pruning.
    Returns list of inclusive end indices per segment.
    Pruning is min_len-safe (short segments do NOT trigger pruning).
    """
    n = len(prefix.t)
    if n == 0:
        return []

    F = [0.0] * (n + 1)
    prev = [-1] * (n + 1)
    R: List[int] = [0]

    F[0] = -penalty

    def seg_cost(s: int, t_excl: int) -> float:
        if t_excl - s < min_len:
            return float("inf")
        return prefix.fit_cost(s, t_excl - 1).sse

    for t_excl in range(1, n + 1):
        best_val = float("inf")
        best_s = -1

        for s in R:
            c = seg_cost(s, t_excl)
            if math.isinf(c):
                continue
            v = F[s] + c + penalty
            if v < best_val:
                best_val = v
                best_s = s

        F[t_excl] = best_val
        prev[t_excl] = best_s

        # --- FIXED pruning: keep s if segment too short ---
        new_R: List[int] = []
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
    ends.reverse()
    return ends


def refine_segment(
    parent_segment_id: int,
    c2_init: float,
    run_id: Optional[str],
    target_min_segs: int = 5,
    target_max_segs: int = 40,
    max_iters: int = 12,
) -> None:
    conn = get_conn()
    conn.autocommit = False

    with conn, dict_cur(conn) as cur:
        # --- parent segment ---
        cur.execute(
            """
            SELECT id, symbol, start_tick_id, end_tick_id, start_ts
            FROM segms
            WHERE id = %s
            """,
            (parent_segment_id,),
        )
        p = cur.fetchone()
        if not p:
            raise RuntimeError(f"parent_segment_id={parent_segment_id} not found")

        symbol = p["symbol"]
        start_tick_id = int(p["start_tick_id"])
        end_tick_id = int(p["end_tick_id"])
        parent_start_ts = p["start_ts"]

        # --- ticks inside parent ---
        # Your DB uses ticks.timestamp
        cur.execute(
            """
            SELECT id, timestamp AS ts, kal
            FROM ticks
            WHERE symbol=%s AND id BETWEEN %s AND %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, start_tick_id, end_tick_id),
        )
        rows = cur.fetchall()

        ticks: List[TickRow] = []
        for r in rows:
            if r["kal"] is None:
                continue
            ticks.append(TickRow(int(r["id"]), r["ts"], float(r["kal"])))

        if len(ticks) < 5:
            print(f"[refineL2] parent={parent_segment_id} symbol={symbol} too few ticks={len(ticks)} -> skip")
            return

        # cleanup prior run rows (so reruns don't accumulate)
        if run_id:
            cur.execute("DELETE FROM segticks_l2 WHERE parent_segment_id=%s AND run_id=%s", (parent_segment_id, run_id))
            cur.execute("DELETE FROM segms_l2   WHERE parent_segment_id=%s AND run_id=%s", (parent_segment_id, run_id))

        t = [(tr.ts - parent_start_ts).total_seconds() for tr in ticks]
        y = [tr.kal for tr in ticks]

        sigma = _stddev_pop(y)
        pref = PrefixSums(t, y)

        # =========================
        # Adaptive penalty search (per your spec)
        # =========================
        c2_min = 1e-6
        c2_max = 1e3
        c2_value = c2_init

        def _segment_with_c2(c2v: float) -> Tuple[float, float, List[int]]:
            lambda2 = c2v * (sigma ** 2)
            ends = pelt(pref, penalty=lambda2, min_len=2)
            if not ends:
                ends = [len(ticks) - 1]
            return c2v, lambda2, ends

        best_ends: Optional[List[int]] = None
        best_K = 0
        best_c2 = c2_init
        best_lambda2 = c2_init * (sigma ** 2)

        for it in range(max_iters):
            c2_value = max(min(c2_value, c2_max), c2_min)
            c2_used, lambda2_used, ends = _segment_with_c2(c2_value)
            K = len(ends)

            if best_ends is None:
                best_ends, best_K, best_c2, best_lambda2 = ends, K, c2_used, lambda2_used
            else:
                if (best_K < target_min_segs and K > best_K) or \
                   (target_min_segs <= K <= target_max_segs and
                    (best_K < target_min_segs or K < best_K)):
                    best_ends, best_K, best_c2, best_lambda2 = ends, K, c2_used, lambda2_used

            if target_min_segs <= K <= target_max_segs:
                break

            if K < target_min_segs:
                c2_value /= 2.0
            else:
                c2_value *= 2.0

        ends = best_ends if best_ends is not None else [len(ticks) - 1]

        print(
            f"[refineL2] parent={parent_segment_id} symbol={symbol} "
            f"ticks={len(ticks)} sigma={sigma:.6g} best_c2={best_c2:.6g} "
            f"lambda2={best_lambda2:.6g} K={best_K} "
            f"target=[{target_min_segs},{target_max_segs}] run_id={run_id}"
        )

        # ends are inclusive end indices -> bounds
        bounds: List[Tuple[int, int]] = []
        s0 = 0
        for e in ends:
            e = int(e)
            if e < s0:
                continue
            bounds.append((s0, e))
            s0 = e + 1
        if not bounds:
            bounds = [(0, len(ticks) - 1)]
        if bounds[-1][1] != len(ticks) - 1:
            bounds[-1] = (bounds[-1][0], len(ticks) - 1)

        # write segments + tick mapping
        for local_seg_index, (i0, i1) in enumerate(bounds):
            if i1 - i0 + 1 < 2:
                continue

            fit = pref.fit_cost(i0, i1)

            st = ticks[i0]
            en = ticks[i1]
            start_ts = st.ts
            end_ts = en.ts

            duration_ticks = i1 - i0 + 1
            duration_seconds = float((end_ts - start_ts).total_seconds())

            t_start = float(t[i0])
            t_end = float(t[i1])

            price_change = float(fit.slope * (t_end - t_start))
            mse = float(fit.sse / duration_ticks) if duration_ticks else 0.0

            cur.execute(
                """
                INSERT INTO segms_l2 (
                    symbol, parent_segment_id, local_seg_index,
                    start_tick_id, end_tick_id,
                    start_ts, end_ts,
                    t_axis_type,
                    slope, intercept,
                    duration_ticks, duration_seconds,
                    price_change, mse,
                    run_id
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s
                )
                RETURNING id
                """,
                (
                    symbol, parent_segment_id, int(local_seg_index),
                    int(st.id), int(en.id),
                    start_ts, end_ts,
                    "seconds",
                    float(fit.slope), float(fit.intercept),
                    int(duration_ticks), float(duration_seconds),
                    float(price_change), float(mse),
                    run_id,
                ),
            )
            l2_id = int(cur.fetchone()["id"])

            denom = t_end - t_start
            rows = []
            for k in range(i0, i1 + 1):
                tk = ticks[k]
                if denom <= 0:
                    seg_pos = 0.0 if k == i0 else 1.0
                else:
                    seg_pos = (float(t[k]) - t_start) / denom
                    seg_pos = 0.0 if seg_pos < 0.0 else 1.0 if seg_pos > 1.0 else seg_pos

                rows.append(
                    (
                        symbol,
                        int(tk.id),
                        int(l2_id),
                        int(parent_segment_id),
                        float(seg_pos),
                        float(fit.slope),
                        float(price_change),
                        float(duration_seconds),
                        run_id,
                    )
                )

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO segticks_l2 (
                    symbol, tick_id,
                    l2_segment_id, parent_segment_id,
                    seg_pos,
                    seg_slope, seg_price_change, seg_duration_seconds,
                    run_id
                )
                VALUES %s
                """,
                rows,
                page_size=50_000,
            )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parent-segment-id", type=int, required=True)
    ap.add_argument("--c2", type=float, default=0.5)
    ap.add_argument("--run-id", default=None)

    ap.add_argument("--target-min-segs", type=int, default=5)
    ap.add_argument("--target-max-segs", type=int, default=40)
    ap.add_argument("--max-iters", type=int, default=12)

    args = ap.parse_args()

    refine_segment(
        parent_segment_id=args.parent_segment_id,
        c2_init=args.c2,
        run_id=args.run_id,
        target_min_segs=args.target_min_segs,
        target_max_segs=args.target_max_segs,
        max_iters=args.max_iters,
    )


if __name__ == "__main__":
    main()
