# jobs/refineSegmentL2.py
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur


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


def _parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _stddev_pop(xs: List[float]) -> float:
    n = len(xs)
    if n <= 1:
        return 0.0
    mu = sum(xs) / n
    v = sum((x - mu) ** 2 for x in xs) / n
    return math.sqrt(v)


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
            ti = t[i]
            yi = y[i]
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
        return SegmentFit(i0, i1, a, b, sse)


def pelt(prefix: PrefixSums, penalty: float, min_len: int = 2) -> List[int]:
    """
    Exact penalized DP with PELT-style pruning.
    Returns list of inclusive end indices per segment.
    """
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

        new_R = []
        for s in R:
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


def refine_segment(parent_segment_id: int, c2: float, run_id: Optional[str]) -> None:
    conn = get_conn()
    conn.autocommit = False

    with conn, dict_cur(conn) as cur:
        # --- parent seg ---
        cur.execute(
            """
            SELECT id, symbol, start_tick_id, end_tick_id, start_ts, end_ts, t_axis_type
            FROM segms
            WHERE id = %s
            """,
            (parent_segment_id,),
        )
        p = cur.fetchone()
        if not p:
            raise RuntimeError(f"parent_segment_id={parent_segment_id} not found in segms")

        symbol = p["symbol"]
        start_tick_id = int(p["start_tick_id"])
        end_tick_id = int(p["end_tick_id"])
        parent_start_ts = p["start_ts"]

        # --- ticks inside parent ---
        # NOTE: your live DB uses ticks.timestamp; if your schema has ticks.ts, rename here.
        cur.execute(
            """
            SELECT id, timestamp AS ts, kal
            FROM ticks
            WHERE symbol = %s
              AND id BETWEEN %s AND %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, start_tick_id, end_tick_id),
        )
        rows = cur.fetchall()
        ticks: List[TickRow] = []
        for r in rows:
            if r["kal"] is None:
                continue
            ticks.append(TickRow(id=int(r["id"]), ts=r["ts"], kal=float(r["kal"])))

        if len(ticks) < 3:
            print(f"[refineL2] parent={parent_segment_id} too few ticks={len(ticks)} -> skip")
            return

        # Optional: remove previous results for same (parent, run_id)
        if run_id:
            cur.execute(
                "DELETE FROM segticks_l2 WHERE parent_segment_id=%s AND run_id=%s",
                (parent_segment_id, run_id),
            )
            cur.execute(
                "DELETE FROM segms_l2 WHERE parent_segment_id=%s AND run_id=%s",
                (parent_segment_id, run_id),
            )

        # --- build time axis relative to parent start ---
        t0 = parent_start_ts
        t = [(x.ts - t0).total_seconds() for x in ticks]
        y = [x.kal for x in ticks]

        sigma = _stddev_pop(y)
        lambda2 = c2 * (sigma ** 2)

        pref = PrefixSums(t, y)
        ends = pelt(pref, penalty=lambda2, min_len=2)
        if not ends:
            ends = [len(ticks) - 1]

        # bounds
        bounds: List[Tuple[int, int]] = []
        s0 = 0
        for e in ends:
            bounds.append((s0, e))
            s0 = e + 1
        if bounds and bounds[-1][1] != len(ticks) - 1:
            bounds[-1] = (bounds[-1][0], len(ticks) - 1)

        # --- insert children + mappings ---
        local_seg_index = 0
        total_children = 0

        for (i0, i1) in bounds:
            fit = pref.fit_cost(i0, i1)

            st = ticks[i0]
            en = ticks[i1]
            start_ts = st.ts
            end_ts = en.ts

            dur_ticks = i1 - i0 + 1
            dur_seconds = (end_ts - start_ts).total_seconds()

            t_start = t[i0]
            t_end = t[i1]
            price_change = fit.slope * (t_end - t_start)
            mse = fit.sse / dur_ticks if dur_ticks else 0.0

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
                    symbol, parent_segment_id, local_seg_index,
                    st.id, en.id,
                    start_ts, end_ts,
                    "seconds",
                    float(fit.slope), float(fit.intercept),
                    int(dur_ticks), float(dur_seconds),
                    float(price_change), float(mse),
                    run_id,
                ),
            )
            l2_id = int(cur.fetchone()["id"])

            denom = (t_end - t_start)
            rows = []
            for k in range(i0, i1 + 1):
                tk = ticks[k]
                if denom <= 0:
                    seg_pos = 0.0 if k == i0 else 1.0
                else:
                    seg_pos = (t[k] - t_start) / denom
                    if seg_pos < 0.0:
                        seg_pos = 0.0
                    elif seg_pos > 1.0:
                        seg_pos = 1.0

                rows.append(
                    (
                        symbol,
                        tk.id,
                        l2_id,
                        parent_segment_id,
                        float(seg_pos),
                        float(fit.slope),
                        float(price_change),
                        float(dur_seconds),
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

            local_seg_index += 1
            total_children += 1

        print(
            f"[refineL2] parent={parent_segment_id} symbol={symbol} "
            f"ticks={len(ticks)} sigma={sigma:.6g} c2={c2} lambda2={lambda2:.6g} "
            f"children={total_children} run_id={run_id}"
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
