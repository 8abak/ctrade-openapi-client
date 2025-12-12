# jobs/buildSegments.py
# DB-based piecewise-linear segmentation of ticks.kal with hard breaks on time gaps
#
# Writes:
#   segms    : one row per segment
#   segticks : one row per tick mapped to a segment
#
# Run:
#   python -m jobs.buildSegments --symbol XAUUSD --ts-start 2025-12-01T00:00:00Z --ts-end 2025-12-02T00:00:00Z
#
# Requires:
#   ticks(id, symbol, timestamp, kal)

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur  # same helpers used by backend :contentReference[oaicite:2]{index=2}


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


# ----------------------------- utilities -----------------------------

def _parse_dt(s: str) -> datetime:
    # Accept ISO strings; force timezone-aware (assume UTC if naive)
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


def _sessionize(ticks: List[TickRow], gap_seconds: float) -> List[Tuple[int, int]]:
    """
    Return list of (start_index, end_index) inclusive sessions over ticks list.
    Start a new session when dt > gap_seconds.
    """
    if not ticks:
        return []
    out: List[Tuple[int, int]] = []
    s0 = 0
    for i in range(1, len(ticks)):
        dt = (ticks[i].ts - ticks[i - 1].ts).total_seconds()
        if dt > gap_seconds:
            out.append((s0, i - 1))
            s0 = i
    out.append((s0, len(ticks) - 1))
    return out


# --------------------- O(1) linear regression SSE ---------------------

class PrefixSums:
    """
    Prefix sums over arrays t,y for O(1) segment regression:
      S_t, S_y, S_tt, S_ty, S_yy
    Prefixes are over [0..k-1].
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
            ti = t[i]
            yi = y[i]
            self.S_t[i + 1] = self.S_t[i] + ti
            self.S_y[i + 1] = self.S_y[i] + yi
            self.S_tt[i + 1] = self.S_tt[i] + ti * ti
            self.S_ty[i + 1] = self.S_ty[i] + ti * yi
            self.S_yy[i + 1] = self.S_yy[i] + yi * yi

    def _seg_sums(self, i0: int, i1: int) -> Tuple[int, float, float, float, float, float]:
        # inclusive [i0..i1]
        n = i1 - i0 + 1
        S_t = self.S_t[i1 + 1] - self.S_t[i0]
        S_y = self.S_y[i1 + 1] - self.S_y[i0]
        S_tt = self.S_tt[i1 + 1] - self.S_tt[i0]
        S_ty = self.S_ty[i1 + 1] - self.S_ty[i0]
        S_yy = self.S_yy[i1 + 1] - self.S_yy[i0]
        return n, S_t, S_y, S_tt, S_ty, S_yy

    def fit_cost(self, i0: int, i1: int) -> SegmentFit:
        n, S_t, S_y, S_tt, S_ty, S_yy = self._seg_sums(i0, i1)
        if n <= 0:
            return SegmentFit(i0, i1, 0.0, 0.0, 0.0)

        den = n * S_tt - S_t * S_t
        if abs(den) < 1e-18:
            # degenerate t: constant model
            b = S_y / n
            a = 0.0
        else:
            a = (n * S_ty - S_t * S_y) / den
            b = (S_y - a * S_t) / n

        # SSE formula
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


# ------------------------------- PELT -------------------------------

def pelt(prefix: PrefixSums, penalty: float, min_len: int = 2) -> List[int]:
    """
    PELT change-point detection for additive costs:
      sum(cost(seg)) + penalty * (#segments)
    Returns breakpoints as end indices inclusive for each segment (monotone list).
    """
    n = len(prefix.t)
    if n == 0:
        return []

    # DP arrays: best cost up to t (exclusive), with t in [0..n]
    F = [0.0] * (n + 1)
    prev = [-1] * (n + 1)

    # Candidate set
    R = [0]
    F[0] = -penalty  # so first segment adds +penalty

    # A small helper to compute segment cost from s..t-1
    def seg_cost(s: int, t: int) -> float:
        if t - s < min_len:
            return float("inf")
        return prefix.fit_cost(s, t - 1).sse

    for t in range(1, n + 1):
        best_val = float("inf")
        best_s = -1
        for s in R:
            v = F[s] + seg_cost(s, t) + penalty
            if v < best_val:
                best_val = v
                best_s = s
        F[t] = best_val
        prev[t] = best_s

        # pruning (basic form)
        new_R = []
        for s in R:
            if F[s] + seg_cost(s, t) <= F[t] + penalty:
                new_R.append(s)
        new_R.append(t)
        R = new_R

    # reconstruct segments from prev pointers
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


# ------------------------------- DB IO -------------------------------

def fetch_ticks(symbol: str, ts_start: datetime, ts_end: datetime) -> List[TickRow]:
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, timestamp, kal
            FROM ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp ASC
            """,
            (symbol, ts_start, ts_end),
        )
        rows = cur.fetchall()

    out: List[TickRow] = []
    for r in rows:
        out.append(TickRow(id=int(r["id"]), ts=r["timestamp"], kal=float(r["kal"])))
    return out


def insert_segments_and_ticks(
    symbol: str,
    run_id: Optional[str],
    ticks: List[TickRow],
    session_slices: List[Tuple[int, int]],
    gap_seconds: float,
    c: float,
) -> None:
    """
    Compute and insert segments into segms and per-tick mapping into segticks.
    """
    conn = get_conn()
    conn.autocommit = False

    global_seg_index = 0

    try:
        with conn, dict_cur(conn) as cur:
            for session_id, (a, b) in enumerate(session_slices):
                # session ticks a..b inclusive
                sess = ticks[a : b + 1]
                n = len(sess)
                if n < 2:
                    continue

                t0 = sess[0].ts
                t = [(x.ts - t0).total_seconds() for x in sess]
                y = [x.kal for x in sess]

                sigma = _stddev_pop(y)
                lam = c * (sigma ** 2)

                pref = PrefixSums(t, y)
                ends = pelt(pref, penalty=lam, min_len=2)
                if not ends:
                    ends = [n - 1]

                # segment boundaries in local session coords
                seg_bounds: List[Tuple[int, int]] = []
                s0 = 0
                for e in ends:
                    seg_bounds.append((s0, e))
                    s0 = e + 1
                if seg_bounds and seg_bounds[-1][1] != n - 1:
                    seg_bounds[-1] = (seg_bounds[-1][0], n - 1)

                for (i0, i1) in seg_bounds:
                    fit = pref.fit_cost(i0, i1)

                    start_tick = sess[i0]
                    end_tick = sess[i1]

                    start_ts = start_tick.ts
                    end_ts = end_tick.ts
                    dur_ticks = i1 - i0 + 1
                    dur_seconds = (end_ts - start_ts).total_seconds()

                    # fitted delta: a*(t_end - t_start)
                    t_start = t[i0]
                    t_end = t[i1]
                    price_change = fit.slope * (t_end - t_start)

                    mse = fit.sse / dur_ticks if dur_ticks > 0 else 0.0

                    # Insert segms row and get id
                    cur.execute(
                        """
                        INSERT INTO segms (
                            symbol, session_id, global_seg_index,
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
                            symbol, session_id, global_seg_index,
                            start_tick.id, end_tick.id,
                            start_ts, end_ts,
                            "seconds",
                            float(fit.slope), float(fit.intercept),
                            int(dur_ticks), float(dur_seconds),
                            float(price_change), float(mse),
                            run_id,
                        ),
                    )
                    segm_id = int(cur.fetchone()["id"])
                    global_seg_index += 1

                    # Batch insert segticks for this segment
                    denom = (t_end - t_start)
                    tick_rows = []
                    for k in range(i0, i1 + 1):
                        tick = sess[k]
                        if denom <= 0:
                            seg_pos = 0.0 if k == i0 else 1.0
                        else:
                            seg_pos = (t[k] - t_start) / denom
                            if seg_pos < 0.0:
                                seg_pos = 0.0
                            elif seg_pos > 1.0:
                                seg_pos = 1.0

                        tick_rows.append((
                            symbol,
                            tick.id,
                            segm_id,
                            session_id,
                            float(seg_pos),
                            float(fit.slope),
                            float(price_change),
                            float(dur_seconds),
                            run_id,
                        ))

                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO segticks (
                            symbol, tick_id, segm_id, session_id,
                            seg_pos, seg_slope, seg_price_change, seg_duration_seconds,
                            run_id
                        )
                        VALUES %s
                        """,
                        tick_rows,
                        page_size=50_000,
                    )

        conn.commit()

    except Exception:
        conn.rollback()
        raise


# -------------------------------- main --------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--ts-start", required=True, help="ISO datetime, e.g. 2025-12-01T00:00:00Z")
    ap.add_argument("--ts-end", required=True, help="ISO datetime, e.g. 2025-12-02T00:00:00Z")
    ap.add_argument("--gap-seconds", type=float, default=3600.0)
    ap.add_argument("--c", type=float, default=5.0)
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    symbol = args.symbol
    ts_start = _parse_dt(args.ts_start)
    ts_end = _parse_dt(args.ts_end)

    ticks = fetch_ticks(symbol, ts_start, ts_end)
    print(f"[buildSegments] symbol={symbol} ticks={len(ticks)} ts_start={ts_start.isoformat()} ts_end={ts_end.isoformat()}")

    sessions = _sessionize(ticks, gap_seconds=args.gap_seconds)
    print(f"[buildSegments] sessions={len(sessions)} gap_seconds={args.gap_seconds} c={args.c} run_id={args.run_id}")

    if not ticks or not sessions:
        print("[buildSegments] nothing to do")
        return

    insert_segments_and_ticks(
        symbol=symbol,
        run_id=args.run_id,
        ticks=ticks,
        session_slices=sessions,
        gap_seconds=args.gap_seconds,
        c=args.c,
    )

    print("[buildSegments] done")


if __name__ == "__main__":
    main()
