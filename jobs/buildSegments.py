# jobs/buildSegments.py
#
# DB-based piecewise-linear segmentation of ticks.kal with hard breaks on time gaps.
# Chunking: one "chunk" = one session (until first dt > GAP_THRESHOLD_SECONDS).
# Resume: starts from last inserted segms end (per symbol).
# No CLI args: run as a single job until current last tick for each symbol.
#
# Writes:
#   segms    : one row per segment
#   segticks : one row per tick mapped to a segment
#
# Run:
#   python -m jobs.buildSegments
#
# Requires:
#   ticks(id, symbol, timestamp, kal)
#
# Notes:
# - Uses ticks.timestamp (timestamptz) not "ts". :contentReference[oaicite:1]{index=1}
# - Commits once per session to avoid choking the server.

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur  # backend-standard DB access :contentReference[oaicite:2]{index=2}

# --------------------------- CONFIG (no inputs) ---------------------------

GAP_THRESHOLD_SECONDS = 3600.0          # hard session break
C_LAMBDA = 5.0                          # lambda = C * sigma^2 per session
FETCH_BATCH_ROWS = 50_000               # DB fetch batch size while building a session
MIN_SEG_LEN = 2                         # min ticks per segment (avoid 1-tick lines)

# Optional tagging
RUN_ID_PREFIX = "auto-segms-v1"

# ------------------------------- Data ------------------------------------

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


# ----------------------------- basic stats --------------------------------

def _stddev_pop(xs: List[float]) -> float:
    n = len(xs)
    if n <= 1:
        return 0.0
    mu = sum(xs) / n
    v = sum((x - mu) ** 2 for x in xs) / n
    return math.sqrt(v)


# --------------------- O(1) linear regression SSE -------------------------

class PrefixSums:
    """Prefix sums for O(1) segment regression cost."""
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
        if n <= 0:
            return SegmentFit(i0, i1, 0.0, 0.0, 0.0)

        den = n * S_tt - S_t * S_t
        if abs(den) < 1e-18:
            a = 0.0
            b = S_y / n
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


# ------------------------------- PELT -------------------------------------

def pelt(prefix: PrefixSums, penalty: float, min_len: int) -> List[int]:
    """
    PELT for: sum(SSE(seg)) + penalty * (#segments)
    Returns list of end indices (inclusive) for each segment in order.
    """
    n = len(prefix.t)
    if n == 0:
        return []

    F = [0.0] * (n + 1)
    prev = [-1] * (n + 1)
    R = [0]

    F[0] = -penalty  # so the first segment adds +penalty exactly once

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

        # simple pruning
        new_R = []
        for s in R:
            if F[s] + seg_cost(s, t_excl) <= F[t_excl] + penalty:
                new_R.append(s)
        new_R.append(t_excl)
        R = new_R

    # reconstruct
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


# ----------------------------- DB helpers ---------------------------------

def list_symbols(conn) -> List[str]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM ticks
            WHERE symbol IS NOT NULL
            ORDER BY symbol
            """
        )
        return [r["symbol"] for r in cur.fetchall()]


def get_symbol_last_tick_key(conn, symbol: str) -> Optional[Tuple[datetime, int]]:
    """Current tail of ticks for symbol as (timestamp, id)."""
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT timestamp, id
            FROM ticks
            WHERE symbol = %s
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (r["timestamp"], int(r["id"]))


def get_symbol_first_tick_key(conn, symbol: str) -> Optional[Tuple[datetime, int]]:
    """Earliest tick for symbol as (timestamp, id)."""
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT timestamp, id
            FROM ticks
            WHERE symbol = %s
            ORDER BY timestamp ASC, id ASC
            LIMIT 1
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (r["timestamp"], int(r["id"]))


def get_resume_key_from_segms(conn, symbol: str) -> Optional[Tuple[datetime, int]]:
    """
    Resume point from segms: latest end_ts / end_tick_id for symbol.
    Returns (end_ts, end_tick_id) or None if no segms yet.
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT end_ts, end_tick_id
            FROM segms
            WHERE symbol = %s
            ORDER BY end_ts DESC, end_tick_id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (r["end_ts"], int(r["end_tick_id"]))


def fetch_ticks_from_key(
    conn,
    symbol: str,
    start_key: Tuple[datetime, int],
    limit: int,
) -> List[TickRow]:
    """
    Fetch ticks for symbol starting from (timestamp,id) >= start_key.
    Ordered by (timestamp,id).
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, timestamp, kal
            FROM ticks
            WHERE symbol = %s
              AND (timestamp, id) >= (%s, %s)
            ORDER BY timestamp ASC, id ASC
            LIMIT %s
            """,
            (symbol, start_key[0], start_key[1], limit),
        )
        rows = cur.fetchall()

    out: List[TickRow] = []
    for r in rows:
        # kal can be NULL in schema snapshot -> skip NULLs (segmentation needs y)
        if r["kal"] is None:
            continue
        out.append(TickRow(id=int(r["id"]), ts=r["timestamp"], kal=float(r["kal"])))
    return out


def next_tick_key_after(
    conn,
    symbol: str,
    key: Tuple[datetime, int],
) -> Optional[Tuple[datetime, int]]:
    """Return the next tick key strictly greater than key."""
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT timestamp, id
            FROM ticks
            WHERE symbol = %s
              AND (timestamp, id) > (%s, %s)
            ORDER BY timestamp ASC, id ASC
            LIMIT 1
            """,
            (symbol, key[0], key[1]),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (r["timestamp"], int(r["id"]))


# ------------------------ session chunk builder ---------------------------

def build_next_session_chunk(
    conn,
    symbol: str,
    start_key: Tuple[datetime, int],
) -> Tuple[List[TickRow], Optional[Tuple[datetime, int]]]:
    """
    Build exactly one session chunk starting from start_key:
      - include ticks forward until we encounter a time gap > GAP_THRESHOLD_SECONDS
      - return (session_ticks, next_start_key_for_following_session)
    next_start_key is the first tick AFTER the gap (the tick that begins next session).
    """
    session: List[TickRow] = []
    carry_key: Optional[Tuple[datetime, int]] = None

    key = start_key
    prev_ts: Optional[datetime] = None

    while True:
        batch = fetch_ticks_from_key(conn, symbol, key, FETCH_BATCH_ROWS)
        if not batch:
            break

        for t in batch:
            if not session:
                session.append(t)
                prev_ts = t.ts
                continue

            assert prev_ts is not None
            dt = (t.ts - prev_ts).total_seconds()
            if dt > GAP_THRESHOLD_SECONDS:
                # boundary: t is first tick of next session
                carry_key = (t.ts, t.id)
                return session, carry_key

            session.append(t)
            prev_ts = t.ts

        # advance key to "next tick after last tick in batch" to avoid refetching
        last = batch[-1]
        nk = next_tick_key_after(conn, symbol, (last.ts, last.id))
        if nk is None:
            break
        key = nk

    return session, carry_key


# --------------------------- inserts per session ---------------------------

def insert_one_session(
    conn,
    symbol: str,
    session_id: int,
    global_seg_index_start: int,
    ticks: List[TickRow],
    run_id: str,
) -> int:
    """
    Segment one session and insert:
      - segms rows (RETURNING id)
      - segticks rows (execute_values)
    Returns number of segments inserted (for incrementing global seg index).
    """
    n = len(ticks)
    if n < MIN_SEG_LEN:
        return 0

    t0 = ticks[0].ts
    t = [(x.ts - t0).total_seconds() for x in ticks]
    y = [x.kal for x in ticks]

    sigma = _stddev_pop(y)
    lam = C_LAMBDA * (sigma ** 2)

    pref = PrefixSums(t, y)
    ends = pelt(pref, penalty=lam, min_len=MIN_SEG_LEN)
    if not ends:
        ends = [n - 1]

    # build (i0,i1) bounds
    seg_bounds: List[Tuple[int, int]] = []
    s0 = 0
    for e in ends:
        seg_bounds.append((s0, e))
        s0 = e + 1
    if seg_bounds and seg_bounds[-1][1] != n - 1:
        seg_bounds[-1] = (seg_bounds[-1][0], n - 1)

    segs_inserted = 0
    global_seg_index = global_seg_index_start

    with dict_cur(conn) as cur:
        for (i0, i1) in seg_bounds:
            fit = pref.fit_cost(i0, i1)
            start_tick = ticks[i0]
            end_tick = ticks[i1]

            start_ts = start_tick.ts
            end_ts = end_tick.ts

            dur_ticks = i1 - i0 + 1
            dur_seconds = (end_ts - start_ts).total_seconds()

            t_start = t[i0]
            t_end = t[i1]
            price_change = fit.slope * (t_end - t_start)
            mse = fit.sse / dur_ticks if dur_ticks > 0 else 0.0

            # insert segm
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

            # insert tick mappings
            denom = (t_end - t_start)
            rows = []
            for k in range(i0, i1 + 1):
                tick = ticks[k]
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
                        tick.id,
                        segm_id,
                        session_id,
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
                INSERT INTO segticks (
                    symbol, tick_id, segm_id, session_id,
                    seg_pos, seg_slope, seg_price_change, seg_duration_seconds,
                    run_id
                )
                VALUES %s
                """,
                rows,
                page_size=50_000,
            )

            global_seg_index += 1
            segs_inserted += 1

    return segs_inserted


# --------------------------------- main ----------------------------------

def main() -> None:
    conn = get_conn()
    conn.autocommit = False

    symbols = list_symbols(conn)
    if not symbols:
        print("[buildSegments] no symbols found in ticks")
        return

    print(f"[buildSegments] symbols={symbols}")
    print(f"[buildSegments] GAP_THRESHOLD_SECONDS={GAP_THRESHOLD_SECONDS} C_LAMBDA={C_LAMBDA} FETCH_BATCH_ROWS={FETCH_BATCH_ROWS}")

    for symbol in symbols:
        try:
            last_tick_key = get_symbol_last_tick_key(conn, symbol)
            if last_tick_key is None:
                print(f"[buildSegments] symbol={symbol} no ticks -> skip")
                continue

            resume_key = get_resume_key_from_segms(conn, symbol)

            if resume_key is None:
                start_key = get_symbol_first_tick_key(conn, symbol)
                if start_key is None:
                    print(f"[buildSegments] symbol={symbol} no ticks -> skip")
                    continue
                global_seg_index = 0
                print(f"[buildSegments] symbol={symbol} start from FIRST tick key={start_key}")
            else:
                # start from next tick strictly after the last inserted segment end
                nk = next_tick_key_after(conn, symbol, resume_key)
                if nk is None:
                    print(f"[buildSegments] symbol={symbol} already up-to-date (no next tick after {resume_key})")
                    continue
                start_key = nk

                # continue global seg index from DB
                with dict_cur(conn) as cur:
                    cur.execute(
                        "SELECT COALESCE(MAX(global_seg_index), -1) AS m FROM segms WHERE symbol=%s",
                        (symbol,),
                    )
                    global_seg_index = int(cur.fetchone()["m"]) + 1

                print(f"[buildSegments] symbol={symbol} resume after {resume_key} -> start_key={start_key} global_seg_index={global_seg_index}")

            session_id = 0
            processed_sessions = 0
            processed_segments = 0

            # Iterate sessions until we reach current last tick key
            while start_key is not None and start_key <= last_tick_key:
                session_ticks, next_start_key = build_next_session_chunk(conn, symbol, start_key)
                if not session_ticks:
                    break

                run_id = f"{RUN_ID_PREFIX}:{symbol}:{session_ticks[0].id}:{session_ticks[-1].id}"

                # Insert per session in its own transaction scope
                segs_in_session = 0
                try:
                    segs_in_session = insert_one_session(
                        conn=conn,
                        symbol=symbol,
                        session_id=session_id,
                        global_seg_index_start=global_seg_index,
                        ticks=session_ticks,
                        run_id=run_id,
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise

                processed_sessions += 1
                processed_segments += segs_in_session
                global_seg_index += segs_in_session

                print(
                    f"[buildSegments] symbol={symbol} session_id={session_id} "
                    f"ticks={len(session_ticks)} segs={segs_in_session} "
                    f"range=({session_ticks[0].ts.isoformat()} .. {session_ticks[-1].ts.isoformat()}) "
                    f"ids=({session_ticks[0].id} .. {session_ticks[-1].id})"
                )

                session_id += 1
                start_key = next_start_key

                # refresh tail occasionally in case new ticks are arriving
                if processed_sessions % 10 == 0:
                    last_tick_key = get_symbol_last_tick_key(conn, symbol) or last_tick_key

            print(
                f"[buildSegments] symbol={symbol} DONE sessions={processed_sessions} segments={processed_segments}"
            )

        except Exception as e:
            # fail one symbol but continue others (optional; change if you prefer hard fail)
            print(f"[buildSegments] symbol={symbol} ERROR: {type(e).__name__}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    print("[buildSegments] all symbols complete")


if __name__ == "__main__":
    main()
