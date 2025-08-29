# PATH: backend/runner.py
# Segment-by-segment pipeline (no future peeking).
# - Segments: split by gaps > 180s.
# - Inside each segment:
#     * Build SMALL pivots (±$2) -> create levels (high/low) and mark used when touched.
#     * Build BIG movements (legs with move >= BIG_TH, default $6) from consecutive SMALL legs.
#     * For each BIG leg, detect SMALL moves within it (>= $2 in same direction).
#     * Emit predictions on earliest resumption after a counter SMALL move:
#         - cross rolling SMA(N) in leg direction AND break minor counter-pivot by ε.
#         - resolve: +$2 goal before -$1 fail (first-touch).
# - Commit per segment: segm, level, bigm, smal, pred, outcome, advance stat.last_done_tick_id.
#
# This runner ignores segm.dir for modeling (kept only for backward compat).
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Tuple, Dict

import psycopg2
import psycopg2.extras

from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr


@dataclass
class Tick:
    id: int
    ts: object  # datetime
    mid: float


@dataclass
class Pivot:
    kind: str  # 'high' or 'low'
    idx: int
    id: int
    ts: object
    price: float


@dataclass
class Leg:
    dir: str  # 'up' | 'dn'
    a_idx: int
    b_idx: int
    a_id: int
    b_id: int
    a_ts: object
    b_ts: object
    move: float
    ticks: int


class Runner:
    def __init__(self):
        self.conn = get_conn()
        self.ts_col = detect_ts_col(self.conn)        # e.g., "timestamp" or "ts"
        self.mid_expr = detect_mid_expr(self.conn)    # "mid" or "price" or "(bid+ask)/2.0"

        # Tunables
        self.SEGMENT_GAP_S = 180                      # >3 minutes
        self.SMALL_TH = 2.0                           # small pivot threshold ($)
        self.BIG_TH = 6.0                             # big-leg threshold ($)
        self.SMA_N = 50                               # rolling window
        self.SMA_FALLBACK = 30
        self.GOAL = 2.0                               # +$2 target
        self.STOP = 1.0                               # -$1 stop
        self.EPS = 0.05                               # minor break epsilon

    # ----------------- Public entry -----------------
    def run_until_now(self, max_segments: int = 20, sleep_between: float = 0.15) -> dict:
        segs = 0
        start_ptr = self._get_ptr()
        last_id = self._scalar("SELECT COALESCE(MAX(id),0) FROM ticks") or 0
        start_seen = None

        while segs < max_segments:
            nxt = self._next_segment_bounds(self._get_ptr() + 1, last_id)
            if not nxt:
                break
            seg_start, seg_end = nxt
            if start_seen is None:
                start_seen = seg_start
            self._process_segment(seg_start, seg_end)
            self._set_ptr(seg_end)
            segs += 1
            time.sleep(sleep_between)

            # refresh table tail occasionally (new ticks might have arrived)
            last_id = self._scalar("SELECT COALESCE(MAX(id),0) FROM ticks") or last_id

        return {"segments": segs, "from_tick": start_seen or start_ptr + 1, "to_tick": self._get_ptr()}

    # ----------------- Segmenting -----------------
    def _next_segment_bounds(self, start_id: int, table_max_id: int) -> Optional[Tuple[int, int]]:
        """Return (start_id, end_id) for the next segment or None."""
        with dict_cur(self.conn) as cur:
            # Get first id >= start_id
            cur.execute("SELECT MIN(id) AS sid FROM ticks WHERE id >= %s", (start_id,))
            r = cur.fetchone()
            if not r or not r["sid"]:
                return None
            sid = int(r["sid"])

            # Find first gap > 180s after sid (limit to a large window for perf)
            cur.execute(f"""
                WITH t AS (
                  SELECT id, {self.ts_col} AS ts,
                         LAG({self.ts_col}) OVER (ORDER BY id) AS prev_ts
                  FROM ticks
                  WHERE id >= %s
                  ORDER BY id
                  LIMIT 1000000
                )
                SELECT id FROM t
                WHERE prev_ts IS NOT NULL
                  AND EXTRACT(EPOCH FROM (ts - prev_ts)) > %s
                ORDER BY id
                LIMIT 1
            """, (sid, self.SEGMENT_GAP_S))
            gap_row = cur.fetchone()
            if gap_row and gap_row["id"]:
                end_id = int(gap_row["id"]) - 1
            else:
                # no gap in the window: end at either max(id) in window or table_max_id
                cur.execute("""
                    SELECT MAX(id) AS mid FROM ticks WHERE id >= %s AND id <= %s
                """, (sid, table_max_id))
                mid = int(cur.fetchone()["mid"] or table_max_id)
                end_id = mid

        if end_id < sid:
            return None
        return (sid, end_id)

    # ----------------- Core processing -----------------
    def _process_segment(self, start_id: int, end_id: int):
        ticks = self._load_ticks(start_id, end_id)
        if not ticks:
            return

        # Build SMALL pivots & LEVELS; also mark used levels as we go
        pivots, levels = self._build_small_pivots_and_levels(ticks)

        # Build BIG legs from consecutive SMALL legs that exceed BIG_TH
        legs = self._build_big_legs_from_pivots(ticks, pivots)

        # For each BIG leg, derive SMALL moves inside it (≥ SMALL_TH in leg direction)
        smal_rows = self._small_moves_inside_big_legs(ticks, legs)

        # Rolling SMA for the whole segment
        sma = self._rolling_sma([t.mid for t in ticks], max(self.SMA_FALLBACK, min(self.SMA_N, len(ticks))))

        # Emit predictions on earliest resumption after a counter SMALL pivot inside each BIG leg
        preds = self._emit_predictions(ticks, pivots, legs, sma)

        # Outcome per segment
        right = sum(1 for p in preds if p["hit"] is True)
        wrong = sum(1 for p in preds if p["hit"] is False)
        total = right + wrong
        ratio = None
        if total == 0:
            ratio = 0
        elif right == total:
            ratio = 1
        elif wrong == total:
            ratio = -1
        else:
            ratio = round((right - wrong) / total, 2)

        # Persist (one transaction per segment)
        self._commit_segment(start_id, end_id, ticks, pivots, levels, legs, smal_rows, preds, ratio)

    # ----------------- Helpers: building blocks -----------------
    def _load_ticks(self, a: int, b: int) -> List[Tick]:
        rows: List[Tick] = []
        with dict_cur(self.conn) as cur:
            cur.execute(f"""
                SELECT id, {self.ts_col} AS ts, {self.mid_expr} AS mid
                FROM ticks
                WHERE id BETWEEN %s AND %s
                ORDER BY id ASC
            """, (a, b))
            for r in cur.fetchall():
                mid = float(r["mid"]) if isinstance(r["mid"], Decimal) else r["mid"]
                rows.append(Tick(int(r["id"]), r["ts"], float(mid)))
        return rows

    def _build_small_pivots_and_levels(self, ticks: List[Tick]) -> Tuple[List[Pivot], List[dict]]:
        pivots: List[Pivot] = []
        levels: List[dict] = []

        if not ticks:
            return pivots, levels

        # Track open levels for "used" marking
        open_levels: List[dict] = []

        last_pivot_idx = 0
        dirn: Optional[str] = None  # None | 'up' | 'dn'
        extreme_idx = 0

        for i in range(1, len(ticks)):
            price = ticks[i].mid
            # mark 'used' levels
            for L in open_levels:
                if L.get("used_at_id"):
                    continue
                if L["kind"] == "high" and price >= L["price"]:
                    L["used_at_id"] = ticks[i].id
                    L["used_at_ts"] = ticks[i].ts
                elif L["kind"] == "low" and price <= L["price"]:
                    L["used_at_id"] = ticks[i].id
                    L["used_at_ts"] = ticks[i].ts

            if dirn is None:
                # Establish direction once we move $2 from the start pivot
                up_move = price - ticks[last_pivot_idx].mid
                dn_move = ticks[last_pivot_idx].mid - price
                if up_move >= self.SMALL_TH:
                    dirn = "up"; extreme_idx = i
                elif dn_move >= self.SMALL_TH:
                    dirn = "dn"; extreme_idx = i
                else:
                    # expand extreme candidate
                    if price > ticks[extreme_idx].mid:
                        extreme_idx = i
                    elif price < ticks[extreme_idx].mid:
                        extreme_idx = i
                continue

            if dirn == "up":
                # extend extreme if making a new high
                if price > ticks[extreme_idx].mid:
                    extreme_idx = i
                # reversal if we retrace >= SMALL_TH from the extreme
                if ticks[extreme_idx].mid - price >= self.SMALL_TH:
                    # Confirm a HIGH pivot at extreme_idx
                    piv = Pivot("high", extreme_idx, ticks[extreme_idx].id, ticks[extreme_idx].ts, ticks[extreme_idx].mid)
                    pivots.append(piv)
                    L = {"segm_kind": "pivot", "kind": "high", "tick_id": piv.id, "ts": piv.ts, "price": piv.price}
                    open_levels.append(L)
                    levels.append(L)
                    # switch
                    last_pivot_idx = extreme_idx
                    dirn = "dn"
                    extreme_idx = i
            else:
                # dirn == 'dn'
                if price < ticks[extreme_idx].mid:
                    extreme_idx = i
                if price - ticks[extreme_idx].mid >= self.SMALL_TH:
                    # Confirm a LOW pivot at extreme_idx
                    piv = Pivot("low", extreme_idx, ticks[extreme_idx].id, ticks[extreme_idx].ts, ticks[extreme_idx].mid)
                    pivots.append(piv)
                    L = {"segm_kind": "pivot", "kind": "low", "tick_id": piv.id, "ts": piv.ts, "price": piv.price}
                    open_levels.append(L)
                    levels.append(L)
                    last_pivot_idx = extreme_idx
                    dirn = "up"
                    extreme_idx = i

        return pivots, levels

    def _build_big_legs_from_pivots(self, ticks: List[Tick], pivots: List[Pivot]) -> List[Leg]:
        legs: List[Leg] = []
        if not pivots:
            return legs

        # legs from pivot-to-pivot segments; confirm if move >= BIG_TH
        prev = pivots[0]
        for p in pivots[1:]:
            dirn = "up" if p.kind == "high" else "dn"  # direction of leg leading INTO this pivot
            a_idx = prev.idx
            b_idx = p.idx
            if b_idx <= a_idx:
                prev = p
                continue
            move = abs(ticks[b_idx].mid - ticks[a_idx].mid)
            if move >= self.BIG_TH:
                legs.append(Leg(
                    dir=dirn,
                    a_idx=a_idx, b_idx=b_idx,
                    a_id=ticks[a_idx].id, b_id=ticks[b_idx].id,
                    a_ts=ticks[a_idx].ts, b_ts=ticks[b_idx].ts,
                    move=move, ticks=(b_idx - a_idx + 1),
                ))
            prev = p
        return legs

    def _small_moves_inside_big_legs(self, ticks: List[Tick], legs: List[Leg]) -> List[dict]:
        smals: List[dict] = []
        for lg in legs:
            # scan within [a_idx, b_idx] in direction of lg.dir using run-up ≥ SMALL_TH resets
            swing_idx = lg.a_idx
            last_peak = ticks[swing_idx].mid
            for i in range(lg.a_idx + 1, lg.b_idx + 1):
                price = ticks[i].mid
                if lg.dir == "up":
                    if price > last_peak:
                        last_peak = price
                    if (last_peak - ticks[swing_idx].mid) >= self.SMALL_TH:
                        # record small move swing_idx -> i (approx at current peak index)
                        smals.append({
                            "dir": "up",
                            "a_idx": swing_idx, "b_idx": i,
                            "a_id": ticks[swing_idx].id, "b_id": ticks[i].id,
                            "a_ts": ticks[swing_idx].ts, "b_ts": ticks[i].ts,
                            "move": last_peak - ticks[swing_idx].mid,
                            "ticks": i - swing_idx + 1,
                            "leg_a_id": lg.a_id, "leg_b_id": lg.b_id
                        })
                        swing_idx = i
                        last_peak = ticks[i].mid
                else:
                    if price < last_peak:
                        last_peak = price
                    if (ticks[swing_idx].mid - last_peak) >= self.SMALL_TH:
                        smals.append({
                            "dir": "dn",
                            "a_idx": swing_idx, "b_idx": i,
                            "a_id": ticks[swing_idx].id, "b_id": ticks[i].id,
                            "a_ts": ticks[swing_idx].ts, "b_ts": ticks[i].ts,
                            "move": ticks[swing_idx].mid - last_peak,
                            "ticks": i - swing_idx + 1,
                            "leg_a_id": lg.a_id, "leg_b_id": lg.b_id
                        })
                        swing_idx = i
                        last_peak = ticks[i].mid
        return smals

    def _rolling_sma(self, arr: List[float], n: int) -> List[float]:
        out = [0.0] * len(arr)
        if len(arr) == 0:
            return out
        n = max(1, min(n, len(arr)))
        s = 0.0
        for i, v in enumerate(arr):
            s += v
            if i >= n:
                s -= arr[i - n]
            out[i] = s / n if i >= n - 1 else arr[i]
        return out

    def _emit_predictions(self, ticks: List[Tick], pivots: List[Pivot], legs: List[Leg], sma: List[float]) -> List[dict]:
        preds: List[dict] = []
        # Build quick pivot lookup by id to know price/idx
        piv_by_idx = {p.idx: p for p in pivots}

        for lg in legs:
            # Find first counter SMALL pivot after leg start within leg window
            # If leg.dir == 'up' -> counter pivot is a 'low' after a_idx
            # If leg.dir == 'dn' -> counter pivot is a 'high' after a_idx
            # Choose the earliest such pivot located between leg a and b.
            counter = None
            for p in pivots:
                if p.idx <= lg.a_idx or p.idx >= lg.b_idx:
                    continue
                if (lg.dir == "up" and p.kind == "low") or (lg.dir == "dn" and p.kind == "high"):
                    counter = p
                    break
            if not counter:
                continue

            # Earliest resumption after counter pivot:
            # condition: cross SMA in leg direction AND break minor counter pivot by epsilon.
            entry_idx = None
            if lg.dir == "up":
                trigger = counter.price + self.EPS
                for i in range(counter.idx + 1, lg.b_idx + 1):
                    if ticks[i].mid >= sma[i] and ticks[i].mid >= trigger:
                        entry_idx = i
                        break
            else:
                trigger = counter.price - self.EPS
                for i in range(counter.idx + 1, lg.b_idx + 1):
                    if ticks[i].mid <= sma[i] and ticks[i].mid <= trigger:
                        entry_idx = i
                        break
            if entry_idx is None:
                continue

            # Resolve hit within segment (first-touch rule)
            entry_price = ticks[entry_idx].mid
            goal = entry_price + self.GOAL if lg.dir == "up" else entry_price - self.GOAL
            stop = entry_price - self.STOP if lg.dir == "up" else entry_price + self.STOP
            hit = None
            resolved_idx = None
            if lg.dir == "up":
                for i in range(entry_idx + 1, lg.b_idx + 1):
                    if ticks[i].mid >= goal:
                        hit = True; resolved_idx = i; break
                    if ticks[i].mid <= stop:
                        hit = False; resolved_idx = i; break
            else:
                for i in range(entry_idx + 1, lg.b_idx + 1):
                    if ticks[i].mid <= goal:
                        hit = True; resolved_idx = i; break
                    if ticks[i].mid >= stop:
                        hit = False; resolved_idx = i; break

            preds.append({
                "segm_id": None,  # filled at insert time
                "at_id": ticks[entry_idx].id,
                "at_ts": ticks[entry_idx].ts,
                "dir": lg.dir,
                "goal_usd": self.GOAL,
                "hit": hit,
                "resolved_at_id": ticks[resolved_idx].id if resolved_idx is not None else None,
                "resolved_at_ts": ticks[resolved_idx].ts if resolved_idx is not None else None
            })

        return preds

    # ----------------- Persistence -----------------
    def _commit_segment(
        self,
        start_id: int,
        end_id: int,
        ticks: List[Tick],
        pivots: List[Pivot],
        levels: List[dict],
        legs: List[Leg],
        smal_rows: List[dict],
        preds: List[dict],
        ratio_val,
    ):
        # Compute segm meta (dir kept for backward compat but not used by model)
        start_mid = ticks[0].mid
        end_mid = ticks[-1].mid
        segm_dir = "up" if end_mid > start_mid else ("dn" if end_mid < start_mid else None)
        segm_span = end_mid - start_mid
        segm_len = len(ticks)

        self.conn.autocommit = False
        try:
            with dict_cur(self.conn) as cur:
                # Insert segm
                cur.execute("""
                    INSERT INTO segm (start_id, end_id, start_ts, end_ts, dir, span, len)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, (ticks[0].id, ticks[-1].id, ticks[0].ts, ticks[-1].ts, segm_dir, segm_span, segm_len))
                segm_id = int(cur.fetchone()["id"])

                # Insert levels
                for L in levels:
                    cur.execute("""
                        INSERT INTO level (segm_id, tick_id, ts, kind, price, used_at_id, used_at_ts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (segm_id, L["tick_id"], L["ts"], L["kind"], L["price"], L.get("used_at_id"), L.get("used_at_ts")))

                # Insert big legs
                for lg in legs:
                    cur.execute("""
                        INSERT INTO bigm (segm_id, a_id, b_id, a_ts, b_ts, dir, move, ticks)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (segm_id, lg.a_id, lg.b_id, lg.a_ts, lg.b_ts, lg.dir, lg.move, lg.ticks))

                # Insert small moves (inside big legs)
                for s in smal_rows:
                    cur.execute("""
                        INSERT INTO smal (segm_id, a_id, b_id, a_ts, b_ts, dir, move, ticks)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (segm_id, s["a_id"], s["b_id"], s["a_ts"], s["b_ts"], s["dir"], s["move"], s["ticks"]))

                # Insert predictions
                right = wrong = 0
                for p in preds:
                    cur.execute("""
                        INSERT INTO pred (segm_id, at_id, at_ts, dir, goal_usd, hit, resolved_at_id, resolved_at_ts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                    """, (segm_id, p["at_id"], p["at_ts"], p["dir"], self.GOAL, p["hit"], p["resolved_at_id"], p["resolved_at_ts"]))
                    _ = cur.fetchone()["id"]
                    if p["hit"] is True: right += 1
                    elif p["hit"] is False: wrong += 1

                # Outcome
                total = right + wrong
                ratio = 0 if total == 0 else (1 if right == total else (-1 if wrong == total else ratio_val))
                cur.execute("""
                    INSERT INTO outcome (time, duration, predictions, ratio, segm_id)
                    VALUES (%s,%s,%s,%s,%s)
                """, (ticks[0].ts, int((ticks[-1].ts - ticks[0].ts).total_seconds()), total, ratio, segm_id))

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = True

    # ----------------- Stat pointer -----------------
    def _get_ptr(self) -> int:
        with dict_cur(self.conn) as cur:
            cur.execute("SELECT val FROM stat WHERE key='last_done_tick_id'")
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO stat (key, val) VALUES ('last_done_tick_id', %s)", (0,))
                return 0
            return int(row["val"])

    def _set_ptr(self, tick_id: int):
        with dict_cur(self.conn) as cur:
            cur.execute("UPDATE stat SET val=%s WHERE key='last_done_tick_id'", (int(tick_id),))

    # ----------------- Small utils -----------------
    def _scalar(self, sql: str, params: tuple = ()) -> Optional[int]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            r = cur.fetchone()
            return r[0] if r else None
