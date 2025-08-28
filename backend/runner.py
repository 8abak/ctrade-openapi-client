# PATH: backend/runner.py
"""
Segment runner that:
- Splits ticks by >GAP_SECONDS gaps.
- Within each segment, finds BIG movements (>= BIG_USD) in either direction.
- Within current big-move direction, finds SMALL moves (>= SMALL_USD).
- Emits predictions on qualifying SMALL moves and resolves within the same segment.
- Writes segm, bigm, smal, pred, outcome, advances stat.last_done_tick_id.
Strictly forward; no future peeking beyond the segment end.
"""
import os
import time
from decimal import Decimal
from typing import List, Dict, Any, Tuple

import psycopg2
import psycopg2.extras
from backend.db import get_conn, dict_cur, detect_ts_col, detect_mid_expr, scalar

GAP_SECONDS = int(os.getenv("GAP_SECONDS", "180"))
BIG_USD = float(os.getenv("BIG_USD", "10"))     # threshold for big movements
SMALL_USD = float(os.getenv("SMALL_USD", "2"))
RETRACE_USD = float(os.getenv("RETRACE_USD", "1"))
ROLL_N = int(os.getenv("ROLL_N", "100"))
SLEEP_BETWEEN_SEGMENTS = float(os.getenv("SLEEP_BETWEEN_SEGMENTS", "0.25"))

def _round_ratio(right: int, wrong: int) -> float:
    if right == 0 and wrong == 0:
        return 0.0
    if right == 0:
        return -1.0
    if wrong == 0:
        return 1.0
    return round((right - wrong) / (right + wrong), 2)

class Runner:
    def __init__(self):
        self.conn = get_conn()
        self.ts_col = detect_ts_col(self.conn)      # e.g., "timestamp"
        self.mid_expr = detect_mid_expr(self.conn)  # e.g., "mid" or "(bid+ask)/2.0"

    # ------ helpers to read ticks windows ------
    def _latest_id(self) -> int:
        return int(scalar(self.conn, "SELECT COALESCE(MAX(id),0) FROM ticks") or 0)

    def _get_pointer(self) -> int:
        val = scalar(self.conn, "SELECT val FROM stat WHERE key='last_done_tick_id'")
        return int(val or 0)

    def _set_pointer(self, tick_id: int):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stat(key, val) VALUES ('last_done_tick_id', %s)
                ON CONFLICT (key) DO UPDATE SET val=EXCLUDED.val
            """, (tick_id,))

    def _find_next_segment_bounds(self, start_from_id: int) -> Tuple[int, int]:
        """
        Returns inclusive [seg_start_id, seg_end_id].
        seg_start_id is max(start_from_id, first available id).
        seg_end_id ends before the first row whose gap > GAP_SECONDS (or last tick id if none).
        """
        with dict_cur(self.conn) as cur:
            # First available id >= start_from_id
            cur.execute(f"""
                SELECT id FROM ticks
                WHERE id >= %s
                ORDER BY id ASC
                LIMIT 1
            """, (start_from_id,))
            row = cur.fetchone()
            if not row:
                return (0, 0)
            seg_start = int(row["id"])

            # Find first row AFTER start that starts a >GAP gap
            cur.execute(f"""
                WITH x AS (
                  SELECT id, {self.ts_col} AS ts,
                         LAG({self.ts_col}) OVER (ORDER BY id) AS prev_ts
                  FROM ticks
                  WHERE id >= %s
                )
                SELECT id
                FROM x
                WHERE prev_ts IS NOT NULL AND EXTRACT(EPOCH FROM (ts - prev_ts)) > %s
                ORDER BY id
                LIMIT 1
            """, (seg_start, GAP_SECONDS))
            row = cur.fetchone()
            if row:
                seg_end = int(row["id"]) - 1
            else:
                seg_end = self._latest_id()
        return (seg_start, seg_end)

    def _load_ticks(self, a: int, b: int) -> List[Dict[str, Any]]:
        with dict_cur(self.conn) as cur:
            cur.execute(f"""
                SELECT id, {self.ts_col} AS ts, {self.mid_expr} AS mid
                FROM ticks
                WHERE id BETWEEN %s AND %s
                ORDER BY id ASC
            """, (a, b))
            rows = cur.fetchall()
            # Ensure mid is python float
            for r in rows:
                v = r["mid"]
                if isinstance(v, Decimal):
                    r["mid"] = float(v)
            return rows

    # ------ movement detectors ------
    def _big_moves(self, ticks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Consecutive big moves: from pivot to first point that reaches BIG_USD either direction."""
        res = []
        if not ticks:
            return res
        piv_i = 0
        piv_price = ticks[0]["mid"]
        n = len(ticks)
        i = 1
        while i < n:
            delta = ticks[i]["mid"] - piv_price
            if abs(delta) >= BIG_USD:
                direction = "up" if delta > 0 else "dn"
                res.append({
                    "a_i": piv_i,
                    "b_i": i,
                    "dir": direction,
                    "move": abs(delta),
                    "ticks": i - piv_i + 1
                })
                piv_i = i
                piv_price = ticks[i]["mid"]
            i += 1
        return res

    def _small_moves_and_preds(self, ticks: List[Dict[str, Any]], bigms: List[Dict[str, Any]]) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]]]:
        """Within each big move window, scan small moves in that big direction.
        Emit predictions when small move completes and baseline condition passes.
        Resolve predictions within the same segment.
        """
        smalls = []
        preds = []
        if not ticks or not bigms:
            return smalls, preds

        # rolling baseline
        def baseline(i: int) -> float:
            # average of last N (within segment window)
            lo = max(0, i - ROLL_N + 1)
            count = i - lo + 1
            if count <= 0:
                return ticks[i]["mid"]
            s = 0.0
            for j in range(lo, i+1):
                s += ticks[j]["mid"]
            return s / count

        # For pred resolution tracking
        live_preds: List[Dict[str,Any]] = []

        for bm in bigms:
            a_i, b_i, bdir = bm["a_i"], bm["b_i"], bm["dir"]
            # swing point for small detection within this window
            swing_i = a_i
            swing_price = ticks[a_i]["mid"]

            for i in range(a_i + 1, b_i + 1):
                price = ticks[i]["mid"]
                delta = price - swing_price
                if (bdir == "up" and delta >= SMALL_USD) or (bdir == "dn" and -delta >= SMALL_USD):
                    # record small move from swing_i -> i
                    smalls.append({
                        "a_i": swing_i, "b_i": i, "dir": bdir,
                        "move": abs(price - swing_price),
                        "ticks": i - swing_i + 1
                    })
                    # emit pred if baseline agrees with direction
                    bl = baseline(i)
                    ok = (bdir == "up" and price > bl) or (bdir == "dn" and price < bl)
                    if ok:
                        p = {
                            "at_i": i,
                            "dir": bdir,
                            "goal_usd": SMALL_USD,
                            "hit": None,
                            "resolved_i": None
                        }
                        preds.append(p)
                        live_preds.append(p)
                    # reset swing
                    swing_i = i
                    swing_price = price
                # resolve live preds by first-touch rule
                new_live = []
                for p in live_preds:
                    entry = ticks[p["at_i"]]["mid"]
                    if p["dir"] == "up":
                        if price >= entry + p["goal_usd"]:
                            p["hit"] = True
                            p["resolved_i"] = i
                        elif price <= entry - RETRACE_USD:
                            p["hit"] = False
                            p["resolved_i"] = i
                    else:
                        if price <= entry - p["goal_usd"]:
                            p["hit"] = True
                            p["resolved_i"] = i
                        elif price >= entry + RETRACE_USD:
                            p["hit"] = False
                            p["resolved_i"] = i
                    if p["resolved_i"] is None:
                        new_live.append(p)
                live_preds = new_live

        return smalls, preds

    # ------ write rows ------
    def _write_segment_commit(self, seg_start_id: int, seg_end_id: int, ticks: List[Dict[str,Any]]):
        if not ticks:
            return
        # overall dir for metadata
        dir_ = "up" if ticks[-1]["mid"] - ticks[0]["mid"] >= 0 else "dn"
        span = ticks[-1]["mid"] - ticks[0]["mid"]
        # big movements
        bigms = self._big_moves(ticks)
        # small + preds aligned with big
        smalls, preds = self._small_moves_and_preds(ticks, bigms)

        with dict_cur(self.conn) as cur:
            # segm
            cur.execute("""
                INSERT INTO segm(start_id,end_id,start_ts,end_ts,dir,span,len)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (ticks[0]["id"], ticks[-1]["id"], ticks[0]["ts"], ticks[-1]["ts"], dir_, span, len(ticks)))
            segm_id = int(cur.fetchone()["id"])

            # bigm
            for bm in bigms:
                a = ticks[bm["a_i"]]; b = ticks[bm["b_i"]]
                cur.execute("""
                    INSERT INTO bigm(segm_id,a_id,b_id,a_ts,b_ts,dir,move,ticks)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (segm_id, a["id"], b["id"], a["ts"], b["ts"], bm["dir"], bm["move"], bm["ticks"]))

            # smal
            for sm in smalls:
                a = ticks[sm["a_i"]]; b = ticks[sm["b_i"]]
                cur.execute("""
                    INSERT INTO smal(segm_id,a_id,b_id,a_ts,b_ts,dir,move,ticks)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (segm_id, a["id"], b["id"], a["ts"], b["ts"], sm["dir"], sm["move"], sm["ticks"]))

            # preds
            right = wrong = 0
            for p in preds:
                at = ticks[p["at_i"]]
                hit = p["hit"]
                resolved_id = None
                resolved_ts = None
                if p["resolved_i"] is not None:
                    rr = ticks[p["resolved_i"]]
                    resolved_id, resolved_ts = rr["id"], rr["ts"]
                if hit is True:
                    right += 1
                elif hit is False:
                    wrong += 1
                cur.execute("""
                    INSERT INTO pred(segm_id,at_id,at_ts,dir,goal_usd,hit,resolved_at_id,resolved_at_ts)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (segm_id, at["id"], at["ts"], p["dir"], SMALL_USD, hit, resolved_id, resolved_ts))

            ratio = _round_ratio(right, wrong)
            duration = int((ticks[-1]["ts"] - ticks[0]["ts"]).total_seconds())
            cur.execute("""
                INSERT INTO outcome(time,duration,predictions,ratio,segm_id)
                VALUES (%s,%s,%s,%s,%s)
            """, (ticks[0]["ts"], duration, len(preds), ratio, segm_id))

        # advance pointer
        self._set_pointer(seg_end_id)

    # ------ public run loop ------
    def run_until_now(self) -> Dict[str, Any]:
        processed = 0
        start_ptr = self._get_pointer()
        if start_ptr <= 0:
            # If completely fresh, start from the very first tick id
            first_id = scalar(self.conn, "SELECT COALESCE(MIN(id),0) FROM ticks")
            start_ptr = int(first_id or 0)
        latest = self._latest_id()
        from_tick = start_ptr + 1
        to_tick = latest
        while True:
            seg_start, seg_end = self._find_next_segment_bounds(start_ptr + 1)
            if seg_start == 0 and seg_end == 0:
                break
            if seg_start > seg_end:
                break
            # Load segment ticks and process
            ticks = self._load_ticks(seg_start, seg_end)
            if ticks:
                self._write_segment_commit(seg_start, seg_end, ticks)
                processed += 1
            start_ptr = seg_end
            if start_ptr >= latest:
                break
            time.sleep(SLEEP_BETWEEN_SEGMENTS)
        return {"segments": processed, "from_tick": from_tick, "to_tick": to_tick}
