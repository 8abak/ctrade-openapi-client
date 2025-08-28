# PATH: backend/runner.py
import math
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .db import (
    q_dicts,
    exec_sql,
    db,
    tick_sql_fields,
    tick_mid_expr,
    tick_ts_col,
    last_tick_id,
    sleep_throttle,
)

# Core "Price-Action Segments" pipeline.
# Processes strictly forward, segment-by-segment (gaps > 180s).
# Commits results per segment and updates stat.last_done_tick_id.


GAP_SECONDS = 180
SMALL_MOVE_USD = Decimal("2.0")
RETRACE_USD = Decimal("1.0")
ROLL_N = 100
ROLL_FALLBACK = 50


def _get_last_done_tick_id() -> int:
    rows = q_dicts("SELECT val FROM stat WHERE key='last_done_tick_id' LIMIT 1")
    if not rows:
        return 0
    try:
        return int(rows[0]["val"])
    except Exception:
        return 0


def _set_last_done_tick_id(tick_id: int):
    exec_sql(
        """
        INSERT INTO stat(key, val) VALUES('last_done_tick_id', %s)
        ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val
        """,
        (str(tick_id),),
    )


def _iter_ticks_from(start_after_id: int, batch: int = 50000) -> Iterable[Dict[str, Any]]:
    # Streams ticks in ascending id order, starting strictly after start_after_id
    ts_col = tick_ts_col()
    mid_expr = tick_mid_expr()
    last = start_after_id
    while True:
        rows = q_dicts(
            f"""
            SELECT id, {ts_col} AS ts, {mid_expr} AS mid
            FROM ticks
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (last, batch),
        )
        if not rows:
            break
        for r in rows:
            # Normalize types
            r["id"] = int(r["id"])
            # mid may be Decimal; normalize to Decimal for precision
            if not isinstance(r["mid"], Decimal):
                r["mid"] = Decimal(str(r["mid"]))
            yield r
        last = rows[-1]["id"]


def _find_next_closed_segment(start_after_id: int) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int]]:
    # Accumulate ticks until first gap > 180s; return (segment_ticks, next_start_after_id)
    seg: List[Dict[str, Any]] = []
    prev_ts = None
    for r in _iter_ticks_from(start_after_id):
        if prev_ts is not None:
            dt = (r["ts"] - prev_ts).total_seconds()
            if dt > GAP_SECONDS:
                # segment closes at previous tick
                if seg:
                    return seg, r["id"] - 1  # next_start_after_id will be end_id
        seg.append(r)
        prev_ts = r["ts"]

    # No more rows; only process if we already have a closed segment signal (we don't).
    # We DO NOT process an open tail segment.
    return None, None


def _segment_direction_and_span(seg: List[Dict[str, Any]]) -> Tuple[str, Decimal]:
    first = seg[0]["mid"]
    last = seg[-1]["mid"]
    if last > first:
        return "up", last - first
    if last < first:
        return "dn", last - first
    # tie-break by larger absolute internal extreme move from first
    max_mid = max(p["mid"] for p in seg)
    min_mid = min(p["mid"] for p in seg)
    up_move = max_mid - first
    dn_move = first - min_mid
    if up_move >= dn_move:
        return "up", last - first
    return "dn", last - first


def _rolling_mean(series: List[Decimal], n: int) -> List[Decimal]:
    n = max(1, n)
    out: List[Decimal] = []
    s = Decimal("0")
    window: List[Decimal] = []
    for x in series:
        window.append(x)
        s += x
        if len(window) > n:
            s -= window.pop(0)
        out.append(s / Decimal(len(window)))
    return out


def _detect_small_moves_and_predictions(
    seg: List[Dict[str, Any]], big_dir: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Returns (smal_list, pred_list)
    mids = [p["mid"] for p in seg]
    roll_n = ROLL_N if len(seg) >= ROLL_N else (ROLL_FALLBACK if len(seg) >= ROLL_FALLBACK else max(5, len(seg) // 4))
    baseline = _rolling_mean(mids, roll_n)

    smals: List[Dict[str, Any]] = []
    preds: List[Dict[str, Any]] = []

    # Swing logic: start at first tick
    swing_idx = 0
    swing_mid = mids[0]

    for i in range(1, len(seg)):
        m = mids[i]
        if big_dir == "up":
            if (m - swing_mid) >= SMALL_MOVE_USD:
                a = seg[swing_idx]
                b = seg[i]
                smals.append(
                    {
                        "a_id": a["id"],
                        "b_id": b["id"],
                        "a_ts": a["ts"],
                        "b_ts": b["ts"],
                        "dir": "up",
                        "move": float(m - swing_mid),
                        "ticks": i - swing_idx + 1,
                    }
                )
                # Emit prediction if aligned with baseline (price above MA)
                if m >= baseline[i]:
                    preds.append(
                        {
                            "at_id": b["id"],
                            "at_ts": b["ts"],
                            "dir": "up",
                            "goal_usd": float(SMALL_MOVE_USD),
                        }
                    )
                # Reset swing to current point
                swing_idx = i
                swing_mid = m
            else:
                # allow swing to track the lowest base within segment for stronger next run-ups
                if m < swing_mid:
                    swing_mid = m
                    swing_idx = i
        else:  # big_dir == 'dn'
            if (swing_mid - m) >= SMALL_MOVE_USD:
                a = seg[swing_idx]
                b = seg[i]
                smals.append(
                    {
                        "a_id": a["id"],
                        "b_id": b["id"],
                        "a_ts": a["ts"],
                        "b_ts": b["ts"],
                        "dir": "dn",
                        "move": float(swing_mid - m),
                        "ticks": i - swing_idx + 1,
                    }
                )
                if m <= baseline[i]:
                    preds.append(
                        {
                            "at_id": b["id"],
                            "at_ts": b["ts"],
                            "dir": "dn",
                            "goal_usd": float(SMALL_MOVE_USD),
                        }
                    )
                # Reset swing
                swing_idx = i
                swing_mid = m
            else:
                if m > swing_mid:
                    swing_mid = m
                    swing_idx = i

    return smals, preds


def _resolve_predictions(
    seg: List[Dict[str, Any]], preds: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    # First-touch rule inside the SAME segment:
    # For 'up': hit if price rises +$2 from entry before falling -$1 from entry.
    # For 'dn': hit if price falls -$2 from entry before rising +$1 from entry.
    id_to_index = {p["id"]: idx for idx, p in enumerate(seg)}
    mids = [p["mid"] for p in seg]
    out: List[Dict[str, Any]] = []
    for p in preds:
        at_id = p["at_id"]
        idx = id_to_index.get(at_id)
        if idx is None:
            # Should not happen
            out.append({**p, "hit": None, "resolved_at_id": None, "resolved_at_ts": None})
            continue
        entry = mids[idx]
        goal = Decimal(str(p["goal_usd"]))
        # scan forward
        hit: Optional[bool] = None
        resolved_i: Optional[int] = None
        if p["dir"] == "up":
            tp = entry + goal
            sl = entry - RETRACE_USD
            # track first-touch
            for j in range(idx + 1, len(seg)):
                m = mids[j]
                if m >= tp:
                    hit = True
                    resolved_i = j
                    break
                if m <= sl:
                    hit = False
                    resolved_i = j
                    break
        else:  # dn
            tp = entry - goal
            sl = entry + RETRACE_USD
            for j in range(idx + 1, len(seg)):
                m = mids[j]
                if m <= tp:
                    hit = True
                    resolved_i = j
                    break
                if m >= sl:
                    hit = False
                    resolved_i = j
                    break
        if resolved_i is None:
            out.append({**p, "hit": None, "resolved_at_id": None, "resolved_at_ts": None})
        else:
            out.append(
                {
                    **p,
                    "hit": hit,
                    "resolved_at_id": seg[resolved_i]["id"],
                    "resolved_at_ts": seg[resolved_i]["ts"],
                }
            )
    return out


def _round_ratio(right: int, wrong: int) -> float:
    if right > 0 and wrong == 0:
        return 1.0
    if wrong > 0 and right == 0:
        return -1.0
    if right == wrong:
        return 0.0
    return round((right - wrong) / (right + wrong), 2)


def _insert_segment_results(
    seg: List[Dict[str, Any]], big_dir: str, span: Decimal, smals: List[Dict[str, Any]], preds: List[Dict[str, Any]]
) -> Dict[str, Any]:
    start_id = seg[0]["id"]
    end_id = seg[-1]["id"]
    start_ts = seg[0]["ts"]
    end_ts = seg[-1]["ts"]
    length = len(seg)

    with db() as (conn, cur):
        # segm
        cur.execute(
            """
            INSERT INTO segm(start_id, end_id, start_ts, end_ts, dir, span, len)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (start_id, end_id, start_ts, end_ts, big_dir, float(span), length),
        )
        segm_id = cur.fetchone()[0]

        # smal
        for s in smals:
            cur.execute(
                """
                INSERT INTO smal(segm_id, a_id, b_id, a_ts, b_ts, dir, move, ticks)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    segm_id,
                    s["a_id"],
                    s["b_id"],
                    s["a_ts"],
                    s["b_ts"],
                    s["dir"],
                    s["move"],
                    s["ticks"],
                ),
            )

        # pred (with resolutions)
        rights = 0
        wrongs = 0
        for p in preds:
            cur.execute(
                """
                INSERT INTO pred(segm_id, at_id, at_ts, dir, goal_usd, hit, resolved_at_id, resolved_at_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    segm_id,
                    p["at_id"],
                    p["at_ts"],
                    p["dir"],
                    p["goal_usd"],
                    p["hit"],
                    p["resolved_at_id"],
                    p["resolved_at_ts"],
                ),
            )
            if p["hit"] is True:
                rights += 1
            elif p["hit"] is False:
                wrongs += 1

        # outcome
        ratio = _round_ratio(rights, wrongs)
        duration = int((end_ts - start_ts).total_seconds())
        predictions = len(preds)
        cur.execute(
            """
            INSERT INTO outcome(time, duration, predictions, ratio, segm_id)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (start_ts, duration, predictions, ratio, segm_id),
        )
        outcome_id = cur.fetchone()[0]

    return {
        "segm_id": segm_id,
        "outcome_id": outcome_id,
        "start_id": start_id,
        "end_id": end_id,
        "predictions": len(preds),
        "rights": rights,
        "wrongs": wrongs,
    }


def run_until_now(max_segments: Optional[int] = None) -> Dict[str, Any]:
    processed = 0
    first_from: Optional[int] = None
    last_to: Optional[int] = None

    start_after = _get_last_done_tick_id()
    max_id = last_tick_id()
    if max_id is None:
        return {"segments": 0, "from_tick": None, "to_tick": None, "note": "no ticks"}

    while True:
        seg, end_marker = _find_next_closed_segment(start_after)
        if seg is None:
            break  # nothing more closed to process
        big_dir, span = _segment_direction_and_span(seg)
        smals, preds = _detect_small_moves_and_predictions(seg, big_dir)
        preds = _resolve_predictions(seg, preds)
        res = _insert_segment_results(seg, big_dir, span, smals, preds)

        _set_last_done_tick_id(res["end_id"])
        start_after = res["end_id"]

        processed += 1
        if first_from is None:
            first_from = res["start_id"]
        last_to = res["end_id"]

        sleep_throttle(0.1)  # small rest between segments

        if max_segments is not None and processed >= max_segments:
            break

    return {"segments": processed, "from_tick": first_from, "to_tick": last_to}
