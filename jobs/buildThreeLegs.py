# jobs/buildThreeLegs.py
from __future__ import annotations

import argparse
from typing import Dict, List, Optional

from backend.db import get_conn, dict_cur, columns_exist, detect_mid_expr


def _ticks_has_kal(conn) -> bool:
    return "kal" in columns_exist(conn, "ticks", ["kal"])


def _load_segline_ticks(conn, start_tick_id: int, end_tick_id: int) -> List[Dict[str, float]]:
    mid_expr = detect_mid_expr(conn)
    kal_expr = "t.kal" if _ticks_has_kal(conn) else mid_expr

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT t.id,
                   t.timestamp,
                   {mid_expr} AS mid,
                   {kal_expr} AS kal
            FROM public.ticks t
            WHERE t.id BETWEEN %s AND %s
            ORDER BY t.id ASC
            """,
            (int(start_tick_id), int(end_tick_id)),
        )
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "mid": float(r["mid"]) if r["mid"] is not None else None,
                "kal": float(r["kal"]) if r["kal"] is not None else None,
            }
        )
    return out


def _direction_from_line(start_price: Optional[float], end_price: Optional[float], fallback_vals: List[float]) -> int:
    if start_price is not None and end_price is not None and start_price != end_price:
        return 1 if end_price > start_price else -1
    if len(fallback_vals) >= 2 and fallback_vals[0] is not None and fallback_vals[-1] is not None:
        return 1 if fallback_vals[-1] > fallback_vals[0] else -1
    return 1


def _tick_fields(ticks: List[Dict[str, float]], idx: Optional[int]) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    if idx is None or idx < 0 or idx >= len(ticks):
        return None, None, None
    t = ticks[idx]
    return int(t["id"]), t.get("mid"), t.get("kal")


def _safe_int_diff(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None or b is None:
        return None
    return int(b - a)


def _safe_move(direction: int, v1: Optional[float], v2: Optional[float]) -> Optional[float]:
    if v1 is None or v2 is None:
        return None
    return float(direction * (v2 - v1))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, required=True)
    ap.add_argument("--early-max-ticks", type=int, default=20000)
    ap.add_argument("--k-neighborhood", type=int, default=60)
    ap.add_argument("--min-move", type=float, default=0.8)
    ap.add_argument("--break-buffer", type=float, default=0.0)
    args = ap.parse_args()

    segm_id = int(args.segm_id)
    early_max_ticks = int(args.early_max_ticks)
    k = int(args.k_neighborhood)
    min_move = float(args.min_move)
    break_buffer = float(args.break_buffer)

    conn = get_conn()
    complete = 0
    incomplete = 0

    try:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT id, segm_id,
                       start_tick_id, end_tick_id,
                       start_price, end_price
                FROM public.seglines
                WHERE segm_id=%s
                ORDER BY start_tick_id ASC, id ASC
                """,
                (segm_id,),
            )
            seglines = cur.fetchall()

        if not seglines:
            print(f"[buildThreeLegs] no seglines for segm_id={segm_id}")
            return

        for ln in seglines:
            segline_id = int(ln["id"])
            start_tick_id = int(ln["start_tick_id"])
            end_tick_id = int(ln["end_tick_id"])
            start_price = float(ln["start_price"]) if ln["start_price"] is not None else None
            end_price = float(ln["end_price"]) if ln["end_price"] is not None else None

            early_end_tick_id = min(end_tick_id, start_tick_id + early_max_ticks)
            ticks = _load_segline_ticks(conn, start_tick_id, early_end_tick_id)

            vals = [t.get("kal") for t in ticks]
            tick_ids = [t.get("id") for t in ticks]

            has_b = False
            has_c = False
            has_d = False
            reason = None

            if not ticks or len(ticks) < (2 * k + 1):
                reason = "TooShort"
                a_idx = 0 if ticks else None
                b_idx = c_idx = d_idx = None
            else:
                try:
                    a_idx = tick_ids.index(start_tick_id)
                except ValueError:
                    a_idx = 0

                if vals[a_idx] is None:
                    reason = "TooShort"
                    b_idx = c_idx = d_idx = None
                else:
                    direction = _direction_from_line(start_price, end_price, vals)
                    a_val = vals[a_idx]
                    b_idx = a_idx
                    b_val = a_val
                    c_idx = None
                    c_val = None

                    for i in range(a_idx + 1, len(vals)):
                        v = vals[i]
                        if v is None:
                            continue

                        if not has_c:
                            if direction > 0:
                                if v >= b_val:
                                    b_val = v
                                    b_idx = i
                                if (b_val - a_val) >= min_move:
                                    has_b = True
                                if has_b and (b_val - v) >= min_move:
                                    has_c = True
                                    c_idx = i
                                    c_val = v
                            else:
                                if v <= b_val:
                                    b_val = v
                                    b_idx = i
                                if (a_val - b_val) >= min_move:
                                    has_b = True
                                if has_b and (v - b_val) >= min_move:
                                    has_c = True
                                    c_idx = i
                                    c_val = v
                        else:
                            if direction > 0:
                                if v <= c_val:
                                    c_val = v
                                    c_idx = i
                                if v > b_val + break_buffer:
                                    d_idx = i
                                    has_d = True
                                    break
                            else:
                                if v >= c_val:
                                    c_val = v
                                    c_idx = i
                                if v < b_val - break_buffer:
                                    d_idx = i
                                    has_d = True
                                    break

                    if not has_b:
                        reason = "NoAttempt"
                    elif not has_c:
                        reason = "NoCounter"
                    elif not has_d:
                        reason = "NoConfirm"

            if ticks:
                direction = _direction_from_line(start_price, end_price, vals)
            else:
                direction = 1

            a_tick_id, a_mid, a_kal = _tick_fields(ticks, a_idx if ticks else None)
            if a_tick_id is None:
                a_tick_id = start_tick_id
            b_tick_id, b_mid, b_kal = _tick_fields(ticks, b_idx)
            c_tick_id, c_mid, c_kal = _tick_fields(ticks, c_idx)
            d_tick_id, d_mid, d_kal = _tick_fields(ticks, d_idx)

            ab_ticks = _safe_int_diff(a_tick_id, b_tick_id)
            bc_ticks = _safe_int_diff(b_tick_id, c_tick_id)
            cd_ticks = _safe_int_diff(c_tick_id, d_tick_id)

            ab_move = _safe_move(direction, a_kal, b_kal)
            bc_move = _safe_move(direction, b_kal, c_kal)
            cd_move = _safe_move(direction, c_kal, d_kal)

            bc_retrace_pct = None
            if ab_move is not None and ab_move != 0 and bc_move is not None:
                bc_retrace_pct = abs(bc_move) / abs(ab_move)

            with dict_cur(conn) as cur:
                cur.execute(
                    "DELETE FROM public.legs WHERE segline_id=%s",
                    (segline_id,),
                )
                cur.execute(
                    """
                    INSERT INTO public.legs (
                        segm_id, segline_id, direction,
                        early_end_tick_id, k_neighborhood, min_move, break_buffer, early_max_ticks,
                        a_tick_id, b_tick_id, c_tick_id, d_tick_id,
                        a_mid, a_kal, b_mid, b_kal, c_mid, c_kal, d_mid, d_kal,
                        ab_ticks, bc_ticks, cd_ticks,
                        ab_move, bc_move, cd_move, bc_retrace_pct,
                        has_b, has_c, has_d, reason
                    )
                    VALUES (
                        %s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,%s
                    )
                    """,
                    (
                        segm_id,
                        segline_id,
                        int(direction),
                        int(early_end_tick_id) if early_end_tick_id is not None else None,
                        int(k),
                        float(min_move),
                        float(break_buffer),
                        int(early_max_ticks),
                        int(a_tick_id) if a_tick_id is not None else None,
                        int(b_tick_id) if b_tick_id is not None else None,
                        int(c_tick_id) if c_tick_id is not None else None,
                        int(d_tick_id) if d_tick_id is not None else None,
                        a_mid,
                        a_kal,
                        b_mid,
                        b_kal,
                        c_mid,
                        c_kal,
                        d_mid,
                        d_kal,
                        ab_ticks,
                        bc_ticks,
                        cd_ticks,
                        ab_move,
                        bc_move,
                        cd_move,
                        bc_retrace_pct,
                        has_b,
                        has_c,
                        has_d,
                        reason,
                    ),
                )

            print(
                f"[segLine {segline_id}] A={a_tick_id} B={b_tick_id} C={c_tick_id} D={d_tick_id} "
                f"has_b={has_b} has_c={has_c} has_d={has_d} reason={reason}"
            )

            if has_d:
                complete += 1
            else:
                incomplete += 1

        print(
            f"[buildThreeLegs] segm_id={segm_id} complete={complete} incomplete={incomplete}"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
