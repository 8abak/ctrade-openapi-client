# jobs/breakLine.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur

LOG_PATH = os.path.join("logs", "breakLine.log")
BATCH_SIZE = 10_000


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _to_ms(ts: datetime) -> int:
    # ts is timestamptz from postgres -> python datetime
    return int(ts.timestamp() * 1000)


def _line_interp(p1: float, p2: float, x1: int, x2: int, xi: int) -> float:
    if x2 == x1:
        return p2
    return p1 + (p2 - p1) * ((xi - x1) / (x2 - x1))


def _append_log(line: str) -> None:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


@dataclass
class Tick:
    segtick_row_id: int
    tick_id: int
    ts: datetime
    price: float


def break_line(segm_id: int, segLine_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Runs one step of break-line for segm_id.
    - init if no seglines exist yet
    - else split one active line (specified or worst by max_abs_dist)
    Returns summary dict.
    """
    if not isinstance(segm_id, int) or segm_id <= 0:
        return {"error": "segm_id must be a positive int"}

    conn = get_conn()
    conn.autocommit = False

    try:
        with dict_cur(conn) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM public.seglines WHERE segm_id = %s", (segm_id,))
            n_lines = int(cur.fetchone()["n"])

        if n_lines == 0:
            out = _init_mode(conn, segm_id)
        else:
            out = _split_mode(conn, segm_id, segLine_id)

        conn.commit()
        return out
    except Exception as e:
        conn.rollback()
        return {"error": str(e), "segm_id": segm_id}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_first_last_tick(conn, segm_id: int) -> Tuple[Tick, Tick]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT st.id AS segtick_row_id, t.id AS tick_id, t.timestamp AS ts,
                   COALESCE(t.kal, t.mid) AS price
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE st.segm_id = %s
            ORDER BY t.timestamp ASC, t.id ASC
            LIMIT 1
            """,
            (segm_id,),
        )
        r1 = cur.fetchone()
        if not r1 or r1["price"] is None:
            raise RuntimeError("no ticks (or no kal/mid) for segm_id")

        cur.execute(
            """
            SELECT st.id AS segtick_row_id, t.id AS tick_id, t.timestamp AS ts,
                   COALESCE(t.kal, t.mid) AS price
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE st.segm_id = %s
            ORDER BY t.timestamp DESC, t.id DESC
            LIMIT 1
            """,
            (segm_id,),
        )
        r2 = cur.fetchone()
        if not r2 or r2["price"] is None:
            raise RuntimeError("no ticks (or no kal/mid) for segm_id")

    return (
        Tick(int(r1["segtick_row_id"]), int(r1["tick_id"]), r1["ts"], float(r1["price"])),
        Tick(int(r2["segtick_row_id"]), int(r2["tick_id"]), r2["ts"], float(r2["price"])),
    )


def _init_mode(conn, segm_id: int) -> Dict[str, Any]:
    first_t, last_t = _get_first_last_tick(conn, segm_id)

    with dict_cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO public.seglines (
              segm_id, parent_id, depth, iteration,
              start_tick_id, end_tick_id,
              start_ts, end_ts,
              start_price, end_price,
              is_active
            )
            VALUES (%s, NULL, 0, 0, %s, %s, %s, %s, %s, %s, true)
            RETURNING id
            """,
            (
                segm_id,
                first_t.tick_id,
                last_t.tick_id,
                first_t.ts,
                last_t.ts,
                first_t.price,
                last_t.price,
            ),
        )
        line_id = int(cur.fetchone()["id"])

    # assign all ticks in batches
    _reassign_all_ticks_to_line(conn, segm_id, line_id, first_t, last_t)

    # stats
    num_ticks, max_abs = _update_line_stats(conn, line_id)
    num_active, global_max = _update_global_stats(conn, segm_id)

    _append_log(
        f"[{_now_utc()}] segm_id={segm_id} action=init line={line_id} "
        f"ticks={num_ticks} global_max_abs_dist={global_max}"
    )

    return {
        "segm_id": segm_id,
        "action": "init",
        "segLine_id": line_id,
        "num_lines_active": num_active,
        "global_max_abs_dist": global_max,
    }


def _reassign_all_ticks_to_line(conn, segm_id: int, line_id: int, first_t: Tick, last_t: Tick) -> None:
    x1 = _to_ms(first_t.ts)
    x2 = _to_ms(last_t.ts)
    p1 = first_t.price
    p2 = last_t.price

    offset = 0
    while True:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT st.id AS segtick_row_id, t.id AS tick_id, t.timestamp AS ts,
                       COALESCE(t.kal, t.mid) AS price
                FROM public.segticks st
                JOIN public.ticks t ON t.id = st.tick_id
                WHERE st.segm_id = %s
                ORDER BY t.timestamp ASC, t.id ASC
                LIMIT %s OFFSET %s
                """,
                (segm_id, BATCH_SIZE, offset),
            )
            rows = cur.fetchall()

        if not rows:
            break

        updates: List[Tuple[int, int, float]] = []
        for r in rows:
            if r["price"] is None:
                continue
            xi = _to_ms(r["ts"])
            phat = _line_interp(p1, p2, x1, x2, xi)
            dist = float(r["price"]) - phat
            updates.append((int(r["segtick_row_id"]), line_id, float(dist)))

        _bulk_update_segticks(conn, updates)
        offset += BATCH_SIZE


def _split_mode(conn, segm_id: int, segLine_id: Optional[int]) -> Dict[str, Any]:
    line = _pick_line_to_split(conn, segm_id, segLine_id)
    if line is None:
        return {"error": "no active segLines to split", "segm_id": segm_id}

    pivot = _pick_pivot_tick(conn, segm_id, int(line["id"]))
    if pivot is None:
        return {"error": "no pivot tick found for segLine", "segm_id": segm_id, "segLine_id": int(line["id"])}

    # create left/right
    new_left_id, new_right_id = _create_children_lines(conn, segm_id, line, pivot)

    # deactivate old
    with dict_cur(conn) as cur:
        cur.execute(
            "UPDATE public.seglines SET is_active=false, updated_at=now() WHERE id=%s",
            (int(line["id"]),),
        )

    # reassign only ticks from old line
    left_ticks, right_ticks = _reassign_ticks_for_split(
        conn,
        segm_id=segm_id,
        old_line_id=int(line["id"]),
        pivot_tick_id=int(pivot["tick_id"]),
        pivot_ts=pivot["ts"],
        left_id=new_left_id,
        right_id=new_right_id,
    )

    # stats per new line
    _update_line_stats(conn, new_left_id)
    _update_line_stats(conn, new_right_id)
    num_active, global_max = _update_global_stats(conn, segm_id)

    pivot_abs = float(abs(float(pivot["dist"]))) if pivot["dist"] is not None else None

    _append_log(
        f"[{_now_utc()}] segm_id={segm_id} action=split split_line={int(line['id'])} "
        f"pivot_tick={int(pivot['tick_id'])} pivot_abs_dist={pivot_abs} "
        f"new_left={new_left_id} new_right={new_right_id} "
        f"left_ticks={left_ticks} right_ticks={right_ticks} global_max_abs_dist={global_max}"
    )

    return {
        "segm_id": segm_id,
        "action": "split",
        "segLine_id": int(line["id"]),
        "pivot_tick_id": int(pivot["tick_id"]),
        "pivot_abs_dist": pivot_abs,
        "new_left_id": new_left_id,
        "new_right_id": new_right_id,
        "num_lines_active": num_active,
        "global_max_abs_dist": global_max,
    }


def _pick_line_to_split(conn, segm_id: int, segLine_id: Optional[int]) -> Optional[Dict[str, Any]]:
    with dict_cur(conn) as cur:
        if segLine_id is not None:
            cur.execute(
                "SELECT * FROM public.seglines WHERE id=%s",
                (int(segLine_id),),
            )
            r = cur.fetchone()
            if not r:
                return None
            if int(r["segm_id"]) != int(segm_id):
                raise RuntimeError("segLine_id does not belong to segm_id")
            if not bool(r["is_active"]):
                raise RuntimeError("segLine_id is not active")
            return r

        cur.execute(
            """
            SELECT *
            FROM public.seglines
            WHERE segm_id=%s AND is_active=true
            ORDER BY max_abs_dist DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (segm_id,),
        )
        return cur.fetchone()


def _pick_pivot_tick(conn, segm_id: int, line_id: int) -> Optional[Dict[str, Any]]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT st.tick_id AS tick_id, t.timestamp AS ts,
                   COALESCE(t.kal, t.mid) AS price,
                   st.dist AS dist
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE st.segm_id=%s AND st.segline_id=%s
            ORDER BY ABS(st.dist) DESC NULLS LAST, t.timestamp ASC
            LIMIT 1
            """,
            (segm_id, line_id),
        )
        r = cur.fetchone()
        return r


def _create_children_lines(conn, segm_id: int, line: Dict[str, Any], pivot: Dict[str, Any]) -> Tuple[int, int]:
    with dict_cur(conn) as cur:
        cur.execute("SELECT COALESCE(MAX(iteration), 0) AS it FROM public.seglines WHERE segm_id=%s", (segm_id,))
        it_next = int(cur.fetchone()["it"]) + 1

    # endpoints
    start_tick_id = int(line["start_tick_id"])
    end_tick_id = int(line["end_tick_id"])
    pivot_tick_id = int(pivot["tick_id"])

    left_start = _get_tick_by_id(conn, start_tick_id)
    left_end = _get_tick_by_id(conn, pivot_tick_id)
    right_start = left_end
    right_end = _get_tick_by_id(conn, end_tick_id)

    depth = int(line["depth"]) + 1
    parent_id = int(line["id"])

    with dict_cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO public.seglines (
              segm_id, parent_id, depth, iteration,
              start_tick_id, end_tick_id, start_ts, end_ts, start_price, end_price,
              is_active
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true)
            RETURNING id
            """,
            (
                segm_id, parent_id, depth, it_next,
                left_start["tick_id"], left_end["tick_id"],
                left_start["ts"], left_end["ts"],
                float(left_start["price"]), float(left_end["price"]),
            ),
        )
        left_id = int(cur.fetchone()["id"])

        cur.execute(
            """
            INSERT INTO public.seglines (
              segm_id, parent_id, depth, iteration,
              start_tick_id, end_tick_id, start_ts, end_ts, start_price, end_price,
              is_active
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true)
            RETURNING id
            """,
            (
                segm_id, parent_id, depth, it_next,
                right_start["tick_id"], right_end["tick_id"],
                right_start["ts"], right_end["ts"],
                float(right_start["price"]), float(right_end["price"]),
            ),
        )
        right_id = int(cur.fetchone()["id"])

    return left_id, right_id


def _get_tick_by_id(conn, tick_id: int) -> Dict[str, Any]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT t.id AS tick_id, t.timestamp AS ts, COALESCE(t.kal, t.mid) AS price
            FROM public.ticks t
            WHERE t.id=%s
            """,
            (tick_id,),
        )
        r = cur.fetchone()
        if not r or r["price"] is None:
            raise RuntimeError(f"tick not found (or no kal/mid): {tick_id}")
        return r


def _reassign_ticks_for_split(
    conn,
    segm_id: int,
    old_line_id: int,
    pivot_tick_id: int,
    pivot_ts: datetime,
    left_id: int,
    right_id: int,
) -> Tuple[int, int]:
    # load endpoints for both new lines
    left_line = _get_line_endpoints(conn, left_id)
    right_line = _get_line_endpoints(conn, right_id)

    left_count = 0
    right_count = 0

    offset = 0
    while True:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT st.id AS segtick_row_id, st.tick_id AS tick_id,
                       t.timestamp AS ts, COALESCE(t.kal, t.mid) AS price
                FROM public.segticks st
                JOIN public.ticks t ON t.id = st.tick_id
                WHERE st.segm_id=%s AND st.segline_id=%s
                ORDER BY t.timestamp ASC, t.id ASC
                LIMIT %s OFFSET %s
                """,
                (segm_id, old_line_id, BATCH_SIZE, offset),
            )
            rows = cur.fetchall()

        if not rows:
            break

        updates: List[Tuple[int, int, float]] = []
        for r in rows:
            if r["price"] is None:
                continue

            is_left = (r["ts"] < pivot_ts) or (r["ts"] == pivot_ts and int(r["tick_id"]) <= pivot_tick_id)
            if is_left:
                dist = _dist_for_line(left_line, r["ts"], float(r["price"]))
                updates.append((int(r["segtick_row_id"]), left_id, float(dist)))
                left_count += 1
            else:
                dist = _dist_for_line(right_line, r["ts"], float(r["price"]))
                updates.append((int(r["segtick_row_id"]), right_id, float(dist)))
                right_count += 1

        _bulk_update_segticks(conn, updates)
        offset += BATCH_SIZE

    return left_count, right_count


def _get_line_endpoints(conn, line_id: int) -> Dict[str, Any]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, start_ts, end_ts, start_price, end_price
            FROM public.seglines
            WHERE id=%s
            """,
            (line_id,),
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError(f"line not found: {line_id}")
        return r


def _dist_for_line(line: Dict[str, Any], ts: datetime, price: float) -> float:
    x1 = _to_ms(line["start_ts"])
    x2 = _to_ms(line["end_ts"])
    xi = _to_ms(ts)
    phat = _line_interp(float(line["start_price"]), float(line["end_price"]), x1, x2, xi)
    return price - phat


def _bulk_update_segticks(conn, rows: List[Tuple[int, int, float]]) -> None:
    if not rows:
        return
    with dict_cur(conn) as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE public.segticks AS st
            SET segline_id = v.segline_id,
                dist = v.dist
            FROM (VALUES %s) AS v(segtick_id, segline_id, dist)
            WHERE st.id = v.segtick_id
            """,
            rows,
            template="(%s,%s,%s)",
            page_size=10_000,
        )


def _update_line_stats(conn, line_id: int) -> Tuple[int, Optional[float]]:
    with dict_cur(conn) as cur:
        # num_ticks + max_abs_dist
        cur.execute(
            """
            SELECT COUNT(*) AS n, MAX(ABS(dist)) AS mx
            FROM public.segticks
            WHERE segline_id=%s
            """,
            (line_id,),
        )
        r = cur.fetchone()
        n = int(r["n"])
        mx = float(r["mx"]) if r["mx"] is not None else None

        # duration_ms from line endpoints
        cur.execute(
            "SELECT start_ts, end_ts FROM public.seglines WHERE id=%s",
            (line_id,),
        )
        lr = cur.fetchone()
        dur_ms = None
        if lr:
            dur_ms = _to_ms(lr["end_ts"]) - _to_ms(lr["start_ts"])

        cur.execute(
            """
            UPDATE public.seglines
            SET num_ticks=%s, duration_ms=%s, max_abs_dist=%s, updated_at=now()
            WHERE id=%s
            """,
            (n, dur_ms, mx, line_id),
        )
    return n, mx


def _update_global_stats(conn, segm_id: int) -> Tuple[int, Optional[float]]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n,
                   MAX(max_abs_dist) AS mx
            FROM public.seglines
            WHERE segm_id=%s AND is_active=true
            """,
            (segm_id,),
        )
        r = cur.fetchone()
        n = int(r["n"])
        mx = float(r["mx"]) if r["mx"] is not None else None
    return n, mx
