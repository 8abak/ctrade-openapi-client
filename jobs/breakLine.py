# jobs/breakLine.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur

# Optional journal (works in your repo: backend/jobs/journal.py)
try:
    from backend.jobs.journal import write_journal  # type: ignore
except Exception:  # pragma: no cover
    write_journal = None  # type: ignore

LOG_PATH = os.path.join("logs", "breakLine.log")
BATCH_SIZE = 25_000


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _append_log(line: str) -> None:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def _journal(msg: str) -> None:
    # journal file requested by you (backend/jobs/journal.py)
    if write_journal is not None:
        try:
            write_journal(msg)
            return
        except Exception:
            pass
    # fallback to local log
    _append_log(f"[{_now_utc()}] JOURNAL {msg}")


def _line_interp(p1: float, p2: float, x1: int, x2: int, xi: int) -> float:
    if x2 == x1:
        return p2
    return p1 + (p2 - p1) * ((xi - x1) / (x2 - x1))


@dataclass(frozen=True)
class TickRow:
    segtick_row_id: int
    tick_id: int
    ts: datetime
    price: float


def break_line(
    segm_id: int,
    segLine_id: Optional[int] = None,
    *,
    price_source: str = "mid",  # "mid" | "kal"
) -> Dict[str, Any]:
    """
    One step of line breaking:
      - if segm has 0 seglines => init root line
      - else split one active line (provided segLine_id or worst by max_abs_dist)

    Dist is computed in tick-order index space (tick_id order).
    IMPORTANT: when splitting, we must use the correct x-span for each child:
      left:  0 .. pivot_index
      right: pivot_index .. (n_old-1)
    """
    if not isinstance(segm_id, int) or segm_id <= 0:
        return {"error": "segm_id must be a positive int"}

    price_source = (price_source or "mid").strip().lower()
    if price_source not in ("mid", "kal"):
        price_source = "mid"

    _journal(f"breakLine started segm_id={segm_id} segLine_id={segLine_id} price_source={price_source}")

    conn = get_conn()
    conn.autocommit = False

    try:
        with dict_cur(conn) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM public.seglines WHERE segm_id=%s", (segm_id,))
            n_lines = int(cur.fetchone()["n"])

        if n_lines == 0:
            out = _init_mode(conn, segm_id, price_source=price_source)
        else:
            out = _split_mode(conn, segm_id, segLine_id, price_source=price_source)

        conn.commit()
        _journal(f"breakLine finished segm_id={segm_id} ok=true action={out.get('action')}")
        return out

    except Exception as e:
        conn.rollback()
        _append_log(f"[{_now_utc()}] ERROR segm_id={segm_id} segLine_id={segLine_id} err={e}")
        _journal(f"breakLine finished segm_id={segm_id} ok=false err={str(e)}")
        return {"error": str(e), "segm_id": segm_id}

    finally:
        try:
            conn.close()
        except Exception:
            pass


# ----------------------------
# Core helpers
# ----------------------------

def _price_sql(price_source: str) -> str:
    # Important: default MID so breaking matches the Mid chart.
    # If kal is null for some rows, we fall back to mid to avoid losing ticks.
    if price_source == "kal":
        return "COALESCE(t.kal, t.mid)"
    return "t.mid"


def _count_ticks(conn, segm_id: int, *, old_line_id: Optional[int] = None) -> int:
    with dict_cur(conn) as cur:
        if old_line_id is None:
            cur.execute("SELECT COUNT(*) AS n FROM public.segticks WHERE segm_id=%s", (segm_id,))
        else:
            cur.execute(
                "SELECT COUNT(*) AS n FROM public.segticks WHERE segm_id=%s AND segline_id=%s",
                (segm_id, old_line_id),
            )
        return int(cur.fetchone()["n"])


def _get_first_last_tick(conn, segm_id: int, *, old_line_id: Optional[int], price_source: str) -> Tuple[TickRow, TickRow]:
    price_expr = _price_sql(price_source)

    where = "st.segm_id=%s"
    params: List[Any] = [segm_id]
    if old_line_id is not None:
        where += " AND st.segline_id=%s"
        params.append(int(old_line_id))

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT st.id AS segtick_row_id, t.id AS tick_id, t.timestamp AS ts, {price_expr} AS price
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE {where}
            ORDER BY t.id ASC
            LIMIT 1
            """,
            tuple(params),
        )
        r1 = cur.fetchone()
        if not r1 or r1["price"] is None:
            raise RuntimeError("no ticks/prices for this selection")

        cur.execute(
            f"""
            SELECT st.id AS segtick_row_id, t.id AS tick_id, t.timestamp AS ts, {price_expr} AS price
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE {where}
            ORDER BY t.id DESC
            LIMIT 1
            """,
            tuple(params),
        )
        r2 = cur.fetchone()
        if not r2 or r2["price"] is None:
            raise RuntimeError("no ticks/prices for this selection")

    return (
        TickRow(int(r1["segtick_row_id"]), int(r1["tick_id"]), r1["ts"], float(r1["price"])),
        TickRow(int(r2["segtick_row_id"]), int(r2["tick_id"]), r2["ts"], float(r2["price"])),
    )


def _iter_ticks_stream(
    conn,
    *,
    segm_id: int,
    old_line_id: Optional[int],
    price_source: str,
) -> Iterable[Tuple[int, int, datetime, float]]:
    """
    Stream ticks in tick-id order without OFFSET.
    Returns tuples: (segtick_row_id, tick_id, ts, price)
    """
    price_expr = _price_sql(price_source)

    where = "st.segm_id=%s"
    params: List[Any] = [segm_id]
    if old_line_id is not None:
        where += " AND st.segline_id=%s"
        params.append(int(old_line_id))

    # Named cursor => server-side streaming (must be inside transaction)
    cur = conn.cursor(name=f"breakline_stream_{segm_id}_{old_line_id or 0}")
    cur.itersize = BATCH_SIZE
    cur.execute(
        f"""
        SELECT st.id AS segtick_row_id, t.id AS tick_id, t.timestamp AS ts, {price_expr} AS price
        FROM public.segticks st
        JOIN public.ticks t ON t.id = st.tick_id
        WHERE {where}
        ORDER BY t.id ASC
        """,
        tuple(params),
    )
    for r in cur:
        segtick_row_id, tick_id, ts, price = r
        if price is None:
            continue
        yield int(segtick_row_id), int(tick_id), ts, float(price)

    cur.close()


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

        cur.execute("SELECT start_ts, end_ts FROM public.seglines WHERE id=%s", (line_id,))
        lr = cur.fetchone()
        dur_ms = None
        if lr:
            dur_ms = int(lr["end_ts"].timestamp() * 1000) - int(lr["start_ts"].timestamp() * 1000)

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
            SELECT COUNT(*) AS n, MAX(max_abs_dist) AS mx
            FROM public.seglines
            WHERE segm_id=%s AND is_active=true
            """,
            (segm_id,),
        )
        r = cur.fetchone()
        n = int(r["n"])
        mx = float(r["mx"]) if r["mx"] is not None else None
    return n, mx


def _get_pivot_index_in_line(conn, segm_id: int, old_line_id: int, pivot_tick_id: int) -> int:
    """
    Returns 0-based index of pivot_tick_id within (segm_id, old_line_id) ticks ordered by tick_id.
    This is required so the child line interpolation uses the correct x-span.
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n_before_or_equal
            FROM public.segticks st
            WHERE st.segm_id=%s AND st.segline_id=%s AND st.tick_id <= %s
            """,
            (segm_id, old_line_id, pivot_tick_id),
        )
        n_be = int(cur.fetchone()["n_before_or_equal"])
    # if pivot exists in this line, count>=1; index is count-1
    return max(0, n_be - 1)


# ----------------------------
# INIT
# ----------------------------

def _init_mode(conn, segm_id: int, *, price_source: str) -> Dict[str, Any]:
    first_t, last_t = _get_first_last_tick(conn, segm_id, old_line_id=None, price_source=price_source)
    n = _count_ticks(conn, segm_id, old_line_id=None)
    if n <= 1:
        raise RuntimeError("segm has <= 1 tick, cannot init line")

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

    # For init, x-span is 0..(n-1)
    _assign_stream_distances(
        conn,
        segm_id=segm_id,
        old_line_id=None,
        new_line_id=line_id,
        x1=0,
        x2=n - 1,
        start_price=first_t.price,
        end_price=last_t.price,
        pivot_tick_id=None,
        left_side=True,  # ignored for init
        price_source=price_source,
    )

    num_ticks, _ = _update_line_stats(conn, line_id)
    num_active, global_max = _update_global_stats(conn, segm_id)

    _append_log(
        f"[{_now_utc()}] segm_id={segm_id} action=init line={line_id} ticks={num_ticks} global_max_abs_dist={global_max}"
    )

    return {
        "segm_id": segm_id,
        "action": "init",
        "segLine_id": line_id,
        "num_lines_active": num_active,
        "global_max_abs_dist": global_max,
    }


# ----------------------------
# SPLIT
# ----------------------------

def _pick_line_to_split(conn, segm_id: int, segLine_id: Optional[int]) -> Optional[Dict[str, Any]]:
    with dict_cur(conn) as cur:
        if segLine_id is not None:
            cur.execute("SELECT * FROM public.seglines WHERE id=%s", (int(segLine_id),))
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


def _pick_pivot_tick(conn, segm_id: int, line_id: int, *, price_source: str) -> Optional[Dict[str, Any]]:
    """
    Pick pivot as a STRUCTURAL extremum:
      - gather top candidates by ABS(dist)
      - choose first that is a local extremum consistent with dist sign:
          dist>0 => local max
          dist<0 => local min
      - fallback to raw max abs(dist) if none pass
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT st.tick_id AS tick_id, st.dist AS dist
            FROM public.segticks st
            WHERE st.segm_id=%s AND st.segline_id=%s
            ORDER BY ABS(st.dist) DESC NULLS LAST
            LIMIT %s
            """,
            (segm_id, line_id, PIVOT_CANDIDATES),
        )
        cands = cur.fetchall()

    best = None
    for c in cands:
        if c["tick_id"] is None or c["dist"] is None:
            continue
        tid = int(c["tick_id"])
        d = float(c["dist"])

        is_max, is_min = _is_local_extremum(
            conn, segm_id, line_id, tid, price_source=price_source, window=PIVOT_WINDOW
        )

        if d > 0 and not is_max:
            continue
        if d < 0 and not is_min:
            continue

        # accept
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT t.id AS tick_id, t.timestamp AS ts, st.dist AS dist
                FROM public.segticks st
                JOIN public.ticks t ON t.id = st.tick_id
                WHERE st.segm_id=%s AND st.segline_id=%s AND st.tick_id=%s
                """,
                (segm_id, line_id, tid),
            )
            best = cur.fetchone()
        if best:
            return best

    # fallback: original behavior
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT st.tick_id AS tick_id, t.timestamp AS ts, st.dist AS dist
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE st.segm_id=%s AND st.segline_id=%s
            ORDER BY ABS(st.dist) DESC NULLS LAST, t.id ASC
            LIMIT 1
            """,
            (segm_id, line_id),
        )
        return cur.fetchone()



def _get_tick_price(conn, tick_id: int, *, price_source: str) -> Tuple[datetime, float]:
    price_expr = _price_sql(price_source)
    with dict_cur(conn) as cur:
        cur.execute(
            f"SELECT t.timestamp AS ts, {price_expr} AS price FROM public.ticks t WHERE t.id=%s",
            (tick_id,),
        )
        r = cur.fetchone()
        if not r or r["price"] is None:
            raise RuntimeError(f"tick not found (or price null): {tick_id}")
        return r["ts"], float(r["price"])


def _create_children_lines(conn, segm_id: int, line: Dict[str, Any], pivot_tick_id: int, *, price_source: str) -> Tuple[int, int]:
    with dict_cur(conn) as cur:
        cur.execute("SELECT COALESCE(MAX(iteration), 0) AS it FROM public.seglines WHERE segm_id=%s", (segm_id,))
        it_next = int(cur.fetchone()["it"]) + 1

    parent_id = int(line["id"])
    depth = int(line["depth"]) + 1

    start_tick_id = int(line["start_tick_id"])
    end_tick_id = int(line["end_tick_id"])

    left_start_ts, left_start_price = _get_tick_price(conn, start_tick_id, price_source=price_source)
    pivot_ts, pivot_price = _get_tick_price(conn, pivot_tick_id, price_source=price_source)
    right_end_ts, right_end_price = _get_tick_price(conn, end_tick_id, price_source=price_source)

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
                start_tick_id, pivot_tick_id,
                left_start_ts, pivot_ts,
                left_start_price, pivot_price,
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
                pivot_tick_id, end_tick_id,
                pivot_ts, right_end_ts,
                pivot_price, right_end_price,
            ),
        )
        right_id = int(cur.fetchone()["id"])

    return left_id, right_id


def _get_line_endpoints(conn, line_id: int) -> Dict[str, Any]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, start_tick_id, end_tick_id, start_price, end_price
            FROM public.seglines
            WHERE id=%s
            """,
            (line_id,),
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError(f"line not found: {line_id}")
        return r


def _assign_stream_distances(
    conn,
    *,
    segm_id: int,
    old_line_id: Optional[int],   # None for init
    new_line_id: int,             # line id to assign for the streamed rows
    x1: int,                      # interpolation x-start in parent index-space
    x2: int,                      # interpolation x-end in parent index-space
    start_price: float,
    end_price: float,
    pivot_tick_id: Optional[int], # used only when old_line_id is not None (split)
    left_side: bool,              # used only in split
    price_source: str,
) -> int:
    """
    Stream ticks and assign segline_id + dist.

    Key point:
      - For init: x1=0, x2=(n-1)
      - For split-left:  x1=0, x2=pivot_index
      - For split-right: x1=pivot_index, x2=(n_old-1)

    We keep xi=i where i is the 0-based index in the parent stream.
    """
    updates: List[Tuple[int, int, float]] = []
    updated = 0

    i = -1
    for segtick_row_id, tick_id, _ts, price in _iter_ticks_stream(
        conn,
        segm_id=segm_id,
        old_line_id=old_line_id,
        price_source=price_source,
    ):
        i += 1

        # split routing:
        if old_line_id is not None and pivot_tick_id is not None:
            if left_side and tick_id > pivot_tick_id:
                continue
            if (not left_side) and tick_id <= pivot_tick_id:
                continue

        phat = _line_interp(start_price, end_price, x1, x2, i)
        dist = price - phat
        updates.append((segtick_row_id, new_line_id, float(dist)))
        updated += 1

        if len(updates) >= BATCH_SIZE:
            _bulk_update_segticks(conn, updates)
            updates.clear()

    if updates:
        _bulk_update_segticks(conn, updates)

    return updated

PIVOT_WINDOW = 25
PIVOT_CANDIDATES = 150

def _get_price_at_tick(conn, tick_id: int, *, price_source: str) -> float:
    _ts, p = _get_tick_price(conn, tick_id, price_source=price_source)
    return float(p)

def _is_local_extremum(conn, segm_id: int, line_id: int, tick_id: int, *, price_source: str, window: int) -> Tuple[bool, bool]:
    """
    Returns (is_local_max, is_local_min) within a +/- window neighborhood, within the SAME segline.
    Uses tick_id ordering.
    """
    price_expr = _price_sql(price_source)

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT t.id AS tick_id, {price_expr} AS price
            FROM public.segticks st
            JOIN public.ticks t ON t.id = st.tick_id
            WHERE st.segm_id=%s AND st.segline_id=%s
              AND t.id BETWEEN %s AND %s
            ORDER BY t.id ASC
            """,
            (segm_id, line_id, tick_id - window, tick_id + window),
        )
        rows = cur.fetchall()

    if not rows or len(rows) < 5:
        return (False, False)

    center_price = None
    prices = []
    for r in rows:
        if r["price"] is None:
            continue
        p = float(r["price"])
        prices.append(p)
        if int(r["tick_id"]) == int(tick_id):
            center_price = p

    if center_price is None or len(prices) < 5:
        return (False, False)

    return (center_price == max(prices), center_price == min(prices))


def _split_mode(conn, segm_id: int, segLine_id: Optional[int], *, price_source: str) -> Dict[str, Any]:
    line = _pick_line_to_split(conn, segm_id, segLine_id)
    if line is None:
        return {"error": "no active segLines to split", "segm_id": segm_id}

    old_line_id = int(line["id"])

    pivot = _pick_pivot_tick(conn, segm_id, old_line_id)
    if pivot is None or pivot["tick_id"] is None:
        return {"error": "no pivot tick found for segLine", "segm_id": segm_id, "segLine_id": old_line_id}

    pivot_tick_id = int(pivot["tick_id"])

    # Count ticks in old line once
    n_old = _count_ticks(conn, segm_id, old_line_id=old_line_id)
    if n_old <= 2:
        raise RuntimeError("old line has <= 2 ticks, cannot split")

    # Determine pivot index within this old line (0-based)
    pivot_index = _get_pivot_index_in_line(conn, segm_id, old_line_id, pivot_tick_id)

    # Guard: if pivot is too close to ends, splitting makes no sense
    if pivot_index <= 0 or pivot_index >= (n_old - 1):
        return {
            "segm_id": segm_id,
            "action": "noop",
            "reason": "pivot_at_endpoint",
            "segLine_id": old_line_id,
            "pivot_tick_id": pivot_tick_id,
            "pivot_index": pivot_index,
            "n_old": n_old,
        }

    # Create children lines
    left_id, right_id = _create_children_lines(conn, segm_id, line, pivot_tick_id, price_source=price_source)

    # Deactivate old
    with dict_cur(conn) as cur:
        cur.execute("UPDATE public.seglines SET is_active=false, updated_at=now() WHERE id=%s", (old_line_id,))

    # Load endpoints for new lines (prices already stored)
    left_line = _get_line_endpoints(conn, left_id)
    right_line = _get_line_endpoints(conn, right_id)

    # Assign distances for left side (<= pivot): interpolate on x=0..pivot_index
    left_updated = _assign_stream_distances(
        conn,
        segm_id=segm_id,
        old_line_id=old_line_id,
        new_line_id=left_id,
        x1=0,
        x2=pivot_index,
        start_price=float(left_line["start_price"]),
        end_price=float(left_line["end_price"]),
        pivot_tick_id=pivot_tick_id,
        left_side=True,
        price_source=price_source,
    )

    # Assign distances for right side (> pivot): interpolate on x=pivot_index..(n_old-1)
    right_updated = _assign_stream_distances(
        conn,
        segm_id=segm_id,
        old_line_id=old_line_id,
        new_line_id=right_id,
        x1=pivot_index,
        x2=n_old - 1,
        start_price=float(right_line["start_price"]),
        end_price=float(right_line["end_price"]),
        pivot_tick_id=pivot_tick_id,
        left_side=False,
        price_source=price_source,
    )

    _update_line_stats(conn, left_id)
    _update_line_stats(conn, right_id)
    num_active, global_max = _update_global_stats(conn, segm_id)

    pivot_abs = float(abs(float(pivot["dist"]))) if pivot.get("dist") is not None else None

    _append_log(
        f"[{_now_utc()}] segm_id={segm_id} action=split split_line={old_line_id} "
        f"pivot_tick={pivot_tick_id} pivot_index={pivot_index} n_old={n_old} pivot_abs_dist={pivot_abs} "
        f"new_left={left_id} new_right={right_id} left_ticks={left_updated} right_ticks={right_updated} "
        f"global_max_abs_dist={global_max}"
    )

    return {
        "segm_id": segm_id,
        "action": "split",
        "segLine_id": old_line_id,
        "pivot_tick_id": pivot_tick_id,
        "pivot_index": pivot_index,
        "pivot_abs_dist": pivot_abs,
        "new_left_id": left_id,
        "new_right_id": right_id,
        "num_lines_active": num_active,
        "global_max_abs_dist": global_max,
    }