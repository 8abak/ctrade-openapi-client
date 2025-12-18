# jobs/buildSegticks.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur

BATCH_INSERT = 20_000
STREAM_ITERSIZE = 50_000


def _cols(conn, table: str) -> List[str]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [r["column_name"] for r in cur.fetchall()]


def _pick(colset: Sequence[str], candidates: Sequence[str], *, table: str, required: bool = True) -> Optional[str]:
    s = set(colset)
    for c in candidates:
        if c in s:
            return c
    if required:
        raise RuntimeError(f"Cannot find required column in {table}. Tried: {candidates}. Have: {list(colset)}")
    return None


def _price_expr(price_source: str, ticks_cols: Sequence[str]) -> str:
    # prefer kal if requested and present, but safely fall back to mid when kal is NULL
    if price_source == "kal" and "kal" in set(ticks_cols) and "mid" in set(ticks_cols):
        return "COALESCE(t.kal, t.mid)"
    if price_source == "kal" and "kal" in set(ticks_cols):
        return "t.kal"
    # default mid
    if "mid" in set(ticks_cols):
        return "t.mid"
    # last resort: try bid/ask average if present
    if "bid" in set(ticks_cols) and "ask" in set(ticks_cols):
        return "(t.bid + t.ask)/2.0"
    raise RuntimeError(f"ticks table has no usable price columns (need mid or kal or bid/ask). cols={ticks_cols}")


def _line_interp(p1: float, p2: float, x1: int, x2: int, xi: int) -> float:
    if x2 == x1:
        return p2
    return p1 + (p2 - p1) * ((xi - x1) / (x2 - x1))


@dataclass
class SegmRow:
    Id: int
    StartTickId: int
    EndTickId: int
    StartTs: datetime
    EndTs: datetime


def _load_segm(conn, segm_id: int) -> SegmRow:
    segms_cols = _cols(conn, "segms")

    c_id = _pick(segms_cols, ["id"], table="segms")
    c_start_tick = _pick(segms_cols, ["start_tick_id", "start_tick", "start_id"], table="segms")
    c_end_tick = _pick(segms_cols, ["end_tick_id", "end_tick", "end_id"], table="segms")
    c_start_ts = _pick(segms_cols, ["start_ts", "start_time", "start_timestamp"], table="segms")
    c_end_ts = _pick(segms_cols, ["end_ts", "end_time", "end_timestamp"], table="segms")

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT {c_id} AS id,
                   {c_start_tick} AS start_tick_id,
                   {c_end_tick} AS end_tick_id,
                   {c_start_ts} AS start_ts,
                   {c_end_ts} AS end_ts
            FROM public.segms
            WHERE {c_id}=%s
            """,
            (segm_id,),
        )
        r = cur.fetchone()
        if not r:
            raise RuntimeError(f"segm not found: {segm_id}")

    return SegmRow(
        Id=int(r["id"]),
        StartTickId=int(r["start_tick_id"]),
        EndTickId=int(r["end_tick_id"]),
        StartTs=r["start_ts"],
        EndTs=r["end_ts"],
    )


def _count_ticks_in_range(conn, start_tick_id: int, end_tick_id: int) -> int:
    with dict_cur(conn) as cur:
        cur.execute("SELECT COUNT(*) AS n FROM public.ticks WHERE id BETWEEN %s AND %s", (start_tick_id, end_tick_id))
        return int(cur.fetchone()["n"])


def _get_tick_price(conn, tick_id: int, price_source: str) -> Tuple[datetime, float]:
    ticks_cols = _cols(conn, "ticks")
    price = _price_expr(price_source, ticks_cols)
    with dict_cur(conn) as cur:
        cur.execute(f"SELECT timestamp AS ts, {price} AS price FROM public.ticks t WHERE t.id=%s", (tick_id,))
        r = cur.fetchone()
        if not r or r["price"] is None:
            raise RuntimeError(f"tick not found or price NULL: {tick_id}")
        return r["ts"], float(r["price"])


def _existing_root_line(conn, segm_id: int) -> Optional[int]:
    seglines_cols = _cols(conn, "seglines")
    c_id = _pick(seglines_cols, ["id"], table="seglines")
    c_segm = _pick(seglines_cols, ["segm_id", "segm"], table="seglines")
    c_depth = _pick(seglines_cols, ["depth"], table="seglines", required=False)
    c_parent = _pick(seglines_cols, ["parent_id", "parent"], table="seglines", required=False)

    where = [f"{c_segm}=%s"]
    params: List[Any] = [segm_id]
    if c_depth:
        where.append(f"{c_depth}=0")
    if c_parent:
        where.append(f"{c_parent} IS NULL")

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT {c_id} AS id
            FROM public.seglines
            WHERE {" AND ".join(where)}
            ORDER BY {c_id} ASC
            LIMIT 1
            """,
            tuple(params),
        )
        r = cur.fetchone()
        return int(r["id"]) if r else None


def _insert_root_line(conn, segm_id: int, segm: SegmRow, price_source: str) -> int:
    seglines_cols = _cols(conn, "seglines")
    ticks_cols = _cols(conn, "ticks")

    c_id = _pick(seglines_cols, ["id"], table="seglines")
    c_segm = _pick(seglines_cols, ["segm_id", "segm"], table="seglines")
    c_parent = _pick(seglines_cols, ["parent_id", "parent"], table="seglines", required=False)
    c_depth = _pick(seglines_cols, ["depth"], table="seglines", required=False)
    c_iter = _pick(seglines_cols, ["iteration", "iter"], table="seglines", required=False)

    c_start_tick = _pick(seglines_cols, ["start_tick_id", "start_tick"], table="seglines")
    c_end_tick = _pick(seglines_cols, ["end_tick_id", "end_tick"], table="seglines")
    c_start_ts = _pick(seglines_cols, ["start_ts", "start_time", "start_timestamp"], table="seglines")
    c_end_ts = _pick(seglines_cols, ["end_ts", "end_time", "end_timestamp"], table="seglines")

    c_start_price = _pick(seglines_cols, ["start_price"], table="seglines")
    c_end_price = _pick(seglines_cols, ["end_price"], table="seglines")

    c_is_active = _pick(seglines_cols, ["is_active", "active"], table="seglines", required=False)

    # endpoint prices
    _sts, p1 = _get_tick_price(conn, segm.StartTickId, price_source)
    _ets, p2 = _get_tick_price(conn, segm.EndTickId, price_source)

    cols: List[str] = []
    vals: List[Any] = []

    cols.append(c_segm); vals.append(segm_id)
    if c_parent:
        cols.append(c_parent); vals.append(None)
    if c_depth:
        cols.append(c_depth); vals.append(0)
    if c_iter:
        cols.append(c_iter); vals.append(0)

    cols += [c_start_tick, c_end_tick, c_start_ts, c_end_ts, c_start_price, c_end_price]
    vals += [segm.StartTickId, segm.EndTickId, segm.StartTs, segm.EndTs, p1, p2]

    if c_is_active:
        cols.append(c_is_active); vals.append(True)

    cols_sql = ", ".join(cols)
    ph_sql = ", ".join(["%s"] * len(cols))

    with dict_cur(conn) as cur:
        cur.execute(f"INSERT INTO public.seglines ({cols_sql}) VALUES ({ph_sql}) RETURNING {c_id} AS id", tuple(vals))
        return int(cur.fetchone()["id"])


def _delete_for_force(conn, segm_id: int) -> None:
    segticks_cols = _cols(conn, "segticks")
    seglines_cols = _cols(conn, "seglines")

    c_segticks_segm = _pick(segticks_cols, ["segm_id", "segm"], table="segticks")
    c_seglines_segm = _pick(seglines_cols, ["segm_id", "segm"], table="seglines")

    with dict_cur(conn) as cur:
        cur.execute(f"DELETE FROM public.segticks WHERE {c_segticks_segm}=%s", (segm_id,))
        cur.execute(f"DELETE FROM public.seglines WHERE {c_seglines_segm}=%s", (segm_id,))


def _stream_ticks(conn, start_tick_id: int, end_tick_id: int, price_source: str) -> Iterable[Tuple[int, datetime, float]]:
    ticks_cols = _cols(conn, "ticks")
    price = _price_expr(price_source, ticks_cols)

    cur = conn.cursor(name=f"buildsegticks_{start_tick_id}_{end_tick_id}")
    cur.itersize = STREAM_ITERSIZE
    cur.execute(
        f"""
        SELECT t.id AS id, t.timestamp AS ts, {price} AS price
        FROM public.ticks t
        WHERE t.id BETWEEN %s AND %s
        ORDER BY t.id ASC
        """,
        (start_tick_id, end_tick_id),
    )

    for (tid, ts, pr) in cur:
        if pr is None:
            continue
        yield int(tid), ts, float(pr)

    cur.close()


def _bulk_insert_segticks(
    conn,
    segm_id: int,
    segline_id: int,
    rows: List[Tuple[int, datetime, float]],
    *,
    start_price: float,
    end_price: float,
    n_ticks_total: int,
    segticks_cols: Sequence[str],
) -> Tuple[int, float]:
    """
    rows: list of (tick_id, ts, price) in order.
    Returns: (inserted_count, local_max_abs_dist)
    """

    c_tick = _pick(segticks_cols, ["tick_id", "tick"], table="segticks")
    c_segm = _pick(segticks_cols, ["segm_id", "segm"], table="segticks")
    c_segline = _pick(segticks_cols, ["segline_id", "segline"], table="segticks")
    c_dist = _pick(segticks_cols, ["dist"], table="segticks", required=False)

    # Insert order-index interpolation uses global index; caller passes rows already sequential,
    # but we compute xi based on "running index" outside—so we include xi in caller by passing
    # it through the list position offset if needed. To keep it simple, we attach xi as we insert.
    # We will assume caller sets a module-global "CurrentIndex" by packing it into rows via closure.
    raise RuntimeError("internal wiring error")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, default=None, help="Process only this segm id")
    ap.add_argument("--force", action="store_true", help="Delete existing segticks+seglines for segm(s) before rebuild")
    ap.add_argument("--price-source", type=str, default="mid", choices=["mid", "kal"], help="Price source for dist")
    args = ap.parse_args()

    price_source = (args.price_source or "mid").strip().lower()

    conn = get_conn()
    conn.autocommit = False

    try:
        # Determine segms to process
        if args.segm_id is not None:
            segm_ids = [int(args.segm_id)]
        else:
            # process all segms newest->oldest that don't yet have segticks
            segms_cols = _cols(conn, "segms")
            segticks_cols = _cols(conn, "segticks")

            c_segm_id = _pick(segms_cols, ["id"], table="segms")
            c_segticks_segm = _pick(segticks_cols, ["segm_id", "segm"], table="segticks")

            with dict_cur(conn) as cur:
                cur.execute(
                    f"""
                    SELECT s.{c_segm_id} AS id
                    FROM public.segms s
                    LEFT JOIN (
                        SELECT {c_segticks_segm} AS segm_key, COUNT(*) AS n
                        FROM public.segticks
                        GROUP BY {c_segticks_segm}
                    ) st ON st.segm_key = s.{c_segm_id}
                    WHERE COALESCE(st.n, 0)=0
                    ORDER BY s.{c_segm_id} DESC
                    """
                )
                segm_ids = [int(r["id"]) for r in cur.fetchall()]

        print(f"[buildSegticks] segms to process: {', '.join(map(str, segm_ids)) if segm_ids else '(none)'}")

        for idx, segm_id in enumerate(segm_ids, start=1):
            segm = _load_segm(conn, segm_id)

            if args.force:
                print(f"[segm {idx}] force delete segticks+seglines for segm={segm_id}")
                _delete_for_force(conn, segm_id)
                conn.commit()

            # root line
            root_id = _existing_root_line(conn, segm_id)
            if root_id is None:
                print(f"[segm {idx}] building root segLine for segm={segm_id}")
                root_id = _insert_root_line(conn, segm_id, segm, price_source)
                conn.commit()
            else:
                print(f"[segm {idx}] root segLine exists: {root_id}")

            # tick count for interpolation denominator
            n_total = _count_ticks_in_range(conn, segm.StartTickId, segm.EndTickId)
            if n_total <= 1:
                print(f"[segm {idx}] skip segm={segm_id} (ticks={n_total})")
                continue

            # endpoints for interpolation
            _ts1, p1 = _get_tick_price(conn, segm.StartTickId, price_source)
            _ts2, p2 = _get_tick_price(conn, segm.EndTickId, price_source)

            segticks_cols = _cols(conn, "segticks")
            c_tick = _pick(segticks_cols, ["tick_id", "tick"], table="segticks")
            c_segm = _pick(segticks_cols, ["segm_id", "segm"], table="segticks")
            c_segline = _pick(segticks_cols, ["segline_id", "segline"], table="segticks")
            c_dist = _pick(segticks_cols, ["dist"], table="segticks", required=False)

            # If segticks already populated for this segm, skip unless --force
            with dict_cur(conn) as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM public.segticks WHERE {c_segm}=%s", (segm_id,))
                already = int(cur.fetchone()["n"])
            if already > 0 and not args.force:
                print(f"[segm {idx}] segticks already exist for segm={segm_id} (n={already}) -> skip (use --force)")
                continue

            print(f"[segm {idx}] inserting segticks segm={segm_id} ticks={n_total} in chunks...")

            # Stream ticks and insert rows in batches
            inserts: List[Tuple[Any, ...]] = []
            max_abs = 0.0
            x1 = 0
            x2 = n_total - 1
            i_global = -1

            # Use one transaction for streaming + inserting; commit per batch to reduce locks
            for tick_id, ts, price in _stream_ticks(conn, segm.StartTickId, segm.EndTickId, price_source):
                i_global += 1

                phat = _line_interp(p1, p2, x1, x2, i_global)
                dist = price - phat
                if abs(dist) > max_abs:
                    max_abs = abs(dist)

                if c_dist:
                    inserts.append((segm_id, tick_id, root_id, float(dist)))
                else:
                    inserts.append((segm_id, tick_id, root_id))

                if len(inserts) >= BATCH_INSERT:
                    with dict_cur(conn) as cur:
                        if c_dist:
                            psycopg2.extras.execute_values(
                                cur,
                                f"""
                                INSERT INTO public.segticks ({c_segm}, {c_tick}, {c_segline}, {c_dist})
                                VALUES %s
                                """,
                                inserts,
                                template="(%s,%s,%s,%s)",
                                page_size=10_000,
                            )
                        else:
                            psycopg2.extras.execute_values(
                                cur,
                                f"""
                                INSERT INTO public.segticks ({c_segm}, {c_tick}, {c_segline})
                                VALUES %s
                                """,
                                inserts,
                                template="(%s,%s,%s)",
                                page_size=10_000,
                            )
                    conn.commit()
                    inserts.clear()

            if inserts:
                with dict_cur(conn) as cur:
                    if c_dist:
                        psycopg2.extras.execute_values(
                            cur,
                            f"""
                            INSERT INTO public.segticks ({c_segm}, {c_tick}, {c_segline}, {c_dist})
                            VALUES %s
                            """,
                            inserts,
                            template="(%s,%s,%s,%s)",
                            page_size=10_000,
                        )
                    else:
                        psycopg2.extras.execute_values(
                            cur,
                            f"""
                            INSERT INTO public.segticks ({c_segm}, {c_tick}, {c_segline})
                            VALUES %s
                            """,
                            inserts,
                            template="(%s,%s,%s)",
                            page_size=10_000,
                        )
                conn.commit()
                inserts.clear()

            # Update seglines stats if those columns exist
            seglines_cols = _cols(conn, "seglines")
            c_num_ticks = _pick(seglines_cols, ["num_ticks"], table="seglines", required=False)
            c_max_abs = _pick(seglines_cols, ["max_abs_dist"], table="seglines", required=False)
            c_updated = _pick(seglines_cols, ["updated_at"], table="seglines", required=False)

            if c_num_ticks or c_max_abs or c_updated:
                sets = []
                params: List[Any] = []
                if c_num_ticks:
                    sets.append(f"{c_num_ticks}=%s"); params.append(n_total)
                if c_max_abs:
                    sets.append(f"{c_max_abs}=%s"); params.append(float(max_abs))
                if c_updated:
                    sets.append(f"{c_updated}=now()")

                params.append(root_id)
                with dict_cur(conn) as cur:
                    cur.execute(
                        f"UPDATE public.seglines SET {', '.join(sets)} WHERE id=%s",
                        tuple(params),
                    )
                conn.commit()

            print(f"[segm {idx}] done segm={segm_id} root_line={root_id} max_abs_dist≈{max_abs:.4f}")

        print("[buildSegticks] complete")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
