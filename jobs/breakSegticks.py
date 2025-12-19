# jobs/breakSegticks.py
from __future__ import annotations

import argparse
from typing import List, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur

BATCH_SIZE = 20_000
STREAM_ITERSIZE = 50_000


def interp(p1: float, p2: float, t: float, T: float) -> float:
    if T <= 0:
        return p1
    return p1 + (p2 - p1) * (t / T)


def _load_segm(conn, segm_id: int) -> dict:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT
                id, symbol, session_id,
                start_tick_id, end_tick_id,
                start_ts, end_ts
            FROM public.segms
            WHERE id=%s
            """,
            (segm_id,),
        )
        segm = cur.fetchone()
    if not segm:
        raise RuntimeError("segm not found")
    return segm


def _check_existing(conn, segm_id: int) -> None:
    with dict_cur(conn) as cur:
        cur.execute("SELECT COUNT(*) AS n FROM public.segticks WHERE segm_id=%s", (segm_id,))
        n_ticks = int(cur.fetchone()["n"])
        cur.execute("SELECT COUNT(*) AS n FROM public.seglines WHERE segm_id=%s", (segm_id,))
        n_lines = int(cur.fetchone()["n"])
    if n_ticks > 0 or n_lines > 0:
        raise RuntimeError(
            f"segticks/seglines already exist for segm_id={segm_id}, use --force to rebuild"
        )


def _load_endpoint_prices(conn, start_tid: int, end_tid: int, price_expr: str) -> Tuple[float, float]:
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT t.id, {price_expr} AS price
            FROM public.ticks t
            WHERE t.id IN (%s, %s)
            ORDER BY t.id
            """,
            (start_tid, end_tid),
        )
        rows = cur.fetchall()
    if len(rows) != 2 or rows[0]["price"] is None or rows[1]["price"] is None:
        raise RuntimeError("cannot read endpoint prices")
    return float(rows[0]["price"]), float(rows[1]["price"])


def _stream_ticks(conn, start_tid: int, end_tid: int, price_expr: str, segm_id: int) -> List[Tuple[int, object, float]]:
    cur_stream = conn.cursor(name=f"segticks_stream_{segm_id}")
    cur_stream.itersize = STREAM_ITERSIZE
    cur_stream.execute(
        f"""
        SELECT t.id, t.timestamp, {price_expr} AS price
        FROM public.ticks t
        WHERE t.id BETWEEN %s AND %s
        ORDER BY t.id
        """,
        (start_tid, end_tid),
    )

    ticks: List[Tuple[int, object, float]] = []
    for tick_id, ts, price in cur_stream:
        if price is None:
            continue
        ticks.append((int(tick_id), ts, float(price)))

    cur_stream.close()
    return ticks


def _insert_segline(
    conn,
    *,
    segm_id: int,
    start_tid: int,
    end_tid: int,
    start_ts,
    end_ts,
    start_price: float,
    end_price: float,
) -> int:
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
            VALUES (%s,NULL,0,0,%s,%s,%s,%s,%s,%s,true)
            RETURNING id
            """,
            (segm_id, start_tid, end_tid, start_ts, end_ts, start_price, end_price),
        )
        return int(cur.fetchone()["id"])


def _update_line_stats(conn, line_id: int) -> Tuple[int, float]:
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
        mx = float(r["mx"]) if r["mx"] is not None else 0.0

        cur.execute("SELECT start_ts, end_ts FROM public.seglines WHERE id=%s", (line_id,))
        lr = cur.fetchone()
        dur_ms = 0
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, required=True)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--price-source", choices=["mid", "kal"], default="mid")
    args = ap.parse_args()

    segm_id = args.segm_id
    price_expr = "t.mid" if args.price_source == "mid" else "COALESCE(t.kal, t.mid)"
    run_id = f"break-segticks:{segm_id}:{args.price_source}"

    conn = get_conn()
    conn.autocommit = False

    try:
        segm = _load_segm(conn, segm_id)
        symbol = segm["symbol"]
        session_id = int(segm["session_id"])
        start_tid = int(segm["start_tick_id"])
        end_tid = int(segm["end_tick_id"])
        start_ts = segm["start_ts"]
        end_ts = segm["end_ts"]

        if args.force:
            with dict_cur(conn) as cur:
                cur.execute("DELETE FROM public.segticks WHERE segm_id=%s", (segm_id,))
                cur.execute("DELETE FROM public.seglines WHERE segm_id=%s", (segm_id,))
        else:
            _check_existing(conn, segm_id)

        ticks = _stream_ticks(conn, start_tid, end_tid, price_expr, segm_id)
        if len(ticks) <= 1:
            raise RuntimeError("segm has <= 1 priced tick")

        min_tick = None
        max_tick = None
        for tick_id, ts, price in ticks:
            if min_tick is None or price < min_tick[2] or (price == min_tick[2] and tick_id < min_tick[0]):
                min_tick = (tick_id, ts, price)
            if max_tick is None or price > max_tick[2] or (price == max_tick[2] and tick_id < max_tick[0]):
                max_tick = (tick_id, ts, price)

        if min_tick is None or max_tick is None:
            raise RuntimeError("extremes not found")

        min_tid, min_ts, min_price = min_tick
        max_tid, max_ts, max_price = max_tick

        if min_tid <= max_tid:
            first_tid, first_ts, first_price = min_tid, min_ts, min_price
            second_tid, second_ts, second_price = max_tid, max_ts, max_price
        else:
            first_tid, first_ts, first_price = max_tid, max_ts, max_price
            second_tid, second_ts, second_price = min_tid, min_ts, min_price

        if not (start_tid <= first_tid <= second_tid <= end_tid):
            raise RuntimeError("extreme tick order violates segm bounds")

        start_price, end_price = _load_endpoint_prices(conn, start_tid, end_tid, price_expr)

        segline_a_id = _insert_segline(
            conn,
            segm_id=segm_id,
            start_tid=start_tid,
            end_tid=first_tid,
            start_ts=start_ts,
            end_ts=first_ts,
            start_price=start_price,
            end_price=first_price,
        )

        segline_b_id = _insert_segline(
            conn,
            segm_id=segm_id,
            start_tid=first_tid,
            end_tid=second_tid,
            start_ts=first_ts,
            end_ts=second_ts,
            start_price=first_price,
            end_price=second_price,
        )

        segline_c_id = _insert_segline(
            conn,
            segm_id=segm_id,
            start_tid=second_tid,
            end_tid=end_tid,
            start_ts=second_ts,
            end_ts=end_ts,
            start_price=second_price,
            end_price=end_price,
        )

        lines = [
            {
                "id": segline_a_id,
                "start_tid": start_tid,
                "end_tid": first_tid,
                "start_ts": start_ts,
                "end_ts": first_ts,
                "start_price": start_price,
                "end_price": first_price,
            },
            {
                "id": segline_b_id,
                "start_tid": first_tid,
                "end_tid": second_tid,
                "start_ts": first_ts,
                "end_ts": second_ts,
                "start_price": first_price,
                "end_price": second_price,
            },
            {
                "id": segline_c_id,
                "start_tid": second_tid,
                "end_tid": end_tid,
                "start_ts": second_ts,
                "end_ts": end_ts,
                "start_price": second_price,
                "end_price": end_price,
            },
        ]

        for line in lines:
            duration_sec = (line["end_ts"] - line["start_ts"]).total_seconds()
            if duration_sec < 0:
                duration_sec = 0.0
            line["duration_sec"] = float(duration_sec)
            line["price_change"] = float(line["end_price"] - line["start_price"])
            if duration_sec > 0:
                line["slope"] = float(line["price_change"] / duration_sec)
            else:
                line["slope"] = 0.0

        rows: List[Tuple] = []
        n_inserted = 0

        for tick_id, ts, price in ticks:
            if tick_id <= first_tid:
                line = lines[0]
            elif tick_id <= second_tid:
                line = lines[1]
            else:
                line = lines[2]

            t_rel = (ts - line["start_ts"]).total_seconds()
            duration_sec = line["duration_sec"]
            seg_pos = 0.0
            if duration_sec > 0:
                seg_pos = t_rel / duration_sec
                if seg_pos < 0.0:
                    seg_pos = 0.0
                elif seg_pos > 1.0:
                    seg_pos = 1.0

            projected = interp(line["start_price"], line["end_price"], t_rel, duration_sec)
            dist = float(price - projected)

            rows.append(
                (
                    symbol,
                    int(tick_id),
                    segm_id,
                    session_id,
                    float(seg_pos),
                    float(line["slope"]),
                    float(line["price_change"]),
                    float(duration_sec),
                    run_id,
                    int(line["id"]),
                    float(dist),
                )
            )

            if len(rows) >= BATCH_SIZE:
                with dict_cur(conn) as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO public.segticks (
                            symbol, tick_id, segm_id, session_id,
                            seg_pos, seg_slope, seg_price_change,
                            seg_duration_seconds, run_id,
                            segline_id, dist
                        )
                        VALUES %s
                        """,
                        rows,
                        page_size=10_000,
                    )
                n_inserted += len(rows)
                rows.clear()

                if n_inserted % 200_000 == 0:
                    print(f"[breakSegticks] inserted {n_inserted} rows...")

        if rows:
            with dict_cur(conn) as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO public.segticks (
                        symbol, tick_id, segm_id, session_id,
                        seg_pos, seg_slope, seg_price_change,
                        seg_duration_seconds, run_id,
                        segline_id, dist
                    )
                    VALUES %s
                    """,
                    rows,
                    page_size=10_000,
                )
            n_inserted += len(rows)
            rows.clear()

        num_a, max_a = _update_line_stats(conn, segline_a_id)
        num_b, max_b = _update_line_stats(conn, segline_b_id)
        num_c, max_c = _update_line_stats(conn, segline_c_id)

        worst = max(max_a, max_b, max_c)
        conn.commit()

        print(
            f"[breakSegticks] DONE segm={segm_id} lines=3 ticks={n_inserted} worst={worst} "
            f"(a={num_a}, b={num_b}, c={num_c})"
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
