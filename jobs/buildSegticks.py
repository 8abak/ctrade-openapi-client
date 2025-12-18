# jobs/buildSegticks.py
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, required=True)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--price-source", choices=["mid", "kal"], default="mid")
    args = ap.parse_args()

    segm_id = args.segm_id
    price_expr = "t.mid" if args.price_source == "mid" else "COALESCE(t.kal, t.mid)"

    conn = get_conn()
    conn.autocommit = False

    try:
        # ------------------------------------------------------------
        # Load segm
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute("""
                SELECT
                    id, symbol, session_id,
                    start_tick_id, end_tick_id,
                    start_ts, end_ts
                FROM public.segms
                WHERE id=%s
            """, (segm_id,))
            segm = cur.fetchone()

        if not segm:
            raise RuntimeError("segm not found")

        symbol = segm["symbol"]
        session_id = segm["session_id"]
        start_tid = int(segm["start_tick_id"])
        end_tid = int(segm["end_tick_id"])
        start_ts = segm["start_ts"]
        end_ts = segm["end_ts"]

        duration_sec = (end_ts - start_ts).total_seconds()
        run_id = f"root-segline:{segm_id}"

        # ------------------------------------------------------------
        # FORCE cleanup
        # ------------------------------------------------------------
        if args.force:
            with dict_cur(conn) as cur:
                cur.execute("DELETE FROM public.segticks WHERE segm_id=%s", (segm_id,))
                cur.execute("DELETE FROM public.seglines WHERE segm_id=%s", (segm_id,))

        # ------------------------------------------------------------
        # Endpoint prices (make sure ticks has alias t)
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute(f"""
                SELECT t.id, {price_expr} AS price
                FROM public.ticks t
                WHERE t.id IN (%s, %s)
                ORDER BY t.id
            """, (start_tid, end_tid))
            r = cur.fetchall()

        if len(r) != 2 or r[0]["price"] is None or r[1]["price"] is None:
            raise RuntimeError("cannot read endpoint prices")

        p1 = float(r[0]["price"])
        p2 = float(r[1]["price"])

        slope = (p2 - p1) / duration_sec if duration_sec > 0 else 0.0
        price_change = p2 - p1

        # ------------------------------------------------------------
        # Create root segLine (DO NOT COMMIT yet)
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute("""
                INSERT INTO public.seglines (
                    segm_id, parent_id, depth, iteration,
                    start_tick_id, end_tick_id,
                    start_ts, end_ts,
                    start_price, end_price,
                    is_active
                )
                VALUES (%s,NULL,0,0,%s,%s,%s,%s,%s,%s,true)
                RETURNING id
            """, (segm_id, start_tid, end_tid, start_ts, end_ts, p1, p2))
            segline_id = int(cur.fetchone()["id"])

        print(f"[buildSegticks] root segLine created id={segline_id}")

        # ------------------------------------------------------------
        # Stream ticks (named cursor must stay in SAME transaction)
        # No commits during iteration.
        # ------------------------------------------------------------
        cur_stream = conn.cursor(name=f"segticks_stream_{segm_id}")
        cur_stream.itersize = STREAM_ITERSIZE
        cur_stream.execute(f"""
            SELECT t.id, t.timestamp, {price_expr} AS price
            FROM public.ticks t
            WHERE t.id BETWEEN %s AND %s
            ORDER BY t.id
        """, (start_tid, end_tid))

        rows: List[Tuple] = []
        max_abs_dist = 0.0
        n_inserted = 0

        for tick_id, ts, price in cur_stream:
            if price is None:
                continue

            t_rel = (ts - start_ts).total_seconds()
            seg_pos = 0.0
            if duration_sec > 0:
                seg_pos = t_rel / duration_sec
                if seg_pos < 0.0:
                    seg_pos = 0.0
                elif seg_pos > 1.0:
                    seg_pos = 1.0

            projected = interp(p1, p2, t_rel, duration_sec)
            dist = float(price - projected)
            if abs(dist) > max_abs_dist:
                max_abs_dist = abs(dist)

            rows.append((
                symbol,
                int(tick_id),
                segm_id,
                int(session_id),
                float(seg_pos),
                float(slope),
                float(price_change),
                float(duration_sec),
                run_id,
                int(segline_id),
                float(dist),
            ))

            if len(rows) >= BATCH_SIZE:
                with dict_cur(conn) as cur:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO public.segticks (
                            symbol, tick_id, segm_id, session_id,
                            seg_pos, seg_slope, seg_price_change,
                            seg_duration_seconds, run_id,
                            segline_id, dist
                        )
                        VALUES %s
                    """, rows, page_size=10_000)
                n_inserted += len(rows)
                rows.clear()

                # progress without committing
                if n_inserted % 200_000 == 0:
                    print(f"[buildSegticks] inserted {n_inserted} rows...")

        if rows:
            with dict_cur(conn) as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO public.segticks (
                        symbol, tick_id, segm_id, session_id,
                        seg_pos, seg_slope, seg_price_change,
                        seg_duration_seconds, run_id,
                        segline_id, dist
                    )
                    VALUES %s
                """, rows, page_size=10_000)
            n_inserted += len(rows)
            rows.clear()

        cur_stream.close()

        # ------------------------------------------------------------
        # Update segLine stats
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute("""
                UPDATE public.seglines
                SET
                    num_ticks = %s,
                    duration_ms = %s,
                    max_abs_dist = %s,
                    updated_at = now()
                WHERE id=%s
            """, (
                n_inserted,
                int(duration_sec * 1000),
                float(max_abs_dist),
                int(segline_id),
            ))

        # One commit at the end keeps named cursor valid throughout
        conn.commit()

        print(f"[buildSegticks] DONE segm={segm_id} inserted={n_inserted} max_abs_dist={max_abs_dist}")

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
