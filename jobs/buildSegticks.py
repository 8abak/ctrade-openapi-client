# jobs/buildSegticks.py
from __future__ import annotations

import argparse
from typing import List, Tuple
from datetime import datetime

import psycopg2
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
                FROM segms
                WHERE id=%s
            """, (segm_id,))
            segm = cur.fetchone()

        if not segm:
            raise RuntimeError("segm not found")

        symbol = segm["symbol"]
        session_id = segm["session_id"]
        start_tid = segm["start_tick_id"]
        end_tid = segm["end_tick_id"]
        start_ts = segm["start_ts"]
        end_ts = segm["end_ts"]

        duration_sec = (end_ts - start_ts).total_seconds()
        run_id = f"root-segline:{segm_id}"

        # ------------------------------------------------------------
        # FORCE cleanup
        # ------------------------------------------------------------
        if args.force:
            with dict_cur(conn) as cur:
                cur.execute("DELETE FROM segticks WHERE segm_id=%s", (segm_id,))
                cur.execute("DELETE FROM seglines WHERE segm_id=%s", (segm_id,))
            conn.commit()

        # ------------------------------------------------------------
        # Endpoint prices
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute(f"""
                SELECT id, {price_expr} AS price
                FROM ticks
                WHERE id IN (%s, %s)
                ORDER BY id
            """, (start_tid, end_tid))
            r = cur.fetchall()

        p1, p2 = float(r[0]["price"]), float(r[1]["price"])
        slope = (p2 - p1) / duration_sec if duration_sec > 0 else 0.0
        price_change = p2 - p1

        # ------------------------------------------------------------
        # Create root segLine
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute("""
                INSERT INTO seglines (
                    segm_id, parent_id, depth, iteration,
                    start_tick_id, end_tick_id,
                    start_ts, end_ts,
                    start_price, end_price,
                    is_active
                )
                VALUES (%s,NULL,0,0,%s,%s,%s,%s,%s,%s,true)
                RETURNING id
            """, (segm_id, start_tid, end_tid, start_ts, end_ts, p1, p2))
            segline_id = cur.fetchone()["id"]

        conn.commit()
        print(f"[buildSegticks] root segLine created id={segline_id}")

        # ------------------------------------------------------------
        # Stream ticks and insert segticks
        # ------------------------------------------------------------
        cur_stream = conn.cursor(name="segticks_stream")
        cur_stream.itersize = STREAM_ITERSIZE
        cur_stream.execute(f"""
            SELECT t.id, t.timestamp, {price_expr}
            FROM ticks t
            WHERE t.id BETWEEN %s AND %s
            ORDER BY t.id
        """, (start_tid, end_tid))

        rows: List[Tuple] = []
        max_abs_dist = 0.0

        for tick_id, ts, price in cur_stream:
            if price is None:
                continue

            t_rel = (ts - start_ts).total_seconds()
            seg_pos = min(1.0, max(0.0, t_rel / duration_sec if duration_sec > 0 else 0.0))
            projected = interp(p1, p2, t_rel, duration_sec)
            dist = float(price - projected)
            max_abs_dist = max(max_abs_dist, abs(dist))

            rows.append((
                symbol,
                tick_id,
                segm_id,
                session_id,
                seg_pos,
                slope,
                price_change,
                duration_sec,
                run_id,
                segline_id,
                dist,
            ))

            if len(rows) >= BATCH_SIZE:
                with dict_cur(conn) as cur:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO segticks (
                            symbol,
                            tick_id,
                            segm_id,
                            session_id,
                            seg_pos,
                            seg_slope,
                            seg_price_change,
                            seg_duration_seconds,
                            run_id,
                            segline_id,
                            dist
                        )
                        VALUES %s
                    """, rows, page_size=10_000)
                conn.commit()
                rows.clear()

        if rows:
            with dict_cur(conn) as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO segticks (
                        symbol, tick_id, segm_id, session_id,
                        seg_pos, seg_slope, seg_price_change,
                        seg_duration_seconds, run_id,
                        segline_id, dist
                    )
                    VALUES %s
                """, rows, page_size=10_000)
            conn.commit()

        cur_stream.close()

        # ------------------------------------------------------------
        # Update segLine stats
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute("""
                UPDATE seglines
                SET
                    num_ticks = (
                        SELECT COUNT(*) FROM segticks WHERE segline_id=%s
                    ),
                    duration_ms = %s,
                    max_abs_dist = %s,
                    updated_at = now()
                WHERE id=%s
            """, (
                segline_id,
                int(duration_sec * 1000),
                max_abs_dist,
                segline_id,
            ))

        conn.commit()
        print(f"[buildSegticks] DONE segm={segm_id} max_abs_dist={max_abs_dist}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
