# jobs/buildSegticks.py
from __future__ import annotations

import argparse
from datetime import datetime
from typing import List, Tuple

import psycopg2
import psycopg2.extras

from backend.db import get_conn, dict_cur

BATCH_SIZE = 20_000
STREAM_ITERSIZE = 50_000


def line_interp(p1: float, p2: float, i: int, n: int) -> float:
    """Linear interpolation on index space [0..n-1]."""
    if n <= 1:
        return p1
    return p1 + (p2 - p1) * (i / (n - 1))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, required=True)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--price-source", choices=["mid", "kal"], default="mid")
    args = ap.parse_args()

    segm_id = args.segm_id
    price_source = args.price_source

    conn = get_conn()
    conn.autocommit = False

    try:
        with dict_cur(conn) as cur:
            # ------------------------------------------------------------
            # 1. Load segm (authoritative source)
            # ------------------------------------------------------------
            cur.execute(
                """
                SELECT
                    id,
                    symbol,
                    start_tick_id,
                    end_tick_id,
                    start_ts,
                    end_ts
                FROM public.segms
                WHERE id = %s
                """,
                (segm_id,),
            )
            segm = cur.fetchone()
            if not segm:
                raise RuntimeError(f"segm {segm_id} not found")

            Symbol = segm["symbol"]
            StartTickId = int(segm["start_tick_id"])
            EndTickId = int(segm["end_tick_id"])
            StartTs = segm["start_ts"]
            EndTs = segm["end_ts"]

        # ------------------------------------------------------------
        # 2. FORCE cleanup if requested
        # ------------------------------------------------------------
        if args.force:
            with dict_cur(conn) as cur:
                cur.execute("DELETE FROM public.segticks WHERE segm_id = %s", (segm_id,))
                cur.execute("DELETE FROM public.seglines WHERE segm_id = %s", (segm_id,))
            conn.commit()

        # ------------------------------------------------------------
        # 3. Get endpoint prices
        # ------------------------------------------------------------
        price_expr = "t.mid" if price_source == "mid" else "COALESCE(t.kal, t.mid)"

        with dict_cur(conn) as cur:
            cur.execute(
                f"""
                SELECT id, {price_expr} AS price
                FROM public.ticks t
                WHERE id IN (%s, %s)
                ORDER BY id
                """,
                (StartTickId, EndTickId),
            )
            rows = cur.fetchall()

        if len(rows) != 2 or rows[0]["price"] is None or rows[1]["price"] is None:
            raise RuntimeError("cannot get endpoint prices")

        P1 = float(rows[0]["price"])
        P2 = float(rows[1]["price"])

        # ------------------------------------------------------------
        # 4. Create ROOT segLine
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute(
                """
                INSERT INTO public.seglines (
                    segm_id,
                    parent_id,
                    depth,
                    iteration,
                    start_tick_id,
                    end_tick_id,
                    start_ts,
                    end_ts,
                    start_price,
                    end_price,
                    is_active
                )
                VALUES (%s, NULL, 0, 0, %s, %s, %s, %s, %s, %s, true)
                RETURNING id
                """,
                (
                    segm_id,
                    StartTickId,
                    EndTickId,
                    StartTs,
                    EndTs,
                    P1,
                    P2,
                ),
            )
            RootLineId = int(cur.fetchone()["id"])

        conn.commit()
        print(f"[buildSegticks] root segLine created id={RootLineId}")

        # ------------------------------------------------------------
        # 5. Count ticks (needed for interpolation denominator)
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM public.ticks WHERE id BETWEEN %s AND %s",
                (StartTickId, EndTickId),
            )
            N = int(cur.fetchone()["n"])

        if N <= 1:
            print("[buildSegticks] segm too small, skipping")
            return

        # ------------------------------------------------------------
        # 6. Stream ticks + insert segticks in chunks
        # ------------------------------------------------------------
        cur_stream = conn.cursor(
            name=f"segticks_stream_{segm_id}"
        )
        cur_stream.itersize = STREAM_ITERSIZE
        cur_stream.execute(
            f"""
            SELECT t.id, t.timestamp, {price_expr} AS price
            FROM public.ticks t
            WHERE t.id BETWEEN %s AND %s
            ORDER BY t.id
            """,
            (StartTickId, EndTickId),
        )

        inserts: List[Tuple] = []
        i = -1
        max_abs_dist = 0.0

        for tick_id, ts, price in cur_stream:
            if price is None:
                continue

            i += 1
            phat = line_interp(P1, P2, i, N)
            dist = float(price - phat)
            max_abs_dist = max(max_abs_dist, abs(dist))

            inserts.append(
                (
                    Symbol,
                    tick_id,
                    segm_id,
                    RootLineId,
                    dist,
                )
            )

            if len(inserts) >= BATCH_SIZE:
                with dict_cur(conn) as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO public.segticks
                            (symbol, tick_id, segm_id, segline_id, dist)
                        VALUES %s
                        """,
                        inserts,
                        page_size=10_000,
                    )
                conn.commit()
                inserts.clear()

        if inserts:
            with dict_cur(conn) as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO public.segticks
                        (symbol, tick_id, segm_id, segline_id, dist)
                    VALUES %s
                    """,
                    inserts,
                    page_size=10_000,
                )
            conn.commit()
            inserts.clear()

        cur_stream.close()

        # ------------------------------------------------------------
        # 7. Update segLine stats
        # ------------------------------------------------------------
        with dict_cur(conn) as cur:
            cur.execute(
                """
                UPDATE public.seglines
                SET
                    num_ticks = %s,
                    duration_ms = %s,
                    max_abs_dist = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (
                    N,
                    int((EndTs - StartTs).total_seconds() * 1000),
                    max_abs_dist,
                    RootLineId,
                ),
            )
        conn.commit()

        print(
            f"[buildSegticks] DONE segm={segm_id} "
            f"ticks={N} max_abs_dist={max_abs_dist:.6f}"
        )

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
