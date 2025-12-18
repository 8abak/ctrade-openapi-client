from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur

# Optional journal (works in your repo: backend/jobs/journal.py)
try:
    from backend.jobs.journal import write_journal  # type: ignore
except Exception:  # pragma: no cover
    write_journal = None  # type: ignore


DEFAULT_BATCH_SIZE = 10_000
DEFAULT_STREAM_ITERSIZE = 25_000


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _journal(msg: str) -> None:
    if write_journal is not None:
        try:
            write_journal(msg)
            return
        except Exception:
            pass
    # fallback local log
    os.makedirs("logs", exist_ok=True)
    with open(os.path.join("logs", "buildSegticks.log"), "a", encoding="utf-8") as f:
        f.write(f"[{_now_utc()}] {msg}\n")


def _price_expr(price_source: str) -> str:
    # match breakLine.py approach: kal falls back to mid if null
    ps = (price_source or "mid").strip().lower()
    if ps == "kal":
        return "COALESCE(t.kal, t.mid)"
    return "t.mid"


def _line_interp(p1: float, p2: float, x1: int, x2: int, xi: int) -> float:
    if x2 == x1:
        return p2
    return p1 + (p2 - p1) * ((xi - x1) / (x2 - x1))


def _iter_ticks_stream(
    conn,
    *,
    start_tick_id: int,
    end_tick_id: int,
    price_source: str,
    itersize: int,
) -> Iterable[Tuple[int, datetime, float]]:
    """
    Stream ticks in tick-id order using a server-side cursor.
    Returns tuples: (tick_id, ts, price)
    """
    price_sql = _price_expr(price_source)

    cur = conn.cursor(name=f"buildsegticks_stream_{start_tick_id}_{end_tick_id}")
    cur.itersize = itersize
    cur.execute(
        f"""
        SELECT t.id AS tick_id, t.timestamp AS ts, {price_sql} AS price
        FROM public.ticks t
        WHERE t.id >= %s AND t.id <= %s
        ORDER BY t.id ASC
        """,
        (start_tick_id, end_tick_id),
    )

    for tick_id, ts, price in cur:
        if price is None:
            continue
        yield int(tick_id), ts, float(price)

    cur.close()


def _get_tick_price(conn, tick_id: int, price_source: str) -> Tuple[datetime, float]:
    price_sql = _price_expr(price_source)
    with dict_cur(conn) as cur:
        cur.execute(
            f"SELECT t.timestamp AS ts, {price_sql} AS price FROM public.ticks t WHERE t.id=%s",
            (tick_id,),
        )
        r = cur.fetchone()
        if not r or r["price"] is None:
            raise RuntimeError(f"tick not found or price null: {tick_id}")
        return r["ts"], float(r["price"])


def _seg_has_segticks(conn, segm_id: int) -> bool:
    with dict_cur(conn) as cur:
        cur.execute("SELECT 1 FROM public.segticks WHERE segm_id=%s LIMIT 1", (segm_id,))
        return cur.fetchone() is not None


def _delete_for_segm(conn, segm_id: int) -> None:
    # Delete segticks first (FK-safe), then seglines.
    with dict_cur(conn) as cur:
        cur.execute("DELETE FROM public.segticks WHERE segm_id=%s", (segm_id,))
        cur.execute("DELETE FROM public.seglines WHERE segm_id=%s", (segm_id,))


def _insert_root_segline(
    conn,
    *,
    segm_id: int,
    start_tick_id: int,
    end_tick_id: int,
    start_ts: datetime,
    end_ts: datetime,
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
            VALUES (%s, NULL, 0, 0, %s, %s, %s, %s, %s, %s, true)
            RETURNING id
            """,
            (
                segm_id,
                start_tick_id,
                end_tick_id,
                start_ts,
                end_ts,
                start_price,
                end_price,
            ),
        )
        return int(cur.fetchone()["id"])


def _update_segline_stats(
    conn,
    *,
    segline_id: int,
    num_ticks: int,
    duration_ms: int,
    max_abs_dist: Optional[float],
) -> None:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            UPDATE public.seglines
            SET num_ticks=%s,
                duration_ms=%s,
                max_abs_dist=%s,
                updated_at=now()
            WHERE id=%s
            """,
            (num_ticks, duration_ms, max_abs_dist, segline_id),
        )


def _bulk_insert_segticks(
    conn,
    rows: List[Tuple[Any, ...]],
) -> None:
    if not rows:
        return
    with dict_cur(conn) as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.segticks (
              symbol, tick_id, segm_id, session_id,
              seg_pos, seg_slope, seg_price_change, seg_duration_seconds,
              run_id,
              segline_id, dist
            )
            VALUES %s
            """,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            page_size=10_000,
        )


def _fetch_segms(
    conn,
    *,
    segm_id: Optional[int],
    from_id: Optional[int],
    to_id: Optional[int],
) -> List[Dict[str, Any]]:
    where = []
    params: List[Any] = []

    if segm_id is not None:
        where.append("id = %s")
        params.append(int(segm_id))
    else:
        if from_id is not None:
            where.append("id >= %s")
            params.append(int(from_id))
        if to_id is not None:
            where.append("id <= %s")
            params.append(int(to_id))

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT
              id, symbol, session_id, global_seg_index,
              start_tick_id, end_tick_id,
              start_ts, end_ts,
              t_axis_type,
              slope, intercept,
              num_ticks, duration_seconds,
              run_id
            FROM public.segms
            {where_sql}
            ORDER BY id ASC
            """,
            tuple(params),
        )
        return list(cur.fetchall() or [])


def build_segticks(
    *,
    segm_id: Optional[int],
    from_id: Optional[int],
    to_id: Optional[int],
    only_missing: bool,
    force: bool,
    price_source: str,
    batch_size: int,
    stream_itersize: int,
) -> int:
    conn = get_conn()
    conn.autocommit = False

    total_done = 0

    try:
        segms = _fetch_segms(conn, segm_id=segm_id, from_id=from_id, to_id=to_id)
        _journal(
            f"buildSegticks started segm_id={segm_id} from_id={from_id} to_id={to_id} "
            f"only_missing={only_missing} force={force} price_source={price_source} segms={len(segms)}"
        )

        for s in segms:
            SegmId = int(s["id"])

            # Skip if already populated
            if only_missing and not force:
                if _seg_has_segticks(conn, SegmId):
                    conn.commit()
                    continue

            _journal(f"segm {SegmId}: start")

            if force:
                _delete_for_segm(conn, SegmId)

            Symbol = s["symbol"]
            SessionId = int(s["session_id"])
            RunId = s.get("run_id")

            StartTickId = int(s["start_tick_id"])
            EndTickId = int(s["end_tick_id"])
            StartTs = s["start_ts"]
            EndTs = s["end_ts"]

            SegSlope = float(s["slope"]) if s["slope"] is not None else 0.0

            # Get endpoint prices from ticks
            _st_ts, StartPrice = _get_tick_price(conn, StartTickId, price_source)
            _en_ts, EndPrice = _get_tick_price(conn, EndTickId, price_source)

            # Create root segline
            RootLineId = _insert_root_segline(
                conn,
                segm_id=SegmId,
                start_tick_id=StartTickId,
                end_tick_id=EndTickId,
                start_ts=StartTs,
                end_ts=EndTs,
                start_price=StartPrice,
                end_price=EndPrice,
            )

            # Interp axis: tick index in this segm
            # Prefer segms.num_ticks; if null, fallback to streaming count logic (but still compute dist on the fly).
            NExpected = int(s["num_ticks"]) if s.get("num_ticks") is not None else 0
            if NExpected <= 1:
                # If segment is tiny or missing num_ticks, still build what we can.
                NExpected = 2

            X1 = 0
            X2 = NExpected - 1

            MaxAbsDist: Optional[float] = None
            Inserted = 0
            i = -1

            rows: List[Tuple[Any, ...]] = []

            for TickId, Ts, Price in _iter_ticks_stream(
                conn,
                start_tick_id=StartTickId,
                end_tick_id=EndTickId,
                price_source=price_source,
                itersize=stream_itersize,
            ):
                i += 1

                # Segment-relative time position (seconds) for UI/analysis compatibility
                SegPosSeconds = (Ts - StartTs).total_seconds()
                SegDurationSeconds = SegPosSeconds
                SegPriceChange = Price - StartPrice

                Phat = _line_interp(StartPrice, EndPrice, X1, X2, i)
                Dist = Price - Phat
                AbsD = abs(Dist)
                if MaxAbsDist is None or AbsD > MaxAbsDist:
                    MaxAbsDist = AbsD

                rows.append(
                    (
                        Symbol,
                        TickId,
                        SegmId,
                        SessionId,
                        float(SegPosSeconds),
                        float(SegSlope),
                        float(SegPriceChange),
                        float(SegDurationSeconds),
                        RunId,
                        RootLineId,
                        float(Dist),
                    )
                )
                Inserted += 1

                if len(rows) >= batch_size:
                    _bulk_insert_segticks(conn, rows)
                    rows.clear()
                    conn.commit()  # commit in the middle of long segments to reduce pressure/locks

            if rows:
                _bulk_insert_segticks(conn, rows)
                rows.clear()

            # Update segline stats (we already computed max_abs_dist in python)
            DurationMs = int((EndTs - StartTs).total_seconds() * 1000)
            _update_segline_stats(
                conn,
                segline_id=RootLineId,
                num_ticks=int(Inserted),
                duration_ms=DurationMs,
                max_abs_dist=float(MaxAbsDist) if MaxAbsDist is not None else None,
            )

            conn.commit()
            total_done += 1

            _journal(
                f"segm {SegmId}: finished segline_id={RootLineId} ticks_inserted={Inserted} "
                f"max_abs_dist={MaxAbsDist}"
            )

        _journal(f"buildSegticks finished ok=true segms_done={total_done}")
        return total_done

    except Exception as e:
        conn.rollback()
        _journal(f"buildSegticks finished ok=false err={str(e)}")
        raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description="Populate segticks from segms (streaming) and build root segLines.")
    ap.add_argument("--segm-id", type=int, default=None, help="Build only this segm_id")
    ap.add_argument("--from-id", type=int, default=None, help="Build segms with id >= from-id")
    ap.add_argument("--to-id", type=int, default=None, help="Build segms with id <= to-id")
    ap.add_argument("--only-missing", action="store_true", help="Only build segms that have no segticks (default)")
    ap.add_argument("--all", action="store_true", help="Build all selected segms even if segticks exist (unless --force is false and segticks exist)")
    ap.add_argument("--force", action="store_true", help="Delete segticks+seglines for segm(s) first, then rebuild")
    ap.add_argument("--price-source", type=str, default="mid", choices=["mid", "kal"], help="Price source for initial root line + dist")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Insert batch size")
    ap.add_argument("--stream-itersize", type=int, default=DEFAULT_STREAM_ITERSIZE, help="Server cursor itersize")

    args = ap.parse_args()

    # default behavior: only_missing unless --all is given
    only_missing = True
    if args.all:
        only_missing = False
    if args.only_missing:
        only_missing = True

    done = build_segticks(
        segm_id=args.segm_id,
        from_id=args.from_id,
        to_id=args.to_id,
        only_missing=only_missing,
        force=bool(args.force),
        price_source=args.price_source,
        batch_size=int(args.batch_size),
        stream_itersize=int(args.stream_itersize),
    )
    print(f"[buildSegticks] segms_done={done}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
