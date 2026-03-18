"""
Usage:
  python -m jobs.buildPivots
  python -m jobs.buildPivots --day-id 123
  python -m jobs.buildPivots --start-day-id 100 --end-day-id 150

Build public.pivots from public.ticks, driven by public.days.
One day is processed at a time, using ticks in [days.startid, days.endid]
ordered by timestamp/id, with ticks.kal as the price source.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import psycopg2.extras
from psycopg2 import sql

from backend.db import detect_ts_col, get_conn


ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = ROOT / "logs" / "buildPivots.log"

LAYER_REVS: Sequence[tuple[str, float]] = (
    ("macro", 6.08),
    ("micro", 2.43),
    ("nano", 0.70),
)


@dataclass
class TickRow:
    id: int
    ts: object
    px: float
    dayrow: int


@dataclass
class DayRow:
    id: int
    startid: int
    endid: int
    startts: object
    endts: object


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("buildPivots")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def ensure_pivots_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.pivots (
                id      BIGSERIAL PRIMARY KEY,
                dayid   BIGINT NOT NULL,
                layer   TEXT NOT NULL,
                rev     DOUBLE PRECISION NOT NULL,
                tickid  BIGINT NOT NULL,
                ts      TIMESTAMPTZ NOT NULL,
                px      DOUBLE PRECISION NOT NULL,
                ptype   CHAR(1) NOT NULL,
                pivotno INTEGER NOT NULL,
                dayrow  INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS pivots_dayid_idx
            ON public.pivots (dayid)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS pivots_day_layer_no_idx
            ON public.pivots (dayid, layer, pivotno)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS pivots_day_layer_tick_idx
            ON public.pivots (dayid, layer, tickid)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS pivots_tickid_idx
            ON public.pivots (tickid)
            """
        )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build reusable pivot rows per day from ticks.kal.")
    ap.add_argument("--day-id", type=int, help="Process one specific days.id")
    ap.add_argument("--start-day-id", type=int, help="Process days.id >= this value")
    ap.add_argument("--end-day-id", type=int, help="Process days.id <= this value")
    return ap.parse_args()


def list_days(conn, args: argparse.Namespace) -> List[DayRow]:
    where = []
    params: List[object] = []

    if args.day_id is not None:
        where.append("id = %s")
        params.append(int(args.day_id))
    else:
        if args.start_day_id is not None:
            where.append("id >= %s")
            params.append(int(args.start_day_id))
        if args.end_day_id is not None:
            where.append("id <= %s")
            params.append(int(args.end_day_id))

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, startid, endid, startts, endts
            FROM public.days
            {where_sql}
            ORDER BY id ASC
            """,
            tuple(params),
        )
        rows = cur.fetchall()

    out: List[DayRow] = []
    for row in rows:
        out.append(
            DayRow(
                id=int(row["id"]),
                startid=int(row["startid"]),
                endid=int(row["endid"]),
                startts=row["startts"],
                endts=row["endts"],
            )
        )
    return out


def load_day_ticks(conn, day: DayRow, ts_col: str) -> List[TickRow]:
    q = sql.SQL(
        """
        SELECT id, {ts_col} AS ts, kal
        FROM public.ticks
        WHERE id BETWEEN %s AND %s
          AND kal IS NOT NULL
          AND {ts_col} IS NOT NULL
        ORDER BY {ts_col} ASC, id ASC
        """
    ).format(ts_col=sql.Identifier(ts_col))

    with conn.cursor() as cur:
        cur.execute(q, (int(day.startid), int(day.endid)))
        rows = cur.fetchall()

    out: List[TickRow] = []
    for idx, (tick_id, tick_ts, kal) in enumerate(rows, start=1):
        out.append(
            TickRow(
                id=int(tick_id),
                ts=tick_ts,
                px=float(kal),
                dayrow=int(idx),
            )
        )
    return out


def compute_layer_pivots(
    ticks: Sequence[TickRow],
    *,
    day_id: int,
    layer: str,
    rev: float,
) -> List[tuple]:
    if not ticks:
        return []

    pivot_rows: List[tuple] = []

    anchor = ticks[0]
    cand_dir: Optional[int] = None  # +1 up, -1 down
    cand_tick: Optional[TickRow] = None

    for tick in ticks[1:]:
        if cand_dir is None:
            delta = tick.px - anchor.px
            if abs(delta) >= rev:
                cand_dir = 1 if delta > 0.0 else -1
                cand_tick = tick
            continue

        if cand_tick is None:
            cand_tick = tick

        if cand_dir == 1:
            if tick.px >= cand_tick.px:
                cand_tick = tick
            elif (cand_tick.px - tick.px) >= rev:
                pivot_rows.append(
                    (
                        int(day_id),
                        str(layer),
                        float(rev),
                        int(cand_tick.id),
                        cand_tick.ts,
                        float(cand_tick.px),
                        "h",
                        len(pivot_rows) + 1,
                        int(cand_tick.dayrow),
                    )
                )
                cand_dir = -1
                cand_tick = tick
        else:
            if tick.px <= cand_tick.px:
                cand_tick = tick
            elif (tick.px - cand_tick.px) >= rev:
                pivot_rows.append(
                    (
                        int(day_id),
                        str(layer),
                        float(rev),
                        int(cand_tick.id),
                        cand_tick.ts,
                        float(cand_tick.px),
                        "l",
                        len(pivot_rows) + 1,
                        int(cand_tick.dayrow),
                    )
                )
                cand_dir = 1
                cand_tick = tick

    return pivot_rows


def delete_day_pivots(conn, day_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.pivots WHERE dayid = %s", (int(day_id),))


def insert_pivot_rows(conn, rows: Iterable[tuple]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.pivots (
                dayid, layer, rev, tickid, ts, px, ptype, pivotno, dayrow
            )
            VALUES %s
            """,
            rows,
            page_size=5000,
        )
    return len(rows)


def process_day(conn, logger: logging.Logger, ts_col: str, day: DayRow) -> Dict[str, int]:
    logger.info(
        "DAY start | day_id=%s tick_range=%s..%s ts_range=%s..%s",
        day.id,
        day.startid,
        day.endid,
        day.startts.isoformat() if hasattr(day.startts, "isoformat") else day.startts,
        day.endts.isoformat() if hasattr(day.endts, "isoformat") else day.endts,
    )

    ticks = load_day_ticks(conn, day, ts_col)
    logger.info(
        "DAY ticks loaded | day_id=%s tick_range=%s..%s ordered_ticks=%s",
        day.id,
        day.startid,
        day.endid,
        len(ticks),
    )

    delete_day_pivots(conn, day.id)

    rows_to_insert: List[tuple] = []
    per_layer_counts: Dict[str, int] = {}
    for layer, rev in LAYER_REVS:
        layer_rows = compute_layer_pivots(ticks, day_id=day.id, layer=layer, rev=rev)
        rows_to_insert.extend(layer_rows)
        per_layer_counts[layer] = len(layer_rows)

    inserted = insert_pivot_rows(conn, rows_to_insert)
    conn.commit()

    logger.info(
        "DAY finish | day_id=%s tick_range=%s..%s rows_inserted=%s macro=%s micro=%s nano=%s",
        day.id,
        day.startid,
        day.endid,
        inserted,
        per_layer_counts.get("macro", 0),
        per_layer_counts.get("micro", 0),
        per_layer_counts.get("nano", 0),
    )

    return {
        "ticks": len(ticks),
        "inserted": inserted,
        "macro": per_layer_counts.get("macro", 0),
        "micro": per_layer_counts.get("micro", 0),
        "nano": per_layer_counts.get("nano", 0),
    }


def main() -> None:
    args = parse_args()
    logger = setup_logging()

    conn = get_conn()
    conn.autocommit = False

    try:
        ts_col = detect_ts_col(conn)
        ensure_pivots_table(conn)
        conn.commit()

        days = list_days(conn, args)
        logger.info(
            "START buildPivots | ts_col=%s day_id=%s start_day_id=%s end_day_id=%s days=%s",
            ts_col,
            args.day_id,
            args.start_day_id,
            args.end_day_id,
            len(days),
        )

        total_inserted = 0
        total_ticks = 0

        for day in days:
            try:
                stats = process_day(conn, logger, ts_col, day)
                total_inserted += int(stats["inserted"])
                total_ticks += int(stats["ticks"])
            except Exception:
                conn.rollback()
                logger.exception(
                    "DAY error | day_id=%s tick_range=%s..%s",
                    day.id,
                    day.startid,
                    day.endid,
                )
                raise

        logger.info(
            "FINISH buildPivots | days=%s total_ticks=%s total_rows_inserted=%s",
            len(days),
            total_ticks,
            total_inserted,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
