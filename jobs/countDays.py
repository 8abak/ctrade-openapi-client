"""
Usage:
  python -m jobs.countDays

Incrementally extend public.days from public.ticks by splitting contiguous
market-active blocks wherever the gap between consecutive ticks is > 45 minutes.
Existing rows are preserved and only the latest day is extended or new days are
appended.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from psycopg2 import sql

from backend.db import detect_ts_col, get_conn


GAP_THRESHOLD_SECONDS = 45.0 * 60.0
FETCH_BATCH_ROWS = 50_000
LOG_EVERY_TICKS = 250_000

ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = ROOT / "logs" / "countDay.log"


@dataclass
class DayRow:
    id: int
    startid: int
    endid: int
    startts: object
    endts: object


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("countDays")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def ensure_days_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.days (
                id      BIGSERIAL PRIMARY KEY,
                startts TIMESTAMPTZ NOT NULL,
                endts   TIMESTAMPTZ NOT NULL,
                startid BIGINT NOT NULL,
                endid   BIGINT NOT NULL,
                donets  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )


def get_tick_bounds(conn, ts_col: str) -> Tuple[int, Optional[int], Optional[int]]:
    q = sql.SQL(
        """
        SELECT COUNT(*)::bigint, MIN(id)::bigint, MAX(id)::bigint
        FROM public.ticks
        WHERE {ts_col} IS NOT NULL
        """
    ).format(ts_col=sql.Identifier(ts_col))

    with conn.cursor() as cur:
        cur.execute(q)
        total_rows, min_id, max_id = cur.fetchone()
    return int(total_rows), min_id, max_id


def load_latest_day(conn) -> Optional[DayRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, startid, endid, startts, endts
            FROM public.days
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return DayRow(
        id=int(row[0]),
        startid=int(row[1]),
        endid=int(row[2]),
        startts=row[3],
        endts=row[4],
    )


def insert_day(conn, start_id: int, start_ts) -> DayRow:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.days (startts, endts, startid, endid, donets)
            VALUES (%s, %s, %s, %s, now())
            RETURNING id, startid, endid, startts, endts
            """,
            (start_ts, start_ts, int(start_id), int(start_id)),
        )
        row = cur.fetchone()
    return DayRow(
        id=int(row[0]),
        startid=int(row[1]),
        endid=int(row[2]),
        startts=row[3],
        endts=row[4],
    )


def update_day_extent(conn, day: DayRow) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.days
            SET endid = %s,
                endts = %s,
                donets = now()
            WHERE id = %s
              AND (
                  endid IS DISTINCT FROM %s
               OR endts IS DISTINCT FROM %s
              )
            """,
            (int(day.endid), day.endts, int(day.id), int(day.endid), day.endts),
        )
        return cur.rowcount > 0


def main() -> None:
    logger = setup_logging()

    write_conn = get_conn()
    read_conn = get_conn()
    read_conn.autocommit = False  # named cursor requires a transaction

    try:
        ts_col = detect_ts_col(write_conn)
        total_ticks, min_id, max_id = get_tick_bounds(write_conn, ts_col)

        logger.info(
            "START countDays | ts_col=%s gap_seconds=%s fetch_batch=%s total_ticks=%s min_id=%s max_id=%s",
            ts_col,
            GAP_THRESHOLD_SECONDS,
            FETCH_BATCH_ROWS,
            total_ticks,
            min_id,
            max_id,
        )

        ensure_days_table(write_conn)

        if total_ticks <= 0:
            logger.info("No ticks found. Finished with rows_written=0 rows_updated=0")
            return

        current_day = load_latest_day(write_conn)
        start_after_id = int(current_day.endid) if current_day is not None else 0

        if current_day is not None and max_id is not None and int(current_day.endid) >= int(max_id):
            logger.info(
                "No new ticks to append | latest_day_id=%s latest_endid=%s max_tick_id=%s",
                current_day.id,
                current_day.endid,
                max_id,
            )
            return

        stream_sql = sql.SQL(
            """
            SELECT id, {ts_col}
            FROM public.ticks
            WHERE {ts_col} IS NOT NULL
              AND id > %s
            ORDER BY {ts_col} ASC, id ASC
            """
        ).format(ts_col=sql.Identifier(ts_col))

        cur = read_conn.cursor(name="count_days_stream")
        cur.itersize = FETCH_BATCH_ROWS
        cur.execute(stream_sql, (int(start_after_id),))

        scanned_ticks = 0
        rows_written = 0
        rows_updated = 0
        last_scanned_id: Optional[int] = None

        prev_id: Optional[int] = int(current_day.endid) if current_day is not None else None
        prev_ts = current_day.endts if current_day is not None else None

        while True:
            rows = cur.fetchmany(FETCH_BATCH_ROWS)
            if not rows:
                break

            for tick_id, tick_ts in rows:
                tick_id = int(tick_id)
                scanned_ticks += 1
                last_scanned_id = tick_id

                if current_day is None:
                    current_day = insert_day(write_conn, tick_id, tick_ts)
                    rows_written += 1
                    prev_id = tick_id
                    prev_ts = tick_ts
                    logger.info(
                        "BOOTSTRAP day | day_id=%s startid=%s startts=%s",
                        current_day.id,
                        current_day.startid,
                        current_day.startts.isoformat(),
                    )
                    continue

                gap_seconds = (tick_ts - prev_ts).total_seconds()
                if gap_seconds > GAP_THRESHOLD_SECONDS:
                    if update_day_extent(write_conn, current_day):
                        rows_updated += 1
                    logger.info(
                        "FINALIZED day | day_id=%s startid=%s endid=%s startts=%s endts=%s gap_seconds=%.3f",
                        current_day.id,
                        current_day.startid,
                        current_day.endid,
                        current_day.startts.isoformat(),
                        current_day.endts.isoformat(),
                        gap_seconds,
                    )
                    current_day = insert_day(write_conn, tick_id, tick_ts)
                    rows_written += 1
                    logger.info(
                        "APPENDED day | day_id=%s startid=%s startts=%s",
                        current_day.id,
                        current_day.startid,
                        current_day.startts.isoformat(),
                    )
                else:
                    current_day.endid = tick_id
                    current_day.endts = tick_ts

                prev_id = tick_id
                prev_ts = tick_ts

                if scanned_ticks % LOG_EVERY_TICKS == 0:
                    logger.info(
                        "PROGRESS scanned_ticks=%s last_scanned_tick_id=%s rows_written=%s rows_updated=%s current_day_id=%s current_start=%s current_end=%s",
                        scanned_ticks,
                        last_scanned_id,
                        rows_written,
                        rows_updated,
                        current_day.id if current_day is not None else None,
                        current_day.startid if current_day is not None else None,
                        current_day.endid if current_day is not None else None,
                    )

        cur.close()

        if current_day is not None and prev_id is not None:
            current_day.endid = int(prev_id)
            current_day.endts = prev_ts
            if update_day_extent(write_conn, current_day):
                rows_updated += 1

        logger.info(
            "FINISH countDays | scanned_ticks=%s rows_written=%s rows_updated=%s last_scanned_tick_id=%s latest_day_id=%s latest_endid=%s",
            scanned_ticks,
            rows_written,
            rows_updated,
            last_scanned_id,
            current_day.id if current_day is not None else None,
            current_day.endid if current_day is not None else None,
        )

    finally:
        try:
            read_conn.close()
        except Exception:
            pass
        try:
            write_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
