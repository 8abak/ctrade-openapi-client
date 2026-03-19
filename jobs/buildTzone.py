"""
Usage:
  python -m jobs.buildTzone
  python -m jobs.buildTzone --day-id 123
  python -m jobs.buildTzone --start-day-id 100 --end-day-id 150

Build public.tzone from public.pivots, driven by public.days.
Each row is a macro top zone derived from one macro high pivot.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from backend.db import get_conn
from jobs.layer2common import (
    DEFAULT_BUILDVER,
    DEFAULT_TICK_SIZE,
    DayRow,
    add_day_args,
    delete_day_rows,
    duration_ms,
    execute_values_insert,
    list_days,
    load_day_pivots,
    price_span_to_ticks,
    require_table_columns,
    setup_logger,
)


DEFAULT_ZONE_HALF_WIDTH_TICKS = 15
DEFAULT_ZONE_HALF_WIDTH_MS = 90_000
TARGET_DIR = "top"
TARGET_STATUS = "built"
SOURCE_LAYER = "macro"
SOURCE_PTYPE = "h"

TZONE_COLUMNS = (
    "id",
    "dayid",
    "pivotid",
    "layername",
    "dir",
    "startts",
    "endts",
    "centerts",
    "topprice",
    "lowprice",
    "highprice",
    "widthticks",
    "widthms",
    "status",
    "buildver",
    "createdts",
    "updatedts",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Layer 2 tzone rows from macro high pivots.")
    add_day_args(parser)
    parser.add_argument("--buildver", default=DEFAULT_BUILDVER, help="Target build version")
    parser.add_argument(
        "--delete-all-buildvers",
        action="store_true",
        help="Delete all existing day rows regardless of buildver before rebuilding",
    )
    parser.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    parser.add_argument("--zone-half-width-ticks", type=int, default=DEFAULT_ZONE_HALF_WIDTH_TICKS)
    parser.add_argument("--zone-half-width-ms", type=int, default=DEFAULT_ZONE_HALF_WIDTH_MS)
    parser.add_argument("--status", default=TARGET_STATUS)
    parser.add_argument("--dir", default=TARGET_DIR)
    parser.add_argument("--source-layer", default=SOURCE_LAYER)
    return parser.parse_args()


def make_tzone_rows(day: DayRow, pivots, args: argparse.Namespace) -> List[tuple]:
    half_ms = max(0, int(args.zone_half_width_ms))
    half_delta = timedelta(milliseconds=half_ms)
    half_price = max(0.0, int(args.zone_half_width_ticks)) * float(args.tick_size)
    created_at = datetime.now(timezone.utc)

    rows: List[tuple] = []
    for pivot in pivots:
        start_ts = max(day.startts, pivot.ts - half_delta)
        end_ts = min(day.endts, pivot.ts + half_delta)
        low_price = float(pivot.px) - half_price
        high_price = float(pivot.px) + half_price
        rows.append(
            (
                int(day.id),
                int(pivot.id),
                str(args.source_layer),
                str(args.dir),
                start_ts,
                end_ts,
                pivot.ts,
                float(pivot.px),
                low_price,
                high_price,
                price_span_to_ticks(low_price, high_price, float(args.tick_size)),
                duration_ms(start_ts, end_ts),
                str(args.status),
                str(args.buildver),
                created_at,
                created_at,
            )
        )
    return rows


def insert_tzones(conn, rows: List[tuple]) -> int:
    return execute_values_insert(
        conn,
        """
        INSERT INTO public.tzone (
            dayid, pivotid, layername, dir, startts, endts, centerts,
            topprice, lowprice, highprice, widthticks, widthms, status,
            buildver, createdts, updatedts
        )
        VALUES %s
        """,
        rows,
        page_size=1000,
    )


def process_day(conn, logger, day: DayRow, args: argparse.Namespace) -> Dict[str, int]:
    deleted = delete_day_rows(
        conn,
        table="tzone",
        day_id=day.id,
        buildver=args.buildver,
        delete_all_buildvers=bool(args.delete_all_buildvers),
    )
    pivots = load_day_pivots(conn, day_id=day.id, layer=args.source_layer, ptype=SOURCE_PTYPE)
    rows = make_tzone_rows(day, pivots, args)
    inserted = insert_tzones(conn, rows)
    conn.commit()

    logger.info(
        "DAY finish | day_id=%s deleted=%s macro_high_pivots=%s inserted=%s buildver=%s",
        day.id,
        deleted,
        len(pivots),
        inserted,
        args.buildver,
    )
    return {"deleted": deleted, "pivots": len(pivots), "inserted": inserted}


def main() -> None:
    args = parse_args()
    logger = setup_logger("buildTzone", "buildTzone.log")

    conn = get_conn()
    conn.autocommit = False

    try:
        require_table_columns(conn, "days", ("id", "startid", "endid", "startts", "endts"))
        require_table_columns(
            conn,
            "pivots",
            ("id", "dayid", "layer", "tickid", "ts", "px", "ptype", "pivotno", "dayrow"),
        )
        require_table_columns(conn, "tzone", TZONE_COLUMNS)

        days = list_days(conn, args)
        logger.info(
            "START buildTzone | day_id=%s start_day_id=%s end_day_id=%s days=%s buildver=%s source_layer=%s",
            args.day_id,
            args.start_day_id,
            args.end_day_id,
            len(days),
            args.buildver,
            args.source_layer,
        )

        totals = {"days": 0, "deleted": 0, "pivots": 0, "inserted": 0}
        for day in days:
            try:
                stats = process_day(conn, logger, day, args)
                totals["days"] += 1
                totals["deleted"] += int(stats["deleted"])
                totals["pivots"] += int(stats["pivots"])
                totals["inserted"] += int(stats["inserted"])
            except Exception:
                conn.rollback()
                logger.exception("DAY error | day_id=%s", day.id)
                raise

        logger.info(
            "FINISH buildTzone | days=%s deleted=%s pivots=%s inserted=%s buildver=%s",
            totals["days"],
            totals["deleted"],
            totals["pivots"],
            totals["inserted"],
            args.buildver,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
