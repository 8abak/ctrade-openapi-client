from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

import psycopg2.extras

from backend.db import columns_exist


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUILDVER = "layer2.v1"
DEFAULT_TICK_SIZE = 0.01


@dataclass
class DayRow:
    id: int
    startid: int
    endid: int
    startts: object
    endts: object


@dataclass
class PivotRow:
    id: int
    dayid: int
    layer: str
    tickid: int
    ts: object
    px: float
    ptype: str
    pivotno: int
    dayrow: int


def setup_logger(name: str, log_name: str) -> logging.Logger:
    log_path = ROOT / "logs" / log_name
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)

    return logger


def add_day_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--day-id", type=int, help="Process one specific days.id")
    parser.add_argument("--start-day-id", type=int, help="Process days.id >= this value")
    parser.add_argument("--end-day-id", type=int, help="Process days.id <= this value")


def list_days(conn, args: argparse.Namespace) -> List[DayRow]:
    where = []
    params: List[object] = []

    if getattr(args, "day_id", None) is not None:
        where.append("id = %s")
        params.append(int(args.day_id))
    else:
        if getattr(args, "start_day_id", None) is not None:
            where.append("id >= %s")
            params.append(int(args.start_day_id))
        if getattr(args, "end_day_id", None) is not None:
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

    return [
        DayRow(
            id=int(row["id"]),
            startid=int(row["startid"]),
            endid=int(row["endid"]),
            startts=row["startts"],
            endts=row["endts"],
        )
        for row in rows
    ]


def require_table_columns(conn, table: str, required_columns: Sequence[str]) -> None:
    present = columns_exist(conn, table, required_columns)
    missing = [col for col in required_columns if col not in present]
    if missing:
        raise RuntimeError(
            f"Missing required table/columns for public.{table}: {', '.join(missing)}. "
            "Apply the required DDL first via sql.html."
        )


def delete_day_rows(
    conn,
    *,
    table: str,
    day_id: int,
    buildver: str,
    delete_all_buildvers: bool,
) -> int:
    with conn.cursor() as cur:
        if delete_all_buildvers:
            cur.execute(f"DELETE FROM public.{table} WHERE dayid = %s", (int(day_id),))
        else:
            cur.execute(
                f"DELETE FROM public.{table} WHERE dayid = %s AND buildver = %s",
                (int(day_id), str(buildver)),
            )
        return int(cur.rowcount or 0)


def load_day_pivots(
    conn,
    *,
    day_id: int,
    layer: str,
    ptype: str,
) -> List[PivotRow]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, dayid, layer, tickid, ts, px, ptype, pivotno, dayrow
            FROM public.pivots
            WHERE dayid = %s
              AND layer = %s
              AND ptype = %s
            ORDER BY ts ASC, id ASC
            """,
            (int(day_id), str(layer), str(ptype)),
        )
        rows = cur.fetchall()

    return [
        PivotRow(
            id=int(row["id"]),
            dayid=int(row["dayid"]),
            layer=str(row["layer"]),
            tickid=int(row["tickid"]),
            ts=row["ts"],
            px=float(row["px"]),
            ptype=str(row["ptype"]),
            pivotno=int(row["pivotno"]),
            dayrow=int(row["dayrow"]),
        )
        for row in rows
    ]


def price_span_to_ticks(low_price: float, high_price: float, tick_size: float) -> int:
    span = max(0.0, float(high_price) - float(low_price))
    return int(round(span / float(tick_size))) if tick_size > 0 else 0


def duration_ms(start_ts, end_ts) -> int:
    if start_ts is None or end_ts is None:
        return 0
    return max(0, int(round((end_ts - start_ts).total_seconds() * 1000.0)))


def absolute_duration_ms(ts_a, ts_b) -> int:
    if ts_a is None or ts_b is None:
        return 0
    return int(round(abs((ts_b - ts_a).total_seconds()) * 1000.0))


def execute_values_insert(conn, sql_text: str, rows: Iterable[tuple], page_size: int = 1000) -> int:
    materialized = list(rows)
    if not materialized:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            sql_text,
            materialized,
            page_size=min(page_size, len(materialized)),
        )
    return len(materialized)
