"""
Export all ticks for a "Sydney market day" into a CSV in train/set/.

Definition:
  Market day for DATE (Sydney) = [DATE 08:00:00, (DATE+1) 07:00:00) Sydney time
  (end is exclusive -> includes up to 06:59:59.999... next day)

Usage:
  python -m jobs.export_market_day_ticks --date 2025-12-12

DB connection uses standard env vars:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
(or DATABASE_URL if you prefer; see build_conn_dsn()).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Optional, Iterator, Any

import psycopg2


SYDNEY_TZ = ZoneInfo("Australia/Sydney")


@dataclass(frozen=True)
class Window:
    start_utc: datetime
    end_utc: datetime


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="Sydney market date, e.g. 2025-12-12 (YYYY-MM-DD)")
    p.add_argument("--table", default="ticks", help="Table name (default: ticks)")
    p.add_argument("--ts-col", default="t", help="Timestamp column name (default: t)")
    p.add_argument("--schema", default="public", help="Schema name (default: public)")
    p.add_argument("--chunk", type=int, default=20000, help="Rows per fetch (default: 20000)")
    p.add_argument("--outdir", default="train/set", help="Output directory (default: train/set)")
    p.add_argument("--order-by", default=None,
                   help="Optional ORDER BY override, e.g. 't, id'. Default: '<ts-col>, id' if id exists else '<ts-col>'")
    return p.parse_args()


def build_conn_dsn() -> str:
    """
    Prefer DATABASE_URL if set, else build from PG* env vars.
    """
    dburl = os.getenv("DATABASE_URL")
    if dburl:
        return dburl

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "trading")
    user = os.getenv("PGUSER", "babak")
    pwd = os.getenv("PGPASSWORD", "babak33044")
    # psycopg2 accepts keyword DSN format
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def compute_window(syd_date: date) -> Window:
    # Sydney local start: date 08:00
    start_local = datetime.combine(syd_date, time(8, 0, 0), tzinfo=SYDNEY_TZ)
    # Sydney local end-exclusive: next day 07:00
    end_local = datetime.combine(syd_date + timedelta(days=1), time(7, 0, 0), tzinfo=SYDNEY_TZ)

    return Window(
        start_utc=start_local.astimezone(timezone.utc),
        end_utc=end_local.astimezone(timezone.utc),
    )


def get_table_columns(conn, schema: str, table: str) -> List[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        cols = [r[0] for r in cur.fetchall()]
    return cols


def quote_ident(name: str) -> str:
    # minimal safe quoting for identifiers (no schema/user input injection)
    return '"' + name.replace('"', '""') + '"'


def export_day(
    conn,
    schema: str,
    table: str,
    ts_col: str,
    window: Window,
    out_csv_path: str,
    chunk: int,
    order_by: Optional[str],
) -> int:
    cols = get_table_columns(conn, schema, table)
    if not cols:
        raise RuntimeError(f"No columns found for {schema}.{table} (table missing?)")

    if ts_col not in cols:
        raise RuntimeError(
            f"Timestamp column '{ts_col}' not found in {schema}.{table}. "
            f"Available columns: {', '.join(cols)}"
        )

    has_id = "id" in cols
    if order_by is None:
        if has_id and ts_col != "id":
            order_by = f"{quote_ident(ts_col)}, {quote_ident('id')}"
        else:
            order_by = f"{quote_ident(ts_col)}"

    # Build SELECT with fully-qualified table
    select_list = ", ".join(quote_ident(c) for c in cols)
    fq_table = f"{quote_ident(schema)}.{quote_ident(table)}"
    where = f"{quote_ident(ts_col)} >= %s AND {quote_ident(ts_col)} < %s"
    sql = f"SELECT {select_list} FROM {fq_table} WHERE {where} ORDER BY {order_by}"

    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)

    rowcount = 0

    # Server-side cursor streams rows without loading everything into RAM.
    with conn.cursor(name="export_ticks_cursor") as cur, open(out_csv_path, "w", newline="") as f:
        cur.itersize = max(1, chunk)
        cur.execute(sql, (window.start_utc, window.end_utc))

        w = csv.writer(f)
        w.writerow(cols)

        while True:
            rows = cur.fetchmany(chunk)
            if not rows:
                break
            w.writerows(rows)
            rowcount += len(rows)

    return rowcount


def main() -> int:
    args = parse_args()

    syd_date = date.fromisoformat(args.date)
    window = compute_window(syd_date)

    out_csv = os.path.join(args.outdir, f"{args.date}.csv")

    dsn = build_conn_dsn()
    # readonly-ish session; you can also set statement_timeout etc if desired
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = True
        n = export_day(
            conn=conn,
            schema=args.schema,
            table=args.table,
            ts_col=args.ts_col,
            window=window,
            out_csv_path=out_csv,
            chunk=args.chunk,
            order_by=args.order_by,
        )
    finally:
        conn.close()

    print(f"[ok] exported {n} rows to {out_csv}")
    print(f"[window] UTC {window.start_utc.isoformat()} -> {window.end_utc.isoformat()} (end-exclusive)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
