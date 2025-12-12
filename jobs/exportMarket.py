from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Optional

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
    p.add_argument("--ts-col", default="timestamp", help="Timestamp column name (default: timestamp)")
    p.add_argument("--schema", default="public", help="Schema name (default: public)")
    p.add_argument("--chunk", type=int, default=20000, help="Rows per fetch (default: 20000)")
    p.add_argument("--outdir", default="train/set", help="Output directory (default: train/set)")
    p.add_argument("--order-by", default=None,
                   help="Optional ORDER BY override, e.g. 'timestamp, id'. Default: '<ts-col>, id' if id exists else '<ts-col>'")

    # optional symbol filter (matches your table)
    p.add_argument("--symbol", default=None, help="Optional symbol filter, e.g. XAUUSD")
    p.add_argument("--symbol-col", default="symbol", help="Symbol column name (default: symbol)")
    return p.parse_args()


def build_conn_dsn() -> str:
    dburl = os.getenv("DATABASE_URL")
    if dburl:
        return dburl

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "trading")
    user = os.getenv("PGUSER", "babak")
    pwd = os.getenv("PGPASSWORD", "babak33044")
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
        return [r[0] for r in cur.fetchall()]


def quote_ident(name: str) -> str:
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
    symbol: Optional[str],
    symbol_col: str,
) -> int:
    cols = get_table_columns(conn, schema, table)
    if not cols:
        raise RuntimeError(f"No columns found for {schema}.{table} (table missing?)")

    if ts_col not in cols:
        raise RuntimeError(f"Timestamp column '{ts_col}' not found. Available: {', '.join(cols)}")

    if symbol is not None and symbol_col not in cols:
        raise RuntimeError(f"Symbol column '{symbol_col}' not found. Available: {', '.join(cols)}")

    has_id = "id" in cols
    if order_by is None:
        if has_id and ts_col != "id":
            order_by = f"{quote_ident(ts_col)}, {quote_ident('id')}"
        else:
            order_by = f"{quote_ident(ts_col)}"

    select_list = ", ".join(quote_ident(c) for c in cols)
    fq_table = f"{quote_ident(schema)}.{quote_ident(table)}"

    where_parts = [f"{quote_ident(ts_col)} >= %s", f"{quote_ident(ts_col)} < %s"]
    params = [window.start_utc, window.end_utc]

    if symbol is not None:
        where_parts.append(f"{quote_ident(symbol_col)} = %s")
        params.append(symbol)

    where = " AND ".join(where_parts)
    sql = f"SELECT {select_list} FROM {fq_table} WHERE {where} ORDER BY {order_by}"

    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)

    rowcount = 0
    with conn.cursor(name="export_ticks_cursor") as cur, open(out_csv_path, "w", newline="") as f:
        cur.itersize = max(1, chunk)
        cur.execute(sql, params)

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

    conn = psycopg2.connect(build_conn_dsn())
    try:
        conn.autocommit = False
        n = export_day(
            conn=conn,
            schema=args.schema,
            table=args.table,
            ts_col=args.ts_col,
            window=window,
            out_csv_path=out_csv,
            chunk=args.chunk,
            order_by=args.order_by,
            symbol=args.symbol,
            symbol_col=args.symbol_col,
        )
    finally:
        conn.close()

    print(f"[ok] exported {n} rows to {out_csv}")
    print(f"[window] UTC {window.start_utc.isoformat()} -> {window.end_utc.isoformat()} (end-exclusive)")
    if args.symbol:
        print(f"[filter] {args.symbol_col} = {args.symbol}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
