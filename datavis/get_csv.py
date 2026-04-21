from __future__ import annotations

import argparse
import csv
import os
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import psycopg2
from dotenv import load_dotenv

from datavis.brokerday import BROKER_DAY_START_HOUR, BROKER_TIMEZONE


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SYMBOL = "XAUUSD"
TICKS_TABLE = "public.ticks"


def _print(message: str) -> None:
    print(message, flush=True)


def _load_env() -> None:
    load_dotenv(BASE_DIR / ".env")


def _database_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    raise RuntimeError("DATABASE_URL is not configured.")


def _current_broker_year() -> int:
    return datetime.now(BROKER_TIMEZONE).year


def _parse_day(value: str) -> date:
    parts = [part.strip() for part in str(value or "").split("/")]
    if len(parts) not in {2, 3} or any(not part for part in parts):
        raise ValueError("day must be DD/MM or DD/MM/YYYY")
    day_value = int(parts[0])
    month_value = int(parts[1])
    year_value = int(parts[2]) if len(parts) == 3 else _current_broker_year()
    return date(year_value, month_value, day_value)


def _broker_window(day_value: date) -> Tuple[datetime, datetime]:
    start_local = datetime.combine(day_value, dt_time(hour=BROKER_DAY_START_HOUR), tzinfo=BROKER_TIMEZONE)
    end_local = start_local + timedelta(hours=23)
    return start_local, end_local


def _csv_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _fetch_tick_columns(conn: Any) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'ticks'
            ORDER BY ordinal_position
            """
        )
        return [str(row[0]) for row in cur.fetchall()]


def _build_projection(columns: Sequence[str]) -> Tuple[List[str], List[str]]:
    available = set(columns)
    missing_required = [name for name in ("id", "timestamp") if name not in available]
    if missing_required:
        raise RuntimeError(f"{TICKS_TABLE} is missing required columns: {', '.join(missing_required)}")

    headers: List[str] = ["id"]
    expressions: List[str] = ["id"]

    if "symbol" in available:
        headers.append("symbol")
        expressions.append("symbol")

    headers.append("timestamp")
    expressions.append('"timestamp"')

    for name in ("bid", "ask"):
        if name in available:
            headers.append(name)
            expressions.append(name)

    if "mid" in available:
        headers.append("mid")
        expressions.append("mid")
    elif {"bid", "ask"}.issubset(available):
        headers.append("mid")
        expressions.append("ROUND((bid + ask) / 2.0, 5) AS mid")

    if "spread" in available:
        headers.append("spread")
        expressions.append("spread")
    elif {"bid", "ask"}.issubset(available):
        headers.append("spread")
        expressions.append("ROUND(ask - bid, 5) AS spread")

    for name in ("kal", "k2"):
        if name in available:
            headers.append(name)
            expressions.append(name)

    return headers, expressions


def _build_query(*, available_columns: Sequence[str], select_sql: Sequence[str], filter_symbol: bool) -> str:
    query = [
        f"SELECT {', '.join(select_sql)}",
        f"FROM {TICKS_TABLE}",
        'WHERE "timestamp" >= %s AND "timestamp" < %s',
    ]
    if filter_symbol and "symbol" in set(available_columns):
        query.append("AND symbol = %s")
    query.append("ORDER BY id ASC")
    return "\n".join(query)


def _default_output_path(day_value: date, symbol: str, include_symbol: bool) -> Path:
    base_name = f"ticks_{symbol}_{day_value.isoformat()}.csv" if include_symbol else f"ticks_{day_value.isoformat()}.csv"
    return Path.cwd() / base_name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export one Sydney broker day of ticks to CSV.")
    parser.add_argument("--day", required=True, help="Broker day in DD/MM or DD/MM/YYYY.")
    parser.add_argument("--out", help="Optional output CSV path.")
    parser.add_argument("--symbol", default=os.getenv("DATAVIS_SYMBOL", DEFAULT_SYMBOL), help="Tick symbol filter.")
    return parser


def export_csv(*, day_value: date, output_path: Path, symbol: str) -> Dict[str, Any]:
    start_local, end_local = _broker_window(day_value)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    conn = psycopg2.connect(_database_url())
    conn.set_session(readonly=True, autocommit=False)
    try:
        available_columns = _fetch_tick_columns(conn)
        headers, select_sql = _build_projection(available_columns)
        filter_symbol = "symbol" in set(available_columns)
        query = _build_query(
            available_columns=available_columns,
            select_sql=select_sql,
            filter_symbol=filter_symbol,
        )

        params: List[Any] = [start_utc, end_utc]
        if filter_symbol:
            params.append(symbol)

        row_count = 0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            with conn.cursor(name="getcsv_export") as cur:
                cur.itersize = 5000
                cur.execute(query, params)
                while True:
                    rows = cur.fetchmany(cur.itersize)
                    if not rows:
                        break
                    for row in rows:
                        writer.writerow([_csv_value(value) for value in row])
                        row_count += 1

        return {
            "day": day_value.isoformat(),
            "start_local": start_local,
            "end_local": end_local,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "row_count": row_count,
            "output_path": str(output_path.resolve()),
        }
    finally:
        conn.close()


def main() -> int:
    _load_env()
    parser = build_parser()
    args = parser.parse_args()
    try:
        day_value = _parse_day(args.day)
    except ValueError as exc:
        parser.error(str(exc))
    symbol = str(args.symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL
    output_path = Path(args.out).expanduser() if args.out else _default_output_path(day_value, symbol, include_symbol=True)

    try:
        result = export_csv(day_value=day_value, output_path=output_path, symbol=symbol)
    except RuntimeError as exc:
        raise SystemExit(str(exc))
    _print(f"selected day: {result['day']}")
    _print(
        "Sydney start/end: {0} -> {1}".format(
            result["start_local"].isoformat(),
            result["end_local"].isoformat(),
        )
    )
    _print(
        "UTC start/end: {0} -> {1}".format(
            result["start_utc"].isoformat(),
            result["end_utc"].isoformat(),
        )
    )
    _print(f"row count: {result['row_count']}")
    _print(f"output: {result['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
