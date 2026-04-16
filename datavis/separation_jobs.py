from __future__ import annotations

import argparse
import os
from datetime import date, datetime

from datavis.separation import SeparationLiveRuntime, db_connection, iter_brokerdays, run_backfill_for_brokerday


def _parse_brokerday(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _print(message: str) -> None:
    print(message, flush=True)


def run_backfill_day(*, symbol: str, brokerday: date, replace: bool) -> int:
    with db_connection(readonly=False, autocommit=False) as conn:
        result = run_backfill_for_brokerday(conn, symbol=symbol, brokerday=brokerday, replace=replace)
        conn.commit()
    counts = result["counts"]
    _print(
        "brokerday={0} ticks={1} micro={2} median={3} macro={4}".format(
            brokerday.isoformat(),
            result["tickcount"],
            counts["micro"],
            counts["median"],
            counts["macro"],
        )
    )
    return 0


def run_backfill_range(*, symbol: str, start_day: date, end_day: date, replace: bool) -> int:
    for brokerday in iter_brokerdays(start_day, end_day):
        run_backfill_day(symbol=symbol, brokerday=brokerday, replace=replace)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill and rebuild separation history.")
    parser.add_argument("--symbol", default=os.getenv("DATAVIS_SYMBOL", "XAUUSD"), help="Symbol to process.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill_day = subparsers.add_parser("backfill-day", help="Backfill one broker day.")
    backfill_day.add_argument("--brokerday", required=True, help="Broker day in YYYY-MM-DD.")
    backfill_day.add_argument("--replace", action="store_true", help="Delete existing rows for the broker day first.")

    backfill_range = subparsers.add_parser("backfill-range", help="Backfill a broker day range.")
    backfill_range.add_argument("--start", required=True, help="First broker day in YYYY-MM-DD.")
    backfill_range.add_argument("--end", required=True, help="Last broker day in YYYY-MM-DD.")
    backfill_range.add_argument("--replace", action="store_true", help="Delete existing rows per broker day first.")

    rebuild_day = subparsers.add_parser("rebuild-day", help="Rebuild one broker day.")
    rebuild_day.add_argument("--brokerday", required=True, help="Broker day in YYYY-MM-DD.")

    bootstrap_current = subparsers.add_parser("bootstrap-current", help="Bootstrap the current broker day.")
    bootstrap_current.add_argument("--brokerday", required=True, help="Broker day in YYYY-MM-DD.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    symbol = str(args.symbol).strip().upper()

    if args.command == "backfill-day":
        return run_backfill_day(symbol=symbol, brokerday=_parse_brokerday(args.brokerday), replace=bool(args.replace))
    if args.command == "backfill-range":
        return run_backfill_range(
            symbol=symbol,
            start_day=_parse_brokerday(args.start),
            end_day=_parse_brokerday(args.end),
            replace=bool(args.replace),
        )
    if args.command == "rebuild-day":
        return run_backfill_day(symbol=symbol, brokerday=_parse_brokerday(args.brokerday), replace=True)
    if args.command == "bootstrap-current":
        with db_connection(readonly=False, autocommit=False) as conn:
            runtime = SeparationLiveRuntime(symbol=symbol)
            result = runtime.bootstrap(conn, brokerday=_parse_brokerday(args.brokerday))
            conn.commit()
        counts = result["counts"]
        _print(
            "bootstrap brokerday={0} ticks={1} micro={2} median={3} macro={4}".format(
                result["brokerday"].isoformat() if result.get("brokerday") else "-",
                result["tickcount"],
                counts["micro"],
                counts["median"],
                counts["macro"],
            )
        )
        return 0
    parser.error("Unknown command.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
