from __future__ import annotations

import argparse
import signal
import sys
import time
from typing import Any, Dict, Optional

from datavis.ott import (
    DEFAULT_OTT_LENGTH,
    DEFAULT_OTT_MA_TYPE,
    DEFAULT_OTT_PERCENT,
    DEFAULT_OTT_SIGNAL_MODE,
    DEFAULT_OTT_SOURCE,
    OttCalculator,
    OttConfig,
)
from datavis.ott_storage import (
    DEFAULT_SYMBOL,
    fetch_tick_batch_after,
    fetch_tick_id_bounds,
    load_job_state,
    resolve_last_week_range,
    run_and_store_backtest,
    save_job_state,
    save_ott_rows,
)


STOP_REQUESTED = False


def request_stop(*_: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def build_config(args: argparse.Namespace) -> OttConfig:
    return OttConfig(
        source=getattr(args, "source", DEFAULT_OTT_SOURCE),
        matype=getattr(args, "matype", DEFAULT_OTT_MA_TYPE),
        length=getattr(args, "length", DEFAULT_OTT_LENGTH),
        percent=getattr(args, "percent", DEFAULT_OTT_PERCENT),
    ).normalized()


def log(message: str) -> None:
    print(message, flush=True)


def process_range(
    *,
    symbol: str,
    config: OttConfig,
    persist_from_id: int,
    end_id: Optional[int],
    batch_size: int,
    log_every: int,
) -> Dict[str, Any]:
    state_row = load_job_state(symbol, config)
    if state_row:
        calculator = OttCalculator(config, state=state_row.get("statejson") or {})
        last_id = int(state_row.get("lasttickid") or 0)
        log("loaded OTT state job={0} lasttickid={1}".format(config.job_name(symbol), last_id))
    else:
        calculator = OttCalculator(config)
        last_id = 0
        log("initializing OTT state job={0} from scratch".format(config.job_name(symbol)))

    processed = 0
    stored = 0
    batches = 0

    if end_id is not None and last_id >= end_id:
        return {"processed": 0, "stored": 0, "lasttickid": last_id, "batches": 0}

    while not STOP_REQUESTED:
        rows = fetch_tick_batch_after(symbol, last_id, batch_size, end_id=end_id)
        if not rows:
            break

        computed_rows = []
        for row in rows:
            result = calculator.process_tick(row)
            if int(row["id"]) >= persist_from_id:
                computed_rows.append(result)

        if computed_rows:
            stored += save_ott_rows(computed_rows)

        last_row = rows[-1]
        last_id = int(last_row["id"])
        save_job_state(symbol, config, last_id, last_row["timestamp"], calculator.snapshot_state())

        processed += len(rows)
        batches += 1
        if batches == 1 or batches % max(1, log_every) == 0:
            log(
                "ott progress symbol={0} source={1} matype={2} length={3} percent={4} lasttickid={5} processed={6} stored={7}".format(
                    symbol,
                    config.source,
                    config.matype,
                    config.length,
                    config.percent,
                    last_id,
                    processed,
                    stored,
                )
            )

        if end_id is not None and last_id >= end_id:
            break

    return {"processed": processed, "stored": stored, "lasttickid": last_id, "batches": batches}


def run_backfill(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    range_info = resolve_last_week_range(symbol, days=args.days)
    log(
        "backfill range symbol={0} starttickid={1} endtickid={2} startts={3} endts={4}".format(
            symbol,
            range_info["starttickid"],
            range_info["endtickid"],
            range_info["startts"],
            range_info["endts"],
        )
    )

    summary = process_range(
        symbol=symbol,
        config=config,
        persist_from_id=range_info["starttickid"],
        end_id=range_info["endtickid"],
        batch_size=args.batch_size,
        log_every=args.log_every,
    )
    log("backfill complete processed={0} stored={1} lasttickid={2}".format(summary["processed"], summary["stored"], summary["lasttickid"]))

    if args.run_backtest:
        run = run_and_store_backtest(
            symbol=symbol,
            config=config,
            signalmode=args.signalmode,
            start_tick_id=range_info["starttickid"],
            end_tick_id=range_info["endtickid"],
            force=args.force,
        )
        log(
            "backtest ready runid={0} tradecount={1} grosspnl={2:.5f} netpnl={3:.5f}".format(
                run["id"],
                int(run["tradecount"]),
                float(run["grosspnl"]),
                float(run["netpnl"]),
            )
        )
    return 0


def run_worker(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    log(
        "starting OTT worker symbol={0} source={1} matype={2} length={3} percent={4} poll={5}s batch={6}".format(
            symbol,
            config.source,
            config.matype,
            config.length,
            config.percent,
            args.poll_seconds,
            args.batch_size,
        )
    )

    idle_cycles = 0
    while not STOP_REQUESTED:
        summary = process_range(
            symbol=symbol,
            config=config,
            persist_from_id=1,
            end_id=None,
            batch_size=args.batch_size,
            log_every=args.log_every,
        )
        if summary["processed"] == 0:
            idle_cycles += 1
            if idle_cycles == 1 or idle_cycles % max(1, args.log_every) == 0:
                bounds = fetch_tick_id_bounds(symbol)
                log(
                    "ott idle symbol={0} firstid={1} lastid={2}".format(
                        symbol,
                        bounds.get("firstid"),
                        bounds.get("lastid"),
                    )
                )
            time.sleep(args.poll_seconds)
        else:
            idle_cycles = 0
    log("OTT worker stop requested")
    return 0


def run_backtest(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    range_info = resolve_last_week_range(symbol, days=args.days)
    run = run_and_store_backtest(
        symbol=symbol,
        config=config,
        signalmode=args.signalmode,
        start_tick_id=range_info["starttickid"],
        end_tick_id=range_info["endtickid"],
        force=args.force,
    )
    log(
        "backtest run ready id={0} tradecount={1} grosspnl={2:.5f} netpnl={3:.5f}".format(
            run["id"],
            int(run["tradecount"]),
            float(run["grosspnl"]),
            float(run["netpnl"]),
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OTT processing and backtest jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(job_parser: argparse.ArgumentParser) -> None:
        job_parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
        job_parser.add_argument("--source", default=DEFAULT_OTT_SOURCE)
        job_parser.add_argument("--matype", default=DEFAULT_OTT_MA_TYPE)
        job_parser.add_argument("--length", type=int, default=DEFAULT_OTT_LENGTH)
        job_parser.add_argument("--percent", type=float, default=DEFAULT_OTT_PERCENT)
        job_parser.add_argument("--batch-size", type=int, default=2000)
        job_parser.add_argument("--log-every", type=int, default=20)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill stored OTT rows")
    add_common(backfill_parser)
    backfill_parser.add_argument("--days", type=int, default=7)
    backfill_parser.add_argument("--run-backtest", action="store_true")
    backfill_parser.add_argument("--signalmode", default=DEFAULT_OTT_SIGNAL_MODE)
    backfill_parser.add_argument("--force", action="store_true")
    backfill_parser.set_defaults(handler=run_backfill)

    worker_parser = subparsers.add_parser("worker", help="Run the OTT incremental worker")
    add_common(worker_parser)
    worker_parser.add_argument("--poll-seconds", type=float, default=1.0)
    worker_parser.set_defaults(handler=run_worker)

    backtest_parser = subparsers.add_parser("backtest", help="Run and persist the last-week backtest")
    add_common(backtest_parser)
    backtest_parser.add_argument("--days", type=int, default=7)
    backtest_parser.add_argument("--signalmode", default=DEFAULT_OTT_SIGNAL_MODE)
    backtest_parser.add_argument("--force", action="store_true")
    backtest_parser.set_defaults(handler=run_backtest)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
