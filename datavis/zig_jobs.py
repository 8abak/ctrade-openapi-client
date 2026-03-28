from __future__ import annotations

import argparse
import re
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from datavis.envelope_storage import resolve_backfill_range
from datavis.ott_storage import fetch_tick_batch_after, fetch_tick_id_bounds
from datavis.zigzag import ZigPipeline, zig_backfill_job_name, zig_worker_job_name
from datavis.zigzag_storage import (
    DEFAULT_SYMBOL,
    fetch_zig_sync_diagnostics,
    load_zig_state,
    persist_level_rows,
    save_zig_state,
)


STOP_REQUESTED = False
DEFAULT_TIMEZONE = ZoneInfo("Australia/Sydney")


def request_stop(*_: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def log(message: str) -> None:
    print(message, flush=True)


def parse_timestamp(raw_value: Optional[str]) -> Optional[datetime]:
    if not raw_value:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DEFAULT_TIMEZONE)
    return parsed


def job_token_part(label: str, raw_value: Optional[str], resolved_value: Optional[int] = None) -> str:
    if raw_value:
        safe = re.sub(r"[^0-9A-Za-z]+", "", raw_value.strip())
        return "{0}{1}".format(label, safe or "value")
    if resolved_value is not None:
        return "{0}{1}".format(label, resolved_value)
    return label


def build_backfill_job_name(args: argparse.Namespace, symbol: str, range_info: Dict[str, Any]) -> str:
    if getattr(args, "job_name", ""):
        return str(args.job_name).strip()
    if args.start_id is None and args.end_id is None and not args.start_time and not args.end_time:
        return zig_backfill_job_name(symbol, "days{0}".format(int(args.days)))
    start_token = (
        job_token_part("id", str(args.start_id), int(range_info["starttickid"]))
        if args.start_id is not None
        else job_token_part("ts", args.start_time, int(range_info["starttickid"]))
        if args.start_time
        else "first"
    )
    end_token = (
        job_token_part("id", str(args.end_id), int(range_info["endtickid"]))
        if args.end_id is not None
        else job_token_part("ts", args.end_time, int(range_info["endtickid"]))
        if args.end_time
        else "latest"
    )
    return zig_backfill_job_name(symbol, "{0}-{1}".format(start_token, end_token))


def initialize_processing_state(symbol: str, job_name: str) -> Dict[str, Any]:
    state_row = load_zig_state(job_name)
    bounds = fetch_tick_id_bounds(symbol)
    last_tick_id = int(bounds.get("lastid") or 0)
    if state_row:
        state_last_id = int(state_row.get("lasttickid") or 0)
        if state_last_id > last_tick_id:
            log(
                "zig state ahead of ticks job={0} state_lasttickid={1} ticks_lastid={2}; restarting from scratch".format(
                    job_name,
                    state_last_id,
                    last_tick_id,
                )
            )
            return {"pipeline": ZigPipeline(symbol), "last_id": 0}
        log("loaded zig state job={0} lasttickid={1}".format(job_name, state_last_id))
        return {
            "pipeline": ZigPipeline(symbol, state=state_row.get("statejson") or {}),
            "last_id": state_last_id,
        }
    log("initializing zig state job={0} from scratch".format(job_name))
    return {"pipeline": ZigPipeline(symbol), "last_id": 0}


def process_range(
    *,
    symbol: str,
    job_name: str,
    job_type: str,
    end_id: Optional[int],
    chunk_size: int,
    sleep_seconds: float,
    log_every: int,
) -> Dict[str, Any]:
    initial = initialize_processing_state(symbol, job_name)
    pipeline = initial["pipeline"]
    last_id = int(initial["last_id"])

    if end_id is not None and last_id >= end_id:
        return {"processed": 0, "stored": 0, "lasttickid": last_id, "chunks": 0, "levelCounts": {}}

    processed = 0
    stored = 0
    chunks = 0
    level_counts = {"micro": 0, "med": 0, "maxi": 0, "macro": 0}

    while not STOP_REQUESTED:
        rows = fetch_tick_batch_after(symbol, last_id, chunk_size, end_id=end_id)
        if not rows:
            break

        saved = pipeline.process_ticks(rows, persist_level_rows)
        for level, level_rows in saved.items():
            count = len(level_rows)
            level_counts[level] += count
            stored += count

        last_row = rows[-1]
        last_id = int(last_row["id"])
        save_zig_state(
            job_name=job_name,
            job_type=job_type,
            symbol=symbol,
            last_tick_id=last_id,
            last_time=last_row["timestamp"],
            statejson=pipeline.snapshot_state(),
        )

        processed += len(rows)
        chunks += 1
        if chunks == 1 or chunks % max(1, log_every) == 0:
            log(
                "zig progress job={0} lasttickid={1} processed={2} stored={3} micro={4} med={5} maxi={6} macro={7}".format(
                    job_name,
                    last_id,
                    processed,
                    stored,
                    level_counts["micro"],
                    level_counts["med"],
                    level_counts["maxi"],
                    level_counts["macro"],
                )
            )

        if end_id is not None and last_id >= end_id:
            break
        if sleep_seconds > 0 and not STOP_REQUESTED:
            time.sleep(sleep_seconds)

    return {
        "processed": processed,
        "stored": stored,
        "lasttickid": last_id,
        "chunks": chunks,
        "levelCounts": level_counts,
    }


def resolve_range(args: argparse.Namespace, symbol: str) -> Dict[str, Any]:
    start_time = parse_timestamp(args.start_time)
    end_time = parse_timestamp(args.end_time)
    if args.start_id is None and args.end_id is None and start_time is None and end_time is None:
        start_time = datetime.now(tz=DEFAULT_TIMEZONE) - timedelta(days=max(1, int(args.days)))
    return resolve_backfill_range(
        symbol,
        start_id=args.start_id,
        end_id=args.end_id,
        start_time=start_time,
        end_time=end_time,
    )


def run_worker(args: argparse.Namespace) -> int:
    symbol = args.symbol
    job_name = zig_worker_job_name(symbol)
    log(
        "starting zig worker symbol={0} poll={1}s chunk={2}".format(
            symbol,
            args.poll_seconds,
            args.chunk_size,
        )
    )
    idle_cycles = 0
    while not STOP_REQUESTED:
        summary = process_range(
            symbol=symbol,
            job_name=job_name,
            job_type="worker",
            end_id=None,
            chunk_size=args.chunk_size,
            sleep_seconds=0.0,
            log_every=args.log_every,
        )
        if summary["processed"] == 0:
            idle_cycles += 1
            if idle_cycles == 1 or idle_cycles % max(1, args.log_every) == 0:
                diagnostics = fetch_zig_sync_diagnostics(symbol, job_name)
                bounds = fetch_tick_id_bounds(symbol)
                log(
                    "zig idle symbol={0} ticks_lastid={1} state_lasttickid={2}".format(
                        symbol,
                        bounds.get("lastid"),
                        diagnostics["jobState"].get("lastTickId"),
                    )
                )
            time.sleep(args.poll_seconds)
        else:
            idle_cycles = 0
    log("zig worker stop requested")
    return 0


def run_backfill(args: argparse.Namespace) -> int:
    symbol = args.symbol
    range_info = resolve_range(args, symbol)
    job_name = build_backfill_job_name(args, symbol, range_info)
    log(
        "zig backfill job={0} starttickid={1} endtickid={2} startts={3} endts={4}".format(
            job_name,
            range_info["starttickid"],
            range_info["endtickid"],
            range_info["startts"],
            range_info["endts"],
        )
    )
    summary = process_range(
        symbol=symbol,
        job_name=job_name,
        job_type="backfill",
        end_id=int(range_info["endtickid"]),
        chunk_size=args.chunk_size,
        sleep_seconds=args.sleep_seconds,
        log_every=args.log_every,
    )
    log(
        "zig backfill complete job={0} processed={1} stored={2} lasttickid={3} micro={4} med={5} maxi={6} macro={7}".format(
            job_name,
            summary["processed"],
            summary["stored"],
            summary["lasttickid"],
            summary["levelCounts"].get("micro", 0),
            summary["levelCounts"].get("med", 0),
            summary["levelCounts"].get("maxi", 0),
            summary["levelCounts"].get("macro", 0),
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Structural Zig processing jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(job_parser: argparse.ArgumentParser) -> None:
        job_parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
        job_parser.add_argument("--chunk-size", type=int, default=2000)
        job_parser.add_argument("--log-every", type=int, default=20)

    worker_parser = subparsers.add_parser("worker", help="Run the incremental Zig worker")
    add_common(worker_parser)
    worker_parser.add_argument("--poll-seconds", type=float, default=1.0)
    worker_parser.set_defaults(handler=run_worker)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill structural Zig rows")
    add_common(backfill_parser)
    backfill_parser.add_argument("--days", type=int, default=30)
    backfill_parser.add_argument("--start-id", type=int, default=None)
    backfill_parser.add_argument("--end-id", type=int, default=None)
    backfill_parser.add_argument("--start-time", default="")
    backfill_parser.add_argument("--end-time", default="")
    backfill_parser.add_argument("--sleep-seconds", type=float, default=0.0)
    backfill_parser.add_argument("--job-name", default="")
    backfill_parser.set_defaults(handler=run_backfill)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    parser = build_parser()
    args = parser.parse_args(argv)
    args.chunk_size = max(1, int(args.chunk_size))
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
