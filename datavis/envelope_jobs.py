from __future__ import annotations

import argparse
import re
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from datavis.envelope import (
    DEFAULT_ENVELOPE_BANDWIDTH,
    DEFAULT_ENVELOPE_LENGTH,
    DEFAULT_ENVELOPE_MULT,
    DEFAULT_ENVELOPE_SOURCE,
    EnvelopeCalculator,
    EnvelopeConfig,
)
from datavis.envelope_storage import (
    DEFAULT_SYMBOL,
    fetch_envelope_storage_bounds,
    fetch_tick_batch_after,
    fetch_tick_id_bounds,
    fetch_tick_rows_in_id_range,
    load_envelope_job_state,
    persist_envelope_progress,
    resolve_backfill_range,
)


STOP_REQUESTED = False
DEFAULT_TIMEZONE = ZoneInfo("Australia/Sydney")


def request_stop(*_: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def log(message: str) -> None:
    print(message, flush=True)


def build_config(args: argparse.Namespace) -> EnvelopeConfig:
    return EnvelopeConfig(
        source=getattr(args, "source", DEFAULT_ENVELOPE_SOURCE),
        length=getattr(args, "length", DEFAULT_ENVELOPE_LENGTH),
        bandwidth=getattr(args, "bandwidth", DEFAULT_ENVELOPE_BANDWIDTH),
        mult=getattr(args, "mult", DEFAULT_ENVELOPE_MULT),
    ).normalized()


def parse_timestamp(raw_value: Optional[str]) -> Optional[datetime]:
    if not raw_value:
        return None
    text = raw_value.strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
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


def build_backfill_job_name(
    args: argparse.Namespace,
    symbol: str,
    config: EnvelopeConfig,
    range_info: Dict[str, Any],
) -> str:
    if getattr(args, "job_name", ""):
        return str(args.job_name).strip()
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
    return config.backfill_job_name(symbol, "{0}-{1}".format(start_token, end_token))


def rebuild_calculator_to_tick(symbol: str, config: EnvelopeConfig, target_tick_id: int) -> Dict[str, Any]:
    if target_tick_id <= 0:
        return {"calculator": EnvelopeCalculator(config), "last_id": 0}

    seed_start_id = max(1, int(target_tick_id) - config.seed_tick_count + 1)
    seed_rows = fetch_tick_rows_in_id_range(symbol, seed_start_id, int(target_tick_id))
    calculator = EnvelopeCalculator(config)
    for row in seed_rows:
        calculator.process_tick(row)
    return {
        "calculator": calculator,
        "last_id": int(seed_rows[-1]["id"]) if seed_rows else 0,
    }


def initialize_processing_state(symbol: str, config: EnvelopeConfig, job_name: str, seed_to_tick_id: int) -> Dict[str, Any]:
    state_row = load_envelope_job_state(job_name)
    if state_row:
        last_id = int(state_row.get("lasttickid") or 0)
        log("loaded envelope state job={0} lasttickid={1}".format(job_name, last_id))
        return {
            "calculator": EnvelopeCalculator(config, state=state_row.get("statejson") or {}),
            "last_id": last_id,
        }
    if seed_to_tick_id > 0:
        log("rebuilding envelope state job={0} to tickid={1}".format(job_name, seed_to_tick_id))
        return rebuild_calculator_to_tick(symbol, config, seed_to_tick_id)
    log("initializing envelope state job={0} from scratch".format(job_name))
    return {"calculator": EnvelopeCalculator(config), "last_id": 0}


def process_range(
    *,
    symbol: str,
    config: EnvelopeConfig,
    job_name: str,
    job_type: str,
    start_tick_id: Optional[int],
    end_tick_id: Optional[int],
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    persist_from_id: int,
    seed_to_tick_id: int,
    chunk_size: int,
    commit_every: int,
    sleep_seconds: float,
    log_every: int,
) -> Dict[str, Any]:
    initial_state = initialize_processing_state(symbol, config, job_name, seed_to_tick_id)
    calculator = initial_state["calculator"]
    last_id = int(initial_state["last_id"])

    if end_tick_id is not None and last_id >= end_tick_id:
        return {"processed": 0, "stored": 0, "lasttickid": last_id, "chunks": 0, "commits": 0}

    processed = 0
    stored = 0
    chunks = 0
    commits = 0
    pending_rows = []
    pending_processed = 0
    last_row = None

    while not STOP_REQUESTED:
        rows = fetch_tick_batch_after(symbol, last_id, chunk_size, end_id=end_tick_id)
        if not rows:
            break

        for row in rows:
            computed = calculator.process_tick(row)
            if int(row["id"]) >= persist_from_id:
                pending_rows.append(computed)

        last_row = rows[-1]
        last_id = int(last_row["id"])
        processed += len(rows)
        chunks += 1
        pending_processed += len(rows)

        should_commit = pending_processed >= commit_every or (end_tick_id is not None and last_id >= end_tick_id)
        if should_commit:
            stored += persist_envelope_progress(
                job_name=job_name,
                job_type=job_type,
                symbol=symbol,
                config=config,
                start_tick_id=start_tick_id,
                end_tick_id=end_tick_id,
                start_ts=start_ts,
                end_ts=end_ts,
                last_tick_id=last_id,
                last_ts=last_row["timestamp"],
                statejson=calculator.snapshot_state(),
                rows=pending_rows,
            )
            pending_rows = []
            pending_processed = 0
            commits += 1

            if commits == 1 or commits % max(1, log_every) == 0:
                log(
                    "envelope progress job={0} symbol={1} source={2} length={3} bandwidth={4} mult={5} lasttickid={6} processed={7} stored={8}".format(
                        job_name,
                        symbol,
                        config.source,
                        config.length,
                        config.bandwidth,
                        config.mult,
                        last_id,
                        processed,
                        stored,
                    )
                )

            if sleep_seconds > 0 and not STOP_REQUESTED and (end_tick_id is None or last_id < end_tick_id):
                time.sleep(sleep_seconds)

        if end_tick_id is not None and last_id >= end_tick_id:
            break

    if last_row is not None and (pending_processed > 0 or pending_rows):
        stored += persist_envelope_progress(
            job_name=job_name,
            job_type=job_type,
            symbol=symbol,
            config=config,
            start_tick_id=start_tick_id,
            end_tick_id=end_tick_id,
            start_ts=start_ts,
            end_ts=end_ts,
            last_tick_id=last_id,
            last_ts=last_row["timestamp"],
            statejson=calculator.snapshot_state(),
            rows=pending_rows,
        )
        commits += 1

    return {
        "processed": processed,
        "stored": stored,
        "lasttickid": last_id,
        "chunks": chunks,
        "commits": commits,
    }


def run_worker(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    job_name = config.worker_job_name(symbol)
    storage_bounds = fetch_envelope_storage_bounds(symbol, config)
    seed_to_tick_id = int(storage_bounds.get("lasttickid") or 0)

    log(
        "starting envelope worker symbol={0} source={1} length={2} bandwidth={3} mult={4} poll={5}s chunk={6} commit={7}".format(
            symbol,
            config.source,
            config.length,
            config.bandwidth,
            config.mult,
            args.poll_seconds,
            args.chunk_size,
            args.commit_every,
        )
    )

    idle_cycles = 0
    while not STOP_REQUESTED:
        summary = process_range(
            symbol=symbol,
            config=config,
            job_name=job_name,
            job_type="worker",
            start_tick_id=None,
            end_tick_id=None,
            start_ts=None,
            end_ts=None,
            persist_from_id=1,
            seed_to_tick_id=seed_to_tick_id,
            chunk_size=args.chunk_size,
            commit_every=args.commit_every,
            sleep_seconds=0.0,
            log_every=args.log_every,
        )
        seed_to_tick_id = summary["lasttickid"]
        if summary["processed"] == 0:
            idle_cycles += 1
            if idle_cycles == 1 or idle_cycles % max(1, args.log_every) == 0:
                bounds = fetch_tick_id_bounds(symbol)
                log(
                    "envelope idle symbol={0} firstid={1} lastid={2}".format(
                        symbol,
                        bounds.get("firstid"),
                        bounds.get("lastid"),
                    )
                )
            time.sleep(args.poll_seconds)
        else:
            idle_cycles = 0

    log("envelope worker stop requested")
    return 0


def run_backfill(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    start_time = parse_timestamp(args.start_time)
    end_time = parse_timestamp(args.end_time)
    range_info = resolve_backfill_range(
        symbol,
        start_id=args.start_id,
        end_id=args.end_id,
        start_time=start_time,
        end_time=end_time,
    )
    job_name = build_backfill_job_name(args, symbol, config, range_info)
    seed_to_tick_id = max(0, int(range_info["starttickid"]) - 1)

    log(
        "envelope backfill job={0} starttickid={1} endtickid={2} startts={3} endts={4}".format(
            job_name,
            range_info["starttickid"],
            range_info["endtickid"],
            range_info["startts"],
            range_info["endts"],
        )
    )

    summary = process_range(
        symbol=symbol,
        config=config,
        job_name=job_name,
        job_type="backfill",
        start_tick_id=range_info["starttickid"],
        end_tick_id=range_info["endtickid"],
        start_ts=range_info["startts"],
        end_ts=range_info["endts"],
        persist_from_id=int(range_info["starttickid"]),
        seed_to_tick_id=seed_to_tick_id,
        chunk_size=args.chunk_size,
        commit_every=args.commit_every,
        sleep_seconds=args.sleep_seconds,
        log_every=args.log_every,
    )
    log(
        "envelope backfill complete job={0} processed={1} stored={2} commits={3} lasttickid={4}".format(
            job_name,
            summary["processed"],
            summary["stored"],
            summary["commits"],
            summary["lasttickid"],
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Envelope processing and backfill jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(job_parser: argparse.ArgumentParser) -> None:
        job_parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
        job_parser.add_argument("--source", default=DEFAULT_ENVELOPE_SOURCE)
        job_parser.add_argument("--length", type=int, default=DEFAULT_ENVELOPE_LENGTH)
        job_parser.add_argument("--bandwidth", type=float, default=DEFAULT_ENVELOPE_BANDWIDTH)
        job_parser.add_argument("--mult", type=float, default=DEFAULT_ENVELOPE_MULT)
        job_parser.add_argument("--chunk-size", type=int, default=500)
        job_parser.add_argument("--commit-every", type=int, default=500)
        job_parser.add_argument("--log-every", type=int, default=20)

    worker_parser = subparsers.add_parser("worker", help="Run the incremental envelope worker")
    add_common(worker_parser)
    worker_parser.add_argument("--poll-seconds", type=float, default=1.0)
    worker_parser.set_defaults(handler=run_worker)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill stored envelope rows")
    add_common(backfill_parser)
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
    args.commit_every = max(1, int(args.commit_every))
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
