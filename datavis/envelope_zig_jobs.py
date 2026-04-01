from __future__ import annotations

import argparse
import re
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from datavis.envelope import EnvelopeCalculator, EnvelopeConfig
from datavis.envelope_storage import resolve_backfill_range
from datavis.envelope_zig import (
    DEFAULT_ENVELOPE_ZIG_LEVEL,
    EnvelopeZigConfig,
)
from datavis.envelope_zig_storage import (
    DEFAULT_SYMBOL,
    ensure_envelope_zig_schema,
    fetch_envelope_zig_storage_bounds,
    fetch_level_source_rows_after,
    fetch_level_source_rows_upto_confirm,
    load_envelope_zig_state,
    persist_envelope_zig_progress,
)
from datavis.zigzag import ZIG_LEVELS


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


def normalize_levels(raw_levels: str) -> List[str]:
    tokens = [token.strip().lower() for token in str(raw_levels or "").split(",") if token.strip()]
    if not tokens or tokens == ["all"]:
        return list(ZIG_LEVELS)
    selected: List[str] = []
    for token in tokens:
        if token not in ZIG_LEVELS:
            raise ValueError("Unsupported zig envelope level: {0}".format(token))
        if token not in selected:
            selected.append(token)
    return selected


def build_configs(args: argparse.Namespace) -> List[EnvelopeZigConfig]:
    base_length = getattr(args, "length")
    base_bandwidth = getattr(args, "bandwidth")
    base_mult = getattr(args, "mult")
    return [
        EnvelopeZigConfig(level=level, length=base_length, bandwidth=base_bandwidth, mult=base_mult).normalized()
        for level in normalize_levels(getattr(args, "levels", DEFAULT_ENVELOPE_ZIG_LEVEL))
    ]


def job_token_part(label: str, raw_value: Optional[str], resolved_value: Optional[int] = None) -> str:
    if raw_value:
        safe = re.sub(r"[^0-9A-Za-z]+", "", str(raw_value).strip())
        return "{0}{1}".format(label, safe or "value")
    if resolved_value is not None:
        return "{0}{1}".format(label, resolved_value)
    return label


def build_backfill_job_name(
    args: argparse.Namespace,
    symbol: str,
    config: EnvelopeZigConfig,
    range_info: Dict[str, Any],
) -> str:
    if getattr(args, "job_name", ""):
        return "{0}:{1}".format(str(args.job_name).strip(), config.level)
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


def rebuild_calculator_to_confirm(symbol: str, config: EnvelopeZigConfig, target_confirm_tick_id: int) -> Dict[str, Any]:
    calculator = EnvelopeCalculator(
        EnvelopeConfig(
            source="mid",
            length=config.length,
            bandwidth=config.bandwidth,
            mult=config.mult,
        )
    )
    if target_confirm_tick_id <= 0:
        return {"calculator": calculator, "last_confirm_tick_id": 0, "last_tick_id": 0}

    seed_rows = fetch_level_source_rows_upto_confirm(
        symbol,
        config.level,
        target_confirm_tick_id,
        limit=max(1, (config.length * 2) - 1),
    )
    last_confirm_tick_id = 0
    last_tick_id = 0
    for row in seed_rows:
        calculator.process_value(
            tickid=int(row["tickid"]),
            symbol=row["symbol"],
            timestamp=row["timestamp"],
            price=float(row["price"]),
        )
        last_confirm_tick_id = int(row["confirmtickid"])
        last_tick_id = int(row["tickid"])
    return {
        "calculator": calculator,
        "last_confirm_tick_id": last_confirm_tick_id,
        "last_tick_id": last_tick_id,
    }


def initialize_processing_state(
    symbol: str,
    config: EnvelopeZigConfig,
    job_name: str,
    seed_confirm_tick_id: int,
) -> Dict[str, Any]:
    ensure_envelope_zig_schema()
    state_row = load_envelope_zig_state(job_name)
    if state_row:
        log(
            "loaded zig envelope state job={0} level={1} lastconfirmtickid={2}".format(
                job_name,
                config.level,
                int(state_row.get("lastconfirmtickid") or 0),
            )
        )
        calculator = EnvelopeCalculator(
            EnvelopeConfig(
                source="mid",
                length=config.length,
                bandwidth=config.bandwidth,
                mult=config.mult,
            ),
            state=state_row.get("statejson") or {},
        )
        return {
            "calculator": calculator,
            "last_confirm_tick_id": int(state_row.get("lastconfirmtickid") or 0),
            "last_tick_id": int(state_row.get("lasttickid") or 0),
        }
    if seed_confirm_tick_id > 0:
        log(
            "rebuilding zig envelope state job={0} level={1} to confirmtickid={2}".format(
                job_name,
                config.level,
                seed_confirm_tick_id,
            )
        )
        return rebuild_calculator_to_confirm(symbol, config, seed_confirm_tick_id)
    return rebuild_calculator_to_confirm(symbol, config, 0)


def process_level_range(
    *,
    symbol: str,
    config: EnvelopeZigConfig,
    job_name: str,
    job_type: str,
    start_tick_id: Optional[int],
    end_tick_id: Optional[int],
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    persist_from_id: int,
    seed_confirm_tick_id: int,
    chunk_size: int,
    commit_every: int,
    log_every: int,
) -> Dict[str, Any]:
    initial = initialize_processing_state(symbol, config, job_name, seed_confirm_tick_id)
    calculator = initial["calculator"]
    last_confirm_tick_id = int(initial["last_confirm_tick_id"])
    last_tick_id = int(initial["last_tick_id"])

    processed = 0
    stored = 0
    chunks = 0
    commits = 0
    pending_rows: List[Dict[str, Any]] = []
    pending_processed = 0
    last_ts: Optional[datetime] = None

    while not STOP_REQUESTED:
        rows = fetch_level_source_rows_after(
            symbol,
            config.level,
            last_confirm_tick_id,
            chunk_size,
            end_id=end_tick_id,
        )
        if not rows:
            break

        for row in rows:
            computed = calculator.process_value(
                tickid=int(row["tickid"]),
                symbol=row["symbol"],
                timestamp=row["timestamp"],
                price=float(row["price"]),
                extra={
                    "confirmtickid": int(row["confirmtickid"]),
                    "sourceid": row.get("sourceid"),
                    "confirmtime": row["confirmtime"],
                },
            )
            last_confirm_tick_id = int(row["confirmtickid"])
            last_tick_id = int(row["tickid"])
            last_ts = row["timestamp"]
            processed += 1
            pending_processed += 1
            if int(row["tickid"]) >= persist_from_id:
                pending_rows.append(computed)

        chunks += 1
        should_commit = pending_processed >= commit_every or (end_tick_id is not None and last_confirm_tick_id >= end_tick_id)
        if should_commit:
            stored += persist_envelope_zig_progress(
                job_name=job_name,
                job_type=job_type,
                symbol=symbol,
                config=config,
                start_tick_id=start_tick_id,
                end_tick_id=end_tick_id,
                start_ts=start_ts,
                end_ts=end_ts,
                last_tick_id=last_tick_id,
                last_confirm_tick_id=last_confirm_tick_id,
                last_ts=last_ts,
                statejson=calculator.snapshot_state(),
                rows=pending_rows,
            )
            pending_rows = []
            pending_processed = 0
            commits += 1
            if commits == 1 or commits % max(1, log_every) == 0:
                log(
                    "zig envelope progress job={0} level={1} lastconfirmtickid={2} processed={3} stored={4}".format(
                        job_name,
                        config.level,
                        last_confirm_tick_id,
                        processed,
                        stored,
                    )
                )

        if end_tick_id is not None and last_confirm_tick_id >= end_tick_id:
            break

    if pending_processed > 0 or pending_rows:
        stored += persist_envelope_zig_progress(
            job_name=job_name,
            job_type=job_type,
            symbol=symbol,
            config=config,
            start_tick_id=start_tick_id,
            end_tick_id=end_tick_id,
            start_ts=start_ts,
            end_ts=end_ts,
            last_tick_id=last_tick_id,
            last_confirm_tick_id=last_confirm_tick_id,
            last_ts=last_ts,
            statejson=calculator.snapshot_state(),
            rows=pending_rows,
        )
        commits += 1

    return {
        "processed": processed,
        "stored": stored,
        "chunks": chunks,
        "commits": commits,
        "lasttickid": last_tick_id,
        "lastconfirmtickid": last_confirm_tick_id,
    }


def run_worker(args: argparse.Namespace) -> int:
    symbol = args.symbol
    configs = build_configs(args)
    log(
        "starting zig envelope worker symbol={0} levels={1} poll={2}s chunk={3} commit={4}".format(
            symbol,
            ",".join(config.level for config in configs),
            args.poll_seconds,
            args.chunk_size,
            args.commit_every,
        )
    )
    idle_cycles = 0
    while not STOP_REQUESTED:
        processed_any = False
        for config in configs:
            storage_bounds = fetch_envelope_zig_storage_bounds(symbol, config)
            summary = process_level_range(
                symbol=symbol,
                config=config,
                job_name=config.worker_job_name(symbol),
                job_type="worker",
                start_tick_id=None,
                end_tick_id=None,
                start_ts=None,
                end_ts=None,
                persist_from_id=1,
                seed_confirm_tick_id=int(storage_bounds.get("lastconfirmtickid") or 0),
                chunk_size=args.chunk_size,
                commit_every=args.commit_every,
                log_every=args.log_every,
            )
            processed_any = processed_any or summary["processed"] > 0

        if processed_any:
            idle_cycles = 0
            continue

        idle_cycles += 1
        if idle_cycles == 1 or idle_cycles % max(1, args.log_every) == 0:
            log("zig envelope idle symbol={0} levels={1}".format(symbol, ",".join(config.level for config in configs)))
        time.sleep(args.poll_seconds)
    return 0


def run_backfill(args: argparse.Namespace) -> int:
    symbol = args.symbol
    range_info = resolve_backfill_range(
        symbol,
        start_id=args.start_id,
        end_id=args.end_id,
        start_time=parse_timestamp(args.start_time),
        end_time=parse_timestamp(args.end_time),
    )
    for config in build_configs(args):
        job_name = build_backfill_job_name(args, symbol, config, range_info)
        summary = process_level_range(
            symbol=symbol,
            config=config,
            job_name=job_name,
            job_type="backfill",
            start_tick_id=int(range_info["starttickid"]),
            end_tick_id=int(range_info["endtickid"]),
            start_ts=range_info["startts"],
            end_ts=range_info["endts"],
            persist_from_id=int(range_info["starttickid"]),
            seed_confirm_tick_id=max(0, int(range_info["starttickid"]) - 1),
            chunk_size=args.chunk_size,
            commit_every=args.commit_every,
            log_every=args.log_every,
        )
        log(
            "zig envelope backfill complete job={0} level={1} processed={2} stored={3} lastconfirmtickid={4}".format(
                job_name,
                config.level,
                summary["processed"],
                summary["stored"],
                summary["lastconfirmtickid"],
            )
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Envelope calculations sourced from confirmed zig pivots")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(job_parser: argparse.ArgumentParser) -> None:
        job_parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
        job_parser.add_argument("--levels", default="all")
        job_parser.add_argument("--length", type=int, default=500)
        job_parser.add_argument("--bandwidth", type=float, default=8.0)
        job_parser.add_argument("--mult", type=float, default=3.0)
        job_parser.add_argument("--chunk-size", type=int, default=500)
        job_parser.add_argument("--commit-every", type=int, default=500)
        job_parser.add_argument("--log-every", type=int, default=20)

    worker_parser = subparsers.add_parser("worker", help="Run the incremental zig envelope worker")
    add_common(worker_parser)
    worker_parser.add_argument("--poll-seconds", type=float, default=1.0)
    worker_parser.set_defaults(handler=run_worker)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill zig envelope rows")
    add_common(backfill_parser)
    backfill_parser.add_argument("--start-id", type=int, default=None)
    backfill_parser.add_argument("--end-id", type=int, default=None)
    backfill_parser.add_argument("--start-time", default="")
    backfill_parser.add_argument("--end-time", default="")
    backfill_parser.add_argument("--job-name", default="")
    backfill_parser.set_defaults(handler=run_backfill)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
