from __future__ import annotations

import argparse
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from datavis.envelope_storage import fetch_tick_row, resolve_backfill_range
from datavis.market_profile import (
    DEFAULT_PROFILE_BIN_SIZE,
    DEFAULT_PROFILE_MAX_GAP_MS,
    DEFAULT_PROFILE_SOURCE,
    MarketProfileConfig,
    MarketProfileProcessor,
    session_bounds,
)
from datavis.market_profile_storage import (
    DEFAULT_SYMBOL,
    apply_bin_deltas,
    ensure_market_profile_schema,
    fetch_market_profile_storage_bounds,
    load_market_profile_state,
    refresh_market_profile_summary,
    save_market_profile_state,
    upsert_market_profile,
)
from datavis.ott_storage import fetch_tick_batch_after, fetch_tick_id_bounds


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


def build_config(args: argparse.Namespace) -> MarketProfileConfig:
    return MarketProfileConfig(
        source=getattr(args, "source", DEFAULT_PROFILE_SOURCE),
        binsize=getattr(args, "binsize", DEFAULT_PROFILE_BIN_SIZE),
        maxgapms=getattr(args, "maxgapms", DEFAULT_PROFILE_MAX_GAP_MS),
    ).normalized()


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
    config: MarketProfileConfig,
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


def initialize_processing_state(
    symbol: str,
    config: MarketProfileConfig,
    job_name: str,
    seed_tick_id: int,
) -> Dict[str, Any]:
    ensure_market_profile_schema()
    state_row = load_market_profile_state(job_name)
    if state_row:
        processor = MarketProfileProcessor(config, state=state_row.get("statejson") or {})
        return {
            "processor": processor,
            "last_id": int(state_row.get("lasttickid") or 0),
        }

    storage_bounds = fetch_market_profile_storage_bounds(symbol, config)
    storage_last_id = int(storage_bounds.get("lasttickid") or 0)
    if storage_last_id > 0:
        pending_rows = fetch_tick_batch_after(symbol, max(0, storage_last_id - 1), 1, end_id=storage_last_id)
        pending_tick = pending_rows[0] if pending_rows else fetch_tick_row(symbol, storage_last_id)
        state = {"pendingtick": None}
        if pending_tick:
            state["pendingtick"] = {
                "id": int(storage_last_id),
                "symbol": symbol,
                "timestamp": pending_tick["timestamp"].isoformat(),
                "bid": pending_tick.get("bid"),
                "ask": pending_tick.get("ask"),
                "mid": pending_tick.get("mid"),
                "price": pending_tick.get("price"),
            }
        processor = MarketProfileProcessor(config, state=state)
        log(
            "recovered market profile state from storage job={0} lasttickid={1}".format(
                job_name,
                storage_last_id,
            )
        )
        return {"processor": processor, "last_id": storage_last_id}

    return {
        "processor": MarketProfileProcessor(config),
        "last_id": max(0, int(seed_tick_id)),
    }


def merge_session_meta(target: Dict[Any, Dict[str, Any]], session_start: datetime, payload: Dict[str, Any]) -> None:
    entry = target.setdefault(
        session_start,
        {
            "sessionlabel": payload["sessionlabel"],
            "sessionstart": payload["sessionstart"],
            "sessionend": payload["sessionend"],
            "firsttickid": int(payload["tickid"]),
            "firstts": payload["timestamp"],
            "lasttickid": int(payload["tickid"]),
            "lastts": payload["timestamp"],
            "status": payload.get("status", "open"),
        },
    )
    entry["firsttickid"] = min(int(entry["firsttickid"]), int(payload["tickid"]))
    entry["firstts"] = min(entry["firstts"], payload["timestamp"])
    entry["lasttickid"] = max(int(entry["lasttickid"]), int(payload["tickid"]))
    entry["lastts"] = max(entry["lastts"], payload["timestamp"])
    entry["status"] = "closed" if payload.get("status") == "closed" else entry["status"]


def process_range(
    *,
    symbol: str,
    config: MarketProfileConfig,
    job_name: str,
    job_type: str,
    seed_tick_id: int,
    end_id: Optional[int],
    batch_size: int,
    log_every: int,
) -> Dict[str, Any]:
    initial = initialize_processing_state(symbol, config, job_name, seed_tick_id)
    processor = initial["processor"]
    last_id = int(initial["last_id"])
    last_ts: Optional[datetime] = None

    processed = 0
    sessions_updated = 0
    batches = 0

    if end_id is not None and last_id >= end_id:
        return {"processed": 0, "sessions": 0, "lasttickid": last_id, "batches": 0}

    while not STOP_REQUESTED:
        rows = fetch_tick_batch_after(symbol, last_id, batch_size, end_id=end_id)
        if not rows:
            break

        session_meta: Dict[Any, Dict[str, Any]] = {}
        session_deltas: Dict[Any, Dict[float, Dict[str, Any]]] = {}

        for row in rows:
            contribution = processor.process_tick(row)
            pending_session = processor.current_pending_session()
            if pending_session:
                merge_session_meta(
                    session_meta,
                    pending_session["sessionstart"],
                    {
                        **pending_session,
                        "status": "open",
                    },
                )
            if contribution:
                merge_session_meta(
                    session_meta,
                    contribution["sessionstart"],
                    {
                        **contribution,
                        "status": "closed" if contribution["sessionchanged"] else "open",
                    },
                )
                if contribution["weightms"] > 0:
                    bin_map = session_deltas.setdefault(contribution["sessionstart"], {})
                    entry = bin_map.setdefault(
                        float(contribution["pricebin"]),
                        {"weightms": 0.0, "tickcount": 0},
                    )
                    entry["weightms"] += float(contribution["weightms"])
                    entry["tickcount"] += int(contribution["tickcount"])

            last_id = int(row["id"])
            last_ts = row["timestamp"]
            processed += 1

        for session_start, meta in session_meta.items():
            profile_id = upsert_market_profile(
                symbol=symbol,
                config=config,
                session_label=meta["sessionlabel"],
                session_start=meta["sessionstart"],
                session_end=meta["sessionend"],
                first_tick_id=int(meta["firsttickid"]),
                first_ts=meta["firstts"],
                last_tick_id=int(meta["lasttickid"]),
                last_ts=meta["lastts"],
                status=meta["status"],
            )
            deltas = session_deltas.get(session_start) or {}
            if deltas:
                apply_bin_deltas(
                    profile_id,
                    [
                        (price_bin, details["weightms"], details["tickcount"])
                        for price_bin, details in deltas.items()
                    ],
                )
            refresh_market_profile_summary(
                profile_id=profile_id,
                config=config,
                status=meta["status"],
                last_tick_id=int(meta["lasttickid"]),
                last_ts=meta["lastts"],
            )
            sessions_updated += 1

        save_market_profile_state(
            job_name=job_name,
            job_type=job_type,
            symbol=symbol,
            config=config,
            last_tick_id=last_id,
            last_ts=last_ts,
            statejson=processor.snapshot_state(),
        )

        batches += 1
        if batches == 1 or batches % max(1, log_every) == 0:
            log(
                "market profile progress job={0} lasttickid={1} processed={2} sessions={3}".format(
                    job_name,
                    last_id,
                    processed,
                    sessions_updated,
                )
            )

        if end_id is not None and last_id >= end_id:
            break

    return {
        "processed": processed,
        "sessions": sessions_updated,
        "lasttickid": last_id,
        "batches": batches,
    }


def run_worker(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    job_name = config.worker_job_name(symbol)
    current_session_start, _ = session_bounds(datetime.now(tz=timezone.utc), config)
    try:
        seed_range = resolve_backfill_range(symbol, start_time=current_session_start, end_time=None)
        seed_tick_id = max(0, int(seed_range["starttickid"]) - 1)
    except Exception:
        seed_tick_id = 0
    log(
        "starting market profile worker symbol={0} source={1} binsize={2} maxgapms={3} poll={4}s batch={5}".format(
            symbol,
            config.source,
            config.binsize,
            config.maxgapms,
            args.poll_seconds,
            args.batch_size,
        )
    )
    idle_cycles = 0
    while not STOP_REQUESTED:
        summary = process_range(
            symbol=symbol,
            config=config,
            job_name=job_name,
            job_type="worker",
            seed_tick_id=seed_tick_id,
            end_id=None,
            batch_size=args.batch_size,
            log_every=args.log_every,
        )
        if summary["processed"] == 0:
            idle_cycles += 1
            if idle_cycles == 1 or idle_cycles % max(1, args.log_every) == 0:
                bounds = fetch_tick_id_bounds(symbol)
                log("market profile idle symbol={0} firstid={1} lastid={2}".format(symbol, bounds.get("firstid"), bounds.get("lastid")))
            time.sleep(args.poll_seconds)
        else:
            idle_cycles = 0
    return 0


def run_backfill(args: argparse.Namespace) -> int:
    symbol = args.symbol
    config = build_config(args)
    range_info = resolve_backfill_range(
        symbol,
        start_id=args.start_id,
        end_id=args.end_id,
        start_time=parse_timestamp(args.start_time),
        end_time=parse_timestamp(args.end_time),
    )
    job_name = build_backfill_job_name(args, symbol, config, range_info)
    summary = process_range(
        symbol=symbol,
        config=config,
        job_name=job_name,
        job_type="backfill",
        seed_tick_id=max(0, int(range_info["starttickid"]) - 1),
        end_id=int(range_info["endtickid"]),
        batch_size=args.batch_size,
        log_every=args.log_every,
    )
    log(
        "market profile backfill complete job={0} processed={1} sessions={2} lasttickid={3}".format(
            job_name,
            summary["processed"],
            summary["sessions"],
            summary["lasttickid"],
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tick/time-based market profile processing jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(job_parser: argparse.ArgumentParser) -> None:
        job_parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
        job_parser.add_argument("--source", default=DEFAULT_PROFILE_SOURCE)
        job_parser.add_argument("--binsize", type=float, default=DEFAULT_PROFILE_BIN_SIZE)
        job_parser.add_argument("--maxgapms", type=int, default=DEFAULT_PROFILE_MAX_GAP_MS)
        job_parser.add_argument("--batch-size", type=int, default=2000)
        job_parser.add_argument("--log-every", type=int, default=20)

    worker_parser = subparsers.add_parser("worker", help="Run the incremental market profile worker")
    add_common(worker_parser)
    worker_parser.add_argument("--poll-seconds", type=float, default=1.0)
    worker_parser.set_defaults(handler=run_worker)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill market profile sessions")
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
