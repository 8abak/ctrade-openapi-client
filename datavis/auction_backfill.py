from __future__ import annotations

import argparse
import os
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, Iterable, List

import psycopg2
import psycopg2.extras

from datavis.auction import (
    CONTEXT_SECONDS,
    HISTORY_SESSION_KINDS,
    AuctionStateStore,
    auction_history_counts,
    build_history_snapshots,
    delete_auction_history_range,
    persist_auction_history_snapshots,
)
from datavis.db import db_connect as shared_db_connect


DEFAULT_BATCH_SIZE = 5000


def database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if value.startswith("postgresql+psycopg2://"):
        value = value.replace("postgresql+psycopg2://", "postgresql://", 1)
    return value


def db_connect(*, readonly: bool = False, autocommit: bool = False) -> Any:
    url = database_url()
    if url:
        conn = psycopg2.connect(url)
        conn.autocommit = autocommit
        if readonly:
            conn.set_session(readonly=True, autocommit=autocommit)
        return conn
    return shared_db_connect(readonly=readonly, autocommit=autocommit)


@contextmanager
def db_connection(*, readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
    conn = db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def tick_select_sql() -> str:
    return """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM public.ticks
        WHERE symbol = %s
          AND timestamp >= %s
          AND timestamp < %s
        ORDER BY id ASC
    """


def iter_tick_batches(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
) -> Iterable[List[Dict[str, Any]]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(tick_select_sql(), (symbol, start_ts, end_ts))
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                return
            yield [dict(row) for row in rows]


def fetch_latest_tick_timestamp(conn: Any, *, symbol: str) -> datetime | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT MAX(timestamp) AS last_timestamp
            FROM public.ticks
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = dict(cur.fetchone() or {})
        value = row.get("last_timestamp")
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def fetch_tick_count(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
) -> int:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS rowcount
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            """,
            (symbol, start_ts, end_ts),
        )
        row = dict(cur.fetchone() or {})
        return int(row.get("rowcount") or 0)


def iter_day_windows(start_ts: datetime, end_ts: datetime) -> Iterable[tuple[datetime, datetime]]:
    cursor = start_ts
    while cursor < end_ts:
        next_cursor = min(cursor + timedelta(days=1), end_ts)
        yield cursor, next_cursor
        cursor = next_cursor


@dataclass
class BackfillSummary:
    warmed_rows: int = 0
    processed_rows: int = 0
    chunk_count: int = 0
    sessions_upserted: int = 0
    bins_written: int = 0
    refs_written: int = 0
    events_written: int = 0
    states_written: int = 0
    states_deleted: int = 0
    replaced_sessions: int = 0
    replaced_states: int = 0

    def merge_history(self, payload: Dict[str, int]) -> None:
        self.sessions_upserted += int(payload.get("sessionsUpserted") or 0)
        self.bins_written += int(payload.get("binsWritten") or 0)
        self.refs_written += int(payload.get("refsWritten") or 0)
        self.events_written += int(payload.get("eventsWritten") or 0)
        self.states_written += int(payload.get("statesWritten") or 0)
        self.states_deleted += int(payload.get("statesDeleted") or 0)


def print_line(message: str) -> None:
    print(message, flush=True)


def format_counts(counts: Dict[str, int]) -> str:
    parts = []
    for kind in HISTORY_SESSION_KINDS:
        parts.append(f"{kind}={int(counts.get(kind) or 0)}")
    return ", ".join(parts)


def run_backfill(
    *,
    symbol: str,
    days: int,
    replace: bool,
    with_bins: bool,
    dry_run: bool,
    batch_size: int,
) -> int:
    if days <= 0:
        raise ValueError("days must be greater than 0")

    with db_connection(readonly=True, autocommit=True) as read_conn:
        latest_ts = fetch_latest_tick_timestamp(read_conn, symbol=symbol)
        if latest_ts is None:
            print_line(f"No ticks were found for symbol {symbol}.")
            return 1
        end_ts = latest_ts + timedelta(microseconds=1)
        start_ts = latest_ts - timedelta(days=days)
        warm_start = start_ts - timedelta(seconds=CONTEXT_SECONDS)
        total_ticks = fetch_tick_count(read_conn, symbol=symbol, start_ts=start_ts, end_ts=end_ts)

        writer_ctx = db_connection(readonly=False, autocommit=False) if not dry_run else nullcontext(None)
        with writer_ctx as write_conn:
            if write_conn is not None:
                before_counts = auction_history_counts(write_conn, symbol=symbol, since=start_ts)
            else:
                with db_connection(readonly=True, autocommit=True) as history_conn:
                    before_counts = auction_history_counts(history_conn, symbol=symbol, since=start_ts)

            print_line(f"Backfill symbol={symbol} days={days} start={start_ts.isoformat()} end={latest_ts.isoformat()}")
            print_line(f"Target tick rows={total_ticks} history counts before: {format_counts(before_counts)}")

            summary = BackfillSummary()
            store = AuctionStateStore(symbol=symbol)

            warm_end = min(start_ts, end_ts)
            if warm_start < warm_end:
                for rows in iter_tick_batches(
                    read_conn,
                    symbol=symbol,
                    start_ts=warm_start,
                    end_ts=warm_end,
                    batch_size=batch_size,
                ):
                    store.apply_rows(rows)
                    summary.warmed_rows += len(rows)
            print_line(f"Warm context rows={summary.warmed_rows}")

            if replace and write_conn is not None:
                deleted = delete_auction_history_range(
                    write_conn,
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=latest_ts,
                )
                write_conn.commit()
                summary.replaced_sessions = int(deleted.get("sessionsDeleted") or 0)
                summary.replaced_states = int(deleted.get("statesDeleted") or 0)
                print_line(
                    f"Replace deleted sessions={summary.replaced_sessions} states={summary.replaced_states}"
                )

            for chunk_start, chunk_end in iter_day_windows(start_ts, end_ts):
                chunk_rows = 0
                for rows in iter_tick_batches(
                    read_conn,
                    symbol=symbol,
                    start_ts=chunk_start,
                    end_ts=chunk_end,
                    batch_size=batch_size,
                ):
                    store.apply_rows(rows)
                    row_count = len(rows)
                    chunk_rows += row_count
                    summary.processed_rows += row_count
                if not chunk_rows:
                    continue

                summary.chunk_count += 1
                snapshots = build_history_snapshots(store)
                if not dry_run and write_conn is not None:
                    history_result = persist_auction_history_snapshots(
                        write_conn,
                        symbol=symbol,
                        snapshots=snapshots,
                        with_bins=with_bins,
                    )
                    write_conn.commit()
                    summary.merge_history(history_result)
                print_line(
                    "Chunk {0}: {1} -> {2} rows={3} snapshots={4}".format(
                        summary.chunk_count,
                        chunk_start.isoformat(),
                        chunk_end.isoformat(),
                        chunk_rows,
                        ",".join(sorted(snapshots.keys())) or "none",
                    )
                )

            if not dry_run and write_conn is not None:
                after_counts = auction_history_counts(write_conn, symbol=symbol, since=start_ts)
            else:
                with db_connection(readonly=True, autocommit=True) as history_conn:
                    after_counts = auction_history_counts(history_conn, symbol=symbol, since=start_ts)

    print_line(
        "Done warmed_rows={0} processed_rows={1} chunks={2} sessions={3} bins={4} refs={5} events={6} states={7}".format(
            summary.warmed_rows,
            summary.processed_rows,
            summary.chunk_count,
            summary.sessions_upserted,
            summary.bins_written,
            summary.refs_written,
            summary.events_written,
            summary.states_written,
        )
    )
    print_line(f"History counts after: {format_counts(after_counts)}")
    if dry_run:
        print_line("Dry run only. No history rows were written.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill durable auction history from ticks.")
    parser.add_argument("--symbol", required=True, help="Symbol to process, for example XAUUSD.")
    parser.add_argument("--days", type=int, default=10, help="Number of recent days to replay.")
    parser.add_argument("--replace", action="store_true", help="Delete overlapping durable history before replay.")
    parser.add_argument("--with-bins", action="store_true", help="Persist auction profile bins as part of history.")
    parser.add_argument("--dry-run", action="store_true", help="Replay ticks without writing durable history.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Tick fetch batch size.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_backfill(
        symbol=str(args.symbol).strip().upper(),
        days=max(1, int(args.days)),
        replace=bool(args.replace),
        with_bins=bool(args.with_bins),
        dry_run=bool(args.dry_run),
        batch_size=max(100, int(args.batch_size)),
    )


if __name__ == "__main__":
    raise SystemExit(main())
