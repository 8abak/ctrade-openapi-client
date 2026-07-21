"""Read one scheduled broker session from PostgreSQL without trading logic.

The source issues one parameterized, read-only ``SELECT`` for the exact
half-open interval returned by :func:`broker_session_bounds`.  Rows are
delivered through a named PostgreSQL cursor in explicit fetch batches.  No
price-derived database columns, labels, signals, or outcomes are requested.

The scanner validates the database stream before exposing each retained quote:

* timestamps must be timezone-aware and inside the requested session;
* rows must be strictly ordered by ``(timestamp in UTC, id)``;
* ids must be globally unique within the requested session;
* symbol, bid, ask, and non-crossed spread invariants must hold; and
* only exact ``(symbol, timestamp, bid, ask)`` collector duplicates collapse.

Equal-timestamp price changes remain separate observations.  The callback API
keeps quote delivery streaming; only compact id intervals, the current
equal-time deduplication group, and detected gap records are retained in
memory.  Transaction ownership remains with the caller and this module never
commits, rolls back, or mutates the connection.
"""

from __future__ import annotations

import math
import re
from bisect import bisect_right
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from datavis.research.fresh_data import CoverageStatus, FreshDataConfig, FreshDataError
from datavis.research.fresh_sessions import (
    AssignedBrokerTick,
    BrokerSessionBounds,
    BrokerTick,
    BrokerTimestampAssignment,
    SessionCompletenessAudit,
    UnexpectedSessionGap,
    broker_session_bounds,
)


_UTC = timezone.utc
_CURSOR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

BROKER_SESSION_QUERY = """\
SELECT id, symbol, timestamp, bid, ask
FROM public.ticks
WHERE symbol = %s
  AND timestamp >= %s
  AND timestamp < %s
ORDER BY timestamp ASC, id ASC
"""


class FreshDbSourceError(FreshDataError):
    """A database row or source configuration cannot be replayed safely."""


class ServerSideCursor(Protocol):
    """Small psycopg-compatible surface used by the session scanner."""

    itersize: int
    arraysize: int

    def execute(self, query: str, parameters: Sequence[Any]) -> Any: ...

    def fetchmany(self, size: int) -> Sequence[Sequence[Any]]: ...

    def close(self) -> Any: ...


class NamedCursorConnection(Protocol):
    """Connection surface required to create a PostgreSQL named cursor."""

    def cursor(self, *, name: str) -> ServerSideCursor: ...


@dataclass(frozen=True, slots=True)
class FreshDbSessionInventory:
    """Counts and schedule QC for one successfully scanned database session."""

    anchor: date
    symbol: str
    bounds: BrokerSessionBounds
    cursor_name: str
    fetch_batch_rows: int
    raw_row_count: int
    valid_quote_count: int
    normalized_quote_count: int
    duplicate_quote_count: int
    duplicate_group_count: int
    locked_quote_count: int
    audit: SessionCompletenessAudit

    @property
    def invalid_quote_count(self) -> int:
        """Invalid database quotes are fatal, so successful scans always have zero."""

        return 0

    @property
    def first_timestamp_utc(self) -> datetime | None:
        return self.audit.first_timestamp_utc

    @property
    def last_timestamp_utc(self) -> datetime | None:
        return self.audit.last_timestamp_utc

    @property
    def open_delay_seconds(self) -> float | None:
        return self.audit.open_delay_seconds

    @property
    def close_lead_seconds(self) -> float | None:
        return self.audit.close_lead_seconds

    @property
    def open_boundary_covered(self) -> bool:
        return self.audit.open_boundary_covered

    @property
    def close_boundary_covered(self) -> bool:
        return self.audit.close_boundary_covered

    @property
    def boundary_complete(self) -> bool:
        return self.audit.boundary_complete

    @property
    def unexpected_gap_count(self) -> int:
        return self.audit.unexpected_gap_count

    @property
    def longest_unexpected_gap_seconds(self) -> float | None:
        return self.audit.longest_unexpected_gap_seconds

    @property
    def total_unexpected_gap_seconds(self) -> float:
        return self.audit.total_unexpected_gap_seconds

    @property
    def unexpected_gap_samples(self) -> tuple[UnexpectedSessionGap, ...]:
        return self.audit.unexpected_gaps

    @property
    def coverage_status(self) -> CoverageStatus:
        if self.raw_row_count == 0:
            return "empty"
        if self.normalized_quote_count == 0 or self.audit.has_unexpected_outage:
            return "ineligible"
        return "complete" if self.audit.boundary_complete else "partial"

    @property
    def is_complete(self) -> bool:
        return self.coverage_status == "complete"


class _IdIntervalSet:
    """Exact duplicate-id detection compact for mostly monotone database ids."""

    def __init__(self) -> None:
        self._starts: list[int] = []
        self._ends: list[int] = []

    def add(self, value: int) -> bool:
        position = bisect_right(self._starts, value) - 1
        if position >= 0 and value <= self._ends[position]:
            return False

        insert_at = position + 1
        merge_left = position >= 0 and self._ends[position] + 1 == value
        merge_right = (
            insert_at < len(self._starts) and self._starts[insert_at] - 1 == value
        )
        if merge_left and merge_right:
            self._ends[position] = self._ends[insert_at]
            del self._starts[insert_at]
            del self._ends[insert_at]
        elif merge_left:
            self._ends[position] = value
        elif merge_right:
            self._starts[insert_at] = value
        else:
            self._starts.insert(insert_at, value)
            self._ends.insert(insert_at, value)
        return True


def _validated_cursor_name(value: str | None, anchor: date) -> str:
    selected = value or f"fresh_ticks_{anchor:%Y%m%d}_{uuid4().hex[:12]}"
    if not isinstance(selected, str) or not _CURSOR_NAME.fullmatch(selected):
        raise ValueError(
            "cursor_name must be a PostgreSQL-safe identifier of at most 63 characters"
        )
    return selected


def _validated_price(value: Any, field: str, row_number: int) -> float:
    if isinstance(value, (bool, str, bytes, bytearray)):
        raise FreshDbSourceError(f"database row {row_number}: {field} is not numeric")
    try:
        converted = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise FreshDbSourceError(
            f"database row {row_number}: {field} is not numeric"
        ) from exc
    if not math.isfinite(converted) or converted <= 0.0:
        raise FreshDbSourceError(
            f"database row {row_number}: {field} must be a positive finite number"
        )
    return converted


def _row_to_tick(
    row: Sequence[Any],
    *,
    row_number: int,
    expected_symbol: str,
    bounds: BrokerSessionBounds,
) -> BrokerTick:
    if isinstance(row, (str, bytes, bytearray)) or not isinstance(row, Sequence):
        raise FreshDbSourceError(
            f"database row {row_number}: expected a five-column sequence"
        )
    if len(row) != 5:
        raise FreshDbSourceError(
            f"database row {row_number}: expected exactly five selected columns"
        )
    raw_id, raw_symbol, raw_timestamp, raw_bid, raw_ask = row
    if not isinstance(raw_id, int) or isinstance(raw_id, bool) or raw_id < 0:
        raise FreshDbSourceError(
            f"database row {row_number}: id must be a non-negative integer"
        )
    if not isinstance(raw_symbol, str) or raw_symbol != expected_symbol:
        raise FreshDbSourceError(
            f"database row {row_number}: symbol does not match {expected_symbol!r}"
        )
    if raw_symbol != raw_symbol.strip():
        raise FreshDbSourceError(
            f"database row {row_number}: symbol contains surrounding whitespace"
        )
    if not isinstance(raw_timestamp, datetime) or raw_timestamp.tzinfo is None:
        raise FreshDbSourceError(
            f"database row {row_number}: timestamp must be timezone-aware"
        )
    if raw_timestamp.utcoffset() is None:
        raise FreshDbSourceError(
            f"database row {row_number}: timestamp has no usable UTC offset"
        )
    timestamp_utc = raw_timestamp.astimezone(_UTC)
    if not bounds.start_utc <= timestamp_utc < bounds.end_utc:
        raise FreshDbSourceError(
            f"database row {row_number}: timestamp is outside requested "
            f"[{bounds.start_utc.isoformat()}, {bounds.end_utc.isoformat()})"
        )
    bid = _validated_price(raw_bid, "bid", row_number)
    ask = _validated_price(raw_ask, "ask", row_number)
    if ask < bid:
        raise FreshDbSourceError(f"database row {row_number}: crossed quote")
    return BrokerTick(
        id=raw_id,
        symbol=raw_symbol,
        timestamp=raw_timestamp,
        bid=bid,
        ask=ask,
    )


def _assignment(tick: BrokerTick, bounds: BrokerSessionBounds) -> AssignedBrokerTick:
    timestamp_utc = tick.timestamp_utc
    return AssignedBrokerTick(
        tick=tick,
        assignment=BrokerTimestampAssignment(
            timestamp_utc=timestamp_utc,
            timestamp_new_york=timestamp_utc.astimezone(
                bounds.start_new_york.tzinfo
            ),
            timestamp_sydney=timestamp_utc.astimezone(bounds.start_sydney.tzinfo),
            status="session",
            anchor=bounds.anchor,
            bounds=bounds,
        ),
    )


def _build_audit(
    *,
    symbol: str,
    bounds: BrokerSessionBounds,
    config: FreshDataConfig,
    raw_count: int,
    normalized_count: int,
    duplicate_count: int,
    first_timestamp: datetime | None,
    last_timestamp: datetime | None,
    gaps: list[UnexpectedSessionGap],
) -> SessionCompletenessAudit:
    settings = config.session_audit
    if first_timestamp is None or last_timestamp is None:
        open_delay = None
        close_lead = None
        open_covered = False
        close_covered = False
    else:
        open_delay = (first_timestamp - bounds.start_utc).total_seconds()
        close_lead = (bounds.end_utc - last_timestamp).total_seconds()
        close_tolerance = (
            settings.friday_close_tolerance_seconds
            if bounds.anchor.weekday() == 4
            else settings.close_tolerance_seconds
        )
        open_covered = open_delay <= float(settings.open_tolerance_seconds)
        close_covered = close_lead <= float(close_tolerance)

    durations = [item.duration_seconds for item in gaps]
    boundary_complete = open_covered and close_covered
    has_outage = bool(gaps)
    return SessionCompletenessAudit(
        symbol=symbol if raw_count else None,
        bounds=bounds,
        raw_tick_count=raw_count,
        normalized_tick_count=normalized_count,
        duplicate_count=duplicate_count,
        in_session_tick_count=normalized_count,
        outside_requested_session_count=0,
        outside_requested_session_ids=(),
        first_timestamp_utc=first_timestamp,
        last_timestamp_utc=last_timestamp,
        open_delay_seconds=open_delay,
        close_lead_seconds=close_lead,
        open_boundary_covered=open_covered,
        close_boundary_covered=close_covered,
        boundary_complete=boundary_complete,
        unexpected_gaps=tuple(gaps),
        longest_unexpected_gap_seconds=max(durations) if durations else None,
        total_unexpected_gap_seconds=sum(durations),
        has_unexpected_outage=has_outage,
        is_complete=boundary_complete and not has_outage,
    )


def scan_fresh_db_session(
    connection: NamedCursorConnection,
    anchor: date | str,
    *,
    config: FreshDataConfig,
    on_tick: Callable[[AssignedBrokerTick], None] | None = None,
    cursor_name: str | None = None,
) -> FreshDbSessionInventory:
    """Stream and audit one exact broker session through a named DB cursor.

    The sole SQL statement is :data:`BROKER_SESSION_QUERY`.  Its lower bound is
    inclusive and its upper bound is exclusive.  ``config.chunk_rows`` controls
    both the named cursor's transfer size and each explicit ``fetchmany`` call.
    Malformed or out-of-range database rows are fatal rather than quarantined.

    A PostgreSQL named cursor requires an open transaction.  Therefore a
    connection explicitly configured with ``autocommit=True`` is rejected; the
    caller retains responsibility for the surrounding read-only transaction.
    """

    if not isinstance(config, FreshDataConfig):
        raise TypeError("config must be a FreshDataConfig")
    symbol = config.expected_symbol
    if symbol is None:
        raise ValueError("config.expected_symbol is required for a session query")
    bounds = broker_session_bounds(anchor)
    selected_cursor_name = _validated_cursor_name(cursor_name, bounds.anchor)
    if getattr(connection, "autocommit", False) is True:
        raise FreshDbSourceError(
            "a PostgreSQL named cursor requires autocommit to be disabled"
        )

    cursor = connection.cursor(name=selected_cursor_name)
    raw_count = 0
    normalized_count = 0
    duplicate_count = 0
    duplicate_group_count = 0
    locked_count = 0
    seen_ids = _IdIntervalSet()
    previous_key: tuple[datetime, int] | None = None
    previous_normalized: BrokerTick | None = None
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    equal_timestamp: datetime | None = None
    equal_time_quote_keys: set[tuple[str, datetime, float, float]] = set()
    equal_time_duplicate_groups: set[tuple[str, datetime, float, float]] = set()
    gaps: list[UnexpectedSessionGap] = []

    try:
        cursor.itersize = config.chunk_rows
        cursor.arraysize = config.chunk_rows
        cursor.execute(
            BROKER_SESSION_QUERY,
            (symbol, bounds.start_utc, bounds.end_utc),
        )
        while True:
            batch = cursor.fetchmany(config.chunk_rows)
            if not batch:
                break
            for row in batch:
                raw_count += 1
                tick = _row_to_tick(
                    row,
                    row_number=raw_count,
                    expected_symbol=symbol,
                    bounds=bounds,
                )
                timestamp_utc = tick.timestamp_utc
                order_key = (timestamp_utc, tick.id)
                if previous_key is not None and order_key <= previous_key:
                    raise FreshDbSourceError(
                        f"database row {raw_count}: rows must be strictly ordered "
                        "by (timestamp UTC, id)"
                    )
                previous_key = order_key
                if not seen_ids.add(tick.id):
                    raise FreshDbSourceError(
                        f"database row {raw_count}: duplicate tick id {tick.id}"
                    )

                if equal_timestamp != timestamp_utc:
                    equal_timestamp = timestamp_utc
                    equal_time_quote_keys.clear()
                    equal_time_duplicate_groups.clear()
                quote_key = tick.exact_quote_key
                if quote_key in equal_time_quote_keys:
                    duplicate_count += 1
                    if quote_key not in equal_time_duplicate_groups:
                        equal_time_duplicate_groups.add(quote_key)
                        duplicate_group_count += 1
                    continue
                equal_time_quote_keys.add(quote_key)

                normalized_count += 1
                if tick.ask == tick.bid:
                    locked_count += 1
                if first_timestamp is None:
                    first_timestamp = timestamp_utc
                last_timestamp = timestamp_utc
                if previous_normalized is not None:
                    duration = (
                        timestamp_utc - previous_normalized.timestamp_utc
                    ).total_seconds()
                    if duration > float(
                        config.session_audit.unexpected_gap_seconds
                    ):
                        gaps.append(
                            UnexpectedSessionGap(
                                left_tick_id=previous_normalized.id,
                                right_tick_id=tick.id,
                                left_timestamp_utc=previous_normalized.timestamp_utc,
                                right_timestamp_utc=timestamp_utc,
                                duration_seconds=duration,
                            )
                        )
                previous_normalized = tick
                if on_tick is not None:
                    on_tick(_assignment(tick, bounds))
    finally:
        cursor.close()

    audit = _build_audit(
        symbol=symbol,
        bounds=bounds,
        config=config,
        raw_count=raw_count,
        normalized_count=normalized_count,
        duplicate_count=duplicate_count,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
        gaps=gaps,
    )
    return FreshDbSessionInventory(
        anchor=bounds.anchor,
        symbol=symbol,
        bounds=bounds,
        cursor_name=selected_cursor_name,
        fetch_batch_rows=config.chunk_rows,
        raw_row_count=raw_count,
        valid_quote_count=raw_count,
        normalized_quote_count=normalized_count,
        duplicate_quote_count=duplicate_count,
        duplicate_group_count=duplicate_group_count,
        locked_quote_count=locked_count,
        audit=audit,
    )


__all__ = [
    "BROKER_SESSION_QUERY",
    "FreshDbSessionInventory",
    "FreshDbSourceError",
    "NamedCursorConnection",
    "ServerSideCursor",
    "scan_fresh_db_session",
]
