"""Streaming, strategy-neutral ingestion and QC for raw broker CSV exports.

Only the five raw fields required to reconstruct an executable quote are read:
``id``, ``symbol``, ``timestamp``, ``bid``, and ``ask``.  Extra export columns
are ignored.  Rows are never sorted.  Their canonical observation order must be
strictly increasing by ``(timestamp in UTC, database id)``.

The scanner keeps only one session's distribution samples and one equal-time
repeated-quote diagnostic group in memory.  Every valid record with a unique id
is retained as a separate tick-volume event and may be consumed through a
callback, so a multi-million-row corpus need not be materialized.  This module
contains no labels, barriers, trades, or outcomes.
"""

from __future__ import annotations

import csv
import math
from bisect import bisect_right
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Literal

from datavis.research.fresh_sessions import (
    AssignedBrokerTick,
    BrokerSessionBounds,
    BrokerTick,
    BrokerTimestampAssignment,
    BrokerTimestampStatus,
    SessionAuditConfig,
    UnexpectedSessionGap,
    assign_broker_timestamp,
    broker_session_bounds,
)


_UTC = timezone.utc
_REQUIRED_COLUMNS = frozenset({"id", "symbol", "timestamp", "bid", "ask"})
CoverageStatus = Literal["empty", "partial", "complete", "ineligible"]


class FreshDataError(ValueError):
    """A structural error that makes deterministic replay unsafe."""


@dataclass(frozen=True, slots=True)
class DistributionSummary:
    count: int
    minimum: float | None
    median: float | None
    p95: float | None
    maximum: float | None


@dataclass(frozen=True, slots=True)
class IdDiscontinuity:
    left_id: int
    right_id: int
    left_timestamp_utc: datetime
    right_timestamp_utc: datetime
    missing_id_count: int

    @property
    def is_backward(self) -> bool:
        return self.right_id < self.left_id


@dataclass(frozen=True, slots=True)
class InvalidQuoteSample:
    source: str
    row_number: int
    tick_id: int
    symbol: str
    timestamp_utc: datetime
    reason: str


@dataclass(frozen=True, slots=True)
class FreshStatusInventory:
    status: BrokerTimestampStatus
    raw_row_count: int
    valid_quote_count: int
    normalized_quote_count: int
    duplicate_quote_count: int
    duplicate_group_count: int
    invalid_quote_count: int
    locked_quote_count: int


@dataclass(frozen=True, slots=True)
class FreshSessionInventory:
    anchor: date
    symbol: str
    bounds: BrokerSessionBounds
    raw_row_count: int
    valid_quote_count: int
    normalized_quote_count: int
    duplicate_quote_count: int
    duplicate_group_count: int
    invalid_quote_count: int
    locked_quote_count: int
    first_timestamp_utc: datetime | None
    last_timestamp_utc: datetime | None
    spread: DistributionSummary
    interarrival_seconds: DistributionSummary
    noncontiguous_id_transition_count: int
    missing_id_count: int
    backward_id_transition_count: int
    id_discontinuity_samples: tuple[IdDiscontinuity, ...]
    unexpected_gap_count: int
    longest_unexpected_gap_seconds: float | None
    total_unexpected_gap_seconds: float
    unexpected_gap_samples: tuple[UnexpectedSessionGap, ...]
    invalid_quote_samples: tuple[InvalidQuoteSample, ...]
    open_delay_seconds: float | None
    close_lead_seconds: float | None
    open_boundary_covered: bool
    close_boundary_covered: bool
    boundary_complete: bool
    coverage_status: CoverageStatus

    @property
    def is_complete(self) -> bool:
        return self.coverage_status == "complete"


@dataclass(frozen=True, slots=True)
class FreshSourceInventory:
    path: str
    byte_size: int
    row_count: int
    first_timestamp_utc: datetime | None
    last_timestamp_utc: datetime | None


@dataclass(frozen=True, slots=True)
class FreshDataConfig:
    """Explicit input, schedule-coverage, and compact-QC assumptions."""

    session_audit: SessionAuditConfig
    expected_symbol: str | None = "XAUUSD"
    chunk_rows: int = 50_000
    expected_anchors: tuple[date | str, ...] = ()
    maximum_issue_samples: int = 20

    def __post_init__(self) -> None:
        if self.expected_symbol is not None:
            if not isinstance(self.expected_symbol, str) or not self.expected_symbol.strip():
                raise ValueError("expected_symbol must be non-empty or None")
            if self.expected_symbol != self.expected_symbol.strip():
                raise ValueError("expected_symbol must not contain surrounding whitespace")
        if not isinstance(self.chunk_rows, int) or isinstance(self.chunk_rows, bool):
            raise ValueError("chunk_rows must be a positive integer")
        if self.chunk_rows <= 0:
            raise ValueError("chunk_rows must be a positive integer")
        if not isinstance(self.maximum_issue_samples, int) or isinstance(
            self.maximum_issue_samples, bool
        ):
            raise ValueError("maximum_issue_samples must be a non-negative integer")
        if self.maximum_issue_samples < 0:
            raise ValueError("maximum_issue_samples must be a non-negative integer")
        if not isinstance(self.session_audit, SessionAuditConfig):
            raise TypeError("session_audit must be a SessionAuditConfig")


@dataclass(frozen=True, slots=True)
class FreshCorpusInventory:
    sources: tuple[FreshSourceInventory, ...]
    sessions: tuple[FreshSessionInventory, ...]
    input_row_count: int
    valid_quote_count: int
    normalized_quote_count: int
    duplicate_quote_count: int
    duplicate_group_count: int
    invalid_quote_count: int
    locked_quote_count: int
    first_timestamp_utc: datetime | None
    last_timestamp_utc: datetime | None
    noncontiguous_id_transition_count: int
    missing_id_count: int
    backward_id_transition_count: int
    id_discontinuity_samples: tuple[IdDiscontinuity, ...]
    invalid_quote_samples: tuple[InvalidQuoteSample, ...]
    session_rows: FreshStatusInventory
    maintenance_rows: FreshStatusInventory
    no_session_rows: FreshStatusInventory
    config: FreshDataConfig

    def session_for_anchor(self, anchor: date | str) -> FreshSessionInventory:
        selected = _normalize_anchor(anchor)
        matches = [item for item in self.sessions if item.anchor == selected]
        if not matches:
            raise KeyError(selected.isoformat())
        return matches[0]


class _IdIntervalSet:
    """Exact duplicate-id detection compact for mostly contiguous DB ids."""

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


@dataclass(frozen=True, slots=True)
class _AssignmentRoute:
    status: BrokerTimestampStatus
    anchor: date | None
    bounds: BrokerSessionBounds | None


class _AssignmentCursor:
    """Reuse the scheduled bounds for the many ticks inside one session."""

    def __init__(self) -> None:
        self._session_route: _AssignmentRoute | None = None

    def route(self, timestamp: datetime, timestamp_utc: datetime) -> _AssignmentRoute:
        cached = self._session_route
        if (
            cached is not None
            and cached.bounds is not None
            and cached.bounds.start_utc <= timestamp_utc < cached.bounds.end_utc
        ):
            return cached
        assignment = assign_broker_timestamp(timestamp)
        route = _AssignmentRoute(
            status=assignment.status,
            anchor=assignment.anchor,
            bounds=assignment.bounds,
        )
        self._session_route = route if route.status == "session" else None
        return route

    @staticmethod
    def assignment(
        route: _AssignmentRoute,
        timestamp: datetime,
        timestamp_utc: datetime,
    ) -> BrokerTimestampAssignment:
        if route.bounds is None:
            return assign_broker_timestamp(timestamp)
        return BrokerTimestampAssignment(
            timestamp_utc=timestamp_utc,
            timestamp_new_york=timestamp_utc.astimezone(
                route.bounds.start_new_york.tzinfo
            ),
            timestamp_sydney=timestamp_utc.astimezone(
                route.bounds.start_sydney.tzinfo
            ),
            status=route.status,
            anchor=route.anchor,
            bounds=route.bounds,
        )


@dataclass(slots=True)
class _StatusAccumulator:
    status: BrokerTimestampStatus
    raw: int = 0
    valid: int = 0
    normalized: int = 0
    duplicates: int = 0
    duplicate_groups: int = 0
    invalid: int = 0
    locked: int = 0

    def freeze(self) -> FreshStatusInventory:
        return FreshStatusInventory(
            status=self.status,
            raw_row_count=self.raw,
            valid_quote_count=self.valid,
            normalized_quote_count=self.normalized,
            duplicate_quote_count=self.duplicates,
            duplicate_group_count=self.duplicate_groups,
            invalid_quote_count=self.invalid,
            locked_quote_count=self.locked,
        )


@dataclass(slots=True)
class _SourceAccumulator:
    path: Path
    row_count: int = 0
    first_timestamp_utc: datetime | None = None
    last_timestamp_utc: datetime | None = None

    def observe(self, timestamp_utc: datetime) -> None:
        self.row_count += 1
        if self.first_timestamp_utc is None:
            self.first_timestamp_utc = timestamp_utc
        self.last_timestamp_utc = timestamp_utc

    def freeze(self) -> FreshSourceInventory:
        return FreshSourceInventory(
            path=str(self.path.resolve()),
            byte_size=self.path.stat().st_size,
            row_count=self.row_count,
            first_timestamp_utc=self.first_timestamp_utc,
            last_timestamp_utc=self.last_timestamp_utc,
        )


@dataclass(slots=True)
class _SessionAccumulator:
    anchor: date
    symbol: str
    config: FreshDataConfig
    raw: int = 0
    valid: int = 0
    normalized: int = 0
    duplicates: int = 0
    duplicate_groups: int = 0
    invalid: int = 0
    locked: int = 0
    first_timestamp_utc: datetime | None = None
    last_timestamp_utc: datetime | None = None
    spreads: list[float] = field(default_factory=list)
    interarrivals: list[float] = field(default_factory=list)
    previous_normalized_tick: BrokerTick | None = None
    previous_raw_id: int | None = None
    previous_raw_timestamp_utc: datetime | None = None
    noncontiguous_ids: int = 0
    missing_ids: int = 0
    backward_ids: int = 0
    id_samples: list[IdDiscontinuity] = field(default_factory=list)
    unexpected_gap_count: int = 0
    longest_gap: float | None = None
    total_gap: float = 0.0
    gap_samples: list[UnexpectedSessionGap] = field(default_factory=list)
    invalid_samples: list[InvalidQuoteSample] = field(default_factory=list)

    def observe_raw_id(self, tick_id: int, timestamp_utc: datetime) -> None:
        self.raw += 1
        if self.previous_raw_id is not None and tick_id != self.previous_raw_id + 1:
            item = IdDiscontinuity(
                left_id=self.previous_raw_id,
                right_id=tick_id,
                left_timestamp_utc=self.previous_raw_timestamp_utc,  # type: ignore[arg-type]
                right_timestamp_utc=timestamp_utc,
                missing_id_count=max(0, tick_id - self.previous_raw_id - 1),
            )
            self.noncontiguous_ids += 1
            self.missing_ids += item.missing_id_count
            if item.is_backward:
                self.backward_ids += 1
            if len(self.id_samples) < self.config.maximum_issue_samples:
                self.id_samples.append(item)
        self.previous_raw_id = tick_id
        self.previous_raw_timestamp_utc = timestamp_utc

    def observe_invalid(self, sample: InvalidQuoteSample) -> None:
        self.invalid += 1
        if len(self.invalid_samples) < self.config.maximum_issue_samples:
            self.invalid_samples.append(sample)

    def observe_normalized(self, tick: BrokerTick) -> None:
        self.normalized += 1
        timestamp_utc = tick.timestamp_utc
        if self.first_timestamp_utc is None:
            self.first_timestamp_utc = timestamp_utc
        self.last_timestamp_utc = timestamp_utc
        self.spreads.append(tick.ask - tick.bid)
        previous = self.previous_normalized_tick
        if previous is not None:
            duration = (timestamp_utc - previous.timestamp_utc).total_seconds()
            self.interarrivals.append(duration)
            if duration > float(self.config.session_audit.unexpected_gap_seconds):
                gap = UnexpectedSessionGap(
                    left_tick_id=previous.id,
                    right_tick_id=tick.id,
                    left_timestamp_utc=previous.timestamp_utc,
                    right_timestamp_utc=timestamp_utc,
                    duration_seconds=duration,
                )
                self.unexpected_gap_count += 1
                self.total_gap += duration
                self.longest_gap = (
                    duration if self.longest_gap is None else max(self.longest_gap, duration)
                )
                if len(self.gap_samples) < self.config.maximum_issue_samples:
                    self.gap_samples.append(gap)
        self.previous_normalized_tick = tick

    def freeze(self) -> FreshSessionInventory:
        bounds = broker_session_bounds(self.anchor)
        if self.first_timestamp_utc is None or self.last_timestamp_utc is None:
            open_delay = None
            close_lead = None
            open_covered = False
            close_covered = False
        else:
            open_delay = (self.first_timestamp_utc - bounds.start_utc).total_seconds()
            close_lead = (bounds.end_utc - self.last_timestamp_utc).total_seconds()
            open_covered = open_delay <= float(
                self.config.session_audit.open_tolerance_seconds
            )
            close_tolerance = (
                self.config.session_audit.friday_close_tolerance_seconds
                if self.anchor.weekday() == 4
                else self.config.session_audit.close_tolerance_seconds
            )
            close_covered = close_lead <= float(close_tolerance)
        boundary_complete = open_covered and close_covered
        if self.raw == 0:
            status: CoverageStatus = "empty"
        elif self.invalid or self.unexpected_gap_count:
            status = "ineligible"
        elif self.normalized == 0:
            status = "ineligible"
        elif boundary_complete:
            status = "complete"
        else:
            status = "partial"
        return FreshSessionInventory(
            anchor=self.anchor,
            symbol=self.symbol,
            bounds=bounds,
            raw_row_count=self.raw,
            valid_quote_count=self.valid,
            normalized_quote_count=self.normalized,
            duplicate_quote_count=self.duplicates,
            duplicate_group_count=self.duplicate_groups,
            invalid_quote_count=self.invalid,
            locked_quote_count=self.locked,
            first_timestamp_utc=self.first_timestamp_utc,
            last_timestamp_utc=self.last_timestamp_utc,
            spread=_distribution(self.spreads),
            interarrival_seconds=_distribution(self.interarrivals),
            noncontiguous_id_transition_count=self.noncontiguous_ids,
            missing_id_count=self.missing_ids,
            backward_id_transition_count=self.backward_ids,
            id_discontinuity_samples=tuple(self.id_samples),
            unexpected_gap_count=self.unexpected_gap_count,
            longest_unexpected_gap_seconds=self.longest_gap,
            total_unexpected_gap_seconds=self.total_gap,
            unexpected_gap_samples=tuple(self.gap_samples),
            invalid_quote_samples=tuple(self.invalid_samples),
            open_delay_seconds=open_delay,
            close_lead_seconds=close_lead,
            open_boundary_covered=open_covered,
            close_boundary_covered=close_covered,
            boundary_complete=boundary_complete,
            coverage_status=status,
        )


def _normalize_anchor(value: date | str) -> date:
    if isinstance(value, datetime):
        raise TypeError("anchor must be a date without a time")
    if isinstance(value, str):
        try:
            selected = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("anchor must be an ISO date") from exc
    elif isinstance(value, date):
        selected = value
    else:
        raise TypeError("anchor must be a date or ISO date string")
    broker_session_bounds(selected)  # validates weekday and schedule support
    return selected


def _distribution(values: list[float]) -> DistributionSummary:
    if not values:
        return DistributionSummary(0, None, None, None, None)
    ordered = sorted(values)
    count = len(ordered)
    middle = count // 2
    median_value = (
        ordered[middle]
        if count % 2
        else (ordered[middle - 1] + ordered[middle]) / 2.0
    )
    position = (count - 1) * 0.95
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        p95 = ordered[lower]
    else:
        weight = position - lower
        p95 = ordered[lower] * (1.0 - weight) + ordered[upper] * weight
    return DistributionSummary(
        count=count,
        minimum=ordered[0],
        median=median_value,
        p95=p95,
        maximum=ordered[-1],
    )


def _parse_timestamp(raw: str, source: Path, row_number: int) -> datetime:
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except Exception as exc:
        raise FreshDataError(
            f"{source}:{row_number}: invalid timestamp {raw!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FreshDataError(
            f"{source}:{row_number}: timestamp must include a timezone"
        )
    return parsed


def _parse_identity(
    row: dict[str, str], source: Path, row_number: int, expected_symbol: str | None
) -> tuple[int, str, datetime, datetime]:
    try:
        tick_id = int((row.get("id") or "").strip())
    except Exception as exc:
        raise FreshDataError(f"{source}:{row_number}: invalid id") from exc
    if tick_id < 0:
        raise FreshDataError(f"{source}:{row_number}: id must be non-negative")
    symbol = (row.get("symbol") or "").strip()
    if not symbol:
        raise FreshDataError(f"{source}:{row_number}: symbol is empty")
    if expected_symbol is not None and symbol != expected_symbol:
        raise FreshDataError(
            f"{source}:{row_number}: symbol {symbol!r} does not match "
            f"expected {expected_symbol!r}"
        )
    timestamp = _parse_timestamp(row.get("timestamp") or "", source, row_number)
    return tick_id, symbol, timestamp, timestamp.astimezone(_UTC)


def _parse_quote(
    row: dict[str, str],
) -> tuple[float | None, float | None, str | None]:
    try:
        bid = float((row.get("bid") or "").strip())
        ask = float((row.get("ask") or "").strip())
    except Exception:
        return None, None, "bid/ask is not numeric"
    if not math.isfinite(bid) or not math.isfinite(ask):
        return bid, ask, "bid/ask is not finite"
    if bid <= 0.0 or ask <= 0.0:
        return bid, ask, "bid/ask is not positive"
    if ask < bid:
        return bid, ask, "crossed quote"
    return bid, ask, None


def _path_sequence(paths: str | Path | Iterable[str | Path]) -> tuple[Path, ...]:
    if isinstance(paths, (str, Path)):
        selected = (Path(paths),)
    else:
        selected = tuple(Path(item) for item in paths)
    if not selected:
        raise ValueError("at least one CSV path is required")
    for path in selected:
        if not path.is_file():
            raise FileNotFoundError(path)
    return selected


def _row_chunks(
    reader: csv.DictReader, chunk_rows: int
) -> Iterator[list[tuple[int, dict[str, str]]]]:
    numbered = enumerate(reader, start=2)
    while True:
        chunk = list(islice(numbered, chunk_rows))
        if not chunk:
            return
        yield chunk


def scan_fresh_csv(
    paths: str | Path | Iterable[str | Path],
    *,
    config: FreshDataConfig,
    on_tick: Callable[[AssignedBrokerTick], None] | None = None,
) -> FreshCorpusInventory:
    """Scan ordered CSV exports with bounded memory and optional tick delivery.

    ``on_tick`` is called exactly once for every valid unique-id executable
    quote, including repeated quote values and quotes assigned to maintenance
    or no-session time.  It is never called for invalid rows.
    """

    settings = config
    if not isinstance(settings, FreshDataConfig):
        raise TypeError("config must be a FreshDataConfig")
    selected_paths = _path_sequence(paths)
    expected_anchors = {_normalize_anchor(item) for item in settings.expected_anchors}

    seen_ids = _IdIntervalSet()
    previous_key: tuple[datetime, int] | None = None
    previous_global_id: int | None = None
    previous_global_timestamp: datetime | None = None
    equal_time: datetime | None = None
    equal_time_keys: set[tuple[str, datetime, float, float]] = set()
    equal_time_duplicate_groups: set[tuple[str, datetime, float, float]] = set()

    statuses = {
        status: _StatusAccumulator(status)
        for status in ("session", "maintenance", "no_session")
    }
    sessions: list[FreshSessionInventory] = []
    current_session: _SessionAccumulator | None = None
    sources: list[FreshSourceInventory] = []
    global_id_samples: list[IdDiscontinuity] = []
    global_invalid_samples: list[InvalidQuoteSample] = []
    assignment_cursor = _AssignmentCursor()

    input_rows = 0
    valid_quotes = 0
    normalized_quotes = 0
    duplicate_quotes = 0
    duplicate_groups = 0
    invalid_quotes = 0
    locked_quotes = 0
    global_noncontiguous = 0
    global_missing = 0
    global_backward = 0
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None

    def session_accumulator(assignment: _AssignmentRoute) -> _SessionAccumulator:
        nonlocal current_session
        assert assignment.anchor is not None
        if current_session is None:
            current_session = _SessionAccumulator(
                anchor=assignment.anchor,
                symbol=settings.expected_symbol or "MULTI",
                config=settings,
            )
        elif assignment.anchor != current_session.anchor:
            if assignment.anchor < current_session.anchor:
                raise FreshDataError("session anchors regressed despite ordered timestamps")
            sessions.append(current_session.freeze())
            current_session = _SessionAccumulator(
                anchor=assignment.anchor,
                symbol=settings.expected_symbol or "MULTI",
                config=settings,
            )
        return current_session

    for path in selected_paths:
        source_state = _SourceAccumulator(path)
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            columns = set(reader.fieldnames or ())
            missing_columns = sorted(_REQUIRED_COLUMNS.difference(columns))
            if missing_columns:
                raise FreshDataError(
                    f"{path}: CSV is missing required columns: {missing_columns}"
                )
            for chunk in _row_chunks(reader, settings.chunk_rows):
                for row_number, row in chunk:
                    tick_id, symbol, timestamp, timestamp_utc = _parse_identity(
                        row, path, row_number, settings.expected_symbol
                    )
                    input_rows += 1
                    source_state.observe(timestamp_utc)
                    if first_timestamp is None:
                        first_timestamp = timestamp_utc
                    last_timestamp = timestamp_utc

                    if not seen_ids.add(tick_id):
                        raise FreshDataError(
                            f"{path}:{row_number}: duplicate tick id {tick_id}"
                        )
                    key = (timestamp_utc, tick_id)
                    if previous_key is not None and key <= previous_key:
                        raise FreshDataError(
                            f"{path}:{row_number}: rows must be strictly ordered by "
                            "(timestamp UTC, id)"
                        )
                    previous_key = key

                    if previous_global_id is not None and tick_id != previous_global_id + 1:
                        discontinuity = IdDiscontinuity(
                            left_id=previous_global_id,
                            right_id=tick_id,
                            left_timestamp_utc=previous_global_timestamp,  # type: ignore[arg-type]
                            right_timestamp_utc=timestamp_utc,
                            missing_id_count=max(0, tick_id - previous_global_id - 1),
                        )
                        global_noncontiguous += 1
                        global_missing += discontinuity.missing_id_count
                        if discontinuity.is_backward:
                            global_backward += 1
                        if len(global_id_samples) < settings.maximum_issue_samples:
                            global_id_samples.append(discontinuity)
                    previous_global_id = tick_id
                    previous_global_timestamp = timestamp_utc

                    assignment_route = assignment_cursor.route(timestamp, timestamp_utc)
                    status_state = statuses[assignment_route.status]
                    status_state.raw += 1
                    active_session = (
                        session_accumulator(assignment_route)
                        if assignment_route.status == "session"
                        else None
                    )
                    if active_session is not None:
                        if active_session.symbol == "MULTI":
                            active_session.symbol = symbol
                        elif active_session.symbol != symbol:
                            raise FreshDataError(
                                "multiple symbols in one session require separate scans"
                            )
                        active_session.observe_raw_id(tick_id, timestamp_utc)

                    bid, ask, invalid_reason = _parse_quote(row)
                    if invalid_reason is not None:
                        invalid_quotes += 1
                        status_state.invalid += 1
                        sample = InvalidQuoteSample(
                            source=str(path.resolve()),
                            row_number=row_number,
                            tick_id=tick_id,
                            symbol=symbol,
                            timestamp_utc=timestamp_utc,
                            reason=invalid_reason,
                        )
                        if len(global_invalid_samples) < settings.maximum_issue_samples:
                            global_invalid_samples.append(sample)
                        if active_session is not None:
                            active_session.observe_invalid(sample)
                        continue

                    assert bid is not None and ask is not None
                    valid_quotes += 1
                    status_state.valid += 1
                    if active_session is not None:
                        active_session.valid += 1
                    if ask == bid:
                        locked_quotes += 1
                        status_state.locked += 1
                        if active_session is not None:
                            active_session.locked += 1

                    if equal_time != timestamp_utc:
                        equal_time = timestamp_utc
                        equal_time_keys.clear()
                        equal_time_duplicate_groups.clear()
                    quote_key = (symbol, timestamp_utc, bid, ask)
                    if quote_key in equal_time_keys:
                        duplicate_quotes += 1
                        status_state.duplicates += 1
                        if active_session is not None:
                            active_session.duplicates += 1
                        if quote_key not in equal_time_duplicate_groups:
                            equal_time_duplicate_groups.add(quote_key)
                            duplicate_groups += 1
                            status_state.duplicate_groups += 1
                            if active_session is not None:
                                active_session.duplicate_groups += 1
                    equal_time_keys.add(quote_key)

                    broker_tick = BrokerTick(
                        id=tick_id,
                        symbol=symbol,
                        timestamp=timestamp,
                        bid=bid,
                        ask=ask,
                    )
                    normalized_quotes += 1
                    status_state.normalized += 1
                    if active_session is not None:
                        active_session.observe_normalized(broker_tick)
                    if on_tick is not None:
                        assignment = assignment_cursor.assignment(
                            assignment_route, timestamp, timestamp_utc
                        )
                        on_tick(
                            AssignedBrokerTick(
                                tick=broker_tick,
                                assignment=assignment,
                            )
                        )
        sources.append(source_state.freeze())

    if current_session is not None:
        sessions.append(current_session.freeze())

    observed = {item.anchor for item in sessions}
    empty_symbol = settings.expected_symbol or "UNKNOWN"
    for anchor in sorted(expected_anchors.difference(observed)):
        sessions.append(
            _SessionAccumulator(anchor=anchor, symbol=empty_symbol, config=settings).freeze()
        )
    sessions.sort(key=lambda item: item.anchor)

    return FreshCorpusInventory(
        sources=tuple(sources),
        sessions=tuple(sessions),
        input_row_count=input_rows,
        valid_quote_count=valid_quotes,
        normalized_quote_count=normalized_quotes,
        duplicate_quote_count=duplicate_quotes,
        duplicate_group_count=duplicate_groups,
        invalid_quote_count=invalid_quotes,
        locked_quote_count=locked_quotes,
        first_timestamp_utc=first_timestamp,
        last_timestamp_utc=last_timestamp,
        noncontiguous_id_transition_count=global_noncontiguous,
        missing_id_count=global_missing,
        backward_id_transition_count=global_backward,
        id_discontinuity_samples=tuple(global_id_samples),
        invalid_quote_samples=tuple(global_invalid_samples),
        session_rows=statuses["session"].freeze(),
        maintenance_rows=statuses["maintenance"].freeze(),
        no_session_rows=statuses["no_session"].freeze(),
        config=settings,
    )


__all__ = [
    "CoverageStatus",
    "DistributionSummary",
    "FreshCorpusInventory",
    "FreshDataConfig",
    "FreshDataError",
    "FreshSessionInventory",
    "FreshSourceInventory",
    "FreshStatusInventory",
    "IdDiscontinuity",
    "InvalidQuoteSample",
    "scan_fresh_csv",
]
