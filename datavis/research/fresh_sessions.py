"""Broker-session scheduling, quote normalization, and coverage auditing.

This is a strategy-neutral data-integrity layer.  A weekday anchor's scheduled
session is the half-open interval from 18:00 America/New_York on the preceding
calendar day through 17:00 America/New_York on the anchor day.  IANA timezone
conversion handles US and Australian daylight-saving transitions.

The schedule is deliberately a weekday schedule rather than a holiday
calendar.  Broker holidays and exceptional closes must be supplied by a later
calendar layer instead of being inferred from absent quotes.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Literal
from zoneinfo import ZoneInfo


_NEW_YORK = ZoneInfo("America/New_York")
_SYDNEY = ZoneInfo("Australia/Sydney")
_SESSION_START = time(18, 0)
_SESSION_END = time(17, 0)
_UTC = timezone.utc

BrokerTimestampStatus = Literal["session", "maintenance", "no_session"]


@dataclass(frozen=True, slots=True)
class BrokerTick:
    """One raw executable quote event identified by its database id."""

    id: int
    symbol: str
    timestamp: datetime
    bid: float
    ask: float

    def __post_init__(self) -> None:
        if not isinstance(self.id, int) or isinstance(self.id, bool) or self.id < 0:
            raise ValueError("tick id must be a non-negative integer")
        if not isinstance(self.symbol, str) or not self.symbol:
            raise ValueError("tick symbol must be a non-empty string")
        if self.symbol != self.symbol.strip():
            raise ValueError("tick symbol must not contain surrounding whitespace")
        if not isinstance(self.timestamp, datetime) or self.timestamp.tzinfo is None:
            raise ValueError("tick timestamp must be a timezone-aware datetime")
        if self.timestamp.utcoffset() is None:
            raise ValueError("tick timestamp must have a usable UTC offset")
        for name in ("bid", "ask"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"tick {name} must be a positive finite number")
            if not math.isfinite(float(value)) or float(value) <= 0.0:
                raise ValueError(f"tick {name} must be a positive finite number")
        if self.ask < self.bid:
            raise ValueError("crossed quote: ask is below bid")

    @property
    def timestamp_utc(self) -> datetime:
        return self.timestamp.astimezone(_UTC)

    @property
    def exact_quote_key(self) -> tuple[str, datetime, float, float]:
        """Repeated-quote diagnostic key; intentionally excludes the event id."""

        return (self.symbol, self.timestamp_utc, float(self.bid), float(self.ask))


@dataclass(frozen=True, slots=True)
class BrokerSessionBounds:
    anchor: date
    start_new_york: datetime
    end_new_york: datetime
    start_utc: datetime
    end_utc: datetime
    start_sydney: datetime
    end_sydney: datetime

    @property
    def duration_seconds(self) -> float:
        return (self.end_utc - self.start_utc).total_seconds()

    def contains(self, timestamp: datetime) -> bool:
        normalized = _aware_utc(timestamp, "timestamp")
        return self.start_utc <= normalized < self.end_utc


@dataclass(frozen=True, slots=True)
class BrokerTimestampAssignment:
    timestamp_utc: datetime
    timestamp_new_york: datetime
    timestamp_sydney: datetime
    status: BrokerTimestampStatus
    anchor: date | None
    bounds: BrokerSessionBounds | None


@dataclass(frozen=True, slots=True)
class DuplicateCollapseResult:
    """Legacy-named normalization result that never collapses unique-id events.

    ``duplicate_count`` and ``duplicate_group_count`` describe repeated quote
    values for audit purposes.  They are not defects.  ``dropped_ids`` remains
    for compatibility and is always empty.
    """

    ticks: tuple[BrokerTick, ...]
    input_count: int
    duplicate_count: int
    duplicate_group_count: int
    dropped_ids: tuple[int, ...]

    @property
    def retained_count(self) -> int:
        return len(self.ticks)


@dataclass(frozen=True, slots=True)
class AssignedBrokerTick:
    tick: BrokerTick
    assignment: BrokerTimestampAssignment


@dataclass(frozen=True, slots=True)
class BrokerTickPartition:
    normalization: DuplicateCollapseResult
    assignments: tuple[AssignedBrokerTick, ...]

    @property
    def session_anchors(self) -> tuple[date, ...]:
        anchors: list[date] = []
        seen: set[date] = set()
        for item in self.assignments:
            anchor = item.assignment.anchor
            if anchor is not None and anchor not in seen:
                anchors.append(anchor)
                seen.add(anchor)
        return tuple(anchors)

    @property
    def maintenance_ticks(self) -> tuple[BrokerTick, ...]:
        return tuple(
            item.tick
            for item in self.assignments
            if item.assignment.status == "maintenance"
        )

    @property
    def no_session_ticks(self) -> tuple[BrokerTick, ...]:
        return tuple(
            item.tick
            for item in self.assignments
            if item.assignment.status == "no_session"
        )

    def ticks_for_anchor(self, anchor: date | str) -> tuple[BrokerTick, ...]:
        selected = _weekday_anchor(anchor)
        return tuple(
            item.tick for item in self.assignments if item.assignment.anchor == selected
        )


@dataclass(frozen=True, slots=True)
class SessionAuditConfig:
    """Explicit data-coverage tolerances, unrelated to trading decisions."""

    open_tolerance_seconds: float
    close_tolerance_seconds: float
    friday_close_tolerance_seconds: float
    unexpected_gap_seconds: float

    def __post_init__(self) -> None:
        for name in (
            "open_tolerance_seconds",
            "close_tolerance_seconds",
            "friday_close_tolerance_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a non-negative finite number")
            if not math.isfinite(float(value)) or float(value) < 0.0:
                raise ValueError(f"{name} must be a non-negative finite number")
        value = self.unexpected_gap_seconds
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("unexpected_gap_seconds must be a positive finite number")
        if not math.isfinite(float(value)) or float(value) <= 0.0:
            raise ValueError("unexpected_gap_seconds must be a positive finite number")


@dataclass(frozen=True, slots=True)
class UnexpectedSessionGap:
    left_tick_id: int
    right_tick_id: int
    left_timestamp_utc: datetime
    right_timestamp_utc: datetime
    duration_seconds: float


@dataclass(frozen=True, slots=True)
class SessionCompletenessAudit:
    symbol: str | None
    bounds: BrokerSessionBounds
    raw_tick_count: int
    normalized_tick_count: int
    duplicate_count: int
    in_session_tick_count: int
    outside_requested_session_count: int
    outside_requested_session_ids: tuple[int, ...]
    first_timestamp_utc: datetime | None
    last_timestamp_utc: datetime | None
    open_delay_seconds: float | None
    close_lead_seconds: float | None
    open_boundary_covered: bool
    close_boundary_covered: bool
    boundary_complete: bool
    unexpected_gaps: tuple[UnexpectedSessionGap, ...]
    longest_unexpected_gap_seconds: float | None
    total_unexpected_gap_seconds: float
    has_unexpected_outage: bool
    is_complete: bool

    @property
    def unexpected_gap_count(self) -> int:
        return len(self.unexpected_gaps)


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must have a usable UTC offset")
    return value.astimezone(_UTC)


def _weekday_anchor(value: date | str) -> date:
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
    if selected.weekday() >= 5:
        raise ValueError("broker session anchor must be Monday through Friday")
    return selected


def broker_session_bounds(anchor: date | str) -> BrokerSessionBounds:
    """Return timezone-aware bounds for one weekday broker-session anchor."""

    selected = _weekday_anchor(anchor)
    previous_day = selected - timedelta(days=1)
    start_new_york = datetime.combine(previous_day, _SESSION_START, tzinfo=_NEW_YORK)
    end_new_york = datetime.combine(selected, _SESSION_END, tzinfo=_NEW_YORK)
    start_utc = start_new_york.astimezone(_UTC)
    end_utc = end_new_york.astimezone(_UTC)
    return BrokerSessionBounds(
        anchor=selected,
        start_new_york=start_new_york,
        end_new_york=end_new_york,
        start_utc=start_utc,
        end_utc=end_utc,
        start_sydney=start_utc.astimezone(_SYDNEY),
        end_sydney=end_utc.astimezone(_SYDNEY),
    )


def assign_broker_timestamp(timestamp: datetime) -> BrokerTimestampAssignment:
    """Assign a timestamp to a weekday anchor, maintenance, or no session.

    Maintenance is the scheduled 17:00-18:00 New York pause after Monday
    through Thursday sessions.  The closure after Friday and all remaining
    weekend time are reported as ``no_session``.
    """

    timestamp_utc = _aware_utc(timestamp, "timestamp")
    timestamp_new_york = timestamp_utc.astimezone(_NEW_YORK)
    timestamp_sydney = timestamp_utc.astimezone(_SYDNEY)
    local_seconds = (
        timestamp_new_york.hour * 3_600
        + timestamp_new_york.minute * 60
        + timestamp_new_york.second
        + timestamp_new_york.microsecond / 1_000_000
    )
    start_seconds = _SESSION_START.hour * 3_600
    end_seconds = _SESSION_END.hour * 3_600

    candidate: date | None = None
    if local_seconds >= start_seconds:
        following_day = timestamp_new_york.date() + timedelta(days=1)
        if following_day.weekday() < 5:
            candidate = following_day
    elif local_seconds < end_seconds:
        same_day = timestamp_new_york.date()
        if same_day.weekday() < 5:
            candidate = same_day

    if candidate is not None:
        bounds = broker_session_bounds(candidate)
        if not bounds.contains(timestamp_utc):  # pragma: no cover - invariant guard
            raise RuntimeError("timestamp assignment disagrees with broker bounds")
        return BrokerTimestampAssignment(
            timestamp_utc=timestamp_utc,
            timestamp_new_york=timestamp_new_york,
            timestamp_sydney=timestamp_sydney,
            status="session",
            anchor=candidate,
            bounds=bounds,
        )

    is_daily_maintenance = (
        timestamp_new_york.weekday() in (0, 1, 2, 3)
        and end_seconds <= local_seconds < start_seconds
    )
    return BrokerTimestampAssignment(
        timestamp_utc=timestamp_utc,
        timestamp_new_york=timestamp_new_york,
        timestamp_sydney=timestamp_sydney,
        status="maintenance" if is_daily_maintenance else "no_session",
        anchor=None,
        bounds=None,
    )


def validate_ordered_broker_ticks(
    ticks: Iterable[BrokerTick],
) -> tuple[BrokerTick, ...]:
    """Materialize ticks and enforce unique ids and causal observation order."""

    points = tuple(ticks)
    previous_key: tuple[datetime, int] | None = None
    seen_ids: set[int] = set()
    for index, tick in enumerate(points):
        if not isinstance(tick, BrokerTick):
            raise TypeError(f"ticks[{index}] is not a BrokerTick")
        if tick.id in seen_ids:
            raise ValueError(f"duplicate tick id at index {index}: {tick.id}")
        seen_ids.add(tick.id)
        key = (tick.timestamp_utc, tick.id)
        if previous_key is not None and key <= previous_key:
            raise ValueError(
                "raw ticks must be strictly ordered by (timestamp, id); "
                f"disorder at index {index}"
            )
        previous_key = key
    return points


def normalize_broker_ticks(ticks: Iterable[BrokerTick]) -> DuplicateCollapseResult:
    """Validate order and retain every quote event with a unique id.

    Repeated ``(symbol, timestamp, bid, ask)`` values are counted for audit but
    remain independent tick-volume units.  No sorting, price rounding, or quote
    collapse is performed.
    """

    points = validate_ordered_broker_ticks(ticks)
    seen: set[tuple[str, datetime, float, float]] = set()
    duplicate_groups: set[tuple[str, datetime, float, float]] = set()
    duplicate_count = 0
    for tick in points:
        key = tick.exact_quote_key
        if key in seen:
            duplicate_groups.add(key)
            duplicate_count += 1
        seen.add(key)
    return DuplicateCollapseResult(
        ticks=points,
        input_count=len(points),
        duplicate_count=duplicate_count,
        duplicate_group_count=len(duplicate_groups),
        dropped_ids=(),
    )


def partition_broker_ticks(ticks: Iterable[BrokerTick]) -> BrokerTickPartition:
    """Normalize an ordered raw batch and attach schedule assignments."""

    normalization = normalize_broker_ticks(ticks)
    assignments = tuple(
        AssignedBrokerTick(tick=tick, assignment=assign_broker_timestamp(tick.timestamp))
        for tick in normalization.ticks
    )
    return BrokerTickPartition(normalization=normalization, assignments=assignments)


def audit_session_completeness(
    ticks: Iterable[BrokerTick],
    anchor: date | str,
    *,
    config: SessionAuditConfig,
) -> SessionCompletenessAudit:
    """Audit scheduled-boundary coverage separately from intraday outages."""

    settings = config
    if not isinstance(settings, SessionAuditConfig):
        raise TypeError("config must be a SessionAuditConfig")
    bounds = broker_session_bounds(anchor)
    normalization = normalize_broker_ticks(ticks)
    symbols = {tick.symbol for tick in normalization.ticks}
    if len(symbols) > 1:
        raise ValueError("a completeness audit must contain exactly one symbol")
    symbol = next(iter(symbols), None)

    in_session: list[BrokerTick] = []
    outside_ids: list[int] = []
    for tick in normalization.ticks:
        if bounds.contains(tick.timestamp):
            in_session.append(tick)
        else:
            outside_ids.append(tick.id)

    if in_session:
        first_timestamp = in_session[0].timestamp_utc
        last_timestamp = in_session[-1].timestamp_utc
        open_delay = (first_timestamp - bounds.start_utc).total_seconds()
        close_lead = (bounds.end_utc - last_timestamp).total_seconds()
        open_covered = open_delay <= float(settings.open_tolerance_seconds)
        close_tolerance = (
            settings.friday_close_tolerance_seconds
            if bounds.anchor.weekday() == 4
            else settings.close_tolerance_seconds
        )
        close_covered = close_lead <= float(close_tolerance)
    else:
        first_timestamp = None
        last_timestamp = None
        open_delay = None
        close_lead = None
        open_covered = False
        close_covered = False

    unexpected: list[UnexpectedSessionGap] = []
    for left, right in zip(in_session, in_session[1:]):
        duration = (right.timestamp_utc - left.timestamp_utc).total_seconds()
        if duration > float(settings.unexpected_gap_seconds):
            unexpected.append(
                UnexpectedSessionGap(
                    left_tick_id=left.id,
                    right_tick_id=right.id,
                    left_timestamp_utc=left.timestamp_utc,
                    right_timestamp_utc=right.timestamp_utc,
                    duration_seconds=duration,
                )
            )

    boundary_complete = open_covered and close_covered
    has_outage = bool(unexpected)
    complete = boundary_complete and not has_outage and not outside_ids
    durations = [gap.duration_seconds for gap in unexpected]
    return SessionCompletenessAudit(
        symbol=symbol,
        bounds=bounds,
        raw_tick_count=normalization.input_count,
        normalized_tick_count=normalization.retained_count,
        duplicate_count=normalization.duplicate_count,
        in_session_tick_count=len(in_session),
        outside_requested_session_count=len(outside_ids),
        outside_requested_session_ids=tuple(outside_ids),
        first_timestamp_utc=first_timestamp,
        last_timestamp_utc=last_timestamp,
        open_delay_seconds=open_delay,
        close_lead_seconds=close_lead,
        open_boundary_covered=open_covered,
        close_boundary_covered=close_covered,
        boundary_complete=boundary_complete,
        unexpected_gaps=tuple(unexpected),
        longest_unexpected_gap_seconds=max(durations) if durations else None,
        total_unexpected_gap_seconds=sum(durations),
        has_unexpected_outage=has_outage,
        is_complete=complete,
    )


__all__ = [
    "AssignedBrokerTick",
    "BrokerSessionBounds",
    "BrokerTick",
    "BrokerTickPartition",
    "BrokerTimestampAssignment",
    "BrokerTimestampStatus",
    "DuplicateCollapseResult",
    "SessionAuditConfig",
    "SessionCompletenessAudit",
    "UnexpectedSessionGap",
    "assign_broker_timestamp",
    "audit_session_completeness",
    "broker_session_bounds",
    "normalize_broker_ticks",
    "partition_broker_ticks",
    "validate_ordered_broker_ticks",
]
