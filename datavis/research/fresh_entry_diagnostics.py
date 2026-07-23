"""Causal execution diagnostics for externally frozen entry events.

This module consumes signals; it never creates, scores, ranks, or tunes them.
Every event is bound to an exact source row.  Execution uses only a strictly
later ``(timestamp, id)`` quote, executable bid/ask sides, and explicit costs.
The observation interval is half-open and capped at 60 seconds, so a quote at
exactly the 60-second deadline is never used as a scalp outcome.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Literal, Mapping, Sequence

import numpy as np

from datavis.research.ticks import Tick


Side = Literal["long", "short"]
SchedulingMode = Literal["independent", "non_overlapping"]
BoundaryEndReason = Literal["boundary_end", "session_end", "fold_end"]
BarrierHit = Literal["profit", "loss"]
STRICT_SCALP_LIMIT_MS = 60_000
_CHECKPOINT_SECONDS = (1, 2, 5, 10, 20, 30, 60)


@dataclass(frozen=True, slots=True)
class FrozenSignalEvent:
    """One externally produced signal bound to an exact input quote row."""

    tick_index: int
    tick_id: int
    timestamp: datetime
    side: Side
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tick_index, int)
            or isinstance(self.tick_index, bool)
            or self.tick_index < 0
        ):
            raise ValueError("tick_index must be a non-negative integer")
        if not isinstance(self.tick_id, int) or isinstance(self.tick_id, bool):
            raise ValueError("tick_id must be an integer")
        if not isinstance(self.timestamp, datetime) or self.timestamp.tzinfo is None:
            raise ValueError("event timestamp must be timezone-aware")
        if self.timestamp.utcoffset() is None:
            raise ValueError("event timestamp must have a usable UTC offset")
        if self.side not in ("long", "short"):
            raise ValueError("event side must be 'long' or 'short'")
        if not isinstance(self.metadata, Mapping):
            raise ValueError("event metadata must be a mapping")


@dataclass(frozen=True, slots=True)
class EntryDiagnosticConfig:
    """Explicit execution and measurement assumptions, not signal parameters."""

    entry_latency_ms: int
    maximum_entry_lag_ms: int
    maximum_intertick_gap_ms: int
    diagnostic_horizon_ms: int
    quantity: float
    entry_slippage_per_unit: float
    exit_slippage_per_unit: float
    entry_commission_per_unit: float
    exit_commission_per_unit: float
    profit_barrier_net_per_unit: float | None = None
    loss_barrier_net_per_unit: float | None = None

    def __post_init__(self) -> None:
        for name in ("entry_latency_ms", "maximum_entry_lag_ms"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in ("maximum_intertick_gap_ms", "diagnostic_horizon_ms"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.diagnostic_horizon_ms > STRICT_SCALP_LIMIT_MS:
            raise ValueError("diagnostic_horizon_ms cannot exceed 60,000ms")
        for name in (
            "quantity",
            "entry_slippage_per_unit",
            "exit_slippage_per_unit",
            "entry_commission_per_unit",
            "exit_commission_per_unit",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be finite")
            if not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite")
        if self.quantity <= 0.0:
            raise ValueError("quantity must be positive")
        if any(
            getattr(self, name) < 0.0
            for name in (
                "entry_slippage_per_unit",
                "exit_slippage_per_unit",
                "entry_commission_per_unit",
                "exit_commission_per_unit",
            )
        ):
            raise ValueError("execution costs cannot be negative")
        for name in (
            "profit_barrier_net_per_unit",
            "loss_barrier_net_per_unit",
        ):
            value = getattr(self, name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a positive finite number or None")
            if not math.isfinite(float(value)) or float(value) <= 0.0:
                raise ValueError(f"{name} must be a positive finite number or None")


@dataclass(frozen=True, slots=True)
class EntrySchedulingConfig:
    """Optional causal event selection; independent mode diagnoses every event."""

    mode: SchedulingMode = "independent"
    cooldown_ms: int = 0

    def __post_init__(self) -> None:
        if self.mode not in ("independent", "non_overlapping"):
            raise ValueError("unsupported scheduling mode")
        if (
            not isinstance(self.cooldown_ms, int)
            or isinstance(self.cooldown_ms, bool)
            or self.cooldown_ms < 0
        ):
            raise ValueError("cooldown_ms must be a non-negative integer")
        if self.mode == "independent" and self.cooldown_ms:
            raise ValueError("cooldown requires non_overlapping scheduling")


@dataclass(frozen=True, slots=True)
class DiagnosticBoundary:
    """Optional half-open fold/session interval used only for censoring."""

    start: datetime | None = None
    end: datetime | None = None
    name: str = "diagnostic"
    end_reason: BoundaryEndReason = "boundary_end"
    input_complete_through_end: bool = False

    def __post_init__(self) -> None:
        for name in ("start", "end"):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, datetime) or value.tzinfo is None:
                    raise ValueError(f"boundary {name} must be timezone-aware")
                if value.utcoffset() is None:
                    raise ValueError(f"boundary {name} must have a usable UTC offset")
        if self.start is not None and self.end is not None and self.end <= self.start:
            raise ValueError("boundary end must be after start")
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("boundary name must be non-empty")
        if self.end_reason not in ("boundary_end", "session_end", "fold_end"):
            raise ValueError("unsupported boundary end_reason")
        if not isinstance(self.input_complete_through_end, bool):
            raise ValueError("input_complete_through_end must be boolean")
        if self.input_complete_through_end and self.end is None:
            raise ValueError("complete boundary coverage requires an end")


@dataclass(frozen=True, slots=True)
class EntryDiagnosticRejection:
    event_position: int
    event: FrozenSignalEvent
    reason: str
    observed_timestamp: datetime | None
    ready_timestamp: datetime
    expires_timestamp: datetime
    scheduling_release_timestamp: datetime


@dataclass(frozen=True, slots=True)
class FilledEntryDiagnostic:
    event_position: int
    event: FrozenSignalEvent
    fill_tick_index: int
    fill_tick_id: int
    fill_timestamp: datetime
    ready_timestamp: datetime
    expires_timestamp: datetime
    decision_to_fill_ms: float
    ready_to_fill_lag_ms: float
    decision_spread: float
    fill_spread: float
    entry_quote_price: float
    entry_fill_price: float
    initial_executable_quote_price: float
    initial_executable_fill_price: float
    explicit_round_trip_cost_per_unit: float
    initial_net_pnl_per_unit: float
    initial_net_pnl: float
    break_even_executable_quote_price: float
    cost_coverage_tick_index: int | None
    cost_coverage_tick_id: int | None
    cost_coverage_timestamp: datetime | None
    time_to_cost_coverage_ms: float | None
    decision_to_cost_coverage_ms: float | None
    cost_covered_by_1s: bool
    cost_covered_by_2s: bool
    cost_covered_by_5s: bool
    cost_covered_by_10s: bool
    cost_covered_by_20s: bool
    cost_covered_by_30s: bool
    cost_covered_by_60s: bool
    observed_quote_count: int
    observation_end_timestamp: datetime
    observation_end_reason: str
    scheduling_release_timestamp: datetime
    horizon_complete: bool
    censored: bool
    mae_before_coverage_per_unit: float
    mfe_before_coverage_per_unit: float
    mae_horizon_per_unit: float
    mfe_horizon_per_unit: float
    mae_before_coverage: float
    mfe_before_coverage: float
    mae_horizon: float
    mfe_horizon: float
    entry_efficiency: float
    profit_barrier_hit: bool
    profit_barrier_first_hit_ms: float | None
    loss_barrier_hit: bool
    loss_barrier_first_hit_ms: float | None
    first_barrier_hit: BarrierHit | None
    first_barrier_hit_tick_id: int | None
    first_barrier_hit_timestamp: datetime | None
    first_barrier_hit_ms: float | None


@dataclass(frozen=True, slots=True)
class FreshEntryDiagnosticsResult:
    diagnostics: tuple[FilledEntryDiagnostic, ...]
    rejections: tuple[EntryDiagnosticRejection, ...]
    rejected_reason_counts: Mapping[str, int]
    event_count: int

    @property
    def filled_count(self) -> int:
        return len(self.diagnostics)

    @property
    def rejected_count(self) -> int:
        return len(self.rejections)


_PREPARED_ENTRY_TAPE_TOKEN = object()
_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class PreparedEntryDiagnosticTape:
    """One validated immutable session tape prepared for repeated diagnostics.

    The compact timestamp and executable-price arrays are created once per
    session.  Repeated candidate batches reuse those arrays and lazily cache
    only the sparse positions of intertick gaps for each requested threshold.
    Construction is restricted to :func:`prepare_entry_diagnostic_tape`.
    """

    __slots__ = (
        "ticks",
        "_timestamp_us",
        "_bid",
        "_ask",
        "_intertick_us",
        "_gap_indexes_by_threshold",
    )

    def __init__(
        self,
        ticks: tuple[Tick, ...],
        timestamp_us: np.ndarray,
        bid: np.ndarray,
        ask: np.ndarray,
        intertick_us: np.ndarray,
        *,
        _token: object,
    ) -> None:
        if _token is not _PREPARED_ENTRY_TAPE_TOKEN:
            raise TypeError(
                "prepared entry diagnostic tapes must be created by the factory"
            )
        self.ticks = ticks
        self._timestamp_us = timestamp_us
        self._bid = bid
        self._ask = ask
        self._intertick_us = intertick_us
        self._gap_indexes_by_threshold: dict[int, np.ndarray] = {}

    @property
    def tick_count(self) -> int:
        return len(self.ticks)

    def _timestamp_index(
        self,
        timestamp: datetime,
        *,
        side: Literal["left", "right"],
        minimum: int,
    ) -> int:
        position = int(
            np.searchsorted(
                self._timestamp_us,
                _utc_microseconds(timestamp),
                side=side,
            )
        )
        return max(position, minimum)

    def _gap_indexes(self, maximum_intertick_gap_ms: int) -> np.ndarray:
        cached = self._gap_indexes_by_threshold.get(maximum_intertick_gap_ms)
        if cached is not None:
            return cached
        positions = np.flatnonzero(
            self._intertick_us > maximum_intertick_gap_ms * 1_000
        ).astype(np.int64, copy=False)
        positions.setflags(write=False)
        self._gap_indexes_by_threshold[maximum_intertick_gap_ms] = positions
        return positions

    def _next_gap_index(
        self,
        index: int,
        maximum_intertick_gap_ms: int,
    ) -> int:
        positions = self._gap_indexes(maximum_intertick_gap_ms)
        offset = int(np.searchsorted(positions, index, side="right"))
        if offset == positions.size:
            return len(self.ticks)
        return int(positions[offset])


@dataclass(frozen=True, slots=True)
class _ExecutableMark:
    tick_index: int
    tick: Tick
    elapsed_ms: float
    exit_quote_price: float
    exit_fill_price: float
    net_pnl_per_unit: float


@dataclass(frozen=True, slots=True)
class _AttemptOutcome:
    diagnostic: FilledEntryDiagnostic | None
    rejection: EntryDiagnosticRejection | None


def _milliseconds(value: timedelta) -> float:
    return value.total_seconds() * 1_000.0


def _utc_microseconds(value: datetime) -> int:
    delta = value.astimezone(timezone.utc) - _UTC_EPOCH
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000
        + delta.microseconds
    )


def _validate_ticks(ticks: Iterable[Tick]) -> tuple[Tick, ...]:
    points = tuple(ticks)
    for index, tick in enumerate(points):
        if not isinstance(tick, Tick):
            raise TypeError(f"ticks[{index}] is not a Tick")
        if index and (tick.timestamp, tick.id) <= (
            points[index - 1].timestamp,
            points[index - 1].id,
        ):
            raise ValueError("ticks must be strictly ordered by (timestamp, id)")
    return points


def _trusted_tick_tuple(ticks: Iterable[Tick]) -> tuple[Tick, ...]:
    """Accept a session-source tuple whose integrity was already validated.

    This private path exists only for ``FreshSessionTape.ticks``.  Requiring an
    existing tuple prevents a purported trusted call from silently consuming or
    materializing a mutable/generator input, while avoiding another full tape
    scan for the immutable tuple validated by ``FreshSessionTape``.
    """

    if not isinstance(ticks, tuple):
        raise TypeError(
            "_trusted_validated_ticks requires an already validated tick tuple"
        )
    return ticks


def prepare_entry_diagnostic_tape(
    ticks: Iterable[Tick],
    *,
    _trusted_validated_ticks: bool = False,
) -> PreparedEntryDiagnosticTape:
    """Validate and prepare one exact tick tuple for repeated entry evaluation.

    The trusted option has the same narrow contract as
    :func:`evaluate_frozen_entries`: it accepts only the immutable tuple already
    validated by ``FreshSessionTape``.  Identical quote values and timestamps
    remain distinct rows; their distinct IDs preserve their execution order and
    volume contribution.
    """

    if not isinstance(_trusted_validated_ticks, bool):
        raise TypeError("_trusted_validated_ticks must be boolean")
    points = (
        _trusted_tick_tuple(ticks)
        if _trusted_validated_ticks
        else _validate_ticks(ticks)
    )
    timestamp_us = np.fromiter(
        (_utc_microseconds(tick.timestamp) for tick in points),
        dtype=np.int64,
        count=len(points),
    )
    bid = np.fromiter(
        (tick.bid for tick in points),
        dtype=np.float64,
        count=len(points),
    )
    ask = np.fromiter(
        (tick.ask for tick in points),
        dtype=np.float64,
        count=len(points),
    )
    intertick_us = np.empty(len(points), dtype=np.int64)
    if len(points):
        intertick_us[0] = 0
        intertick_us[1:] = np.diff(timestamp_us)
    for values in (timestamp_us, bid, ask, intertick_us):
        values.setflags(write=False)
    return PreparedEntryDiagnosticTape(
        points,
        timestamp_us,
        bid,
        ask,
        intertick_us,
        _token=_PREPARED_ENTRY_TAPE_TOKEN,
    )


def _validate_events(
    events: Iterable[FrozenSignalEvent], points: Sequence[Tick]
) -> tuple[FrozenSignalEvent, ...]:
    selected = tuple(events)
    previous_index = -1
    for position, event in enumerate(selected):
        if not isinstance(event, FrozenSignalEvent):
            raise TypeError(f"events[{position}] is not a FrozenSignalEvent")
        if event.tick_index >= len(points):
            raise ValueError(f"event {position} tick_index is outside the tick sequence")
        source = points[event.tick_index]
        if source.id != event.tick_id or source.timestamp != event.timestamp:
            raise ValueError(
                f"event {position} does not match its bound tick index/id/timestamp"
            )
        if event.tick_index < previous_index:
            raise ValueError("events must be supplied in non-decreasing tick-index order")
        previous_index = event.tick_index
    return selected


def _entry_prices(tick: Tick, side: Side, config: EntryDiagnosticConfig) -> tuple[float, float]:
    quote = tick.ask if side == "long" else tick.bid
    fill = (
        quote + config.entry_slippage_per_unit
        if side == "long"
        else quote - config.entry_slippage_per_unit
    )
    return quote, fill


def _executable_mark(
    tick_index: int,
    tick: Tick,
    fill_tick: Tick,
    side: Side,
    entry_fill_price: float,
    config: EntryDiagnosticConfig,
) -> _ExecutableMark:
    exit_quote = tick.bid if side == "long" else tick.ask
    exit_fill = (
        exit_quote - config.exit_slippage_per_unit
        if side == "long"
        else exit_quote + config.exit_slippage_per_unit
    )
    direction = 1.0 if side == "long" else -1.0
    commission = (
        config.entry_commission_per_unit + config.exit_commission_per_unit
    )
    net_per_unit = direction * (exit_fill - entry_fill_price) - commission
    return _ExecutableMark(
        tick_index=tick_index,
        tick=tick,
        elapsed_ms=_milliseconds(tick.timestamp - fill_tick.timestamp),
        exit_quote_price=exit_quote,
        exit_fill_price=exit_fill,
        net_pnl_per_unit=net_per_unit,
    )


def _rejection(
    event_position: int,
    event: FrozenSignalEvent,
    reason: str,
    observed_timestamp: datetime | None,
    ready_timestamp: datetime,
    expires_timestamp: datetime,
    release_timestamp: datetime,
) -> _AttemptOutcome:
    return _AttemptOutcome(
        diagnostic=None,
        rejection=EntryDiagnosticRejection(
            event_position=event_position,
            event=event,
            reason=reason,
            observed_timestamp=observed_timestamp,
            ready_timestamp=ready_timestamp,
            expires_timestamp=expires_timestamp,
            scheduling_release_timestamp=release_timestamp,
        ),
    )


def _boundary_rejection_reason(boundary: DiagnosticBoundary) -> str:
    return f"{boundary.end_reason}_before_fill"


def _attempt_event(
    event_position: int,
    event: FrozenSignalEvent,
    points: Sequence[Tick],
    config: EntryDiagnosticConfig,
    boundary: DiagnosticBoundary,
) -> _AttemptOutcome:
    decision_tick = points[event.tick_index]
    ready_timestamp = event.timestamp + timedelta(milliseconds=config.entry_latency_ms)
    expires_timestamp = ready_timestamp + timedelta(
        milliseconds=config.maximum_entry_lag_ms
    )
    if (
        (boundary.start is not None and event.timestamp < boundary.start)
        or (boundary.end is not None and event.timestamp >= boundary.end)
    ):
        return _rejection(
            event_position,
            event,
            "outside_boundary",
            event.timestamp,
            ready_timestamp,
            expires_timestamp,
            event.timestamp,
        )

    fill_index: int | None = None
    previous = decision_tick
    for index in range(event.tick_index + 1, len(points)):
        tick = points[index]
        if boundary.end is not None and tick.timestamp >= boundary.end:
            return _rejection(
                event_position,
                event,
                _boundary_rejection_reason(boundary),
                boundary.end,
                ready_timestamp,
                expires_timestamp,
                boundary.end,
            )
        if _milliseconds(tick.timestamp - previous.timestamp) > config.maximum_intertick_gap_ms:
            return _rejection(
                event_position,
                event,
                "intertick_gap_before_fill",
                tick.timestamp,
                ready_timestamp,
                expires_timestamp,
                tick.timestamp,
            )
        previous = tick
        if tick.timestamp > expires_timestamp:
            return _rejection(
                event_position,
                event,
                "maximum_entry_lag_exceeded",
                tick.timestamp,
                ready_timestamp,
                expires_timestamp,
                expires_timestamp,
            )
        if tick.timestamp >= ready_timestamp:
            fill_index = index
            break

    if fill_index is None:
        if boundary.end is not None and boundary.input_complete_through_end:
            if boundary.end <= expires_timestamp:
                return _rejection(
                    event_position,
                    event,
                    _boundary_rejection_reason(boundary),
                    boundary.end,
                    ready_timestamp,
                    expires_timestamp,
                    boundary.end,
                )
            return _rejection(
                event_position,
                event,
                "maximum_entry_lag_exceeded",
                expires_timestamp,
                ready_timestamp,
                expires_timestamp,
                expires_timestamp,
            )
        return _rejection(
            event_position,
            event,
            "input_ended_before_fill",
            points[-1].timestamp if points else None,
            ready_timestamp,
            expires_timestamp,
            expires_timestamp,
        )

    fill_tick = points[fill_index]
    entry_quote, entry_fill = _entry_prices(fill_tick, event.side, config)
    horizon_end = fill_tick.timestamp + timedelta(
        milliseconds=config.diagnostic_horizon_ms
    )
    marks = [
        _executable_mark(
            fill_index,
            fill_tick,
            fill_tick,
            event.side,
            entry_fill,
            config,
        )
    ]
    observation_end_timestamp: datetime | None = None
    observation_end_reason: str | None = None
    scheduling_release_timestamp: datetime | None = None
    horizon_complete = False
    previous = fill_tick

    scheduled_stop = horizon_end
    scheduled_stop_reason = "horizon_complete"
    if boundary.end is not None and boundary.end <= horizon_end:
        scheduled_stop = boundary.end
        scheduled_stop_reason = boundary.end_reason

    for index in range(fill_index + 1, len(points)):
        tick = points[index]
        if tick.timestamp >= scheduled_stop:
            trailing_ms = _milliseconds(scheduled_stop - previous.timestamp)
            observation_end_timestamp = scheduled_stop
            scheduling_release_timestamp = scheduled_stop
            if trailing_ms > config.maximum_intertick_gap_ms:
                observation_end_reason = "intertick_gap"
            else:
                observation_end_reason = scheduled_stop_reason
                horizon_complete = scheduled_stop_reason == "horizon_complete"
            break
        elapsed_gap_ms = _milliseconds(tick.timestamp - previous.timestamp)
        if elapsed_gap_ms > config.maximum_intertick_gap_ms:
            observation_end_timestamp = tick.timestamp
            observation_end_reason = "intertick_gap"
            scheduling_release_timestamp = tick.timestamp
            break
        marks.append(
            _executable_mark(
                index,
                tick,
                fill_tick,
                event.side,
                entry_fill,
                config,
            )
        )
        previous = tick

    if observation_end_reason is None:
        if boundary.end is not None and boundary.input_complete_through_end:
            trailing_ms = _milliseconds(scheduled_stop - previous.timestamp)
            observation_end_timestamp = scheduled_stop
            scheduling_release_timestamp = scheduled_stop
            if trailing_ms > config.maximum_intertick_gap_ms:
                observation_end_reason = "intertick_gap"
            else:
                observation_end_reason = scheduled_stop_reason
                horizon_complete = scheduled_stop_reason == "horizon_complete"
        else:
            observation_end_timestamp = previous.timestamp
            observation_end_reason = "input_end"
            # With no later observation, a selected event remains reserved for
            # its fixed diagnostic window; a favorable result never releases it.
            scheduling_release_timestamp = horizon_end

    assert observation_end_timestamp is not None
    assert observation_end_reason is not None
    assert scheduling_release_timestamp is not None

    coverage_position = next(
        (
            index
            for index, mark in enumerate(marks)
            if mark.net_pnl_per_unit >= 0.0
        ),
        None,
    )
    coverage = marks[coverage_position] if coverage_position is not None else None
    coverage_ms = coverage.elapsed_ms if coverage is not None else None
    checkpoints = {
        seconds: bool(
            coverage_ms is not None
            and (
                coverage_ms < seconds * 1_000.0
                if seconds == 60
                else coverage_ms <= seconds * 1_000.0
            )
        )
        for seconds in _CHECKPOINT_SECONDS
    }

    before_coverage = marks if coverage_position is None else marks[:coverage_position]
    # An immediately covered entry has no pre-coverage quote interval.
    before_values = (
        [mark.net_pnl_per_unit for mark in before_coverage]
        if before_coverage
        else [0.0]
    )
    horizon_values = [mark.net_pnl_per_unit for mark in marks]
    mae_before = min(before_values)
    mfe_before = max(before_values)
    mae_horizon = min(horizon_values)
    mfe_horizon = max(horizon_values)
    favorable = max(mfe_horizon, 0.0)
    adverse = max(-mae_horizon, 0.0)
    efficiency_denominator = favorable + adverse
    entry_efficiency = (
        favorable / efficiency_denominator
        if efficiency_denominator > 0.0
        else math.nan
    )

    profit_position: int | None = None
    loss_position: int | None = None
    if config.profit_barrier_net_per_unit is not None:
        profit_position = next(
            (
                index
                for index, mark in enumerate(marks)
                if mark.net_pnl_per_unit >= config.profit_barrier_net_per_unit
            ),
            None,
        )
    if config.loss_barrier_net_per_unit is not None:
        loss_position = next(
            (
                index
                for index, mark in enumerate(marks)
                if mark.net_pnl_per_unit <= -config.loss_barrier_net_per_unit
            ),
            None,
        )
    profit_mark = marks[profit_position] if profit_position is not None else None
    loss_mark = marks[loss_position] if loss_position is not None else None
    first_barrier: BarrierHit | None = None
    first_barrier_mark: _ExecutableMark | None = None
    if profit_mark is not None and loss_mark is not None:
        if profit_position < loss_position:
            first_barrier, first_barrier_mark = "profit", profit_mark
        else:
            first_barrier, first_barrier_mark = "loss", loss_mark
    elif profit_mark is not None:
        first_barrier, first_barrier_mark = "profit", profit_mark
    elif loss_mark is not None:
        first_barrier, first_barrier_mark = "loss", loss_mark

    initial = marks[0]
    explicit_cost = (
        config.entry_slippage_per_unit
        + config.exit_slippage_per_unit
        + config.entry_commission_per_unit
        + config.exit_commission_per_unit
    )
    if event.side == "long":
        break_even_quote = (
            entry_fill
            + config.exit_slippage_per_unit
            + config.entry_commission_per_unit
            + config.exit_commission_per_unit
        )
    else:
        break_even_quote = (
            entry_fill
            - config.exit_slippage_per_unit
            - config.entry_commission_per_unit
            - config.exit_commission_per_unit
        )
    diagnostic = FilledEntryDiagnostic(
        event_position=event_position,
        event=event,
        fill_tick_index=fill_index,
        fill_tick_id=fill_tick.id,
        fill_timestamp=fill_tick.timestamp,
        ready_timestamp=ready_timestamp,
        expires_timestamp=expires_timestamp,
        decision_to_fill_ms=_milliseconds(fill_tick.timestamp - event.timestamp),
        ready_to_fill_lag_ms=_milliseconds(fill_tick.timestamp - ready_timestamp),
        decision_spread=decision_tick.spread,
        fill_spread=fill_tick.spread,
        entry_quote_price=entry_quote,
        entry_fill_price=entry_fill,
        initial_executable_quote_price=initial.exit_quote_price,
        initial_executable_fill_price=initial.exit_fill_price,
        explicit_round_trip_cost_per_unit=explicit_cost,
        initial_net_pnl_per_unit=initial.net_pnl_per_unit,
        initial_net_pnl=initial.net_pnl_per_unit * config.quantity,
        break_even_executable_quote_price=break_even_quote,
        cost_coverage_tick_index=coverage.tick_index if coverage is not None else None,
        cost_coverage_tick_id=coverage.tick.id if coverage is not None else None,
        cost_coverage_timestamp=coverage.tick.timestamp if coverage is not None else None,
        time_to_cost_coverage_ms=coverage_ms,
        decision_to_cost_coverage_ms=(
            _milliseconds(coverage.tick.timestamp - event.timestamp)
            if coverage is not None
            else None
        ),
        cost_covered_by_1s=checkpoints[1],
        cost_covered_by_2s=checkpoints[2],
        cost_covered_by_5s=checkpoints[5],
        cost_covered_by_10s=checkpoints[10],
        cost_covered_by_20s=checkpoints[20],
        cost_covered_by_30s=checkpoints[30],
        cost_covered_by_60s=checkpoints[60],
        observed_quote_count=len(marks),
        observation_end_timestamp=observation_end_timestamp,
        observation_end_reason=observation_end_reason,
        scheduling_release_timestamp=scheduling_release_timestamp,
        horizon_complete=horizon_complete,
        censored=not horizon_complete,
        mae_before_coverage_per_unit=mae_before,
        mfe_before_coverage_per_unit=mfe_before,
        mae_horizon_per_unit=mae_horizon,
        mfe_horizon_per_unit=mfe_horizon,
        mae_before_coverage=mae_before * config.quantity,
        mfe_before_coverage=mfe_before * config.quantity,
        mae_horizon=mae_horizon * config.quantity,
        mfe_horizon=mfe_horizon * config.quantity,
        entry_efficiency=entry_efficiency,
        profit_barrier_hit=profit_mark is not None,
        profit_barrier_first_hit_ms=(
            profit_mark.elapsed_ms if profit_mark is not None else None
        ),
        loss_barrier_hit=loss_mark is not None,
        loss_barrier_first_hit_ms=(
            loss_mark.elapsed_ms if loss_mark is not None else None
        ),
        first_barrier_hit=first_barrier,
        first_barrier_hit_tick_id=(
            first_barrier_mark.tick.id if first_barrier_mark is not None else None
        ),
        first_barrier_hit_timestamp=(
            first_barrier_mark.tick.timestamp
            if first_barrier_mark is not None
            else None
        ),
        first_barrier_hit_ms=(
            first_barrier_mark.elapsed_ms if first_barrier_mark is not None else None
        ),
    )
    return _AttemptOutcome(diagnostic=diagnostic, rejection=None)


def _first_true_position(values: np.ndarray) -> int | None:
    if not bool(np.any(values)):
        return None
    return int(np.argmax(values))


def _attempt_event_prepared(
    event_position: int,
    event: FrozenSignalEvent,
    prepared: PreparedEntryDiagnosticTape,
    config: EntryDiagnosticConfig,
    boundary: DiagnosticBoundary,
) -> _AttemptOutcome:
    """Vectorized equivalent of :func:`_attempt_event` on a prepared tape."""

    points = prepared.ticks
    decision_tick = points[event.tick_index]
    ready_timestamp = event.timestamp + timedelta(milliseconds=config.entry_latency_ms)
    expires_timestamp = ready_timestamp + timedelta(
        milliseconds=config.maximum_entry_lag_ms
    )
    if (
        (boundary.start is not None and event.timestamp < boundary.start)
        or (boundary.end is not None and event.timestamp >= boundary.end)
    ):
        return _rejection(
            event_position,
            event,
            "outside_boundary",
            event.timestamp,
            ready_timestamp,
            expires_timestamp,
            event.timestamp,
        )

    tick_count = len(points)
    first_later_index = event.tick_index + 1
    boundary_index = tick_count
    if boundary.end is not None:
        boundary_index = prepared._timestamp_index(
            boundary.end,
            side="left",
            minimum=first_later_index,
        )
    gap_index = prepared._next_gap_index(
        event.tick_index,
        config.maximum_intertick_gap_ms,
    )
    expiry_index = prepared._timestamp_index(
        expires_timestamp,
        side="right",
        minimum=first_later_index,
    )
    eligible_fill_index = prepared._timestamp_index(
        ready_timestamp,
        side="left",
        minimum=first_later_index,
    )
    first_decisive_index = min(
        boundary_index,
        gap_index,
        expiry_index,
        eligible_fill_index,
    )

    fill_index: int | None = None
    if first_decisive_index < tick_count:
        observed_tick = points[first_decisive_index]
        # These checks intentionally preserve the scalar loop's precedence when
        # two conditions occur on the same physical quote row.
        if boundary_index == first_decisive_index:
            assert boundary.end is not None
            return _rejection(
                event_position,
                event,
                _boundary_rejection_reason(boundary),
                boundary.end,
                ready_timestamp,
                expires_timestamp,
                boundary.end,
            )
        if gap_index == first_decisive_index:
            return _rejection(
                event_position,
                event,
                "intertick_gap_before_fill",
                observed_tick.timestamp,
                ready_timestamp,
                expires_timestamp,
                observed_tick.timestamp,
            )
        if expiry_index == first_decisive_index:
            return _rejection(
                event_position,
                event,
                "maximum_entry_lag_exceeded",
                observed_tick.timestamp,
                ready_timestamp,
                expires_timestamp,
                expires_timestamp,
            )
        assert eligible_fill_index == first_decisive_index
        fill_index = eligible_fill_index

    if fill_index is None:
        if boundary.end is not None and boundary.input_complete_through_end:
            if boundary.end <= expires_timestamp:
                return _rejection(
                    event_position,
                    event,
                    _boundary_rejection_reason(boundary),
                    boundary.end,
                    ready_timestamp,
                    expires_timestamp,
                    boundary.end,
                )
            return _rejection(
                event_position,
                event,
                "maximum_entry_lag_exceeded",
                expires_timestamp,
                ready_timestamp,
                expires_timestamp,
                expires_timestamp,
            )
        return _rejection(
            event_position,
            event,
            "input_ended_before_fill",
            points[-1].timestamp if points else None,
            ready_timestamp,
            expires_timestamp,
            expires_timestamp,
        )

    fill_tick = points[fill_index]
    entry_quote, entry_fill = _entry_prices(fill_tick, event.side, config)
    horizon_end = fill_tick.timestamp + timedelta(
        milliseconds=config.diagnostic_horizon_ms
    )
    scheduled_stop = horizon_end
    scheduled_stop_reason = "horizon_complete"
    if boundary.end is not None and boundary.end <= horizon_end:
        scheduled_stop = boundary.end
        scheduled_stop_reason = boundary.end_reason

    stop_index = prepared._timestamp_index(
        scheduled_stop,
        side="left",
        minimum=fill_index + 1,
    )
    observation_gap_index = prepared._next_gap_index(
        fill_index,
        config.maximum_intertick_gap_ms,
    )
    observation_end_timestamp: datetime
    observation_end_reason: str
    scheduling_release_timestamp: datetime
    horizon_complete = False

    if stop_index < tick_count and stop_index <= observation_gap_index:
        observed_stop_index = stop_index
        previous_tick = points[observed_stop_index - 1]
        trailing_ms = _milliseconds(scheduled_stop - previous_tick.timestamp)
        observation_end_timestamp = scheduled_stop
        scheduling_release_timestamp = scheduled_stop
        if trailing_ms > config.maximum_intertick_gap_ms:
            observation_end_reason = "intertick_gap"
        else:
            observation_end_reason = scheduled_stop_reason
            horizon_complete = scheduled_stop_reason == "horizon_complete"
    elif (
        observation_gap_index < tick_count
        and observation_gap_index < stop_index
    ):
        observed_stop_index = observation_gap_index
        gap_tick = points[observation_gap_index]
        observation_end_timestamp = gap_tick.timestamp
        observation_end_reason = "intertick_gap"
        scheduling_release_timestamp = gap_tick.timestamp
    else:
        observed_stop_index = tick_count
        previous_tick = points[observed_stop_index - 1]
        if boundary.end is not None and boundary.input_complete_through_end:
            trailing_ms = _milliseconds(scheduled_stop - previous_tick.timestamp)
            observation_end_timestamp = scheduled_stop
            scheduling_release_timestamp = scheduled_stop
            if trailing_ms > config.maximum_intertick_gap_ms:
                observation_end_reason = "intertick_gap"
            else:
                observation_end_reason = scheduled_stop_reason
                horizon_complete = scheduled_stop_reason == "horizon_complete"
        else:
            observation_end_timestamp = previous_tick.timestamp
            observation_end_reason = "input_end"
            scheduling_release_timestamp = horizon_end

    quote_slice = (
        prepared._bid[fill_index:observed_stop_index]
        if event.side == "long"
        else prepared._ask[fill_index:observed_stop_index]
    )
    net_values = np.array(quote_slice, dtype=np.float64, copy=True)
    if event.side == "long":
        net_values -= config.exit_slippage_per_unit
        direction = 1.0
    else:
        net_values += config.exit_slippage_per_unit
        direction = -1.0
    initial_exit_fill = float(net_values[0])
    commission = (
        config.entry_commission_per_unit + config.exit_commission_per_unit
    )
    net_values -= entry_fill
    net_values *= direction
    net_values -= commission

    coverage_position = _first_true_position(net_values >= 0.0)
    coverage_index = (
        fill_index + coverage_position
        if coverage_position is not None
        else None
    )
    coverage_tick = points[coverage_index] if coverage_index is not None else None
    coverage_ms = (
        _milliseconds(coverage_tick.timestamp - fill_tick.timestamp)
        if coverage_tick is not None
        else None
    )
    checkpoints = {
        seconds: bool(
            coverage_ms is not None
            and (
                coverage_ms < seconds * 1_000.0
                if seconds == 60
                else coverage_ms <= seconds * 1_000.0
            )
        )
        for seconds in _CHECKPOINT_SECONDS
    }

    if coverage_position == 0:
        mae_before = 0.0
        mfe_before = 0.0
    else:
        before_values = (
            net_values
            if coverage_position is None
            else net_values[:coverage_position]
        )
        mae_before = float(np.min(before_values))
        mfe_before = float(np.max(before_values))
    mae_horizon = float(np.min(net_values))
    mfe_horizon = float(np.max(net_values))
    favorable = max(mfe_horizon, 0.0)
    adverse = max(-mae_horizon, 0.0)
    efficiency_denominator = favorable + adverse
    entry_efficiency = (
        favorable / efficiency_denominator
        if efficiency_denominator > 0.0
        else math.nan
    )

    profit_position: int | None = None
    loss_position: int | None = None
    if config.profit_barrier_net_per_unit is not None:
        profit_position = _first_true_position(
            net_values >= config.profit_barrier_net_per_unit
        )
    if config.loss_barrier_net_per_unit is not None:
        loss_position = _first_true_position(
            net_values <= -config.loss_barrier_net_per_unit
        )
    profit_index = (
        fill_index + profit_position if profit_position is not None else None
    )
    loss_index = fill_index + loss_position if loss_position is not None else None
    first_barrier: BarrierHit | None = None
    first_barrier_index: int | None = None
    if profit_position is not None and loss_position is not None:
        if profit_position < loss_position:
            first_barrier, first_barrier_index = "profit", profit_index
        else:
            first_barrier, first_barrier_index = "loss", loss_index
    elif profit_position is not None:
        first_barrier, first_barrier_index = "profit", profit_index
    elif loss_position is not None:
        first_barrier, first_barrier_index = "loss", loss_index

    initial_quote = float(quote_slice[0])
    initial_net = float(net_values[0])
    explicit_cost = (
        config.entry_slippage_per_unit
        + config.exit_slippage_per_unit
        + config.entry_commission_per_unit
        + config.exit_commission_per_unit
    )
    if event.side == "long":
        break_even_quote = (
            entry_fill
            + config.exit_slippage_per_unit
            + config.entry_commission_per_unit
            + config.exit_commission_per_unit
        )
    else:
        break_even_quote = (
            entry_fill
            - config.exit_slippage_per_unit
            - config.entry_commission_per_unit
            - config.exit_commission_per_unit
        )

    def elapsed_at(index: int | None) -> float | None:
        if index is None:
            return None
        return _milliseconds(points[index].timestamp - fill_tick.timestamp)

    diagnostic = FilledEntryDiagnostic(
        event_position=event_position,
        event=event,
        fill_tick_index=fill_index,
        fill_tick_id=fill_tick.id,
        fill_timestamp=fill_tick.timestamp,
        ready_timestamp=ready_timestamp,
        expires_timestamp=expires_timestamp,
        decision_to_fill_ms=_milliseconds(fill_tick.timestamp - event.timestamp),
        ready_to_fill_lag_ms=_milliseconds(fill_tick.timestamp - ready_timestamp),
        decision_spread=decision_tick.spread,
        fill_spread=fill_tick.spread,
        entry_quote_price=entry_quote,
        entry_fill_price=entry_fill,
        initial_executable_quote_price=initial_quote,
        initial_executable_fill_price=initial_exit_fill,
        explicit_round_trip_cost_per_unit=explicit_cost,
        initial_net_pnl_per_unit=initial_net,
        initial_net_pnl=initial_net * config.quantity,
        break_even_executable_quote_price=break_even_quote,
        cost_coverage_tick_index=coverage_index,
        cost_coverage_tick_id=coverage_tick.id if coverage_tick is not None else None,
        cost_coverage_timestamp=(
            coverage_tick.timestamp if coverage_tick is not None else None
        ),
        time_to_cost_coverage_ms=coverage_ms,
        decision_to_cost_coverage_ms=(
            _milliseconds(coverage_tick.timestamp - event.timestamp)
            if coverage_tick is not None
            else None
        ),
        cost_covered_by_1s=checkpoints[1],
        cost_covered_by_2s=checkpoints[2],
        cost_covered_by_5s=checkpoints[5],
        cost_covered_by_10s=checkpoints[10],
        cost_covered_by_20s=checkpoints[20],
        cost_covered_by_30s=checkpoints[30],
        cost_covered_by_60s=checkpoints[60],
        observed_quote_count=observed_stop_index - fill_index,
        observation_end_timestamp=observation_end_timestamp,
        observation_end_reason=observation_end_reason,
        scheduling_release_timestamp=scheduling_release_timestamp,
        horizon_complete=horizon_complete,
        censored=not horizon_complete,
        mae_before_coverage_per_unit=mae_before,
        mfe_before_coverage_per_unit=mfe_before,
        mae_horizon_per_unit=mae_horizon,
        mfe_horizon_per_unit=mfe_horizon,
        mae_before_coverage=mae_before * config.quantity,
        mfe_before_coverage=mfe_before * config.quantity,
        mae_horizon=mae_horizon * config.quantity,
        mfe_horizon=mfe_horizon * config.quantity,
        entry_efficiency=entry_efficiency,
        profit_barrier_hit=profit_position is not None,
        profit_barrier_first_hit_ms=elapsed_at(profit_index),
        loss_barrier_hit=loss_position is not None,
        loss_barrier_first_hit_ms=elapsed_at(loss_index),
        first_barrier_hit=first_barrier,
        first_barrier_hit_tick_id=(
            points[first_barrier_index].id
            if first_barrier_index is not None
            else None
        ),
        first_barrier_hit_timestamp=(
            points[first_barrier_index].timestamp
            if first_barrier_index is not None
            else None
        ),
        first_barrier_hit_ms=elapsed_at(first_barrier_index),
    )
    return _AttemptOutcome(diagnostic=diagnostic, rejection=None)


def _scheduling_rejection(
    event_position: int,
    event: FrozenSignalEvent,
    reason: str,
    config: EntryDiagnosticConfig,
) -> EntryDiagnosticRejection:
    ready = event.timestamp + timedelta(milliseconds=config.entry_latency_ms)
    expires = ready + timedelta(milliseconds=config.maximum_entry_lag_ms)
    return EntryDiagnosticRejection(
        event_position=event_position,
        event=event,
        reason=reason,
        observed_timestamp=event.timestamp,
        ready_timestamp=ready,
        expires_timestamp=expires,
        scheduling_release_timestamp=event.timestamp,
    )


def evaluate_frozen_entries(
    ticks: Iterable[Tick],
    events: Iterable[FrozenSignalEvent],
    *,
    config: EntryDiagnosticConfig,
    boundary: DiagnosticBoundary | None = None,
    scheduling: EntrySchedulingConfig | None = None,
    _trusted_validated_ticks: bool = False,
) -> FreshEntryDiagnosticsResult:
    """Execute and diagnose frozen events without constructing entry signals.

    ``_trusted_validated_ticks`` is an internal performance path exclusively for
    an immutable ``FreshSessionTape.ticks`` tuple whose type, unique IDs, and
    strict ``(timestamp, id)`` order were already checked at construction.  The
    default path always performs the complete independent tape validation.
    """

    if not isinstance(config, EntryDiagnosticConfig):
        raise TypeError("config must be an EntryDiagnosticConfig")
    bounds = boundary or DiagnosticBoundary()
    if not isinstance(bounds, DiagnosticBoundary):
        raise TypeError("boundary must be a DiagnosticBoundary")
    schedule = scheduling or EntrySchedulingConfig()
    if not isinstance(schedule, EntrySchedulingConfig):
        raise TypeError("scheduling must be an EntrySchedulingConfig")
    if not isinstance(_trusted_validated_ticks, bool):
        raise TypeError("_trusted_validated_ticks must be boolean")
    points = (
        _trusted_tick_tuple(ticks)
        if _trusted_validated_ticks
        else _validate_ticks(ticks)
    )
    selected_events = _validate_events(events, points)

    diagnostics: list[FilledEntryDiagnostic] = []
    rejections: list[EntryDiagnosticRejection] = []
    active_until: datetime | None = None
    cooldown_until: datetime | None = None
    for event_position, event in enumerate(selected_events):
        if schedule.mode == "non_overlapping":
            if active_until is not None and event.timestamp < active_until:
                rejections.append(
                    _scheduling_rejection(
                        event_position,
                        event,
                        "scheduling_overlap",
                        config,
                    )
                )
                continue
            if cooldown_until is not None and event.timestamp < cooldown_until:
                rejections.append(
                    _scheduling_rejection(
                        event_position,
                        event,
                        "scheduling_cooldown",
                        config,
                    )
                )
                continue

        outcome = _attempt_event(event_position, event, points, config, bounds)
        if outcome.diagnostic is not None:
            diagnostic = outcome.diagnostic
            diagnostics.append(diagnostic)
            if schedule.mode == "non_overlapping":
                active_until = diagnostic.scheduling_release_timestamp
                cooldown_until = active_until + timedelta(
                    milliseconds=schedule.cooldown_ms
                )
        else:
            assert outcome.rejection is not None
            rejection = outcome.rejection
            rejections.append(rejection)
            if schedule.mode == "non_overlapping" and rejection.reason not in (
                "outside_boundary",
            ):
                active_until = rejection.scheduling_release_timestamp
                # An unfilled attempt is not a trade, so it does not consume the
                # post-trade cooldown after its pending interval is released.
                cooldown_until = active_until

    counts = Counter(rejection.reason for rejection in rejections)
    return FreshEntryDiagnosticsResult(
        diagnostics=tuple(diagnostics),
        rejections=tuple(rejections),
        rejected_reason_counts=dict(sorted(counts.items())),
        event_count=len(selected_events),
    )


def evaluate_prepared_frozen_entries(
    prepared: PreparedEntryDiagnosticTape,
    events: Iterable[FrozenSignalEvent],
    *,
    config: EntryDiagnosticConfig,
    boundary: DiagnosticBoundary | None = None,
    scheduling: EntrySchedulingConfig | None = None,
) -> FreshEntryDiagnosticsResult:
    """Diagnose one bounded event batch against a reusable prepared session.

    Results are intentionally identical to :func:`evaluate_frozen_entries`.
    Only tape preparation, timestamp searches, gap lookup, and quote-path
    reductions are accelerated; event scheduling and all causal execution
    semantics retain their scalar ordering.
    """

    if not isinstance(prepared, PreparedEntryDiagnosticTape):
        raise TypeError("prepared must be a PreparedEntryDiagnosticTape")
    if not isinstance(config, EntryDiagnosticConfig):
        raise TypeError("config must be an EntryDiagnosticConfig")
    bounds = boundary or DiagnosticBoundary()
    if not isinstance(bounds, DiagnosticBoundary):
        raise TypeError("boundary must be a DiagnosticBoundary")
    schedule = scheduling or EntrySchedulingConfig()
    if not isinstance(schedule, EntrySchedulingConfig):
        raise TypeError("scheduling must be an EntrySchedulingConfig")
    selected_events = _validate_events(events, prepared.ticks)

    diagnostics: list[FilledEntryDiagnostic] = []
    rejections: list[EntryDiagnosticRejection] = []
    active_until: datetime | None = None
    cooldown_until: datetime | None = None
    for event_position, event in enumerate(selected_events):
        if schedule.mode == "non_overlapping":
            if active_until is not None and event.timestamp < active_until:
                rejections.append(
                    _scheduling_rejection(
                        event_position,
                        event,
                        "scheduling_overlap",
                        config,
                    )
                )
                continue
            if cooldown_until is not None and event.timestamp < cooldown_until:
                rejections.append(
                    _scheduling_rejection(
                        event_position,
                        event,
                        "scheduling_cooldown",
                        config,
                    )
                )
                continue

        outcome = _attempt_event_prepared(
            event_position,
            event,
            prepared,
            config,
            bounds,
        )
        if outcome.diagnostic is not None:
            diagnostic = outcome.diagnostic
            diagnostics.append(diagnostic)
            if schedule.mode == "non_overlapping":
                active_until = diagnostic.scheduling_release_timestamp
                cooldown_until = active_until + timedelta(
                    milliseconds=schedule.cooldown_ms
                )
        else:
            assert outcome.rejection is not None
            rejection = outcome.rejection
            rejections.append(rejection)
            if schedule.mode == "non_overlapping" and rejection.reason not in (
                "outside_boundary",
            ):
                active_until = rejection.scheduling_release_timestamp
                cooldown_until = active_until

    counts = Counter(rejection.reason for rejection in rejections)
    return FreshEntryDiagnosticsResult(
        diagnostics=tuple(diagnostics),
        rejections=tuple(rejections),
        rejected_reason_counts=dict(sorted(counts.items())),
        event_count=len(selected_events),
    )


__all__ = [
    "BarrierHit",
    "BoundaryEndReason",
    "DiagnosticBoundary",
    "EntryDiagnosticConfig",
    "EntryDiagnosticRejection",
    "EntrySchedulingConfig",
    "FilledEntryDiagnostic",
    "FreshEntryDiagnosticsResult",
    "FrozenSignalEvent",
    "PreparedEntryDiagnosticTape",
    "SchedulingMode",
    "Side",
    "evaluate_frozen_entries",
    "evaluate_prepared_frozen_entries",
    "prepare_entry_diagnostic_tape",
]
