"""Strategy-neutral, causal replay for offline tick research.

The runner owns execution state only.  It contains no entry thresholds, market
features, or prior strategy imports.  A decision source is invoked once per tick,
after fills due on that tick have been processed, and cannot request a fill on the
same row.  Causality of a custom policy's internal calculations remains the
caller's responsibility; :class:`DecisionFrame` additionally binds every supplied
decision to an exact tick index, database ID, and timestamp.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from statistics import mean, median
from typing import Any, Iterable, Literal, Mapping, Protocol, Sequence, runtime_checkable

from datavis.research.ticks import Tick


Action = Literal["enter_long", "enter_short", "exit"]
Side = Literal["long", "short"]
PendingAction = Literal["enter_long", "enter_short", "exit"]
STRICT_SCALP_LIMIT_MS = 60_000


@dataclass(frozen=True, slots=True)
class FreshExecutionConfig:
    """Explicit execution assumptions; no prior-study value is implicit."""

    entry_latency_ms: int
    exit_latency_ms: int
    maximum_entry_lag_ms: int
    maximum_exit_lag_ms: int
    maximum_intertick_gap_ms: int
    actual_fill_deadline_ms: int
    cooldown_ms: int
    post_gap_rearm_ms: int
    quantity: float
    slippage_per_side: float
    commission_per_unit_per_side: float
    pnl_classification_tolerance: float

    def __post_init__(self) -> None:
        integer_values = {
            "entry_latency_ms": self.entry_latency_ms,
            "exit_latency_ms": self.exit_latency_ms,
            "maximum_entry_lag_ms": self.maximum_entry_lag_ms,
            "maximum_exit_lag_ms": self.maximum_exit_lag_ms,
            "cooldown_ms": self.cooldown_ms,
            "post_gap_rearm_ms": self.post_gap_rearm_ms,
        }
        for name, value in integer_values.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name, value in (
            ("maximum_intertick_gap_ms", self.maximum_intertick_gap_ms),
            ("actual_fill_deadline_ms", self.actual_fill_deadline_ms),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name, value in (
            ("quantity", self.quantity),
            ("slippage_per_side", self.slippage_per_side),
            ("commission_per_unit_per_side", self.commission_per_unit_per_side),
            ("pnl_classification_tolerance", self.pnl_classification_tolerance),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if (
            self.slippage_per_side < 0
            or self.commission_per_unit_per_side < 0
            or self.pnl_classification_tolerance < 0
        ):
            raise ValueError("execution costs cannot be negative")
        if self.exit_latency_ms >= self.actual_fill_deadline_ms:
            raise ValueError(
                "exit_latency_ms must be strictly below actual_fill_deadline_ms"
            )
        if self.actual_fill_deadline_ms > STRICT_SCALP_LIMIT_MS:
            raise ValueError(
                "actual_fill_deadline_ms cannot exceed the strict 60-second scalp limit"
            )


@dataclass(frozen=True, slots=True)
class ReplayBoundary:
    """Half-open scoring interval ``[start, end)`` for one fold or session."""

    start: datetime | None = None
    end: datetime | None = None
    name: str = "replay"
    input_complete_through_end: bool = False

    def __post_init__(self) -> None:
        for field_name, value in (("start", self.start), ("end", self.end)):
            if value is not None and value.tzinfo is None:
                raise ValueError(f"boundary {field_name} must include a timezone")
        if self.start is not None and self.end is not None and self.end <= self.start:
            raise ValueError("boundary end must be after start")
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("boundary name must be non-empty")
        if not isinstance(self.input_complete_through_end, bool):
            raise ValueError("input_complete_through_end must be boolean")
        if self.input_complete_through_end and self.end is None:
            raise ValueError("complete boundary coverage requires an end timestamp")


@dataclass(frozen=True, slots=True)
class ReplayDecision:
    action: Action
    reason: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action not in ("enter_long", "enter_short", "exit"):
            raise ValueError(f"unsupported action: {self.action!r}")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("decision reason must be non-empty")
        if not isinstance(self.metadata, Mapping):
            raise ValueError("decision metadata must be a mapping")


@dataclass(frozen=True, slots=True)
class DecisionRow:
    """A decision bound to one exact row in the supplied tick sequence."""

    tick_index: int
    tick_id: int
    timestamp: datetime
    decision: ReplayDecision

    def __post_init__(self) -> None:
        if not isinstance(self.tick_index, int) or isinstance(self.tick_index, bool):
            raise ValueError("tick_index must be an integer")
        if self.tick_index < 0:
            raise ValueError("tick_index cannot be negative")
        if self.timestamp.tzinfo is None:
            raise ValueError("decision timestamp must include a timezone")
        if not isinstance(self.decision, ReplayDecision):
            raise ValueError("decision must be ReplayDecision")


@dataclass(frozen=True, slots=True)
class PositionView:
    side: Side
    quantity: float
    entry_tick_id: int
    entry_timestamp: datetime
    entry_quote_price: float
    entry_fill_price: float


@dataclass(frozen=True, slots=True)
class PendingView:
    action: PendingAction
    created_tick_id: int
    created_timestamp: datetime
    ready_timestamp: datetime
    expires_timestamp: datetime


@dataclass(frozen=True, slots=True)
class ReplayContext:
    position: PositionView | None
    pending: PendingView | None
    allow_entry: bool
    cooldown_until: datetime | None
    gap_rearm_until: datetime | None


@runtime_checkable
class CausalDecisionSource(Protocol):
    def on_tick(
        self,
        tick_index: int,
        tick: Tick,
        context: ReplayContext,
    ) -> ReplayDecision | None:
        """Return a decision using information available through ``tick`` only."""


@runtime_checkable
class ReplayPreflightSource(Protocol):
    """Optional exact-input validation hook for wrapped decision sources."""

    def validate(self, ticks: Sequence[Tick]) -> None:
        """Reject stale or incorrectly bound decisions before replay begins."""


class DecisionFrame:
    """Deterministic decision source with exact tick-integrity binding."""

    def __init__(self, rows: Iterable[DecisionRow] = ()) -> None:
        materialized = tuple(rows)
        indexes = [row.tick_index for row in materialized]
        if indexes != sorted(indexes):
            raise ValueError("decision rows must be sorted by tick_index")
        if len(indexes) != len(set(indexes)):
            raise ValueError("only one decision is allowed per tick")
        self._rows = materialized
        self._by_index = {row.tick_index: row for row in materialized}

    @property
    def rows(self) -> tuple[DecisionRow, ...]:
        return self._rows

    def validate(self, ticks: Sequence[Tick]) -> None:
        for row in self._rows:
            if row.tick_index >= len(ticks):
                raise ValueError(
                    f"decision tick_index {row.tick_index} is outside the tick sequence"
                )
            tick = ticks[row.tick_index]
            if tick.id != row.tick_id:
                raise ValueError(
                    f"decision at index {row.tick_index} has tick_id {row.tick_id}, "
                    f"expected {tick.id}"
                )
            if tick.timestamp != row.timestamp:
                raise ValueError(
                    f"decision at index {row.tick_index} has a mismatched timestamp"
                )

    def on_tick(
        self,
        tick_index: int,
        tick: Tick,
        context: ReplayContext,
    ) -> ReplayDecision | None:
        del tick, context
        row = self._by_index.get(tick_index)
        return row.decision if row is not None else None


@dataclass(frozen=True, slots=True)
class ReplayTrade:
    side: Side
    quantity: float
    entry_decision_tick_id: int
    entry_decision_timestamp: datetime
    entry_fill_tick_id: int
    entry_fill_timestamp: datetime
    entry_quote_price: float
    entry_fill_price: float
    entry_decision_to_fill_ms: float
    entry_ready_to_fill_lag_ms: float
    exit_decision_tick_id: int
    exit_decision_timestamp: datetime
    exit_fill_tick_id: int
    exit_fill_timestamp: datetime
    exit_quote_price: float
    exit_fill_price: float
    exit_decision_to_fill_ms: float
    exit_ready_to_fill_lag_ms: float
    raw_quote_pnl: float
    slippage_cost: float
    pnl_after_slippage: float
    commission: float
    net_pnl: float
    holding_ms: float
    exit_reason: str
    entry_metadata: Mapping[str, Any]
    exit_metadata: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReplayCensor:
    side: Side
    entry_tick_id: int
    entry_timestamp: datetime
    entry_fill_price: float
    reason: str
    censor_timestamp: datetime
    pending_exit_decision_timestamp: datetime | None = None


@dataclass(frozen=True, slots=True)
class EntryCancellation:
    decision_tick_id: int
    decision_timestamp: datetime
    side: Side
    ready_timestamp: datetime
    observed_timestamp: datetime | None
    reason: str


@dataclass(frozen=True, slots=True)
class DecisionDisposition:
    tick_index: int
    tick_id: int
    timestamp: datetime
    action: Action
    disposition: str


@dataclass(frozen=True)
class FreshReplayResult:
    ticks_seen: int
    trades: tuple[ReplayTrade, ...]
    censors: tuple[ReplayCensor, ...]
    entry_cancellations: tuple[EntryCancellation, ...]
    decisions: tuple[DecisionDisposition, ...]
    halted: bool
    halt_reason: str | None
    boundary_reached: bool
    last_processed_timestamp: datetime | None
    config: FreshExecutionConfig
    boundary: ReplayBoundary

    def summary(self) -> dict[str, Any]:
        net_values = [trade.net_pnl for trade in self.trades]
        tolerance = self.config.pnl_classification_tolerance
        winners = [value for value in net_values if value > tolerance]
        losers = [value for value in net_values if value < -tolerance]
        gross_profit = sum(winners)
        gross_loss = -sum(losers)
        if gross_loss > 0:
            profit_factor: float | str | None = gross_profit / gross_loss
        elif gross_profit > 0:
            profit_factor = "Infinity"
        else:
            profit_factor = None
        return {
            "complete": not self.censors,
            "profitabilityValid": not self.censors,
            "ticksSeen": self.ticks_seen,
            "closedTrades": len(self.trades),
            "censoredPositions": len(self.censors),
            "entryCancellations": len(self.entry_cancellations),
            "halted": self.halted,
            "haltReason": self.halt_reason,
            "boundaryReached": self.boundary_reached,
            "netPnl": sum(net_values),
            "rawQuotePnl": sum(trade.raw_quote_pnl for trade in self.trades),
            "slippageCost": sum(trade.slippage_cost for trade in self.trades),
            "commission": sum(trade.commission for trade in self.trades),
            "positiveRate": len(winners) / len(net_values) if net_values else None,
            "flatTrades": len(net_values) - len(winners) - len(losers),
            "pnlClassificationTolerance": tolerance,
            "profitFactor": profit_factor,
            "meanNetPnl": mean(net_values) if net_values else None,
            "medianNetPnl": median(net_values) if net_values else None,
            "decisionDispositions": dict(
                sorted(Counter(item.disposition for item in self.decisions).items())
            ),
            "censorReasons": dict(
                sorted(Counter(item.reason for item in self.censors).items())
            ),
            "cancellationReasons": dict(
                sorted(Counter(item.reason for item in self.entry_cancellations).items())
            ),
            "assumptions": asdict(self.config),
            "boundary": asdict(self.boundary),
        }


@dataclass(slots=True)
class _Pending:
    action: PendingAction
    decision: ReplayDecision
    created_index: int
    created_tick_id: int
    created_timestamp: datetime
    ready_timestamp: datetime
    expires_timestamp: datetime

    def view(self) -> PendingView:
        return PendingView(
            action=self.action,
            created_tick_id=self.created_tick_id,
            created_timestamp=self.created_timestamp,
            ready_timestamp=self.ready_timestamp,
            expires_timestamp=self.expires_timestamp,
        )


@dataclass(slots=True)
class _Position:
    side: Side
    entry_decision: ReplayDecision
    entry_decision_tick_id: int
    entry_decision_timestamp: datetime
    entry_ready_timestamp: datetime
    entry_tick_id: int
    entry_timestamp: datetime
    entry_quote_price: float
    entry_fill_price: float

    def view(self, quantity: float) -> PositionView:
        return PositionView(
            side=self.side,
            quantity=quantity,
            entry_tick_id=self.entry_tick_id,
            entry_timestamp=self.entry_timestamp,
            entry_quote_price=self.entry_quote_price,
            entry_fill_price=self.entry_fill_price,
        )


def _milliseconds(delta: timedelta) -> float:
    return delta.total_seconds() * 1_000.0


def _validate_ticks(ticks: Iterable[Tick]) -> tuple[Tick, ...]:
    points = tuple(ticks)
    seen_ids: set[int] = set()
    for index, tick in enumerate(points):
        if not isinstance(tick, Tick):
            raise TypeError(f"tick at index {index} is not Tick")
        if tick.id in seen_ids:
            raise ValueError(f"duplicate tick id at index {index}: {tick.id}")
        seen_ids.add(tick.id)
        if index:
            previous = points[index - 1]
            if (tick.timestamp, tick.id) <= (previous.timestamp, previous.id):
                raise ValueError(
                    "ticks must be strictly ordered by (timestamp, id)"
                )
    return points


def _validate_decision(decision: ReplayDecision | None) -> ReplayDecision | None:
    if decision is not None and not isinstance(decision, ReplayDecision):
        raise TypeError("decision source must return ReplayDecision or None")
    return decision


def _pending_order(
    action: PendingAction,
    decision: ReplayDecision,
    index: int,
    tick: Tick,
    latency_ms: int,
    maximum_lag_ms: int,
) -> _Pending:
    ready = tick.timestamp + timedelta(milliseconds=latency_ms)
    return _Pending(
        action=action,
        decision=decision,
        created_index=index,
        created_tick_id=tick.id,
        created_timestamp=tick.timestamp,
        ready_timestamp=ready,
        expires_timestamp=ready + timedelta(milliseconds=maximum_lag_ms),
    )


def _entry_prices(tick: Tick, side: Side, config: FreshExecutionConfig) -> tuple[float, float]:
    quote = tick.ask if side == "long" else tick.bid
    fill = (
        quote + config.slippage_per_side
        if side == "long"
        else quote - config.slippage_per_side
    )
    return quote, fill


def _exit_prices(tick: Tick, side: Side, config: FreshExecutionConfig) -> tuple[float, float]:
    quote = tick.bid if side == "long" else tick.ask
    fill = (
        quote - config.slippage_per_side
        if side == "long"
        else quote + config.slippage_per_side
    )
    return quote, fill


def _close_trade(
    position: _Position,
    pending: _Pending,
    tick: Tick,
    config: FreshExecutionConfig,
) -> ReplayTrade:
    exit_quote, exit_fill = _exit_prices(tick, position.side, config)
    direction = 1.0 if position.side == "long" else -1.0
    raw_quote_pnl = (
        direction * (exit_quote - position.entry_quote_price) * config.quantity
    )
    pnl_after_slippage = (
        direction * (exit_fill - position.entry_fill_price) * config.quantity
    )
    slippage_cost = 2.0 * config.slippage_per_side * config.quantity
    commission = 2.0 * config.commission_per_unit_per_side * config.quantity
    return ReplayTrade(
        side=position.side,
        quantity=config.quantity,
        entry_decision_tick_id=position.entry_decision_tick_id,
        entry_decision_timestamp=position.entry_decision_timestamp,
        entry_fill_tick_id=position.entry_tick_id,
        entry_fill_timestamp=position.entry_timestamp,
        entry_quote_price=position.entry_quote_price,
        entry_fill_price=position.entry_fill_price,
        entry_decision_to_fill_ms=_milliseconds(
            position.entry_timestamp - position.entry_decision_timestamp
        ),
        entry_ready_to_fill_lag_ms=_milliseconds(
            position.entry_timestamp - position.entry_ready_timestamp
        ),
        exit_decision_tick_id=pending.created_tick_id,
        exit_decision_timestamp=pending.created_timestamp,
        exit_fill_tick_id=tick.id,
        exit_fill_timestamp=tick.timestamp,
        exit_quote_price=exit_quote,
        exit_fill_price=exit_fill,
        exit_decision_to_fill_ms=_milliseconds(tick.timestamp - pending.created_timestamp),
        exit_ready_to_fill_lag_ms=_milliseconds(tick.timestamp - pending.ready_timestamp),
        raw_quote_pnl=raw_quote_pnl,
        slippage_cost=slippage_cost,
        pnl_after_slippage=pnl_after_slippage,
        commission=commission,
        net_pnl=pnl_after_slippage - commission,
        holding_ms=_milliseconds(tick.timestamp - position.entry_timestamp),
        exit_reason=pending.decision.reason,
        entry_metadata=dict(position.entry_decision.metadata),
        exit_metadata=dict(pending.decision.metadata),
    )


def run_fresh_replay(
    ticks: Iterable[Tick],
    decisions: CausalDecisionSource,
    *,
    config: FreshExecutionConfig,
    boundary: ReplayBoundary | None = None,
) -> FreshReplayResult:
    """Replay decisions causally with no invented boundary or stale exit fills."""

    settings = config
    bounds = boundary or ReplayBoundary()
    points = _validate_ticks(ticks)
    if not isinstance(decisions, CausalDecisionSource):
        raise TypeError("decisions must implement CausalDecisionSource")
    if isinstance(decisions, ReplayPreflightSource):
        decisions.validate(points)

    trades: list[ReplayTrade] = []
    censors: list[ReplayCensor] = []
    cancellations: list[EntryCancellation] = []
    dispositions: list[DecisionDisposition] = []
    pending: _Pending | None = None
    position: _Position | None = None
    cooldown_until: datetime | None = None
    gap_rearm_until: datetime | None = None
    previous_tick: Tick | None = None
    last_processed: datetime | None = None
    ticks_seen = 0
    halted = False
    halt_reason: str | None = None
    boundary_reached = False

    def cancel_entry(reason: str, observed: datetime | None) -> None:
        nonlocal pending
        assert pending is not None and pending.action in ("enter_long", "enter_short")
        cancellations.append(
            EntryCancellation(
                decision_tick_id=pending.created_tick_id,
                decision_timestamp=pending.created_timestamp,
                side="long" if pending.action == "enter_long" else "short",
                ready_timestamp=pending.ready_timestamp,
                observed_timestamp=observed,
                reason=reason,
            )
        )
        pending = None

    def censor_open(reason: str, when: datetime) -> None:
        nonlocal position, pending, halted, halt_reason
        assert position is not None
        censors.append(
            ReplayCensor(
                side=position.side,
                entry_tick_id=position.entry_tick_id,
                entry_timestamp=position.entry_timestamp,
                entry_fill_price=position.entry_fill_price,
                reason=reason,
                censor_timestamp=when,
                pending_exit_decision_timestamp=(
                    pending.created_timestamp
                    if pending is not None and pending.action == "exit"
                    else None
                ),
            )
        )
        position = None
        pending = None
        halted = True
        halt_reason = reason

    for index, tick in enumerate(points):
        if bounds.start is not None and tick.timestamp < bounds.start:
            continue
        if bounds.end is not None and tick.timestamp >= bounds.end:
            boundary_reached = True
            if pending is not None and pending.action in ("enter_long", "enter_short"):
                cancel_entry("boundary-before-entry", bounds.end)
            if position is not None:
                reason = (
                    "boundary-pending-exit"
                    if pending is not None and pending.action == "exit"
                    else "boundary-open"
                )
                censor_open(reason, bounds.end)
            break

        ticks_seen += 1
        last_processed = tick.timestamp
        gap = bool(
            previous_tick is not None
            and _milliseconds(tick.timestamp - previous_tick.timestamp)
            > settings.maximum_intertick_gap_ms
        )
        previous_tick = tick
        if gap:
            if pending is not None and pending.action in ("enter_long", "enter_short"):
                cancel_entry("intertick-gap-before-entry", tick.timestamp)
            if position is not None:
                censor_open("intertick-gap", tick.timestamp)
                break
            gap_rearm_until = tick.timestamp + timedelta(
                milliseconds=settings.post_gap_rearm_ms
            )

        filled_entry_now = False
        if pending is not None and index > pending.created_index:
            if tick.timestamp > pending.expires_timestamp:
                if pending.action in ("enter_long", "enter_short"):
                    cancel_entry("stale-entry", tick.timestamp)
                elif position is not None:
                    censor_open("stale-exit", tick.timestamp)
                    break
                else:
                    pending = None
            elif tick.timestamp >= pending.ready_timestamp:
                if pending.action in ("enter_long", "enter_short"):
                    side: Side = "long" if pending.action == "enter_long" else "short"
                    quote_price, fill_price = _entry_prices(tick, side, settings)
                    position = _Position(
                        side=side,
                        entry_decision=pending.decision,
                        entry_decision_tick_id=pending.created_tick_id,
                        entry_decision_timestamp=pending.created_timestamp,
                        entry_ready_timestamp=pending.ready_timestamp,
                        entry_tick_id=tick.id,
                        entry_timestamp=tick.timestamp,
                        entry_quote_price=quote_price,
                        entry_fill_price=fill_price,
                    )
                    pending = None
                    filled_entry_now = True
                elif position is not None:
                    if (
                        tick.timestamp
                        >= position.entry_timestamp
                        + timedelta(milliseconds=settings.actual_fill_deadline_ms)
                    ):
                        censor_open("actual-fill-deadline", tick.timestamp)
                        break
                    trade = _close_trade(position, pending, tick, settings)
                    trades.append(trade)
                    cooldown_until = tick.timestamp + timedelta(
                        milliseconds=settings.cooldown_ms
                    )
                    position = None
                    pending = None
                else:
                    pending = None

        if position is not None and not filled_entry_now:
            if (
                tick.timestamp
                >= position.entry_timestamp
                + timedelta(milliseconds=settings.actual_fill_deadline_ms)
            ):
                censor_open("actual-fill-deadline", tick.timestamp)
                break

        context = ReplayContext(
            position=position.view(settings.quantity) if position is not None else None,
            pending=pending.view() if pending is not None else None,
            allow_entry=bool(
                position is None
                and pending is None
                and (cooldown_until is None or tick.timestamp >= cooldown_until)
                and (gap_rearm_until is None or tick.timestamp >= gap_rearm_until)
            ),
            cooldown_until=cooldown_until,
            gap_rearm_until=gap_rearm_until,
        )
        decision = _validate_decision(decisions.on_tick(index, tick, context))
        if decision is None:
            continue

        if decision.action in ("enter_long", "enter_short"):
            if position is not None or pending is not None:
                disposition = "ignored-busy"
            elif cooldown_until is not None and tick.timestamp < cooldown_until:
                disposition = "ignored-cooldown"
            elif gap_rearm_until is not None and tick.timestamp < gap_rearm_until:
                disposition = "ignored-gap-rearm"
            else:
                pending = _pending_order(
                    decision.action,
                    decision,
                    index,
                    tick,
                    settings.entry_latency_ms,
                    settings.maximum_entry_lag_ms,
                )
                disposition = "scheduled-entry"
        else:
            if position is None:
                disposition = "ignored-no-position"
            elif pending is not None:
                disposition = "ignored-pending-order"
            else:
                pending = _pending_order(
                    "exit",
                    decision,
                    index,
                    tick,
                    settings.exit_latency_ms,
                    settings.maximum_exit_lag_ms,
                )
                disposition = "scheduled-exit"
        dispositions.append(
            DecisionDisposition(
                tick_index=index,
                tick_id=tick.id,
                timestamp=tick.timestamp,
                action=decision.action,
                disposition=disposition,
            )
        )

    if (
        not halted
        and not boundary_reached
        and bounds.input_complete_through_end
        and bounds.end is not None
    ):
        boundary_reached = True
        if pending is not None and pending.action in ("enter_long", "enter_short"):
            cancel_entry("boundary-before-entry", bounds.end)
        if position is not None:
            reason = (
                "boundary-pending-exit"
                if pending is not None and pending.action == "exit"
                else "boundary-open"
            )
            censor_open(reason, bounds.end)

    if not halted:
        if pending is not None and pending.action in ("enter_long", "enter_short"):
            cancel_entry(
                "boundary-before-entry" if boundary_reached else "eof-before-entry",
                bounds.end if boundary_reached else last_processed,
            )
        if position is not None:
            if boundary_reached and bounds.end is not None:
                reason = (
                    "boundary-pending-exit"
                    if pending is not None and pending.action == "exit"
                    else "boundary-open"
                )
                censor_when = bounds.end
            else:
                reason = (
                    "eof-pending-exit"
                    if pending is not None and pending.action == "exit"
                    else "eof-open"
                )
                censor_when = last_processed or position.entry_timestamp
            censor_open(reason, censor_when)

    return FreshReplayResult(
        ticks_seen=ticks_seen,
        trades=tuple(trades),
        censors=tuple(censors),
        entry_cancellations=tuple(cancellations),
        decisions=tuple(dispositions),
        halted=halted,
        halt_reason=halt_reason,
        boundary_reached=boundary_reached,
        last_processed_timestamp=last_processed,
        config=settings,
        boundary=bounds,
    )


__all__ = [
    "Action",
    "CausalDecisionSource",
    "DecisionDisposition",
    "DecisionFrame",
    "DecisionRow",
    "EntryCancellation",
    "FreshExecutionConfig",
    "FreshReplayResult",
    "PendingView",
    "PositionView",
    "ReplayBoundary",
    "ReplayCensor",
    "ReplayContext",
    "ReplayDecision",
    "ReplayPreflightSource",
    "ReplayTrade",
    "Side",
    "STRICT_SCALP_LIMIT_MS",
    "run_fresh_replay",
]
