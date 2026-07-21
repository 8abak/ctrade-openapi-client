"""Exact-row entry decisions and causal momentum-weakening exits.

The module does not discover signals or select thresholds.  It turns an
already-frozen sequence of :class:`FrozenSignalEvent` objects into replay
decisions and, when explicitly configured, can request an early exit when a
causal feature loses side-aligned momentum or when the trade fails to make the
registered executable progress by a deadline.

Every feature row is bound to the corresponding database id and timestamp.
The source is consumed strictly in ``(timestamp, id)`` order and never reads a
later row while deciding on the current row.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
from datavis.research.fresh_replay import (
    FreshExecutionConfig,
    ReplayContext,
    ReplayDecision,
    STRICT_SCALP_LIMIT_MS,
)
from datavis.research.ticks import Tick


@dataclass(frozen=True, slots=True)
class CausalDecisionFeatureRow:
    """The two optional weakening measurements bound to one exact quote."""

    tick_index: int
    tick_id: int
    timestamp: datetime
    bid: float
    ask: float
    velocity: float | None
    acceleration: float | None

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
            raise ValueError("timestamp must be timezone-aware")
        if (
            not math.isfinite(self.bid)
            or not math.isfinite(self.ask)
            or self.bid <= 0.0
            or self.ask < self.bid
        ):
            raise ValueError("feature-row bid/ask must be finite positive quotes")
        for name in ("velocity", "acceleration"):
            value = getattr(self, name)
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{name} must be finite or None")


@dataclass(frozen=True, slots=True)
class MomentumWeakeningExitConfig:
    """Explicit early-invalidation rules; no parameter is inherited.

    ``velocity_exit_threshold`` and ``acceleration_exit_threshold`` are applied
    after multiplying a measurement by ``+1`` for a long and ``-1`` for a
    short.  An acceleration threshold of ``None`` means velocity alone defines
    weakening.  ``stall_deadline_ms`` and ``minimum_best_net_progress_per_unit``
    must either both be present or both be absent.
    """

    minimum_holding_ms: int
    weakening_confirmation_ms: int
    velocity_exit_threshold: float
    acceleration_exit_threshold: float | None
    stall_deadline_ms: int | None
    minimum_best_net_progress_per_unit: float | None

    def __post_init__(self) -> None:
        for name in ("minimum_holding_ms", "weakening_confirmation_ms"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
            if value >= STRICT_SCALP_LIMIT_MS:
                raise ValueError(f"{name} must be strictly below 60 seconds")
        if not math.isfinite(self.velocity_exit_threshold):
            raise ValueError("velocity_exit_threshold must be finite")
        if self.acceleration_exit_threshold is not None and not math.isfinite(
            self.acceleration_exit_threshold
        ):
            raise ValueError("acceleration_exit_threshold must be finite or None")
        if (self.stall_deadline_ms is None) != (
            self.minimum_best_net_progress_per_unit is None
        ):
            raise ValueError(
                "stall deadline and minimum progress must both be set or both be None"
            )
        if self.stall_deadline_ms is not None:
            if (
                not isinstance(self.stall_deadline_ms, int)
                or isinstance(self.stall_deadline_ms, bool)
                or self.stall_deadline_ms <= 0
                or self.stall_deadline_ms >= STRICT_SCALP_LIMIT_MS
            ):
                raise ValueError("stall_deadline_ms must be in (0, 60000)")
            if self.stall_deadline_ms < self.minimum_holding_ms:
                raise ValueError("stall_deadline_ms cannot precede minimum_holding_ms")
            assert self.minimum_best_net_progress_per_unit is not None
            if not math.isfinite(self.minimum_best_net_progress_per_unit):
                raise ValueError("minimum best net progress must be finite")


class FrozenSignalDecisionSource:
    """Replay source for frozen entries plus optional causal weakening exits."""

    def __init__(
        self,
        events: Iterable[FrozenSignalEvent],
        *,
        feature_rows: Iterable[CausalDecisionFeatureRow],
        weakening: MomentumWeakeningExitConfig | None,
        execution: FreshExecutionConfig,
        source_metadata: Mapping[str, object],
    ) -> None:
        if weakening is not None and not isinstance(
            weakening, MomentumWeakeningExitConfig
        ):
            raise TypeError("weakening must be MomentumWeakeningExitConfig or None")
        if not isinstance(execution, FreshExecutionConfig):
            raise TypeError("execution must be FreshExecutionConfig")
        if not isinstance(source_metadata, Mapping):
            raise TypeError("source_metadata must be a mapping")
        if weakening is not None:
            deadlines = [weakening.minimum_holding_ms]
            if weakening.stall_deadline_ms is not None:
                deadlines.append(weakening.stall_deadline_ms)
            if max(deadlines) + execution.exit_latency_ms + execution.maximum_exit_lag_ms > STRICT_SCALP_LIMIT_MS:
                raise ValueError(
                    "weakening deadlines plus exit latency and lag must not exceed 60 seconds"
                )

        selected_events = tuple(events)
        prior_index = -1
        event_indexes: set[int] = set()
        for position, event in enumerate(selected_events):
            if not isinstance(event, FrozenSignalEvent):
                raise TypeError(f"events[{position}] is not FrozenSignalEvent")
            if event.tick_index < prior_index:
                raise ValueError("events must be ordered by tick_index")
            if event.tick_index in event_indexes:
                raise ValueError("only one frozen signal is allowed per tick")
            prior_index = event.tick_index
            event_indexes.add(event.tick_index)

        selected_features = tuple(feature_rows)
        feature_indexes = [row.tick_index for row in selected_features]
        if feature_indexes != sorted(feature_indexes):
            raise ValueError("feature rows must be ordered by tick_index")
        if len(feature_indexes) != len(set(feature_indexes)):
            raise ValueError("only one decision-feature row is allowed per tick")

        self._events = selected_events
        self._events_by_index = {event.tick_index: event for event in selected_events}
        self._features = selected_features
        self._features_by_index = {row.tick_index: row for row in selected_features}
        self._weakening = weakening
        self._execution = execution
        self._source_metadata = dict(source_metadata)
        self._last_key: tuple[datetime, int] | None = None
        self._active_entry_tick_id: int | None = None
        self._weakening_since: datetime | None = None
        self._best_net_progress_per_unit = -math.inf

    def validate(self, ticks: Sequence[Tick]) -> None:
        """Reject stale signals or features before any replay state is mutated."""

        for event in self._events:
            if event.tick_index >= len(ticks):
                raise ValueError("event tick_index is outside the tick sequence")
            tick = ticks[event.tick_index]
            if event.tick_id != tick.id or event.timestamp != tick.timestamp:
                raise ValueError("event row is not bound to its exact replay tick")
        for row in self._features:
            if row.tick_index >= len(ticks):
                raise ValueError("feature tick_index is outside the tick sequence")
            tick = ticks[row.tick_index]
            if (
                row.tick_id != tick.id
                or row.timestamp != tick.timestamp
                or row.bid != tick.bid
                or row.ask != tick.ask
            ):
                raise ValueError("feature row is not bound to its exact replay tick")

    def _entry(self, event: FrozenSignalEvent) -> ReplayDecision:
        metadata = {
            **self._source_metadata,
            **dict(event.metadata),
            "signalTickIndex": event.tick_index,
            "signalTickId": event.tick_id,
            "signalTimestamp": event.timestamp.isoformat(),
            "signalSide": event.side,
        }
        return ReplayDecision(
            "enter_long" if event.side == "long" else "enter_short",
            "fresh-frozen-signal",
            metadata,
        )

    def _current_net_progress(self, tick: Tick, context: ReplayContext) -> float:
        assert context.position is not None
        position = context.position
        if position.side == "long":
            exit_fill = tick.bid - self._execution.slippage_per_side
            quote_progress = exit_fill - position.entry_fill_price
        else:
            exit_fill = tick.ask + self._execution.slippage_per_side
            quote_progress = position.entry_fill_price - exit_fill
        commission = 2.0 * self._execution.commission_per_unit_per_side
        return quote_progress - commission

    def _reset_position_state(self, entry_tick_id: int | None) -> None:
        self._active_entry_tick_id = entry_tick_id
        self._weakening_since = None
        self._best_net_progress_per_unit = -math.inf

    def on_tick(
        self,
        tick_index: int,
        tick: Tick,
        context: ReplayContext,
    ) -> ReplayDecision | None:
        key = (tick.timestamp, tick.id)
        if self._last_key is not None and key <= self._last_key:
            raise ValueError("ticks must be consumed in strict (timestamp, id) order")
        self._last_key = key

        if context.position is None:
            if self._active_entry_tick_id is not None:
                self._reset_position_state(None)
            if context.pending is not None or not context.allow_entry:
                return None
            event = self._events_by_index.get(tick_index)
            return self._entry(event) if event is not None else None

        position = context.position
        if self._active_entry_tick_id != position.entry_tick_id:
            self._reset_position_state(position.entry_tick_id)
        self._best_net_progress_per_unit = max(
            self._best_net_progress_per_unit,
            self._current_net_progress(tick, context),
        )
        if context.pending is not None or self._weakening is None:
            return None

        config = self._weakening
        holding_ms = (tick.timestamp - position.entry_timestamp).total_seconds() * 1_000.0
        if (
            config.stall_deadline_ms is not None
            and holding_ms >= config.stall_deadline_ms
            and self._best_net_progress_per_unit
            < config.minimum_best_net_progress_per_unit  # type: ignore[operator]
        ):
            return ReplayDecision(
                "exit",
                "fresh-exit:acceleration-stall",
                {
                    "triggerKind": "acceleration-stall",
                    "holdingAtDecisionMs": holding_ms,
                    "bestExecutableNetProgressPerUnit": self._best_net_progress_per_unit,
                    "requiredBestNetProgressPerUnit": config.minimum_best_net_progress_per_unit,
                },
            )

        if holding_ms < config.minimum_holding_ms:
            self._weakening_since = None
            return None
        row = self._features_by_index.get(tick_index)
        if row is None or row.velocity is None:
            self._weakening_since = None
            return None
        direction = 1.0 if position.side == "long" else -1.0
        aligned_velocity = direction * row.velocity
        weak = aligned_velocity <= config.velocity_exit_threshold
        aligned_acceleration: float | None = None
        if config.acceleration_exit_threshold is not None:
            if row.acceleration is None:
                weak = False
            else:
                aligned_acceleration = direction * row.acceleration
                weak = weak and (
                    aligned_acceleration <= config.acceleration_exit_threshold
                )
        if not weak:
            self._weakening_since = None
            return None
        if self._weakening_since is None:
            self._weakening_since = tick.timestamp
        confirmation_ms = (
            tick.timestamp - self._weakening_since
        ).total_seconds() * 1_000.0
        if confirmation_ms < config.weakening_confirmation_ms:
            return None
        return ReplayDecision(
            "exit",
            "fresh-exit:momentum-weakening",
            {
                "triggerKind": "momentum-weakening",
                "holdingAtDecisionMs": holding_ms,
                "weakeningConfirmationMs": confirmation_ms,
                "sideAlignedVelocity": aligned_velocity,
                "velocityExitThreshold": config.velocity_exit_threshold,
                "sideAlignedAcceleration": aligned_acceleration,
                "accelerationExitThreshold": config.acceleration_exit_threshold,
                "bestExecutableNetProgressPerUnit": self._best_net_progress_per_unit,
                "featureTickIndex": row.tick_index,
                "featureTickId": row.tick_id,
            },
        )
__all__ = [
    "CausalDecisionFeatureRow",
    "FrozenSignalDecisionSource",
    "MomentumWeakeningExitConfig",
]
