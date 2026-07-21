"""Causal, bid/ask-aware protective exits for fresh tick research.

This module deliberately does not choose entries or parameter values.  It wraps
an arbitrary :class:`~datavis.research.fresh_replay.CausalDecisionSource` and
adds an auditable stop, executable break-even ratchet, trailing stop, and time
exit.  Every threshold is explicit.

Stop prices are *trigger levels*, never assumed fill prices.  A long stop is
tested against the observed bid and a short stop against the observed ask.  On
a crossing, the policy returns an exit decision bound to that quote.  The replay
engine then fills the order only on a later eligible row, using its normal
latency, lag, slippage, and executable-side rules.  Consequently, a gap through
a stop remains a gap loss rather than an invented fill at the stop level.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Literal, Mapping, Protocol, Sequence, runtime_checkable

from datavis.research.fresh_replay import (
    CausalDecisionSource,
    FreshExecutionConfig,
    PositionView,
    ReplayContext,
    ReplayDecision,
    ReplayPreflightSource,
    STRICT_SCALP_LIMIT_MS,
)
from datavis.research.ticks import Tick


DistanceMode = Literal["fixed", "volatility"]
TrailingVolatilityBasis = Literal["entry", "current"]


@dataclass(frozen=True, slots=True)
class ExitDistance:
    """A positive price distance or positive multiple of causal volatility."""

    mode: DistanceMode
    value: float

    def __post_init__(self) -> None:
        if self.mode not in ("fixed", "volatility"):
            raise ValueError("distance mode must be 'fixed' or 'volatility'")
        if not math.isfinite(self.value) or self.value <= 0:
            raise ValueError("distance value must be finite and positive")

    def resolve(self, volatility: float | None) -> float:
        if self.mode == "fixed":
            return self.value
        if volatility is None:
            raise ValueError("causal volatility is required by a volatility distance")
        if not math.isfinite(volatility) or volatility <= 0:
            raise ValueError("causal volatility must be finite and positive")
        return self.value * volatility


@dataclass(frozen=True, slots=True)
class FreshExitPolicyConfig:
    """Explicit protective-exit rules; the class has no strategy defaults.

    Distances are measured in quote-price units.  Initial stops are anchored to
    the actual entry fill.  Break-even and trailing activation are measured from
    the entry fill to the currently executable close quote.  If volatility is
    used, activation distances are frozen from entry-time volatility.  A
    volatility trailing distance can either remain frozen at entry or be
    recalculated causally on every tick; in both cases an existing stop is never
    loosened.
    """

    initial_stop: ExitDistance
    break_even_activation: ExitDistance | None
    break_even_buffer_net_per_unit: float
    trailing_activation: ExitDistance | None
    trailing_distance: ExitDistance | None
    trailing_volatility_basis: TrailingVolatilityBasis
    maximum_holding_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.initial_stop, ExitDistance):
            raise ValueError("initial_stop must be ExitDistance")
        if self.break_even_activation is not None and not isinstance(
            self.break_even_activation, ExitDistance
        ):
            raise ValueError("break_even_activation must be ExitDistance or None")
        if (self.trailing_activation is None) != (self.trailing_distance is None):
            raise ValueError(
                "trailing_activation and trailing_distance must both be set or both be None"
            )
        if self.trailing_activation is not None and not isinstance(
            self.trailing_activation, ExitDistance
        ):
            raise ValueError("trailing_activation must be ExitDistance or None")
        if self.trailing_distance is not None and not isinstance(
            self.trailing_distance, ExitDistance
        ):
            raise ValueError("trailing_distance must be ExitDistance or None")
        if self.trailing_volatility_basis not in ("entry", "current"):
            raise ValueError("trailing_volatility_basis must be 'entry' or 'current'")
        if (
            not math.isfinite(self.break_even_buffer_net_per_unit)
            or self.break_even_buffer_net_per_unit < 0
        ):
            raise ValueError(
                "break_even_buffer_net_per_unit must be finite and non-negative"
            )
        if (
            not isinstance(self.maximum_holding_ms, int)
            or isinstance(self.maximum_holding_ms, bool)
            or self.maximum_holding_ms <= 0
            or self.maximum_holding_ms >= STRICT_SCALP_LIMIT_MS
        ):
            raise ValueError(
                "maximum_holding_ms must be a positive integer strictly below 60 seconds"
            )

    @property
    def requires_volatility(self) -> bool:
        rules = (
            self.initial_stop,
            self.break_even_activation,
            self.trailing_activation,
            self.trailing_distance,
        )
        return any(rule is not None and rule.mode == "volatility" for rule in rules)


@runtime_checkable
class CausalVolatilitySource(Protocol):
    """Sequential volatility source evaluated once for every replayed tick."""

    def on_tick(self, tick_index: int, tick: Tick) -> float | None:
        """Return volatility computed with information through ``tick`` only."""


@dataclass(frozen=True, slots=True)
class VolatilityRow:
    """One precomputed causal value bound to an exact replay row."""

    tick_index: int
    tick_id: int
    timestamp: datetime
    value: float | None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tick_index, int)
            or isinstance(self.tick_index, bool)
            or self.tick_index < 0
        ):
            raise ValueError("tick_index must be a non-negative integer")
        if self.timestamp.tzinfo is None:
            raise ValueError("volatility timestamp must include a timezone")
        if self.value is not None and (
            not math.isfinite(self.value) or self.value <= 0
        ):
            raise ValueError("volatility value must be None or finite and positive")


class VolatilityFrame:
    """Exact-row binding for values produced by a separately audited causal feature.

    The frame prevents a value from being attached to the wrong quote and
    enforces sequential access, including equal timestamps ordered by ID.  It
    cannot itself prove how a supplied value was calculated; that calculation
    must still be causal upstream.
    """

    def __init__(self, rows: Iterable[VolatilityRow]) -> None:
        materialized = tuple(rows)
        indexes = [row.tick_index for row in materialized]
        if indexes != sorted(indexes):
            raise ValueError("volatility rows must be sorted by tick_index")
        if len(indexes) != len(set(indexes)):
            raise ValueError("only one volatility value is allowed per tick")
        self._rows = materialized
        self._by_index = {row.tick_index: row for row in materialized}
        self._last_index: int | None = None
        self._last_key: tuple[datetime, int] | None = None

    def validate(self, ticks: Sequence[Tick]) -> None:
        for row in self._rows:
            if row.tick_index >= len(ticks):
                raise ValueError(
                    f"volatility tick_index {row.tick_index} is outside the tick sequence"
                )
            tick = ticks[row.tick_index]
            if row.tick_id != tick.id:
                raise ValueError(
                    f"volatility at index {row.tick_index} has tick_id {row.tick_id}, "
                    f"expected {tick.id}"
                )
            if row.timestamp != tick.timestamp:
                raise ValueError(
                    f"volatility at index {row.tick_index} has a mismatched timestamp"
                )

    def on_tick(self, tick_index: int, tick: Tick) -> float | None:
        if self._last_index is not None and tick_index <= self._last_index:
            raise ValueError("volatility frame must be consumed in increasing tick order")
        key = (tick.timestamp, tick.id)
        if self._last_key is not None and key <= self._last_key:
            raise ValueError("ticks must be strictly ordered by (timestamp, id)")
        self._last_index = tick_index
        self._last_key = key
        row = self._by_index.get(tick_index)
        if row is None:
            return None
        if row.tick_id != tick.id:
            raise ValueError(
                f"volatility at index {tick_index} has tick_id {row.tick_id}, expected {tick.id}"
            )
        if row.timestamp != tick.timestamp:
            raise ValueError(
                f"volatility at index {tick_index} has a mismatched timestamp"
            )
        return row.value


@dataclass(slots=True)
class _ExitState:
    side: Literal["long", "short"]
    entry_tick_id: int
    entry_fill_price: float
    entry_timestamp: datetime
    entry_volatility: float | None
    break_even_activation_distance: float | None
    trailing_activation_distance: float | None
    entry_trailing_distance: float | None
    stop_quote: float
    stop_origin: str
    best_executable_quote: float
    break_even_armed: bool = False
    trailing_armed: bool = False


def executable_break_even_quote(
    position: PositionView,
    execution: FreshExecutionConfig,
    *,
    buffer_net_per_unit: float,
) -> float:
    """Return the close-side quote that nets ``buffer_net_per_unit``.

    ``position.entry_fill_price`` already includes entry slippage.  The required
    quote additionally covers exit slippage, both commissions, and the requested
    buffer.  For a long this is a bid; for a short it is an ask.
    """

    if not math.isfinite(buffer_net_per_unit) or buffer_net_per_unit < 0:
        raise ValueError("buffer_net_per_unit must be finite and non-negative")
    remaining_cost = (
        execution.slippage_per_side
        + 2.0 * execution.commission_per_unit_per_side
        + buffer_net_per_unit
    )
    if position.side == "long":
        return position.entry_fill_price + remaining_cost
    return position.entry_fill_price - remaining_cost


def _validate_policy_decision(
    decision: ReplayDecision | None,
) -> ReplayDecision | None:
    if decision is not None and not isinstance(decision, ReplayDecision):
        raise TypeError("wrapped decision source must return ReplayDecision or None")
    return decision


class FreshProtectiveExitPolicy:
    """Wrap an entry/ discretionary-exit source with causal protective exits."""

    def __init__(
        self,
        wrapped: CausalDecisionSource,
        *,
        config: FreshExitPolicyConfig,
        execution: FreshExecutionConfig,
        volatility: CausalVolatilitySource | None,
    ) -> None:
        if not isinstance(wrapped, CausalDecisionSource):
            raise TypeError("wrapped must implement CausalDecisionSource")
        if not isinstance(config, FreshExitPolicyConfig):
            raise TypeError("config must be FreshExitPolicyConfig")
        if not isinstance(execution, FreshExecutionConfig):
            raise TypeError("execution must be FreshExecutionConfig")
        if config.requires_volatility and volatility is None:
            raise ValueError("a causal volatility source is required by this config")
        if volatility is not None and not isinstance(volatility, CausalVolatilitySource):
            raise TypeError("volatility must implement CausalVolatilitySource or be None")
        if (
            config.maximum_holding_ms + execution.exit_latency_ms
            >= execution.actual_fill_deadline_ms
        ):
            raise ValueError(
                "maximum_holding_ms plus exit_latency_ms must be strictly below "
                "the replay actual_fill_deadline_ms"
            )
        self._wrapped = wrapped
        self._config = config
        self._execution = execution
        self._volatility = volatility
        self._state: _ExitState | None = None
        self._last_key: tuple[datetime, int] | None = None

    @property
    def config(self) -> FreshExitPolicyConfig:
        return self._config

    def validate(self, ticks: Sequence[Tick]) -> None:
        """Forward replay preflight to every exact-row-bound wrapped source."""

        if isinstance(self._wrapped, ReplayPreflightSource):
            self._wrapped.validate(ticks)
        if isinstance(self._volatility, VolatilityFrame):
            self._volatility.validate(ticks)

    def _resolve_entry_state(
        self,
        position: PositionView,
        tick: Tick,
        volatility: float | None,
    ) -> _ExitState:
        initial_distance = self._config.initial_stop.resolve(volatility)
        break_even_distance = (
            self._config.break_even_activation.resolve(volatility)
            if self._config.break_even_activation is not None
            else None
        )
        trailing_activation = (
            self._config.trailing_activation.resolve(volatility)
            if self._config.trailing_activation is not None
            else None
        )
        entry_trailing_distance = (
            self._config.trailing_distance.resolve(volatility)
            if self._config.trailing_distance is not None
            else None
        )
        executable_quote = tick.bid if position.side == "long" else tick.ask
        stop = (
            position.entry_fill_price - initial_distance
            if position.side == "long"
            else position.entry_fill_price + initial_distance
        )
        return _ExitState(
            side=position.side,
            entry_tick_id=position.entry_tick_id,
            entry_fill_price=position.entry_fill_price,
            entry_timestamp=position.entry_timestamp,
            entry_volatility=volatility,
            break_even_activation_distance=break_even_distance,
            trailing_activation_distance=trailing_activation,
            entry_trailing_distance=entry_trailing_distance,
            stop_quote=stop,
            stop_origin="initial-stop",
            best_executable_quote=executable_quote,
        )

    def _ratchet_stop(
        self,
        state: _ExitState,
        position: PositionView,
        executable_quote: float,
        current_volatility: float | None,
    ) -> None:
        if state.side == "long":
            state.best_executable_quote = max(
                state.best_executable_quote, executable_quote
            )
            favourable = state.best_executable_quote - state.entry_fill_price
        else:
            state.best_executable_quote = min(
                state.best_executable_quote, executable_quote
            )
            favourable = state.entry_fill_price - state.best_executable_quote

        candidates: list[tuple[float, str]] = [(state.stop_quote, state.stop_origin)]
        if (
            state.break_even_activation_distance is not None
            and favourable >= state.break_even_activation_distance
        ):
            state.break_even_armed = True
            candidates.append(
                (
                    executable_break_even_quote(
                        position,
                        self._execution,
                        buffer_net_per_unit=(
                            self._config.break_even_buffer_net_per_unit
                        ),
                    ),
                    "executable-break-even",
                )
            )

        if (
            state.trailing_activation_distance is not None
            and favourable >= state.trailing_activation_distance
        ):
            state.trailing_armed = True
        if state.trailing_armed:
            assert self._config.trailing_distance is not None
            if (
                self._config.trailing_distance.mode == "volatility"
                and self._config.trailing_volatility_basis == "current"
            ):
                trailing_distance = self._config.trailing_distance.resolve(
                    current_volatility
                )
            else:
                assert state.entry_trailing_distance is not None
                trailing_distance = state.entry_trailing_distance
            trailing_stop = (
                state.best_executable_quote - trailing_distance
                if state.side == "long"
                else state.best_executable_quote + trailing_distance
            )
            candidates.append((trailing_stop, "trailing-stop"))

        if state.side == "long":
            new_stop, new_origin = max(candidates, key=lambda item: item[0])
            if new_stop > state.stop_quote:
                state.stop_quote = new_stop
                state.stop_origin = new_origin
        else:
            new_stop, new_origin = min(candidates, key=lambda item: item[0])
            if new_stop < state.stop_quote:
                state.stop_quote = new_stop
                state.stop_origin = new_origin

    def _exit_metadata(
        self,
        *,
        kind: str,
        tick_index: int,
        tick: Tick,
        executable_quote: float,
        state: _ExitState,
    ) -> Mapping[str, Any]:
        gap_through = (
            max(0.0, state.stop_quote - executable_quote)
            if state.side == "long"
            else max(0.0, executable_quote - state.stop_quote)
        )
        return {
            "exitPolicy": "fresh-protective-exit-v1",
            "triggerKind": kind,
            "triggerTickIndex": tick_index,
            "triggerTickId": tick.id,
            "triggerTimestamp": tick.timestamp.isoformat(),
            "triggerExecutableSide": "bid" if state.side == "long" else "ask",
            "triggerQuotePrice": executable_quote,
            "activeStopQuote": state.stop_quote,
            "activeStopOrigin": state.stop_origin,
            "gapThroughPrice": gap_through,
            "bestExecutableQuote": state.best_executable_quote,
            "breakEvenArmed": state.break_even_armed,
            "trailingArmed": state.trailing_armed,
            "fillContract": (
                "later eligible executable quote via fresh replay; trigger/stop price "
                "is never an assumed fill"
            ),
        }

    def on_tick(
        self,
        tick_index: int,
        tick: Tick,
        context: ReplayContext,
    ) -> ReplayDecision | None:
        key = (tick.timestamp, tick.id)
        if self._last_key is not None and key <= self._last_key:
            raise ValueError("ticks must be strictly ordered by (timestamp, id)")
        self._last_key = key

        current_volatility = (
            self._volatility.on_tick(tick_index, tick)
            if self._volatility is not None
            else None
        )

        if context.position is None:
            self._state = None
            if context.pending is not None:
                return None
            return _validate_policy_decision(
                self._wrapped.on_tick(tick_index, tick, context)
            )

        position = context.position
        if self._state is None or self._state.entry_tick_id != position.entry_tick_id:
            self._state = self._resolve_entry_state(
                position, tick, current_volatility
            )
        state = self._state
        executable_quote = tick.bid if position.side == "long" else tick.ask
        self._ratchet_stop(state, position, executable_quote, current_volatility)

        if context.pending is not None:
            return None

        stop_crossed = (
            executable_quote <= state.stop_quote
            if position.side == "long"
            else executable_quote >= state.stop_quote
        )
        if stop_crossed:
            metadata = self._exit_metadata(
                kind=state.stop_origin,
                tick_index=tick_index,
                tick=tick,
                executable_quote=executable_quote,
                state=state,
            )
            return ReplayDecision(
                "exit", f"fresh-exit:{state.stop_origin}", metadata
            )

        holding_ms = (
            tick.timestamp - position.entry_timestamp
        ).total_seconds() * 1_000.0
        if holding_ms >= self._config.maximum_holding_ms:
            metadata = dict(
                self._exit_metadata(
                    kind="time-stop",
                    tick_index=tick_index,
                    tick=tick,
                    executable_quote=executable_quote,
                    state=state,
                )
            )
            metadata["holdingAtDecisionMs"] = holding_ms
            metadata["maximumHoldingMs"] = self._config.maximum_holding_ms
            return ReplayDecision("exit", "fresh-exit:time-stop", metadata)

        return _validate_policy_decision(
            self._wrapped.on_tick(tick_index, tick, context)
        )


__all__ = [
    "CausalVolatilitySource",
    "DistanceMode",
    "ExitDistance",
    "FreshExitPolicyConfig",
    "FreshProtectiveExitPolicy",
    "TrailingVolatilityBasis",
    "VolatilityFrame",
    "VolatilityRow",
    "executable_break_even_quote",
]
