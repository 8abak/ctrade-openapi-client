"""Frozen, causal entry-event generators for the fresh XAUUSD study.

This module does not fit a threshold, inspect an outcome, or choose a winning
rule.  It consumes the causal frame produced by :mod:`fresh_features` and
applies configurations whose columns and finite thresholds were frozen by the
caller.  Each emitted event is tied to the exact positional row, tick ID, and
timestamp accepted by the execution diagnostic layer.

All five preregistered families are implemented as onset/state transitions.
An eligible condition that remains true for many quotes therefore emits one
event, rather than one event per quote.  Feed gaps, incomplete feature rows,
and missing configured measurements reset family state.  The implementations
iterate from left to right and never read a row after the decision row.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import lru_cache
from numbers import Integral, Real
from typing import Iterable, Iterator, Literal, Sequence, Union

try:
    from typing import TypeAlias
except ImportError:  # pragma: no cover - exercised by the Python 3.9 server
    TypeAlias = object

import numpy as np
import pandas as pd

from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
from datavis.research.ticks import Tick


SignalSide = Literal["long", "short"]
GenerationEngine = Literal["batch", "reference"]

TREND_ACCELERATION = "trend-acceleration"
PULLBACK_RESUMPTION = "pullback-resumption"
COUNTERTREND_PIVOT = "countertrend-pivot"
COMPRESSION_EXPANSION_BREAKOUT = "compression-expansion-breakout"
QUOTE_TRANSLATION_PRESSURE = "quote-translation-pressure"

_BASE_COLUMNS = (
    "tick_id",
    "timestamp",
    "bid",
    "ask",
    "mid",
    "gap_detected",
    "segment_id",
    "feature_ready",
)
_FORBIDDEN_COLUMN_TOKENS = (
    "future",
    "label",
    "target",
    "outcome",
    "profit",
    "coverage",
    "trade_pnl",
    "mfe",
    "mae",
)


def _candidate_id(value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("candidate_id must be a non-empty string")


def _column(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty feature column name")
    lowered = value.casefold()
    if any(token in lowered for token in _FORBIDDEN_COLUMN_TOKENS):
        raise ValueError(f"{name} refers to a forbidden outcome-like column")


def _finite(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite number")
    if not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite number")


def _positive(value: float, name: str) -> None:
    _finite(value, name)
    if float(value) <= 0.0:
        raise ValueError(f"{name} must be positive")


def _non_negative(value: float, name: str) -> None:
    _finite(value, name)
    if float(value) < 0.0:
        raise ValueError(f"{name} must be non-negative")


def _non_negative_int(value: int, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _positive_int(value: int, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class TrendAccelerationSignalConfig:
    """Established trend followed by a fresh acceleration onset."""

    candidate_id: str
    trend_column: str
    velocity_column: str
    acceleration_column: str
    translation_coherence_column: str
    minimum_trend: float
    minimum_velocity: float
    reset_velocity: float
    minimum_acceleration: float
    minimum_translation_coherence: float

    def __post_init__(self) -> None:
        _candidate_id(self.candidate_id)
        for name in (
            "trend_column",
            "velocity_column",
            "acceleration_column",
            "translation_coherence_column",
        ):
            _column(getattr(self, name), name)
        _positive(self.minimum_trend, "minimum_trend")
        _positive(self.minimum_velocity, "minimum_velocity")
        _finite(self.reset_velocity, "reset_velocity")
        if self.reset_velocity >= self.minimum_velocity:
            raise ValueError("reset_velocity must be below minimum_velocity")
        _non_negative(self.minimum_acceleration, "minimum_acceleration")
        _finite(
            self.minimum_translation_coherence,
            "minimum_translation_coherence",
        )
        if not -1.0 <= self.minimum_translation_coherence <= 1.0:
            raise ValueError("minimum_translation_coherence must be in [-1, 1]")


@dataclass(frozen=True, slots=True)
class PullbackResumptionSignalConfig:
    """Trend, smaller counter-move, and renewed trend-direction acceleration."""

    candidate_id: str
    trend_column: str
    movement_column: str
    acceleration_column: str
    depth_normalizer_column: str
    minimum_established_trend: float
    minimum_residual_trend: float
    minimum_pullback_speed: float
    minimum_pullback_depth_fraction: float
    maximum_pullback_depth_fraction: float
    minimum_resumption_speed: float
    minimum_resumption_acceleration: float
    minimum_pullback_duration_ms: int
    maximum_pullback_duration_ms: int

    def __post_init__(self) -> None:
        _candidate_id(self.candidate_id)
        for name in (
            "trend_column",
            "movement_column",
            "acceleration_column",
            "depth_normalizer_column",
        ):
            _column(getattr(self, name), name)
        _positive(self.minimum_established_trend, "minimum_established_trend")
        _finite(self.minimum_residual_trend, "minimum_residual_trend")
        if self.minimum_residual_trend > self.minimum_established_trend:
            raise ValueError(
                "minimum_residual_trend cannot exceed minimum_established_trend"
            )
        _positive(self.minimum_pullback_speed, "minimum_pullback_speed")
        _non_negative(
            self.minimum_pullback_depth_fraction,
            "minimum_pullback_depth_fraction",
        )
        _positive(
            self.maximum_pullback_depth_fraction,
            "maximum_pullback_depth_fraction",
        )
        if (
            self.minimum_pullback_depth_fraction
            >= self.maximum_pullback_depth_fraction
        ):
            raise ValueError(
                "minimum_pullback_depth_fraction must be below the maximum"
            )
        _positive(self.minimum_resumption_speed, "minimum_resumption_speed")
        _non_negative(
            self.minimum_resumption_acceleration,
            "minimum_resumption_acceleration",
        )
        _non_negative_int(
            self.minimum_pullback_duration_ms, "minimum_pullback_duration_ms"
        )
        _positive_int(
            self.maximum_pullback_duration_ms, "maximum_pullback_duration_ms"
        )
        if self.minimum_pullback_duration_ms >= self.maximum_pullback_duration_ms:
            raise ValueError("minimum pullback duration must be below the maximum")


@dataclass(frozen=True, slots=True)
class CountertrendPivotSignalConfig:
    """Trend-aligned pivot after a causal countertrend extreme and deceleration."""

    candidate_id: str
    trend_column: str
    movement_column: str
    acceleration_column: str
    depth_normalizer_column: str
    minimum_established_trend: float
    minimum_residual_trend: float
    minimum_pullback_speed: float
    minimum_pullback_depth_fraction: float
    maximum_pullback_depth_fraction: float
    minimum_rebound_fraction: float
    minimum_pivot_speed: float
    minimum_velocity_improvement: float
    minimum_pivot_acceleration: float
    minimum_pullback_duration_ms: int
    maximum_pullback_duration_ms: int

    def __post_init__(self) -> None:
        _candidate_id(self.candidate_id)
        for name in (
            "trend_column",
            "movement_column",
            "acceleration_column",
            "depth_normalizer_column",
        ):
            _column(getattr(self, name), name)
        _positive(self.minimum_established_trend, "minimum_established_trend")
        _finite(self.minimum_residual_trend, "minimum_residual_trend")
        if self.minimum_residual_trend > self.minimum_established_trend:
            raise ValueError(
                "minimum_residual_trend cannot exceed minimum_established_trend"
            )
        _positive(self.minimum_pullback_speed, "minimum_pullback_speed")
        _non_negative(
            self.minimum_pullback_depth_fraction,
            "minimum_pullback_depth_fraction",
        )
        _positive(
            self.maximum_pullback_depth_fraction,
            "maximum_pullback_depth_fraction",
        )
        if (
            self.minimum_pullback_depth_fraction
            >= self.maximum_pullback_depth_fraction
        ):
            raise ValueError(
                "minimum_pullback_depth_fraction must be below the maximum"
            )
        _non_negative(self.minimum_rebound_fraction, "minimum_rebound_fraction")
        _finite(self.minimum_pivot_speed, "minimum_pivot_speed")
        _non_negative(
            self.minimum_velocity_improvement, "minimum_velocity_improvement"
        )
        _non_negative(
            self.minimum_pivot_acceleration, "minimum_pivot_acceleration"
        )
        _non_negative_int(
            self.minimum_pullback_duration_ms, "minimum_pullback_duration_ms"
        )
        _positive_int(
            self.maximum_pullback_duration_ms, "maximum_pullback_duration_ms"
        )
        if self.minimum_pullback_duration_ms >= self.maximum_pullback_duration_ms:
            raise ValueError("minimum pullback duration must be below the maximum")


@dataclass(frozen=True, slots=True)
class CompressionExpansionBreakoutSignalConfig:
    """Causal compression followed by expansion through a frozen quote level."""

    candidate_id: str
    short_volatility_column: str
    long_volatility_column: str
    short_arrival_rate_column: str
    long_arrival_rate_column: str
    movement_column: str
    maximum_compression_ratio: float
    minimum_expansion_ratio: float
    minimum_arrival_rate_ratio: float
    minimum_breakout_speed: float
    breakout_buffer: float
    minimum_compression_rows: int
    maximum_breakout_wait_ms: int

    def __post_init__(self) -> None:
        _candidate_id(self.candidate_id)
        for name in (
            "short_volatility_column",
            "long_volatility_column",
            "short_arrival_rate_column",
            "long_arrival_rate_column",
            "movement_column",
        ):
            _column(getattr(self, name), name)
        _positive(self.maximum_compression_ratio, "maximum_compression_ratio")
        _positive(self.minimum_expansion_ratio, "minimum_expansion_ratio")
        if self.minimum_expansion_ratio <= self.maximum_compression_ratio:
            raise ValueError(
                "minimum_expansion_ratio must exceed maximum_compression_ratio"
            )
        _positive(self.minimum_arrival_rate_ratio, "minimum_arrival_rate_ratio")
        _positive(self.minimum_breakout_speed, "minimum_breakout_speed")
        _non_negative(self.breakout_buffer, "breakout_buffer")
        _positive_int(self.minimum_compression_rows, "minimum_compression_rows")
        _positive_int(self.maximum_breakout_wait_ms, "maximum_breakout_wait_ms")


@dataclass(frozen=True, slots=True)
class QuoteTranslationPressureSignalConfig:
    """Coherent side-aligned quote translation with rising tick pressure."""

    candidate_id: str
    translation_pressure_column: str
    translation_coherence_column: str
    movement_column: str
    persistence_column: str
    short_arrival_rate_column: str
    long_arrival_rate_column: str
    minimum_translation_pressure: float
    reset_translation_pressure: float
    minimum_translation_coherence: float
    minimum_movement_speed: float
    minimum_persistence: float
    minimum_arrival_rate_ratio: float

    def __post_init__(self) -> None:
        _candidate_id(self.candidate_id)
        for name in (
            "translation_pressure_column",
            "translation_coherence_column",
            "movement_column",
            "persistence_column",
            "short_arrival_rate_column",
            "long_arrival_rate_column",
        ):
            _column(getattr(self, name), name)
        _positive(
            self.minimum_translation_pressure, "minimum_translation_pressure"
        )
        if self.minimum_translation_pressure > 1.0:
            raise ValueError("minimum_translation_pressure cannot exceed 1")
        _finite(
            self.reset_translation_pressure, "reset_translation_pressure"
        )
        if self.reset_translation_pressure >= self.minimum_translation_pressure:
            raise ValueError(
                "reset_translation_pressure must be below the entry threshold"
            )
        _finite(
            self.minimum_translation_coherence,
            "minimum_translation_coherence",
        )
        if not -1.0 <= self.minimum_translation_coherence <= 1.0:
            raise ValueError("minimum_translation_coherence must be in [-1, 1]")
        _positive(self.minimum_movement_speed, "minimum_movement_speed")
        _finite(self.minimum_persistence, "minimum_persistence")
        if not 0.0 <= self.minimum_persistence <= 1.0:
            raise ValueError("minimum_persistence must be in [0, 1]")
        _positive(self.minimum_arrival_rate_ratio, "minimum_arrival_rate_ratio")


FreshSignalConfig: TypeAlias = (
    Union[
        TrendAccelerationSignalConfig,
        PullbackResumptionSignalConfig,
        CountertrendPivotSignalConfig,
        CompressionExpansionBreakoutSignalConfig,
        QuoteTranslationPressureSignalConfig,
    ]
)


def signal_config_fingerprint(config: FreshSignalConfig) -> str:
    """Return a stable hash for a completely explicit frozen rule."""

    if not isinstance(config, _SIGNAL_CONFIG_TYPES):
        raise TypeError("config must be a supported fresh signal configuration")
    return _cached_signal_config_fingerprint(config)


@lru_cache(maxsize=None)
def _cached_signal_config_fingerprint(config: FreshSignalConfig) -> str:
    payload = {
        "configType": type(config).__name__,
        "values": asdict(config),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


_SIGNAL_CONFIG_TYPES = (
    TrendAccelerationSignalConfig,
    PullbackResumptionSignalConfig,
    CountertrendPivotSignalConfig,
    CompressionExpansionBreakoutSignalConfig,
    QuoteTranslationPressureSignalConfig,
)


def _configured_columns(config: FreshSignalConfig) -> tuple[str, ...]:
    if isinstance(config, TrendAccelerationSignalConfig):
        return (
            config.trend_column,
            config.velocity_column,
            config.acceleration_column,
            config.translation_coherence_column,
        )
    if isinstance(config, (PullbackResumptionSignalConfig, CountertrendPivotSignalConfig)):
        return (
            config.trend_column,
            config.movement_column,
            config.acceleration_column,
            config.depth_normalizer_column,
        )
    if isinstance(config, CompressionExpansionBreakoutSignalConfig):
        return (
            config.short_volatility_column,
            config.long_volatility_column,
            config.short_arrival_rate_column,
            config.long_arrival_rate_column,
            config.movement_column,
        )
    if isinstance(config, QuoteTranslationPressureSignalConfig):
        return (
            config.translation_pressure_column,
            config.translation_coherence_column,
            config.movement_column,
            config.persistence_column,
            config.short_arrival_rate_column,
            config.long_arrival_rate_column,
        )
    raise TypeError("unsupported signal configuration")


def signal_required_columns(config: FreshSignalConfig) -> tuple[str, ...]:
    """Return the exact causal feature columns consumed by one frozen rule."""

    if not isinstance(config, _SIGNAL_CONFIG_TYPES):
        raise TypeError("config must be a supported fresh signal configuration")
    return _configured_columns(config)


@dataclass(frozen=True, slots=True)
class _PreparedFrame:
    frame: pd.DataFrame
    tick_ids: tuple[int, ...]
    timestamps: tuple[datetime, ...]
    timestamp_ns: np.ndarray
    bid: np.ndarray
    ask: np.ndarray
    mid: np.ndarray
    gaps: np.ndarray
    segments: np.ndarray
    ready: np.ndarray
    measurements: dict[str, np.ndarray]


def _timestamp(value: object, position: int) -> tuple[datetime, int]:
    try:
        point = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"timestamp at row {position} is invalid") from exc
    if pd.isna(point) or point.tzinfo is None or point.utcoffset() is None:
        raise ValueError(f"timestamp at row {position} must be timezone-aware")
    as_datetime = point.to_pydatetime()
    return as_datetime, int(point.tz_convert("UTC").value)


def _bool_array(frame: pd.DataFrame, column: str) -> np.ndarray:
    values = frame[column].to_numpy(copy=False)
    if any(not isinstance(item, (bool, np.bool_)) for item in values):
        raise ValueError(f"{column} must contain only booleans")
    return np.asarray(values, dtype=bool)


def _prepare_frame(
    features: pd.DataFrame,
    required_measurements: Iterable[str],
) -> _PreparedFrame:
    if not isinstance(features, pd.DataFrame):
        raise TypeError("features must be a pandas DataFrame")
    required = tuple(dict.fromkeys((*_BASE_COLUMNS, *required_measurements)))
    missing = [column for column in required if column not in features.columns]
    if missing:
        raise ValueError(f"feature frame is missing columns: {', '.join(missing)}")

    tick_ids: list[int] = []
    for position, value in enumerate(features["tick_id"].to_numpy(copy=False)):
        if not isinstance(value, Integral) or isinstance(value, (bool, np.bool_)):
            raise ValueError(f"tick_id at row {position} must be an integer")
        tick_ids.append(int(value))
    if len(set(tick_ids)) != len(tick_ids):
        raise ValueError("feature frame contains duplicate tick IDs")

    timestamps: list[datetime] = []
    timestamp_ns = np.empty(len(features), dtype=np.int64)
    previous_key: tuple[int, int] | None = None
    for position, (value, tick_id) in enumerate(
        zip(features["timestamp"].to_numpy(copy=False), tick_ids)
    ):
        point, point_ns = _timestamp(value, position)
        key = (point_ns, tick_id)
        if previous_key is not None and key <= previous_key:
            raise ValueError("feature rows must be strictly ordered by (timestamp, id)")
        previous_key = key
        timestamps.append(point)
        timestamp_ns[position] = point_ns

    numeric_base: dict[str, np.ndarray] = {}
    for column in ("bid", "ask", "mid"):
        try:
            values = features[column].to_numpy(dtype=float, copy=True)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{column} must be numeric") from exc
        if not np.all(np.isfinite(values)):
            raise ValueError(f"{column} must contain only finite values")
        numeric_base[column] = values
    if np.any(numeric_base["ask"] < numeric_base["bid"]):
        raise ValueError("ask must be greater than or equal to bid on every row")
    expected_mid = (numeric_base["bid"] + numeric_base["ask"]) / 2.0
    if not np.allclose(
        numeric_base["mid"], expected_mid, rtol=1e-12, atol=1e-12
    ):
        raise ValueError("mid must equal (bid + ask) / 2 on every row")

    gaps = _bool_array(features, "gap_detected")
    ready = _bool_array(features, "feature_ready")
    segments: list[int] = []
    for position, value in enumerate(features["segment_id"].to_numpy(copy=False)):
        if not isinstance(value, Integral) or isinstance(value, (bool, np.bool_)):
            raise ValueError(f"segment_id at row {position} must be an integer")
        segments.append(int(value))
    segment_array = np.asarray(segments, dtype=np.int64)
    if len(features):
        if gaps[0]:
            raise ValueError("the first feature row cannot be marked as a gap")
        for position in range(1, len(features)):
            delta = int(segment_array[position] - segment_array[position - 1])
            if gaps[position] != (delta == 1) or delta not in (0, 1):
                raise ValueError(
                    "gap_detected and segment_id must describe one-step segment resets"
                )
        if np.any(gaps & ready):
            raise ValueError("a gap row cannot already be feature-ready")

    measurements: dict[str, np.ndarray] = {}
    for column in dict.fromkeys(required_measurements):
        _column(column, "configured column")
        if pd.api.types.is_bool_dtype(features[column].dtype):
            raise ValueError(f"configured measurement {column} cannot be boolean")
        try:
            values = features[column].to_numpy(dtype=float, copy=True)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"configured measurement {column} must be numeric") from exc
        if np.any(np.isinf(values)):
            raise ValueError(f"configured measurement {column} contains infinity")
        measurements[column] = values

    return _PreparedFrame(
        frame=features,
        tick_ids=tuple(tick_ids),
        timestamps=tuple(timestamps),
        timestamp_ns=timestamp_ns,
        bid=numeric_base["bid"],
        ask=numeric_base["ask"],
        mid=numeric_base["mid"],
        gaps=gaps,
        segments=segment_array,
        ready=ready,
        measurements=measurements,
    )


def _values(prepared: _PreparedFrame, columns: Sequence[str], index: int) -> tuple[float, ...] | None:
    values = tuple(float(prepared.measurements[column][index]) for column in columns)
    return values if all(math.isfinite(value) for value in values) else None


def _event(
    prepared: _PreparedFrame,
    index: int,
    side: SignalSide,
    config: FreshSignalConfig,
    family: str,
    trigger: str,
    state_metadata: dict[str, float | int | str],
) -> FrozenSignalEvent:
    metadata: dict[str, object] = {
        "candidate_id": config.candidate_id,
        "family": family,
        "trigger": trigger,
        "config_sha256": signal_config_fingerprint(config),
        "segment_id": int(prepared.segments[index]),
    }
    metadata.update(state_metadata)
    return FrozenSignalEvent(
        tick_index=index,
        tick_id=prepared.tick_ids[index],
        timestamp=prepared.timestamps[index],
        side=side,
        metadata=metadata,
    )


def _unusable(prepared: _PreparedFrame, index: int) -> bool:
    return bool(prepared.gaps[index] or not prepared.ready[index])


def _trend_acceleration_reference(
    prepared: _PreparedFrame, config: TrendAccelerationSignalConfig
) -> list[FrozenSignalEvent]:
    columns = _configured_columns(config)
    latched = {1: False, -1: False}
    events: list[FrozenSignalEvent] = []
    for index in range(len(prepared.frame)):
        values = None if _unusable(prepared, index) else _values(prepared, columns, index)
        if values is None:
            latched = {1: False, -1: False}
            continue
        trend, velocity, acceleration, coherence = values
        for sign, side in ((1, "long"), (-1, "short")):
            signed_velocity = sign * velocity
            if latched[sign] and signed_velocity <= config.reset_velocity:
                latched[sign] = False
            if latched[sign]:
                continue
            if (
                sign * trend >= config.minimum_trend
                and signed_velocity >= config.minimum_velocity
                and sign * acceleration >= config.minimum_acceleration
                and coherence >= config.minimum_translation_coherence
            ):
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        TREND_ACCELERATION,
                        "inactive-to-accelerating",
                        {
                            "signed_trend": sign * trend,
                            "signed_velocity": signed_velocity,
                            "signed_acceleration": sign * acceleration,
                            "translation_coherence": coherence,
                        },
                    )
                )
                latched[sign] = True
    return events


def _first_onset_after_reset(enter: np.ndarray, reset: np.ndarray) -> np.ndarray:
    """Return first true ``enter`` row after each reset, including the prefix.

    This is the vector equivalent of the reference engine's hysteretic latch.
    Both inputs describe only the current row. Entries are grouped by the
    number of reset rows at or before them, so no suffix reset can alter an
    earlier onset.
    """

    if reset.shape != enter.shape:
        raise ValueError("enter and reset masks must have equal shape")
    if enter.size == 0:
        return np.empty(0, dtype=np.int64)
    enter_positions = np.flatnonzero(enter)
    if enter_positions.size == 0:
        return enter_positions
    reset_positions = np.flatnonzero(reset)
    # ``side='right'`` assigns an entry to the number of resets observed at or
    # before that row. Future reset positions therefore cannot change its
    # episode. Keeping the first entry in each episode exactly implements the
    # hysteretic reference latch without full-length integer state arrays.
    episodes = np.searchsorted(reset_positions, enter_positions, side="right")
    keep = np.empty(enter_positions.size, dtype=bool)
    keep[0] = True
    keep[1:] = episodes[1:] != episodes[:-1]
    return enter_positions[keep]


def _trend_acceleration_batch(
    prepared: _PreparedFrame, config: TrendAccelerationSignalConfig
) -> list[FrozenSignalEvent]:
    trend = prepared.measurements[config.trend_column]
    velocity = prepared.measurements[config.velocity_column]
    acceleration = prepared.measurements[config.acceleration_column]
    coherence = prepared.measurements[config.translation_coherence_column]
    valid = (
        prepared.ready
        & ~prepared.gaps
        & np.isfinite(trend)
        & np.isfinite(velocity)
        & np.isfinite(acceleration)
        & np.isfinite(coherence)
    )
    ranked: list[tuple[int, int, FrozenSignalEvent]] = []
    for side_order, (sign, side) in enumerate(((1, "long"), (-1, "short"))):
        signed_trend = sign * trend
        signed_velocity = sign * velocity
        signed_acceleration = sign * acceleration
        enter = (
            valid
            & (signed_trend >= config.minimum_trend)
            & (signed_velocity >= config.minimum_velocity)
            & (signed_acceleration >= config.minimum_acceleration)
            & (coherence >= config.minimum_translation_coherence)
        )
        reset = ~valid | (signed_velocity <= config.reset_velocity)
        for index_value in _first_onset_after_reset(enter, reset):
            index = int(index_value)
            ranked.append(
                (
                    index,
                    side_order,
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        TREND_ACCELERATION,
                        "inactive-to-accelerating",
                        {
                            "signed_trend": float(signed_trend[index]),
                            "signed_velocity": float(signed_velocity[index]),
                            "signed_acceleration": float(signed_acceleration[index]),
                            "translation_coherence": float(coherence[index]),
                        },
                    ),
                )
            )
    ranked.sort(key=lambda item: item[:2])
    return [item[2] for item in ranked]


@dataclass(slots=True)
class _PullbackState:
    mode: str = "idle"
    favorable_extreme: float = math.nan
    adverse_extreme: float = math.nan
    normalizer: float = math.nan
    started_ns: int = 0
    previous_signed_movement: float = math.nan


def _start_or_reset_trend(
    state: _PullbackState,
    *,
    established: bool,
    price: float,
) -> None:
    if established:
        state.mode = "trend"
        state.favorable_extreme = price
    else:
        state.mode = "idle"
        state.favorable_extreme = math.nan
    state.adverse_extreme = math.nan
    state.normalizer = math.nan
    state.started_ns = 0
    state.previous_signed_movement = math.nan


def _finite_measurement_arrays(
    prepared: _PreparedFrame,
    columns: Sequence[str],
) -> tuple[np.ndarray, tuple[np.ndarray, ...]]:
    """Return a shared causal usability mask and direct measurement arrays."""

    arrays = tuple(prepared.measurements[column] for column in columns)
    valid = prepared.ready & ~prepared.gaps
    for values in arrays:
        valid = valid & np.isfinite(values)
    return valid, arrays


def _pullback_resumption_reference(
    prepared: _PreparedFrame, config: PullbackResumptionSignalConfig
) -> list[FrozenSignalEvent]:
    columns = _configured_columns(config)
    states = {1: _PullbackState(), -1: _PullbackState()}
    events: list[FrozenSignalEvent] = []
    for index in range(len(prepared.frame)):
        values = None if _unusable(prepared, index) else _values(prepared, columns, index)
        if values is None:
            states = {1: _PullbackState(), -1: _PullbackState()}
            continue
        trend, movement, acceleration, normalizer = values
        for sign, side in ((1, "long"), (-1, "short")):
            state = states[sign]
            signed_trend = sign * trend
            signed_movement = sign * movement
            price = float(prepared.mid[index])
            established = signed_trend >= config.minimum_established_trend
            if state.mode == "idle":
                if established:
                    _start_or_reset_trend(state, established=True, price=price)
                continue
            if state.mode == "trend":
                if signed_trend < config.minimum_residual_trend:
                    _start_or_reset_trend(state, established=established, price=price)
                    continue
                if sign * (price - state.favorable_extreme) > 0.0:
                    state.favorable_extreme = price
                if (
                    signed_movement <= -config.minimum_pullback_speed
                    and normalizer > 0.0
                ):
                    state.mode = "pullback"
                    state.normalizer = normalizer
                    state.started_ns = int(prepared.timestamp_ns[index])
                    state.previous_signed_movement = signed_movement
                continue

            elapsed_ms = (prepared.timestamp_ns[index] - state.started_ns) / 1_000_000.0
            depth_fraction = sign * (state.favorable_extreme - price) / state.normalizer
            if (
                signed_trend < config.minimum_residual_trend
                or elapsed_ms > config.maximum_pullback_duration_ms
                or depth_fraction > config.maximum_pullback_depth_fraction
            ):
                _start_or_reset_trend(state, established=established, price=price)
                continue
            if depth_fraction <= 0.0:
                _start_or_reset_trend(state, established=established, price=price)
                continue
            if (
                elapsed_ms >= config.minimum_pullback_duration_ms
                and depth_fraction >= config.minimum_pullback_depth_fraction
                and signed_movement >= config.minimum_resumption_speed
                and sign * acceleration >= config.minimum_resumption_acceleration
            ):
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        PULLBACK_RESUMPTION,
                        "pullback-to-resumption",
                        {
                            "pullback_depth_fraction": depth_fraction,
                            "pullback_duration_ms": elapsed_ms,
                            "signed_trend": signed_trend,
                            "signed_resumption_speed": signed_movement,
                            "signed_resumption_acceleration": sign * acceleration,
                        },
                    )
                )
                _start_or_reset_trend(state, established=established, price=price)
            else:
                state.previous_signed_movement = signed_movement
    return events


def _pullback_resumption_batch(
    prepared: _PreparedFrame, config: PullbackResumptionSignalConfig
) -> list[FrozenSignalEvent]:
    valid, arrays = _finite_measurement_arrays(
        prepared, _configured_columns(config)
    )
    trend_values, movement_values, acceleration_values, normalizer_values = arrays
    states = {1: _PullbackState(), -1: _PullbackState()}
    events: list[FrozenSignalEvent] = []
    was_valid = False
    for index in range(len(prepared.frame)):
        if not valid[index]:
            if was_valid:
                states = {1: _PullbackState(), -1: _PullbackState()}
            was_valid = False
            continue
        was_valid = True
        trend = float(trend_values[index])
        movement = float(movement_values[index])
        acceleration = float(acceleration_values[index])
        normalizer = float(normalizer_values[index])
        price = float(prepared.mid[index])
        timestamp_ns = int(prepared.timestamp_ns[index])
        for sign, side in ((1, "long"), (-1, "short")):
            state = states[sign]
            signed_trend = sign * trend
            signed_movement = sign * movement
            established = signed_trend >= config.minimum_established_trend
            if state.mode == "idle":
                if established:
                    _start_or_reset_trend(
                        state, established=True, price=price
                    )
                continue
            if state.mode == "trend":
                if signed_trend < config.minimum_residual_trend:
                    _start_or_reset_trend(
                        state, established=established, price=price
                    )
                    continue
                if sign * (price - state.favorable_extreme) > 0.0:
                    state.favorable_extreme = price
                if (
                    signed_movement <= -config.minimum_pullback_speed
                    and normalizer > 0.0
                ):
                    state.mode = "pullback"
                    state.normalizer = normalizer
                    state.started_ns = timestamp_ns
                    state.previous_signed_movement = signed_movement
                continue

            elapsed_ms = (timestamp_ns - state.started_ns) / 1_000_000.0
            depth_fraction = (
                sign * (state.favorable_extreme - price) / state.normalizer
            )
            if (
                signed_trend < config.minimum_residual_trend
                or elapsed_ms > config.maximum_pullback_duration_ms
                or depth_fraction > config.maximum_pullback_depth_fraction
            ):
                _start_or_reset_trend(
                    state, established=established, price=price
                )
                continue
            if depth_fraction <= 0.0:
                _start_or_reset_trend(
                    state, established=established, price=price
                )
                continue
            if (
                elapsed_ms >= config.minimum_pullback_duration_ms
                and depth_fraction >= config.minimum_pullback_depth_fraction
                and signed_movement >= config.minimum_resumption_speed
                and sign * acceleration
                >= config.minimum_resumption_acceleration
            ):
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        PULLBACK_RESUMPTION,
                        "pullback-to-resumption",
                        {
                            "pullback_depth_fraction": depth_fraction,
                            "pullback_duration_ms": elapsed_ms,
                            "signed_trend": signed_trend,
                            "signed_resumption_speed": signed_movement,
                            "signed_resumption_acceleration": sign
                            * acceleration,
                        },
                    )
                )
                _start_or_reset_trend(
                    state, established=established, price=price
                )
            else:
                state.previous_signed_movement = signed_movement
    return events


def _countertrend_pivot_reference(
    prepared: _PreparedFrame, config: CountertrendPivotSignalConfig
) -> list[FrozenSignalEvent]:
    columns = _configured_columns(config)
    states = {1: _PullbackState(), -1: _PullbackState()}
    events: list[FrozenSignalEvent] = []
    for index in range(len(prepared.frame)):
        values = None if _unusable(prepared, index) else _values(prepared, columns, index)
        if values is None:
            states = {1: _PullbackState(), -1: _PullbackState()}
            continue
        trend, movement, acceleration, normalizer = values
        for sign, side in ((1, "long"), (-1, "short")):
            state = states[sign]
            signed_trend = sign * trend
            signed_movement = sign * movement
            price = float(prepared.mid[index])
            established = signed_trend >= config.minimum_established_trend
            if state.mode == "idle":
                if established:
                    _start_or_reset_trend(state, established=True, price=price)
                continue
            if state.mode == "trend":
                if signed_trend < config.minimum_residual_trend:
                    _start_or_reset_trend(state, established=established, price=price)
                    continue
                if sign * (price - state.favorable_extreme) > 0.0:
                    state.favorable_extreme = price
                if (
                    signed_movement <= -config.minimum_pullback_speed
                    and normalizer > 0.0
                ):
                    state.mode = "pullback"
                    state.normalizer = normalizer
                    state.started_ns = int(prepared.timestamp_ns[index])
                    state.adverse_extreme = price
                    state.previous_signed_movement = signed_movement
                continue

            elapsed_ms = (prepared.timestamp_ns[index] - state.started_ns) / 1_000_000.0
            if sign * (price - state.adverse_extreme) < 0.0:
                state.adverse_extreme = price
            depth_fraction = (
                sign * (state.favorable_extreme - state.adverse_extreme)
                / state.normalizer
            )
            rebound_fraction = (
                sign * (price - state.adverse_extreme) / state.normalizer
            )
            if (
                signed_trend < config.minimum_residual_trend
                or elapsed_ms > config.maximum_pullback_duration_ms
                or depth_fraction > config.maximum_pullback_depth_fraction
            ):
                _start_or_reset_trend(state, established=established, price=price)
                continue
            improvement = signed_movement - state.previous_signed_movement
            if (
                elapsed_ms >= config.minimum_pullback_duration_ms
                and depth_fraction >= config.minimum_pullback_depth_fraction
                and rebound_fraction >= config.minimum_rebound_fraction
                and signed_movement >= config.minimum_pivot_speed
                and improvement >= config.minimum_velocity_improvement
                and sign * acceleration >= config.minimum_pivot_acceleration
            ):
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        COUNTERTREND_PIVOT,
                        "countertrend-to-pivot",
                        {
                            "pullback_depth_fraction": depth_fraction,
                            "rebound_fraction": rebound_fraction,
                            "pullback_duration_ms": elapsed_ms,
                            "signed_pivot_speed": signed_movement,
                            "signed_velocity_improvement": improvement,
                            "signed_pivot_acceleration": sign * acceleration,
                        },
                    )
                )
                _start_or_reset_trend(state, established=established, price=price)
            else:
                state.previous_signed_movement = signed_movement
    return events


def _countertrend_pivot_batch(
    prepared: _PreparedFrame, config: CountertrendPivotSignalConfig
) -> list[FrozenSignalEvent]:
    valid, arrays = _finite_measurement_arrays(
        prepared, _configured_columns(config)
    )
    trend_values, movement_values, acceleration_values, normalizer_values = arrays
    states = {1: _PullbackState(), -1: _PullbackState()}
    events: list[FrozenSignalEvent] = []
    was_valid = False
    for index in range(len(prepared.frame)):
        if not valid[index]:
            if was_valid:
                states = {1: _PullbackState(), -1: _PullbackState()}
            was_valid = False
            continue
        was_valid = True
        trend = float(trend_values[index])
        movement = float(movement_values[index])
        acceleration = float(acceleration_values[index])
        normalizer = float(normalizer_values[index])
        price = float(prepared.mid[index])
        timestamp_ns = int(prepared.timestamp_ns[index])
        for sign, side in ((1, "long"), (-1, "short")):
            state = states[sign]
            signed_trend = sign * trend
            signed_movement = sign * movement
            established = signed_trend >= config.minimum_established_trend
            if state.mode == "idle":
                if established:
                    _start_or_reset_trend(
                        state, established=True, price=price
                    )
                continue
            if state.mode == "trend":
                if signed_trend < config.minimum_residual_trend:
                    _start_or_reset_trend(
                        state, established=established, price=price
                    )
                    continue
                if sign * (price - state.favorable_extreme) > 0.0:
                    state.favorable_extreme = price
                if (
                    signed_movement <= -config.minimum_pullback_speed
                    and normalizer > 0.0
                ):
                    state.mode = "pullback"
                    state.normalizer = normalizer
                    state.started_ns = timestamp_ns
                    state.adverse_extreme = price
                    state.previous_signed_movement = signed_movement
                continue

            elapsed_ms = (timestamp_ns - state.started_ns) / 1_000_000.0
            if sign * (price - state.adverse_extreme) < 0.0:
                state.adverse_extreme = price
            depth_fraction = (
                sign * (state.favorable_extreme - state.adverse_extreme)
                / state.normalizer
            )
            rebound_fraction = (
                sign * (price - state.adverse_extreme) / state.normalizer
            )
            if (
                signed_trend < config.minimum_residual_trend
                or elapsed_ms > config.maximum_pullback_duration_ms
                or depth_fraction > config.maximum_pullback_depth_fraction
            ):
                _start_or_reset_trend(
                    state, established=established, price=price
                )
                continue
            improvement = signed_movement - state.previous_signed_movement
            if (
                elapsed_ms >= config.minimum_pullback_duration_ms
                and depth_fraction >= config.minimum_pullback_depth_fraction
                and rebound_fraction >= config.minimum_rebound_fraction
                and signed_movement >= config.minimum_pivot_speed
                and improvement >= config.minimum_velocity_improvement
                and sign * acceleration >= config.minimum_pivot_acceleration
            ):
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        COUNTERTREND_PIVOT,
                        "countertrend-to-pivot",
                        {
                            "pullback_depth_fraction": depth_fraction,
                            "rebound_fraction": rebound_fraction,
                            "pullback_duration_ms": elapsed_ms,
                            "signed_pivot_speed": signed_movement,
                            "signed_velocity_improvement": improvement,
                            "signed_pivot_acceleration": sign * acceleration,
                        },
                    )
                )
                _start_or_reset_trend(
                    state, established=established, price=price
                )
            else:
                state.previous_signed_movement = signed_movement
    return events


@dataclass(slots=True)
class _CompressionState:
    row_count: int = 0
    high_bid: float = math.nan
    low_ask: float = math.nan
    last_compression_ns: int = 0
    armed: bool = False


def _compression_breakout_reference(
    prepared: _PreparedFrame,
    config: CompressionExpansionBreakoutSignalConfig,
) -> list[FrozenSignalEvent]:
    columns = _configured_columns(config)
    state = _CompressionState()
    events: list[FrozenSignalEvent] = []
    for index in range(len(prepared.frame)):
        values = None if _unusable(prepared, index) else _values(prepared, columns, index)
        if values is None:
            state = _CompressionState()
            continue
        short_vol, long_vol, short_arrival, long_arrival, movement = values
        if long_vol <= 0.0 or long_arrival <= 0.0:
            state = _CompressionState()
            continue
        volatility_ratio = short_vol / long_vol
        arrival_ratio = short_arrival / long_arrival
        if volatility_ratio <= config.maximum_compression_ratio:
            if state.row_count == 0:
                state.high_bid = float(prepared.bid[index])
                state.low_ask = float(prepared.ask[index])
            else:
                state.high_bid = max(state.high_bid, float(prepared.bid[index]))
                state.low_ask = min(state.low_ask, float(prepared.ask[index]))
            state.row_count += 1
            state.last_compression_ns = int(prepared.timestamp_ns[index])
            state.armed = state.row_count >= config.minimum_compression_rows
            continue
        if not state.armed:
            state = _CompressionState()
            continue
        wait_ms = (
            prepared.timestamp_ns[index] - state.last_compression_ns
        ) / 1_000_000.0
        if wait_ms > config.maximum_breakout_wait_ms:
            state = _CompressionState()
            continue
        if (
            volatility_ratio >= config.minimum_expansion_ratio
            and arrival_ratio >= config.minimum_arrival_rate_ratio
        ):
            long_breakout = (
                movement >= config.minimum_breakout_speed
                and prepared.bid[index] > state.high_bid + config.breakout_buffer
            )
            short_breakout = (
                -movement >= config.minimum_breakout_speed
                and prepared.ask[index] < state.low_ask - config.breakout_buffer
            )
            if long_breakout or short_breakout:
                side: SignalSide = "long" if long_breakout else "short"
                sign = 1 if long_breakout else -1
                fixed_level = state.high_bid if long_breakout else state.low_ask
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        COMPRESSION_EXPANSION_BREAKOUT,
                        "compressed-to-expanded-breakout",
                        {
                            "compression_rows": state.row_count,
                            "breakout_wait_ms": wait_ms,
                            "fixed_prior_executable_level": fixed_level,
                            "volatility_ratio": volatility_ratio,
                            "arrival_rate_ratio": arrival_ratio,
                            "signed_breakout_speed": sign * movement,
                        },
                    )
                )
                state = _CompressionState()
    return events


def _compression_breakout_batch(
    prepared: _PreparedFrame,
    config: CompressionExpansionBreakoutSignalConfig,
) -> list[FrozenSignalEvent]:
    valid, arrays = _finite_measurement_arrays(
        prepared, _configured_columns(config)
    )
    (
        short_vol_values,
        long_vol_values,
        short_arrival_values,
        long_arrival_values,
        movement_values,
    ) = arrays
    state = _CompressionState()
    events: list[FrozenSignalEvent] = []
    was_valid = False
    for index in range(len(prepared.frame)):
        if not valid[index]:
            if was_valid:
                state = _CompressionState()
            was_valid = False
            continue
        was_valid = True
        short_vol = float(short_vol_values[index])
        long_vol = float(long_vol_values[index])
        short_arrival = float(short_arrival_values[index])
        long_arrival = float(long_arrival_values[index])
        movement = float(movement_values[index])
        if long_vol <= 0.0 or long_arrival <= 0.0:
            if state.row_count or state.armed:
                state = _CompressionState()
            continue
        volatility_ratio = short_vol / long_vol
        arrival_ratio = short_arrival / long_arrival
        if volatility_ratio <= config.maximum_compression_ratio:
            if state.row_count == 0:
                state.high_bid = float(prepared.bid[index])
                state.low_ask = float(prepared.ask[index])
            else:
                state.high_bid = max(
                    state.high_bid, float(prepared.bid[index])
                )
                state.low_ask = min(
                    state.low_ask, float(prepared.ask[index])
                )
            state.row_count += 1
            state.last_compression_ns = int(prepared.timestamp_ns[index])
            state.armed = state.row_count >= config.minimum_compression_rows
            continue
        if not state.armed:
            if state.row_count:
                state = _CompressionState()
            continue
        wait_ms = (
            prepared.timestamp_ns[index] - state.last_compression_ns
        ) / 1_000_000.0
        if wait_ms > config.maximum_breakout_wait_ms:
            state = _CompressionState()
            continue
        if (
            volatility_ratio >= config.minimum_expansion_ratio
            and arrival_ratio >= config.minimum_arrival_rate_ratio
        ):
            long_breakout = (
                movement >= config.minimum_breakout_speed
                and prepared.bid[index]
                > state.high_bid + config.breakout_buffer
            )
            short_breakout = (
                -movement >= config.minimum_breakout_speed
                and prepared.ask[index]
                < state.low_ask - config.breakout_buffer
            )
            if long_breakout or short_breakout:
                side: SignalSide = "long" if long_breakout else "short"
                sign = 1 if long_breakout else -1
                fixed_level = (
                    state.high_bid if long_breakout else state.low_ask
                )
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        COMPRESSION_EXPANSION_BREAKOUT,
                        "compressed-to-expanded-breakout",
                        {
                            "compression_rows": state.row_count,
                            "breakout_wait_ms": wait_ms,
                            "fixed_prior_executable_level": fixed_level,
                            "volatility_ratio": volatility_ratio,
                            "arrival_rate_ratio": arrival_ratio,
                            "signed_breakout_speed": sign * movement,
                        },
                    )
                )
                state = _CompressionState()
    return events


def _quote_translation_pressure_reference(
    prepared: _PreparedFrame,
    config: QuoteTranslationPressureSignalConfig,
) -> list[FrozenSignalEvent]:
    columns = _configured_columns(config)
    latched = {1: False, -1: False}
    events: list[FrozenSignalEvent] = []
    for index in range(len(prepared.frame)):
        values = None if _unusable(prepared, index) else _values(prepared, columns, index)
        if values is None:
            latched = {1: False, -1: False}
            continue
        pressure, coherence, movement, persistence, short_arrival, long_arrival = values
        if long_arrival <= 0.0:
            latched = {1: False, -1: False}
            continue
        arrival_ratio = short_arrival / long_arrival
        for sign, side in ((1, "long"), (-1, "short")):
            signed_pressure = sign * pressure
            if latched[sign] and signed_pressure <= config.reset_translation_pressure:
                latched[sign] = False
            if latched[sign]:
                continue
            if (
                signed_pressure >= config.minimum_translation_pressure
                and coherence >= config.minimum_translation_coherence
                and sign * movement >= config.minimum_movement_speed
                and persistence >= config.minimum_persistence
                and arrival_ratio >= config.minimum_arrival_rate_ratio
            ):
                events.append(
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        QUOTE_TRANSLATION_PRESSURE,
                        "inactive-to-translation-pressure",
                        {
                            "signed_translation_pressure": signed_pressure,
                            "translation_coherence": coherence,
                            "signed_movement_speed": sign * movement,
                            "persistence": persistence,
                            "arrival_rate_ratio": arrival_ratio,
                        },
                    )
                )
                latched[sign] = True
    return events


def _quote_translation_pressure_batch(
    prepared: _PreparedFrame,
    config: QuoteTranslationPressureSignalConfig,
) -> list[FrozenSignalEvent]:
    pressure = prepared.measurements[config.translation_pressure_column]
    coherence = prepared.measurements[config.translation_coherence_column]
    movement = prepared.measurements[config.movement_column]
    persistence = prepared.measurements[config.persistence_column]
    short_arrival = prepared.measurements[config.short_arrival_rate_column]
    long_arrival = prepared.measurements[config.long_arrival_rate_column]
    valid = (
        prepared.ready
        & ~prepared.gaps
        & np.isfinite(pressure)
        & np.isfinite(coherence)
        & np.isfinite(movement)
        & np.isfinite(persistence)
        & np.isfinite(short_arrival)
        & np.isfinite(long_arrival)
        & (long_arrival > 0.0)
    )
    arrival_ratio = np.full(len(prepared.frame), np.nan, dtype=float)
    np.divide(short_arrival, long_arrival, out=arrival_ratio, where=valid)
    ranked: list[tuple[int, int, FrozenSignalEvent]] = []
    for side_order, (sign, side) in enumerate(((1, "long"), (-1, "short"))):
        signed_pressure = sign * pressure
        signed_movement = sign * movement
        enter = (
            valid
            & (signed_pressure >= config.minimum_translation_pressure)
            & (coherence >= config.minimum_translation_coherence)
            & (signed_movement >= config.minimum_movement_speed)
            & (persistence >= config.minimum_persistence)
            & (arrival_ratio >= config.minimum_arrival_rate_ratio)
        )
        reset = ~valid | (
            signed_pressure <= config.reset_translation_pressure
        )
        for index_value in _first_onset_after_reset(enter, reset):
            index = int(index_value)
            ranked.append(
                (
                    index,
                    side_order,
                    _event(
                        prepared,
                        index,
                        side,
                        config,
                        QUOTE_TRANSLATION_PRESSURE,
                        "inactive-to-translation-pressure",
                        {
                            "signed_translation_pressure": float(
                                signed_pressure[index]
                            ),
                            "translation_coherence": float(coherence[index]),
                            "signed_movement_speed": float(signed_movement[index]),
                            "persistence": float(persistence[index]),
                            "arrival_rate_ratio": float(arrival_ratio[index]),
                        },
                    ),
                )
            )
    ranked.sort(key=lambda item: item[:2])
    return [item[2] for item in ranked]


def _selected_configs(
    configs: Sequence[FreshSignalConfig],
    engine: GenerationEngine,
) -> tuple[FreshSignalConfig, ...]:
    selected = tuple(configs)
    if engine not in ("batch", "reference"):
        raise ValueError("engine must be 'batch' or 'reference'")
    if any(not isinstance(config, _SIGNAL_CONFIG_TYPES) for config in selected):
        raise TypeError("configs must contain only supported signal configurations")
    candidate_ids = [config.candidate_id for config in selected]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("candidate_id values must be unique within a generation run")
    return selected


def _generate_prepared_config_events(
    prepared: _PreparedFrame,
    config: FreshSignalConfig,
    *,
    engine: GenerationEngine,
) -> tuple[FrozenSignalEvent, ...]:
    if isinstance(config, TrendAccelerationSignalConfig):
        events = (
            _trend_acceleration_batch(prepared, config)
            if engine == "batch"
            else _trend_acceleration_reference(prepared, config)
        )
    elif isinstance(config, PullbackResumptionSignalConfig):
        events = (
            _pullback_resumption_batch(prepared, config)
            if engine == "batch"
            else _pullback_resumption_reference(prepared, config)
        )
    elif isinstance(config, CountertrendPivotSignalConfig):
        events = (
            _countertrend_pivot_batch(prepared, config)
            if engine == "batch"
            else _countertrend_pivot_reference(prepared, config)
        )
    elif isinstance(config, CompressionExpansionBreakoutSignalConfig):
        events = (
            _compression_breakout_batch(prepared, config)
            if engine == "batch"
            else _compression_breakout_reference(prepared, config)
        )
    elif isinstance(config, QuoteTranslationPressureSignalConfig):
        events = (
            _quote_translation_pressure_batch(prepared, config)
            if engine == "batch"
            else _quote_translation_pressure_reference(prepared, config)
        )
    else:  # pragma: no cover - guarded by the caller's type check
        raise TypeError("unsupported signal configuration")
    output = tuple(events)
    _validate_event_bindings_prepared(prepared, output)
    return output


def iter_frozen_signal_event_groups(
    features: pd.DataFrame,
    *,
    configs: Sequence[FreshSignalConfig],
    engine: GenerationEngine = "batch",
) -> Iterator[tuple[str, tuple[FrozenSignalEvent, ...]]]:
    """Yield one frozen candidate's causal events after one shared preparation.

    The prepared feature arrays live only for the duration of this iterator,
    and at most one candidate event tuple is yielded at a time.  This is the
    bounded-memory equivalent of grouping :func:`generate_frozen_signal_events`
    by ``candidate_id``.  Each group's event order is exactly the order the
    materialized API would retain for that candidate.
    """

    selected = _selected_configs(configs, engine)
    required_columns = tuple(
        column for config in selected for column in _configured_columns(config)
    )
    prepared = _prepare_frame(features, required_columns)
    for config in selected:
        yield (
            config.candidate_id,
            _generate_prepared_config_events(prepared, config, engine=engine),
        )


def generate_frozen_signal_events(
    features: pd.DataFrame,
    *,
    configs: Sequence[FreshSignalConfig],
    engine: GenerationEngine = "batch",
) -> tuple[FrozenSignalEvent, ...]:
    """Apply frozen family configs and return exact-row-bound causal events.

    Config order is a deterministic tie-break when several families emit on
    the same quote.  Candidate IDs must be unique so an experiment ledger can
    unambiguously map every event back to one frozen rule.  The default batch
    engine compiles the two hysteretic onset families into NumPy masks.  The
    reference engine retains the literal row-loop implementation as a semantic
    oracle; pullback, pivot, and compression use their same causal state
    machines in both modes.
    """

    ranked: list[tuple[int, int, int, FrozenSignalEvent]] = []
    selected = _selected_configs(configs, engine)
    config_order_by_id = {
        config.candidate_id: order for order, config in enumerate(selected)
    }
    for candidate_id, events in iter_frozen_signal_event_groups(
        features,
        configs=selected,
        engine=engine,
    ):
        config_order = config_order_by_id[candidate_id]
        for event in events:
            side_order = 0 if event.side == "long" else 1
            ranked.append((event.tick_index, config_order, side_order, event))
    ranked.sort(key=lambda item: item[:3])
    output = tuple(item[3] for item in ranked)
    return output


def _validate_event_bindings_prepared(
    prepared: _PreparedFrame,
    events: Sequence[FrozenSignalEvent],
) -> None:
    previous_index = -1
    for position, event in enumerate(events):
        if not isinstance(event, FrozenSignalEvent):
            raise TypeError(f"events[{position}] is not a FrozenSignalEvent")
        if event.tick_index < previous_index:
            raise ValueError("events must be ordered by non-decreasing tick_index")
        previous_index = event.tick_index
        if event.tick_index >= len(prepared.frame):
            raise ValueError(f"event {position} points outside the feature frame")
        index = event.tick_index
        if event.tick_id != prepared.tick_ids[index]:
            raise ValueError(f"event {position} tick_id does not match its feature row")
        if pd.Timestamp(event.timestamp) != pd.Timestamp(prepared.timestamps[index]):
            raise ValueError(f"event {position} timestamp does not match its feature row")
        if prepared.gaps[index] or not prepared.ready[index]:
            raise ValueError(f"event {position} is bound to an unusable feature row")


def preflight_signal_bindings(
    features: pd.DataFrame,
    events: Iterable[FrozenSignalEvent],
    *,
    ticks: Sequence[Tick] | None = None,
) -> None:
    """Validate event-to-feature binding, optionally binding the whole tick tape.

    Passing ``ticks`` is the final hand-off audit before execution diagnostics:
    every frame row must match the replay tape by positional ID, timestamp,
    bid, and ask.  Equal timestamps are valid only in increasing ID order.
    """

    prepared = _prepare_frame(features, ())
    materialized = tuple(events)
    _validate_event_bindings_prepared(prepared, materialized)

    if ticks is None:
        return
    if len(ticks) != len(features):
        raise ValueError("tick tape length does not match the feature frame")
    for index, tick in enumerate(ticks):
        if not isinstance(tick, Tick):
            raise TypeError(f"ticks[{index}] is not a Tick")
        if tick.id != prepared.tick_ids[index]:
            raise ValueError(f"tick tape ID mismatch at index {index}")
        if pd.Timestamp(tick.timestamp) != pd.Timestamp(prepared.timestamps[index]):
            raise ValueError(f"tick tape timestamp mismatch at index {index}")
        if tick.bid != prepared.bid[index] or tick.ask != prepared.ask[index]:
            raise ValueError(f"tick tape quote mismatch at index {index}")


__all__ = [
    "COMPRESSION_EXPANSION_BREAKOUT",
    "COUNTERTREND_PIVOT",
    "PULLBACK_RESUMPTION",
    "QUOTE_TRANSLATION_PRESSURE",
    "TREND_ACCELERATION",
    "CompressionExpansionBreakoutSignalConfig",
    "CountertrendPivotSignalConfig",
    "FreshSignalConfig",
    "GenerationEngine",
    "PullbackResumptionSignalConfig",
    "QuoteTranslationPressureSignalConfig",
    "TrendAccelerationSignalConfig",
    "generate_frozen_signal_events",
    "iter_frozen_signal_event_groups",
    "preflight_signal_bindings",
    "signal_config_fingerprint",
    "signal_required_columns",
]
