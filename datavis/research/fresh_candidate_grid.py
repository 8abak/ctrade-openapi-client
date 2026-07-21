"""Outcome-blind candidate grid for the fresh XAUUSD entry study.

The grid is deliberately a pure configuration transform.  It accepts a
session-balanced :class:`~datavis.research.fresh_thresholds.FreshQuantileBank`
and emits explicit configurations for the five causal signal families.  It
does not accept ticks, labels, trades, P&L, or any other outcome-bearing
object.

Every price-, speed-, acceleration-, pressure-, or persistence-valued
threshold is obtained through ``bank.threshold``.  Dimensionless structural
ratios and state durations are fixed constants declared in this module.  Each
structural rule has a three-point empirical-rank neighbourhood so later
research can reject isolated lucky thresholds rather than promote them.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Sequence

from datavis.research.fresh_feature_bank import kalman_bank_column
from datavis.research.fresh_signals import (
    COMPRESSION_EXPANSION_BREAKOUT,
    COUNTERTREND_PIVOT,
    PULLBACK_RESUMPTION,
    QUOTE_TRANSLATION_PRESSURE,
    TREND_ACCELERATION,
    CompressionExpansionBreakoutSignalConfig,
    CountertrendPivotSignalConfig,
    FreshSignalConfig,
    PullbackResumptionSignalConfig,
    QuoteTranslationPressureSignalConfig,
    TrendAccelerationSignalConfig,
    signal_config_fingerprint,
)
from datavis.research.fresh_thresholds import (
    FreshQuantileBank,
    QuantileMeasurementSpec,
)


FRESH_CANDIDATE_GRID_SCHEMA = "fresh-xauusd-candidate-grid/v1"
FRESH_CANDIDATE_QUANTILE_RANKS = (
    0.45,
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
)
FRESH_RANK_NEIGHBOURHOOD = (-0.05, 0.0, 0.05)
MAXIMUM_TOTAL_CANDIDATES = 240
MAXIMUM_CANDIDATES_PER_FAMILY = 60
MAXIMUM_OPTIONAL_KALMAN_MODELS = 16
SYMMETRIC_DIRECTION_POLICY = (
    "one frozen configuration emits long and short events by sign reflection"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FAMILY_PREFIX = {
    TREND_ACCELERATION: "ta",
    PULLBACK_RESUMPTION: "pr",
    COUNTERTREND_PIVOT: "cp",
    COMPRESSION_EXPANSION_BREAKOUT: "ce",
    QUOTE_TRANSLATION_PRESSURE: "qt",
}
_CONFIG_TYPES = (
    TrendAccelerationSignalConfig,
    PullbackResumptionSignalConfig,
    CountertrendPivotSignalConfig,
    CompressionExpansionBreakoutSignalConfig,
    QuoteTranslationPressureSignalConfig,
)
_FAMILY_CONFIG_TYPE = {
    TREND_ACCELERATION: TrendAccelerationSignalConfig,
    PULLBACK_RESUMPTION: PullbackResumptionSignalConfig,
    COUNTERTREND_PIVOT: CountertrendPivotSignalConfig,
    COMPRESSION_EXPANSION_BREAKOUT: CompressionExpansionBreakoutSignalConfig,
    QUOTE_TRANSLATION_PRESSURE: QuoteTranslationPressureSignalConfig,
}


def _measurement_name(column: str, transform: str) -> str:
    return f"{transform}::{column}"


def _normalized_kalman_model_ids(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError("kalman_model_ids must be a sequence of model IDs")
    normalized = tuple(values)
    if any(not isinstance(value, str) or not value.strip() for value in normalized):
        raise ValueError("kalman model IDs must be non-empty strings")
    if len(normalized) != len(set(normalized)):
        raise ValueError("kalman model IDs must be unique")
    ordered = tuple(sorted(normalized))
    if len(ordered) > MAXIMUM_OPTIONAL_KALMAN_MODELS:
        raise ValueError(
            "too many optional Kalman models for the per-family candidate budget"
        )
    # Reuse the feature-bank validator rather than maintaining a second model
    # ID grammar here.
    for model_id in ordered:
        kalman_bank_column(model_id, "kalman_velocity")
    return ordered


def fresh_candidate_quantile_measurements(
    *,
    kalman_model_ids: Sequence[str] = (),
) -> tuple[QuantileMeasurementSpec, ...]:
    """Return the exact outcome-free measurement bank required by the grid.

    The returned order is stable and independent of the caller's Kalman model
    order.  The supplied quantile-bank configuration must include
    :data:`FRESH_CANDIDATE_QUANTILE_RANKS`.
    """

    models = _normalized_kalman_model_ids(kalman_model_ids)
    specifications: list[QuantileMeasurementSpec] = []

    absolute_columns = (
        "10s_mid_speed",
        "30s_mid_speed",
        "ewma_10s_slope",
        "ewma_30s_slope",
        "250ms_mid_speed",
        "500ms_mid_speed",
        "1s_mid_speed",
        "2s_mid_speed",
        "500ms_mid_acceleration",
        "1s_mid_acceleration",
        "2s_mid_acceleration",
        "250ms_translation_pressure",
        "500ms_translation_pressure",
        "1s_translation_pressure",
    )
    positive_columns = (
        "250ms_translation_coherence",
        "500ms_translation_coherence",
        "1s_translation_coherence",
    )
    identity_columns = (
        "250ms_persistence",
        "500ms_persistence",
        "1s_persistence",
        "spread",
    )

    for column in absolute_columns:
        specifications.append(
            QuantileMeasurementSpec(
                name=_measurement_name(column, "absolute"),
                column=column,
                transform="absolute",
            )
        )
    for column in positive_columns:
        specifications.append(
            QuantileMeasurementSpec(
                name=_measurement_name(column, "positive"),
                column=column,
                transform="positive",
            )
        )
    for column in identity_columns:
        specifications.append(
            QuantileMeasurementSpec(
                name=_measurement_name(column, "identity"),
                column=column,
                transform="identity",
            )
        )
    for model_id in models:
        for measurement in ("kalman_velocity", "kalman_velocity_change"):
            column = kalman_bank_column(model_id, measurement)
            specifications.append(
                QuantileMeasurementSpec(
                    name=_measurement_name(column, "absolute"),
                    column=column,
                    transform="absolute",
                )
            )
    names = tuple(spec.name for spec in specifications)
    if len(names) != len(set(names)):
        raise AssertionError("candidate measurement names must be unique")
    return tuple(specifications)


@dataclass(frozen=True, slots=True)
class ThresholdRankProvenance:
    """Exact empirical-rank source for one configuration value."""

    parameter: str
    measurement: str
    column: str
    transform: str
    base_rank: float
    rank_offset: float
    resolved_rank: float
    bank_value: float
    multiplier: float
    final_value: float


@dataclass(frozen=True, slots=True)
class ExplicitGridConstant:
    """A dimensionless structural ratio or state duration fixed pre-outcome."""

    parameter: str
    value: int | float
    unit: str


@dataclass(frozen=True, slots=True)
class FreshCandidate:
    """One auditable frozen signal configuration and its provenance."""

    family: str
    structure_id: str
    neighbourhood_id: str
    rank_offset: float
    direction_policy: str
    config: FreshSignalConfig
    config_sha256: str
    threshold_provenance: tuple[ThresholdRankProvenance, ...]
    explicit_constants: tuple[ExplicitGridConstant, ...]

    def __post_init__(self) -> None:
        if self.family not in _FAMILY_PREFIX:
            raise ValueError("unsupported candidate family")
        if not isinstance(self.config, _CONFIG_TYPES):
            raise TypeError("config must be a supported fresh signal config")
        if not isinstance(self.config, _FAMILY_CONFIG_TYPE[self.family]):
            raise ValueError("candidate family does not match its config type")
        if not self.structure_id or not self.neighbourhood_id:
            raise ValueError("candidate structure and neighbourhood IDs are required")
        if self.direction_policy != SYMMETRIC_DIRECTION_POLICY:
            raise ValueError("fresh candidates must use symmetric direction handling")
        if self.config_sha256 != signal_config_fingerprint(self.config):
            raise ValueError("candidate config hash does not match the configuration")
        if not self.threshold_provenance:
            raise ValueError("candidate threshold provenance must not be empty")


@dataclass(frozen=True, slots=True)
class FreshCandidateGrid:
    """Deterministic result of applying a quantile bank to the fresh grid."""

    schema: str
    quantile_bank_sha256: str
    kalman_model_ids: tuple[str, ...]
    required_quantile_ranks: tuple[float, ...]
    required_measurements: tuple[QuantileMeasurementSpec, ...]
    candidates: tuple[FreshCandidate, ...]
    grid_sha256: str

    @property
    def configs(self) -> tuple[FreshSignalConfig, ...]:
        return tuple(candidate.config for candidate in self.candidates)


class _ThresholdResolver:
    def __init__(self, bank: FreshQuantileBank, rank_offset: float) -> None:
        self.bank = bank
        self.rank_offset = rank_offset
        self.provenance: list[ThresholdRankProvenance] = []

    def value(
        self,
        parameter: str,
        *,
        column: str,
        transform: str,
        base_rank: float,
        multiplier: float = 1.0,
    ) -> float:
        resolved_rank = round(base_rank + self.rank_offset, 12)
        measurement = _measurement_name(column, transform)
        bank_value = float(self.bank.threshold(measurement, resolved_rank))
        if not math.isfinite(bank_value):
            raise ValueError(f"quantile {measurement!r} is not finite")
        final_value = bank_value * float(multiplier)
        if not math.isfinite(final_value):
            raise ValueError(f"resolved threshold for {parameter!r} is not finite")
        self.provenance.append(
            ThresholdRankProvenance(
                parameter=parameter,
                measurement=measurement,
                column=column,
                transform=transform,
                base_rank=base_rank,
                rank_offset=self.rank_offset,
                resolved_rank=resolved_rank,
                bank_value=bank_value,
                multiplier=float(multiplier),
                final_value=final_value,
            )
        )
        return final_value


@dataclass(frozen=True, slots=True)
class _TrendStructure:
    structure_id: str
    trend_column: str
    velocity_column: str
    acceleration_column: str
    coherence_column: str
    reset_fraction: float


@dataclass(frozen=True, slots=True)
class _PullbackStructure:
    structure_id: str
    trend_column: str
    movement_column: str
    acceleration_column: str
    normalizer_column: str
    residual_trend_fraction: float
    minimum_depth_fraction: float
    maximum_depth_fraction: float
    minimum_duration_ms: int
    maximum_duration_ms: int


@dataclass(frozen=True, slots=True)
class _PivotStructure:
    structure_id: str
    trend_column: str
    movement_column: str
    acceleration_column: str
    normalizer_column: str
    residual_trend_fraction: float
    minimum_depth_fraction: float
    maximum_depth_fraction: float
    minimum_rebound_fraction: float
    pivot_speed_fraction: float
    improvement_fraction: float
    minimum_duration_ms: int
    maximum_duration_ms: int


@dataclass(frozen=True, slots=True)
class _CompressionStructure:
    structure_id: str
    short_volatility_column: str
    long_volatility_column: str
    short_arrival_column: str
    long_arrival_column: str
    movement_column: str
    maximum_compression_ratio: float
    minimum_expansion_ratio: float
    minimum_arrival_ratio: float
    breakout_buffer_spread_fraction: float
    minimum_compression_rows: int
    maximum_breakout_wait_ms: int


@dataclass(frozen=True, slots=True)
class _PressureStructure:
    structure_id: str
    pressure_column: str
    coherence_column: str
    movement_column: str
    persistence_column: str
    short_arrival_column: str
    long_arrival_column: str
    reset_pressure_fraction: float
    minimum_arrival_ratio: float


_TREND_STRUCTURES = (
    _TrendStructure("raw10-fast500", "10s_mid_speed", "500ms_mid_speed", "500ms_mid_acceleration", "500ms_translation_coherence", 0.35),
    _TrendStructure("raw30-fast1", "30s_mid_speed", "1s_mid_speed", "1s_mid_acceleration", "1s_translation_coherence", 0.40),
    _TrendStructure("ewma10-fast500", "ewma_10s_slope", "500ms_mid_speed", "500ms_mid_acceleration", "500ms_translation_coherence", 0.35),
    _TrendStructure("ewma30-fast1", "ewma_30s_slope", "1s_mid_speed", "1s_mid_acceleration", "1s_translation_coherence", 0.40),
)

_PULLBACK_STRUCTURES = (
    _PullbackStructure("raw10-fast500", "10s_mid_speed", "500ms_mid_speed", "500ms_mid_acceleration", "10s_mid_range", 0.35, 0.06, 0.70, 125, 2_000),
    _PullbackStructure("raw10-fast1", "10s_mid_speed", "1s_mid_speed", "1s_mid_acceleration", "10s_mid_range", 0.45, 0.08, 0.80, 250, 4_000),
    _PullbackStructure("raw30-fast1", "30s_mid_speed", "1s_mid_speed", "1s_mid_acceleration", "30s_mid_range", 0.35, 0.06, 0.70, 250, 5_000),
    _PullbackStructure("ewma10-fast500", "ewma_10s_slope", "500ms_mid_speed", "500ms_mid_acceleration", "10s_mid_range", 0.45, 0.08, 0.80, 125, 3_000),
    _PullbackStructure("ewma30-fast2", "ewma_30s_slope", "2s_mid_speed", "2s_mid_acceleration", "30s_mid_range", 0.55, 0.10, 0.90, 500, 8_000),
)

# The discovery-only support gate in sealed run 29874435384 found no session
# with the required 1,000 finite 250 ms acceleration observations.  The feed's
# cadence cannot identify the two distinct ~83 ms subintervals that measurement
# needs.  Retain the supported 250 ms speed onset, but confirm it with the
# already registered 500 ms causal acceleration.  No price outcome informed
# this structural availability correction.
_PIVOT_STRUCTURES = (
    _PivotStructure("raw10-speed250-accel500", "10s_mid_speed", "250ms_mid_speed", "500ms_mid_acceleration", "10s_mid_range", 0.35, 0.05, 0.65, 0.025, 0.45, 0.30, 75, 1_500),
    _PivotStructure("raw10-fast500", "10s_mid_speed", "500ms_mid_speed", "500ms_mid_acceleration", "10s_mid_range", 0.45, 0.06, 0.75, 0.040, 0.40, 0.40, 125, 2_500),
    _PivotStructure("raw30-fast1", "30s_mid_speed", "1s_mid_speed", "1s_mid_acceleration", "30s_mid_range", 0.35, 0.06, 0.75, 0.050, 0.35, 0.45, 250, 5_000),
    _PivotStructure("ewma10-fast500", "ewma_10s_slope", "500ms_mid_speed", "500ms_mid_acceleration", "10s_mid_range", 0.45, 0.08, 0.80, 0.050, 0.40, 0.40, 125, 3_000),
    _PivotStructure("ewma30-fast2", "ewma_30s_slope", "2s_mid_speed", "2s_mid_acceleration", "30s_mid_range", 0.55, 0.10, 0.90, 0.075, 0.30, 0.50, 500, 8_000),
)

_COMPRESSION_STRUCTURES = (
    _CompressionStructure("noise500-5", "500ms_noise", "5s_noise", "500ms_arrival_rate", "5s_arrival_rate", "500ms_mid_speed", 0.65, 1.15, 1.05, 0.20, 3, 500),
    _CompressionStructure("noise1-10", "1s_noise", "10s_noise", "1s_arrival_rate", "10s_arrival_rate", "1s_mid_speed", 0.70, 1.20, 1.10, 0.30, 5, 1_000),
    _CompressionStructure("std500-5", "500ms_bollinger_std", "5s_bollinger_std", "500ms_arrival_rate", "5s_arrival_rate", "500ms_mid_speed", 0.70, 1.25, 1.10, 0.40, 5, 1_000),
    _CompressionStructure("std1-10", "1s_bollinger_std", "10s_bollinger_std", "1s_arrival_rate", "10s_arrival_rate", "1s_mid_speed", 0.75, 1.30, 1.15, 0.50, 8, 2_000),
)

_PRESSURE_STRUCTURES = (
    _PressureStructure("pressure250-arrival250-5", "250ms_translation_pressure", "250ms_translation_coherence", "250ms_mid_speed", "250ms_persistence", "250ms_arrival_rate", "5s_arrival_rate", 0.35, 1.05),
    _PressureStructure("pressure500-arrival500-5", "500ms_translation_pressure", "500ms_translation_coherence", "500ms_mid_speed", "500ms_persistence", "500ms_arrival_rate", "5s_arrival_rate", 0.40, 1.10),
    _PressureStructure("pressure500-arrival500-10", "500ms_translation_pressure", "500ms_translation_coherence", "500ms_mid_speed", "500ms_persistence", "500ms_arrival_rate", "10s_arrival_rate", 0.45, 1.20),
    _PressureStructure("pressure1-arrival1-10", "1s_translation_pressure", "1s_translation_coherence", "1s_mid_speed", "1s_persistence", "1s_arrival_rate", "10s_arrival_rate", 0.50, 1.30),
)


def _constant(parameter: str, value: int | float, unit: str) -> ExplicitGridConstant:
    return ExplicitGridConstant(parameter=parameter, value=value, unit=unit)


def _candidate_id(
    family: str,
    structure_number: int,
    neighbourhood_name: str,
    bank_sha256: str,
) -> str:
    return (
        f"fresh-{_FAMILY_PREFIX[family]}-{structure_number:02d}-"
        f"{neighbourhood_name}-{bank_sha256[:10]}"
    )


def _candidate(
    *,
    family: str,
    structure_id: str,
    structure_number: int,
    rank_offset: float,
    config: FreshSignalConfig,
    resolver: _ThresholdResolver,
    constants: Sequence[ExplicitGridConstant],
) -> FreshCandidate:
    return FreshCandidate(
        family=family,
        structure_id=structure_id,
        neighbourhood_id=f"{_FAMILY_PREFIX[family]}-{structure_number:02d}",
        rank_offset=rank_offset,
        direction_policy=SYMMETRIC_DIRECTION_POLICY,
        config=config,
        config_sha256=signal_config_fingerprint(config),
        threshold_provenance=tuple(resolver.provenance),
        explicit_constants=tuple(constants),
    )


def _neighbourhoods() -> tuple[tuple[str, float], ...]:
    return (
        ("rank-minus", FRESH_RANK_NEIGHBOURHOOD[0]),
        ("rank-base", FRESH_RANK_NEIGHBOURHOOD[1]),
        ("rank-plus", FRESH_RANK_NEIGHBOURHOOD[2]),
    )


def _validate_bank(
    bank: FreshQuantileBank,
    required: tuple[QuantileMeasurementSpec, ...],
) -> None:
    if not isinstance(bank, FreshQuantileBank):
        raise TypeError("bank must be a FreshQuantileBank")
    if not isinstance(bank.bank_sha256, str) or _SHA256.fullmatch(
        bank.bank_sha256.lower()
    ) is None:
        raise ValueError("quantile bank must have a SHA-256 digest")
    actual_by_name: dict[str, QuantileMeasurementSpec] = {}
    for spec in bank.measurements:
        if spec.name in actual_by_name:
            raise ValueError("quantile bank measurement names must be unique")
        actual_by_name[spec.name] = spec
    for spec in required:
        if actual_by_name.get(spec.name) != spec:
            raise ValueError(
                f"quantile bank is missing the exact required measurement {spec.name!r}"
            )
    available_ranks = {round(float(rank), 12) for rank in bank.config.ranks}
    missing_ranks = [
        rank
        for rank in FRESH_CANDIDATE_QUANTILE_RANKS
        if round(rank, 12) not in available_ranks
    ]
    if missing_ranks:
        raise ValueError(f"quantile bank is missing required ranks: {missing_ranks}")


def _trend_candidates(
    bank: FreshQuantileBank,
    models: tuple[str, ...],
) -> list[FreshCandidate]:
    structures = list(_TREND_STRUCTURES)
    structures.extend(
        _TrendStructure(
            structure_id=f"{model_id}-fast500",
            trend_column=kalman_bank_column(model_id, "kalman_velocity"),
            velocity_column="500ms_mid_speed",
            acceleration_column=kalman_bank_column(
                model_id, "kalman_velocity_change"
            ),
            coherence_column="500ms_translation_coherence",
            reset_fraction=0.35,
        )
        for model_id in models
    )
    output: list[FreshCandidate] = []
    for number, structure in enumerate(structures):
        for neighbourhood_name, rank_offset in _neighbourhoods():
            resolver = _ThresholdResolver(bank, rank_offset)
            minimum_trend = resolver.value(
                "minimum_trend",
                column=structure.trend_column,
                transform="absolute",
                base_rank=0.70,
            )
            minimum_velocity = resolver.value(
                "minimum_velocity",
                column=structure.velocity_column,
                transform="absolute",
                base_rank=0.75,
            )
            reset_velocity = resolver.value(
                "reset_velocity",
                column=structure.velocity_column,
                transform="absolute",
                base_rank=0.75,
                multiplier=structure.reset_fraction,
            )
            minimum_acceleration = resolver.value(
                "minimum_acceleration",
                column=structure.acceleration_column,
                transform="absolute",
                base_rank=0.70,
            )
            minimum_coherence = resolver.value(
                "minimum_translation_coherence",
                column=structure.coherence_column,
                transform="positive",
                base_rank=0.60,
            )
            candidate_id = _candidate_id(
                TREND_ACCELERATION,
                number,
                neighbourhood_name,
                bank.bank_sha256,
            )
            config = TrendAccelerationSignalConfig(
                candidate_id=candidate_id,
                trend_column=structure.trend_column,
                velocity_column=structure.velocity_column,
                acceleration_column=structure.acceleration_column,
                translation_coherence_column=structure.coherence_column,
                minimum_trend=minimum_trend,
                minimum_velocity=minimum_velocity,
                reset_velocity=reset_velocity,
                minimum_acceleration=minimum_acceleration,
                minimum_translation_coherence=minimum_coherence,
            )
            output.append(
                _candidate(
                    family=TREND_ACCELERATION,
                    structure_id=structure.structure_id,
                    structure_number=number,
                    rank_offset=rank_offset,
                    config=config,
                    resolver=resolver,
                    constants=(
                        _constant(
                            "reset_velocity_fraction",
                            structure.reset_fraction,
                            "dimensionless",
                        ),
                    ),
                )
            )
    return output


def _pullback_candidates(bank: FreshQuantileBank) -> list[FreshCandidate]:
    output: list[FreshCandidate] = []
    for number, structure in enumerate(_PULLBACK_STRUCTURES):
        for neighbourhood_name, rank_offset in _neighbourhoods():
            resolver = _ThresholdResolver(bank, rank_offset)
            established = resolver.value(
                "minimum_established_trend",
                column=structure.trend_column,
                transform="absolute",
                base_rank=0.70,
            )
            residual = resolver.value(
                "minimum_residual_trend",
                column=structure.trend_column,
                transform="absolute",
                base_rank=0.70,
                multiplier=structure.residual_trend_fraction,
            )
            pullback_speed = resolver.value(
                "minimum_pullback_speed",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.65,
            )
            resumption_speed = resolver.value(
                "minimum_resumption_speed",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.65,
            )
            resumption_acceleration = resolver.value(
                "minimum_resumption_acceleration",
                column=structure.acceleration_column,
                transform="absolute",
                base_rank=0.65,
            )
            config = PullbackResumptionSignalConfig(
                candidate_id=_candidate_id(
                    PULLBACK_RESUMPTION,
                    number,
                    neighbourhood_name,
                    bank.bank_sha256,
                ),
                trend_column=structure.trend_column,
                movement_column=structure.movement_column,
                acceleration_column=structure.acceleration_column,
                depth_normalizer_column=structure.normalizer_column,
                minimum_established_trend=established,
                minimum_residual_trend=residual,
                minimum_pullback_speed=pullback_speed,
                minimum_pullback_depth_fraction=structure.minimum_depth_fraction,
                maximum_pullback_depth_fraction=structure.maximum_depth_fraction,
                minimum_resumption_speed=resumption_speed,
                minimum_resumption_acceleration=resumption_acceleration,
                minimum_pullback_duration_ms=structure.minimum_duration_ms,
                maximum_pullback_duration_ms=structure.maximum_duration_ms,
            )
            output.append(
                _candidate(
                    family=PULLBACK_RESUMPTION,
                    structure_id=structure.structure_id,
                    structure_number=number,
                    rank_offset=rank_offset,
                    config=config,
                    resolver=resolver,
                    constants=(
                        _constant("residual_trend_fraction", structure.residual_trend_fraction, "dimensionless"),
                        _constant("minimum_pullback_depth_fraction", structure.minimum_depth_fraction, "dimensionless"),
                        _constant("maximum_pullback_depth_fraction", structure.maximum_depth_fraction, "dimensionless"),
                        _constant("minimum_pullback_duration_ms", structure.minimum_duration_ms, "milliseconds"),
                        _constant("maximum_pullback_duration_ms", structure.maximum_duration_ms, "milliseconds"),
                    ),
                )
            )
    return output


def _pivot_candidates(bank: FreshQuantileBank) -> list[FreshCandidate]:
    output: list[FreshCandidate] = []
    for number, structure in enumerate(_PIVOT_STRUCTURES):
        for neighbourhood_name, rank_offset in _neighbourhoods():
            resolver = _ThresholdResolver(bank, rank_offset)
            established = resolver.value(
                "minimum_established_trend",
                column=structure.trend_column,
                transform="absolute",
                base_rank=0.70,
            )
            residual = resolver.value(
                "minimum_residual_trend",
                column=structure.trend_column,
                transform="absolute",
                base_rank=0.70,
                multiplier=structure.residual_trend_fraction,
            )
            pullback_speed = resolver.value(
                "minimum_pullback_speed",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.65,
            )
            pivot_speed = resolver.value(
                "minimum_pivot_speed",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.65,
                multiplier=-structure.pivot_speed_fraction,
            )
            improvement = resolver.value(
                "minimum_velocity_improvement",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.60,
                multiplier=structure.improvement_fraction,
            )
            pivot_acceleration = resolver.value(
                "minimum_pivot_acceleration",
                column=structure.acceleration_column,
                transform="absolute",
                base_rank=0.60,
            )
            config = CountertrendPivotSignalConfig(
                candidate_id=_candidate_id(
                    COUNTERTREND_PIVOT,
                    number,
                    neighbourhood_name,
                    bank.bank_sha256,
                ),
                trend_column=structure.trend_column,
                movement_column=structure.movement_column,
                acceleration_column=structure.acceleration_column,
                depth_normalizer_column=structure.normalizer_column,
                minimum_established_trend=established,
                minimum_residual_trend=residual,
                minimum_pullback_speed=pullback_speed,
                minimum_pullback_depth_fraction=structure.minimum_depth_fraction,
                maximum_pullback_depth_fraction=structure.maximum_depth_fraction,
                minimum_rebound_fraction=structure.minimum_rebound_fraction,
                minimum_pivot_speed=pivot_speed,
                minimum_velocity_improvement=improvement,
                minimum_pivot_acceleration=pivot_acceleration,
                minimum_pullback_duration_ms=structure.minimum_duration_ms,
                maximum_pullback_duration_ms=structure.maximum_duration_ms,
            )
            output.append(
                _candidate(
                    family=COUNTERTREND_PIVOT,
                    structure_id=structure.structure_id,
                    structure_number=number,
                    rank_offset=rank_offset,
                    config=config,
                    resolver=resolver,
                    constants=(
                        _constant("residual_trend_fraction", structure.residual_trend_fraction, "dimensionless"),
                        _constant("minimum_pullback_depth_fraction", structure.minimum_depth_fraction, "dimensionless"),
                        _constant("maximum_pullback_depth_fraction", structure.maximum_depth_fraction, "dimensionless"),
                        _constant("minimum_rebound_fraction", structure.minimum_rebound_fraction, "dimensionless"),
                        _constant("pivot_speed_fraction", structure.pivot_speed_fraction, "dimensionless"),
                        _constant("improvement_fraction", structure.improvement_fraction, "dimensionless"),
                        _constant("minimum_pullback_duration_ms", structure.minimum_duration_ms, "milliseconds"),
                        _constant("maximum_pullback_duration_ms", structure.maximum_duration_ms, "milliseconds"),
                    ),
                )
            )
    return output


def _compression_candidates(bank: FreshQuantileBank) -> list[FreshCandidate]:
    output: list[FreshCandidate] = []
    for number, structure in enumerate(_COMPRESSION_STRUCTURES):
        for neighbourhood_name, rank_offset in _neighbourhoods():
            resolver = _ThresholdResolver(bank, rank_offset)
            breakout_speed = resolver.value(
                "minimum_breakout_speed",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.70,
            )
            breakout_buffer = resolver.value(
                "breakout_buffer",
                column="spread",
                transform="identity",
                base_rank=0.50,
                multiplier=structure.breakout_buffer_spread_fraction,
            )
            config = CompressionExpansionBreakoutSignalConfig(
                candidate_id=_candidate_id(
                    COMPRESSION_EXPANSION_BREAKOUT,
                    number,
                    neighbourhood_name,
                    bank.bank_sha256,
                ),
                short_volatility_column=structure.short_volatility_column,
                long_volatility_column=structure.long_volatility_column,
                short_arrival_rate_column=structure.short_arrival_column,
                long_arrival_rate_column=structure.long_arrival_column,
                movement_column=structure.movement_column,
                maximum_compression_ratio=structure.maximum_compression_ratio,
                minimum_expansion_ratio=structure.minimum_expansion_ratio,
                minimum_arrival_rate_ratio=structure.minimum_arrival_ratio,
                minimum_breakout_speed=breakout_speed,
                breakout_buffer=breakout_buffer,
                minimum_compression_rows=structure.minimum_compression_rows,
                maximum_breakout_wait_ms=structure.maximum_breakout_wait_ms,
            )
            output.append(
                _candidate(
                    family=COMPRESSION_EXPANSION_BREAKOUT,
                    structure_id=structure.structure_id,
                    structure_number=number,
                    rank_offset=rank_offset,
                    config=config,
                    resolver=resolver,
                    constants=(
                        _constant("maximum_compression_ratio", structure.maximum_compression_ratio, "dimensionless"),
                        _constant("minimum_expansion_ratio", structure.minimum_expansion_ratio, "dimensionless"),
                        _constant("minimum_arrival_rate_ratio", structure.minimum_arrival_ratio, "dimensionless"),
                        _constant("breakout_buffer_spread_fraction", structure.breakout_buffer_spread_fraction, "dimensionless"),
                        _constant("minimum_compression_rows", structure.minimum_compression_rows, "quote rows"),
                        _constant("maximum_breakout_wait_ms", structure.maximum_breakout_wait_ms, "milliseconds"),
                    ),
                )
            )
    return output


def _pressure_candidates(bank: FreshQuantileBank) -> list[FreshCandidate]:
    output: list[FreshCandidate] = []
    for number, structure in enumerate(_PRESSURE_STRUCTURES):
        for neighbourhood_name, rank_offset in _neighbourhoods():
            resolver = _ThresholdResolver(bank, rank_offset)
            pressure = resolver.value(
                "minimum_translation_pressure",
                column=structure.pressure_column,
                transform="absolute",
                base_rank=0.70,
            )
            reset_pressure = resolver.value(
                "reset_translation_pressure",
                column=structure.pressure_column,
                transform="absolute",
                base_rank=0.70,
                multiplier=structure.reset_pressure_fraction,
            )
            coherence = resolver.value(
                "minimum_translation_coherence",
                column=structure.coherence_column,
                transform="positive",
                base_rank=0.60,
            )
            movement = resolver.value(
                "minimum_movement_speed",
                column=structure.movement_column,
                transform="absolute",
                base_rank=0.70,
            )
            persistence = resolver.value(
                "minimum_persistence",
                column=structure.persistence_column,
                transform="identity",
                base_rank=0.60,
            )
            config = QuoteTranslationPressureSignalConfig(
                candidate_id=_candidate_id(
                    QUOTE_TRANSLATION_PRESSURE,
                    number,
                    neighbourhood_name,
                    bank.bank_sha256,
                ),
                translation_pressure_column=structure.pressure_column,
                translation_coherence_column=structure.coherence_column,
                movement_column=structure.movement_column,
                persistence_column=structure.persistence_column,
                short_arrival_rate_column=structure.short_arrival_column,
                long_arrival_rate_column=structure.long_arrival_column,
                minimum_translation_pressure=pressure,
                reset_translation_pressure=reset_pressure,
                minimum_translation_coherence=coherence,
                minimum_movement_speed=movement,
                minimum_persistence=persistence,
                minimum_arrival_rate_ratio=structure.minimum_arrival_ratio,
            )
            output.append(
                _candidate(
                    family=QUOTE_TRANSLATION_PRESSURE,
                    structure_id=structure.structure_id,
                    structure_number=number,
                    rank_offset=rank_offset,
                    config=config,
                    resolver=resolver,
                    constants=(
                        _constant("reset_translation_pressure_fraction", structure.reset_pressure_fraction, "dimensionless"),
                        _constant("minimum_arrival_rate_ratio", structure.minimum_arrival_ratio, "dimensionless"),
                    ),
                )
            )
    return output


def _grid_payload(
    *,
    bank_sha256: str,
    models: tuple[str, ...],
    measurements: tuple[QuantileMeasurementSpec, ...],
    candidates: tuple[FreshCandidate, ...],
) -> dict[str, object]:
    return {
        "schema": FRESH_CANDIDATE_GRID_SCHEMA,
        "quantileBankSha256": bank_sha256,
        "kalmanModelIds": list(models),
        "requiredQuantileRanks": list(FRESH_CANDIDATE_QUANTILE_RANKS),
        "requiredMeasurements": [asdict(item) for item in measurements],
        "candidates": [
            {
                "family": item.family,
                "structureId": item.structure_id,
                "neighbourhoodId": item.neighbourhood_id,
                "rankOffset": item.rank_offset,
                "directionPolicy": item.direction_policy,
                "configType": type(item.config).__name__,
                "config": asdict(item.config),
                "configSha256": item.config_sha256,
                "thresholdProvenance": [
                    asdict(value) for value in item.threshold_provenance
                ],
                "explicitConstants": [
                    asdict(value) for value in item.explicit_constants
                ],
            }
            for item in candidates
        ],
    }


def build_fresh_candidate_grid(
    bank: FreshQuantileBank,
    *,
    kalman_model_ids: Sequence[str] = (),
) -> FreshCandidateGrid:
    """Build all frozen discovery candidates without inspecting outcomes.

    Four non-Kalman structures plus all explicitly requested Kalman models
    feed the trend-acceleration family.  The remaining families use several
    short/medium causal-horizon structures.  Every structure has the same
    three-point rank neighbourhood.  With no Kalman models the grid has 66
    candidates; with the registered nine-model bank it has 93.
    """

    models = _normalized_kalman_model_ids(kalman_model_ids)
    required = fresh_candidate_quantile_measurements(kalman_model_ids=models)
    _validate_bank(bank, required)
    candidates = tuple(
        (
            *_trend_candidates(bank, models),
            *_pullback_candidates(bank),
            *_pivot_candidates(bank),
            *_compression_candidates(bank),
            *_pressure_candidates(bank),
        )
    )
    candidate_ids = tuple(item.config.candidate_id for item in candidates)
    if len(candidate_ids) != len(set(candidate_ids)):
        raise AssertionError("fresh candidate IDs must be unique")
    config_hashes = tuple(item.config_sha256 for item in candidates)
    if len(config_hashes) != len(set(config_hashes)):
        raise AssertionError("fresh candidate configuration hashes must be unique")
    if len(candidates) > MAXIMUM_TOTAL_CANDIDATES:
        raise AssertionError("fresh candidate grid exceeds its total budget")
    family_counts = {
        family: sum(item.family == family for item in candidates)
        for family in _FAMILY_PREFIX
    }
    if any(count > MAXIMUM_CANDIDATES_PER_FAMILY for count in family_counts.values()):
        raise AssertionError("fresh candidate grid exceeds a per-family budget")
    if not 60 <= len(candidates) <= 120:
        raise AssertionError("fresh candidate grid is outside its tractable design range")

    payload = _grid_payload(
        bank_sha256=bank.bank_sha256.lower(),
        models=models,
        measurements=required,
        candidates=candidates,
    )
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return FreshCandidateGrid(
        schema=FRESH_CANDIDATE_GRID_SCHEMA,
        quantile_bank_sha256=bank.bank_sha256.lower(),
        kalman_model_ids=models,
        required_quantile_ranks=FRESH_CANDIDATE_QUANTILE_RANKS,
        required_measurements=required,
        candidates=candidates,
        grid_sha256=digest,
    )


__all__ = [
    "FRESH_CANDIDATE_GRID_SCHEMA",
    "FRESH_CANDIDATE_QUANTILE_RANKS",
    "FRESH_RANK_NEIGHBOURHOOD",
    "MAXIMUM_CANDIDATES_PER_FAMILY",
    "MAXIMUM_TOTAL_CANDIDATES",
    "SYMMETRIC_DIRECTION_POLICY",
    "ExplicitGridConstant",
    "FreshCandidate",
    "FreshCandidateGrid",
    "ThresholdRankProvenance",
    "build_fresh_candidate_grid",
    "fresh_candidate_quantile_measurements",
]
