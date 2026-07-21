"""Outcome-blind protective-exit bank for the fresh XAUUSD study.

The bank is a deterministic configuration transform.  It accepts only a
training-window :class:`~datavis.research.fresh_thresholds.FreshQuantileBank`
and explicitly registered replay execution scenarios.  It cannot inspect
ticks, entries, trades, labels, or P&L.

The 72 variants form a compact, auditable 2 x 3 x 4 x 3 design:

* fixed-spread or causal-volatility initial stops;
* no ratchet, true executable break-even plus trail, or beyond-cost
  break-even plus trail;
* time-only, velocity weakening, velocity/acceleration weakening, or
  weakening plus a progress-stall deadline; and
* a three-point parameter neighbourhood.

Every fixed quote-price distance is derived from a session-balanced empirical
spread rank.  Velocity, acceleration, and positive stall thresholds likewise
come from causal feature ranks.  Volatility stop/trail values are dimensionless
multiples of the explicitly named causal ``1s_bollinger_std`` feature.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Mapping, Sequence

from datavis.research.fresh_decisions import MomentumWeakeningExitConfig
from datavis.research.fresh_exits import ExitDistance, FreshExitPolicyConfig
from datavis.research.fresh_replay import (
    FreshExecutionConfig,
    STRICT_SCALP_LIMIT_MS,
)
from datavis.research.fresh_thresholds import (
    FreshQuantileBank,
    QuantileMeasurementSpec,
)


FRESH_EXIT_GRID_SCHEMA = "fresh-xauusd-exit-grid/v1"
FRESH_EXIT_QUANTILE_RANKS = (0.45, 0.50, 0.55, 0.60, 0.65, 0.70)
FRESH_EXIT_RANK_NEIGHBOURHOOD = (-0.05, 0.0, 0.05)
FRESH_EXIT_VOLATILITY_COLUMN = "1s_bollinger_std"
MAXIMUM_EXIT_VARIANTS = 96
REGISTERED_MAXIMUM_HOLDING_MS = 58_000

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SPREAD_MEASUREMENT = "identity::spread"
_VELOCITY_MEASUREMENT = "absolute::1s_mid_speed"
_ACCELERATION_MEASUREMENT = "absolute::1s_mid_acceleration"


def fresh_exit_quantile_measurements() -> tuple[QuantileMeasurementSpec, ...]:
    """Return the exact causal measurements consumed by the exit bank."""

    return (
        QuantileMeasurementSpec(_SPREAD_MEASUREMENT, "spread", "identity"),
        QuantileMeasurementSpec(
            _VELOCITY_MEASUREMENT,
            "1s_mid_speed",
            "absolute",
        ),
        QuantileMeasurementSpec(
            _ACCELERATION_MEASUREMENT,
            "1s_mid_acceleration",
            "absolute",
        ),
    )


@dataclass(frozen=True, slots=True)
class ExitThresholdProvenance:
    """Exact training-bank source and transformation for one numeric field."""

    parameter: str
    measurement: str
    column: str
    transform: str
    base_rank: float
    rank_offset: float
    resolved_rank: float
    bank_value: float
    multiplier: float
    additive_floor: float
    final_value: float


@dataclass(frozen=True, slots=True)
class ExplicitExitConstant:
    """A non-fitted duration, feature binding, or dimensionless multiplier."""

    parameter: str
    value: str | int | float
    unit: str


def _execution_config_hash(config: FreshExecutionConfig) -> str:
    encoded = json.dumps(
        {
            "schema": "fresh-replay-execution-config/v1",
            "config": asdict(config),
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class RegisteredExecutionScenario:
    """One named, fully explicit execution configuration bound by hash."""

    scenario_id: str
    config: FreshExecutionConfig
    config_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.scenario_id, str) or not self.scenario_id.strip():
            raise ValueError("execution scenario id must be a non-empty string")
        if not isinstance(self.config, FreshExecutionConfig):
            raise TypeError("execution scenario config must be FreshExecutionConfig")
        if self.config_sha256 != _execution_config_hash(self.config):
            raise ValueError("execution scenario hash does not match its config")


@dataclass(frozen=True, slots=True)
class FreshExitVariant:
    """One frozen protective policy and optional causal invalidation policy."""

    variant_id: str
    stop_structure_id: str
    management_structure_id: str
    invalidation_structure_id: str
    neighbourhood_id: str
    rank_offset: float
    policy: FreshExitPolicyConfig
    weakening: MomentumWeakeningExitConfig | None
    volatility_feature_column: str | None
    threshold_provenance: tuple[ExitThresholdProvenance, ...]
    explicit_constants: tuple[ExplicitExitConstant, ...]
    variant_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "variant_id",
            "stop_structure_id",
            "management_structure_id",
            "invalidation_structure_id",
            "neighbourhood_id",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if self.rank_offset not in FRESH_EXIT_RANK_NEIGHBOURHOOD:
            raise ValueError("rank_offset is outside the registered neighbourhood")
        if not isinstance(self.policy, FreshExitPolicyConfig):
            raise TypeError("policy must be FreshExitPolicyConfig")
        if self.weakening is not None and not isinstance(
            self.weakening,
            MomentumWeakeningExitConfig,
        ):
            raise TypeError("weakening must be MomentumWeakeningExitConfig or None")
        if self.policy.requires_volatility:
            if self.volatility_feature_column != FRESH_EXIT_VOLATILITY_COLUMN:
                raise ValueError("volatility policies must bind the registered feature")
        elif self.volatility_feature_column is not None:
            raise ValueError("fixed-only policies cannot bind a volatility feature")
        if _SHA256.fullmatch(self.variant_sha256) is None:
            raise ValueError("variant_sha256 must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class FreshExitGrid:
    """The complete frozen, outcome-blind exit-design bank."""

    schema: str
    quantile_bank_sha256: str
    execution_scenarios_sha256: str
    execution_scenarios: tuple[RegisteredExecutionScenario, ...]
    required_quantile_ranks: tuple[float, ...]
    required_measurements: tuple[QuantileMeasurementSpec, ...]
    variants: tuple[FreshExitVariant, ...]
    grid_sha256: str

    def __post_init__(self) -> None:
        if self.schema != FRESH_EXIT_GRID_SCHEMA:
            raise ValueError("unsupported fresh exit-grid schema")
        for digest in (
            self.quantile_bank_sha256,
            self.execution_scenarios_sha256,
            self.grid_sha256,
        ):
            if _SHA256.fullmatch(digest) is None:
                raise ValueError("fresh exit-grid digests must be lowercase SHA-256")
        if not self.execution_scenarios:
            raise ValueError("at least one execution scenario is required")
        if not self.variants or len(self.variants) > MAXIMUM_EXIT_VARIANTS:
            raise ValueError("exit grid must contain 1..96 variants")


@dataclass(frozen=True, slots=True)
class _StopStructure:
    structure_id: str
    mode: str
    initial_multiplier: float


@dataclass(frozen=True, slots=True)
class _ManagementStructure:
    structure_id: str
    break_even_kind: str
    trailing_mode: str | None
    trailing_volatility_basis: str


@dataclass(frozen=True, slots=True)
class _InvalidationStructure:
    structure_id: str
    maximum_holding_ms: int
    minimum_holding_ms: int | None
    confirmation_ms: int | None
    velocity_fraction: float | None
    acceleration_fraction: float | None
    stall_deadline_ms: int | None
    stall_progress_spread_fraction: float | None


_STOP_STRUCTURES = (
    _StopStructure("fixed-spread-stop", "fixed", 3.0),
    _StopStructure("causal-volatility-stop", "volatility", 2.5),
)

_MANAGEMENT_STRUCTURES = (
    _ManagementStructure("time-only", "none", None, "entry"),
    _ManagementStructure("true-break-even-trail", "true", "matching", "entry"),
    _ManagementStructure(
        "beyond-cost-break-even-trail",
        "beyond-cost",
        "matching",
        "current",
    ),
)

_INVALIDATION_STRUCTURES = (
    _InvalidationStructure("time-5s", 5_000, None, None, None, None, None, None),
    _InvalidationStructure(
        "velocity-weakening-10s",
        10_000,
        250,
        250,
        0.0,
        None,
        None,
        None,
    ),
    _InvalidationStructure(
        "velocity-acceleration-20s",
        20_000,
        500,
        250,
        0.25,
        0.25,
        None,
        None,
    ),
    _InvalidationStructure(
        "weakening-stall-30s",
        30_000,
        250,
        125,
        0.35,
        0.35,
        3_000,
        0.25,
    ),
)

_NEIGHBOURHOODS = (
    ("rank-minus", -0.05, 0.85),
    ("rank-base", 0.0, 1.0),
    ("rank-plus", 0.05, 1.15),
)


class _ThresholdResolver:
    def __init__(self, bank: FreshQuantileBank, rank_offset: float) -> None:
        self._bank = bank
        self._rank_offset = rank_offset
        self.provenance: list[ExitThresholdProvenance] = []

    def value(
        self,
        parameter: str,
        *,
        measurement: QuantileMeasurementSpec,
        base_rank: float,
        multiplier: float,
        additive_floor: float = 0.0,
    ) -> float:
        resolved_rank = round(base_rank + self._rank_offset, 12)
        bank_value = float(self._bank.threshold(measurement.name, resolved_rank))
        if not math.isfinite(bank_value) or bank_value <= 0.0:
            raise ValueError(
                f"exit measurement {measurement.name!r} must resolve positive"
            )
        final_value = additive_floor + bank_value * multiplier
        if not math.isfinite(final_value) or final_value <= 0.0:
            raise ValueError(f"exit threshold {parameter!r} must resolve positive")
        self.provenance.append(
            ExitThresholdProvenance(
                parameter=parameter,
                measurement=measurement.name,
                column=measurement.column,
                transform=measurement.transform,
                base_rank=base_rank,
                rank_offset=self._rank_offset,
                resolved_rank=resolved_rank,
                bank_value=bank_value,
                multiplier=multiplier,
                additive_floor=additive_floor,
                final_value=final_value,
            )
        )
        return final_value


def _constant(
    parameter: str,
    value: str | int | float,
    unit: str,
) -> ExplicitExitConstant:
    return ExplicitExitConstant(parameter, value, unit)


def _validate_bank(
    bank: FreshQuantileBank,
    required: tuple[QuantileMeasurementSpec, ...],
) -> None:
    if not isinstance(bank, FreshQuantileBank):
        raise TypeError("bank must be a FreshQuantileBank")
    if (
        not isinstance(bank.bank_sha256, str)
        or _SHA256.fullmatch(bank.bank_sha256.lower()) is None
    ):
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
    ranks = {round(float(rank), 12) for rank in bank.config.ranks}
    missing = [
        rank for rank in FRESH_EXIT_QUANTILE_RANKS if round(rank, 12) not in ranks
    ]
    if missing:
        raise ValueError(f"quantile bank is missing required ranks: {missing}")


def _registered_executions(
    configs: Mapping[str, FreshExecutionConfig],
) -> tuple[tuple[RegisteredExecutionScenario, ...], str]:
    if not isinstance(configs, Mapping) or not configs:
        raise ValueError("execution_configs must be a non-empty mapping")
    if any(not isinstance(key, str) or not key.strip() for key in configs):
        raise ValueError("execution scenario ids must be non-empty strings")
    scenarios: list[RegisteredExecutionScenario] = []
    for scenario_id in sorted(configs):
        config = configs[scenario_id]
        if not isinstance(config, FreshExecutionConfig):
            raise TypeError("execution scenario values must be FreshExecutionConfig")
        scenarios.append(
            RegisteredExecutionScenario(
                scenario_id=scenario_id,
                config=config,
                config_sha256=_execution_config_hash(config),
            )
        )
    selected = tuple(scenarios)
    encoded = json.dumps(
        {
            "schema": "fresh-registered-execution-scenarios/v1",
            "scenarios": [
                {
                    "scenarioId": item.scenario_id,
                    "config": asdict(item.config),
                    "configSha256": item.config_sha256,
                }
                for item in selected
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return selected, hashlib.sha256(encoded).hexdigest()


def _remaining_cost_floor(
    scenarios: Sequence[RegisteredExecutionScenario],
) -> float:
    return max(
        item.config.slippage_per_side + 2.0 * item.config.commission_per_unit_per_side
        for item in scenarios
    )


def _policy_and_weakening(
    *,
    bank: FreshQuantileBank,
    stop: _StopStructure,
    management: _ManagementStructure,
    invalidation: _InvalidationStructure,
    rank_offset: float,
    neighbourhood_factor: float,
    worst_remaining_cost: float,
) -> tuple[
    FreshExitPolicyConfig,
    MomentumWeakeningExitConfig | None,
    tuple[ExitThresholdProvenance, ...],
    tuple[ExplicitExitConstant, ...],
]:
    measurements = {item.name: item for item in fresh_exit_quantile_measurements()}
    spread = measurements[_SPREAD_MEASUREMENT]
    velocity = measurements[_VELOCITY_MEASUREMENT]
    acceleration = measurements[_ACCELERATION_MEASUREMENT]
    resolver = _ThresholdResolver(bank, rank_offset)
    constants: list[ExplicitExitConstant] = []

    if stop.mode == "fixed":
        initial_stop = ExitDistance(
            "fixed",
            resolver.value(
                "policy.initial_stop.value",
                measurement=spread,
                base_rank=0.65,
                multiplier=stop.initial_multiplier * neighbourhood_factor,
            ),
        )
    else:
        initial_multiple = stop.initial_multiplier * neighbourhood_factor
        initial_stop = ExitDistance("volatility", initial_multiple)
        constants.extend(
            (
                _constant(
                    "policy.initial_stop.value",
                    initial_multiple,
                    "causal volatility multiples",
                ),
                _constant(
                    "volatility_feature_column",
                    FRESH_EXIT_VOLATILITY_COLUMN,
                    "causal feature column",
                ),
            )
        )

    break_even_activation: ExitDistance | None = None
    break_even_buffer = 0.0
    trailing_activation: ExitDistance | None = None
    trailing_distance: ExitDistance | None = None
    trailing_basis = management.trailing_volatility_basis
    if management.break_even_kind != "none":
        if management.break_even_kind == "beyond-cost":
            break_even_buffer = resolver.value(
                "policy.break_even_buffer_net_per_unit",
                measurement=spread,
                base_rank=0.50,
                multiplier=0.25 * neighbourhood_factor,
            )
        else:
            constants.append(
                _constant(
                    "policy.break_even_buffer_net_per_unit",
                    0.0,
                    "net quote-price units per unit",
                )
            )
        activation = resolver.value(
            "policy.break_even_activation.value",
            measurement=spread,
            base_rank=0.55,
            multiplier=1.0 * neighbourhood_factor,
            additive_floor=worst_remaining_cost + break_even_buffer,
        )
        break_even_activation = ExitDistance("fixed", activation)
        trailing_activation = ExitDistance(
            "fixed",
            resolver.value(
                "policy.trailing_activation.value",
                measurement=spread,
                base_rank=0.65,
                multiplier=1.5 * neighbourhood_factor,
                additive_floor=activation,
            ),
        )
        if stop.mode == "fixed":
            trailing_distance = ExitDistance(
                "fixed",
                resolver.value(
                    "policy.trailing_distance.value",
                    measurement=spread,
                    base_rank=0.55,
                    multiplier=0.75 * neighbourhood_factor,
                ),
            )
            trailing_basis = "entry"
        else:
            trailing_multiple = 0.75 * neighbourhood_factor
            trailing_distance = ExitDistance("volatility", trailing_multiple)
            constants.append(
                _constant(
                    "policy.trailing_distance.value",
                    trailing_multiple,
                    "causal volatility multiples",
                )
            )

    policy = FreshExitPolicyConfig(
        initial_stop=initial_stop,
        break_even_activation=break_even_activation,
        break_even_buffer_net_per_unit=break_even_buffer,
        trailing_activation=trailing_activation,
        trailing_distance=trailing_distance,
        trailing_volatility_basis=trailing_basis,  # type: ignore[arg-type]
        maximum_holding_ms=invalidation.maximum_holding_ms,
    )

    weakening: MomentumWeakeningExitConfig | None = None
    if invalidation.minimum_holding_ms is not None:
        assert invalidation.confirmation_ms is not None
        assert invalidation.velocity_fraction is not None
        if invalidation.velocity_fraction == 0.0:
            velocity_threshold = 0.0
            constants.append(
                _constant(
                    "weakening.velocity_exit_threshold",
                    0.0,
                    "side-aligned sign boundary",
                )
            )
        else:
            velocity_threshold = resolver.value(
                "weakening.velocity_exit_threshold",
                measurement=velocity,
                base_rank=0.55,
                multiplier=(invalidation.velocity_fraction * neighbourhood_factor),
            )
        acceleration_threshold: float | None = None
        if invalidation.acceleration_fraction is not None:
            acceleration_threshold = resolver.value(
                "weakening.acceleration_exit_threshold",
                measurement=acceleration,
                base_rank=0.55,
                multiplier=(invalidation.acceleration_fraction * neighbourhood_factor),
            )
        minimum_progress: float | None = None
        if invalidation.stall_progress_spread_fraction is not None:
            minimum_progress = resolver.value(
                "weakening.minimum_best_net_progress_per_unit",
                measurement=spread,
                base_rank=0.50,
                multiplier=(
                    invalidation.stall_progress_spread_fraction * neighbourhood_factor
                ),
            )
        weakening = MomentumWeakeningExitConfig(
            minimum_holding_ms=invalidation.minimum_holding_ms,
            weakening_confirmation_ms=invalidation.confirmation_ms,
            velocity_exit_threshold=velocity_threshold,
            acceleration_exit_threshold=acceleration_threshold,
            stall_deadline_ms=invalidation.stall_deadline_ms,
            minimum_best_net_progress_per_unit=minimum_progress,
        )

    constants.extend(
        (
            _constant(
                "policy.maximum_holding_ms",
                invalidation.maximum_holding_ms,
                "milliseconds from actual entry fill",
            ),
            _constant(
                "weakening.minimum_holding_ms",
                invalidation.minimum_holding_ms
                if invalidation.minimum_holding_ms is not None
                else "disabled",
                "milliseconds from actual entry fill",
            ),
            _constant(
                "weakening.weakening_confirmation_ms",
                invalidation.confirmation_ms
                if invalidation.confirmation_ms is not None
                else "disabled",
                "milliseconds",
            ),
            _constant(
                "weakening.stall_deadline_ms",
                invalidation.stall_deadline_ms
                if invalidation.stall_deadline_ms is not None
                else "disabled",
                "milliseconds from actual entry fill",
            ),
            _constant(
                "worst_registered_remaining_exit_cost",
                worst_remaining_cost,
                "quote-price units per unit",
            ),
        )
    )
    return policy, weakening, tuple(resolver.provenance), tuple(constants)


def _variant_payload(
    *,
    variant_id: str,
    stop_structure_id: str,
    management_structure_id: str,
    invalidation_structure_id: str,
    neighbourhood_id: str,
    rank_offset: float,
    policy: FreshExitPolicyConfig,
    weakening: MomentumWeakeningExitConfig | None,
    volatility_feature_column: str | None,
    provenance: Sequence[ExitThresholdProvenance],
    constants: Sequence[ExplicitExitConstant],
    bank_sha256: str,
    executions_sha256: str,
) -> dict[str, object]:
    return {
        "schema": "fresh-xauusd-exit-variant/v1",
        "variantId": variant_id,
        "stopStructureId": stop_structure_id,
        "managementStructureId": management_structure_id,
        "invalidationStructureId": invalidation_structure_id,
        "neighbourhoodId": neighbourhood_id,
        "rankOffset": rank_offset,
        "policy": asdict(policy),
        "weakening": asdict(weakening) if weakening is not None else None,
        "volatilityFeatureColumn": volatility_feature_column,
        "thresholdProvenance": [asdict(item) for item in provenance],
        "explicitConstants": [asdict(item) for item in constants],
        "quantileBankSha256": bank_sha256,
        "executionScenariosSha256": executions_sha256,
    }


def _sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_deadlines(
    variant: FreshExitVariant,
    scenarios: Sequence[RegisteredExecutionScenario],
) -> None:
    holding = variant.policy.maximum_holding_ms
    if holding > REGISTERED_MAXIMUM_HOLDING_MS:
        raise ValueError("exit variant exceeds the registered holding upper bound")
    for scenario in scenarios:
        config = scenario.config
        if (
            holding + config.exit_latency_ms + config.maximum_exit_lag_ms
            > STRICT_SCALP_LIMIT_MS
        ):
            raise ValueError(
                f"exit variant {variant.variant_id!r} is incompatible with "
                f"scenario {scenario.scenario_id!r}: holding, exit latency, and "
                "maximum exit lag exceed 60 seconds"
            )
        if holding + config.exit_latency_ms >= config.actual_fill_deadline_ms:
            raise ValueError(
                f"exit variant {variant.variant_id!r} is incompatible with "
                f"scenario {scenario.scenario_id!r}: replay fill deadline"
            )
        if variant.weakening is not None:
            deadlines = [variant.weakening.minimum_holding_ms]
            if variant.weakening.stall_deadline_ms is not None:
                deadlines.append(variant.weakening.stall_deadline_ms)
                if variant.weakening.stall_deadline_ms > holding:
                    raise ValueError("stall deadline cannot exceed maximum holding")
            if (
                max(deadlines) + config.exit_latency_ms + config.maximum_exit_lag_ms
                > STRICT_SCALP_LIMIT_MS
            ):
                raise ValueError("weakening deadline cannot fill within 60 seconds")
            if variant.weakening.minimum_holding_ms >= holding:
                raise ValueError("weakening cannot start at or after maximum holding")


def build_fresh_exit_grid(
    bank: FreshQuantileBank,
    *,
    execution_configs: Mapping[str, FreshExecutionConfig],
) -> FreshExitGrid:
    """Build 72 frozen exit variants without reading any market outcome.

    ``execution_configs`` should be the registered replay scenarios materialized
    from the preregistration.  Every variant is checked against every supplied
    scenario, including ``maximum holding + exit latency + maximum exit lag <=
    60 seconds`` and the replay engine's stricter fill-deadline condition.
    """

    required = fresh_exit_quantile_measurements()
    _validate_bank(bank, required)
    scenarios, executions_sha256 = _registered_executions(execution_configs)
    worst_cost = _remaining_cost_floor(scenarios)
    bank_sha256 = bank.bank_sha256.lower()

    variants: list[FreshExitVariant] = []
    for stop_number, stop in enumerate(_STOP_STRUCTURES):
        for management_number, management in enumerate(_MANAGEMENT_STRUCTURES):
            for invalidation_number, invalidation in enumerate(
                _INVALIDATION_STRUCTURES
            ):
                neighbourhood_id = (
                    f"s{stop_number + 1}-m{management_number + 1}-"
                    f"i{invalidation_number + 1}"
                )
                for neighbourhood_name, rank_offset, factor in _NEIGHBOURHOODS:
                    policy, weakening, provenance, constants = _policy_and_weakening(
                        bank=bank,
                        stop=stop,
                        management=management,
                        invalidation=invalidation,
                        rank_offset=rank_offset,
                        neighbourhood_factor=factor,
                        worst_remaining_cost=worst_cost,
                    )
                    variant_id = (
                        f"fresh-exit-{neighbourhood_id}-{neighbourhood_name}-"
                        f"{bank_sha256[:8]}-{executions_sha256[:8]}"
                    )
                    volatility_column = (
                        FRESH_EXIT_VOLATILITY_COLUMN
                        if policy.requires_volatility
                        else None
                    )
                    payload = _variant_payload(
                        variant_id=variant_id,
                        stop_structure_id=stop.structure_id,
                        management_structure_id=management.structure_id,
                        invalidation_structure_id=invalidation.structure_id,
                        neighbourhood_id=neighbourhood_id,
                        rank_offset=rank_offset,
                        policy=policy,
                        weakening=weakening,
                        volatility_feature_column=volatility_column,
                        provenance=provenance,
                        constants=constants,
                        bank_sha256=bank_sha256,
                        executions_sha256=executions_sha256,
                    )
                    variant = FreshExitVariant(
                        variant_id=variant_id,
                        stop_structure_id=stop.structure_id,
                        management_structure_id=management.structure_id,
                        invalidation_structure_id=invalidation.structure_id,
                        neighbourhood_id=neighbourhood_id,
                        rank_offset=rank_offset,
                        policy=policy,
                        weakening=weakening,
                        volatility_feature_column=volatility_column,
                        threshold_provenance=provenance,
                        explicit_constants=constants,
                        variant_sha256=_sha256(payload),
                    )
                    _validate_deadlines(variant, scenarios)
                    variants.append(variant)

    selected = tuple(variants)
    if len(selected) != 72 or len(selected) > MAXIMUM_EXIT_VARIANTS:
        raise AssertionError("fresh exit grid violated its registered 72/96 budget")
    identifiers = [item.variant_id for item in selected]
    digests = [item.variant_sha256 for item in selected]
    if len(identifiers) != len(set(identifiers)):
        raise AssertionError("fresh exit variant IDs must be unique")
    if len(digests) != len(set(digests)):
        raise AssertionError("fresh exit variant hashes must be unique")

    payload = {
        "schema": FRESH_EXIT_GRID_SCHEMA,
        "quantileBankSha256": bank_sha256,
        "executionScenariosSha256": executions_sha256,
        "executionScenarios": [
            {
                "scenarioId": item.scenario_id,
                "config": asdict(item.config),
                "configSha256": item.config_sha256,
            }
            for item in scenarios
        ],
        "requiredQuantileRanks": list(FRESH_EXIT_QUANTILE_RANKS),
        "requiredMeasurements": [asdict(item) for item in required],
        "variants": [
            {
                **_variant_payload(
                    variant_id=item.variant_id,
                    stop_structure_id=item.stop_structure_id,
                    management_structure_id=item.management_structure_id,
                    invalidation_structure_id=item.invalidation_structure_id,
                    neighbourhood_id=item.neighbourhood_id,
                    rank_offset=item.rank_offset,
                    policy=item.policy,
                    weakening=item.weakening,
                    volatility_feature_column=item.volatility_feature_column,
                    provenance=item.threshold_provenance,
                    constants=item.explicit_constants,
                    bank_sha256=bank_sha256,
                    executions_sha256=executions_sha256,
                ),
                "variantSha256": item.variant_sha256,
            }
            for item in selected
        ],
    }
    return FreshExitGrid(
        schema=FRESH_EXIT_GRID_SCHEMA,
        quantile_bank_sha256=bank_sha256,
        execution_scenarios_sha256=executions_sha256,
        execution_scenarios=scenarios,
        required_quantile_ranks=FRESH_EXIT_QUANTILE_RANKS,
        required_measurements=required,
        variants=selected,
        grid_sha256=_sha256(payload),
    )


__all__ = [
    "FRESH_EXIT_GRID_SCHEMA",
    "FRESH_EXIT_QUANTILE_RANKS",
    "FRESH_EXIT_RANK_NEIGHBOURHOOD",
    "FRESH_EXIT_VOLATILITY_COLUMN",
    "MAXIMUM_EXIT_VARIANTS",
    "REGISTERED_MAXIMUM_HOLDING_MS",
    "ExitThresholdProvenance",
    "ExplicitExitConstant",
    "FreshExitGrid",
    "FreshExitVariant",
    "RegisteredExecutionScenario",
    "build_fresh_exit_grid",
    "fresh_exit_quantile_measurements",
]
