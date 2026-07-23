"""Deterministic scoring and chronological promotion gates for fresh research.

The module consumes already completed entry diagnostics and replay trades.  It
does not read ticks, fit parameters, generate signals, or authorize holdout
access.  All selection thresholds are supplied explicitly, or materialized
from a validated fresh preregistration document.

Input objects are intentionally duck typed so the scorer can consume immutable
dataclasses as well as decoded audit artifacts.  Validation is strict: missing
fields, non-finite monetary values, inconsistent event counts, and
non-chronological trades are rejected instead of being silently repaired.
"""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from statistics import median
from typing import Any, Iterable, Mapping, Sequence


REQUIRED_COVERAGE_CHECKPOINTS_SECONDS = (1, 2, 5, 10, 20, 30, 60)
BALANCED_COMPONENT_NAMES = (
    "expectancyScaledByMedianAbsoluteTradePnl",
    "coverageProbabilityAndSpeed",
    "profitFactorCappedAtTwo",
    "inverseDrawdownToGrossProfit",
    "positiveSessionFraction",
    "requiredStressPassFraction",
    "inverseLargestSessionProfitConcentration",
    "tradeCountAdequacy",
)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite(value: Any, name: str) -> float:
    if not _is_number(value) or not math.isfinite(float(value)):
        raise ValueError(f"{name} must be a finite number")
    return float(value)


def _non_negative(value: Any, name: str) -> float:
    result = _finite(value, name)
    if result < 0.0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _probability(value: Any, name: str) -> float:
    result = _finite(value, name)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{name} must be in [0, 1]")
    return result


def _positive_integer(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _non_negative_integer(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _get(obj: Any, name: str) -> Any:
    if isinstance(obj, Mapping):
        if name not in obj:
            raise ValueError(f"input is missing required field {name!r}")
        return obj[name]
    try:
        return getattr(obj, name)
    except AttributeError as exc:
        raise ValueError(f"input is missing required field {name!r}") from exc


def _optional(obj: Any, name: str, fallback: Any) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, fallback)
    return getattr(obj, name, fallback)


def _as_sequence(value: Any, name: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{name} must be a sequence, not text")
    try:
        return tuple(value)
    except TypeError as exc:
        raise ValueError(f"{name} must be iterable") from exc


def _as_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return value


def _label(value: Any) -> str:
    """Return a stable, loss-resistant label for a slice value."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        if not value:
            raise ValueError("slice values cannot be empty strings")
        return value
    if value is None:
        raise ValueError("slice metadata cannot be null")
    if isinstance(value, bool):
        return "true" if value else "false"
    if _is_number(value):
        numeric = _finite(value, "slice value")
        return str(int(numeric)) if numeric.is_integer() else repr(numeric)
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "slice metadata must have a deterministic representation"
        ) from exc


def _metadata_path(metadata: Mapping[str, Any], path: str) -> Any:
    current: Any = metadata
    for component in path.split("."):
        if not isinstance(current, Mapping) or component not in current:
            raise ValueError(f"metadata is missing required path {path!r}")
        current = current[component]
    return current


def _sorted_counts(values: Iterable[str]) -> tuple[tuple[str, int], ...]:
    return tuple(sorted(Counter(values).items()))


def _quantile_linear(values: Sequence[float], probability: float) -> float | None:
    """R-7/NumPy-linear quantile, with an explicit deterministic singleton case."""

    if not values:
        return None
    probability = _probability(probability, "quantile probability")
    ordered = sorted(_finite(value, "quantile value") for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + fraction * (ordered[upper] - ordered[lower])


def _clip(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


@dataclass(frozen=True, slots=True)
class SliceDimensions:
    """Explicit metadata paths used for day, activity-session, and regime slices."""

    day_metadata_path: str
    market_session_metadata_path: str
    regime_metadata_path: str

    def __post_init__(self) -> None:
        for name in (
            "day_metadata_path",
            "market_session_metadata_path",
            "regime_metadata_path",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, str)
                or not value.strip()
                or value != value.strip()
            ):
                raise ValueError(f"{name} must be non-empty and trimmed")
            if any(not part for part in value.split(".")):
                raise ValueError(f"{name} must be a valid dotted metadata path")


@dataclass(frozen=True, slots=True)
class EntryMetricConfig:
    coverage_checkpoints_seconds: tuple[int, ...]
    restricted_uncovered_milliseconds: int
    profit_barrier_net_per_unit: float
    loss_barrier_net_per_unit: float

    def __post_init__(self) -> None:
        if not isinstance(self.coverage_checkpoints_seconds, tuple):
            raise ValueError("coverage_checkpoints_seconds must be a tuple")
        if (
            tuple(self.coverage_checkpoints_seconds)
            != REQUIRED_COVERAGE_CHECKPOINTS_SECONDS
        ):
            raise ValueError(
                "coverage checkpoints must be exactly 1,2,5,10,20,30,60 seconds"
            )
        _positive_integer(
            self.restricted_uncovered_milliseconds,
            "restricted_uncovered_milliseconds",
        )
        if self.restricted_uncovered_milliseconds != 60_000:
            raise ValueError("restricted uncovered time must be exactly 60,000ms")
        for name in (
            "profit_barrier_net_per_unit",
            "loss_barrier_net_per_unit",
        ):
            if _finite(getattr(self, name), name) <= 0.0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class TradeMetricConfig:
    pnl_classification_tolerance: float
    loss_tail_quantile_probability: float
    require_boundary_reached: bool

    def __post_init__(self) -> None:
        _non_negative(
            self.pnl_classification_tolerance,
            "pnl_classification_tolerance",
        )
        _probability(
            self.loss_tail_quantile_probability,
            "loss_tail_quantile_probability",
        )
        if self.loss_tail_quantile_probability != 0.95:
            raise ValueError("loss tail quantile must be exactly 0.95")
        if not isinstance(self.require_boundary_reached, bool):
            raise ValueError("require_boundary_reached must be boolean")


@dataclass(frozen=True, slots=True)
class MinimumSampleThresholds:
    filled_trades_per_session: int
    absolute_filled_trades: int
    active_session_fraction_minimum: float

    def __post_init__(self) -> None:
        _positive_integer(self.filled_trades_per_session, "filled_trades_per_session")
        _positive_integer(self.absolute_filled_trades, "absolute_filled_trades")
        _probability(
            self.active_session_fraction_minimum,
            "active_session_fraction_minimum",
        )

    def required_count(self, evaluated_session_count: int) -> int:
        _positive_integer(evaluated_session_count, "evaluated_session_count")
        return max(
            self.absolute_filled_trades,
            self.filled_trades_per_session * evaluated_session_count,
        )


@dataclass(frozen=True, slots=True)
class EntryPromotionThresholds:
    fill_rate_minimum: float
    coverage_10_seconds_minimum: float
    coverage_30_seconds_minimum: float
    coverage_60_seconds_minimum: float
    restricted_median_coverage_milliseconds_maximum: float
    censored_fraction_maximum: float
    equal_barrier_distance_per_unit: float
    equal_barrier_profit_first_rate_minimum: float

    def __post_init__(self) -> None:
        for name in (
            "fill_rate_minimum",
            "coverage_10_seconds_minimum",
            "coverage_30_seconds_minimum",
            "coverage_60_seconds_minimum",
            "censored_fraction_maximum",
            "equal_barrier_profit_first_rate_minimum",
        ):
            _probability(getattr(self, name), name)
        _non_negative(
            self.restricted_median_coverage_milliseconds_maximum,
            "restricted_median_coverage_milliseconds_maximum",
        )
        if (
            _finite(
                self.equal_barrier_distance_per_unit,
                "equal_barrier_distance_per_unit",
            )
            <= 0.0
        ):
            raise ValueError("equal_barrier_distance_per_unit must be positive")


@dataclass(frozen=True, slots=True)
class FullStrategyThresholds:
    reference_profit_factor_minimum: float
    positive_session_fraction_minimum: float
    maximum_drawdown_to_gross_profit_maximum: float
    largest_trade_share_of_gross_profit_maximum: float
    largest_session_share_of_gross_profit_maximum: float
    loss_95_to_median_absolute_loss_maximum: float
    required_stress_profit_factor_minimum: float
    full_replay_censor_count_maximum: int
    reference_net_pnl_strictly_positive: bool
    reference_expectancy_strictly_positive: bool
    required_stress_net_pnl_strictly_positive: bool
    profitability_valid_required: bool
    entry_promotion_gates_still_required: bool

    def __post_init__(self) -> None:
        for name in (
            "reference_profit_factor_minimum",
            "maximum_drawdown_to_gross_profit_maximum",
            "largest_trade_share_of_gross_profit_maximum",
            "largest_session_share_of_gross_profit_maximum",
            "loss_95_to_median_absolute_loss_maximum",
            "required_stress_profit_factor_minimum",
        ):
            _non_negative(getattr(self, name), name)
        _probability(
            self.positive_session_fraction_minimum,
            "positive_session_fraction_minimum",
        )
        _non_negative_integer(
            self.full_replay_censor_count_maximum,
            "full_replay_censor_count_maximum",
        )
        for name in (
            "reference_net_pnl_strictly_positive",
            "reference_expectancy_strictly_positive",
            "required_stress_net_pnl_strictly_positive",
            "profitability_valid_required",
            "entry_promotion_gates_still_required",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be boolean")


@dataclass(frozen=True, slots=True)
class BalancedScoreSpecification:
    component_weights: tuple[tuple[str, float], ...]
    coverage_probability_weights: tuple[tuple[int, float], ...]
    coverage_probability_share: float
    restricted_median_speed_share: float

    def __post_init__(self) -> None:
        if not isinstance(self.component_weights, tuple):
            raise ValueError("component_weights must be a tuple")
        if not isinstance(self.coverage_probability_weights, tuple):
            raise ValueError("coverage_probability_weights must be a tuple")
        components = tuple(name for name, _ in self.component_weights)
        if components != BALANCED_COMPONENT_NAMES:
            raise ValueError(
                "component_weights must use the registered component order exactly"
            )
        component_values = tuple(
            _non_negative(weight, f"component weight {name!r}")
            for name, weight in self.component_weights
        )
        if not math.isclose(sum(component_values), 1.0, abs_tol=1e-12):
            raise ValueError("balanced component weights must sum to one")
        checkpoint_names = tuple(
            checkpoint for checkpoint, _ in self.coverage_probability_weights
        )
        if checkpoint_names != (2, 5, 10, 30, 60):
            raise ValueError("coverage score checkpoints must be exactly 2,5,10,30,60")
        probability_values = tuple(
            _non_negative(weight, f"coverage weight {checkpoint}s")
            for checkpoint, weight in self.coverage_probability_weights
        )
        if not math.isclose(sum(probability_values), 1.0, abs_tol=1e-12):
            raise ValueError("coverage probability weights must sum to one")
        probability_share = _probability(
            self.coverage_probability_share,
            "coverage_probability_share",
        )
        speed_share = _probability(
            self.restricted_median_speed_share,
            "restricted_median_speed_share",
        )
        if not math.isclose(probability_share + speed_share, 1.0, abs_tol=1e-12):
            raise ValueError("coverage probability and speed shares must sum to one")


@dataclass(frozen=True, slots=True)
class RegisteredScoringConfig:
    entry_metrics: EntryMetricConfig
    trade_metrics: TradeMetricConfig
    minimum_sample: MinimumSampleThresholds
    entry_gate: EntryPromotionThresholds
    full_gate: FullStrategyThresholds
    balanced_score: BalancedScoreSpecification
    required_stress_scenario_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        expected_types = (
            ("entry_metrics", EntryMetricConfig),
            ("trade_metrics", TradeMetricConfig),
            ("minimum_sample", MinimumSampleThresholds),
            ("entry_gate", EntryPromotionThresholds),
            ("full_gate", FullStrategyThresholds),
            ("balanced_score", BalancedScoreSpecification),
        )
        for name, expected in expected_types:
            if not isinstance(getattr(self, name), expected):
                raise ValueError(f"{name} must be {expected.__name__}")
        if not isinstance(self.required_stress_scenario_ids, tuple):
            raise ValueError("required_stress_scenario_ids must be a tuple")
        if not self.required_stress_scenario_ids:
            raise ValueError("at least one required stress scenario is required")
        if len(set(self.required_stress_scenario_ids)) != len(
            self.required_stress_scenario_ids
        ):
            raise ValueError("required stress scenario ids must be unique")
        if any(
            not isinstance(value, str) or not value.strip() or value != value.strip()
            for value in self.required_stress_scenario_ids
        ):
            raise ValueError(
                "required stress scenario ids must be non-empty and trimmed"
            )


@dataclass(frozen=True, slots=True)
class EntryMetrics:
    signal_count: int
    filled_count: int
    rejected_count: int
    censored_count: int
    fill_rate: float | None
    censored_fraction: float | None
    coverage_probabilities: tuple[tuple[int, float | None], ...]
    restricted_median_coverage_milliseconds: float | None
    median_covered_time_milliseconds: float | None
    barrier_profit_first_count: int
    barrier_loss_first_count: int
    barrier_no_hit_count: int
    barrier_profit_first_rate: float | None
    rejection_reason_counts: tuple[tuple[str, int], ...]
    censor_reason_counts: tuple[tuple[str, int], ...]
    evaluated_session_count: int
    active_session_count: int
    active_session_fraction: float | None
    profit_barrier_net_per_unit: float
    loss_barrier_net_per_unit: float

    def coverage_probability(self, checkpoint_seconds: int) -> float | None:
        for checkpoint, value in self.coverage_probabilities:
            if checkpoint == checkpoint_seconds:
                return value
        raise KeyError(checkpoint_seconds)


@dataclass(frozen=True, slots=True)
class EntryScoreReport:
    overall: EntryMetrics
    by_day: tuple[tuple[str, EntryMetrics], ...]
    by_side: tuple[tuple[str, EntryMetrics], ...]
    by_market_session: tuple[tuple[str, EntryMetrics], ...]
    by_regime: tuple[tuple[str, EntryMetrics], ...]


@dataclass(frozen=True, slots=True)
class TradeMetrics:
    trade_count: int
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float | None
    net_pnl: float
    expectancy: float | None
    gross_profit: float
    gross_loss: float
    profit_factor: float | str | None
    maximum_drawdown: float
    maximum_drawdown_to_gross_profit: float | None
    median_absolute_trade_pnl: float | None
    loss_95_absolute: float | None
    median_absolute_loss: float | None
    loss_95_to_median_absolute_loss: float | None
    largest_trade_share_of_gross_profit: float | None
    positive_trade_profit_hhi: float | None
    largest_session_share_of_gross_profit: float | None
    positive_session_profit_hhi: float | None
    evaluated_session_count: int
    active_session_count: int
    active_session_fraction: float | None
    positive_session_count: int
    positive_session_fraction: float | None
    session_net_pnl: tuple[tuple[str, float], ...]
    replay_censor_count: int
    profitability_valid: bool


@dataclass(frozen=True, slots=True)
class TradeScoreReport:
    overall: TradeMetrics
    by_day: tuple[tuple[str, TradeMetrics], ...]
    by_side: tuple[tuple[str, TradeMetrics], ...]
    by_market_session: tuple[tuple[str, TradeMetrics], ...]
    by_regime: tuple[tuple[str, TradeMetrics], ...]


@dataclass(frozen=True, slots=True)
class GateCheck:
    name: str
    passed: bool
    actual: Any
    comparator: str
    threshold: Any


@dataclass(frozen=True, slots=True)
class GateResult:
    passed: bool
    checks: tuple[GateCheck, ...]

    @property
    def failed_check_names(self) -> tuple[str, ...]:
        return tuple(check.name for check in self.checks if not check.passed)


@dataclass(frozen=True, slots=True)
class BalancedScoreResult:
    score: float | None
    components: tuple[tuple[str, float | None], ...]
    invalid_components: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return self.score is not None


@dataclass(frozen=True, slots=True)
class CandidateScorecard:
    entry: EntryScoreReport
    reference: TradeScoreReport
    stresses: tuple[tuple[str, TradeScoreReport], ...]
    entry_gate: GateResult
    full_gate: GateResult
    balanced_score: BalancedScoreResult


@dataclass(frozen=True, slots=True)
class ChronologicalGateItem:
    window_name: str
    start: datetime
    end: datetime
    gate: GateResult

    def __post_init__(self) -> None:
        if not isinstance(self.window_name, str) or not self.window_name.strip():
            raise ValueError("window_name must be non-empty")
        if self.start.tzinfo is None or self.start.utcoffset() is None:
            raise ValueError("window start must be timezone-aware")
        if self.end.tzinfo is None or self.end.utcoffset() is None:
            raise ValueError("window end must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("window end must be after start")
        if not isinstance(self.gate, GateResult):
            raise ValueError("gate must be GateResult")


@dataclass(frozen=True, slots=True)
class ChronologicalGateResult:
    passed: bool
    required_windows: tuple[str, ...]
    failed_windows: tuple[str, ...]
    evaluated_windows: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CandidateRankRecord:
    candidate_id: str
    hard_gate_passed: bool
    balanced_score: float | None
    per_window_expectancies: tuple[float, ...]
    required_stress_expectancies: tuple[float, ...]
    maximum_drawdown: float
    rule_complexity: int

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_id, str) or not self.candidate_id.strip():
            raise ValueError("candidate_id must be non-empty")
        if self.candidate_id != self.candidate_id.strip():
            raise ValueError("candidate_id must be trimmed")
        if not isinstance(self.hard_gate_passed, bool):
            raise ValueError("hard_gate_passed must be boolean")
        if self.balanced_score is not None:
            score = _finite(self.balanced_score, "balanced_score")
            if not -1.0 <= score <= 1.0:
                raise ValueError("balanced_score must be in [-1,1] or None")
        if not self.per_window_expectancies:
            raise ValueError("per_window_expectancies cannot be empty")
        if not self.required_stress_expectancies:
            raise ValueError("required_stress_expectancies cannot be empty")
        for index, value in enumerate(self.per_window_expectancies):
            _finite(value, f"per_window_expectancies[{index}]")
        for index, value in enumerate(self.required_stress_expectancies):
            _finite(value, f"required_stress_expectancies[{index}]")
        _non_negative(self.maximum_drawdown, "maximum_drawdown")
        _non_negative_integer(self.rule_complexity, "rule_complexity")


@dataclass(frozen=True, slots=True)
class _EntryObservation:
    source: Any
    is_fill: bool
    event_position: int
    day: str
    side: str
    market_session: str
    regime: str


@dataclass(frozen=True, slots=True)
class _TradeObservation:
    source: Any
    net_pnl: float
    day: str
    side: str
    market_session: str
    regime: str
    entry_key: tuple[datetime, int]
    exit_key: tuple[datetime, int]


def _event_dimensions(
    event: Any, dimensions: SliceDimensions
) -> tuple[str, str, str, str]:
    metadata = _as_mapping(_get(event, "metadata"), "event metadata")
    side = _get(event, "side")
    if side not in ("long", "short"):
        raise ValueError("entry event side must be 'long' or 'short'")
    return (
        _label(_metadata_path(metadata, dimensions.day_metadata_path)),
        side,
        _label(_metadata_path(metadata, dimensions.market_session_metadata_path)),
        _label(_metadata_path(metadata, dimensions.regime_metadata_path)),
    )


def _evaluated_session_labels(values: Sequence[Any]) -> tuple[str, ...]:
    labels = tuple(_label(value) for value in values)
    if not labels:
        raise ValueError("evaluated_sessions cannot be empty")
    if len(labels) != len(set(labels)):
        raise ValueError("evaluated_sessions must be unique after normalization")
    return labels


def _entry_observations(
    result: Any,
    dimensions: SliceDimensions,
) -> tuple[tuple[_EntryObservation, ...], int]:
    diagnostics = _as_sequence(_get(result, "diagnostics"), "diagnostics")
    rejections = _as_sequence(_get(result, "rejections"), "rejections")
    event_count = _non_negative_integer(_get(result, "event_count"), "event_count")
    if event_count != len(diagnostics) + len(rejections):
        raise ValueError("event_count must equal filled plus rejected entries")

    observations: list[_EntryObservation] = []
    positions: set[int] = set()
    for is_fill, items in ((True, diagnostics), (False, rejections)):
        for item in items:
            position = _non_negative_integer(
                _get(item, "event_position"), "event_position"
            )
            if position in positions:
                raise ValueError(f"duplicate event_position {position}")
            positions.add(position)
            event = _get(item, "event")
            day, side, market_session, regime = _event_dimensions(event, dimensions)
            observations.append(
                _EntryObservation(
                    source=item,
                    is_fill=is_fill,
                    event_position=position,
                    day=day,
                    side=side,
                    market_session=market_session,
                    regime=regime,
                )
            )
    if positions != set(range(event_count)):
        raise ValueError("event positions must cover exactly range(event_count)")
    observations.sort(key=lambda observation: observation.event_position)

    reported_reasons = _as_mapping(
        _get(result, "rejected_reason_counts"), "rejected_reason_counts"
    )
    calculated = Counter(str(_get(item, "reason")) for item in rejections)
    normalized_reported: dict[str, int] = {}
    for reason, count in reported_reasons.items():
        if not isinstance(reason, str) or not reason:
            raise ValueError("rejection reason keys must be non-empty strings")
        normalized_reported[reason] = _non_negative_integer(
            count, f"rejection count for {reason!r}"
        )
    if dict(calculated) != normalized_reported:
        raise ValueError("rejected_reason_counts does not match rejection records")
    return tuple(observations), event_count


def _coverage_successes(
    diagnostic: Any,
    config: EntryMetricConfig,
) -> tuple[dict[int, bool], float | None, bool, str | None]:
    censored = _get(diagnostic, "censored")
    horizon_complete = _get(diagnostic, "horizon_complete")
    if not isinstance(censored, bool) or not isinstance(horizon_complete, bool):
        raise ValueError("diagnostic censor flags must be boolean")
    if censored == horizon_complete:
        raise ValueError("censored must be the inverse of horizon_complete")

    raw_coverage = _get(diagnostic, "time_to_cost_coverage_ms")
    if raw_coverage is None:
        coverage_ms = None
    else:
        coverage_ms = _non_negative(raw_coverage, "time_to_cost_coverage_ms")
        if coverage_ms >= config.restricted_uncovered_milliseconds:
            raise ValueError(
                "first cost coverage must occur strictly before 60 seconds"
            )

    successes: dict[int, bool] = {}
    for checkpoint in config.coverage_checkpoints_seconds:
        expected = bool(
            coverage_ms is not None
            and (
                coverage_ms < checkpoint * 1_000.0
                if checkpoint == 60
                else coverage_ms <= checkpoint * 1_000.0
            )
        )
        field_name = f"cost_covered_by_{checkpoint}s"
        reported = _get(diagnostic, field_name)
        if not isinstance(reported, bool):
            raise ValueError(f"{field_name} must be boolean")
        if reported != expected:
            raise ValueError(f"{field_name} is inconsistent with first-coverage time")
        # The registered denominator explicitly treats every censored fill as a
        # failure, even if it happened to cross break-even before censoring.
        successes[checkpoint] = expected and not censored

    barrier = _get(diagnostic, "first_barrier_hit")
    if barrier not in (None, "profit", "loss"):
        raise ValueError("first_barrier_hit must be profit, loss, or None")
    barrier_success = barrier == "profit" and not censored
    censor_reason = (
        str(_get(diagnostic, "observation_end_reason")) if censored else None
    )
    if censor_reason == "":
        raise ValueError("censored diagnostic must have a non-empty reason")
    return successes, coverage_ms, barrier_success, censor_reason


def _compute_entry_metrics(
    observations: Sequence[_EntryObservation],
    evaluated_sessions: Sequence[str],
    config: EntryMetricConfig,
) -> EntryMetrics:
    signals = len(observations)
    fills = [observation for observation in observations if observation.is_fill]
    rejections = [
        observation for observation in observations if not observation.is_fill
    ]
    fill_count = len(fills)
    coverage_counts = {
        checkpoint: 0 for checkpoint in config.coverage_checkpoints_seconds
    }
    restricted_times: list[float] = []
    covered_times: list[float] = []
    censored_count = 0
    profit_first = 0
    loss_first = 0
    no_hit = 0
    censor_reasons: list[str] = []

    for observation in fills:
        diagnostic = observation.source
        successes, coverage_ms, barrier_success, censor_reason = _coverage_successes(
            diagnostic, config
        )
        censored = bool(_get(diagnostic, "censored"))
        if censored:
            censored_count += 1
            assert censor_reason is not None
            censor_reasons.append(censor_reason)
        if coverage_ms is not None and not censored:
            restricted_times.append(coverage_ms)
            covered_times.append(coverage_ms)
        else:
            restricted_times.append(float(config.restricted_uncovered_milliseconds))
        for checkpoint, success in successes.items():
            coverage_counts[checkpoint] += int(success)

        barrier = _get(diagnostic, "first_barrier_hit")
        if barrier_success:
            profit_first += 1
        elif barrier == "loss":
            loss_first += 1
        else:
            no_hit += 1

    active = {observation.day for observation in fills}
    evaluated_set = set(evaluated_sessions)
    if not active <= evaluated_set:
        outside = sorted(active - evaluated_set)
        raise ValueError(f"filled entries refer to unevaluated sessions: {outside}")
    if any(observation.day not in evaluated_set for observation in rejections):
        raise ValueError("rejected entries refer to an unevaluated session")
    evaluated_count = len(evaluated_sessions)

    return EntryMetrics(
        signal_count=signals,
        filled_count=fill_count,
        rejected_count=len(rejections),
        censored_count=censored_count,
        fill_rate=fill_count / signals if signals else None,
        censored_fraction=censored_count / fill_count if fill_count else None,
        coverage_probabilities=tuple(
            (
                checkpoint,
                coverage_counts[checkpoint] / fill_count if fill_count else None,
            )
            for checkpoint in config.coverage_checkpoints_seconds
        ),
        restricted_median_coverage_milliseconds=(
            float(median(restricted_times)) if restricted_times else None
        ),
        median_covered_time_milliseconds=(
            float(median(covered_times)) if covered_times else None
        ),
        barrier_profit_first_count=profit_first,
        barrier_loss_first_count=loss_first,
        barrier_no_hit_count=no_hit,
        barrier_profit_first_rate=(profit_first / fill_count if fill_count else None),
        rejection_reason_counts=_sorted_counts(
            str(_get(observation.source, "reason")) for observation in rejections
        ),
        censor_reason_counts=_sorted_counts(censor_reasons),
        evaluated_session_count=evaluated_count,
        active_session_count=len(active),
        active_session_fraction=(
            len(active) / evaluated_count if evaluated_count else None
        ),
        profit_barrier_net_per_unit=float(config.profit_barrier_net_per_unit),
        loss_barrier_net_per_unit=float(config.loss_barrier_net_per_unit),
    )


def _entry_slices(
    observations: Sequence[_EntryObservation],
    attribute: str,
    evaluated_sessions: tuple[str, ...],
    config: EntryMetricConfig,
    include_empty_days: bool,
) -> tuple[tuple[str, EntryMetrics], ...]:
    grouped: dict[str, list[_EntryObservation]] = defaultdict(list)
    for observation in observations:
        grouped[str(getattr(observation, attribute))].append(observation)
    labels = set(grouped)
    if include_empty_days:
        labels.update(evaluated_sessions)
    output: list[tuple[str, EntryMetrics]] = []
    for label in sorted(labels):
        slice_sessions = (label,) if include_empty_days else evaluated_sessions
        output.append(
            (
                label,
                _compute_entry_metrics(grouped.get(label, ()), slice_sessions, config),
            )
        )
    return tuple(output)


def score_entry_diagnostics(
    result: Any,
    *,
    config: EntryMetricConfig,
    dimensions: SliceDimensions,
    evaluated_sessions: Sequence[Any],
) -> EntryScoreReport:
    """Score filled/rejected entry diagnostics without reading their source ticks."""

    if not isinstance(config, EntryMetricConfig):
        raise ValueError("config must be EntryMetricConfig")
    if not isinstance(dimensions, SliceDimensions):
        raise ValueError("dimensions must be SliceDimensions")
    sessions = _evaluated_session_labels(evaluated_sessions)
    observations, _ = _entry_observations(result, dimensions)
    overall = _compute_entry_metrics(observations, sessions, config)
    return EntryScoreReport(
        overall=overall,
        by_day=_entry_slices(observations, "day", sessions, config, True),
        by_side=_entry_slices(observations, "side", sessions, config, False),
        by_market_session=_entry_slices(
            observations, "market_session", sessions, config, False
        ),
        by_regime=_entry_slices(observations, "regime", sessions, config, False),
    )


def _trade_observations(
    trades: Iterable[Any],
    dimensions: SliceDimensions,
) -> tuple[_TradeObservation, ...]:
    output: list[_TradeObservation] = []
    seen_entry_ids: set[int] = set()
    previous_entry_key: tuple[datetime, int] | None = None
    previous_exit_key: tuple[datetime, int] | None = None
    for position, trade in enumerate(trades):
        pnl = _finite(_get(trade, "net_pnl"), f"trade[{position}].net_pnl")
        side = _get(trade, "side")
        if side not in ("long", "short"):
            raise ValueError(f"trade[{position}] side must be long or short")
        metadata = _as_mapping(
            _get(trade, "entry_metadata"), f"trade[{position}].entry_metadata"
        )
        day = _label(_metadata_path(metadata, dimensions.day_metadata_path))
        market_session = _label(
            _metadata_path(metadata, dimensions.market_session_metadata_path)
        )
        regime = _label(_metadata_path(metadata, dimensions.regime_metadata_path))

        entry_timestamp = _get(trade, "entry_fill_timestamp")
        exit_timestamp = _get(trade, "exit_fill_timestamp")
        if not isinstance(entry_timestamp, datetime) or entry_timestamp.tzinfo is None:
            raise ValueError(
                f"trade[{position}] entry timestamp must be timezone-aware"
            )
        if entry_timestamp.utcoffset() is None:
            raise ValueError(f"trade[{position}] entry timestamp offset is unusable")
        if not isinstance(exit_timestamp, datetime) or exit_timestamp.tzinfo is None:
            raise ValueError(f"trade[{position}] exit timestamp must be timezone-aware")
        if exit_timestamp.utcoffset() is None:
            raise ValueError(f"trade[{position}] exit timestamp offset is unusable")
        entry_id = _get(trade, "entry_fill_tick_id")
        exit_id = _get(trade, "exit_fill_tick_id")
        if not isinstance(entry_id, int) or isinstance(entry_id, bool):
            raise ValueError(f"trade[{position}] entry tick id must be an integer")
        if not isinstance(exit_id, int) or isinstance(exit_id, bool):
            raise ValueError(f"trade[{position}] exit tick id must be an integer")
        if entry_id in seen_entry_ids:
            raise ValueError(f"duplicate trade entry tick id {entry_id}")
        seen_entry_ids.add(entry_id)
        entry_key = (entry_timestamp, entry_id)
        exit_key = (exit_timestamp, exit_id)
        if exit_key <= entry_key:
            raise ValueError(f"trade[{position}] exit must be later than entry")
        if previous_entry_key is not None and entry_key <= previous_entry_key:
            raise ValueError("trades must be strictly chronological by entry fill")
        if previous_exit_key is not None and exit_key <= previous_exit_key:
            raise ValueError("trades must be strictly chronological by exit fill")
        previous_entry_key = entry_key
        previous_exit_key = exit_key
        output.append(
            _TradeObservation(
                source=trade,
                net_pnl=pnl,
                day=day,
                side=side,
                market_session=market_session,
                regime=regime,
                entry_key=entry_key,
                exit_key=exit_key,
            )
        )
    return tuple(output)


def _compute_trade_metrics(
    observations: Sequence[_TradeObservation],
    evaluated_sessions: Sequence[str],
    config: TradeMetricConfig,
    *,
    replay_censor_count: int,
    profitability_valid: bool,
) -> TradeMetrics:
    tolerance = float(config.pnl_classification_tolerance)
    values = [observation.net_pnl for observation in observations]
    winners = [value for value in values if value > tolerance]
    losers = [value for value in values if value < -tolerance]
    flat_count = len(values) - len(winners) - len(losers)
    try:
        gross_profit = _finite(math.fsum(winners), "gross_profit")
        gross_loss = _finite(-math.fsum(losers), "gross_loss")
        net_pnl = _finite(math.fsum(values), "net_pnl")
    except OverflowError as exc:
        raise ValueError("trade P&L aggregation overflowed") from exc
    if gross_loss > 0.0:
        calculated_profit_factor = gross_profit / gross_loss
        profit_factor: float | str | None = (
            calculated_profit_factor
            if math.isfinite(calculated_profit_factor)
            else "Infinity"
        )
    elif gross_profit > 0.0:
        # Use the same JSON-safe convention as FreshReplayResult.summary().
        # This avoids leaking a non-finite float into the append-only ledger.
        profit_factor = "Infinity"
    else:
        profit_factor = None

    equity = 0.0
    peak = 0.0
    maximum_drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum_drawdown = max(maximum_drawdown, peak - equity)

    session_pnl = {session: 0.0 for session in evaluated_sessions}
    for observation in observations:
        if observation.day not in session_pnl:
            raise ValueError(f"trade refers to unevaluated session {observation.day!r}")
        session_pnl[observation.day] += observation.net_pnl
    active = {observation.day for observation in observations}
    positive_sessions = [value for value in session_pnl.values() if value > tolerance]
    evaluated_count = len(evaluated_sessions)

    absolute_losses = [-value for value in losers]
    loss_95 = _quantile_linear(absolute_losses, config.loss_tail_quantile_probability)
    median_loss = float(median(absolute_losses)) if absolute_losses else None
    loss_tail_ratio = (
        loss_95 / median_loss
        if loss_95 is not None and median_loss is not None and median_loss > 0.0
        else None
    )
    positive_trade_shares = (
        [value / gross_profit for value in winners] if gross_profit > 0.0 else []
    )
    positive_session_shares = (
        [value / gross_profit for value in positive_sessions]
        if gross_profit > 0.0
        else []
    )

    return TradeMetrics(
        trade_count=len(values),
        win_count=len(winners),
        loss_count=len(losers),
        flat_count=flat_count,
        win_rate=len(winners) / len(values) if values else None,
        net_pnl=net_pnl,
        expectancy=net_pnl / len(values) if values else None,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=profit_factor,
        maximum_drawdown=maximum_drawdown,
        maximum_drawdown_to_gross_profit=(
            maximum_drawdown / gross_profit if gross_profit > 0.0 else None
        ),
        median_absolute_trade_pnl=(
            float(median(abs(value) for value in values)) if values else None
        ),
        loss_95_absolute=loss_95,
        median_absolute_loss=median_loss,
        loss_95_to_median_absolute_loss=loss_tail_ratio,
        largest_trade_share_of_gross_profit=(
            max(positive_trade_shares) if positive_trade_shares else None
        ),
        positive_trade_profit_hhi=(
            math.fsum(share * share for share in positive_trade_shares)
            if positive_trade_shares
            else None
        ),
        largest_session_share_of_gross_profit=(
            max(positive_session_shares) if positive_session_shares else None
        ),
        positive_session_profit_hhi=(
            math.fsum(share * share for share in positive_session_shares)
            if positive_session_shares
            else None
        ),
        evaluated_session_count=evaluated_count,
        active_session_count=len(active),
        active_session_fraction=(
            len(active) / evaluated_count if evaluated_count else None
        ),
        positive_session_count=len(positive_sessions),
        positive_session_fraction=(
            len(positive_sessions) / evaluated_count if evaluated_count else None
        ),
        session_net_pnl=tuple(
            (session, session_pnl[session]) for session in evaluated_sessions
        ),
        replay_censor_count=replay_censor_count,
        profitability_valid=profitability_valid,
    )


def _trade_slices(
    observations: Sequence[_TradeObservation],
    attribute: str,
    evaluated_sessions: tuple[str, ...],
    config: TradeMetricConfig,
    profitability_valid: bool,
    include_empty_days: bool,
) -> tuple[tuple[str, TradeMetrics], ...]:
    grouped: dict[str, list[_TradeObservation]] = defaultdict(list)
    for observation in observations:
        grouped[str(getattr(observation, attribute))].append(observation)
    labels = set(grouped)
    if include_empty_days:
        labels.update(evaluated_sessions)
    output: list[tuple[str, TradeMetrics]] = []
    for label in sorted(labels):
        sessions = (label,) if include_empty_days else evaluated_sessions
        output.append(
            (
                label,
                _compute_trade_metrics(
                    grouped.get(label, ()),
                    sessions,
                    config,
                    replay_censor_count=0,
                    profitability_valid=profitability_valid,
                ),
            )
        )
    return tuple(output)


def score_trade_records(
    trades: Iterable[Any],
    *,
    config: TradeMetricConfig,
    dimensions: SliceDimensions,
    evaluated_sessions: Sequence[Any],
    replay_censor_count: int,
    profitability_valid: bool,
) -> TradeScoreReport:
    """Score a chronological trade sequence with explicit completeness facts."""

    if not isinstance(config, TradeMetricConfig):
        raise ValueError("config must be TradeMetricConfig")
    if not isinstance(dimensions, SliceDimensions):
        raise ValueError("dimensions must be SliceDimensions")
    censor_count = _non_negative_integer(replay_censor_count, "replay_censor_count")
    if not isinstance(profitability_valid, bool):
        raise ValueError("profitability_valid must be boolean")
    if censor_count and profitability_valid:
        raise ValueError("profitability cannot be valid when replay censors exist")
    sessions = _evaluated_session_labels(evaluated_sessions)
    observations = _trade_observations(trades, dimensions)
    overall = _compute_trade_metrics(
        observations,
        sessions,
        config,
        replay_censor_count=censor_count,
        profitability_valid=profitability_valid,
    )
    return TradeScoreReport(
        overall=overall,
        by_day=_trade_slices(
            observations, "day", sessions, config, profitability_valid, True
        ),
        by_side=_trade_slices(
            observations, "side", sessions, config, profitability_valid, False
        ),
        by_market_session=_trade_slices(
            observations,
            "market_session",
            sessions,
            config,
            profitability_valid,
            False,
        ),
        by_regime=_trade_slices(
            observations, "regime", sessions, config, profitability_valid, False
        ),
    )


def score_replay_result(
    replay_result: Any,
    *,
    config: TradeMetricConfig,
    dimensions: SliceDimensions,
    evaluated_sessions: Sequence[Any],
) -> TradeScoreReport:
    """Score a fresh replay result and conservatively classify completeness."""

    trades = _as_sequence(_get(replay_result, "trades"), "replay trades")
    censors = _as_sequence(_get(replay_result, "censors"), "replay censors")
    halted = _get(replay_result, "halted")
    boundary_reached = _get(replay_result, "boundary_reached")
    if not isinstance(halted, bool) or not isinstance(boundary_reached, bool):
        raise ValueError("replay halt and boundary flags must be boolean")

    replay_config = _optional(replay_result, "config", None)
    if replay_config is not None:
        replay_tolerance = _finite(
            _get(replay_config, "pnl_classification_tolerance"),
            "replay pnl_classification_tolerance",
        )
        if not math.isclose(
            replay_tolerance,
            config.pnl_classification_tolerance,
            rel_tol=0.0,
            abs_tol=0.0,
        ):
            raise ValueError("scoring tolerance does not match replay tolerance")

    complete = not halted and not censors
    if config.require_boundary_reached:
        complete = complete and boundary_reached
    return score_trade_records(
        trades,
        config=config,
        dimensions=dimensions,
        evaluated_sessions=evaluated_sessions,
        replay_censor_count=len(censors),
        profitability_valid=complete,
    )


def _gate_check(
    name: str,
    passed: bool,
    actual: Any,
    comparator: str,
    threshold: Any,
) -> GateCheck:
    return GateCheck(
        name=name,
        passed=bool(passed),
        actual=actual,
        comparator=comparator,
        threshold=threshold,
    )


def _at_least(value: float | int | None, threshold: float | int) -> bool:
    return value is not None and value >= threshold


def _at_most(value: float | int | None, threshold: float | int) -> bool:
    return value is not None and value <= threshold


def _profit_factor_at_least(value: float | str | None, threshold: float) -> bool:
    if value == "Infinity":
        return True
    return value is not None and not isinstance(value, str) and value >= threshold


def evaluate_entry_gate(
    metrics: EntryMetrics,
    *,
    minimum_sample: MinimumSampleThresholds,
    thresholds: EntryPromotionThresholds,
) -> GateResult:
    """Apply every registered entry-edge gate to one window independently."""

    if not isinstance(metrics, EntryMetrics):
        raise ValueError("metrics must be EntryMetrics")
    if not isinstance(minimum_sample, MinimumSampleThresholds):
        raise ValueError("minimum_sample must be MinimumSampleThresholds")
    if not isinstance(thresholds, EntryPromotionThresholds):
        raise ValueError("thresholds must be EntryPromotionThresholds")
    required = minimum_sample.required_count(metrics.evaluated_session_count)
    coverage_10 = metrics.coverage_probability(10)
    coverage_30 = metrics.coverage_probability(30)
    coverage_60 = metrics.coverage_probability(60)
    barrier_distance_matches = math.isclose(
        metrics.profit_barrier_net_per_unit,
        thresholds.equal_barrier_distance_per_unit,
        rel_tol=0.0,
        abs_tol=1e-12,
    ) and math.isclose(
        metrics.loss_barrier_net_per_unit,
        thresholds.equal_barrier_distance_per_unit,
        rel_tol=0.0,
        abs_tol=1e-12,
    )
    checks = (
        _gate_check(
            "minimum_filled_count",
            metrics.filled_count >= required,
            metrics.filled_count,
            ">=",
            required,
        ),
        _gate_check(
            "minimum_active_session_fraction",
            _at_least(
                metrics.active_session_fraction,
                minimum_sample.active_session_fraction_minimum,
            ),
            metrics.active_session_fraction,
            ">=",
            minimum_sample.active_session_fraction_minimum,
        ),
        _gate_check(
            "minimum_fill_rate",
            _at_least(metrics.fill_rate, thresholds.fill_rate_minimum),
            metrics.fill_rate,
            ">=",
            thresholds.fill_rate_minimum,
        ),
        _gate_check(
            "minimum_coverage_10_seconds",
            _at_least(coverage_10, thresholds.coverage_10_seconds_minimum),
            coverage_10,
            ">=",
            thresholds.coverage_10_seconds_minimum,
        ),
        _gate_check(
            "minimum_coverage_30_seconds",
            _at_least(coverage_30, thresholds.coverage_30_seconds_minimum),
            coverage_30,
            ">=",
            thresholds.coverage_30_seconds_minimum,
        ),
        _gate_check(
            "minimum_coverage_60_seconds",
            _at_least(coverage_60, thresholds.coverage_60_seconds_minimum),
            coverage_60,
            ">=",
            thresholds.coverage_60_seconds_minimum,
        ),
        _gate_check(
            "maximum_restricted_median_coverage_milliseconds",
            _at_most(
                metrics.restricted_median_coverage_milliseconds,
                thresholds.restricted_median_coverage_milliseconds_maximum,
            ),
            metrics.restricted_median_coverage_milliseconds,
            "<=",
            thresholds.restricted_median_coverage_milliseconds_maximum,
        ),
        _gate_check(
            "maximum_censored_fraction",
            _at_most(
                metrics.censored_fraction,
                thresholds.censored_fraction_maximum,
            ),
            metrics.censored_fraction,
            "<=",
            thresholds.censored_fraction_maximum,
        ),
        _gate_check(
            "equal_barrier_measurement_contract",
            barrier_distance_matches,
            (
                metrics.profit_barrier_net_per_unit,
                metrics.loss_barrier_net_per_unit,
            ),
            "==",
            thresholds.equal_barrier_distance_per_unit,
        ),
        _gate_check(
            "minimum_equal_barrier_profit_first_rate",
            _at_least(
                metrics.barrier_profit_first_rate,
                thresholds.equal_barrier_profit_first_rate_minimum,
            ),
            metrics.barrier_profit_first_rate,
            ">=",
            thresholds.equal_barrier_profit_first_rate_minimum,
        ),
    )
    return GateResult(passed=all(check.passed for check in checks), checks=checks)


def evaluate_full_strategy_gate(
    reference: TradeMetrics,
    stresses: Mapping[str, TradeMetrics],
    entry_gate: GateResult,
    *,
    minimum_sample: MinimumSampleThresholds,
    thresholds: FullStrategyThresholds,
    required_stress_scenario_ids: Sequence[str],
) -> GateResult:
    """Apply full-strategy and required-stress gates without pooled rescue."""

    if not isinstance(reference, TradeMetrics):
        raise ValueError("reference must be TradeMetrics")
    if not isinstance(entry_gate, GateResult):
        raise ValueError("entry_gate must be GateResult")
    if not isinstance(minimum_sample, MinimumSampleThresholds):
        raise ValueError("minimum_sample must be MinimumSampleThresholds")
    if not isinstance(thresholds, FullStrategyThresholds):
        raise ValueError("thresholds must be FullStrategyThresholds")
    if not isinstance(stresses, Mapping):
        raise ValueError("stresses must be a mapping")
    required_stresses = tuple(required_stress_scenario_ids)
    if not required_stresses or len(required_stresses) != len(set(required_stresses)):
        raise ValueError("required stress scenario ids must be non-empty and unique")
    if any(identifier not in stresses for identifier in required_stresses):
        missing = [
            identifier for identifier in required_stresses if identifier not in stresses
        ]
        raise ValueError(f"missing required stress metrics: {missing}")
    for identifier, metrics in stresses.items():
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("stress identifiers must be non-empty strings")
        if not isinstance(metrics, TradeMetrics):
            raise ValueError(f"stress {identifier!r} must contain TradeMetrics")

    required_count = minimum_sample.required_count(reference.evaluated_session_count)
    checks: list[GateCheck] = [
        _gate_check(
            "entry_promotion_gate",
            entry_gate.passed or not thresholds.entry_promotion_gates_still_required,
            entry_gate.passed,
            "is true"
            if thresholds.entry_promotion_gates_still_required
            else "not required",
            thresholds.entry_promotion_gates_still_required,
        ),
        _gate_check(
            "minimum_trade_count",
            reference.trade_count >= required_count,
            reference.trade_count,
            ">=",
            required_count,
        ),
        _gate_check(
            "minimum_active_session_fraction",
            _at_least(
                reference.active_session_fraction,
                minimum_sample.active_session_fraction_minimum,
            ),
            reference.active_session_fraction,
            ">=",
            minimum_sample.active_session_fraction_minimum,
        ),
        _gate_check(
            "reference_net_pnl_positive",
            reference.net_pnl > 0.0
            if thresholds.reference_net_pnl_strictly_positive
            else reference.net_pnl >= 0.0,
            reference.net_pnl,
            ">" if thresholds.reference_net_pnl_strictly_positive else ">=",
            0.0,
        ),
        _gate_check(
            "reference_expectancy_positive",
            reference.expectancy is not None
            and (
                reference.expectancy > 0.0
                if thresholds.reference_expectancy_strictly_positive
                else reference.expectancy >= 0.0
            ),
            reference.expectancy,
            ">" if thresholds.reference_expectancy_strictly_positive else ">=",
            0.0,
        ),
        _gate_check(
            "minimum_reference_profit_factor",
            _profit_factor_at_least(
                reference.profit_factor,
                thresholds.reference_profit_factor_minimum,
            ),
            reference.profit_factor,
            ">=",
            thresholds.reference_profit_factor_minimum,
        ),
        _gate_check(
            "minimum_positive_session_fraction",
            _at_least(
                reference.positive_session_fraction,
                thresholds.positive_session_fraction_minimum,
            ),
            reference.positive_session_fraction,
            ">=",
            thresholds.positive_session_fraction_minimum,
        ),
        _gate_check(
            "maximum_drawdown_to_gross_profit",
            _at_most(
                reference.maximum_drawdown_to_gross_profit,
                thresholds.maximum_drawdown_to_gross_profit_maximum,
            ),
            reference.maximum_drawdown_to_gross_profit,
            "<=",
            thresholds.maximum_drawdown_to_gross_profit_maximum,
        ),
        _gate_check(
            "maximum_largest_trade_profit_share",
            _at_most(
                reference.largest_trade_share_of_gross_profit,
                thresholds.largest_trade_share_of_gross_profit_maximum,
            ),
            reference.largest_trade_share_of_gross_profit,
            "<=",
            thresholds.largest_trade_share_of_gross_profit_maximum,
        ),
        _gate_check(
            "maximum_largest_session_profit_share",
            _at_most(
                reference.largest_session_share_of_gross_profit,
                thresholds.largest_session_share_of_gross_profit_maximum,
            ),
            reference.largest_session_share_of_gross_profit,
            "<=",
            thresholds.largest_session_share_of_gross_profit_maximum,
        ),
        _gate_check(
            "maximum_loss_95_to_median_absolute_loss",
            _at_most(
                reference.loss_95_to_median_absolute_loss,
                thresholds.loss_95_to_median_absolute_loss_maximum,
            ),
            reference.loss_95_to_median_absolute_loss,
            "<=",
            thresholds.loss_95_to_median_absolute_loss_maximum,
        ),
        _gate_check(
            "maximum_full_replay_censors",
            reference.replay_censor_count
            <= thresholds.full_replay_censor_count_maximum,
            reference.replay_censor_count,
            "<=",
            thresholds.full_replay_censor_count_maximum,
        ),
        _gate_check(
            "profitability_valid",
            reference.profitability_valid
            or not thresholds.profitability_valid_required,
            reference.profitability_valid,
            "is true" if thresholds.profitability_valid_required else "not required",
            thresholds.profitability_valid_required,
        ),
    ]

    for identifier in required_stresses:
        metrics = stresses[identifier]
        stress_required_count = minimum_sample.required_count(
            metrics.evaluated_session_count
        )
        checks.extend(
            (
                _gate_check(
                    f"stress.{identifier}.minimum_trade_count",
                    metrics.trade_count >= stress_required_count,
                    metrics.trade_count,
                    ">=",
                    stress_required_count,
                ),
                _gate_check(
                    f"stress.{identifier}.minimum_active_session_fraction",
                    _at_least(
                        metrics.active_session_fraction,
                        minimum_sample.active_session_fraction_minimum,
                    ),
                    metrics.active_session_fraction,
                    ">=",
                    minimum_sample.active_session_fraction_minimum,
                ),
                _gate_check(
                    f"stress.{identifier}.net_pnl_positive",
                    metrics.net_pnl > 0.0
                    if thresholds.required_stress_net_pnl_strictly_positive
                    else metrics.net_pnl >= 0.0,
                    metrics.net_pnl,
                    ">"
                    if thresholds.required_stress_net_pnl_strictly_positive
                    else ">=",
                    0.0,
                ),
                _gate_check(
                    f"stress.{identifier}.minimum_profit_factor",
                    _profit_factor_at_least(
                        metrics.profit_factor,
                        thresholds.required_stress_profit_factor_minimum,
                    ),
                    metrics.profit_factor,
                    ">=",
                    thresholds.required_stress_profit_factor_minimum,
                ),
                _gate_check(
                    f"stress.{identifier}.profitability_valid",
                    metrics.profitability_valid
                    or not thresholds.profitability_valid_required,
                    metrics.profitability_valid,
                    "is true"
                    if thresholds.profitability_valid_required
                    else "not required",
                    thresholds.profitability_valid_required,
                ),
            )
        )
    frozen_checks = tuple(checks)
    return GateResult(
        passed=all(check.passed for check in frozen_checks), checks=frozen_checks
    )


def compute_balanced_score(
    entry: EntryMetrics,
    reference: TradeMetrics,
    stresses: Mapping[str, TradeMetrics],
    *,
    minimum_sample: MinimumSampleThresholds,
    specification: BalancedScoreSpecification,
    required_stress_scenario_ids: Sequence[str],
) -> BalancedScoreResult:
    """Compute the preregistered score; undefined components stay undefined."""

    if not isinstance(entry, EntryMetrics) or not isinstance(reference, TradeMetrics):
        raise ValueError("entry and reference metrics have incorrect types")
    if not isinstance(minimum_sample, MinimumSampleThresholds):
        raise ValueError("minimum_sample must be MinimumSampleThresholds")
    if not isinstance(specification, BalancedScoreSpecification):
        raise ValueError("specification must be BalancedScoreSpecification")
    required_stresses = tuple(required_stress_scenario_ids)
    if not required_stresses or len(required_stresses) != len(set(required_stresses)):
        raise ValueError("required stress scenario ids must be non-empty and unique")

    components: dict[str, float | None] = {}
    denominator = reference.median_absolute_trade_pnl
    components["expectancyScaledByMedianAbsoluteTradePnl"] = (
        _clip(reference.expectancy / denominator, -1.0, 1.0)
        if reference.expectancy is not None
        and denominator is not None
        and denominator > 0.0
        else None
    )

    probability_terms: list[float] = []
    coverage_valid = True
    for checkpoint, weight in specification.coverage_probability_weights:
        probability = entry.coverage_probability(checkpoint)
        if probability is None:
            coverage_valid = False
            break
        probability_terms.append(weight * (2.0 * probability - 1.0))
    restricted_median = entry.restricted_median_coverage_milliseconds
    if not coverage_valid or restricted_median is None:
        components["coverageProbabilityAndSpeed"] = None
    else:
        probability_component = math.fsum(probability_terms)
        speed_component = 1.0 - 2.0 * _clip(restricted_median / 60_000.0, 0.0, 1.0)
        components["coverageProbabilityAndSpeed"] = (
            specification.coverage_probability_share * probability_component
            + specification.restricted_median_speed_share * speed_component
        )

    if reference.profit_factor is None:
        components["profitFactorCappedAtTwo"] = None
    elif reference.profit_factor == "Infinity":
        components["profitFactorCappedAtTwo"] = 1.0
    else:
        assert not isinstance(reference.profit_factor, str)
        components["profitFactorCappedAtTwo"] = _clip(
            reference.profit_factor - 1.0, -1.0, 1.0
        )

    components["inverseDrawdownToGrossProfit"] = (
        1.0 - 2.0 * _clip(reference.maximum_drawdown / reference.gross_profit, 0.0, 1.0)
        if reference.gross_profit > 0.0
        else None
    )
    components["positiveSessionFraction"] = (
        2.0 * reference.positive_session_fraction - 1.0
        if reference.positive_session_fraction is not None
        else None
    )

    passed_stresses = 0
    stress_inputs_valid = True
    for identifier in required_stresses:
        if identifier not in stresses:
            stress_inputs_valid = False
            continue
        metrics = stresses[identifier]
        if not isinstance(metrics, TradeMetrics):
            raise ValueError(f"stress {identifier!r} must contain TradeMetrics")
        passed_stresses += int(
            metrics.expectancy is not None
            and metrics.expectancy > 0.0
            and _profit_factor_at_least(metrics.profit_factor, 1.0)
        )
    components["requiredStressPassFraction"] = (
        2.0 * (passed_stresses / len(required_stresses)) - 1.0
        if stress_inputs_valid
        else None
    )
    components["inverseLargestSessionProfitConcentration"] = (
        1.0 - 2.0 * _clip(reference.largest_session_share_of_gross_profit, 0.0, 1.0)
        if reference.largest_session_share_of_gross_profit is not None
        else None
    )
    required_count = minimum_sample.required_count(reference.evaluated_session_count)
    components["tradeCountAdequacy"] = (
        2.0 * min(1.0, reference.trade_count / required_count) - 1.0
    )

    ordered_components = tuple(
        (name, components[name]) for name in BALANCED_COMPONENT_NAMES
    )
    invalid = tuple(name for name, value in ordered_components if value is None)
    if invalid:
        score = None
    else:
        weights = dict(specification.component_weights)
        score = _clip(
            math.fsum(
                weights[name] * float(value) for name, value in ordered_components
            ),
            -1.0,
            1.0,
        )
    return BalancedScoreResult(
        score=score,
        components=ordered_components,
        invalid_components=invalid,
    )


def build_candidate_scorecard(
    entry: EntryScoreReport,
    reference: TradeScoreReport,
    stresses: Mapping[str, TradeScoreReport],
    *,
    config: RegisteredScoringConfig,
) -> CandidateScorecard:
    """Assemble one auditable reference/stress scorecard for a frozen candidate."""

    if not isinstance(entry, EntryScoreReport):
        raise ValueError("entry must be EntryScoreReport")
    if not isinstance(reference, TradeScoreReport):
        raise ValueError("reference must be TradeScoreReport")
    if not isinstance(config, RegisteredScoringConfig):
        raise ValueError("config must be RegisteredScoringConfig")
    if any(not isinstance(report, TradeScoreReport) for report in stresses.values()):
        raise ValueError("every stress value must be TradeScoreReport")
    stress_metrics = {
        identifier: report.overall for identifier, report in stresses.items()
    }
    entry_gate = evaluate_entry_gate(
        entry.overall,
        minimum_sample=config.minimum_sample,
        thresholds=config.entry_gate,
    )
    full_gate = evaluate_full_strategy_gate(
        reference.overall,
        stress_metrics,
        entry_gate,
        minimum_sample=config.minimum_sample,
        thresholds=config.full_gate,
        required_stress_scenario_ids=config.required_stress_scenario_ids,
    )
    balanced = compute_balanced_score(
        entry.overall,
        reference.overall,
        stress_metrics,
        minimum_sample=config.minimum_sample,
        specification=config.balanced_score,
        required_stress_scenario_ids=config.required_stress_scenario_ids,
    )
    return CandidateScorecard(
        entry=entry,
        reference=reference,
        stresses=tuple(sorted(stresses.items())),
        entry_gate=entry_gate,
        full_gate=full_gate,
        balanced_score=balanced,
    )


def scoring_config_from_preregistration(
    preregistration: Mapping[str, Any],
    *,
    verify_current_implementation_files: bool = True,
) -> RegisteredScoringConfig:
    """Parse scoring assumptions only after the canonical v2 document validates."""

    # Local import avoids making preregistration construction depend on the
    # scoring module.  Validation also verifies the document's source bindings
    # and canonical hash before any threshold is trusted.
    from datavis.research.fresh_preregistration import (  # noqa: PLC0415
        validate_fresh_preregistration,
    )

    validate_fresh_preregistration(
        preregistration,
        verify_current_implementation_files=verify_current_implementation_files,
    )
    entry_spec = _as_mapping(preregistration["entryDiagnostics"], "entryDiagnostics")
    execution = _as_mapping(preregistration["execution"], "execution")
    robustness = _as_mapping(
        preregistration["robustnessAndGates"], "robustnessAndGates"
    )
    minimum = _as_mapping(robustness["minimumSample"], "minimumSample")
    entry_gate = _as_mapping(robustness["entryPromotionGates"], "entryPromotionGates")
    full_gate = _as_mapping(robustness["fullStrategyGates"], "fullStrategyGates")
    balanced = _as_mapping(robustness["balancedScore"], "balancedScore")
    coverage_subscore = _as_mapping(balanced["coverageSubscore"], "coverageSubscore")

    scenario_id = str(entry_gate["scenario"])
    scenarios = _as_sequence(execution["scenarios"], "execution scenarios")
    matching = [
        scenario for scenario in scenarios if _get(scenario, "id") == scenario_id
    ]
    if len(matching) != 1:
        raise ValueError("entry gate execution scenario must resolve exactly once")
    reference_scenario = matching[0]

    component_weights_map = _as_mapping(balanced["weights"], "balanced score weights")
    probability_weights_map = _as_mapping(
        coverage_subscore["probabilityWeights"], "coverage probability weights"
    )
    barrier_distance = float(entry_gate["equalBarrierDistancePerUnit"])
    return RegisteredScoringConfig(
        entry_metrics=EntryMetricConfig(
            coverage_checkpoints_seconds=tuple(
                int(value) for value in entry_spec["coverageCheckpointsSeconds"]
            ),
            restricted_uncovered_milliseconds=int(
                reference_scenario["diagnosticHorizonMs"]
            ),
            profit_barrier_net_per_unit=barrier_distance,
            loss_barrier_net_per_unit=barrier_distance,
        ),
        trade_metrics=TradeMetricConfig(
            pnl_classification_tolerance=float(
                reference_scenario["pnlClassificationTolerance"]
            ),
            loss_tail_quantile_probability=0.95,
            require_boundary_reached=True,
        ),
        minimum_sample=MinimumSampleThresholds(
            filled_trades_per_session=int(minimum["filledTradesPerSession"]),
            absolute_filled_trades=int(minimum["absoluteFilledTrades"]),
            active_session_fraction_minimum=float(minimum["activeSessionFraction"]),
        ),
        entry_gate=EntryPromotionThresholds(
            fill_rate_minimum=float(entry_gate["fillRateMinimum"]),
            coverage_10_seconds_minimum=float(entry_gate["costCoverage10Seconds"]),
            coverage_30_seconds_minimum=float(entry_gate["costCoverage30Seconds"]),
            coverage_60_seconds_minimum=float(entry_gate["costCoverage60Seconds"]),
            restricted_median_coverage_milliseconds_maximum=float(
                entry_gate["restrictedMedianCoverageMillisecondsMaximum"]
            ),
            censored_fraction_maximum=float(entry_gate["censoredFractionMaximum"]),
            equal_barrier_distance_per_unit=barrier_distance,
            equal_barrier_profit_first_rate_minimum=float(
                entry_gate["equalBarrierProfitFirstRateMinimum"]
            ),
        ),
        full_gate=FullStrategyThresholds(
            reference_profit_factor_minimum=float(
                full_gate["referenceProfitFactorMinimum"]
            ),
            positive_session_fraction_minimum=float(
                full_gate["positiveSessionFractionMinimum"]
            ),
            maximum_drawdown_to_gross_profit_maximum=float(
                full_gate["maximumDrawdownToGrossProfitMaximum"]
            ),
            largest_trade_share_of_gross_profit_maximum=float(
                full_gate["largestTradeShareOfGrossProfitMaximum"]
            ),
            largest_session_share_of_gross_profit_maximum=float(
                full_gate["largestSessionShareOfGrossProfitMaximum"]
            ),
            loss_95_to_median_absolute_loss_maximum=float(
                full_gate["loss95ToMedianAbsoluteLossMaximum"]
            ),
            required_stress_profit_factor_minimum=float(
                full_gate["requiredStressProfitFactorMinimum"]
            ),
            full_replay_censor_count_maximum=int(
                full_gate["fullReplayCensorCountMaximum"]
            ),
            reference_net_pnl_strictly_positive=bool(
                full_gate["referenceNetPnlStrictlyPositive"]
            ),
            reference_expectancy_strictly_positive=bool(
                full_gate["referenceExpectancyStrictlyPositive"]
            ),
            required_stress_net_pnl_strictly_positive=bool(
                full_gate["requiredStressNetPnlStrictlyPositive"]
            ),
            profitability_valid_required=bool(full_gate["profitabilityValidRequired"]),
            entry_promotion_gates_still_required=bool(
                full_gate["entryPromotionGatesStillRequired"]
            ),
        ),
        balanced_score=BalancedScoreSpecification(
            component_weights=tuple(
                (name, float(component_weights_map[name]))
                for name in BALANCED_COMPONENT_NAMES
            ),
            coverage_probability_weights=tuple(
                (checkpoint, float(probability_weights_map[f"{checkpoint}s"]))
                for checkpoint in (2, 5, 10, 30, 60)
            ),
            coverage_probability_share=float(coverage_subscore["probabilityShare"]),
            restricted_median_speed_share=float(
                coverage_subscore["restrictedMedianSpeedShare"]
            ),
        ),
        required_stress_scenario_ids=tuple(
            str(value) for value in execution["requiredStressScenarioIds"]
        ),
    )


def evaluate_chronological_gates(
    items: Sequence[ChronologicalGateItem],
    *,
    required_windows: Sequence[str],
) -> ChronologicalGateResult:
    """Require named windows independently after validating chronological order."""

    materialized = tuple(items)
    required = tuple(required_windows)
    if not materialized:
        raise ValueError("chronological gate items cannot be empty")
    if not required or len(required) != len(set(required)):
        raise ValueError("required_windows must be non-empty and unique")
    names: list[str] = []
    previous: ChronologicalGateItem | None = None
    for item in materialized:
        if not isinstance(item, ChronologicalGateItem):
            raise ValueError("items must contain ChronologicalGateItem values")
        if item.window_name in names:
            raise ValueError(f"duplicate window name {item.window_name!r}")
        if previous is not None:
            if item.start < previous.start:
                raise ValueError("windows must be supplied in chronological order")
            if item.start < previous.end:
                raise ValueError("chronological windows cannot overlap")
        names.append(item.window_name)
        previous = item
    if any(name not in names for name in required):
        missing = [name for name in required if name not in names]
        raise ValueError(f"required windows were not evaluated: {missing}")
    positions = [names.index(name) for name in required]
    if positions != sorted(positions):
        raise ValueError("required_windows must follow chronological item order")
    by_name = {item.window_name: item for item in materialized}
    failed = tuple(name for name in required if not by_name[name].gate.passed)
    return ChronologicalGateResult(
        passed=not failed,
        required_windows=required,
        failed_windows=failed,
        evaluated_windows=tuple(names),
    )


def rank_candidates(
    records: Iterable[CandidateRankRecord],
) -> tuple[CandidateRankRecord, ...]:
    """Rank deterministically using score then the preregistered tie-break order."""

    materialized = tuple(records)
    for record in materialized:
        if not isinstance(record, CandidateRankRecord):
            raise ValueError("records must contain CandidateRankRecord values")
    identifiers = [record.candidate_id for record in materialized]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("candidate ids must be unique")

    def key(record: CandidateRankRecord) -> tuple[Any, ...]:
        score_valid = record.balanced_score is not None
        score = record.balanced_score if score_valid else -1.0
        return (
            -int(record.hard_gate_passed),
            -int(score_valid),
            -score,
            -min(record.per_window_expectancies),
            -min(record.required_stress_expectancies),
            record.maximum_drawdown,
            record.rule_complexity,
            record.candidate_id,
        )

    return tuple(sorted(materialized, key=key))


__all__ = [
    "BALANCED_COMPONENT_NAMES",
    "REQUIRED_COVERAGE_CHECKPOINTS_SECONDS",
    "BalancedScoreResult",
    "BalancedScoreSpecification",
    "CandidateRankRecord",
    "CandidateScorecard",
    "ChronologicalGateItem",
    "ChronologicalGateResult",
    "EntryMetricConfig",
    "EntryMetrics",
    "EntryPromotionThresholds",
    "EntryScoreReport",
    "FullStrategyThresholds",
    "GateCheck",
    "GateResult",
    "MinimumSampleThresholds",
    "RegisteredScoringConfig",
    "SliceDimensions",
    "TradeMetricConfig",
    "TradeMetrics",
    "TradeScoreReport",
    "build_candidate_scorecard",
    "compute_balanced_score",
    "evaluate_chronological_gates",
    "evaluate_entry_gate",
    "evaluate_full_strategy_gate",
    "rank_candidates",
    "score_entry_diagnostics",
    "score_replay_result",
    "score_trade_records",
    "scoring_config_from_preregistration",
]
