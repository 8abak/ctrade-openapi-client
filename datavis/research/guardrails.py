from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping

from datavis.research.models import EntryResearchParameterPatch, EntryResearchParameters, SupervisorDecision


APPROVED_FEATURES = (
    "spread",
    "short_momentum",
    "short_acceleration",
    "recent_tick_imbalance",
    "burst_persistence",
    "micro_breakout",
    "breakout_failure",
    "pullback_depth",
    "distance_recent_high",
    "distance_recent_low",
    "flip_frequency",
)

DEFAULT_FEATURES = (
    "spread",
    "short_momentum",
    "short_acceleration",
    "recent_tick_imbalance",
    "burst_persistence",
    "micro_breakout",
    "pullback_depth",
    "flip_frequency",
)

APPROVED_LABEL_VARIANTS: Dict[str, Dict[str, float | int]] = {
    "entry_2x_t060_s300_a15": {"horizon_ticks": 60, "horizon_seconds": 300, "target_multiplier": 2.0, "adverse_multiplier": 1.5},
    "entry_2x_t120_s600_a15": {"horizon_ticks": 120, "horizon_seconds": 600, "target_multiplier": 2.0, "adverse_multiplier": 1.5},
    "entry_2x_t180_s900_a15": {"horizon_ticks": 180, "horizon_seconds": 900, "target_multiplier": 2.0, "adverse_multiplier": 1.5},
    "entry_25x_t120_s600_a20": {"horizon_ticks": 120, "horizon_seconds": 600, "target_multiplier": 2.5, "adverse_multiplier": 2.0},
}

APPROVED_CANDIDATE_FAMILIES = ("threshold_grid", "pair_combo")
APPROVED_THRESHOLD_PROFILES: Dict[str, List[float]] = {
    "balanced": [0.65, 0.75, 0.85],
    "strict": [0.75, 0.85, 0.92],
    "wide": [0.55, 0.65, 0.75],
}
APPROVED_TRAIN_VALIDATION_PLANS = ("chronological_70_30", "chronological_60_40", "chronological_80_20")
APPROVED_DEDUP_RULES = {
    "none": 0,
    "gap_5": 5,
    "gap_10": 10,
    "gap_20": 20,
}
APPROVED_SPREAD_FILTERS = {
    "any": None,
    "tight_q50": ("qmax", 0.50),
    "tight_q65": ("qmax", 0.65),
    "mid_q25_q75": ("qband", 0.25, 0.75),
    "avoid_wide_q80": ("qmax", 0.80),
}
APPROVED_SESSION_BUCKETS = (
    "bucket_00_04",
    "bucket_04_08",
    "bucket_08_12",
    "bucket_12_16",
    "bucket_16_20",
    "bucket_20_24",
)
APPROVED_STOP_REASONS = {
    "strong_narrow_regime_found",
    "good_precision_but_too_low_frequency",
    "moderate_edge_not_near_target",
    "unstable_out_of_sample",
    "no_robust_edge_found",
    "stopped_by_budget_guardrail",
    "supervisor_instruction_rejected",
}


@dataclass(frozen=True)
class SearchGuardrails:
    max_slice_rows: int
    max_warmup_rows: int
    max_feature_count: int = 8


def label_variant_payload(name: str) -> Dict[str, float | int]:
    if name not in APPROVED_LABEL_VARIANTS:
        raise ValueError(f"unsupported label variant: {name}")
    return dict(APPROVED_LABEL_VARIANTS[name])


def default_parameters(*, symbol: str, slice_rows: int, warmup_rows: int, iteration: int = 1) -> EntryResearchParameters:
    return EntryResearchParameters(
        symbol=symbol,
        iteration=iteration,
        slice_rows=slice_rows,
        warmup_rows=warmup_rows,
        label_variant="entry_2x_t120_s600_a15",
        candidate_family="threshold_grid",
        threshold_profile="balanced",
        feature_toggles=list(DEFAULT_FEATURES),
        session_filter=[],
        spread_filter="avoid_wide_q80",
        dedup_rule="gap_10",
        train_validation_plan="chronological_70_30",
    )


def _validate_feature_list(values: Iterable[str], *, max_count: int) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        if value not in APPROVED_FEATURES:
            raise ValueError(f"unsupported feature toggle: {value}")
        if value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    if not normalized:
        normalized = list(DEFAULT_FEATURES)
    if len(normalized) > max_count:
        raise ValueError(f"too many feature toggles: {len(normalized)} > {max_count}")
    return normalized


def sanitize_parameters(payload: Mapping[str, Any], *, limits: SearchGuardrails) -> EntryResearchParameters:
    params = EntryResearchParameters.model_validate(dict(payload))
    if params.slice_rows > limits.max_slice_rows:
        raise ValueError(f"slice_rows exceeds limit: {params.slice_rows} > {limits.max_slice_rows}")
    if params.warmup_rows > limits.max_warmup_rows:
        raise ValueError(f"warmup_rows exceeds limit: {params.warmup_rows} > {limits.max_warmup_rows}")
    if params.label_variant not in APPROVED_LABEL_VARIANTS:
        raise ValueError(f"unsupported label variant: {params.label_variant}")
    if params.candidate_family not in APPROVED_CANDIDATE_FAMILIES:
        raise ValueError(f"unsupported candidate family: {params.candidate_family}")
    if params.threshold_profile not in APPROVED_THRESHOLD_PROFILES:
        raise ValueError(f"unsupported threshold profile: {params.threshold_profile}")
    if params.train_validation_plan not in APPROVED_TRAIN_VALIDATION_PLANS:
        raise ValueError(f"unsupported train/validation plan: {params.train_validation_plan}")
    if params.dedup_rule not in APPROVED_DEDUP_RULES:
        raise ValueError(f"unsupported dedup rule: {params.dedup_rule}")
    if params.spread_filter not in APPROVED_SPREAD_FILTERS:
        raise ValueError(f"unsupported spread filter: {params.spread_filter}")
    if any(bucket not in APPROVED_SESSION_BUCKETS for bucket in params.session_filter):
        raise ValueError(f"unsupported session bucket in {params.session_filter}")
    normalized_features = _validate_feature_list(params.feature_toggles, max_count=limits.max_feature_count)
    return params.model_copy(update={"feature_toggles": normalized_features})


def merge_parameters(
    base: EntryResearchParameters,
    patch: EntryResearchParameterPatch | None,
    *,
    limits: SearchGuardrails,
) -> EntryResearchParameters:
    merged = dict(base.model_dump())
    if patch is not None:
        for key, value in patch.model_dump(exclude_none=True).items():
            merged[key] = value
    return sanitize_parameters(merged, limits=limits)


def validate_supervisor_decision(
    decision_payload: Mapping[str, Any],
    *,
    base_parameters: EntryResearchParameters,
    limits: SearchGuardrails,
) -> tuple[SupervisorDecision, EntryResearchParameters | None]:
    decision = SupervisorDecision.model_validate(dict(decision_payload))
    if decision.decision == "stop":
        if decision.stop_reason and decision.stop_reason not in APPROVED_STOP_REASONS:
            raise ValueError(f"unsupported stop_reason: {decision.stop_reason}")
        return decision, None
    if decision.next_action is None:
        raise ValueError("next_action is required for continue/refine decisions")
    if decision.next_action.type != "run_entry_research":
        raise ValueError(f"unsupported next_action.type: {decision.next_action.type}")
    next_params = merge_parameters(base_parameters, decision.next_action.parameters, limits=limits)
    next_params = next_params.model_copy(update={"iteration": base_parameters.iteration + 1})
    return decision, next_params

