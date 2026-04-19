from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from datavis.research.models import (
    CandidateSeed,
    ContrastHint,
    EntryResearchParameterPatch,
    EntryResearchParameters,
    SupervisorDecision,
)


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

APPROVED_CANDIDATE_FAMILIES = (
    "threshold_grid",
    "pair_combo",
    "triad_combo",
    "contrast_gate",
    "crossover_confirmation",
    "regime_split",
    "tighten_winner",
    "slice_expand",
    "side_locked_refine",
)

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
APPROVED_SIDE_LOCKS = ("both", "long", "short")
APPROVED_STOP_REASONS = {
    "strong_narrow_regime_found",
    "good_precision_but_too_low_frequency",
    "moderate_edge_not_near_target",
    "unstable_out_of_sample",
    "no_robust_edge_found",
    "stopped_by_budget_guardrail",
    "explicit_stop_requested",
    "hard_technical_failure",
    "supervisor_instruction_rejected",
}


@dataclass(frozen=True)
class SearchGuardrails:
    max_slice_rows: int
    max_warmup_rows: int
    max_slice_offset_rows: int
    max_feature_count: int = 8
    max_contrast_hints: int = 4
    max_next_actions: int = 8


def label_variant_payload(name: str) -> Dict[str, float | int]:
    if name not in APPROVED_LABEL_VARIANTS:
        raise ValueError(f"unsupported label variant: {name}")
    return dict(APPROVED_LABEL_VARIANTS[name])


def default_parameters(*, symbol: str, slice_rows: int, warmup_rows: int, iteration: int = 1) -> EntryResearchParameters:
    return EntryResearchParameters(
        symbol=symbol,
        iteration=iteration,
        slice_rows=slice_rows,
        slice_offset_rows=0,
        warmup_rows=warmup_rows,
        label_variant="entry_2x_t120_s600_a15",
        candidate_family="threshold_grid",
        threshold_profile="balanced",
        feature_toggles=list(DEFAULT_FEATURES),
        session_filter=[],
        spread_filter="avoid_wide_q80",
        dedup_rule="gap_10",
        train_validation_plan="chronological_70_30",
        side_lock="both",
        contrast_hints=[],
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
        normalized = normalized[:max_count]
    return normalized


def _validate_predicates(seed_rule: CandidateSeed | None) -> CandidateSeed | None:
    if seed_rule is None:
        return None
    for predicate in seed_rule.predicates:
        if predicate.feature not in APPROVED_FEATURES:
            raise ValueError(f"unsupported predicate feature: {predicate.feature}")
    return seed_rule


def _validate_contrast_hints(hints: Sequence[ContrastHint], *, max_count: int) -> List[ContrastHint]:
    normalized: List[ContrastHint] = []
    seen = set()
    for hint in hints or []:
        if hint.feature not in APPROVED_FEATURES:
            raise ValueError(f"unsupported contrast hint feature: {hint.feature}")
        key = (hint.feature, hint.operator, round(float(hint.threshold), 6))
        if key in seen:
            continue
        normalized.append(hint)
        seen.add(key)
    if len(normalized) > max_count:
        raise ValueError(f"too many contrast hints: {len(normalized)} > {max_count}")
    return normalized


def sanitize_parameters(payload: Mapping[str, Any], *, limits: SearchGuardrails) -> EntryResearchParameters:
    bounded_payload = dict(payload)
    if isinstance(bounded_payload.get("feature_toggles"), list):
        bounded_payload["feature_toggles"] = list(dict.fromkeys(str(item).strip() for item in bounded_payload["feature_toggles"] if str(item or "").strip()))[: limits.max_feature_count]
    params = EntryResearchParameters.model_validate(bounded_payload)
    if params.slice_rows > limits.max_slice_rows:
        raise ValueError(f"slice_rows exceeds limit: {params.slice_rows} > {limits.max_slice_rows}")
    if params.slice_offset_rows > limits.max_slice_offset_rows:
        raise ValueError(f"slice_offset_rows exceeds limit: {params.slice_offset_rows} > {limits.max_slice_offset_rows}")
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
    if params.side_lock not in APPROVED_SIDE_LOCKS:
        raise ValueError(f"unsupported side lock: {params.side_lock}")
    if any(bucket not in APPROVED_SESSION_BUCKETS for bucket in params.session_filter):
        raise ValueError(f"unsupported session bucket in {params.session_filter}")
    normalized_features = _validate_feature_list(params.feature_toggles, max_count=limits.max_feature_count)
    normalized_seed = _validate_predicates(params.seed_rule)
    normalized_hints = _validate_contrast_hints(params.contrast_hints, max_count=limits.max_contrast_hints)
    normalized = params.model_copy(
        update={
            "feature_toggles": normalized_features,
            "seed_rule": normalized_seed,
            "contrast_hints": normalized_hints,
        }
    )
    fingerprint = normalized.config_fingerprint or build_config_fingerprint(normalized)
    return normalized.model_copy(update={"config_fingerprint": fingerprint})


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


def coerce_supervisor_decision_payload(decision_payload: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(decision_payload or {})
    legacy_next = payload.pop("next_action", None)
    if not payload.get("decision"):
        payload["decision"] = "refine" if legacy_next or payload.get("next_actions") else "stop" if payload.get("stop_reason") else "continue"
    payload["reason"] = str(payload.get("reason") or "Supervisor response omitted a reason; control plane backfilled a bounded default.")
    payload["confidence_note"] = str(payload.get("confidence_note") or payload["reason"])
    payload["verdict"] = str(payload.get("verdict") or payload.get("decision") or "continue")
    next_actions = payload.get("next_actions")
    if next_actions is None and legacy_next is not None:
        action_name = str(payload.get("decision") or "refine")
        if action_name == "stop":
            action_name = "refine"
        next_actions = [
            {
                "action": action_name,
                "reason": str(payload["reason"]),
                "parameters": (legacy_next or {}).get("parameters"),
            }
        ]
    payload["next_actions"] = list(next_actions or [])
    if payload["decision"] == "stop":
        payload["next_actions"] = []
    return payload


def validate_supervisor_decision(decision_payload: Mapping[str, Any]) -> SupervisorDecision:
    payload = coerce_supervisor_decision_payload(decision_payload)
    decision = SupervisorDecision.model_validate(payload)
    if decision.stop_reason and decision.stop_reason not in APPROVED_STOP_REASONS:
        raise ValueError(f"unsupported stop_reason: {decision.stop_reason}")
    return decision


def build_config_fingerprint(payload: EntryResearchParameters | Mapping[str, Any]) -> str:
    if isinstance(payload, EntryResearchParameters):
        data = payload.model_dump(exclude_none=True)
    else:
        data = dict(payload)
    normalized = {
        "symbol": str(data.get("symbol") or "").upper(),
        "slice_rows": int(data.get("slice_rows") or 0),
        "slice_offset_rows": int(data.get("slice_offset_rows") or 0),
        "warmup_rows": int(data.get("warmup_rows") or 0),
        "label_variant": data.get("label_variant"),
        "candidate_family": data.get("candidate_family"),
        "threshold_profile": data.get("threshold_profile"),
        "feature_toggles": sorted(str(value) for value in (data.get("feature_toggles") or [])),
        "session_filter": sorted(str(value) for value in (data.get("session_filter") or [])),
        "spread_filter": data.get("spread_filter"),
        "dedup_rule": data.get("dedup_rule"),
        "train_validation_plan": data.get("train_validation_plan"),
        "side_lock": data.get("side_lock") or "both",
        "seed_rule": data.get("seed_rule") or None,
        "contrast_hints": data.get("contrast_hints") or [],
    }
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:16]
