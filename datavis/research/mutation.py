from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence

from datavis.research.config import ResearchSettings
from datavis.research.guardrails import (
    APPROVED_CANDIDATE_FAMILIES,
    SearchGuardrails,
    build_config_fingerprint,
    merge_parameters,
    sanitize_parameters,
)
from datavis.research.models import CandidateSeed, ContrastHint, EntryResearchParameterPatch, EntryResearchParameters


def next_slice_size(current: int, ladder: Sequence[int]) -> int | None:
    for value in ladder:
        if int(value) > int(current):
            return int(value)
    return None


def direction_key_from_config(config: Mapping[str, Any]) -> str:
    session_filter = ",".join(sorted(str(item) for item in (config.get("session_filter") or [])))
    seed_rule = config.get("seed_rule") or {}
    seed_family = seed_rule.get("family") or ""
    seed_side = seed_rule.get("side") or ""
    return "|".join(
        [
            str(config.get("candidate_family") or ""),
            str(config.get("label_variant") or ""),
            str(config.get("side_lock") or "both"),
            str(config.get("spread_filter") or ""),
            session_filter,
            seed_family,
            seed_side,
        ]
    )


def run_is_strong(metrics: Mapping[str, Any]) -> bool:
    return (
        float(metrics.get("cleanPrecision") or 0.0) >= 0.70
        and float(metrics.get("entriesPerDay") or 0.0) >= 0.75
        and float(metrics.get("walkForwardRange") or 0.0) <= 0.15
        and int(metrics.get("signalCount") or 0) >= 10
    )


def run_is_failed(row: Mapping[str, Any]) -> bool:
    verdict_hint = str(row.get("verdict_hint") or row.get("verdictHint") or "")
    metrics = row.get("metrics") or row.get("validationMetrics") or {}
    clean_precision = float(metrics.get("cleanPrecision") or 0.0)
    walk_forward_range = float(metrics.get("walkForwardRange") or 0.0)
    if verdict_hint in {"strong_narrow_regime_found", "good_precision_but_too_low_frequency"}:
        return False
    return clean_precision < 0.40 or walk_forward_range > 0.30


def summarize_history(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    completed_runs = 0
    distinct_failed_directions = set()
    distinct_directions = set()
    strong_runs = 0
    seen_fingerprints = set()
    for row in rows:
        status = str(row.get("status") or "")
        if status and status != "completed":
            continue
        completed_runs += 1
        config = dict(row.get("config") or {})
        if config.get("config_fingerprint"):
            seen_fingerprints.add(str(config["config_fingerprint"]))
        direction_key = direction_key_from_config(config)
        if direction_key:
            distinct_directions.add(direction_key)
        if run_is_failed(row):
            distinct_failed_directions.add(direction_key)
        metrics = row.get("metrics") or row.get("validationMetrics") or {}
        if run_is_strong(metrics):
            strong_runs += 1
    return {
        "completedRuns": completed_runs,
        "strongRuns": strong_runs,
        "distinctDirectionsTried": len(distinct_directions),
        "distinctFailedDirections": len({item for item in distinct_failed_directions if item}),
        "seenFingerprints": sorted(seen_fingerprints),
    }


def evaluate_stop_guardrails(
    *,
    decision_stop_reason: str | None,
    control: Mapping[str, Any],
    latest_metrics: Mapping[str, Any],
    history_summary: Mapping[str, Any],
    settings: ResearchSettings,
    policy: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    completed_runs = int(history_summary.get("completedRuns") or 0)
    distinct_failed = int(history_summary.get("distinctFailedDirections") or 0)
    min_runs_before_stop = int((policy or {}).get("minRunsBeforeStop") or settings.min_runs_before_stop)
    failed_direction_stop_count = int((policy or {}).get("failedDirectionStopCount") or settings.failed_direction_stop_count)
    if control.get("stop_requested"):
        return {
            "allowStop": True,
            "acceptedReason": "explicit_stop_requested",
            "policyNote": "explicit stop requested in control state",
        }
    if run_is_strong(latest_metrics):
        if completed_runs >= min_runs_before_stop:
            return {
                "allowStop": True,
                "acceptedReason": decision_stop_reason or "strong_narrow_regime_found",
                "policyNote": "strong stable regime found after minimum run count",
            }
        return {
            "allowStop": False,
            "acceptedReason": None,
            "policyNote": f"minimum completed runs not reached ({completed_runs} < {min_runs_before_stop})",
        }
    if distinct_failed >= failed_direction_stop_count and completed_runs >= min_runs_before_stop:
        return {
            "allowStop": True,
            "acceptedReason": decision_stop_reason or "no_robust_edge_found",
            "policyNote": "multiple distinct search directions failed",
        }
    return {
        "allowStop": False,
        "acceptedReason": None,
        "policyNote": "stop guardrail rejected early stop because evidence remains inconclusive",
    }


def generate_mutation_proposals(
    *,
    base_params: EntryResearchParameters,
    summary_payload: Mapping[str, Any],
    settings: ResearchSettings,
    source_run_id: int | None,
    seen_fingerprints: Iterable[str] = (),
    pending_fingerprints: Iterable[str] = (),
    policy: Mapping[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    max_next_jobs = int((policy or {}).get("maxNextJobs") or settings.max_next_jobs)
    approved_slice_ladder = tuple((policy or {}).get("approvedSliceLadder") or settings.slice_ladder)
    preferred_side_lock = str((policy or {}).get("preferredSideLock") or base_params.side_lock or "both")
    allowed_candidate_families = set((policy or {}).get("allowedCandidateFamilies") or APPROVED_CANDIDATE_FAMILIES)
    limits = SearchGuardrails(
        max_slice_rows=settings.max_slice_rows,
        max_warmup_rows=settings.max_warmup_rows,
        max_slice_offset_rows=settings.max_slice_offset_rows,
        max_next_actions=max_next_jobs,
    )
    best_candidate = dict(summary_payload.get("bestCandidate") or {})
    best_rule = dict(best_candidate.get("rule") or {})
    validation_metrics = dict(best_candidate.get("validationMetrics") or {})
    contrast_summary = dict(best_candidate.get("contrastSummary") or {})
    seen = {str(item) for item in seen_fingerprints if item}
    seen.update(str(item) for item in pending_fingerprints if item)
    proposals: List[Dict[str, Any]] = []
    local_fingerprints = set()

    def add_proposal(action: str, reason: str, patch_payload: Mapping[str, Any]) -> None:
        if len(proposals) >= max_next_jobs:
            return
        family = str((patch_payload or {}).get("candidate_family") or base_params.candidate_family or "")
        if family and family not in allowed_candidate_families:
            return
        patch = EntryResearchParameterPatch.model_validate(dict(patch_payload))
        next_params = merge_parameters(base_params, patch, limits=limits)
        if preferred_side_lock in {"long", "short"} and next_params.side_lock == "both":
            next_params = next_params.model_copy(update={"side_lock": preferred_side_lock})
        update_payload = {
            "iteration": base_params.iteration + 1,
            "source_run_id": source_run_id,
            "mutation_note": reason[:512],
        }
        next_params = sanitize_parameters(next_params.model_copy(update=update_payload).model_dump(), limits=limits)
        fingerprint = build_config_fingerprint(next_params)
        if fingerprint in seen or fingerprint in local_fingerprints:
            return
        local_fingerprints.add(fingerprint)
        base_dump = base_params.model_dump(exclude_none=True)
        next_dump = next_params.model_dump(exclude_none=True)
        mutated_fields = sorted(key for key, value in next_dump.items() if base_dump.get(key) != value and key != "iteration")
        proposals.append(
            {
                "action": action,
                "reason": reason,
                "configFingerprint": fingerprint,
                "mutatedFields": mutated_fields,
                "parameters": next_dump,
            }
        )

    strong_contrasts = list((contrast_summary.get("topFeatures") or [])[:3])
    hints = [
        ContrastHint(
            feature=str(item.get("feature") or ""),
            operator=str(item.get("preferredOperator") or ">="),  # type: ignore[arg-type]
            threshold=float(item.get("suggestedThreshold") or 0.0),
            score=float(item.get("score") or 0.0),
            reason=str(item.get("reason") or ""),
        )
        for item in strong_contrasts
        if item.get("feature")
    ]
    seed_rule = None
    if best_rule.get("name") and best_rule.get("side") in {"long", "short"}:
        seed_rule = CandidateSeed.model_validate(best_rule)

    if str(base_params.candidate_family or "") == "divergence_sweep":
        divergence = dict(best_rule.get("divergence") or {})
        if best_rule.get("side") in {"long", "short"} and base_params.side_lock == "both":
            add_proposal(
                "refine",
                f"Re-run divergence_sweep with {best_rule['side']}-only focus because the best selected-day edge is side-specific.",
                {
                    "candidate_family": "divergence_sweep",
                    "side_lock": best_rule["side"],
                },
            )
        session_bucket = best_session_bucket(contrast_summary)
        if session_bucket and session_bucket not in set(base_params.session_filter):
            add_proposal(
                "split_by_pattern",
                f"Re-run divergence_sweep inside the strongest session bucket {session_bucket}.",
                {
                    "candidate_family": "divergence_sweep",
                    "session_filter": [session_bucket],
                    "side_lock": best_rule.get("side") or base_params.side_lock,
                },
            )
        spread_filter = better_spread_filter(contrast_summary, base_params.spread_filter)
        if spread_filter and spread_filter != base_params.spread_filter:
            add_proposal(
                "split_by_pattern",
                f"Re-run divergence_sweep with tighter spread focus ({spread_filter}) because the selected-day edge clusters in tighter spreads.",
                {
                    "candidate_family": "divergence_sweep",
                    "spread_filter": spread_filter,
                    "side_lock": best_rule.get("side") or base_params.side_lock,
                },
            )
        if divergence.get("style") == "reversal" and best_rule.get("side") in {"long", "short"} and not proposals:
            add_proposal(
                "continue",
                "Repeat divergence_sweep without mutation to confirm whether the selected-day reversal edge is stable.",
                {
                    "candidate_family": "divergence_sweep",
                    "side_lock": best_rule.get("side") or base_params.side_lock,
                },
            )
        return proposals[: max_next_jobs]

    if seed_rule is not None and hints:
        add_proposal(
            "refine",
            f"Tighten the current winner with the strongest contrast gate on {hints[0].feature}.",
            {
                "candidate_family": "tighten_winner",
                "side_lock": seed_rule.side,
                "seed_rule": seed_rule,
                "contrast_hints": [hints[0]],
                "threshold_profile": "strict",
                "feature_toggles": sorted(set(base_params.feature_toggles + [hints[0].feature])),
            },
        )
    if seed_rule is not None and len(hints) >= 2:
        add_proposal(
            "refine",
            f"Tighten the winner with two contrast gates on {hints[0].feature} and {hints[1].feature}.",
            {
                "candidate_family": "tighten_winner",
                "side_lock": seed_rule.side,
                "seed_rule": seed_rule,
                "contrast_hints": hints[:2],
                "threshold_profile": "strict",
                "feature_toggles": sorted(set(base_params.feature_toggles + [hints[0].feature, hints[1].feature])),
            },
        )
    if hints:
        add_proposal(
            "explore_new_family",
            f"Explore a contrast-driven family using the strongest positive-vs-false-positive deltas ({', '.join(h.feature for h in hints[:2])}).",
            {
                "candidate_family": "contrast_gate",
                "side_lock": best_rule.get("side") or base_params.side_lock,
                "contrast_hints": hints[: min(3, len(hints))],
                "threshold_profile": "strict",
                "feature_toggles": sorted(set(base_params.feature_toggles + [hint.feature for hint in hints[:3]])),
            },
        )
    if seed_rule is not None:
        add_proposal(
            "explore_new_family",
            "Test bounded crossover confirmations on the current winner and keep only variants that cut noise or improve precision.",
            {
                "candidate_family": "crossover_confirmation",
                "side_lock": seed_rule.side,
                "seed_rule": seed_rule,
                "spread_filter": base_params.spread_filter,
                "threshold_profile": "strict",
            },
        )

    current_family = str(base_params.candidate_family or "")
    if current_family in {"threshold_grid", "slice_expand"}:
        add_proposal(
            "explore_new_family",
            "Escalate from single-threshold search into bounded pair conjunctions.",
            {
                "candidate_family": "pair_combo",
                "side_lock": best_rule.get("side") or base_params.side_lock,
                "threshold_profile": "balanced",
            },
        )
    if current_family in {"pair_combo", "contrast_gate", "regime_split"}:
        add_proposal(
            "explore_new_family",
            "Try a tightly bounded triad conjunction family to test whether one extra gate sharpens precision.",
            {
                "candidate_family": "triad_combo",
                "side_lock": best_rule.get("side") or base_params.side_lock,
                "threshold_profile": "strict",
            },
        )

    session_bucket = best_session_bucket(contrast_summary)
    if session_bucket and session_bucket not in set(base_params.session_filter):
        add_proposal(
            "split_by_pattern",
            f"Split the regime into the strongest session bucket {session_bucket}.",
            {
                "candidate_family": "regime_split",
                "session_filter": [session_bucket],
                "side_lock": best_rule.get("side") or base_params.side_lock,
                "seed_rule": seed_rule,
                "contrast_hints": hints[:1],
            },
        )

    spread_filter = better_spread_filter(contrast_summary, base_params.spread_filter)
    if spread_filter and spread_filter != base_params.spread_filter:
        add_proposal(
            "split_by_pattern",
            f"Split the regime with a tighter spread filter ({spread_filter}) because positives cluster in tighter spread buckets.",
            {
                "candidate_family": "regime_split",
                "spread_filter": spread_filter,
                "side_lock": best_rule.get("side") or base_params.side_lock,
                "seed_rule": seed_rule,
                "contrast_hints": hints[:1],
            },
        )

    if (best_rule.get("side") in {"long", "short"}) and base_params.side_lock == "both":
        add_proposal(
            "refine",
            f"Lock the search to {best_rule['side']}-only because the best current evidence is side-dominant.",
            {
                "candidate_family": "side_locked_refine",
                "side_lock": best_rule["side"],
                "seed_rule": seed_rule,
                "contrast_hints": hints[:1],
            },
        )

    next_slice = next_slice_size(base_params.slice_rows, approved_slice_ladder)
    low_signal = int(validation_metrics.get("signalCount") or 0) < 12
    moderate_precision = float(validation_metrics.get("cleanPrecision") or 0.0) >= 0.40
    if next_slice is not None and (low_signal or moderate_precision):
        add_proposal(
            "increase_slice",
            f"Increase the slice from {base_params.slice_rows} to {next_slice} to check whether the current regime survives a larger sample.",
            {
                "candidate_family": "slice_expand",
                "slice_rows": next_slice,
                "seed_rule": seed_rule,
                "contrast_hints": hints[:1],
                "side_lock": best_rule.get("side") or base_params.side_lock,
            },
        )

    unstable = float(validation_metrics.get("walkForwardRange") or 0.0) > 0.25
    next_offset = int(base_params.slice_offset_rows) + int(base_params.slice_rows)
    if unstable and next_offset <= settings.max_slice_offset_rows:
        add_proposal(
            "continue",
            f"Shift to the next contiguous slice offset ({next_offset}) because the current slice looks unstable out of sample.",
            {
                "candidate_family": "slice_expand",
                "slice_offset_rows": next_offset,
                "seed_rule": seed_rule,
                "contrast_hints": hints[:1],
            },
        )

    if not proposals:
        fallback_slice = next_slice or base_params.slice_rows
        fallback_family = "pair_combo" if base_params.candidate_family == "threshold_grid" else "threshold_grid"
        add_proposal(
            "continue",
            "Fallback bounded mutation after an inconclusive run.",
            {
                "candidate_family": fallback_family,
                "slice_rows": fallback_slice,
                "threshold_profile": "strict" if fallback_family != "threshold_grid" else "balanced",
            },
        )

    return proposals[: max_next_jobs]


def best_session_bucket(contrast_summary: Mapping[str, Any]) -> str | None:
    buckets = contrast_summary.get("sessionBuckets") or {}
    best_bucket = None
    best_score = None
    for bucket, payload in buckets.items():
        precision = float((payload or {}).get("cleanPrecision") or 0.0)
        signals = int((payload or {}).get("signals") or 0)
        if signals < 2:
            continue
        score = (precision, signals)
        if best_score is None or score > best_score:
            best_bucket = str(bucket)
            best_score = score
    return best_bucket


def better_spread_filter(contrast_summary: Mapping[str, Any], current_filter: str) -> str | None:
    buckets = contrast_summary.get("spreadBuckets") or {}
    low_bucket = buckets.get("low") or {}
    mid_bucket = buckets.get("mid") or {}
    low_precision = float(low_bucket.get("cleanPrecision") or 0.0)
    mid_precision = float(mid_bucket.get("cleanPrecision") or 0.0)
    if low_precision >= max(0.45, mid_precision + 0.08):
        return "tight_q50" if current_filter != "tight_q50" else "tight_q65"
    return None
