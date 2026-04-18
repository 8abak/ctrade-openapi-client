from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

from datavis.research.config import ResearchSettings
from datavis.research.guardrails import (
    APPROVED_DEDUP_RULES,
    APPROVED_FEATURES,
    APPROVED_SPREAD_FILTERS,
    APPROVED_THRESHOLD_PROFILES,
    label_variant_payload,
)
from datavis.research.models import CandidateSeed, ContrastHint, EntryResearchParameters, PredicateSpec
from datavis.research.mutation import generate_mutation_proposals


BROKER_TZ = ZoneInfo("Australia/Sydney")


@dataclass(frozen=True)
class TickRow:
    id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float
    spread: float


@dataclass(frozen=True)
class CandidateRule:
    name: str
    family: str
    side: str
    predicates: List[Dict[str, Any]]


def execute_entry_research(
    conn: Any,
    *,
    params: EntryResearchParameters,
    settings: ResearchSettings,
) -> Dict[str, Any]:
    variant = label_variant_payload(params.label_variant)
    slice_bounds = resolve_slice_bounds(
        conn,
        symbol=params.symbol,
        slice_rows=params.slice_rows,
        slice_offset_rows=params.slice_offset_rows,
        lookahead_ticks=int(variant["horizon_ticks"]),
    )
    context_rows = fetch_context_rows(
        conn,
        symbol=params.symbol,
        start_tick_id=slice_bounds["start_tick_id"],
        end_tick_id=slice_bounds["end_tick_id"],
        warmup_rows=params.warmup_rows,
        lookahead_rows=int(variant["horizon_ticks"]) + 4,
        limit=max(1, params.slice_rows + params.warmup_rows + int(variant["horizon_ticks"]) + 8),
    )
    cases = build_cases(
        context_rows,
        start_tick_id=slice_bounds["start_tick_id"],
        end_tick_id=slice_bounds["end_tick_id"],
        variant=variant,
    )
    if not cases:
        raise RuntimeError("No eligible research cases were produced from the bounded slice.")

    train_cases, validation_cases = split_cases(cases, params.train_validation_plan)
    candidate_results = search_candidates(
        train_cases=train_cases,
        validation_cases=validation_cases,
        params=params,
        settings=settings,
    )
    if not candidate_results:
        raise RuntimeError("Candidate search produced no bounded results.")

    best_candidate = candidate_results[0]
    summary_payload = build_summary_payload(
        params=params,
        slice_bounds=slice_bounds,
        cases=cases,
        train_cases=train_cases,
        validation_cases=validation_cases,
        candidate_results=candidate_results,
        best_candidate=best_candidate,
        settings=settings,
    )
    label_rows, feature_rows, candidate_rows = build_storage_rows(run_id=0, cases=cases, candidate_results=candidate_results)
    return {
        "sliceBounds": slice_bounds,
        "cases": cases,
        "labelRows": label_rows,
        "featureRows": feature_rows,
        "candidateRows": candidate_rows,
        "summaryPayload": summary_payload,
    }


def resolve_slice_bounds(
    conn: Any,
    *,
    symbol: str,
    slice_rows: int,
    slice_offset_rows: int,
    lookahead_ticks: int,
) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(id) AS max_id
            FROM public.ticks
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()
    latest_id = int((row or [0])[0] or 0)
    if latest_id <= 0:
        raise RuntimeError(f"No ticks found for symbol {symbol}.")
    end_tick_id = max(1, latest_id - max(lookahead_ticks, 4) - max(0, int(slice_offset_rows)))
    start_tick_id = max(1, end_tick_id - max(1, slice_rows) + 1)
    return {
        "latest_tick_id": latest_id,
        "start_tick_id": start_tick_id,
        "end_tick_id": end_tick_id,
        "slice_rows": max(0, end_tick_id - start_tick_id + 1),
        "slice_offset_rows": max(0, int(slice_offset_rows)),
    }


def fetch_context_rows(
    conn: Any,
    *,
    symbol: str,
    start_tick_id: int,
    end_tick_id: int,
    warmup_rows: int,
    lookahead_rows: int,
    limit: int,
) -> List[TickRow]:
    context_start = max(1, int(start_tick_id) - max(0, int(warmup_rows)))
    context_end = max(int(end_tick_id), int(end_tick_id) + max(0, int(lookahead_rows)))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND id >= %s
              AND id <= %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, context_start, context_end, limit),
        )
        rows = cur.fetchall()
    return [
        TickRow(
            id=int(row[0]),
            timestamp=row[1],
            bid=float(row[2]),
            ask=float(row[3]),
            mid=float(row[4] if row[4] is not None else (float(row[2]) + float(row[3])) / 2.0),
            spread=float(row[5] if row[5] is not None else max(0.0, float(row[3]) - float(row[2]))),
        )
        for row in rows
    ]


def build_cases(
    rows: Sequence[TickRow],
    *,
    start_tick_id: int,
    end_tick_id: int,
    variant: Dict[str, Any],
) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []
    rows_by_id = {row.id: index for index, row in enumerate(rows)}
    start_index = rows_by_id.get(start_tick_id)
    end_index = rows_by_id.get(end_tick_id)
    if start_index is None or end_index is None or end_index <= start_index:
        return cases
    minimum_history = 12
    for index in range(start_index, end_index + 1):
        if index < minimum_history:
            continue
        tick = rows[index]
        features = compute_features(rows, index)
        long_label = compute_entry_label(rows, index, side="long", variant=variant)
        short_label = compute_entry_label(rows, index, side="short", variant=variant)
        if long_label is None or short_label is None:
            continue
        cases.append(
            {
                "tickId": tick.id,
                "timestamp": tick.timestamp,
                "sessionBucket": session_bucket_for_timestamp(tick.timestamp),
                "spread": tick.spread,
                "features": features,
                "labels": {"long": long_label, "short": short_label},
            }
        )
    return cases


def compute_features(rows: Sequence[TickRow], index: int) -> Dict[str, float | str]:
    tick = rows[index]
    prev_3 = rows[index - 3]
    prev_4 = rows[index - 4]
    prev_5 = rows[index - 5]
    prev_10 = rows[index - 10]
    prev_12_window = rows[index - 12:index]
    mids_10 = [row.mid for row in prev_12_window]
    recent_high = max(mids_10)
    recent_low = min(mids_10)
    momentum_3 = tick.mid - prev_3.mid
    previous_momentum_3 = rows[index - 1].mid - prev_4.mid
    deltas = [rows[pos].mid - rows[pos - 1].mid for pos in range(index - 7, index + 1)]
    direction_flags = [sign(delta) for delta in deltas]
    longest_streak = longest_same_sign_streak(direction_flags)
    direction_changes = sum(1 for pos in range(1, len(direction_flags)) if direction_flags[pos] and direction_flags[pos] != direction_flags[pos - 1])
    prev_prev_high = max(row.mid for row in rows[index - 11:index - 1])
    prev_prev_low = min(row.mid for row in rows[index - 11:index - 1])
    prev_mid = rows[index - 1].mid
    breakout_failure = 0.0
    if prev_mid > prev_prev_high and tick.mid <= recent_high:
        breakout_failure = -(prev_mid - prev_prev_high)
    elif prev_mid < prev_prev_low and tick.mid >= recent_low:
        breakout_failure = prev_prev_low - prev_mid
    breakout = 0.0
    if tick.mid > recent_high:
        breakout = tick.mid - recent_high
    elif tick.mid < recent_low:
        breakout = -(recent_low - tick.mid)
    pullback_depth = (recent_high - tick.mid) if momentum_3 >= 0 else (tick.mid - recent_low)
    imbalance = sum(direction_flags) / max(1, len(direction_flags))
    return {
        "spread": round(tick.spread, 6),
        "short_momentum": round(tick.mid - prev_5.mid, 6),
        "short_acceleration": round(momentum_3 - previous_momentum_3, 6),
        "recent_tick_imbalance": round(imbalance, 6),
        "burst_persistence": round(longest_streak / max(1, len(direction_flags)), 6),
        "micro_breakout": round(breakout, 6),
        "breakout_failure": round(breakout_failure, 6),
        "pullback_depth": round(pullback_depth, 6),
        "distance_recent_high": round(recent_high - tick.mid, 6),
        "distance_recent_low": round(tick.mid - recent_low, 6),
        "flip_frequency": round(direction_changes / max(1, len(direction_flags) - 1), 6),
        "session_bucket": session_bucket_for_timestamp(tick.timestamp),
        "momentum_anchor": round(tick.mid - prev_10.mid, 6),
    }


def compute_entry_label(rows: Sequence[TickRow], index: int, *, side: str, variant: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tick = rows[index]
    spread = max(0.000001, tick.spread)
    horizon_ticks = int(variant["horizon_ticks"])
    horizon_seconds = int(variant["horizon_seconds"])
    target_multiplier = float(variant["target_multiplier"])
    adverse_multiplier = float(variant["adverse_multiplier"])
    if side == "long":
        entry_price = tick.ask
        target_price = entry_price + (target_multiplier * spread)
        adverse_price = entry_price - (adverse_multiplier * spread)
    else:
        entry_price = tick.bid
        target_price = entry_price - (target_multiplier * spread)
        adverse_price = entry_price + (adverse_multiplier * spread)
    target_hit_offset: Optional[int] = None
    target_hit_seconds: Optional[float] = None
    adverse_hit_offset: Optional[int] = None
    max_favorable = 0.0
    max_adverse = 0.0
    observed = 0
    last_elapsed_seconds = 0.0
    for offset, future in enumerate(rows[index + 1:], start=1):
        elapsed_seconds = max(0.0, (future.timestamp - tick.timestamp).total_seconds())
        if offset > horizon_ticks or elapsed_seconds > horizon_seconds:
            break
        observed += 1
        last_elapsed_seconds = elapsed_seconds
        executable = future.bid if side == "long" else future.ask
        if side == "long":
            max_favorable = max(max_favorable, executable - entry_price)
            max_adverse = max(max_adverse, entry_price - executable)
            if target_hit_offset is None and executable >= target_price:
                target_hit_offset = offset
                target_hit_seconds = elapsed_seconds
            if adverse_hit_offset is None and executable <= adverse_price:
                adverse_hit_offset = offset
        else:
            max_favorable = max(max_favorable, entry_price - executable)
            max_adverse = max(max_adverse, executable - entry_price)
            if target_hit_offset is None and executable <= target_price:
                target_hit_offset = offset
                target_hit_seconds = elapsed_seconds
            if adverse_hit_offset is None and executable >= adverse_price:
                adverse_hit_offset = offset
    if observed < horizon_ticks and last_elapsed_seconds < horizon_seconds:
        return None
    return {
        "side": side,
        "entryPrice": round(entry_price, 6),
        "spreadAtEntry": round(spread, 6),
        "targetPrice": round(target_price, 6),
        "targetMultiplier": target_multiplier,
        "adversePrice": round(adverse_price, 6),
        "adverseMultiplier": adverse_multiplier,
        "horizonTicks": horizon_ticks,
        "horizonSeconds": horizon_seconds,
        "hit2x": target_hit_offset is not None,
        "hitTicks": target_hit_offset,
        "hitSeconds": round(target_hit_seconds, 6) if target_hit_seconds is not None else None,
        "maxFavorable": round(max_favorable, 6),
        "maxAdverse": round(max_adverse, 6),
        "adverseHit": adverse_hit_offset is not None,
        "targetBeforeAdverse": bool(
            target_hit_offset is not None and (adverse_hit_offset is None or target_hit_offset <= adverse_hit_offset)
        ),
    }


def split_cases(cases: Sequence[Dict[str, Any]], plan: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ratios = {
        "chronological_70_30": 0.70,
        "chronological_60_40": 0.60,
        "chronological_80_20": 0.80,
    }
    ratio = ratios.get(plan, 0.70)
    split_at = max(1, min(len(cases) - 1, int(len(cases) * ratio)))
    return list(cases[:split_at]), list(cases[split_at:])


def search_candidates(
    *,
    train_cases: Sequence[Dict[str, Any]],
    validation_cases: Sequence[Dict[str, Any]],
    params: EntryResearchParameters,
    settings: ResearchSettings,
) -> List[Dict[str, Any]]:
    candidate_limit = max(1, settings.max_candidates)
    dedup_gap = APPROVED_DEDUP_RULES[params.dedup_rule]
    filtered_train = prefilter_cases(train_cases, params.spread_filter, params.session_filter)
    filtered_validation = prefilter_cases(validation_cases, params.spread_filter, params.session_filter)
    if not filtered_train or not filtered_validation:
        return []
    threshold_values = APPROVED_THRESHOLD_PROFILES[params.threshold_profile]
    allowed_sides = resolve_sides(params.side_lock)
    single_rules = build_threshold_rules(filtered_train, params.feature_toggles, threshold_values, allowed_sides=allowed_sides)
    single_results = evaluate_rules(single_rules, filtered_train, filtered_validation, dedup_gap=dedup_gap, example_limit=settings.max_examples)
    if not single_results:
        return []
    candidate_results: List[Dict[str, Any]] = []
    strategy = params.candidate_family
    if strategy == "threshold_grid":
        candidate_results.extend(single_results[:candidate_limit])
    elif strategy in {"pair_combo", "regime_split", "slice_expand", "side_locked_refine"}:
        candidate_results.extend(single_results[:candidate_limit])
        pair_rules = build_pair_rules(single_results)
        pair_results = evaluate_rules(pair_rules, filtered_train, filtered_validation, dedup_gap=dedup_gap, example_limit=settings.max_examples)
        candidate_results.extend(pair_results[:candidate_limit])
    elif strategy == "triad_combo":
        pair_rules = build_pair_rules(single_results)
        pair_results = evaluate_rules(pair_rules, filtered_train, filtered_validation, dedup_gap=dedup_gap, example_limit=settings.max_examples)
        triad_rules = build_triad_rules(single_results)
        triad_results = evaluate_rules(triad_rules, filtered_train, filtered_validation, dedup_gap=dedup_gap, example_limit=settings.max_examples)
        candidate_results.extend(single_results[:candidate_limit])
        candidate_results.extend(pair_results[:candidate_limit])
        candidate_results.extend(triad_results[:candidate_limit])
    elif strategy == "contrast_gate":
        contrast_rules = build_contrast_rules(params, single_results)
        contrast_results = evaluate_rules(contrast_rules, filtered_train, filtered_validation, dedup_gap=dedup_gap, example_limit=settings.max_examples)
        candidate_results.extend(contrast_results[:candidate_limit])
    elif strategy == "tighten_winner":
        tighten_rules = build_tighten_rules(params, single_results)
        tighten_results = evaluate_rules(tighten_rules, filtered_train, filtered_validation, dedup_gap=dedup_gap, example_limit=settings.max_examples)
        candidate_results.extend(tighten_results[:candidate_limit])
    deduped: List[Dict[str, Any]] = []
    seen_names = set()
    for result in sorted(candidate_results, key=candidate_sort_key, reverse=True):
        name = result["candidateName"]
        if name in seen_names:
            continue
        deduped.append(result)
        seen_names.add(name)
        if len(deduped) >= candidate_limit:
            break
    return deduped


def resolve_sides(side_lock: str) -> List[str]:
    if side_lock == "long":
        return ["long"]
    if side_lock == "short":
        return ["short"]
    return ["long", "short"]


def build_threshold_rules(
    train_cases: Sequence[Dict[str, Any]],
    feature_names: Sequence[str],
    threshold_values: Sequence[float],
    *,
    allowed_sides: Sequence[str],
) -> List[CandidateRule]:
    rules: List[CandidateRule] = []
    for side in allowed_sides:
        for feature_name in feature_names:
            values = [float(case["features"][feature_name]) for case in train_cases if feature_name in case["features"]]
            for threshold in quantile_points(values, threshold_values):
                for operator in (">=", "<="):
                    rules.append(
                        CandidateRule(
                            name=f"{side}:{feature_name}:{operator}:{round(threshold, 6)}",
                            family="threshold_grid",
                            side=side,
                            predicates=[{"feature": feature_name, "operator": operator, "threshold": threshold}],
                        )
                    )
    return rules


def build_pair_rules(single_results: Sequence[Dict[str, Any]]) -> List[CandidateRule]:
    pair_rules: List[CandidateRule] = []
    seed_rules = single_results[: min(8, len(single_results))]
    seen = set()
    for left, right in combinations(seed_rules, 2):
        left_rule = dict(left["rule"])
        right_rule = dict(right["rule"])
        if left_rule["side"] != right_rule["side"]:
            continue
        features = tuple(sorted(predicate["feature"] for predicate in left_rule["predicates"] + right_rule["predicates"]))
        if len(set(features)) != 2 or features in seen:
            continue
        seen.add(features)
        pair_rules.append(
            CandidateRule(
                name=f"{left_rule['side']}:pair:{features[0]}:{features[1]}",
                family="pair_combo",
                side=left_rule["side"],
                predicates=list(left_rule["predicates"]) + list(right_rule["predicates"]),
            )
        )
    return pair_rules[:12]


def build_triad_rules(single_results: Sequence[Dict[str, Any]]) -> List[CandidateRule]:
    triad_rules: List[CandidateRule] = []
    seed_rules = single_results[: min(6, len(single_results))]
    seen = set()
    for combo in combinations(seed_rules, 3):
        rules = [dict(item["rule"]) for item in combo]
        side = rules[0]["side"]
        if any(rule["side"] != side for rule in rules):
            continue
        features = tuple(sorted(predicate["feature"] for rule in rules for predicate in rule["predicates"]))
        if len(set(features)) != 3 or features in seen:
            continue
        seen.add(features)
        triad_rules.append(
            CandidateRule(
                name=f"{side}:triad:{':'.join(features)}",
                family="triad_combo",
                side=side,
                predicates=[predicate for rule in rules for predicate in rule["predicates"]],
            )
        )
    return triad_rules[:10]


def build_contrast_rules(params: EntryResearchParameters, single_results: Sequence[Dict[str, Any]]) -> List[CandidateRule]:
    hints = list(params.contrast_hints) or derive_hints_from_results(single_results)
    if not hints:
        return []
    target_side = resolve_seed_side(params, single_results)
    rules: List[CandidateRule] = []
    for hint in hints[:3]:
        rules.append(
            CandidateRule(
                name=f"{target_side}:contrast:{hint.feature}:{hint.operator}:{round(hint.threshold, 6)}",
                family="contrast_gate",
                side=target_side,
                predicates=[hint_to_predicate(hint)],
            )
        )
    if len(hints) >= 2:
        rules.append(
            CandidateRule(
                name=f"{target_side}:contrast_pair:{hints[0].feature}:{hints[1].feature}",
                family="contrast_gate",
                side=target_side,
                predicates=[hint_to_predicate(hints[0]), hint_to_predicate(hints[1])],
            )
        )
    return rules[:8]


def build_tighten_rules(params: EntryResearchParameters, single_results: Sequence[Dict[str, Any]]) -> List[CandidateRule]:
    seed = params.seed_rule or derive_seed_from_results(single_results)
    if seed is None:
        return []
    hints = [hint for hint in (list(params.contrast_hints) or derive_hints_from_results(single_results)) if hint.feature not in {p.feature for p in seed.predicates}]
    base_predicates = [predicate.model_dump() if isinstance(predicate, PredicateSpec) else dict(predicate) for predicate in seed.predicates]
    rules = [
        CandidateRule(
            name=f"{seed.side}:tighten:baseline:{seed.name}",
            family="tighten_winner",
            side=seed.side,
            predicates=base_predicates,
        )
    ]
    if hints:
        rules.append(
            CandidateRule(
                name=f"{seed.side}:tighten:{seed.name}:{hints[0].feature}",
                family="tighten_winner",
                side=seed.side,
                predicates=base_predicates + [hint_to_predicate(hints[0])],
            )
        )
    if len(hints) >= 2:
        rules.append(
            CandidateRule(
                name=f"{seed.side}:tighten:{seed.name}:{hints[0].feature}:{hints[1].feature}",
                family="tighten_winner",
                side=seed.side,
                predicates=base_predicates + [hint_to_predicate(hints[0]), hint_to_predicate(hints[1])],
            )
        )
    return rules[:6]


def derive_seed_from_results(single_results: Sequence[Dict[str, Any]]) -> CandidateSeed | None:
    if not single_results:
        return None
    return CandidateSeed.model_validate(single_results[0]["rule"])


def derive_hints_from_results(single_results: Sequence[Dict[str, Any]]) -> List[ContrastHint]:
    if not single_results:
        return []
    top_features = (single_results[0].get("contrastSummary") or {}).get("topFeatures") or []
    hints = []
    for item in top_features[:3]:
        if not item.get("feature"):
            continue
        hints.append(
            ContrastHint(
                feature=str(item["feature"]),
                operator=str(item.get("preferredOperator") or ">="),  # type: ignore[arg-type]
                threshold=float(item.get("suggestedThreshold") or 0.0),
                score=float(item.get("score") or 0.0),
                reason=str(item.get("reason") or ""),
            )
        )
    return hints


def resolve_seed_side(params: EntryResearchParameters, single_results: Sequence[Dict[str, Any]]) -> str:
    if params.seed_rule is not None:
        return params.seed_rule.side
    if params.side_lock in {"long", "short"}:
        return params.side_lock
    if single_results:
        return str(single_results[0]["rule"]["side"])
    return "long"


def hint_to_predicate(hint: ContrastHint) -> Dict[str, Any]:
    return {"feature": hint.feature, "operator": hint.operator, "threshold": hint.threshold}


def evaluate_rules(
    rules: Sequence[CandidateRule],
    train_cases: Sequence[Dict[str, Any]],
    validation_cases: Sequence[Dict[str, Any]],
    *,
    dedup_gap: int,
    example_limit: int,
) -> List[Dict[str, Any]]:
    results = [evaluate_candidate(rule, train_cases, validation_cases, dedup_gap=dedup_gap, example_limit=example_limit) for rule in rules]
    filtered = [result for result in results if result["validationMetrics"]["signalCount"] > 0]
    filtered.sort(key=candidate_sort_key, reverse=True)
    return filtered


def prefilter_cases(
    cases: Sequence[Dict[str, Any]],
    spread_filter_name: str,
    session_filter: Sequence[str],
) -> List[Dict[str, Any]]:
    filtered = list(cases)
    if session_filter:
        allowed = set(session_filter)
        filtered = [case for case in filtered if case["sessionBucket"] in allowed]
    spread_values = [float(case["spread"]) for case in filtered]
    if not spread_values:
        return []
    filter_rule = APPROVED_SPREAD_FILTERS.get(spread_filter_name)
    if filter_rule is None:
        return filtered
    if filter_rule[0] == "qmax":
        threshold = quantile_value(spread_values, float(filter_rule[1]))
        return [case for case in filtered if float(case["spread"]) <= threshold]
    min_threshold = quantile_value(spread_values, float(filter_rule[1]))
    max_threshold = quantile_value(spread_values, float(filter_rule[2]))
    return [case for case in filtered if min_threshold <= float(case["spread"]) <= max_threshold]


def evaluate_candidate(
    rule: CandidateRule,
    train_cases: Sequence[Dict[str, Any]],
    validation_cases: Sequence[Dict[str, Any]],
    *,
    dedup_gap: int,
    example_limit: int,
) -> Dict[str, Any]:
    predicate = compile_rule(rule)
    train_selected = apply_rule(train_cases, rule.side, predicate, dedup_gap=dedup_gap)
    validation_selected = apply_rule(validation_cases, rule.side, predicate, dedup_gap=dedup_gap)
    train_metrics = compute_metrics(train_selected, side=rule.side)
    validation_metrics = compute_metrics(validation_selected, side=rule.side)
    validation_metrics["walkForward"] = walk_forward_summary(validation_selected, side=rule.side)
    validation_metrics["walkForwardRange"] = validation_metrics["walkForward"]["precisionRange"]
    validation_metrics["bySession"] = summarize_by_bucket(validation_selected, side=rule.side, key="sessionBucket")
    validation_metrics["bySpread"] = summarize_spread_buckets(validation_selected, side=rule.side)
    positives, negatives = split_selected_cases(validation_selected, side=rule.side)
    contrast_summary = build_contrast_summary(validation_selected, positives, negatives, side=rule.side)
    positive_examples = render_examples(positives, side=rule.side, limit=example_limit)
    negative_examples = render_examples(negatives, side=rule.side, limit=example_limit)
    return {
        "candidateName": rule.name,
        "family": rule.family,
        "rule": {"name": rule.name, "family": rule.family, "side": rule.side, "predicates": rule.predicates},
        "trainMetrics": train_metrics,
        "validationMetrics": validation_metrics,
        "positives": positive_examples,
        "falsePositives": negative_examples,
        "patternSummary": {
            "positivePattern": summarize_feature_pattern(positives),
            "falsePositivePattern": summarize_feature_pattern(negatives),
        },
        "contrastSummary": contrast_summary,
    }


def compile_rule(rule: CandidateRule) -> Callable[[Dict[str, float | str]], bool]:
    predicates = list(rule.predicates)

    def predicate(features: Dict[str, float | str]) -> bool:
        for item in predicates:
            value = float(features[item["feature"]])
            threshold = float(item["threshold"])
            operator = item["operator"]
            if operator == ">=" and value < threshold:
                return False
            if operator == "<=" and value > threshold:
                return False
        return True

    return predicate


def apply_rule(
    cases: Sequence[Dict[str, Any]],
    side: str,
    predicate: Callable[[Dict[str, float | str]], bool],
    *,
    dedup_gap: int,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    last_selected_tick_id: Optional[int] = None
    for case in cases:
        if not predicate(case["features"]):
            continue
        tick_id = int(case["tickId"])
        if dedup_gap > 0 and last_selected_tick_id is not None and (tick_id - last_selected_tick_id) <= dedup_gap:
            continue
        selected.append(case)
        last_selected_tick_id = tick_id
    return selected


def compute_metrics(selected_cases: Sequence[Dict[str, Any]], *, side: str) -> Dict[str, Any]:
    if not selected_cases:
        return {
            "signalCount": 0,
            "hitCount": 0,
            "cleanHitCount": 0,
            "precision": 0.0,
            "cleanPrecision": 0.0,
            "entriesPerDay": 0.0,
            "medianHitSeconds": None,
            "avgMaxFavorable": 0.0,
            "avgMaxAdverse": 0.0,
        }
    labels = [case["labels"][side] for case in selected_cases]
    hits = [label for label in labels if label["hit2x"]]
    clean_hits = [label for label in labels if label["targetBeforeAdverse"]]
    hit_seconds = [float(label["hitSeconds"]) for label in hits if label["hitSeconds"] is not None]
    timestamps = [case["timestamp"] for case in selected_cases]
    total_days = max(1.0, (timestamps[-1] - timestamps[0]).total_seconds() / 86400.0)
    return {
        "signalCount": len(selected_cases),
        "hitCount": len(hits),
        "cleanHitCount": len(clean_hits),
        "precision": round(len(hits) / len(selected_cases), 6),
        "cleanPrecision": round(len(clean_hits) / len(selected_cases), 6),
        "entriesPerDay": round(len(selected_cases) / total_days, 4),
        "medianHitSeconds": round(statistics.median(hit_seconds), 6) if hit_seconds else None,
        "avgMaxFavorable": round(sum(float(label["maxFavorable"]) for label in labels) / len(labels), 6),
        "avgMaxAdverse": round(sum(float(label["maxAdverse"]) for label in labels) / len(labels), 6),
    }


def walk_forward_summary(selected_cases: Sequence[Dict[str, Any]], *, side: str) -> Dict[str, Any]:
    if not selected_cases:
        return {"blocks": [], "precisionRange": 0.0}
    block_size = max(1, math.ceil(len(selected_cases) / 3))
    blocks = []
    precisions: List[float] = []
    for start in range(0, len(selected_cases), block_size):
        block = selected_cases[start:start + block_size]
        metrics = compute_metrics(block, side=side)
        precisions.append(float(metrics["cleanPrecision"]))
        blocks.append(
            {
                "startTickId": int(block[0]["tickId"]),
                "endTickId": int(block[-1]["tickId"]),
                "signalCount": metrics["signalCount"],
                "cleanPrecision": metrics["cleanPrecision"],
            }
        )
    precision_range = round(max(precisions) - min(precisions), 6) if precisions else 0.0
    return {"blocks": blocks, "precisionRange": precision_range}


def summarize_by_bucket(selected_cases: Sequence[Dict[str, Any]], *, side: str, key: str) -> Dict[str, Any]:
    payload: Dict[str, Dict[str, int | float]] = {}
    for case in selected_cases:
        bucket = str(case.get(key) or "unknown")
        label = case["labels"][side]
        entry = payload.setdefault(bucket, {"signals": 0, "hits": 0, "cleanHits": 0})
        entry["signals"] = int(entry["signals"]) + 1
        entry["hits"] = int(entry["hits"]) + int(bool(label["hit2x"]))
        entry["cleanHits"] = int(entry["cleanHits"]) + int(bool(label["targetBeforeAdverse"]))
    for bucket, values in payload.items():
        signals = max(1, int(values["signals"]))
        values["cleanPrecision"] = round(int(values["cleanHits"]) / signals, 6)
    return payload


def summarize_spread_buckets(selected_cases: Sequence[Dict[str, Any]], *, side: str) -> Dict[str, Any]:
    if not selected_cases:
        return {}
    spreads = [float(case["spread"]) for case in selected_cases]
    low_cut = quantile_value(spreads, 0.33)
    high_cut = quantile_value(spreads, 0.66)
    payload = {"low": [], "mid": [], "high": []}
    for case in selected_cases:
        spread = float(case["spread"])
        bucket = "low" if spread <= low_cut else "high" if spread >= high_cut else "mid"
        payload[bucket].append(case)
    return {bucket: compute_metrics(rows, side=side) for bucket, rows in payload.items() if rows}


def split_selected_cases(selected_cases: Sequence[Dict[str, Any]], *, side: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    positives = []
    negatives = []
    for case in selected_cases:
        label = case["labels"][side]
        if label["targetBeforeAdverse"]:
            positives.append(case)
        else:
            negatives.append(case)
    return positives, negatives


def render_examples(selected_cases: Sequence[Dict[str, Any]], *, side: str, limit: int) -> List[Dict[str, Any]]:
    payload = []
    for case in selected_cases:
        label = case["labels"][side]
        payload.append(
            {
                "tickId": int(case["tickId"]),
                "timestamp": case["timestamp"].isoformat(),
                "sessionBucket": case["sessionBucket"],
                "spread": round(float(case["spread"]), 6),
                "hit2x": bool(label["hit2x"]),
                "targetBeforeAdverse": bool(label["targetBeforeAdverse"]),
                "hitSeconds": label["hitSeconds"],
                "maxFavorable": label["maxFavorable"],
                "maxAdverse": label["maxAdverse"],
                "features": compact_feature_payload(case["features"]),
            }
        )
    payload.sort(key=lambda item: (float(item["maxFavorable"]), -float(item["maxAdverse"])), reverse=True)
    return payload[: max(1, limit)]


def summarize_feature_pattern(cases: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not cases:
        return {}
    summary: Dict[str, float] = {}
    for feature_name in APPROVED_FEATURES:
        values = [float(case["features"][feature_name]) for case in cases if feature_name in case["features"]]
        if values:
            summary[feature_name] = round(sum(values) / len(values), 6)
    return summary


def build_contrast_summary(
    selected_cases: Sequence[Dict[str, Any]],
    positives: Sequence[Dict[str, Any]],
    negatives: Sequence[Dict[str, Any]],
    *,
    side: str,
) -> Dict[str, Any]:
    feature_contrasts = []
    for feature_name in APPROVED_FEATURES:
        positive_values = [float(case["features"][feature_name]) for case in positives if feature_name in case["features"]]
        negative_values = [float(case["features"][feature_name]) for case in negatives if feature_name in case["features"]]
        if not positive_values or not negative_values:
            continue
        positive_mean = sum(positive_values) / len(positive_values)
        negative_mean = sum(negative_values) / len(negative_values)
        positive_median = statistics.median(positive_values)
        negative_median = statistics.median(negative_values)
        delta = positive_mean - negative_mean
        feature_contrasts.append(
            {
                "feature": feature_name,
                "positiveMean": round(positive_mean, 6),
                "falsePositiveMean": round(negative_mean, 6),
                "positiveMedian": round(float(positive_median), 6),
                "falsePositiveMedian": round(float(negative_median), 6),
                "delta": round(delta, 6),
                "absDelta": round(abs(delta), 6),
                "preferredOperator": ">=" if delta >= 0 else "<=",
                "suggestedThreshold": round((float(positive_median) + float(negative_median)) / 2.0, 6),
                "score": round(abs(delta), 6),
                "reason": f"positives {'exceed' if delta >= 0 else 'stay below'} false positives on {feature_name}",
            }
        )
    feature_contrasts.sort(key=lambda item: (float(item["absDelta"]), float(item["score"])), reverse=True)
    return {
        "positiveCount": len(positives),
        "falsePositiveCount": len(negatives),
        "topFeatures": feature_contrasts[:5],
        "sessionBuckets": summarize_by_bucket(selected_cases, side=side, key="sessionBucket"),
        "spreadBuckets": summarize_spread_buckets(selected_cases, side=side),
    }


def build_summary_payload(
    *,
    params: EntryResearchParameters,
    slice_bounds: Dict[str, Any],
    cases: Sequence[Dict[str, Any]],
    train_cases: Sequence[Dict[str, Any]],
    validation_cases: Sequence[Dict[str, Any]],
    candidate_results: Sequence[Dict[str, Any]],
    best_candidate: Dict[str, Any],
    settings: ResearchSettings,
) -> Dict[str, Any]:
    validation_metrics = best_candidate["validationMetrics"]
    verdict_hint = infer_verdict_hint(best_candidate)
    candidate_comparison = build_candidate_comparison(candidate_results)
    draft_mutations = generate_mutation_proposals(
        base_params=params,
        summary_payload={"bestCandidate": best_candidate, "candidateResults": candidate_results},
        settings=settings,
        source_run_id=None,
        seen_fingerprints=(),
        pending_fingerprints=(),
    )
    briefing = {
        "runScope": {
            "symbol": params.symbol,
            "iteration": params.iteration,
            "sliceStartTickId": slice_bounds["start_tick_id"],
            "sliceEndTickId": slice_bounds["end_tick_id"],
            "sliceRows": slice_bounds["slice_rows"],
            "sliceOffsetRows": slice_bounds["slice_offset_rows"],
            "caseCount": len(cases),
            "trainCount": len(train_cases),
            "validationCount": len(validation_cases),
        },
        "config": params.model_dump(),
        "bestCandidate": {
            "name": best_candidate["candidateName"],
            "side": best_candidate["rule"]["side"],
            "family": best_candidate["family"],
            "predicates": best_candidate["rule"]["predicates"],
            "trainMetrics": best_candidate["trainMetrics"],
            "validationMetrics": validation_metrics,
            "contrastSummary": best_candidate["contrastSummary"],
        },
        "topCandidates": [
            {
                "name": item["candidateName"],
                "side": item["rule"]["side"],
                "family": item["family"],
                "predicates": item["rule"]["predicates"],
                "validationMetrics": item["validationMetrics"],
            }
            for item in candidate_results[:5]
        ],
        "positivePatternSummary": best_candidate["patternSummary"]["positivePattern"],
        "falsePositivePatternSummary": best_candidate["patternSummary"]["falsePositivePattern"],
        "contrastSummary": best_candidate["contrastSummary"],
        "candidateComparisonSummary": candidate_comparison,
        "mutationProposals": draft_mutations,
        "positives": best_candidate["positives"][: settings.max_examples],
        "falsePositives": best_candidate["falsePositives"][: settings.max_examples],
        "verdictHint": verdict_hint,
    }
    return {
        "headline": f"Entry-only bounded run {params.iteration} on {params.symbol}",
        "config": params.model_dump(),
        "sliceBounds": slice_bounds,
        "caseCount": len(cases),
        "bestCandidate": {
            "candidateName": best_candidate["candidateName"],
            "rule": best_candidate["rule"],
            "trainMetrics": best_candidate["trainMetrics"],
            "validationMetrics": validation_metrics,
            "positiveExamples": best_candidate["positives"],
            "falsePositiveExamples": best_candidate["falsePositives"],
            "patternSummary": best_candidate["patternSummary"],
            "contrastSummary": best_candidate["contrastSummary"],
        },
        "candidateResults": [
            {
                "candidateName": item["candidateName"],
                "family": item["family"],
                "rule": item["rule"],
                "trainMetrics": item["trainMetrics"],
                "validationMetrics": item["validationMetrics"],
                "contrastSummary": item["contrastSummary"],
            }
            for item in candidate_results
        ],
        "analysis": {
            "positivePatternSummary": best_candidate["patternSummary"]["positivePattern"],
            "falsePositivePatternSummary": best_candidate["patternSummary"]["falsePositivePattern"],
            "contrastSummary": best_candidate["contrastSummary"],
            "candidateComparisonSummary": candidate_comparison,
        },
        "mutationProposals": draft_mutations,
        "verdictHint": verdict_hint,
        "briefing": briefing,
    }


def build_candidate_comparison(candidate_results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidate_results:
        return {}
    best = candidate_results[0]
    runner_up = candidate_results[1] if len(candidate_results) > 1 else None
    families = sorted({str(item["family"]) for item in candidate_results[:5]})
    sides = {}
    for item in candidate_results[:5]:
        side = str(item["rule"]["side"])
        sides[side] = int(sides.get(side, 0)) + 1
    payload = {
        "familiesSeen": families,
        "sideCounts": sides,
        "bestCleanPrecision": best["validationMetrics"]["cleanPrecision"],
        "bestSignalCount": best["validationMetrics"]["signalCount"],
    }
    if runner_up is not None:
        payload["runnerUpCleanPrecision"] = runner_up["validationMetrics"]["cleanPrecision"]
        payload["cleanPrecisionGapVsRunnerUp"] = round(
            float(best["validationMetrics"]["cleanPrecision"]) - float(runner_up["validationMetrics"]["cleanPrecision"]),
            6,
        )
    return payload


def infer_verdict_hint(candidate: Dict[str, Any]) -> str:
    validation = candidate["validationMetrics"]
    clean_precision = float(validation["cleanPrecision"])
    entries_per_day = float(validation["entriesPerDay"])
    walk_forward_range = float(validation.get("walkForwardRange") or 0.0)
    signal_count = int(validation.get("signalCount") or 0)
    if clean_precision >= 0.70 and entries_per_day >= 0.75 and walk_forward_range <= 0.15 and signal_count >= 10:
        return "strong_narrow_regime_found"
    if clean_precision >= 0.70 and entries_per_day < 0.75:
        return "good_precision_but_too_low_frequency"
    if clean_precision >= 0.45 and walk_forward_range > 0.25:
        return "unstable_out_of_sample"
    if clean_precision >= 0.40:
        return "moderate_edge_not_near_target"
    return "no_robust_edge_found"


def build_storage_rows(
    *,
    run_id: int,
    cases: Sequence[Dict[str, Any]],
    candidate_results: Sequence[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    label_rows: List[Dict[str, Any]] = []
    feature_rows: List[Dict[str, Any]] = []
    for case in cases:
        feature_rows.append(
            {
                "runId": run_id,
                "tickId": int(case["tickId"]),
                "timestamp": case["timestamp"],
                "sessionBucket": case["sessionBucket"],
                "features": dict(case["features"]),
            }
        )
        for side in ("long", "short"):
            label_rows.append(
                {
                    "runId": run_id,
                    "tickId": int(case["tickId"]),
                    "timestamp": case["timestamp"],
                    "sessionBucket": case["sessionBucket"],
                    **dict(case["labels"][side]),
                }
            )
    candidate_rows = []
    for index, candidate in enumerate(candidate_results, start=1):
        candidate_rows.append(
            {
                "runId": run_id,
                "rank": index,
                "candidateName": candidate["candidateName"],
                "family": candidate["family"],
                "side": candidate["rule"]["side"],
                "selected": index == 1,
                "rule": dict(candidate["rule"]),
                "trainMetrics": dict(candidate["trainMetrics"]),
                "validationMetrics": dict(candidate["validationMetrics"]),
            }
        )
    return label_rows, feature_rows, candidate_rows


def compact_feature_payload(features: Dict[str, Any]) -> Dict[str, float]:
    return {key: round(float(value), 6) for key, value in features.items() if key in set(APPROVED_FEATURES)}


def quantile_points(values: Sequence[float], quantiles: Iterable[float]) -> List[float]:
    points = []
    for quantile in quantiles:
        if not values:
            continue
        points.append(quantile_value(values, quantile))
    return sorted(set(round(point, 6) for point in points))


def quantile_value(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, float(quantile))) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def candidate_sort_key(item: Dict[str, Any]) -> tuple[float, float, float, int]:
    validation = item["validationMetrics"]
    train = item["trainMetrics"]
    return (
        float(validation["cleanPrecision"]),
        float(validation["precision"]),
        -float(validation.get("walkForwardRange") or 0.0),
        int(train["signalCount"]),
    )


def longest_same_sign_streak(values: Sequence[int]) -> int:
    longest = 0
    current = 0
    last = 0
    for value in values:
        if value == 0:
            current = 0
            last = 0
            continue
        if value == last:
            current += 1
        else:
            current = 1
            last = value
        longest = max(longest, current)
    return longest


def sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def session_bucket_for_timestamp(value: datetime) -> str:
    local = value.astimezone(BROKER_TZ)
    hour = int(local.hour)
    if hour < 4:
        return "bucket_00_04"
    if hour < 8:
        return "bucket_04_08"
    if hour < 12:
        return "bucket_08_12"
    if hour < 16:
        return "bucket_12_16"
    if hour < 20:
        return "bucket_16_20"
    return "bucket_20_24"
