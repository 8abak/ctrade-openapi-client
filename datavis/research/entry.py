from __future__ import annotations

import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from datetime import date, datetime
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
from datavis.separation import brokerday_bounds, brokerday_for_timestamp


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


DIVERGENCE_PIVOT_SPAN = 4
DIVERGENCE_MIN_PIVOT_GAP = 12
DIVERGENCE_MAX_PIVOT_GAP = 180
DIVERGENCE_MIN_MOVE_MULTIPLIER = 1.25
DIVERGENCE_SIMILAR_TOLERANCE_MULTIPLIER = 0.75
DIVERGENCE_INDICATOR_RANGE_FRACTION = 0.15
DIVERGENCE_INDICATOR_NOISE_MULTIPLIER = 2.0
DIVERGENCE_TREND_LOOKBACK = 24
DIVERGENCE_RSI_PERIOD = 14
DIVERGENCE_ROC_PERIOD = 8
DIVERGENCE_AGREEMENT_WINDOW = 6
DIVERGENCE_STRUCTURE_PACK = (
    "pivot_span4_gap12_max180_move125_sim75_ind15_noise2_trend24_confirmed"
)


def _rule_setup_fingerprint(rule_json: Dict[str, Any], *, fallback_name: str = "") -> str:
    encoded = json.dumps(dict(rule_json or {}), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    digest = hashlib.sha1(encoded).hexdigest()[:16]
    name = str(fallback_name or (rule_json or {}).get("name") or "setup").lower().replace(" ", "-")
    name = "".join(ch for ch in name if ch.isalnum() or ch in {"-", "_", ":"})[:48] or "setup"
    return f"{name}-{digest}"


def _payload_fingerprint(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:20]


def _latest_available_brokerday(conn: Any, *, symbol: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX((((timestamp AT TIME ZONE 'Australia/Sydney') - INTERVAL '8 hours')::date)) AS brokerday
            FROM public.ticks
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return date.fromisoformat(str(row[0])).isoformat()


def resolve_divergence_slice_bounds(
    conn: Any,
    *,
    symbol: str,
    study_brokerday: Optional[str],
    lookahead_ticks: int,
) -> Dict[str, Any]:
    brokerday_text = study_brokerday or _latest_available_brokerday(conn, symbol=symbol)
    if not brokerday_text:
        raise RuntimeError(f"No ticks found for symbol {symbol}.")
    day_value = date.fromisoformat(brokerday_text)
    start_ts, end_ts = brokerday_bounds(day_value)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(id) AS min_id, MAX(id) AS max_id
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            """,
            (symbol, start_ts, end_ts),
        )
        row = cur.fetchone()
    min_day_id = int(row[0] or 0) if row else 0
    max_day_id = int(row[1] or 0) if row else 0
    if min_day_id <= 0 or max_day_id <= 0:
        raise RuntimeError(f"No ticks found for symbol {symbol} on broker day {brokerday_text}.")
    end_tick_id = max(min_day_id, max_day_id - max(lookahead_ticks, 4))
    return {
        "latest_tick_id": max_day_id,
        "start_tick_id": min_day_id,
        "end_tick_id": end_tick_id,
        "slice_rows": max(0, end_tick_id - min_day_id + 1),
        "slice_offset_rows": 0,
        "study_brokerday": brokerday_text,
    }


def execute_divergence_sweep(
    conn: Any,
    *,
    params: EntryResearchParameters,
    settings: ResearchSettings,
    variant: Dict[str, Any],
) -> Dict[str, Any]:
    slice_bounds = resolve_divergence_slice_bounds(
        conn,
        symbol=params.symbol,
        study_brokerday=params.study_brokerday,
        lookahead_ticks=int(variant["horizon_ticks"]),
    )
    context_rows = fetch_context_rows(
        conn,
        symbol=params.symbol,
        start_tick_id=slice_bounds["start_tick_id"],
        end_tick_id=slice_bounds["end_tick_id"],
        warmup_rows=max(params.warmup_rows, 80),
        lookahead_rows=int(variant["horizon_ticks"]) + 4,
        limit=max(1, slice_bounds["slice_rows"] + max(params.warmup_rows, 80) + int(variant["horizon_ticks"]) + 16),
    )
    events, candidate_results = build_divergence_candidates(
        rows=context_rows,
        start_tick_id=slice_bounds["start_tick_id"],
        end_tick_id=slice_bounds["end_tick_id"],
        params=params,
        settings=settings,
        variant=variant,
    )
    if not candidate_results:
        candidate_results = [build_empty_divergence_candidate(params)]
    best_candidate = candidate_results[0]
    summary_payload = build_divergence_summary_payload(
        params=params,
        slice_bounds=slice_bounds,
        events=events,
        candidate_results=candidate_results,
        best_candidate=best_candidate,
        settings=settings,
    )
    divergence_event_rows = build_divergence_event_rows(events)
    _, _, candidate_rows = build_storage_rows(run_id=0, cases=(), candidate_results=candidate_results)
    return {
        "sliceBounds": slice_bounds,
        "cases": [],
        "labelRows": [],
        "featureRows": [],
        "candidateRows": candidate_rows,
        "divergenceEventRows": divergence_event_rows,
        "summaryPayload": summary_payload,
    }


def execute_entry_research(
    conn: Any,
    *,
    params: EntryResearchParameters,
    settings: ResearchSettings,
) -> Dict[str, Any]:
    variant = label_variant_payload(params.label_variant)
    if params.candidate_family == "divergence_sweep":
        return execute_divergence_sweep(conn, params=params, settings=settings, variant=variant)
    slice_bounds = resolve_slice_bounds(
        conn,
        symbol=params.symbol,
        study_brokerday=params.study_brokerday,
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
    study_brokerday: str | None,
    slice_rows: int,
    slice_offset_rows: int,
    lookahead_ticks: int,
) -> Dict[str, Any]:
    with conn.cursor() as cur:
        if study_brokerday:
            day_value = date.fromisoformat(study_brokerday)
            start_ts, end_ts = brokerday_bounds(day_value)
            cur.execute(
                """
                SELECT MIN(id) AS min_id, MAX(id) AS max_id
                FROM public.ticks
                WHERE symbol = %s
                  AND timestamp >= %s
                  AND timestamp < %s
                """,
                (symbol, start_ts, end_ts),
            )
        else:
            cur.execute(
                """
                SELECT NULL::BIGINT AS min_id, MAX(id) AS max_id
                FROM public.ticks
                WHERE symbol = %s
                """,
                (symbol,),
            )
        row = cur.fetchone()
    row_values = row or (None, 0)
    min_day_id = int(row_values[0]) if row_values[0] is not None else None
    latest_id = int(row_values[1] or 0)
    if latest_id <= 0:
        if study_brokerday:
            raise RuntimeError(f"No ticks found for symbol {symbol} on broker day {study_brokerday}.")
        raise RuntimeError(f"No ticks found for symbol {symbol}.")
    end_tick_id = max(1, latest_id - max(lookahead_ticks, 4) - max(0, int(slice_offset_rows)))
    if min_day_id is not None and end_tick_id < min_day_id:
        end_tick_id = latest_id
    start_tick_id = max(1, end_tick_id - max(1, slice_rows) + 1)
    if min_day_id is not None:
        start_tick_id = max(min_day_id, start_tick_id)
    return {
        "latest_tick_id": latest_id,
        "start_tick_id": start_tick_id,
        "end_tick_id": end_tick_id,
        "slice_rows": max(0, end_tick_id - start_tick_id + 1),
        "slice_offset_rows": max(0, int(slice_offset_rows)),
        "study_brokerday": study_brokerday,
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


def build_divergence_candidates(
    *,
    rows: Sequence[TickRow],
    start_tick_id: int,
    end_tick_id: int,
    params: EntryResearchParameters,
    settings: ResearchSettings,
    variant: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows_by_id = {row.id: index for index, row in enumerate(rows)}
    start_index = rows_by_id.get(start_tick_id)
    end_index = rows_by_id.get(end_tick_id)
    if start_index is None or end_index is None or end_index <= start_index:
        return [], []
    indicator_series = build_divergence_indicator_series(rows)
    pivots = detect_price_pivots(rows, start_index=start_index, end_index=end_index)
    base_events: List[Dict[str, Any]] = []
    for indicator_name, values in indicator_series.items():
        base_events.extend(
            detect_divergence_events(
                rows=rows,
                start_index=start_index,
                end_index=end_index,
                pivots=pivots,
                indicator_name=indicator_name,
                indicator_values=values,
                variant=variant,
            )
        )
    agreement_events = build_multi_indicator_agreement_events(rows=rows, base_events=base_events, variant=variant)
    all_events = base_events + agreement_events
    by_setup: Dict[str, Dict[str, Any]] = {}
    dedup_gap = APPROVED_DEDUP_RULES[params.dedup_rule]
    for event in all_events:
        payload = by_setup.setdefault(
            str(event["setupFingerprint"]),
            {
                "candidateName": event["candidateName"],
                "rule": dict(event["rule"]),
                "setupFingerprint": event["setupFingerprint"],
                "cases": [],
            },
        )
        payload["cases"].append(divergence_event_to_case(event))
    candidate_results: List[Dict[str, Any]] = []
    for payload in by_setup.values():
        cases = dedupe_divergence_cases(payload["cases"], gap=dedup_gap)
        if not cases:
            continue
        train_cases, validation_cases = split_divergence_cases(cases, params.train_validation_plan)
        candidate_results.append(
            build_divergence_candidate_result(
                candidate_name=str(payload["candidateName"]),
                rule=dict(payload["rule"]),
                setup_fingerprint=str(payload["setupFingerprint"]),
                train_cases=train_cases,
                validation_cases=validation_cases,
                example_limit=settings.max_examples,
            )
        )
    candidate_results.sort(key=divergence_candidate_sort_key, reverse=True)
    selected_candidates = candidate_results[: max(1, settings.max_candidates)]
    allowed_fingerprints = {str(item["setupFingerprint"]) for item in selected_candidates}
    selected_events = [event for event in all_events if str(event["setupFingerprint"]) in allowed_fingerprints]
    selected_events.sort(key=lambda item: (int(item["tickId"]), str(item["candidateName"])))
    return selected_events, selected_candidates


def build_divergence_indicator_series(rows: Sequence[TickRow]) -> Dict[str, List[Optional[float]]]:
    mids = [float(row.mid) for row in rows]
    short_baseline = weighted_window_series(mids, window=5)
    long_baseline = weighted_window_series(mids, window=9)
    macd_line, macd_hist = macd_series(mids, fast=12, slow=26, signal=9)
    return {
        "rsi14": rsi_series(mids, period=DIVERGENCE_RSI_PERIOD),
        "macd_line": macd_line,
        "macd_hist": macd_hist,
        "roc8": roc_series(mids, period=DIVERGENCE_ROC_PERIOD),
        "kal_gap": [
            round(float(short_value) - float(long_value), 8) if short_value is not None and long_value is not None else None
            for short_value, long_value in zip(short_baseline, long_baseline)
        ],
    }


def detect_price_pivots(
    rows: Sequence[TickRow],
    *,
    start_index: int,
    end_index: int,
) -> Dict[str, List[Dict[str, Any]]]:
    lows: List[Dict[str, Any]] = []
    highs: List[Dict[str, Any]] = []
    for index in range(max(start_index, DIVERGENCE_PIVOT_SPAN), min(end_index, len(rows) - DIVERGENCE_PIVOT_SPAN - 1) + 1):
        left = [rows[pos].mid for pos in range(index - DIVERGENCE_PIVOT_SPAN, index)]
        right = [rows[pos].mid for pos in range(index + 1, index + DIVERGENCE_PIVOT_SPAN + 1)]
        pivot = rows[index]
        if left and right and pivot.mid <= min(left) and pivot.mid < min(right):
            lows.append(
                {
                    "index": index,
                    "tickId": pivot.id,
                    "timestamp": pivot.timestamp,
                    "price": round(float(pivot.mid), 8),
                    "spread": round(float(pivot.spread), 8),
                }
            )
        if left and right and pivot.mid >= max(left) and pivot.mid > max(right):
            highs.append(
                {
                    "index": index,
                    "tickId": pivot.id,
                    "timestamp": pivot.timestamp,
                    "price": round(float(pivot.mid), 8),
                    "spread": round(float(pivot.spread), 8),
                }
            )
    return {"low": lows, "high": highs}


def detect_divergence_events(
    *,
    rows: Sequence[TickRow],
    start_index: int,
    end_index: int,
    pivots: Dict[str, List[Dict[str, Any]]],
    indicator_name: str,
    indicator_values: Sequence[Optional[float]],
    variant: Dict[str, Any],
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    baseline = ema_series([row.mid for row in rows], period=12)
    indicator_noise = divergence_indicator_noise(indicator_values)
    for pivot_type in ("low", "high"):
        series = list(pivots[pivot_type])
        for left_pivot, right_pivot in zip(series, series[1:]):
            gap = int(right_pivot["index"]) - int(left_pivot["index"])
            if gap < DIVERGENCE_MIN_PIVOT_GAP or gap > DIVERGENCE_MAX_PIVOT_GAP:
                continue
            event_index = int(right_pivot["index"]) + DIVERGENCE_PIVOT_SPAN
            if event_index < start_index or event_index > end_index or event_index >= len(rows):
                continue
            left_indicator = indicator_values[int(left_pivot["index"])]
            right_indicator = indicator_values[int(right_pivot["index"])]
            if left_indicator is None or right_indicator is None:
                continue
            local_values = [float(value) for value in indicator_values[int(left_pivot["index"]):int(right_pivot["index"]) + 1] if value is not None]
            local_range = (max(local_values) - min(local_values)) if local_values else 0.0
            indicator_move_min = max(local_range * DIVERGENCE_INDICATOR_RANGE_FRACTION, indicator_noise * DIVERGENCE_INDICATOR_NOISE_MULTIPLIER, 1e-6)
            price_move_min = max(float(left_pivot["spread"]), float(right_pivot["spread"])) * DIVERGENCE_MIN_MOVE_MULTIPLIER
            similar_tolerance = max(float(left_pivot["spread"]), float(right_pivot["spread"])) * DIVERGENCE_SIMILAR_TOLERANCE_MULTIPLIER
            baseline_now = baseline[event_index]
            baseline_prev_index = max(0, event_index - DIVERGENCE_TREND_LOOKBACK)
            baseline_prev = baseline[baseline_prev_index]
            trend_slope = (float(baseline_now) - float(baseline_prev)) if baseline_now is not None and baseline_prev is not None else 0.0
            for classification in classify_divergence_pair(
                pivot_type=pivot_type,
                price_left=float(left_pivot["price"]),
                price_right=float(right_pivot["price"]),
                indicator_left=float(left_indicator),
                indicator_right=float(right_indicator),
                price_move_min=price_move_min,
                similar_tolerance=similar_tolerance,
                indicator_move_min=indicator_move_min,
                trend_slope=trend_slope,
            ):
                outcome = compute_divergence_outcome(rows, event_index=event_index, side=str(classification["side"]), variant=variant)
                if outcome is None:
                    continue
                rule = build_divergence_rule(
                    event_family=str(classification["eventFamily"]),
                    event_subtype=str(classification["eventSubtype"]),
                    indicator_name=indicator_name,
                    side=str(classification["side"]),
                    signal_style=str(classification["signalStyle"]),
                )
                setup_fingerprint = _rule_setup_fingerprint(rule, fallback_name=str(rule["name"]))
                event_payload = {
                    "candidateName": rule["name"],
                    "setupFingerprint": setup_fingerprint,
                    "rule": rule,
                    "brokerday": brokerday_for_timestamp(rows[event_index].timestamp),
                    "tickId": rows[event_index].id,
                    "eventIndex": event_index,
                    "timestamp": rows[event_index].timestamp,
                    "eventFamily": classification["eventFamily"],
                    "eventSubtype": classification["eventSubtype"],
                    "indicatorName": indicator_name,
                    "side": classification["side"],
                    "signalStyle": classification["signalStyle"],
                    "pivotMethod": "confirmed_local_price_pivot_pair",
                    "structurePack": DIVERGENCE_STRUCTURE_PACK,
                    "pivotLeftTickId": left_pivot["tickId"],
                    "pivotRightTickId": right_pivot["tickId"],
                    "priceValue1": round(float(left_pivot["price"]), 8),
                    "priceValue2": round(float(right_pivot["price"]), 8),
                    "indicatorValue1": round(float(left_indicator), 8),
                    "indicatorValue2": round(float(right_indicator), 8),
                    "indicatorPayload": {
                        "indicatorMoveMin": round(float(indicator_move_min), 8),
                        "indicatorNoise": round(float(indicator_noise), 8),
                        "localIndicatorRange": round(float(local_range), 8),
                        "priceMoveMin": round(float(price_move_min), 8),
                        "similarTolerance": round(float(similar_tolerance), 8),
                        "trendSlope": round(float(trend_slope), 8),
                    },
                    "eventScore": round(float(classification["score"]), 8),
                    "sessionBucket": session_bucket_for_timestamp(rows[event_index].timestamp),
                    "labels": {classification["side"]: outcome},
                }
                event_payload["eventFingerprint"] = _payload_fingerprint(
                    {
                        "setupFingerprint": setup_fingerprint,
                        "tickId": event_payload["tickId"],
                        "pivotLeftTickId": event_payload["pivotLeftTickId"],
                        "pivotRightTickId": event_payload["pivotRightTickId"],
                        "eventSubtype": event_payload["eventSubtype"],
                        "indicatorName": indicator_name,
                    }
                )
                events.append(event_payload)
    return events


def classify_divergence_pair(
    *,
    pivot_type: str,
    price_left: float,
    price_right: float,
    indicator_left: float,
    indicator_right: float,
    price_move_min: float,
    similar_tolerance: float,
    indicator_move_min: float,
    trend_slope: float,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    price_delta = price_right - price_left
    indicator_delta = indicator_right - indicator_left
    if pivot_type == "low":
        if price_delta <= -price_move_min and indicator_delta >= indicator_move_min:
            results.append(
                {
                    "eventFamily": "price_indicator_divergence",
                    "eventSubtype": "classic_bullish",
                    "side": "long",
                    "signalStyle": "reversal",
                    "score": divergence_score(abs(price_delta), price_move_min, abs(indicator_delta), indicator_move_min),
                }
            )
        if price_delta >= price_move_min and indicator_delta <= -indicator_move_min:
            results.append(
                {
                    "eventFamily": "price_indicator_divergence",
                    "eventSubtype": "hidden_bullish",
                    "side": "long",
                    "signalStyle": "reversal",
                    "score": divergence_score(abs(price_delta), price_move_min, abs(indicator_delta), indicator_move_min),
                }
            )
        if abs(price_delta) <= similar_tolerance and indicator_delta >= indicator_move_min:
            results.append(
                {
                    "eventFamily": "price_indicator_divergence",
                    "eventSubtype": "exaggerated_bullish",
                    "side": "long",
                    "signalStyle": "reversal",
                    "score": divergence_score(similar_tolerance - abs(price_delta), max(similar_tolerance, 1e-6), abs(indicator_delta), indicator_move_min),
                }
            )
        if price_delta >= price_move_min and indicator_delta >= indicator_move_min and trend_slope > price_move_min:
            results.append(
                {
                    "eventFamily": "continuation_convergence",
                    "eventSubtype": "strengthening_bullish_convergence",
                    "side": "long",
                    "signalStyle": "continuation",
                    "score": divergence_score(abs(trend_slope), price_move_min, abs(indicator_delta), indicator_move_min),
                }
            )
    else:
        if price_delta >= price_move_min and indicator_delta <= -indicator_move_min:
            results.append(
                {
                    "eventFamily": "price_indicator_divergence",
                    "eventSubtype": "classic_bearish",
                    "side": "short",
                    "signalStyle": "reversal",
                    "score": divergence_score(abs(price_delta), price_move_min, abs(indicator_delta), indicator_move_min),
                }
            )
        if price_delta <= -price_move_min and indicator_delta >= indicator_move_min:
            results.append(
                {
                    "eventFamily": "price_indicator_divergence",
                    "eventSubtype": "hidden_bearish",
                    "side": "short",
                    "signalStyle": "reversal",
                    "score": divergence_score(abs(price_delta), price_move_min, abs(indicator_delta), indicator_move_min),
                }
            )
        if abs(price_delta) <= similar_tolerance and indicator_delta <= -indicator_move_min:
            results.append(
                {
                    "eventFamily": "price_indicator_divergence",
                    "eventSubtype": "exaggerated_bearish",
                    "side": "short",
                    "signalStyle": "reversal",
                    "score": divergence_score(similar_tolerance - abs(price_delta), max(similar_tolerance, 1e-6), abs(indicator_delta), indicator_move_min),
                }
            )
        if price_delta <= -price_move_min and indicator_delta <= -indicator_move_min and trend_slope < -price_move_min:
            results.append(
                {
                    "eventFamily": "continuation_convergence",
                    "eventSubtype": "strengthening_bearish_convergence",
                    "side": "short",
                    "signalStyle": "continuation",
                    "score": divergence_score(abs(trend_slope), price_move_min, abs(indicator_delta), indicator_move_min),
                }
            )
    return results


def build_divergence_rule(
    *,
    event_family: str,
    event_subtype: str,
    indicator_name: str,
    side: str,
    signal_style: str,
) -> Dict[str, Any]:
    name = f"{side}:divergence:{event_subtype}:{indicator_name}"
    return {
        "name": name,
        "family": "divergence_sweep",
        "side": side,
        "predicates": [],
        "divergence": {
            "eventFamily": event_family,
            "eventSubtype": event_subtype,
            "indicator": indicator_name,
            "style": signal_style,
            "pivotMethod": "confirmed_local_price_pivot_pair",
            "structurePack": DIVERGENCE_STRUCTURE_PACK,
            "agreementWindowTicks": DIVERGENCE_AGREEMENT_WINDOW,
        },
    }


def build_multi_indicator_agreement_events(
    *,
    rows: Sequence[TickRow],
    base_events: Sequence[Dict[str, Any]],
    variant: Dict[str, Any],
) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for event in base_events:
        grouped.setdefault((str(event["eventSubtype"]), str(event["side"])), []).append(dict(event))
    agreement_events: List[Dict[str, Any]] = []
    for (event_subtype, side), events in grouped.items():
        ordered = sorted(events, key=lambda item: int(item["eventIndex"]))
        index = 0
        while index < len(ordered):
            anchor = ordered[index]
            window_members = [anchor]
            cursor = index + 1
            while cursor < len(ordered) and (int(ordered[cursor]["eventIndex"]) - int(anchor["eventIndex"])) <= DIVERGENCE_AGREEMENT_WINDOW:
                window_members.append(ordered[cursor])
                cursor += 1
            distinct = {}
            for member in window_members:
                indicator_name = str(member["indicatorName"])
                current = distinct.get(indicator_name)
                if current is None or float(member.get("eventScore") or 0.0) > float(current.get("eventScore") or 0.0):
                    distinct[indicator_name] = member
            selected_members = sorted(distinct.values(), key=lambda item: float(item.get("eventScore") or 0.0), reverse=True)
            if len(selected_members) >= 2:
                confirmation = max(selected_members, key=lambda item: int(item["eventIndex"]))
                member_names = [str(item["indicatorName"]) for item in selected_members[:3]]
                rule = build_divergence_rule(
                    event_family="multi_indicator_agreement",
                    event_subtype=event_subtype,
                    indicator_name="+".join(member_names),
                    side=side,
                    signal_style=str(confirmation["signalStyle"]),
                )
                setup_fingerprint = _rule_setup_fingerprint(rule, fallback_name=str(rule["name"]))
                outcome = compute_divergence_outcome(rows, event_index=int(confirmation["eventIndex"]), side=side, variant=variant)
                if outcome is not None:
                    event_payload = {
                        "candidateName": rule["name"],
                        "setupFingerprint": setup_fingerprint,
                        "rule": rule,
                        "brokerday": confirmation["brokerday"],
                        "tickId": confirmation["tickId"],
                        "eventIndex": confirmation["eventIndex"],
                        "timestamp": confirmation["timestamp"],
                        "eventFamily": "multi_indicator_agreement",
                        "eventSubtype": event_subtype,
                        "indicatorName": "+".join(member_names),
                        "side": side,
                        "signalStyle": confirmation["signalStyle"],
                        "pivotMethod": confirmation["pivotMethod"],
                        "structurePack": confirmation["structurePack"],
                        "pivotLeftTickId": confirmation["pivotLeftTickId"],
                        "pivotRightTickId": confirmation["pivotRightTickId"],
                        "priceValue1": confirmation["priceValue1"],
                        "priceValue2": confirmation["priceValue2"],
                        "indicatorValue1": None,
                        "indicatorValue2": None,
                        "indicatorPayload": {
                            "agreementMembers": [
                                {
                                    "indicator": member["indicatorName"],
                                    "indicatorValue1": member["indicatorValue1"],
                                    "indicatorValue2": member["indicatorValue2"],
                                    "eventScore": member["eventScore"],
                                }
                                for member in selected_members[:3]
                            ],
                            "agreementCount": len(selected_members),
                        },
                        "eventScore": round(sum(float(item.get("eventScore") or 0.0) for item in selected_members[:3]) / len(selected_members[:3]), 8),
                        "sessionBucket": confirmation["sessionBucket"],
                        "labels": {side: outcome},
                    }
                    event_payload["eventFingerprint"] = _payload_fingerprint(
                        {
                            "setupFingerprint": setup_fingerprint,
                            "tickId": event_payload["tickId"],
                            "eventSubtype": event_payload["eventSubtype"],
                            "indicatorName": event_payload["indicatorName"],
                        }
                    )
                    agreement_events.append(event_payload)
            index = cursor
    return agreement_events


def compute_divergence_outcome(
    rows: Sequence[TickRow],
    *,
    event_index: int,
    side: str,
    variant: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    tick = rows[event_index]
    spread = max(0.000001, float(tick.spread))
    horizon_ticks = int(variant["horizon_ticks"])
    horizon_seconds = int(variant["horizon_seconds"])
    target_multiplier = float(variant["target_multiplier"])
    adverse_multiplier = float(variant["adverse_multiplier"])
    entry_price = float(tick.ask if side == "long" else tick.bid)
    target_amount = target_multiplier * spread
    adverse_amount = adverse_multiplier * spread
    target_price = entry_price + target_amount if side == "long" else entry_price - target_amount
    adverse_price = entry_price - adverse_amount if side == "long" else entry_price + adverse_amount
    target_hit_offset: Optional[int] = None
    target_hit_seconds: Optional[float] = None
    adverse_hit_offset: Optional[int] = None
    adverse_hit_seconds: Optional[float] = None
    max_favorable = 0.0
    max_adverse = 0.0
    observed = 0
    last_elapsed_seconds = 0.0
    resolution_offset: Optional[int] = None
    for offset, future in enumerate(rows[event_index + 1:], start=1):
        elapsed_seconds = max(0.0, (future.timestamp - tick.timestamp).total_seconds())
        if offset > horizon_ticks or elapsed_seconds > horizon_seconds:
            break
        observed += 1
        last_elapsed_seconds = elapsed_seconds
        executable = float(future.bid if side == "long" else future.ask)
        if side == "long":
            max_favorable = max(max_favorable, executable - entry_price)
            max_adverse = max(max_adverse, entry_price - executable)
            if target_hit_offset is None and executable >= target_price:
                target_hit_offset = offset
                target_hit_seconds = elapsed_seconds
            if adverse_hit_offset is None and executable <= adverse_price:
                adverse_hit_offset = offset
                adverse_hit_seconds = elapsed_seconds
        else:
            max_favorable = max(max_favorable, entry_price - executable)
            max_adverse = max(max_adverse, executable - entry_price)
            if target_hit_offset is None and executable <= target_price:
                target_hit_offset = offset
                target_hit_seconds = elapsed_seconds
            if adverse_hit_offset is None and executable >= adverse_price:
                adverse_hit_offset = offset
                adverse_hit_seconds = elapsed_seconds
        if resolution_offset is None:
            if target_hit_offset is not None and (adverse_hit_offset is None or target_hit_offset <= adverse_hit_offset):
                resolution_offset = target_hit_offset
            elif adverse_hit_offset is not None and (target_hit_offset is None or adverse_hit_offset < target_hit_offset):
                resolution_offset = adverse_hit_offset
        if resolution_offset is not None and offset >= resolution_offset:
            break
    if observed < horizon_ticks and last_elapsed_seconds < horizon_seconds and resolution_offset is None:
        return None
    first_side_hit = "timeout"
    first_hit_ticks = None
    first_hit_seconds = None
    if target_hit_offset is not None and (adverse_hit_offset is None or target_hit_offset <= adverse_hit_offset):
        first_side_hit = "target"
        first_hit_ticks = target_hit_offset
        first_hit_seconds = target_hit_seconds
    elif adverse_hit_offset is not None:
        first_side_hit = "adverse"
        first_hit_ticks = adverse_hit_offset
        first_hit_seconds = adverse_hit_seconds
    return {
        "side": side,
        "entryPrice": round(entry_price, 8),
        "spreadAtEntry": round(spread, 8),
        "targetPrice": round(target_price, 8),
        "targetMultiplier": target_multiplier,
        "targetAmount": round(target_amount, 8),
        "adversePrice": round(adverse_price, 8),
        "adverseMultiplier": adverse_multiplier,
        "horizonTicks": horizon_ticks,
        "horizonSeconds": horizon_seconds,
        "hit2x": target_hit_offset is not None,
        "hitTicks": first_hit_ticks,
        "hitSeconds": round(float(first_hit_seconds), 6) if first_hit_seconds is not None else None,
        "maxFavorable": round(max_favorable, 8),
        "maxAdverse": round(max_adverse, 8),
        "adverseHit": adverse_hit_offset is not None,
        "targetBeforeAdverse": bool(target_hit_offset is not None and (adverse_hit_offset is None or target_hit_offset <= adverse_hit_offset)),
        "firstSideHit": first_side_hit,
        "scalpQualified": bool(target_hit_offset is not None and (adverse_hit_offset is None or target_hit_offset <= adverse_hit_offset)),
    }


def divergence_event_to_case(event: Dict[str, Any]) -> Dict[str, Any]:
    side = str(event["side"])
    label = dict(event["labels"][side])
    return {
        "tickId": int(event["tickId"]),
        "timestamp": event["timestamp"],
        "sessionBucket": event["sessionBucket"],
        "spread": float(label["spreadAtEntry"]),
        "features": {},
        "labels": {side: label},
        "eventFamily": event["eventFamily"],
        "eventSubtype": event["eventSubtype"],
        "indicatorName": event["indicatorName"],
        "signalStyle": event["signalStyle"],
        "eventScore": float(event.get("eventScore") or 0.0),
    }


def dedupe_divergence_cases(cases: Sequence[Dict[str, Any]], *, gap: int) -> List[Dict[str, Any]]:
    if gap <= 0:
        return sorted(list(cases), key=lambda item: int(item["tickId"]))
    selected: List[Dict[str, Any]] = []
    last_tick_id: Optional[int] = None
    for case in sorted(cases, key=lambda item: int(item["tickId"])):
        tick_id = int(case["tickId"])
        if last_tick_id is not None and (tick_id - last_tick_id) <= gap:
            continue
        selected.append(case)
        last_tick_id = tick_id
    return selected


def split_divergence_cases(cases: Sequence[Dict[str, Any]], plan: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if len(cases) <= 2:
        materialized = list(cases)
        return materialized, materialized
    return split_cases(cases, plan)


def build_divergence_candidate_result(
    *,
    candidate_name: str,
    rule: Dict[str, Any],
    setup_fingerprint: str,
    train_cases: Sequence[Dict[str, Any]],
    validation_cases: Sequence[Dict[str, Any]],
    example_limit: int,
) -> Dict[str, Any]:
    side = str(rule["side"])
    train_metrics = compute_metrics(train_cases, side=side)
    validation_metrics = compute_metrics(validation_cases, side=side)
    validation_metrics["walkForward"] = walk_forward_summary(validation_cases, side=side)
    validation_metrics["walkForwardRange"] = validation_metrics["walkForward"]["precisionRange"]
    validation_metrics["bySession"] = summarize_by_bucket(validation_cases, side=side, key="sessionBucket")
    validation_metrics["bySpread"] = summarize_spread_buckets(validation_cases, side=side)
    validation_metrics["medianEventScore"] = round(
        statistics.median(float(case.get("eventScore") or 0.0) for case in validation_cases),
        6,
    ) if validation_cases else 0.0
    validation_metrics["avgSpreadAtEvent"] = round(sum(float(case["spread"]) for case in validation_cases) / len(validation_cases), 8) if validation_cases else 0.0
    positives, negatives = split_selected_cases(validation_cases, side=side)
    return {
        "candidateName": candidate_name,
        "family": "divergence_sweep",
        "rule": rule,
        "setupFingerprint": setup_fingerprint,
        "trainMetrics": train_metrics,
        "validationMetrics": validation_metrics,
        "positives": render_divergence_examples(positives, side=side, limit=example_limit),
        "falsePositives": render_divergence_examples(negatives, side=side, limit=example_limit),
        "patternSummary": {
            "positivePattern": summarize_divergence_pattern(positives),
            "falsePositivePattern": summarize_divergence_pattern(negatives),
        },
        "contrastSummary": build_divergence_contrast_summary(validation_cases, positives, negatives, side=side),
    }


def render_divergence_examples(selected_cases: Sequence[Dict[str, Any]], *, side: str, limit: int) -> List[Dict[str, Any]]:
    payload = []
    for case in selected_cases:
        label = case["labels"][side]
        payload.append(
            {
                "tickId": int(case["tickId"]),
                "timestamp": case["timestamp"].isoformat(),
                "sessionBucket": case["sessionBucket"],
                "spread": round(float(case["spread"]), 8),
                "hit2x": bool(label["hit2x"]),
                "targetBeforeAdverse": bool(label["targetBeforeAdverse"]),
                "hitSeconds": label["hitSeconds"],
                "maxFavorable": label["maxFavorable"],
                "maxAdverse": label["maxAdverse"],
                "eventFamily": case.get("eventFamily"),
                "eventSubtype": case.get("eventSubtype"),
                "indicatorName": case.get("indicatorName"),
                "signalStyle": case.get("signalStyle"),
                "eventScore": round(float(case.get("eventScore") or 0.0), 6),
            }
        )
    payload.sort(key=lambda item: (float(item["eventScore"]), item["targetBeforeAdverse"], -float(item["spread"])), reverse=True)
    return payload[: max(1, limit)]


def summarize_divergence_pattern(cases: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not cases:
        return {}
    indicators = sorted({str(case.get("indicatorName") or "") for case in cases if case.get("indicatorName")})
    subtypes = sorted({str(case.get("eventSubtype") or "") for case in cases if case.get("eventSubtype")})
    hit_seconds = [
        float(case["labels"][next(iter(case["labels"]))]["hitSeconds"])
        for case in cases
        if case["labels"][next(iter(case["labels"]))].get("hitSeconds") is not None
    ]
    return {
        "count": len(cases),
        "indicators": indicators,
        "subtypes": subtypes,
        "medianHitSeconds": round(statistics.median(hit_seconds), 6) if hit_seconds else None,
        "avgEventScore": round(sum(float(case.get("eventScore") or 0.0) for case in cases) / len(cases), 6),
    }


def build_divergence_contrast_summary(
    selected_cases: Sequence[Dict[str, Any]],
    positives: Sequence[Dict[str, Any]],
    negatives: Sequence[Dict[str, Any]],
    *,
    side: str,
) -> Dict[str, Any]:
    return {
        "positiveCount": len(positives),
        "falsePositiveCount": len(negatives),
        "topFeatures": [],
        "sessionBuckets": summarize_by_bucket(selected_cases, side=side, key="sessionBucket"),
        "spreadBuckets": summarize_spread_buckets(selected_cases, side=side),
        "indicatorsSeen": sorted({str(case.get("indicatorName") or "") for case in selected_cases if case.get("indicatorName")}),
        "subtypesSeen": sorted({str(case.get("eventSubtype") or "") for case in selected_cases if case.get("eventSubtype")}),
    }


def divergence_candidate_sort_key(item: Dict[str, Any]) -> tuple[float, float, float, float, float]:
    validation = item["validationMetrics"]
    speed_score = 0.0
    if validation.get("medianHitSeconds") is not None:
        speed_score = 1.0 / (1.0 + float(validation["medianHitSeconds"]))
    stability_score = -float(validation.get("walkForwardRange") or 0.0)
    adverse_score = -float(validation.get("avgMaxAdverse") or 0.0)
    frequency_score = min(float(validation.get("signalCount") or 0.0), 12.0)
    return (
        float(validation.get("cleanPrecision") or 0.0),
        speed_score,
        stability_score,
        adverse_score,
        frequency_score,
    )


def build_divergence_summary_payload(
    *,
    params: EntryResearchParameters,
    slice_bounds: Dict[str, Any],
    events: Sequence[Dict[str, Any]],
    candidate_results: Sequence[Dict[str, Any]],
    best_candidate: Dict[str, Any],
    settings: ResearchSettings,
) -> Dict[str, Any]:
    validation_metrics = dict(best_candidate["validationMetrics"])
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
    divergence_summary = summarize_divergence_events(events)
    briefing = {
        "runScope": {
            "symbol": params.symbol,
            "iteration": params.iteration,
            "sliceStartTickId": slice_bounds["start_tick_id"],
            "sliceEndTickId": slice_bounds["end_tick_id"],
            "sliceRows": slice_bounds["slice_rows"],
            "sliceOffsetRows": slice_bounds["slice_offset_rows"],
            "caseCount": len(events),
            "trainCount": int(best_candidate["trainMetrics"].get("signalCount") or 0),
            "validationCount": int(best_candidate["validationMetrics"].get("signalCount") or 0),
        },
        "config": params.model_dump(),
        "bestCandidate": {
            "name": best_candidate["candidateName"],
            "rule": best_candidate["rule"],
            "side": best_candidate["rule"]["side"],
            "family": best_candidate["family"],
            "predicates": best_candidate["rule"]["predicates"],
            "trainMetrics": best_candidate["trainMetrics"],
            "validationMetrics": validation_metrics,
            "contrastSummary": best_candidate["contrastSummary"],
            "divergence": dict((best_candidate.get("rule") or {}).get("divergence") or {}),
        },
        "topCandidates": [
            {
                "name": item["candidateName"],
                "side": item["rule"]["side"],
                "family": item["family"],
                "predicates": item["rule"]["predicates"],
                "validationMetrics": item["validationMetrics"],
                "divergence": dict((item.get("rule") or {}).get("divergence") or {}),
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
        "divergenceSummary": divergence_summary,
        "verdictHint": verdict_hint,
    }
    headline = f"Divergence sweep on {params.symbol} for broker day {slice_bounds['study_brokerday']}"
    return {
        "headline": headline,
        "config": params.model_dump(),
        "sliceBounds": slice_bounds,
        "caseCount": len(events),
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
            "divergenceSummary": divergence_summary,
        },
        "mutationProposals": draft_mutations,
        "verdictHint": verdict_hint,
        "briefing": briefing,
    }


def build_empty_divergence_candidate(params: EntryResearchParameters) -> Dict[str, Any]:
    side = "short" if params.side_lock == "short" else "long"
    rule = build_divergence_rule(
        event_family="divergence_sweep",
        event_subtype="no_detectable_edge",
        indicator_name="none",
        side=side,
        signal_style="reversal",
    )
    validation_metrics = {
        "signalCount": 0,
        "hitCount": 0,
        "cleanHitCount": 0,
        "precision": 0.0,
        "cleanPrecision": 0.0,
        "entriesPerDay": 0.0,
        "medianHitSeconds": None,
        "avgMaxFavorable": 0.0,
        "avgMaxAdverse": 0.0,
        "walkForward": {"blocks": [], "precisionRange": 0.0},
        "walkForwardRange": 0.0,
        "bySession": {},
        "bySpread": {},
        "avgSpreadAtEvent": 0.0,
        "medianEventScore": 0.0,
    }
    return {
        "candidateName": str(rule["name"]),
        "family": "divergence_sweep",
        "rule": rule,
        "setupFingerprint": _rule_setup_fingerprint(rule, fallback_name=str(rule["name"])),
        "trainMetrics": dict(validation_metrics),
        "validationMetrics": dict(validation_metrics),
        "positives": [],
        "falsePositives": [],
        "patternSummary": {"positivePattern": {}, "falsePositivePattern": {}},
        "contrastSummary": {"positiveCount": 0, "falsePositiveCount": 0, "topFeatures": [], "sessionBuckets": {}, "spreadBuckets": {}},
    }


def summarize_divergence_events(events: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not events:
        return {"eventCount": 0, "subtypes": {}, "indicators": {}}
    subtypes: Dict[str, int] = {}
    indicators: Dict[str, int] = {}
    for event in events:
        subtype = str(event.get("eventSubtype") or "unknown")
        indicator = str(event.get("indicatorName") or "unknown")
        subtypes[subtype] = int(subtypes.get(subtype, 0)) + 1
        indicators[indicator] = int(indicators.get(indicator, 0)) + 1
    return {
        "eventCount": len(events),
        "subtypes": subtypes,
        "indicators": indicators,
    }


def build_divergence_event_rows(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for event in events:
        side = str(event["side"])
        label = dict(event["labels"][side])
        rows.append(
            {
                "setupFingerprint": event["setupFingerprint"],
                "fingerprint": event["eventFingerprint"],
                "brokerday": event["brokerday"],
                "symbol": None,
                "tickId": int(event["tickId"]),
                "timestamp": event["timestamp"],
                "eventFamily": event["eventFamily"],
                "eventSubtype": event["eventSubtype"],
                "indicatorName": event["indicatorName"],
                "side": side,
                "signalStyle": event["signalStyle"],
                "pivotMethod": event["pivotMethod"],
                "structurePack": event["structurePack"],
                "pivotLeftTickId": event["pivotLeftTickId"],
                "pivotRightTickId": event["pivotRightTickId"],
                "entryPrice": label["entryPrice"],
                "priceValue1": event["priceValue1"],
                "priceValue2": event["priceValue2"],
                "indicatorValue1": event.get("indicatorValue1"),
                "indicatorValue2": event.get("indicatorValue2"),
                "indicatorPayload": dict(event.get("indicatorPayload") or {}),
                "spreadAtEvent": label["spreadAtEntry"],
                "targetAmount": label["targetAmount"],
                "targetHit": label["hit2x"],
                "firstSideHit": label["firstSideHit"],
                "hitSeconds": label["hitSeconds"],
                "hitTicks": label["hitTicks"],
                "maxAdverse": label["maxAdverse"],
                "maxFavorable": label["maxFavorable"],
                "sessionBucket": event["sessionBucket"],
                "scalpQualified": label["scalpQualified"],
                "eventJson": {
                    "eventScore": event.get("eventScore"),
                    "rule": event.get("rule"),
                },
            }
        )
    return rows


def weighted_window_series(values: Sequence[float], *, window: int) -> List[Optional[float]]:
    series: List[Optional[float]] = []
    for index in range(len(values)):
        if index + 1 < window:
            series.append(None)
            continue
        series.append(weighted_mean(values[index - window + 1:index + 1]))
    return series


def ema_series(values: Sequence[float], *, period: int) -> List[Optional[float]]:
    if period <= 1:
        return [float(value) for value in values]
    series: List[Optional[float]] = []
    multiplier = 2.0 / (period + 1.0)
    ema_value: Optional[float] = None
    for index, value in enumerate(values):
        current = float(value)
        if ema_value is None:
            if index + 1 < period:
                series.append(None)
                continue
            ema_value = sum(float(item) for item in values[index - period + 1:index + 1]) / period
            series.append(ema_value)
            continue
        ema_value = ((current - ema_value) * multiplier) + ema_value
        series.append(ema_value)
    return series


def macd_series(values: Sequence[float], *, fast: int, slow: int, signal: int) -> tuple[List[Optional[float]], List[Optional[float]]]:
    fast_ema = ema_series(values, period=fast)
    slow_ema = ema_series(values, period=slow)
    macd_line: List[Optional[float]] = []
    for fast_value, slow_value in zip(fast_ema, slow_ema):
        if fast_value is None or slow_value is None:
            macd_line.append(None)
        else:
            macd_line.append(float(fast_value) - float(slow_value))
    signal_line = ema_series([float(value or 0.0) for value in macd_line], period=signal)
    macd_hist: List[Optional[float]] = []
    for line_value, signal_value in zip(macd_line, signal_line):
        if line_value is None or signal_value is None:
            macd_hist.append(None)
        else:
            macd_hist.append(float(line_value) - float(signal_value))
    return macd_line, macd_hist


def roc_series(values: Sequence[float], *, period: int) -> List[Optional[float]]:
    series: List[Optional[float]] = []
    for index, value in enumerate(values):
        if index < period:
            series.append(None)
            continue
        previous = float(values[index - period])
        if previous == 0:
            series.append(None)
            continue
        series.append(((float(value) - previous) / previous) * 100.0)
    return series


def rsi_series(values: Sequence[float], *, period: int) -> List[Optional[float]]:
    if len(values) < period + 1:
        return [None for _ in values]
    deltas = [float(values[index]) - float(values[index - 1]) for index in range(1, len(values))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [max(-delta, 0.0) for delta in deltas]
    series: List[Optional[float]] = [None]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for index in range(1, len(values)):
        if index < period:
            series.append(None)
            continue
        if index == period:
            gain = avg_gain
            loss = avg_loss
        else:
            gain = ((avg_gain * (period - 1)) + gains[index - 1]) / period
            loss = ((avg_loss * (period - 1)) + losses[index - 1]) / period
            avg_gain = gain
            avg_loss = loss
        if loss == 0:
            series.append(100.0)
            continue
        rs = gain / loss
        series.append(100.0 - (100.0 / (1.0 + rs)))
    return series


def divergence_indicator_noise(values: Sequence[Optional[float]]) -> float:
    deltas = [
        abs(float(current) - float(previous))
        for previous, current in zip(values, values[1:])
        if previous is not None and current is not None
    ]
    if not deltas:
        return 0.0
    return float(statistics.median(deltas))


def divergence_score(primary_move: float, primary_minimum: float, indicator_move: float, indicator_minimum: float) -> float:
    primary_score = float(primary_move) / max(float(primary_minimum), 1e-6)
    indicator_score = float(indicator_move) / max(float(indicator_minimum), 1e-6)
    return round(primary_score + indicator_score, 8)


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
    prev_8 = rows[index - 8]
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
    fast_short_momentum = tick.mid - prev_3.mid
    slow_short_momentum = tick.mid - prev_8.mid
    prev_fast_short_momentum = rows[index - 1].mid - rows[index - 4].mid
    prev_slow_short_momentum = rows[index - 1].mid - rows[index - 9].mid
    smoother_mid = rolling_mean([row.mid for row in rows[index - 7:index + 1]])
    prev_smoother_mid = rolling_mean([row.mid for row in rows[index - 8:index]])
    kal = weighted_mean([row.mid for row in rows[index - 4:index + 1]])
    k2 = weighted_mean([row.mid for row in rows[index - 8:index + 1]])
    prev_kal = weighted_mean([row.mid for row in rows[index - 5:index]])
    prev_k2 = weighted_mean([row.mid for row in rows[index - 9:index]])
    fast_imbalance = mean_sign(rows, start=index - 3, end=index)
    slow_imbalance = mean_sign(rows, start=index - 7, end=index)
    prev_fast_imbalance = mean_sign(rows, start=index - 4, end=index - 1)
    prev_slow_imbalance = mean_sign(rows, start=index - 8, end=index - 1)
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
        "fast_short_momentum": round(fast_short_momentum, 6),
        "slow_short_momentum": round(slow_short_momentum, 6),
        "price_smoother_gap": round(tick.mid - smoother_mid, 6),
        "kal": round(kal, 6),
        "k2": round(k2, 6),
        "fast_tick_imbalance": round(fast_imbalance, 6),
        "slow_tick_imbalance": round(slow_imbalance, 6),
        "short_momentum_cross_down": float(crossed_below(prev_fast_short_momentum, prev_slow_short_momentum, fast_short_momentum, slow_short_momentum)),
        "short_momentum_cross_up": float(crossed_above(prev_fast_short_momentum, prev_slow_short_momentum, fast_short_momentum, slow_short_momentum)),
        "price_cross_below_smoother": float(crossed_below(rows[index - 1].mid, prev_smoother_mid, tick.mid, smoother_mid)),
        "price_cross_above_smoother": float(crossed_above(rows[index - 1].mid, prev_smoother_mid, tick.mid, smoother_mid)),
        "kal_cross_below_k2": float(crossed_below(prev_kal, prev_k2, kal, k2)),
        "kal_cross_above_k2": float(crossed_above(prev_kal, prev_k2, kal, k2)),
        "imbalance_cross_down": float(crossed_below(prev_fast_imbalance, prev_slow_imbalance, fast_imbalance, slow_imbalance)),
        "imbalance_cross_up": float(crossed_above(prev_fast_imbalance, prev_slow_imbalance, fast_imbalance, slow_imbalance)),
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
    if not single_results and params.seed_rule is None:
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
    elif strategy == "crossover_confirmation":
        crossover_seed, crossover_rules = build_crossover_rules(params, filtered_train, single_results)
        if crossover_seed is not None and crossover_rules:
            baseline_result = evaluate_candidate(
                crossover_seed,
                filtered_train,
                filtered_validation,
                dedup_gap=dedup_gap,
                example_limit=settings.max_examples,
            )
            crossover_results = evaluate_rules(
                crossover_rules,
                filtered_train,
                filtered_validation,
                dedup_gap=dedup_gap,
                example_limit=settings.max_examples,
            )
            candidate_results.extend(select_crossover_candidates(crossover_results, baseline_result)[:candidate_limit])
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


def build_crossover_rules(
    params: EntryResearchParameters,
    train_cases: Sequence[Dict[str, Any]],
    single_results: Sequence[Dict[str, Any]],
) -> tuple[CandidateRule | None, List[CandidateRule]]:
    seed = params.seed_rule or derive_seed_from_results(single_results)
    if seed is None:
        return None, []
    base_predicates = [predicate.model_dump() if isinstance(predicate, PredicateSpec) else dict(predicate) for predicate in seed.predicates]
    baseline_rule = CandidateRule(
        name=f"{seed.side}:crossover_seed:{seed.name}",
        family="crossover_confirmation",
        side=seed.side,
        predicates=base_predicates,
    )
    direction = "down" if seed.side == "short" else "up"
    spread_values = [float(case["spread"]) for case in train_cases]
    if not spread_values:
        return baseline_rule, []
    low_spread = quantile_value(spread_values, 0.50)
    low_mid_spread = quantile_value(spread_values, 0.65)
    gate_specs = [
        ("short_momentum", f"short_momentum_cross_{direction}"),
        ("price_smoother", f"price_cross_{'below' if direction == 'down' else 'above'}_smoother"),
        ("kal_k2", f"kal_cross_{'below' if direction == 'down' else 'above'}_k2"),
        ("imbalance", f"imbalance_cross_{direction}"),
    ]
    rules: List[CandidateRule] = []
    for label, crossover_feature in gate_specs:
        rules.append(
            CandidateRule(
                name=f"{seed.side}:crossover:{label}:seed",
                family="crossover_confirmation",
                side=seed.side,
                predicates=base_predicates + [{"feature": crossover_feature, "operator": ">=", "threshold": 0.5}],
            )
        )
        rules.append(
            CandidateRule(
                name=f"{seed.side}:crossover:{label}:lowmid_spread",
                family="crossover_confirmation",
                side=seed.side,
                predicates=base_predicates + [
                    {"feature": crossover_feature, "operator": ">=", "threshold": 0.5},
                    {"feature": "spread", "operator": "<=", "threshold": low_mid_spread},
                ],
            )
        )
        rules.append(
            CandidateRule(
                name=f"{seed.side}:crossover:{label}:low_spread",
                family="crossover_confirmation",
                side=seed.side,
                predicates=base_predicates + [
                    {"feature": crossover_feature, "operator": ">=", "threshold": 0.5},
                    {"feature": "spread", "operator": "<=", "threshold": low_spread},
                ],
            )
        )
    return baseline_rule, rules[:12]


def select_crossover_candidates(candidates: Sequence[Dict[str, Any]], baseline_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    baseline_metrics = dict(baseline_result.get("validationMetrics") or {})
    baseline_clean_precision = float(baseline_metrics.get("cleanPrecision") or 0.0)
    baseline_entries_per_day = float(baseline_metrics.get("entriesPerDay") or 0.0)
    baseline_median_hit_seconds = metric_or_inf(baseline_metrics.get("medianHitSeconds"))
    accepted: List[Dict[str, Any]] = []
    for candidate in candidates:
        validation = dict(candidate.get("validationMetrics") or {})
        clean_precision = float(validation.get("cleanPrecision") or 0.0)
        entries_per_day = float(validation.get("entriesPerDay") or 0.0)
        median_hit_seconds = metric_or_inf(validation.get("medianHitSeconds"))
        precision_gain = clean_precision - baseline_clean_precision
        noise_reduction = baseline_entries_per_day - entries_per_day
        spread_stats = crossover_spread_preference(validation)
        materially_improves_precision = precision_gain >= 0.04
        materially_reduces_noise = noise_reduction >= max(0.10, baseline_entries_per_day * 0.15)
        follow_through_ok = baseline_median_hit_seconds == math.inf or median_hit_seconds <= (baseline_median_hit_seconds * 1.10)
        if clean_precision < 0.333:
            continue
        if not (materially_improves_precision or materially_reduces_noise):
            continue
        if not follow_through_ok and not materially_improves_precision:
            continue
        if spread_stats["lowMidShare"] < 0.50 and precision_gain < 0.05:
            continue
        validation["baselineComparison"] = {
            "baselineCandidate": baseline_result.get("candidateName"),
            "baselineCleanPrecision": round(baseline_clean_precision, 6),
            "baselineEntriesPerDay": round(baseline_entries_per_day, 4),
            "baselineMedianHitSeconds": None if baseline_median_hit_seconds == math.inf else round(baseline_median_hit_seconds, 6),
            "cleanPrecisionGain": round(precision_gain, 6),
            "entryReductionPerDay": round(noise_reduction, 4),
            "medianHitSecondsDelta": None if median_hit_seconds == math.inf or baseline_median_hit_seconds == math.inf else round(baseline_median_hit_seconds - median_hit_seconds, 6),
            "materiallyImprovesPrecision": materially_improves_precision,
            "materiallyReducesNoise": materially_reduces_noise,
        }
        validation["spreadPreference"] = {
            "lowMidShare": round(spread_stats["lowMidShare"], 6),
            "lowPrecision": round(spread_stats["lowPrecision"], 6),
            "midPrecision": round(spread_stats["midPrecision"], 6),
            "highPrecision": round(spread_stats["highPrecision"], 6),
        }
        candidate["validationMetrics"] = validation
        candidate["rankingBonus"] = {
            "spreadPreference": round(spread_stats["lowMidShare"], 6),
            "followThroughScore": 0.0 if median_hit_seconds == math.inf else round(1.0 / (1.0 + median_hit_seconds), 9),
            "noiseReduction": round(max(0.0, noise_reduction), 6),
            "precisionGain": round(max(0.0, precision_gain), 6),
        }
        accepted.append(candidate)
    accepted.sort(key=candidate_sort_key, reverse=True)
    return accepted


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
            "rule": best_candidate["rule"],
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
                "setupFingerprint": candidate.get("setupFingerprint"),
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


def candidate_sort_key(item: Dict[str, Any]) -> tuple[float, float, float, float, float, float, float, int]:
    validation = item["validationMetrics"]
    train = item["trainMetrics"]
    ranking_bonus = item.get("rankingBonus") or {}
    return (
        float(validation["cleanPrecision"]),
        float(ranking_bonus.get("spreadPreference") or 0.0),
        float(ranking_bonus.get("followThroughScore") or 0.0),
        float(ranking_bonus.get("noiseReduction") or 0.0),
        float(ranking_bonus.get("precisionGain") or 0.0),
        float(validation["precision"]),
        -float(validation.get("walkForwardRange") or 0.0),
        int(train["signalCount"]),
    )


def rolling_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(float(value) for value in values) / len(values)


def weighted_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    weights = list(range(1, len(values) + 1))
    denominator = sum(weights)
    if denominator <= 0:
        return 0.0
    return sum(float(value) * weight for value, weight in zip(values, weights)) / denominator


def mean_sign(rows: Sequence[TickRow], *, start: int, end: int) -> float:
    flags = [sign(rows[pos].mid - rows[pos - 1].mid) for pos in range(start, end + 1)]
    if not flags:
        return 0.0
    return sum(flags) / len(flags)


def crossed_below(previous_fast: float, previous_slow: float, current_fast: float, current_slow: float) -> bool:
    return previous_fast >= previous_slow and current_fast < current_slow


def crossed_above(previous_fast: float, previous_slow: float, current_fast: float, current_slow: float) -> bool:
    return previous_fast <= previous_slow and current_fast > current_slow


def metric_or_inf(value: Any) -> float:
    if value is None:
        return math.inf
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.inf


def crossover_spread_preference(validation: Dict[str, Any]) -> Dict[str, float]:
    spread_buckets = validation.get("bySpread") or {}
    low_bucket = spread_buckets.get("low") or {}
    mid_bucket = spread_buckets.get("mid") or {}
    high_bucket = spread_buckets.get("high") or {}
    low_signals = float(low_bucket.get("signalCount") or 0.0)
    mid_signals = float(mid_bucket.get("signalCount") or 0.0)
    high_signals = float(high_bucket.get("signalCount") or 0.0)
    total_signals = max(1.0, low_signals + mid_signals + high_signals)
    return {
        "lowMidShare": (low_signals + mid_signals) / total_signals,
        "lowPrecision": float(low_bucket.get("cleanPrecision") or 0.0),
        "midPrecision": float(mid_bucket.get("cleanPrecision") or 0.0),
        "highPrecision": float(high_bucket.get("cleanPrecision") or 0.0),
    }


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
