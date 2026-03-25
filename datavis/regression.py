from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence


MIN_REGRESSION_POINTS = 3
MIN_ANALYSIS_WINDOW = 20
EPSILON = 1e-9


def iso_utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def clamp_window(value: int, available: int, minimum: int = MIN_ANALYSIS_WINDOW) -> int:
    if available <= 0:
        return 0
    floor = min(minimum, available)
    return max(floor, min(int(value), available))


def build_regression_payload(
    rows: Sequence[Dict[str, Any]],
    *,
    series: str,
    mode: str,
    visible_window: int,
    fast_window_ticks: int,
    slow_window_ticks: int,
) -> Dict[str, Any]:
    computed_at = iso_utc_now()
    if not rows:
        return {
            "computedAt": computed_at,
            "series": series,
            "mode": mode,
            "visibleWindowTicks": visible_window,
            "fastWindowTicks": 0,
            "slowWindowTicks": 0,
            "window": {
                "rowCount": 0,
                "firstId": None,
                "lastId": None,
                "firstTimestamp": None,
                "lastTimestamp": None,
                "durationMs": 0,
                "priceMin": None,
                "priceMax": None,
                "priceRange": 0.0,
                "currentPrice": None,
            },
            "regressions": {
                "fast": empty_regression("fast"),
                "slow": empty_regression("slow"),
            },
            "relationship": {
                "slopeDifference": 0.0,
                "slopeRatio": None,
                "angleDifferenceDeg": 0.0,
                "directionalAgreement": 0,
                "currentFastSlowDistance": 0.0,
                "alignmentState": "empty",
                "fastAcceleration": 0.0,
                "fastAccelerating": False,
                "fastDominanceRatio": None,
            },
            "breakPressure": {
                "recentResidualWindowTicks": 0,
                "recentResidualSignImbalance": 0.0,
                "recentResidualRunLength": 0,
                "recentPositiveResidualRatio": 0.0,
                "recentNegativeResidualRatio": 0.0,
                "slowFitDeterioration": 0.0,
                "slowFitDeteriorationPct": 0.0,
                "fastSlowDisagreementScore": 0.0,
                "bestCandidateSplitTickId": None,
                "bestTwoLineImprovementPct": 0.0,
                "bestTwoLineLeftSse": None,
                "bestTwoLineRightSse": None,
                "bestTwoLineTotalSse": None,
                "breakPressureScore": 0.0,
                "pressureState": "empty",
                "confidenceState": "low",
                "splitProbeWindowTicks": 0,
                "splitProbeMinSegmentTicks": 0,
            },
        }

    prices = [select_price(row, series) for row in rows]
    timestamps_ms = [int(row["timestampMs"]) for row in rows]
    available = len(rows)
    fast_count = clamp_window(fast_window_ticks, available)
    slow_count = clamp_window(slow_window_ticks, available)

    fast = compute_regression(rows, prices, fast_count, name="fast")
    slow = compute_regression(rows, prices, slow_count, name="slow")
    relationship = build_relationship_metrics(prices, fast, slow)
    break_pressure = build_break_pressure(rows, prices, fast, slow, relationship, fast_count, slow_count)

    window_payload = {
        "rowCount": available,
        "firstId": rows[0]["id"],
        "lastId": rows[-1]["id"],
        "firstTimestamp": rows[0]["timestamp"],
        "lastTimestamp": rows[-1]["timestamp"],
        "durationMs": max(0, timestamps_ms[-1] - timestamps_ms[0]),
        "priceMin": min(prices),
        "priceMax": max(prices),
        "priceRange": max(prices) - min(prices),
        "currentPrice": prices[-1],
    }

    return {
        "computedAt": computed_at,
        "series": series,
        "mode": mode,
        "visibleWindowTicks": available,
        "fastWindowTicks": fast_count,
        "slowWindowTicks": slow_count,
        "window": window_payload,
        "regressions": {
            "fast": fast,
            "slow": slow,
        },
        "relationship": relationship,
        "breakPressure": break_pressure,
    }


def select_price(row: Dict[str, Any], series: str) -> float:
    if series == "ask":
        value = row.get("ask")
    elif series == "bid":
        value = row.get("bid")
    else:
        value = row.get("mid")
        if value is None:
            bid = row.get("bid")
            ask = row.get("ask")
            value = (float(bid) + float(ask)) / 2.0 if bid is not None and ask is not None else row.get("price")

    if value is None:
        raise ValueError("Missing price series value for row id {0}".format(row.get("id")))
    return float(value)


def empty_regression(name: str) -> Dict[str, Any]:
    return {
        "name": name,
        "slope": 0.0,
        "intercept": 0.0,
        "angleDeg": 0.0,
        "r2": None,
        "mae": None,
        "residualStd": None,
        "sse": None,
        "tickCount": 0,
        "durationMs": 0,
        "priceChange": 0.0,
        "efficiency": 0.0,
        "windowStartTickId": None,
        "windowEndTickId": None,
        "windowStartTimestamp": None,
        "windowEndTimestamp": None,
        "windowStartIndex": None,
        "windowEndIndex": None,
        "currentFittedValue": None,
        "currentResidual": None,
        "fittedValues": [],
        "residuals": [],
        "windowIndices": [],
    }


def compute_regression(
    rows: Sequence[Dict[str, Any]],
    prices: Sequence[float],
    fit_count: int,
    *,
    name: str,
) -> Dict[str, Any]:
    total_count = len(rows)
    if total_count == 0 or fit_count <= 0:
        return empty_regression(name)

    fit_start_index = total_count - fit_count
    fit_end_index = total_count - 1
    xs = [float(index) for index in range(fit_start_index, total_count)]
    ys = [float(prices[index]) for index in range(fit_start_index, total_count)]
    regression = regression_from_xy(xs, ys)

    fitted_values = [regression["intercept"] + regression["slope"] * float(index) for index in range(total_count)]
    residuals = [float(prices[index]) - fitted_values[index] for index in range(total_count)]

    window_start = rows[fit_start_index]
    window_end = rows[-1]
    duration_ms = max(0, int(window_end["timestampMs"]) - int(window_start["timestampMs"]))
    price_change = ys[-1] - ys[0]
    travel = sum(abs(ys[index] - ys[index - 1]) for index in range(1, len(ys)))
    efficiency = abs(price_change) / travel if travel > EPSILON else 1.0

    return {
        "name": name,
        "slope": regression["slope"],
        "intercept": regression["intercept"],
        "angleDeg": math.degrees(math.atan(regression["slope"])),
        "r2": regression["r2"],
        "mae": regression["mae"],
        "residualStd": regression["residualStd"],
        "sse": regression["sse"],
        "tickCount": fit_count,
        "durationMs": duration_ms,
        "priceChange": price_change,
        "efficiency": efficiency,
        "windowStartTickId": window_start["id"],
        "windowEndTickId": window_end["id"],
        "windowStartTimestamp": window_start["timestamp"],
        "windowEndTimestamp": window_end["timestamp"],
        "windowStartIndex": fit_start_index,
        "windowEndIndex": fit_end_index,
        "currentFittedValue": fitted_values[-1],
        "currentResidual": residuals[-1],
        "fittedValues": fitted_values,
        "residuals": residuals,
        "windowIndices": [fit_start_index, fit_end_index],
    }


def regression_from_xy(xs: Sequence[float], ys: Sequence[float]) -> Dict[str, Optional[float]]:
    count = len(xs)
    if count == 0:
        return {"slope": 0.0, "intercept": 0.0, "r2": None, "mae": None, "residualStd": None, "sse": None}

    mean_y = sum(ys) / float(count)
    if count == 1:
        return {"slope": 0.0, "intercept": mean_y, "r2": 1.0, "mae": 0.0, "residualStd": 0.0, "sse": 0.0}

    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xx = sum(value * value for value in xs)
    sum_xy = sum(x_value * y_value for x_value, y_value in zip(xs, ys))
    denom = (count * sum_xx) - (sum_x * sum_x)

    if abs(denom) <= EPSILON:
        slope = 0.0
        intercept = mean_y
    else:
        slope = ((count * sum_xy) - (sum_x * sum_y)) / denom
        intercept = (sum_y - (slope * sum_x)) / float(count)

    fitted = [intercept + (slope * value) for value in xs]
    residuals = [actual - estimate for actual, estimate in zip(ys, fitted)]
    sse = sum(error * error for error in residuals)
    mae = sum(abs(error) for error in residuals) / float(count)
    residual_std = math.sqrt(sse / float(count))
    sst = sum((value - mean_y) * (value - mean_y) for value in ys)
    if sst <= EPSILON:
        r2 = 1.0 if sse <= EPSILON else 0.0
    else:
        r2 = 1.0 - (sse / sst)

    return {
        "slope": slope,
        "intercept": intercept,
        "r2": r2,
        "mae": mae,
        "residualStd": residual_std,
        "sse": sse,
    }


def build_relationship_metrics(
    prices: Sequence[float],
    fast: Dict[str, Any],
    slow: Dict[str, Any],
) -> Dict[str, Any]:
    price_range = (max(prices) - min(prices)) if prices else 0.0
    flat_threshold = max(0.0005, price_range / max(len(prices), 1) / 10.0)

    fast_slope = float(fast["slope"])
    slow_slope = float(slow["slope"])
    slope_difference = fast_slope - slow_slope
    slope_ratio = None if abs(slow_slope) <= EPSILON else fast_slope / slow_slope
    angle_difference = float(fast["angleDeg"]) - float(slow["angleDeg"])
    distance = float(fast["currentFittedValue"] or 0.0) - float(slow["currentFittedValue"] or 0.0)

    fast_sign = slope_sign(fast_slope, flat_threshold)
    slow_sign = slope_sign(slow_slope, flat_threshold)
    if fast_sign == 0 and slow_sign == 0:
        alignment_state = "near-flat"
    elif fast_sign == 0:
        alignment_state = "fast-flat"
    elif slow_sign == 0:
        alignment_state = "slow-flat"
    elif fast_sign == slow_sign:
        alignment_state = "aligned"
    else:
        alignment_state = "opposed"

    directional_agreement = 0 if 0 in (fast_sign, slow_sign) else (1 if fast_sign == slow_sign else -1)
    dominance_ratio = None if abs(slow_slope) <= flat_threshold else abs(fast_slope) / max(abs(slow_slope), EPSILON)
    fast_accelerating = directional_agreement == 1 and abs(fast_slope) > (abs(slow_slope) * 1.2)

    return {
        "slopeDifference": slope_difference,
        "slopeRatio": slope_ratio,
        "angleDifferenceDeg": angle_difference,
        "directionalAgreement": directional_agreement,
        "currentFastSlowDistance": distance,
        "alignmentState": alignment_state,
        "fastAcceleration": slope_difference,
        "fastAccelerating": fast_accelerating,
        "fastDominanceRatio": dominance_ratio,
    }


def slope_sign(value: float, flat_threshold: float) -> int:
    if abs(value) <= flat_threshold:
        return 0
    return 1 if value > 0 else -1


def build_break_pressure(
    rows: Sequence[Dict[str, Any]],
    prices: Sequence[float],
    fast: Dict[str, Any],
    slow: Dict[str, Any],
    relationship: Dict[str, Any],
    fast_window_ticks: int,
    slow_window_ticks: int,
) -> Dict[str, Any]:
    recent_window = min(len(rows), max(MIN_ANALYSIS_WINDOW, max(10, fast_window_ticks // 2)))
    recent_residuals = list(slow["residuals"][-recent_window:]) if recent_window else []
    residual_epsilon = max(0.01, float(slow["residualStd"] or 0.0) * 0.2)
    signs = [residual_sign(value, residual_epsilon) for value in recent_residuals]
    signed_sum = sum(signs)
    sign_imbalance = (signed_sum / float(len(signs))) if signs else 0.0
    positive_ratio = (sum(1 for value in signs if value > 0) / float(len(signs))) if signs else 0.0
    negative_ratio = (sum(1 for value in signs if value < 0) / float(len(signs))) if signs else 0.0
    run_length = trailing_residual_run(signs)

    recent_mae = average_absolute(recent_residuals)
    prior_window = recent_window
    prior_residuals = []
    if len(slow["residuals"]) > recent_window:
        prior_residuals = list(slow["residuals"][-(recent_window + prior_window):-recent_window])
    prior_mae = average_absolute(prior_residuals)
    deterioration = recent_mae - prior_mae
    deterioration_pct = 0.0 if prior_mae <= EPSILON else deterioration / prior_mae

    disagreement_score = score_disagreement(fast, slow, relationship)

    split_probe_window = min(len(rows), max(slow_window_ticks, fast_window_ticks * 2))
    split_probe = two_line_improvement_probe(rows, prices, split_probe_window)
    split_score = min(1.0, max(0.0, float(split_probe["improvementPct"] or 0.0)) / 0.35)
    streak_score = min(1.0, run_length / max(5.0, recent_window * 0.35))
    deterioration_score = min(1.0, max(0.0, deterioration_pct))
    imbalance_score = min(1.0, abs(sign_imbalance))

    break_pressure_score = 100.0 * (
        (0.22 * imbalance_score)
        + (0.16 * streak_score)
        + (0.20 * deterioration_score)
        + (0.22 * (disagreement_score / 100.0))
        + (0.20 * split_score)
    )

    if break_pressure_score >= 80.0:
        pressure_state = "break-risk"
    elif break_pressure_score >= 60.0:
        pressure_state = "elevated"
    elif break_pressure_score >= 35.0:
        pressure_state = "building"
    else:
        pressure_state = "calm"

    evidence_count = sum(
        1
        for value in (
            imbalance_score >= 0.4,
            streak_score >= 0.35,
            deterioration_score >= 0.2,
            disagreement_score >= 45.0,
            split_score >= 0.2,
        )
        if value
    )
    if evidence_count >= 4:
        confidence_state = "high"
    elif evidence_count >= 2:
        confidence_state = "moderate"
    else:
        confidence_state = "low"

    return {
        "recentResidualWindowTicks": recent_window,
        "recentResidualSignImbalance": sign_imbalance,
        "recentResidualRunLength": run_length,
        "recentPositiveResidualRatio": positive_ratio,
        "recentNegativeResidualRatio": negative_ratio,
        "slowFitDeterioration": deterioration,
        "slowFitDeteriorationPct": deterioration_pct,
        "fastSlowDisagreementScore": disagreement_score,
        "bestCandidateSplitTickId": split_probe["candidateSplitTickId"],
        "bestTwoLineImprovementPct": split_probe["improvementPct"],
        "bestTwoLineLeftSse": split_probe["leftSse"],
        "bestTwoLineRightSse": split_probe["rightSse"],
        "bestTwoLineTotalSse": split_probe["twoLineSse"],
        "breakPressureScore": break_pressure_score,
        "pressureState": pressure_state,
        "confidenceState": confidence_state,
        "splitProbeWindowTicks": split_probe["probeWindowTicks"],
        "splitProbeMinSegmentTicks": split_probe["minSegmentTicks"],
    }


def residual_sign(value: float, epsilon: float) -> int:
    if value > epsilon:
        return 1
    if value < -epsilon:
        return -1
    return 0


def trailing_residual_run(signs: Sequence[int]) -> int:
    run_length = 0
    active_sign = 0
    for value in reversed(signs):
        if value == 0:
            continue
        if active_sign == 0:
            active_sign = value
        if value != active_sign:
            break
        run_length += 1
    return run_length


def average_absolute(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(abs(value) for value in values) / float(len(values))


def score_disagreement(fast: Dict[str, Any], slow: Dict[str, Any], relationship: Dict[str, Any]) -> float:
    slow_slope = abs(float(slow["slope"]))
    slope_gap = min(1.0, abs(float(relationship["slopeDifference"])) / max(slow_slope, 0.0005))
    distance_gap = min(1.0, abs(float(relationship["currentFastSlowDistance"])) / max(float(slow["residualStd"] or 0.02) * 2.0, 0.05))
    direction_gap = 1.0 if relationship["directionalAgreement"] == -1 else (0.4 if relationship["alignmentState"] in {"fast-flat", "slow-flat"} else 0.0)
    angle_gap = min(1.0, abs(float(relationship["angleDifferenceDeg"])) / 35.0)
    return 100.0 * ((0.38 * slope_gap) + (0.28 * distance_gap) + (0.22 * direction_gap) + (0.12 * angle_gap))


def two_line_improvement_probe(
    rows: Sequence[Dict[str, Any]],
    prices: Sequence[float],
    probe_window_ticks: int,
) -> Dict[str, Any]:
    if not rows or probe_window_ticks <= 0:
        return {
            "candidateSplitTickId": None,
            "improvementPct": 0.0,
            "leftSse": None,
            "rightSse": None,
            "twoLineSse": None,
            "probeWindowTicks": 0,
            "minSegmentTicks": 0,
        }

    probe_window_ticks = min(len(rows), probe_window_ticks)
    probe_start = len(rows) - probe_window_ticks
    probe_prices = [float(value) for value in prices[probe_start:]]
    min_segment_ticks = min(max(MIN_ANALYSIS_WINDOW, probe_window_ticks // 6), probe_window_ticks // 2)
    if probe_window_ticks < (min_segment_ticks * 2):
        return {
            "candidateSplitTickId": None,
            "improvementPct": 0.0,
            "leftSse": None,
            "rightSse": None,
            "twoLineSse": None,
            "probeWindowTicks": probe_window_ticks,
            "minSegmentTicks": min_segment_ticks,
        }

    prefix = build_prefix_stats(probe_prices)
    single = stats_regression(prefix, 0, probe_window_ticks)
    best_split_index = None
    best_left = None
    best_right = None
    best_sse = None

    for split in range(min_segment_ticks, probe_window_ticks - min_segment_ticks + 1):
        left = stats_regression(prefix, 0, split)
        right = stats_regression(prefix, split, probe_window_ticks)
        combined_sse = left["sse"] + right["sse"]
        if best_sse is None or combined_sse < best_sse:
            best_sse = combined_sse
            best_split_index = split
            best_left = left
            best_right = right

    baseline_sse = float(single["sse"])
    if best_sse is None or baseline_sse <= EPSILON:
        improvement_pct = 0.0
    else:
        improvement_pct = max(0.0, (baseline_sse - best_sse) / baseline_sse)

    candidate_tick_id = None
    if best_split_index is not None and best_split_index < probe_window_ticks:
        candidate_tick_id = rows[probe_start + best_split_index]["id"]

    return {
        "candidateSplitTickId": candidate_tick_id,
        "improvementPct": improvement_pct,
        "leftSse": None if best_left is None else best_left["sse"],
        "rightSse": None if best_right is None else best_right["sse"],
        "twoLineSse": best_sse,
        "probeWindowTicks": probe_window_ticks,
        "minSegmentTicks": min_segment_ticks,
    }


def build_prefix_stats(values: Sequence[float]) -> Dict[str, List[float]]:
    sum_x = [0.0]
    sum_y = [0.0]
    sum_xx = [0.0]
    sum_xy = [0.0]
    sum_yy = [0.0]

    for index, value in enumerate(values):
        x_value = float(index)
        y_value = float(value)
        sum_x.append(sum_x[-1] + x_value)
        sum_y.append(sum_y[-1] + y_value)
        sum_xx.append(sum_xx[-1] + (x_value * x_value))
        sum_xy.append(sum_xy[-1] + (x_value * y_value))
        sum_yy.append(sum_yy[-1] + (y_value * y_value))

    return {
        "sumX": sum_x,
        "sumY": sum_y,
        "sumXX": sum_xx,
        "sumXY": sum_xy,
        "sumYY": sum_yy,
    }


def stats_regression(prefix: Dict[str, List[float]], start: int, end: int) -> Dict[str, float]:
    count = end - start
    if count <= 0:
        return {"slope": 0.0, "intercept": 0.0, "sse": 0.0}

    sum_x = prefix["sumX"][end] - prefix["sumX"][start]
    sum_y = prefix["sumY"][end] - prefix["sumY"][start]
    sum_xx = prefix["sumXX"][end] - prefix["sumXX"][start]
    sum_xy = prefix["sumXY"][end] - prefix["sumXY"][start]
    sum_yy = prefix["sumYY"][end] - prefix["sumYY"][start]
    denom = (count * sum_xx) - (sum_x * sum_x)

    if abs(denom) <= EPSILON:
        slope = 0.0
        intercept = sum_y / float(count)
    else:
        slope = ((count * sum_xy) - (sum_x * sum_y)) / denom
        intercept = (sum_y - (slope * sum_x)) / float(count)

    sse = (
        sum_yy
        + (count * intercept * intercept)
        + (slope * slope * sum_xx)
        + (2.0 * intercept * slope * sum_x)
        - (2.0 * intercept * sum_y)
        - (2.0 * slope * sum_xy)
    )
    if sse < 0.0 and abs(sse) <= 1e-7:
        sse = 0.0

    return {
        "slope": slope,
        "intercept": intercept,
        "sse": sse,
    }
