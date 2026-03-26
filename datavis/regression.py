from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple


MIN_ANALYSIS_WINDOW = 20
MIN_REGRESSION_POINTS = 3
MIN_POLYNOMIAL_ORDER = 1
MAX_POLYNOMIAL_ORDER = 5
DEFAULT_FAST_POLY_ORDER = 2
DEFAULT_SLOW_POLY_ORDER = 3
DEFAULT_MOVE_QUALITY_SIGNAL_THRESHOLD = 68.0
DEFAULT_TUNING_SIGNAL_THRESHOLD = 72.0
DEFAULT_TUNING_FAST_WINDOWS = (120, 180, 240)
DEFAULT_TUNING_SLOW_WINDOWS = (720, 960, 1200)
DEFAULT_TUNING_FAST_ORDERS = (2, 3)
DEFAULT_TUNING_SLOW_ORDERS = (2, 3)
EPSILON = 1e-9


def iso_utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def clamp_window(value: int, available: int, minimum: int = MIN_ANALYSIS_WINDOW) -> int:
    if available <= 0:
        return 0
    floor = min(minimum, available)
    return max(floor, min(int(value), available))


def clamp_polynomial_order(requested_order: int, fit_count: int) -> int:
    if fit_count <= 1:
        return 0
    return max(
        MIN_POLYNOMIAL_ORDER,
        min(int(requested_order), min(MAX_POLYNOMIAL_ORDER, fit_count - 1)),
    )


def build_regression_payload(
    rows: Sequence[Dict[str, Any]],
    *,
    series: str,
    mode: str,
    visible_window: int,
    fast_window_ticks: int,
    slow_window_ticks: int,
    fast_poly_order: int = DEFAULT_FAST_POLY_ORDER,
    slow_poly_order: int = DEFAULT_SLOW_POLY_ORDER,
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
            "fastPolyOrder": 0,
            "slowPolyOrder": 0,
            "window": empty_window_payload(),
            "regressions": {
                "fast": empty_regression("fast"),
                "slow": empty_regression("slow"),
            },
            "relationship": empty_linear_relationship(),
            "breakPressure": empty_break_pressure(),
            "polynomials": {
                "fast": empty_polynomial("fast", fast_poly_order),
                "slow": empty_polynomial("slow", slow_poly_order),
            },
            "polyRelationship": empty_poly_relationship(),
            "moveQuality": empty_move_quality(),
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

    fast_poly, slow_poly, poly_relationship, move_quality = build_polynomial_analysis(
        rows=rows,
        prices=prices,
        fast_window_ticks=fast_count,
        slow_window_ticks=slow_count,
        fast_poly_order=fast_poly_order,
        slow_poly_order=slow_poly_order,
        break_pressure_score=break_pressure["breakPressureScore"],
        signal_threshold=DEFAULT_MOVE_QUALITY_SIGNAL_THRESHOLD,
    )

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
        "fastPolyOrder": fast_poly["order"],
        "slowPolyOrder": slow_poly["order"],
        "window": window_payload,
        "regressions": {
            "fast": fast,
            "slow": slow,
        },
        "relationship": relationship,
        "breakPressure": break_pressure,
        "polynomials": {
            "fast": fast_poly,
            "slow": slow_poly,
        },
        "polyRelationship": poly_relationship,
        "moveQuality": move_quality,
    }


def build_polynomial_analysis(
    *,
    rows: Sequence[Dict[str, Any]],
    prices: Sequence[float],
    fast_window_ticks: int,
    slow_window_ticks: int,
    fast_poly_order: int,
    slow_poly_order: int,
    break_pressure_score: Optional[float] = None,
    signal_threshold: float = DEFAULT_MOVE_QUALITY_SIGNAL_THRESHOLD,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    fast_poly = compute_polynomial_fit(
        rows,
        prices,
        fast_window_ticks,
        requested_order=fast_poly_order,
        name="fast",
    )
    slow_poly = compute_polynomial_fit(
        rows,
        prices,
        slow_window_ticks,
        requested_order=slow_poly_order,
        name="slow",
    )
    poly_relationship = build_polynomial_relationship(prices, fast_poly, slow_poly)
    move_quality = build_move_quality(
        prices=prices,
        fast_poly=fast_poly,
        slow_poly=slow_poly,
        poly_relationship=poly_relationship,
        break_pressure_score=break_pressure_score,
        signal_threshold=signal_threshold,
    )
    return fast_poly, slow_poly, poly_relationship, move_quality


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


def empty_window_payload() -> Dict[str, Any]:
    return {
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
    }


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


def empty_linear_relationship() -> Dict[str, Any]:
    return {
        "slopeDifference": 0.0,
        "slopeRatio": None,
        "angleDifferenceDeg": 0.0,
        "directionalAgreement": 0,
        "currentFastSlowDistance": 0.0,
        "alignmentState": "empty",
        "fastAcceleration": 0.0,
        "fastAccelerating": False,
        "fastDominanceRatio": None,
    }


def empty_break_pressure() -> Dict[str, Any]:
    return {
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
    }


def empty_polynomial(name: str, requested_order: int) -> Dict[str, Any]:
    return {
        "name": name,
        "requestedOrder": int(requested_order),
        "order": 0,
        "coefficients": [],
        "slope": 0.0,
        "curvature": 0.0,
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
        "normalizedDistance": 0.0,
        "distanceScale": None,
        "fittedValues": [],
        "residuals": [],
        "windowIndices": [],
    }


def empty_poly_relationship() -> Dict[str, Any]:
    return {
        "direction": "flat",
        "slopeAgreement": "empty",
        "curvatureAgreement": "empty",
        "fittedSpread": 0.0,
        "slopeSpread": 0.0,
        "residualCompressionRatio": None,
        "residualRegime": "empty",
        "alignedWithBoth": False,
        "stretchState": "empty",
    }


def empty_move_quality() -> Dict[str, Any]:
    return {
        "score": 0.0,
        "state": "empty",
        "direction": "flat",
        "candidate": False,
        "signalThreshold": DEFAULT_MOVE_QUALITY_SIGNAL_THRESHOLD,
        "summary": "No rows available.",
        "components": {},
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


def compute_polynomial_fit(
    rows: Sequence[Dict[str, Any]],
    prices: Sequence[float],
    fit_count: int,
    *,
    requested_order: int,
    name: str,
) -> Dict[str, Any]:
    total_count = len(rows)
    empty = empty_polynomial(name, requested_order)
    if total_count == 0 or fit_count <= 1:
        return empty

    fit_start_index = total_count - fit_count
    fit_end_index = total_count - 1
    xs = [float(index) for index in range(fit_count)]
    ys = [float(prices[index]) for index in range(fit_start_index, total_count)]

    solved = polynomial_regression_from_xy(xs, ys, requested_order=requested_order)
    actual_order = solved["order"]
    coefficients = solved["coefficients"]
    if not coefficients:
        return empty

    window_fit = [evaluate_polynomial(coefficients, x_value) for x_value in xs]
    fitted_values = [None] * fit_start_index + window_fit
    residuals = [None] * fit_start_index + [actual - fitted for actual, fitted in zip(ys, window_fit)]

    sse = sum(error * error for error in residuals[fit_start_index:] if error is not None)
    mae = average_absolute([error for error in residuals[fit_start_index:] if error is not None])
    residual_std = math.sqrt(sse / float(fit_count))
    mean_y = sum(ys) / float(fit_count)
    sst = sum((value - mean_y) * (value - mean_y) for value in ys)
    if sst <= EPSILON:
        r2 = 1.0 if sse <= EPSILON else 0.0
    else:
        r2 = 1.0 - (sse / sst)

    current_x = xs[-1]
    current_fitted = window_fit[-1]
    current_residual = ys[-1] - current_fitted
    distance_scale = normalisation_scale(ys, residual_std)
    slope = evaluate_polynomial_derivative(coefficients, current_x, derivative_order=1)
    curvature = evaluate_polynomial_derivative(coefficients, current_x, derivative_order=2)

    window_start = rows[fit_start_index]
    window_end = rows[-1]
    duration_ms = max(0, int(window_end["timestampMs"]) - int(window_start["timestampMs"]))
    price_change = ys[-1] - ys[0]
    travel = sum(abs(ys[index] - ys[index - 1]) for index in range(1, len(ys)))
    efficiency = abs(price_change) / travel if travel > EPSILON else 1.0

    return {
        "name": name,
        "requestedOrder": int(requested_order),
        "order": actual_order,
        "coefficients": coefficients,
        "slope": slope,
        "curvature": curvature,
        "r2": r2,
        "mae": mae,
        "residualStd": residual_std,
        "sse": sse,
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
        "currentFittedValue": current_fitted,
        "currentResidual": current_residual,
        "normalizedDistance": 0.0 if distance_scale <= EPSILON else current_residual / distance_scale,
        "distanceScale": distance_scale,
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


def polynomial_regression_from_xy(
    xs: Sequence[float],
    ys: Sequence[float],
    *,
    requested_order: int,
) -> Dict[str, Any]:
    if not xs:
        return {"order": 0, "coefficients": []}

    actual_order = clamp_polynomial_order(requested_order, len(xs))
    while actual_order >= MIN_POLYNOMIAL_ORDER:
        matrix = build_normal_matrix(xs, actual_order)
        vector = build_normal_vector(xs, ys, actual_order)
        coefficients = solve_linear_system(matrix, vector)
        if coefficients is not None:
            return {"order": actual_order, "coefficients": coefficients}
        actual_order -= 1

    mean_y = sum(ys) / float(len(ys))
    return {"order": 0, "coefficients": [mean_y]}


def build_normal_matrix(xs: Sequence[float], order: int) -> List[List[float]]:
    powers = [sum((x_value ** power) for x_value in xs) for power in range((order * 2) + 1)]
    return [[powers[row + column] for column in range(order + 1)] for row in range(order + 1)]


def build_normal_vector(xs: Sequence[float], ys: Sequence[float], order: int) -> List[float]:
    return [
        sum((y_value * (x_value ** power)) for x_value, y_value in zip(xs, ys))
        for power in range(order + 1)
    ]


def solve_linear_system(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> Optional[List[float]]:
    size = len(matrix)
    if size == 0:
        return []

    augmented = [
        [float(matrix[row_index][column_index]) for column_index in range(size)] + [float(vector[row_index])]
        for row_index in range(size)
    ]

    for pivot_index in range(size):
        pivot_row = max(range(pivot_index, size), key=lambda row_index: abs(augmented[row_index][pivot_index]))
        pivot_value = augmented[pivot_row][pivot_index]
        if abs(pivot_value) <= EPSILON:
            return None
        if pivot_row != pivot_index:
            augmented[pivot_index], augmented[pivot_row] = augmented[pivot_row], augmented[pivot_index]

        pivot_value = augmented[pivot_index][pivot_index]
        for column_index in range(pivot_index, size + 1):
            augmented[pivot_index][column_index] /= pivot_value

        for row_index in range(size):
            if row_index == pivot_index:
                continue
            factor = augmented[row_index][pivot_index]
            if abs(factor) <= EPSILON:
                continue
            for column_index in range(pivot_index, size + 1):
                augmented[row_index][column_index] -= factor * augmented[pivot_index][column_index]

    return [augmented[row_index][size] for row_index in range(size)]


def evaluate_polynomial(coefficients: Sequence[float], x_value: float) -> float:
    return sum(coefficient * (x_value ** power) for power, coefficient in enumerate(coefficients))


def evaluate_polynomial_derivative(
    coefficients: Sequence[float],
    x_value: float,
    *,
    derivative_order: int,
) -> float:
    if derivative_order <= 0:
        return evaluate_polynomial(coefficients, x_value)

    total = 0.0
    for power, coefficient in enumerate(coefficients):
        if power < derivative_order:
            continue
        multiplier = coefficient
        for index in range(derivative_order):
            multiplier *= (power - index)
        total += multiplier * (x_value ** (power - derivative_order))
    return total


def normalisation_scale(values: Sequence[float], residual_std: float) -> float:
    if not values:
        return 1.0
    price_range = max(values) - min(values)
    travel = sum(abs(values[index] - values[index - 1]) for index in range(1, len(values)))
    average_step = travel / float(max(1, len(values) - 1))
    range_per_tick = price_range / float(max(1, len(values)))
    return max(residual_std, average_step, range_per_tick, 0.01)


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


def build_polynomial_relationship(
    prices: Sequence[float],
    fast_poly: Dict[str, Any],
    slow_poly: Dict[str, Any],
) -> Dict[str, Any]:
    if not prices:
        return empty_poly_relationship()

    price_range = max(prices) - min(prices)
    flat_threshold = max(0.0005, price_range / max(1, len(prices)) / 8.0)
    curvature_threshold = max(0.0001, flat_threshold / 3.0)

    fast_sign = slope_sign(float(fast_poly["slope"]), flat_threshold)
    slow_sign = slope_sign(float(slow_poly["slope"]), flat_threshold)

    if fast_sign == 0 and slow_sign == 0:
        slope_agreement = "flat"
        direction = "flat"
    elif fast_sign == slow_sign and fast_sign != 0:
        direction = "up" if fast_sign > 0 else "down"
        slope_agreement = "aligned_{0}".format(direction)
    elif fast_sign == 0 or slow_sign == 0:
        direction = "up" if max(fast_sign, slow_sign) > 0 else ("down" if min(fast_sign, slow_sign) < 0 else "flat")
        slope_agreement = "partial"
    else:
        direction = "flat"
        slope_agreement = "conflicted"

    fast_curvature = float(fast_poly["curvature"])
    slow_curvature = float(slow_poly["curvature"])
    if abs(fast_curvature) <= curvature_threshold and abs(slow_curvature) <= curvature_threshold:
        curvature_agreement = "flat"
    elif (fast_curvature > 0 and slow_curvature > 0) or (fast_curvature < 0 and slow_curvature < 0):
        if direction == "up":
            curvature_agreement = "reinforcing" if fast_curvature > 0 else "shared_exhaustion"
        elif direction == "down":
            curvature_agreement = "reinforcing" if fast_curvature < 0 else "shared_exhaustion"
        else:
            curvature_agreement = "shared_bias"
    else:
        curvature_agreement = "mixed"

    fitted_spread = float(fast_poly["currentFittedValue"] or 0.0) - float(slow_poly["currentFittedValue"] or 0.0)
    slope_spread = float(fast_poly["slope"]) - float(slow_poly["slope"])
    residual_ratio = None
    if fast_poly["residualStd"] is not None and slow_poly["residualStd"] is not None and float(slow_poly["residualStd"]) > EPSILON:
        residual_ratio = float(fast_poly["residualStd"]) / float(slow_poly["residualStd"])

    if residual_ratio is None:
        residual_regime = "unknown"
    elif residual_ratio <= 0.82:
        residual_regime = "compression"
    elif residual_ratio >= 1.08:
        residual_regime = "expansion"
    else:
        residual_regime = "balanced"

    current_price = float(prices[-1])
    fast_fit = float(fast_poly["currentFittedValue"] or current_price)
    slow_fit = float(slow_poly["currentFittedValue"] or current_price)
    if direction == "up":
        aligned_with_both = current_price >= fast_fit and current_price >= slow_fit
    elif direction == "down":
        aligned_with_both = current_price <= fast_fit and current_price <= slow_fit
    else:
        aligned_with_both = False

    stretch = max(abs(float(fast_poly["normalizedDistance"])), abs(float(slow_poly["normalizedDistance"])))
    if stretch >= 2.5:
        stretch_state = "very_stretched"
    elif stretch >= 1.7:
        stretch_state = "stretched"
    elif stretch >= 0.95:
        stretch_state = "extended"
    else:
        stretch_state = "contained"

    return {
        "direction": direction,
        "slopeAgreement": slope_agreement,
        "curvatureAgreement": curvature_agreement,
        "fittedSpread": fitted_spread,
        "slopeSpread": slope_spread,
        "residualCompressionRatio": residual_ratio,
        "residualRegime": residual_regime,
        "alignedWithBoth": aligned_with_both,
        "stretchState": stretch_state,
    }


def build_move_quality(
    *,
    prices: Sequence[float],
    fast_poly: Dict[str, Any],
    slow_poly: Dict[str, Any],
    poly_relationship: Dict[str, Any],
    break_pressure_score: Optional[float],
    signal_threshold: float,
) -> Dict[str, Any]:
    direction = poly_relationship["direction"]
    price_range = (max(prices) - min(prices)) if prices else 0.0
    baseline_slope = max(
        0.0008,
        price_range / float(max(1, len(prices))) / 2.0,
        float(slow_poly["distanceScale"] or 0.01) / float(max(20, slow_poly["tickCount"] or 20)),
    )

    same_direction = poly_relationship["slopeAgreement"] in {"aligned_up", "aligned_down"}
    slope_alignment_score = 22.0 if same_direction else (10.0 if poly_relationship["slopeAgreement"] == "partial" else 0.0)

    fast_strength_ratio = abs(float(fast_poly["slope"])) / max(baseline_slope, EPSILON)
    fast_strength_score = 18.0 * clamp01((fast_strength_ratio - 0.8) / 2.2)

    slow_support_ratio = abs(float(slow_poly["slope"])) / max(baseline_slope * 0.8, EPSILON)
    slow_support_score = 15.0 * clamp01((slow_support_ratio - 0.55) / 1.8)
    if not same_direction:
        slow_support_score *= 0.25

    curvature_state = poly_relationship["curvatureAgreement"]
    if curvature_state == "reinforcing":
        curvature_score = 12.0
    elif curvature_state == "flat":
        curvature_score = 8.5
    elif curvature_state == "shared_exhaustion":
        curvature_score = 3.0
    elif curvature_state == "mixed":
        curvature_score = 4.5
    else:
        curvature_score = 6.0

    max_distance = max(abs(float(fast_poly["normalizedDistance"])), abs(float(slow_poly["normalizedDistance"])))
    stretch_score = 17.0 * clamp01(1.0 - max(0.0, max_distance - 0.35) / 2.15)

    stability_floor = 1.0
    if break_pressure_score is not None:
        stability_floor = clamp01(1.0 - (float(break_pressure_score) / 100.0))
    residual_regime = poly_relationship["residualRegime"]
    regime_bonus = 0.15 if residual_regime == "compression" else (-0.2 if residual_regime == "expansion" else 0.0)
    stability_score = 8.0 * clamp01(stability_floor + regime_bonus)

    alignment_score = 8.0 if poly_relationship["alignedWithBoth"] else (3.0 if direction in {"up", "down"} else 0.0)

    total_score = (
        slope_alignment_score
        + fast_strength_score
        + slow_support_score
        + curvature_score
        + stretch_score
        + stability_score
        + alignment_score
    )

    if poly_relationship["stretchState"] in {"very_stretched", "stretched"} and total_score >= 55.0:
        state = "stretched"
    elif poly_relationship["slopeAgreement"] == "conflicted":
        state = "conflicted"
    elif break_pressure_score is not None and float(break_pressure_score) >= 88.0 and residual_regime == "expansion":
        state = "unstable"
    elif residual_regime == "expansion" and max_distance >= 1.5:
        state = "unstable"
    elif direction == "up":
        state = "strong_up_flow" if total_score >= 80.0 else ("weak_up_flow" if total_score >= 60.0 else "building_up_flow")
    elif direction == "down":
        state = "strong_down_flow" if total_score >= 80.0 else ("weak_down_flow" if total_score >= 60.0 else "building_down_flow")
    else:
        state = "neutral"

    candidate = (
        total_score >= signal_threshold
        and direction in {"up", "down"}
        and state not in {"stretched", "unstable", "conflicted"}
    )

    summary = build_move_quality_summary(
        direction=direction,
        state=state,
        slope_agreement=poly_relationship["slopeAgreement"],
        curvature_agreement=poly_relationship["curvatureAgreement"],
        residual_regime=residual_regime,
        max_distance=max_distance,
    )

    return {
        "score": round(total_score, 3),
        "state": state,
        "direction": direction,
        "candidate": candidate,
        "signalThreshold": float(signal_threshold),
        "summary": summary,
        "components": {
            "slopeAlignment": component_payload(slope_alignment_score, 22.0, poly_relationship["slopeAgreement"]),
            "fastSlopeStrength": component_payload(fast_strength_score, 18.0, fast_strength_ratio),
            "slowSupport": component_payload(slow_support_score, 15.0, slow_support_ratio),
            "curvatureSupport": component_payload(curvature_score, 12.0, curvature_state),
            "stretchControl": component_payload(stretch_score, 17.0, max_distance),
            "residualStability": component_payload(stability_score, 8.0, residual_regime),
            "priceAlignment": component_payload(alignment_score, 8.0, poly_relationship["alignedWithBoth"]),
        },
    }


def component_payload(score: float, maximum: float, detail: Any) -> Dict[str, Any]:
    return {
        "score": round(score, 3),
        "maxScore": float(maximum),
        "detail": detail,
    }


def build_move_quality_summary(
    *,
    direction: str,
    state: str,
    slope_agreement: str,
    curvature_agreement: str,
    residual_regime: str,
    max_distance: float,
) -> str:
    if state == "conflicted":
        return "Fast and slow polynomial slopes disagree, so the move is not structurally aligned."
    if state == "stretched":
        return "Direction exists, but price is stretched too far from the fitted path for a clean scalp."
    if state == "unstable":
        return "Residuals are expanding or break pressure is high, so the move quality is unstable."
    if direction == "up":
        return "Up-flow with {0}, {1}, residuals {2}, and distance {3:.2f}.".format(
            slope_agreement,
            curvature_agreement,
            residual_regime,
            max_distance,
        )
    if direction == "down":
        return "Down-flow with {0}, {1}, residuals {2}, and distance {3:.2f}.".format(
            slope_agreement,
            curvature_agreement,
            residual_regime,
            max_distance,
        )
    return "The polynomial structure is not directional enough yet."


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


def average_absolute(values: Sequence[Optional[float]]) -> float:
    numeric = [abs(value) for value in values if value is not None]
    if not numeric:
        return 0.0
    return sum(numeric) / float(len(numeric))


def score_disagreement(fast: Dict[str, Any], slow: Dict[str, Any], relationship: Dict[str, Any]) -> float:
    slow_slope = abs(float(slow["slope"]))
    slope_gap = min(1.0, abs(float(relationship["slopeDifference"])) / max(slow_slope, 0.0005))
    distance_gap = min(
        1.0,
        abs(float(relationship["currentFastSlowDistance"])) / max(float(slow["residualStd"] or 0.02) * 2.0, 0.05),
    )
    direction_gap = 1.0 if relationship["directionalAgreement"] == -1 else (
        0.4 if relationship["alignmentState"] in {"fast-flat", "slow-flat"} else 0.0
    )
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


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def generate_tuning_configs() -> List[Dict[str, Any]]:
    configs: List[Dict[str, Any]] = []
    for fast_window in DEFAULT_TUNING_FAST_WINDOWS:
        for slow_window in DEFAULT_TUNING_SLOW_WINDOWS:
            if slow_window <= fast_window:
                continue
            for fast_order in DEFAULT_TUNING_FAST_ORDERS:
                for slow_order in DEFAULT_TUNING_SLOW_ORDERS:
                    if slow_order < fast_order:
                        continue
                    configs.append(
                        {
                            "fastWindowTicks": fast_window,
                            "slowWindowTicks": slow_window,
                            "fastPolyOrder": fast_order,
                            "slowPolyOrder": slow_order,
                            "scoreThreshold": DEFAULT_TUNING_SIGNAL_THRESHOLD,
                        }
                    )
    return configs


def evaluate_polynomial_tuning(
    rows: Sequence[Dict[str, Any]],
    *,
    series: str,
    target_move: float,
    adverse_move: float,
    horizon_ticks: int,
    min_signals: int,
) -> Dict[str, Any]:
    payload_rows = list(rows)
    if len(payload_rows) < (MIN_ANALYSIS_WINDOW * 4):
        return {
            "evaluatedAt": iso_utc_now(),
            "series": series,
            "rowCount": len(payload_rows),
            "targetMove": target_move,
            "adverseMove": adverse_move,
            "horizonTicks": horizon_ticks,
            "minSignals": min_signals,
            "configs": [],
            "bestConfig": None,
            "stableConfig": None,
            "summary": "Not enough rows to evaluate tuning candidates.",
        }

    prices = [select_price(row, series) for row in payload_rows]
    results = [
        evaluate_single_tuning_config(
            rows=payload_rows,
            prices=prices,
            config=config,
            target_move=target_move,
            adverse_move=adverse_move,
            horizon_ticks=horizon_ticks,
            min_signals=min_signals,
        )
        for config in generate_tuning_configs()
    ]

    ranked = sorted(
        results,
        key=lambda item: (
            float(item["rankScore"]),
            float(item["successRate"]),
            float(item["decisiveRate"]),
            float(item["signalCount"]),
        ),
        reverse=True,
    )
    best_config = ranked[0] if ranked else None
    stable_config = next((item for item in ranked if item["eligible"]), best_config)

    summary = "No tuning candidates produced signals."
    if best_config:
        summary = (
            "Best recent config: fast {0}/{1}, slow {2}/{3}, score >= {4:.0f}, success {5:.1%} over {6} signals."
            .format(
                best_config["fastWindowTicks"],
                best_config["fastPolyOrder"],
                best_config["slowWindowTicks"],
                best_config["slowPolyOrder"],
                best_config["scoreThreshold"],
                best_config["successRate"],
                best_config["signalCount"],
            )
        )

    return {
        "evaluatedAt": iso_utc_now(),
        "series": series,
        "rowCount": len(payload_rows),
        "targetMove": target_move,
        "adverseMove": adverse_move,
        "horizonTicks": horizon_ticks,
        "minSignals": min_signals,
        "configs": ranked,
        "bestConfig": best_config,
        "stableConfig": stable_config,
        "summary": summary,
    }


def evaluate_single_tuning_config(
    *,
    rows: Sequence[Dict[str, Any]],
    prices: Sequence[float],
    config: Dict[str, Any],
    target_move: float,
    adverse_move: float,
    horizon_ticks: int,
    min_signals: int,
) -> Dict[str, Any]:
    slow_window = int(config["slowWindowTicks"])
    fast_window = int(config["fastWindowTicks"])
    step = max(12, fast_window // 3)
    start_index = max(slow_window, MIN_ANALYSIS_WINDOW * 2)
    last_entry_index = len(rows) - horizon_ticks - 1

    wins = 0
    losses = 0
    expired = 0
    signal_count = 0
    favorable_total = 0.0
    adverse_total = 0.0

    for end_index in range(start_index, last_entry_index + 1, step):
        slice_start = max(0, end_index - max(slow_window * 2, slow_window + 80) + 1)
        analysis_rows = rows[slice_start:end_index + 1]
        analysis_prices = prices[slice_start:end_index + 1]
        _, _, _, move_quality = build_polynomial_analysis(
            rows=analysis_rows,
            prices=analysis_prices,
            fast_window_ticks=fast_window,
            slow_window_ticks=slow_window,
            fast_poly_order=int(config["fastPolyOrder"]),
            slow_poly_order=int(config["slowPolyOrder"]),
            break_pressure_score=None,
            signal_threshold=float(config["scoreThreshold"]),
        )

        if not move_quality["candidate"] or move_quality["direction"] not in {"up", "down"}:
            continue

        signal_count += 1
        outcome = evaluate_future_outcome(
            prices=prices,
            entry_index=end_index,
            direction=move_quality["direction"],
            target_move=target_move,
            adverse_move=adverse_move,
            horizon_ticks=horizon_ticks,
        )

        favorable_total += outcome["maxFavorableExcursion"]
        adverse_total += outcome["maxAdverseExcursion"]

        if outcome["state"] == "win":
            wins += 1
        elif outcome["state"] == "loss":
            losses += 1
        else:
            expired += 1

    success_rate = (wins / float(signal_count)) if signal_count else 0.0
    decisive_rate = ((wins + losses) / float(signal_count)) if signal_count else 0.0
    coverage_score = clamp01(signal_count / float(max(min_signals, 1)))
    favorable_ratio = favorable_total / float(max(signal_count, 1) * max(target_move, EPSILON))
    adverse_ratio = adverse_total / float(max(signal_count, 1) * max(adverse_move, EPSILON))
    expectancy_edge = clamp01((favorable_ratio - (0.65 * adverse_ratio)) / 2.0)
    rank_score = 100.0 * (
        (0.52 * success_rate)
        + (0.18 * decisive_rate)
        + (0.15 * coverage_score)
        + (0.15 * expectancy_edge)
    )

    return {
        **config,
        "signalCount": signal_count,
        "wins": wins,
        "losses": losses,
        "expired": expired,
        "successRate": success_rate,
        "decisiveRate": decisive_rate,
        "avgFavorableExcursion": favorable_total / float(max(signal_count, 1)),
        "avgAdverseExcursion": adverse_total / float(max(signal_count, 1)),
        "rankScore": round(rank_score, 3),
        "eligible": signal_count >= min_signals,
    }


def evaluate_future_outcome(
    *,
    prices: Sequence[float],
    entry_index: int,
    direction: str,
    target_move: float,
    adverse_move: float,
    horizon_ticks: int,
) -> Dict[str, Any]:
    entry_price = float(prices[entry_index])
    max_favorable = 0.0
    max_adverse = 0.0

    for future_index in range(entry_index + 1, min(len(prices), entry_index + horizon_ticks + 1)):
        delta = float(prices[future_index]) - entry_price
        favorable = delta if direction == "up" else -delta
        adverse = -delta if direction == "up" else delta

        max_favorable = max(max_favorable, favorable)
        max_adverse = max(max_adverse, adverse)

        if favorable >= target_move:
            return {
                "state": "win",
                "maxFavorableExcursion": max_favorable,
                "maxAdverseExcursion": max_adverse,
            }
        if adverse >= adverse_move:
            return {
                "state": "loss",
                "maxFavorableExcursion": max_favorable,
                "maxAdverseExcursion": max_adverse,
            }

    return {
        "state": "expired",
        "maxFavorableExcursion": max_favorable,
        "maxAdverseExcursion": max_adverse,
    }
