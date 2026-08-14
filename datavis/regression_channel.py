from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional


class RegressionChannelError(ValueError):
    pass


def _mid(row: Dict[str, Any]) -> Optional[float]:
    raw = row.get("mid")
    if raw is None:
        bid = row.get("bid")
        ask = row.get("ask")
        if bid is None or ask is None:
            return None
        raw = (float(bid) + float(ask)) / 2.0
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def fit_regression_channel(
    rows: Iterable[Dict[str, Any]],
    *,
    start_tick_id: int,
    end_tick_id: int,
    deviations: float,
) -> Dict[str, Any]:
    start_id = int(start_tick_id)
    end_id = int(end_tick_id)
    deviation_count = float(deviations)
    if start_id <= 0 or end_id <= start_id:
        raise RegressionChannelError("Channel end tick must be greater than its start tick.")
    if not math.isfinite(deviation_count) or deviation_count <= 0 or deviation_count > 10:
        raise RegressionChannelError("Regression number must be greater than 0 and no more than 10.")

    points: List[tuple[int, float]] = []
    for row in rows:
        tick_id = int(row.get("id") or 0)
        value = _mid(row)
        if start_id <= tick_id <= end_id and value is not None:
            points.append((tick_id, value))
    points.sort(key=lambda item: item[0])
    if len(points) < 2:
        raise RegressionChannelError("At least two ticks are required to draw a regression channel.")

    xs = [float(tick_id - start_id) for tick_id, _ in points]
    ys = [price for _, price in points]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    variance_x = sum((value - mean_x) ** 2 for value in xs)
    slope = 0.0 if variance_x <= 0 else sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / variance_x
    intercept = mean_y - slope * mean_x
    residuals = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
    residual_std = math.sqrt(sum(value * value for value in residuals) / len(residuals))
    half_width = max(0.000001, residual_std * deviation_count)

    def values_at(tick_id: int) -> Dict[str, float]:
        center = intercept + slope * float(int(tick_id) - start_id)
        return {
            "center": center,
            "upper": center + half_width,
            "lower": center - half_width,
        }

    return {
        "startTickId": start_id,
        "endTickId": end_id,
        "deviations": deviation_count,
        "sampleCount": len(points),
        "intercept": intercept,
        "slopePerTick": slope,
        "residualStd": residual_std,
        "halfWidth": half_width,
        "start": values_at(start_id),
        "end": values_at(end_id),
    }


def channel_values_at(model: Dict[str, Any], tick_id: int) -> Dict[str, float]:
    start_id = int(model.get("startTickId") or 0)
    intercept = float(model.get("intercept") or 0.0)
    slope = float(model.get("slopePerTick") or 0.0)
    half_width = float(model.get("halfWidth") or 0.0)
    center = intercept + slope * float(int(tick_id) - start_id)
    return {
        "center": center,
        "upper": center + half_width,
        "lower": center - half_width,
    }

