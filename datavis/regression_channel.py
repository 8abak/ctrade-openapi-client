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
        "kind": "channel",
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


def fit_pitchfork(
    rows: Iterable[Dict[str, Any]],
    *,
    start_tick_id: int,
    end_tick_id: int,
    anchor_tick_id: int,
) -> Dict[str, Any]:
    anchor_ids = [int(start_tick_id), int(end_tick_id), int(anchor_tick_id)]
    if any(value <= 0 for value in anchor_ids) or not (anchor_ids[0] < anchor_ids[1] < anchor_ids[2]):
        raise RegressionChannelError("Pitchfork ticks must be three increasing tick IDs.")

    prices: Dict[int, float] = {}
    for row in rows:
        tick_id = int(row.get("id") or 0)
        value = _mid(row)
        if tick_id in anchor_ids and value is not None:
            prices[tick_id] = value
    if any(tick_id not in prices for tick_id in anchor_ids):
        raise RegressionChannelError("All three pitchfork ticks must exist in the loaded market data.")

    a_id, b_id, c_id = anchor_ids
    a_price, b_price, c_price = prices[a_id], prices[b_id], prices[c_id]
    midpoint_id = (b_id + c_id) / 2.0
    midpoint_price = (b_price + c_price) / 2.0
    distance = midpoint_id - a_id
    if distance <= 0:
        raise RegressionChannelError("Pitchfork anchors do not form a valid timeline.")
    slope = (midpoint_price - a_price) / distance
    b_offset = b_price - (a_price + slope * (b_id - a_id))
    c_offset = c_price - (a_price + slope * (c_id - a_id))
    lower_offset, upper_offset = sorted((b_offset, c_offset))

    model = {
        "kind": "pitchfork",
        "startTickId": a_id,
        "endTickId": c_id,
        "anchorTickId": c_id,
        "pivotTickIds": anchor_ids,
        "pivotPrices": [a_price, b_price, c_price],
        "intercept": a_price,
        "slopePerTick": slope,
        "lowerOffset": lower_offset,
        "upperOffset": upper_offset,
    }
    model["start"] = geometry_values_at(model, a_id)
    model["end"] = geometry_values_at(model, c_id)
    return model


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


def geometry_values_at(model: Dict[str, Any], tick_id: int) -> Dict[str, float]:
    start_id = int(model.get("startTickId") or 0)
    intercept = float(model.get("intercept") or 0.0)
    slope = float(model.get("slopePerTick") or 0.0)
    center = intercept + slope * float(int(tick_id) - start_id)
    if str(model.get("kind") or "channel") == "pitchfork":
        return {
            "center": center,
            "upper": center + float(model.get("upperOffset") or 0.0),
            "lower": center + float(model.get("lowerOffset") or 0.0),
        }
    half_width = float(model.get("halfWidth") or 0.0)
    return {"center": center, "upper": center + half_width, "lower": center - half_width}
