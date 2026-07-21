"""Causal raw features for the fresh irregular-tick research track.

The module deliberately contains no entry rule, threshold, score, label, or
future-dependent calculation.  Every row is a function only of that tick and
earlier ticks in the same uninterrupted quote segment.  Session flags use IANA
time zones and therefore follow civil daylight-saving transitions; they are
not exchange-holiday calendars.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from datavis.research.ticks import Tick


_NANOSECONDS_PER_SECOND = 1_000_000_000
_EPSILON = np.finfo(float).eps
_TOKYO = ZoneInfo("Asia/Tokyo")
_LONDON = ZoneInfo("Europe/London")
_NEW_YORK = ZoneInfo("America/New_York")
_SESSION_COLUMN_NAMES = frozenset(
    {
        "tokyo_open",
        "london_open",
        "new_york_open",
        "tokyo_opening_hour",
        "london_opening_hour",
        "new_york_opening_hour",
        "major_session_open_count",
        "any_major_session_open",
        "tokyo_london_overlap",
        "london_new_york_overlap",
        "tokyo_new_york_overlap",
        "any_major_session_overlap",
    }
)


@dataclass(frozen=True, slots=True)
class FreshFeatureConfig:
    """Explicit structural measurements, never implicit trading selections.

    There are deliberately no defaults.  A research run must freeze every
    horizon, gap boundary, and filter sensitivity before it can inspect price
    outcomes.  This prevents a caller from silently inheriting parameters
    chosen in an earlier experiment.
    """

    horizons_seconds: tuple[float, ...]
    maximum_intertick_gap_ms: int
    ewma_half_lives_seconds: tuple[float, ...]
    kalman_acceleration_variance: float
    kalman_measurement_variance: float
    bollinger_width: float

    def __post_init__(self) -> None:
        horizons = _positive_unique_times(self.horizons_seconds, "horizons_seconds")
        half_lives = _positive_unique_times(
            self.ewma_half_lives_seconds, "ewma_half_lives_seconds"
        )
        object.__setattr__(self, "horizons_seconds", horizons)
        object.__setattr__(self, "ewma_half_lives_seconds", half_lives)
        if (
            not isinstance(self.maximum_intertick_gap_ms, int)
            or isinstance(self.maximum_intertick_gap_ms, bool)
            or self.maximum_intertick_gap_ms <= 0
        ):
            raise ValueError("maximum_intertick_gap_ms must be a positive integer")
        for name in (
            "kalman_acceleration_variance",
            "kalman_measurement_variance",
            "bollinger_width",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a positive finite number")
            if not math.isfinite(float(value)) or float(value) <= 0.0:
                raise ValueError(f"{name} must be a positive finite number")

    @property
    def longest_horizon_seconds(self) -> float:
        return self.horizons_seconds[-1]


def _positive_unique_times(values: Sequence[float], name: str) -> tuple[float, ...]:
    try:
        normalized = tuple(float(value) for value in values)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must contain positive finite numbers") from exc
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    if any(not math.isfinite(value) or value <= 0.0 for value in normalized):
        raise ValueError(f"{name} must contain positive finite numbers")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{name} must not contain duplicates")
    return tuple(sorted(normalized))


def _horizon_tag(seconds: float) -> str:
    milliseconds = seconds * 1_000.0
    if math.isclose(milliseconds, round(milliseconds), abs_tol=1e-9):
        whole_ms = int(round(milliseconds))
        if whole_ms % 1_000 == 0:
            return f"{whole_ms // 1_000}s"
        return f"{whole_ms}ms"
    text = format(seconds, ".9g").replace(".", "p")
    return f"{text}s"


def _materialize_ticks(ticks: Iterable[Tick]) -> tuple[Tick, ...]:
    points = tuple(ticks)
    previous_key: tuple[datetime, int] | None = None
    seen_ids: set[int] = set()
    for position, point in enumerate(points):
        if not isinstance(point, Tick):
            raise TypeError(f"ticks[{position}] is not a Tick")
        if point.id in seen_ids:
            raise ValueError(f"duplicate tick id at index {position}: {point.id}")
        seen_ids.add(point.id)
        key = (point.timestamp, point.id)
        if previous_key is not None and key <= previous_key:
            raise ValueError("ticks must be strictly ordered by (timestamp, id)")
        previous_key = key
    return points


def _anchors_at_or_before(
    timestamp_ns: np.ndarray,
    segment_start: np.ndarray,
    offset_ns: int,
) -> np.ndarray:
    anchors = np.searchsorted(
        timestamp_ns, timestamp_ns - offset_ns, side="right"
    ).astype(np.int64) - 1
    return np.maximum(anchors, segment_start)


def _interval_sum(cumulative: np.ndarray, left: np.ndarray) -> np.ndarray:
    right = np.arange(left.size, dtype=np.int64) + 1
    return cumulative[right] - cumulative[left]


def _prefix_sum(values: np.ndarray) -> np.ndarray:
    return np.concatenate((np.zeros(1, dtype=float), np.cumsum(values, dtype=float)))


def _masked(values: np.ndarray, ready: np.ndarray) -> np.ndarray:
    result = np.asarray(values, dtype=float).copy()
    result[~ready] = np.nan
    return result


def _rolling_min_max(
    values: np.ndarray,
    anchors: np.ndarray,
    gap_detected: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    minimum = np.empty(values.size, dtype=float)
    maximum = np.empty(values.size, dtype=float)
    minimum_queue: deque[int] = deque()
    maximum_queue: deque[int] = deque()
    for index, value in enumerate(values):
        if gap_detected[index]:
            minimum_queue.clear()
            maximum_queue.clear()
        while minimum_queue and values[minimum_queue[-1]] >= value:
            minimum_queue.pop()
        while maximum_queue and values[maximum_queue[-1]] <= value:
            maximum_queue.pop()
        minimum_queue.append(index)
        maximum_queue.append(index)
        left = int(anchors[index])
        while minimum_queue[0] < left:
            minimum_queue.popleft()
        while maximum_queue[0] < left:
            maximum_queue.popleft()
        minimum[index] = values[minimum_queue[0]]
        maximum[index] = values[maximum_queue[0]]
    return minimum, maximum


def _kinematics(
    values: np.ndarray,
    timestamp_ns: np.ndarray,
    first: np.ndarray,
    second: np.ndarray,
    third: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return elapsed-time speed, acceleration, and jerk within four anchors."""

    current = np.arange(values.size, dtype=np.int64)
    dt1 = (timestamp_ns[second] - timestamp_ns[first]) / _NANOSECONDS_PER_SECOND
    dt2 = (timestamp_ns[third] - timestamp_ns[second]) / _NANOSECONDS_PER_SECOND
    dt3 = (timestamp_ns[current] - timestamp_ns[third]) / _NANOSECONDS_PER_SECOND
    total = dt1 + dt2 + dt3

    speed = np.full(values.size, np.nan, dtype=float)
    np.divide(values[current] - values[first], total, out=speed, where=total > 0.0)

    velocity1 = np.full(values.size, np.nan, dtype=float)
    velocity2 = np.full(values.size, np.nan, dtype=float)
    velocity3 = np.full(values.size, np.nan, dtype=float)
    np.divide(
        values[second] - values[first], dt1, out=velocity1, where=dt1 > 0.0
    )
    np.divide(
        values[third] - values[second], dt2, out=velocity2, where=dt2 > 0.0
    )
    np.divide(
        values[current] - values[third], dt3, out=velocity3, where=dt3 > 0.0
    )

    acceleration1_elapsed = (dt1 + dt2) / 2.0
    acceleration2_elapsed = (dt2 + dt3) / 2.0
    acceleration1 = np.full(values.size, np.nan, dtype=float)
    acceleration2 = np.full(values.size, np.nan, dtype=float)
    np.divide(
        velocity2 - velocity1,
        acceleration1_elapsed,
        out=acceleration1,
        where=acceleration1_elapsed > 0.0,
    )
    np.divide(
        velocity3 - velocity2,
        acceleration2_elapsed,
        out=acceleration2,
        where=acceleration2_elapsed > 0.0,
    )

    acceleration_time_delta = dt1 / 4.0 + dt2 / 2.0 + dt3 / 4.0
    jerk = np.full(values.size, np.nan, dtype=float)
    np.divide(
        acceleration2 - acceleration1,
        acceleration_time_delta,
        out=jerk,
        where=acceleration_time_delta > 0.0,
    )
    return speed, acceleration2, jerk


def _continuous_ewmas(
    mid: np.ndarray,
    timestamp_ns: np.ndarray,
    gap_detected: np.ndarray,
    half_lives: Sequence[float],
) -> dict[str, np.ndarray]:
    columns: dict[str, np.ndarray] = {}
    levels: dict[str, np.ndarray] = {}
    for half_life in half_lives:
        tag = _horizon_tag(half_life)
        level = np.empty(mid.size, dtype=float)
        slope = np.full(mid.size, np.nan, dtype=float)
        separation = np.empty(mid.size, dtype=float)
        for index, value in enumerate(mid):
            if index == 0 or gap_detected[index]:
                level[index] = value
                separation[index] = 0.0
                continue
            elapsed = (
                timestamp_ns[index] - timestamp_ns[index - 1]
            ) / _NANOSECONDS_PER_SECOND
            if elapsed <= 0.0:
                level[index] = level[index - 1]
                separation[index] = value - level[index]
                continue
            alpha = -math.expm1(-math.log(2.0) * elapsed / half_life)
            level[index] = level[index - 1] + alpha * (value - level[index - 1])
            slope[index] = (level[index] - level[index - 1]) / elapsed
            separation[index] = value - level[index]
        levels[tag] = level
        columns[f"ewma_{tag}_mid"] = level
        columns[f"ewma_{tag}_slope"] = slope
        columns[f"ewma_{tag}_price_separation"] = separation
    tags = [_horizon_tag(value) for value in half_lives]
    for short, long in zip(tags, tags[1:]):
        columns[f"ewma_separation_{short}_{long}"] = levels[short] - levels[long]
    return columns


def _constant_velocity_kalman(
    mid: np.ndarray,
    timestamp_ns: np.ndarray,
    gap_detected: np.ndarray,
    acceleration_variance: float,
    measurement_variance: float,
    output_columns: Sequence[str] | None = None,
) -> dict[str, np.ndarray]:
    size = mid.size
    available = (
        "kalman_price",
        "kalman_velocity",
        "kalman_innovation",
        "kalman_innovation_variance",
        "kalman_price_separation",
        "kalman_velocity_change",
        "kalman_velocity_uncertainty",
    )
    if output_columns is None:
        requested = set(available)
    else:
        requested = set(output_columns)
        unknown = requested.difference(available)
        if unknown:
            raise ValueError(
                "unknown Kalman output columns: " + ", ".join(sorted(unknown))
            )

    price = np.empty(size, dtype=float) if "kalman_price" in requested else None
    velocity = (
        np.empty(size, dtype=float) if "kalman_velocity" in requested else None
    )
    innovation = (
        np.empty(size, dtype=float) if "kalman_innovation" in requested else None
    )
    innovation_variance = (
        np.empty(size, dtype=float)
        if "kalman_innovation_variance" in requested
        else None
    )
    velocity_change = (
        np.full(size, np.nan, dtype=float)
        if "kalman_velocity_change" in requested
        else None
    )
    price_separation = (
        np.empty(size, dtype=float)
        if "kalman_price_separation" in requested
        else None
    )
    velocity_uncertainty = (
        np.empty(size, dtype=float)
        if "kalman_velocity_uncertainty" in requested
        else None
    )

    state_price = float(mid[0])
    state_velocity = 0.0
    p00 = measurement_variance
    p01 = 0.0
    p11 = acceleration_variance
    for index, measurement in enumerate(mid):
        if index == 0 or gap_detected[index]:
            state_price = float(measurement)
            state_velocity = 0.0
            p00 = measurement_variance
            p01 = 0.0
            p11 = acceleration_variance
            current_innovation = 0.0
            current_innovation_variance = p00 + measurement_variance
        else:
            elapsed = (
                timestamp_ns[index] - timestamp_ns[index - 1]
            ) / _NANOSECONDS_PER_SECOND
            previous_velocity = state_velocity
            predicted_price = state_price + elapsed * state_velocity
            predicted_p00 = (
                p00
                + 2.0 * elapsed * p01
                + elapsed * elapsed * p11
                + acceleration_variance * elapsed**4 / 4.0
            )
            predicted_p01 = (
                p01
                + elapsed * p11
                + acceleration_variance * elapsed**3 / 2.0
            )
            predicted_p11 = p11 + acceleration_variance * elapsed * elapsed
            current_innovation = float(measurement) - predicted_price
            current_innovation_variance = predicted_p00 + measurement_variance
            gain_price = predicted_p00 / current_innovation_variance
            gain_velocity = predicted_p01 / current_innovation_variance
            state_price = predicted_price + gain_price * current_innovation
            state_velocity = state_velocity + gain_velocity * current_innovation
            p00 = max((1.0 - gain_price) * predicted_p00, 0.0)
            p01 = (1.0 - gain_price) * predicted_p01
            p11 = max(predicted_p11 - gain_velocity * predicted_p01, 0.0)
            if elapsed > 0.0:
                if velocity_change is not None:
                    velocity_change[index] = (
                        state_velocity - previous_velocity
                    ) / elapsed

        if price is not None:
            price[index] = state_price
        if velocity is not None:
            velocity[index] = state_velocity
        if innovation is not None:
            innovation[index] = current_innovation
        if innovation_variance is not None:
            innovation_variance[index] = current_innovation_variance
        if price_separation is not None:
            price_separation[index] = float(measurement) - state_price
        if velocity_uncertainty is not None:
            velocity_uncertainty[index] = math.sqrt(p11)

    values = {
        "kalman_price": price,
        "kalman_velocity": velocity,
        "kalman_innovation": innovation,
        "kalman_innovation_variance": innovation_variance,
        "kalman_price_separation": price_separation,
        "kalman_velocity_change": velocity_change,
        "kalman_velocity_uncertainty": velocity_uncertainty,
    }
    return {name: value for name, value in values.items() if value is not None}


def _civil_session_masks(
    timestamps: pd.DatetimeIndex,
    zone: ZoneInfo,
    start_hour: int,
    end_hour: int,
) -> tuple[np.ndarray, np.ndarray]:
    local = timestamps.tz_convert(zone)
    local_clock = (
        local.hour.to_numpy(dtype=float) * 3_600
        + local.minute.to_numpy(dtype=float) * 60
        + local.second.to_numpy(dtype=float)
        + local.microsecond.to_numpy(dtype=float) / 1e6
    )
    weekday = local.dayofweek.to_numpy(dtype=np.int8) < 5
    opening = weekday & (
        local_clock >= start_hour * 3_600
    ) & (local_clock < (start_hour + 1) * 3_600)
    session = weekday & (
        local_clock >= start_hour * 3_600
    ) & (local_clock < end_hour * 3_600)
    return session, opening


def _session_columns(timestamps: pd.DatetimeIndex) -> dict[str, np.ndarray]:
    tokyo, tokyo_opening = _civil_session_masks(timestamps, _TOKYO, 9, 18)
    london, london_opening = _civil_session_masks(timestamps, _LONDON, 8, 17)
    new_york, new_york_opening = _civil_session_masks(
        timestamps, _NEW_YORK, 8, 17
    )
    tokyo_london = tokyo & london
    london_new_york = london & new_york
    tokyo_new_york = tokyo & new_york
    open_count = tokyo.astype(np.int8) + london.astype(np.int8) + new_york.astype(np.int8)
    return {
        "tokyo_open": tokyo,
        "london_open": london,
        "new_york_open": new_york,
        "tokyo_opening_hour": tokyo_opening,
        "london_opening_hour": london_opening,
        "new_york_opening_hour": new_york_opening,
        "major_session_open_count": open_count,
        "any_major_session_open": open_count > 0,
        "tokyo_london_overlap": tokyo_london,
        "london_new_york_overlap": london_new_york,
        "tokyo_new_york_overlap": tokyo_new_york,
        "any_major_session_overlap": tokyo_london | london_new_york | tokyo_new_york,
    }


def _empty_frame(config: FreshFeatureConfig) -> pd.DataFrame:
    base = [
        "tick_id",
        "timestamp",
        "bid",
        "ask",
        "mid",
        "spread",
        "interarrival_seconds",
        "gap_detected",
        "segment_id",
        "segment_age_seconds",
        "feature_ready",
    ]
    per_horizon: list[str] = []
    for seconds in config.horizons_seconds:
        tag = _horizon_tag(seconds)
        per_horizon.extend(
            f"{tag}_{suffix}"
            for suffix in (
                "ready",
                "bid_displacement",
                "ask_displacement",
                "mid_displacement",
                "bid_speed",
                "ask_speed",
                "mid_speed",
                "bid_acceleration",
                "ask_acceleration",
                "mid_acceleration",
                "bid_jerk",
                "ask_jerk",
                "mid_jerk",
                "tick_count",
                "arrival_rate",
                "persistence",
                "path_efficiency",
                "noise",
                "translation_coherence",
                "translation_pressure",
                "spread_mean",
                "spread_std",
                "spread_regime_zscore",
                "spread_regime_ratio",
                "spread_range_position",
                "mid_range",
                "range_position",
                "bollinger_mean",
                "bollinger_std",
                "bollinger_lower",
                "bollinger_upper",
                "bollinger_zscore",
                "bollinger_position",
            )
        )
    ewma: list[str] = []
    ewma_tags = [_horizon_tag(value) for value in config.ewma_half_lives_seconds]
    for tag in ewma_tags:
        ewma.extend(
            (f"ewma_{tag}_mid", f"ewma_{tag}_slope", f"ewma_{tag}_price_separation")
        )
    ewma.extend(
        f"ewma_separation_{short}_{long}"
        for short, long in zip(ewma_tags, ewma_tags[1:])
    )
    kalman = [
        "kalman_price",
        "kalman_velocity",
        "kalman_innovation",
        "kalman_innovation_variance",
        "kalman_price_separation",
        "kalman_velocity_change",
        "kalman_velocity_uncertainty",
    ]
    sessions = [
        "tokyo_open",
        "london_open",
        "new_york_open",
        "tokyo_opening_hour",
        "london_opening_hour",
        "new_york_opening_hour",
        "major_session_open_count",
        "any_major_session_open",
        "tokyo_london_overlap",
        "london_new_york_overlap",
        "tokyo_new_york_overlap",
        "any_major_session_overlap",
    ]
    return pd.DataFrame(columns=base + per_horizon + ewma + kalman + sessions)


def _normalize_output_columns(
    config: FreshFeatureConfig,
    output_columns: Sequence[str] | None,
) -> tuple[str, ...] | None:
    if output_columns is None:
        return None
    if isinstance(output_columns, (str, bytes)):
        raise TypeError("output columns must be a sequence of column names")
    requested = tuple(output_columns)
    if any(not isinstance(column, str) or not column for column in requested):
        raise ValueError("output columns must contain non-empty strings")
    if len(set(requested)) != len(requested):
        raise ValueError("output columns must not contain duplicates")
    available = set(_empty_frame(config).columns)
    unknown = [column for column in requested if column not in available]
    if unknown:
        raise ValueError("unknown fresh feature columns: " + ", ".join(unknown))
    return requested


def _compute_fresh_feature_projection(
    points: tuple[Tick, ...],
    selected: FreshFeatureConfig,
    requested: tuple[str, ...],
) -> pd.DataFrame:
    """Compute an exact subset without materialising discarded measurements."""

    if not points:
        return _empty_frame(selected).loc[:, list(requested)]

    wanted = set(requested)
    size = len(points)
    positions = np.arange(size, dtype=np.int64)
    timestamp = pd.to_datetime([point.timestamp for point in points], utc=True)
    timestamp_ns = timestamp.astype("int64").to_numpy(dtype=np.int64)
    bid = np.fromiter((point.bid for point in points), dtype=float, count=size)
    ask = np.fromiter((point.ask for point in points), dtype=float, count=size)
    mid = (bid + ask) / 2.0
    spread = ask - bid

    delta_ns = np.zeros(size, dtype=np.int64)
    delta_ns[1:] = np.diff(timestamp_ns)
    maximum_gap_ns = selected.maximum_intertick_gap_ms * 1_000_000
    gap_detected = delta_ns > maximum_gap_ns
    segment_id = np.cumsum(gap_detected, dtype=np.int64)
    segment_start = np.maximum.accumulate(np.where(gap_detected, positions, 0))
    segment_age_ns = timestamp_ns - timestamp_ns[segment_start]
    interarrival_seconds = delta_ns.astype(float) / _NANOSECONDS_PER_SECOND
    interarrival_seconds[0] = np.nan
    interarrival_seconds[gap_detected] = np.nan

    base_values: dict[str, object] = {
        "tick_id": np.fromiter(
            (point.id for point in points), dtype=np.int64, count=size
        ),
        "timestamp": timestamp,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "interarrival_seconds": interarrival_seconds,
        "gap_detected": gap_detected,
        "segment_id": segment_id,
        "segment_age_seconds": segment_age_ns.astype(float)
        / _NANOSECONDS_PER_SECOND,
        "feature_ready": segment_age_ns
        >= round(selected.longest_horizon_seconds * _NANOSECONDS_PER_SECOND),
    }
    columns: dict[str, object] = {
        name: value for name, value in base_values.items() if name in wanted
    }

    suffixes_by_tag: dict[str, set[str]] = {}
    for horizon in selected.horizons_seconds:
        tag = _horizon_tag(horizon)
        prefix = f"{tag}_"
        suffixes_by_tag[tag] = {
            column.removeprefix(prefix)
            for column in requested
            if column.startswith(prefix)
        }

    selected_suffixes = set().union(*suffixes_by_tag.values())
    path_suffixes = {"persistence", "path_efficiency", "noise"}
    need_path = bool(selected_suffixes & path_suffixes)
    if need_path:
        step = np.zeros(size, dtype=float)
        step[1:] = np.diff(mid)
        step[gap_detected] = 0.0
        cumulative_path = _prefix_sum(np.abs(step))
        cumulative_square_step = _prefix_sum(step * step)
        cumulative_positive = _prefix_sum((step > 0.0).astype(float))
        cumulative_negative = _prefix_sum((step < 0.0).astype(float))

    bollinger_suffixes = {
        "bollinger_mean",
        "bollinger_std",
        "bollinger_lower",
        "bollinger_upper",
        "bollinger_zscore",
        "bollinger_position",
    }
    spread_stat_suffixes = {
        "spread_mean",
        "spread_std",
        "spread_regime_zscore",
        "spread_regime_ratio",
    }
    need_mid_stats = bool(selected_suffixes & bollinger_suffixes)
    need_spread_stats = bool(selected_suffixes & spread_stat_suffixes)
    if need_mid_stats:
        mid_origin = mid[segment_start]
        centered_mid = mid - mid_origin
        cumulative_mid = _prefix_sum(centered_mid)
        cumulative_mid_square = _prefix_sum(centered_mid * centered_mid)
    if need_spread_stats:
        spread_origin = spread[segment_start]
        centered_spread = spread - spread_origin
        cumulative_spread = _prefix_sum(centered_spread)
        cumulative_spread_square = _prefix_sum(centered_spread * centered_spread)

    for horizon in selected.horizons_seconds:
        tag = _horizon_tag(horizon)
        suffixes = suffixes_by_tag[tag]
        if not suffixes:
            continue
        horizon_ns = round(horizon * _NANOSECONDS_PER_SECOND)
        ready = segment_age_ns >= horizon_ns
        if "ready" in suffixes:
            columns[f"{tag}_ready"] = ready
        if suffixes == {"ready"}:
            continue

        first = _anchors_at_or_before(timestamp_ns, segment_start, horizon_ns)
        elapsed = (timestamp_ns - timestamp_ns[first]) / _NANOSECONDS_PER_SECOND
        count = positions - first + 1

        displacement_suffixes = {
            "bid_displacement",
            "ask_displacement",
            "mid_displacement",
            "persistence",
            "path_efficiency",
            "translation_coherence",
            "translation_pressure",
        }
        if suffixes & displacement_suffixes:
            bid_displacement = bid - bid[first]
            ask_displacement = ask - ask[first]
            mid_displacement = mid - mid[first]
            for name, values in (
                ("bid_displacement", bid_displacement),
                ("ask_displacement", ask_displacement),
                ("mid_displacement", mid_displacement),
            ):
                if name in suffixes:
                    columns[f"{tag}_{name}"] = _masked(values, ready)

        kinematic_sources = (
            ("bid", bid),
            ("ask", ask),
            ("mid", mid),
        )
        if any(
            f"{source}_{measurement}" in suffixes
            for source, _ in kinematic_sources
            for measurement in ("speed", "acceleration", "jerk")
        ):
            second = _anchors_at_or_before(
                timestamp_ns, segment_start, round(2.0 * horizon_ns / 3.0)
            )
            third = _anchors_at_or_before(
                timestamp_ns, segment_start, round(horizon_ns / 3.0)
            )
            for source, values in kinematic_sources:
                names = (
                    f"{source}_speed",
                    f"{source}_acceleration",
                    f"{source}_jerk",
                )
                if not any(name in suffixes for name in names):
                    continue
                measurements = _kinematics(
                    values, timestamp_ns, first, second, third
                )
                for name, measurement in zip(names, measurements):
                    if name in suffixes:
                        columns[f"{tag}_{name}"] = _masked(measurement, ready)

        if "tick_count" in suffixes:
            columns[f"{tag}_tick_count"] = _masked(count.astype(float), ready)
        if "arrival_rate" in suffixes:
            arrival_rate = np.full(size, np.nan, dtype=float)
            np.divide(count - 1, elapsed, out=arrival_rate, where=elapsed > 0.0)
            columns[f"{tag}_arrival_rate"] = _masked(arrival_rate, ready)

        if suffixes & path_suffixes:
            step_left = first + 1
            path = _interval_sum(cumulative_path, step_left)
            square_step = _interval_sum(cumulative_square_step, step_left)
            positive = _interval_sum(cumulative_positive, step_left)
            negative = _interval_sum(cumulative_negative, step_left)
            if "persistence" in suffixes:
                moving_steps = positive + negative
                persistence = np.full(size, np.nan, dtype=float)
                positive_direction = (mid_displacement > 0.0) & (
                    moving_steps > 0.0
                )
                negative_direction = (mid_displacement < 0.0) & (
                    moving_steps > 0.0
                )
                flat_direction = (mid_displacement == 0.0) & (
                    moving_steps > 0.0
                )
                persistence[positive_direction] = (
                    positive[positive_direction] / moving_steps[positive_direction]
                )
                persistence[negative_direction] = (
                    negative[negative_direction] / moving_steps[negative_direction]
                )
                persistence[flat_direction] = 0.5
                columns[f"{tag}_persistence"] = _masked(persistence, ready)
            if "path_efficiency" in suffixes:
                efficiency = np.full(size, np.nan, dtype=float)
                np.divide(
                    np.abs(mid_displacement),
                    path,
                    out=efficiency,
                    where=path > _EPSILON,
                )
                columns[f"{tag}_path_efficiency"] = _masked(efficiency, ready)
            if "noise" in suffixes:
                noise = np.full(size, np.nan, dtype=float)
                np.divide(square_step, elapsed, out=noise, where=elapsed > 0.0)
                columns[f"{tag}_noise"] = _masked(np.sqrt(noise), ready)

        if suffixes & {"translation_coherence", "translation_pressure"}:
            translation_scale = np.maximum(
                np.abs(bid_displacement), np.abs(ask_displacement)
            )
            if "translation_coherence" in suffixes:
                translation_coherence = np.full(size, np.nan, dtype=float)
                coherence_numerator = (
                    np.sign(bid_displacement * ask_displacement)
                    * np.minimum(
                        np.abs(bid_displacement), np.abs(ask_displacement)
                    )
                )
                np.divide(
                    coherence_numerator,
                    translation_scale,
                    out=translation_coherence,
                    where=translation_scale > _EPSILON,
                )
                columns[f"{tag}_translation_coherence"] = _masked(
                    translation_coherence, ready
                )
            if "translation_pressure" in suffixes:
                translation_total = np.abs(bid_displacement) + np.abs(
                    ask_displacement
                )
                translation_pressure = np.full(size, np.nan, dtype=float)
                np.divide(
                    bid_displacement + ask_displacement,
                    translation_total,
                    out=translation_pressure,
                    where=translation_total > _EPSILON,
                )
                columns[f"{tag}_translation_pressure"] = _masked(
                    translation_pressure, ready
                )

        interval_count = count.astype(float)
        if suffixes & bollinger_suffixes:
            centered_mid_mean = (
                _interval_sum(cumulative_mid, first) / interval_count
            )
            mid_mean = mid_origin + centered_mid_mean
            mid_second_moment = (
                _interval_sum(cumulative_mid_square, first) / interval_count
            )
            mid_std = np.sqrt(
                np.maximum(
                    mid_second_moment - centered_mid_mean * centered_mid_mean,
                    0.0,
                )
            )
            width = selected.bollinger_width
            bollinger_lower = mid_mean - width * mid_std
            bollinger_upper = mid_mean + width * mid_std
            values_by_suffix = {
                "bollinger_mean": mid_mean,
                "bollinger_std": mid_std,
                "bollinger_lower": bollinger_lower,
                "bollinger_upper": bollinger_upper,
            }
            if "bollinger_zscore" in suffixes:
                bollinger_zscore = np.full(size, np.nan, dtype=float)
                np.divide(
                    mid - mid_mean,
                    mid_std,
                    out=bollinger_zscore,
                    where=mid_std > _EPSILON,
                )
                values_by_suffix["bollinger_zscore"] = bollinger_zscore
            if "bollinger_position" in suffixes:
                bollinger_position = np.full(size, np.nan, dtype=float)
                np.divide(
                    mid - bollinger_lower,
                    bollinger_upper - bollinger_lower,
                    out=bollinger_position,
                    where=(bollinger_upper - bollinger_lower) > _EPSILON,
                )
                values_by_suffix["bollinger_position"] = bollinger_position
            for name, values in values_by_suffix.items():
                if name in suffixes:
                    columns[f"{tag}_{name}"] = _masked(values, ready)

        if suffixes & spread_stat_suffixes:
            centered_spread_mean = (
                _interval_sum(cumulative_spread, first) / interval_count
            )
            spread_mean = spread_origin + centered_spread_mean
            spread_second_moment = (
                _interval_sum(cumulative_spread_square, first) / interval_count
            )
            spread_std = np.sqrt(
                np.maximum(
                    spread_second_moment
                    - centered_spread_mean * centered_spread_mean,
                    0.0,
                )
            )
            values_by_suffix = {
                "spread_mean": spread_mean,
                "spread_std": spread_std,
            }
            if "spread_regime_zscore" in suffixes:
                spread_zscore = np.full(size, np.nan, dtype=float)
                np.divide(
                    spread - spread_mean,
                    spread_std,
                    out=spread_zscore,
                    where=spread_std > _EPSILON,
                )
                values_by_suffix["spread_regime_zscore"] = spread_zscore
            if "spread_regime_ratio" in suffixes:
                spread_ratio = np.full(size, np.nan, dtype=float)
                np.divide(
                    spread,
                    spread_mean,
                    out=spread_ratio,
                    where=spread_mean > _EPSILON,
                )
                values_by_suffix["spread_regime_ratio"] = spread_ratio
            for name, values in values_by_suffix.items():
                if name in suffixes:
                    columns[f"{tag}_{name}"] = _masked(values, ready)

        if suffixes & {"mid_range", "range_position"}:
            mid_minimum, mid_maximum = _rolling_min_max(
                mid, first, gap_detected
            )
            mid_range = mid_maximum - mid_minimum
            if "mid_range" in suffixes:
                columns[f"{tag}_mid_range"] = _masked(mid_range, ready)
            if "range_position" in suffixes:
                range_position = np.full(size, np.nan, dtype=float)
                np.divide(
                    mid - mid_minimum,
                    mid_range,
                    out=range_position,
                    where=mid_range > _EPSILON,
                )
                columns[f"{tag}_range_position"] = _masked(
                    range_position, ready
                )
        if "spread_range_position" in suffixes:
            spread_minimum, spread_maximum = _rolling_min_max(
                spread, first, gap_detected
            )
            spread_range = spread_maximum - spread_minimum
            spread_range_position = np.full(size, np.nan, dtype=float)
            np.divide(
                spread - spread_minimum,
                spread_range,
                out=spread_range_position,
                where=spread_range > _EPSILON,
            )
            columns[f"{tag}_spread_range_position"] = _masked(
                spread_range_position, ready
            )

    ewma_columns = {
        column for column in requested if column.startswith("ewma_")
    }
    if ewma_columns:
        ewma = _continuous_ewmas(
            mid,
            timestamp_ns,
            gap_detected,
            selected.ewma_half_lives_seconds,
        )
        columns.update({name: ewma[name] for name in ewma_columns})

    kalman_columns = tuple(
        column for column in requested if column.startswith("kalman_")
    )
    if kalman_columns:
        columns.update(
            _constant_velocity_kalman(
                mid,
                timestamp_ns,
                gap_detected,
                float(selected.kalman_acceleration_variance),
                float(selected.kalman_measurement_variance),
                kalman_columns,
            )
        )

    requested_sessions = wanted & _SESSION_COLUMN_NAMES
    if requested_sessions:
        sessions = _session_columns(timestamp)
        columns.update({name: sessions[name] for name in requested_sessions})
    return pd.DataFrame(columns, copy=False).loc[:, list(requested)]


def compute_fresh_features(
    ticks: Iterable[Tick],
    *,
    config: FreshFeatureConfig,
    _output_columns: Sequence[str] | None = None,
    _ticks_are_validated: bool = False,
) -> pd.DataFrame:
    """Compute prefix-invariant measurements for strictly ordered quotes.

    A horizon uses the most recent observed quote at or before its backward
    time boundary.  Durations always use the anchors' actual elapsed time, so
    uneven tick spacing is not treated as an even sample grid.  A feed gap
    starts a new segment and no trailing measurement can cross it.
    """

    selected = config
    if not isinstance(selected, FreshFeatureConfig):
        raise TypeError("config must be a FreshFeatureConfig")
    if not isinstance(_ticks_are_validated, bool):
        raise TypeError("_ticks_are_validated must be a boolean")
    requested = _normalize_output_columns(selected, _output_columns)
    points = tuple(ticks) if _ticks_are_validated else _materialize_ticks(ticks)
    if requested is not None:
        return _compute_fresh_feature_projection(points, selected, requested)
    if not points:
        return _empty_frame(selected)

    size = len(points)
    positions = np.arange(size, dtype=np.int64)
    timestamp = pd.to_datetime([point.timestamp for point in points], utc=True)
    timestamp_ns = timestamp.astype("int64").to_numpy(dtype=np.int64)
    bid = np.fromiter((point.bid for point in points), dtype=float, count=size)
    ask = np.fromiter((point.ask for point in points), dtype=float, count=size)
    mid = (bid + ask) / 2.0
    spread = ask - bid

    delta_ns = np.zeros(size, dtype=np.int64)
    delta_ns[1:] = np.diff(timestamp_ns)
    maximum_gap_ns = selected.maximum_intertick_gap_ms * 1_000_000
    gap_detected = delta_ns > maximum_gap_ns
    segment_id = np.cumsum(gap_detected, dtype=np.int64)
    segment_start = np.maximum.accumulate(np.where(gap_detected, positions, 0))
    segment_age_ns = timestamp_ns - timestamp_ns[segment_start]
    interarrival_seconds = delta_ns.astype(float) / _NANOSECONDS_PER_SECOND
    interarrival_seconds[0] = np.nan
    interarrival_seconds[gap_detected] = np.nan

    columns: dict[str, object] = {
        "tick_id": np.fromiter((point.id for point in points), dtype=np.int64, count=size),
        "timestamp": timestamp,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "interarrival_seconds": interarrival_seconds,
        "gap_detected": gap_detected,
        "segment_id": segment_id,
        "segment_age_seconds": segment_age_ns.astype(float) / _NANOSECONDS_PER_SECOND,
        "feature_ready": segment_age_ns
        >= round(selected.longest_horizon_seconds * _NANOSECONDS_PER_SECOND),
    }

    step = np.zeros(size, dtype=float)
    step[1:] = np.diff(mid)
    step[gap_detected] = 0.0
    cumulative_path = _prefix_sum(np.abs(step))
    cumulative_square_step = _prefix_sum(step * step)
    cumulative_positive = _prefix_sum((step > 0.0).astype(float))
    cumulative_negative = _prefix_sum((step < 0.0).astype(float))
    # Center within each gap-bounded segment before accumulating second
    # moments.  This avoids subtracting two large, nearly equal raw-price
    # squares and preserves the natural symmetry under an affine reflection.
    mid_origin = mid[segment_start]
    spread_origin = spread[segment_start]
    centered_mid = mid - mid_origin
    centered_spread = spread - spread_origin
    cumulative_mid = _prefix_sum(centered_mid)
    cumulative_mid_square = _prefix_sum(centered_mid * centered_mid)
    cumulative_spread = _prefix_sum(centered_spread)
    cumulative_spread_square = _prefix_sum(centered_spread * centered_spread)

    for horizon in selected.horizons_seconds:
        tag = _horizon_tag(horizon)
        horizon_ns = round(horizon * _NANOSECONDS_PER_SECOND)
        ready = segment_age_ns >= horizon_ns
        first = _anchors_at_or_before(timestamp_ns, segment_start, horizon_ns)
        second = _anchors_at_or_before(
            timestamp_ns, segment_start, round(2.0 * horizon_ns / 3.0)
        )
        third = _anchors_at_or_before(
            timestamp_ns, segment_start, round(horizon_ns / 3.0)
        )
        elapsed = (timestamp_ns - timestamp_ns[first]) / _NANOSECONDS_PER_SECOND
        count = positions - first + 1

        bid_displacement = bid - bid[first]
        ask_displacement = ask - ask[first]
        mid_displacement = mid - mid[first]
        bid_speed, bid_acceleration, bid_jerk = _kinematics(
            bid, timestamp_ns, first, second, third
        )
        ask_speed, ask_acceleration, ask_jerk = _kinematics(
            ask, timestamp_ns, first, second, third
        )
        mid_speed, mid_acceleration, mid_jerk = _kinematics(
            mid, timestamp_ns, first, second, third
        )

        # step[j] belongs to (j - 1, j], so a window anchored at ``first``
        # starts with step[first + 1], not the step that arrived at the anchor.
        step_left = first + 1
        path = _interval_sum(cumulative_path, step_left)
        square_step = _interval_sum(cumulative_square_step, step_left)
        positive = _interval_sum(cumulative_positive, step_left)
        negative = _interval_sum(cumulative_negative, step_left)
        moving_steps = positive + negative
        persistence = np.full(size, np.nan, dtype=float)
        positive_direction = (mid_displacement > 0.0) & (moving_steps > 0.0)
        negative_direction = (mid_displacement < 0.0) & (moving_steps > 0.0)
        flat_direction = (mid_displacement == 0.0) & (moving_steps > 0.0)
        persistence[positive_direction] = positive[positive_direction] / moving_steps[positive_direction]
        persistence[negative_direction] = negative[negative_direction] / moving_steps[negative_direction]
        persistence[flat_direction] = 0.5
        efficiency = np.full(size, np.nan, dtype=float)
        np.divide(
            np.abs(mid_displacement),
            path,
            out=efficiency,
            where=path > _EPSILON,
        )
        noise = np.full(size, np.nan, dtype=float)
        np.divide(square_step, elapsed, out=noise, where=elapsed > 0.0)
        noise = np.sqrt(noise)
        arrival_rate = np.full(size, np.nan, dtype=float)
        np.divide(count - 1, elapsed, out=arrival_rate, where=elapsed > 0.0)

        translation_scale = np.maximum(
            np.abs(bid_displacement), np.abs(ask_displacement)
        )
        translation_coherence = np.full(size, np.nan, dtype=float)
        coherence_numerator = (
            np.sign(bid_displacement * ask_displacement)
            * np.minimum(np.abs(bid_displacement), np.abs(ask_displacement))
        )
        np.divide(
            coherence_numerator,
            translation_scale,
            out=translation_coherence,
            where=translation_scale > _EPSILON,
        )
        translation_total = np.abs(bid_displacement) + np.abs(ask_displacement)
        translation_pressure = np.full(size, np.nan, dtype=float)
        np.divide(
            bid_displacement + ask_displacement,
            translation_total,
            out=translation_pressure,
            where=translation_total > _EPSILON,
        )

        interval_count = count.astype(float)
        centered_mid_mean = _interval_sum(cumulative_mid, first) / interval_count
        mid_mean = mid_origin + centered_mid_mean
        mid_second_moment = _interval_sum(cumulative_mid_square, first) / interval_count
        mid_std = np.sqrt(
            np.maximum(mid_second_moment - centered_mid_mean * centered_mid_mean, 0.0)
        )
        centered_spread_mean = (
            _interval_sum(cumulative_spread, first) / interval_count
        )
        spread_mean = spread_origin + centered_spread_mean
        spread_second_moment = (
            _interval_sum(cumulative_spread_square, first) / interval_count
        )
        spread_std = np.sqrt(
            np.maximum(
                spread_second_moment - centered_spread_mean * centered_spread_mean,
                0.0,
            )
        )
        spread_zscore = np.full(size, np.nan, dtype=float)
        np.divide(
            spread - spread_mean,
            spread_std,
            out=spread_zscore,
            where=spread_std > _EPSILON,
        )
        spread_ratio = np.full(size, np.nan, dtype=float)
        np.divide(
            spread,
            spread_mean,
            out=spread_ratio,
            where=spread_mean > _EPSILON,
        )

        mid_minimum, mid_maximum = _rolling_min_max(mid, first, gap_detected)
        spread_minimum, spread_maximum = _rolling_min_max(spread, first, gap_detected)
        mid_range = mid_maximum - mid_minimum
        range_position = np.full(size, np.nan, dtype=float)
        np.divide(
            mid - mid_minimum,
            mid_range,
            out=range_position,
            where=mid_range > _EPSILON,
        )
        spread_range = spread_maximum - spread_minimum
        spread_range_position = np.full(size, np.nan, dtype=float)
        np.divide(
            spread - spread_minimum,
            spread_range,
            out=spread_range_position,
            where=spread_range > _EPSILON,
        )
        bollinger_zscore = np.full(size, np.nan, dtype=float)
        np.divide(
            mid - mid_mean,
            mid_std,
            out=bollinger_zscore,
            where=mid_std > _EPSILON,
        )
        width = selected.bollinger_width
        bollinger_lower = mid_mean - width * mid_std
        bollinger_upper = mid_mean + width * mid_std
        bollinger_position = np.full(size, np.nan, dtype=float)
        np.divide(
            mid - bollinger_lower,
            bollinger_upper - bollinger_lower,
            out=bollinger_position,
            where=(bollinger_upper - bollinger_lower) > _EPSILON,
        )

        columns[f"{tag}_ready"] = ready
        columns[f"{tag}_bid_displacement"] = _masked(bid_displacement, ready)
        columns[f"{tag}_ask_displacement"] = _masked(ask_displacement, ready)
        columns[f"{tag}_mid_displacement"] = _masked(mid_displacement, ready)
        columns[f"{tag}_bid_speed"] = _masked(bid_speed, ready)
        columns[f"{tag}_ask_speed"] = _masked(ask_speed, ready)
        columns[f"{tag}_mid_speed"] = _masked(mid_speed, ready)
        columns[f"{tag}_bid_acceleration"] = _masked(bid_acceleration, ready)
        columns[f"{tag}_ask_acceleration"] = _masked(ask_acceleration, ready)
        columns[f"{tag}_mid_acceleration"] = _masked(mid_acceleration, ready)
        columns[f"{tag}_bid_jerk"] = _masked(bid_jerk, ready)
        columns[f"{tag}_ask_jerk"] = _masked(ask_jerk, ready)
        columns[f"{tag}_mid_jerk"] = _masked(mid_jerk, ready)
        columns[f"{tag}_tick_count"] = _masked(count.astype(float), ready)
        columns[f"{tag}_arrival_rate"] = _masked(arrival_rate, ready)
        columns[f"{tag}_persistence"] = _masked(persistence, ready)
        columns[f"{tag}_path_efficiency"] = _masked(efficiency, ready)
        columns[f"{tag}_noise"] = _masked(noise, ready)
        columns[f"{tag}_translation_coherence"] = _masked(
            translation_coherence, ready
        )
        columns[f"{tag}_translation_pressure"] = _masked(
            translation_pressure, ready
        )
        columns[f"{tag}_spread_mean"] = _masked(spread_mean, ready)
        columns[f"{tag}_spread_std"] = _masked(spread_std, ready)
        columns[f"{tag}_spread_regime_zscore"] = _masked(spread_zscore, ready)
        columns[f"{tag}_spread_regime_ratio"] = _masked(spread_ratio, ready)
        columns[f"{tag}_spread_range_position"] = _masked(
            spread_range_position, ready
        )
        columns[f"{tag}_mid_range"] = _masked(mid_range, ready)
        columns[f"{tag}_range_position"] = _masked(range_position, ready)
        columns[f"{tag}_bollinger_mean"] = _masked(mid_mean, ready)
        columns[f"{tag}_bollinger_std"] = _masked(mid_std, ready)
        columns[f"{tag}_bollinger_lower"] = _masked(bollinger_lower, ready)
        columns[f"{tag}_bollinger_upper"] = _masked(bollinger_upper, ready)
        columns[f"{tag}_bollinger_zscore"] = _masked(bollinger_zscore, ready)
        columns[f"{tag}_bollinger_position"] = _masked(
            bollinger_position, ready
        )

    columns.update(
        _continuous_ewmas(
            mid,
            timestamp_ns,
            gap_detected,
            selected.ewma_half_lives_seconds,
        )
    )
    columns.update(
        _constant_velocity_kalman(
            mid,
            timestamp_ns,
            gap_detected,
            float(selected.kalman_acceleration_variance),
            float(selected.kalman_measurement_variance),
        )
    )
    columns.update(_session_columns(timestamp))
    return pd.DataFrame(columns, copy=False)


__all__ = ["FreshFeatureConfig", "compute_fresh_features"]
