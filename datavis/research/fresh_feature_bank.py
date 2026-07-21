"""Shared causal feature computation for a frozen Kalman measurement bank.

``compute_fresh_features`` deliberately represents one completely explicit
Kalman model.  A preregistered bank can contain several such models whose
non-Kalman measurements are identical.  Recomputing every rolling, EWMA, and
calendar feature once per model wastes both time and memory.  This module
computes that shared layer once, retains one namespaced copy of each requested
Kalman output, and can project the result to the columns needed by named
candidate families.

The module fits no parameters and contains no outcome or trading logic.  Every
output row remains bound to the exact input quote.  The numerical kernels are
the same kernels used by :mod:`datavis.research.fresh_features`, so the bank is
bit-for-bit comparable with separate one-model runs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Integral
from typing import Iterable

import numpy as np
import pandas as pd

from datavis.research.fresh_features import (
    FreshFeatureConfig,
    _constant_velocity_kalman,
    _empty_frame,
    _materialize_ticks,
    compute_fresh_features,
)
from datavis.research.ticks import Tick


KALMAN_MEASUREMENT_COLUMNS = (
    "kalman_price",
    "kalman_velocity",
    "kalman_innovation",
    "kalman_innovation_variance",
    "kalman_price_separation",
    "kalman_velocity_change",
    "kalman_velocity_uncertainty",
)

# These are sufficient both for an exact quote-tape preflight and for the
# frozen signal generators' base-frame contract.
FEATURE_BANK_BINDING_COLUMNS = (
    "tick_id",
    "timestamp",
    "bid",
    "ask",
    "mid",
    "gap_detected",
    "segment_id",
    "feature_ready",
)

_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}\Z")


def _tuple(value: object, name: str) -> tuple[object, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple")
    return value


def _non_empty_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(set(values)) != len(values):
        raise ValueError(f"{name} must not contain duplicates")


@dataclass(frozen=True, slots=True)
class FreshKalmanBankMember:
    """One explicitly named one-model feature configuration."""

    model_id: str
    feature_config: FreshFeatureConfig

    def __post_init__(self) -> None:
        _non_empty_text(self.model_id, "model_id")
        if _MODEL_ID.fullmatch(self.model_id) is None:
            raise ValueError(
                "model_id must contain only letters, digits, '.', '_', or '-'"
            )
        if not isinstance(self.feature_config, FreshFeatureConfig):
            raise TypeError("feature_config must be a FreshFeatureConfig")


@dataclass(frozen=True, slots=True)
class NamedFeatureFamily:
    """The exact causal columns consumed by one named candidate family."""

    family_name: str
    required_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        _non_empty_text(self.family_name, "family_name")
        required = _tuple(self.required_columns, "required_columns")
        normalized: list[str] = []
        for position, column in enumerate(required):
            normalized.append(
                _non_empty_text(column, f"required_columns[{position}]")
            )
        _unique(tuple(normalized), "required_columns")
        object.__setattr__(self, "required_columns", tuple(normalized))


@dataclass(frozen=True, slots=True)
class FreshFeatureBankOutputSelection:
    """Explicitly request either the full bank or named-family projection.

    ``include_all_columns=True`` requires an empty selected-family tuple.  In
    compact mode, at least one named family must be selected.  The family
    definitions can contain unselected families so a single frozen registry
    can serve several independent computations.
    """

    include_all_columns: bool
    candidate_families: tuple[NamedFeatureFamily, ...]
    selected_candidate_families: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.include_all_columns, bool):
            raise TypeError("include_all_columns must be a boolean")
        families = _tuple(self.candidate_families, "candidate_families")
        selected = _tuple(
            self.selected_candidate_families,
            "selected_candidate_families",
        )
        if any(not isinstance(item, NamedFeatureFamily) for item in families):
            raise TypeError(
                "candidate_families must contain NamedFeatureFamily values"
            )
        family_names = tuple(item.family_name for item in families)
        _unique(family_names, "candidate family names")
        normalized_selected = tuple(
            _non_empty_text(value, f"selected_candidate_families[{position}]")
            for position, value in enumerate(selected)
        )
        _unique(normalized_selected, "selected_candidate_families")
        if self.include_all_columns:
            if normalized_selected:
                raise ValueError(
                    "selected_candidate_families must be empty in full-output mode"
                )
        else:
            if not normalized_selected:
                raise ValueError(
                    "compact output requires at least one selected candidate family"
                )
            unknown = [
                name for name in normalized_selected if name not in set(family_names)
            ]
            if unknown:
                raise ValueError(
                    "selected candidate families are not defined: "
                    + ", ".join(unknown)
                )
        object.__setattr__(self, "candidate_families", families)
        object.__setattr__(
            self,
            "selected_candidate_families",
            normalized_selected,
        )


@dataclass(frozen=True, slots=True)
class FreshFeatureBankConfig:
    """A frozen model bank and its explicit output projection."""

    members: tuple[FreshKalmanBankMember, ...]
    output_selection: FreshFeatureBankOutputSelection

    def __post_init__(self) -> None:
        members = _tuple(self.members, "members")
        if not members:
            raise ValueError("members must not be empty")
        if any(not isinstance(item, FreshKalmanBankMember) for item in members):
            raise TypeError("members must contain FreshKalmanBankMember values")
        if not isinstance(self.output_selection, FreshFeatureBankOutputSelection):
            raise TypeError(
                "output_selection must be a FreshFeatureBankOutputSelection"
            )
        model_ids = tuple(item.model_id for item in members)
        _unique(model_ids, "model IDs")
        model_parameters = tuple(
            (
                float(item.feature_config.kalman_acceleration_variance),
                float(item.feature_config.kalman_measurement_variance),
            )
            for item in members
        )
        _unique(model_parameters, "Kalman parameter pairs")

        first = members[0].feature_config
        shared = (
            first.horizons_seconds,
            first.maximum_intertick_gap_ms,
            first.ewma_half_lives_seconds,
            float(first.bollinger_width),
        )
        for position, member in enumerate(members[1:], start=1):
            current = member.feature_config
            signature = (
                current.horizons_seconds,
                current.maximum_intertick_gap_ms,
                current.ewma_half_lives_seconds,
                float(current.bollinger_width),
            )
            if signature != shared:
                raise ValueError(
                    "all bank members must have identical non-Kalman settings; "
                    f"member {position} differs"
                )
        object.__setattr__(self, "members", members)


def kalman_bank_column(model_id: str, measurement_column: str) -> str:
    """Return the collision-free output name for one model measurement."""

    _non_empty_text(model_id, "model_id")
    if _MODEL_ID.fullmatch(model_id) is None:
        raise ValueError(
            "model_id must contain only letters, digits, '.', '_', or '-'"
        )
    if measurement_column not in KALMAN_MEASUREMENT_COLUMNS:
        raise ValueError(
            "measurement_column must be one of: "
            + ", ".join(KALMAN_MEASUREMENT_COLUMNS)
        )
    suffix = measurement_column.removeprefix("kalman_")
    return f"{model_id}__{suffix}"


def _schema_columns(config: FreshFeatureBankConfig) -> tuple[str, ...]:
    first_frame = _empty_frame(config.members[0].feature_config)
    shared = tuple(
        column
        for column in first_frame.columns
        if column not in KALMAN_MEASUREMENT_COLUMNS
    )
    kalman = tuple(
        kalman_bank_column(member.model_id, measurement)
        for member in config.members
        for measurement in KALMAN_MEASUREMENT_COLUMNS
    )
    return shared + kalman


def feature_bank_columns(config: FreshFeatureBankConfig) -> tuple[str, ...]:
    """Return the deterministic schema selected by ``config``."""

    if not isinstance(config, FreshFeatureBankConfig):
        raise TypeError("config must be a FreshFeatureBankConfig")
    available = _schema_columns(config)
    selection = config.output_selection
    if selection.include_all_columns:
        return available

    family_by_name = {
        family.family_name: family for family in selection.candidate_families
    }
    requested = list(FEATURE_BANK_BINDING_COLUMNS)
    for family_name in selection.selected_candidate_families:
        requested.extend(family_by_name[family_name].required_columns)
    selected = tuple(dict.fromkeys(requested))
    available_set = set(available)
    unknown = [column for column in selected if column not in available_set]
    if unknown:
        raise ValueError(
            "candidate families require unavailable feature columns: "
            + ", ".join(unknown)
        )
    return selected


def _timestamp_utc(value: object, position: int) -> datetime:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"feature timestamp at row {position} is invalid") from exc
    if timestamp.tzinfo is None:
        raise ValueError(f"feature timestamp at row {position} has no timezone")
    return timestamp.to_pydatetime().astimezone(timezone.utc)


_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _datetime_ns(value: datetime) -> int:
    delta = value.astimezone(timezone.utc) - _EPOCH_UTC
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _first_mismatch(left: np.ndarray, right: np.ndarray) -> int | None:
    mismatches = np.flatnonzero(left != right)
    return int(mismatches[0]) if mismatches.size else None


def preflight_feature_bank_bindings(
    ticks: Iterable[Tick],
    features: pd.DataFrame,
    *,
    _ticks_are_validated: bool = False,
) -> None:
    """Prove that every feature row is bound to the exact input quote."""

    if not isinstance(features, pd.DataFrame):
        raise TypeError("features must be a pandas DataFrame")
    if not isinstance(_ticks_are_validated, bool):
        raise TypeError("_ticks_are_validated must be a boolean")
    points = tuple(ticks) if _ticks_are_validated else _materialize_ticks(ticks)
    missing = [
        column for column in FEATURE_BANK_BINDING_COLUMNS if column not in features
    ]
    if missing:
        raise ValueError(
            "feature bank is missing binding columns: " + ", ".join(missing)
        )
    if len(points) != len(features):
        raise ValueError("tick tape length does not match the feature bank")

    raw_tick_ids = features["tick_id"].to_numpy(copy=False)
    if pd.api.types.is_integer_dtype(raw_tick_ids.dtype) and not pd.api.types.is_bool_dtype(
        raw_tick_ids.dtype
    ):
        tick_ids = raw_tick_ids.astype(np.int64, copy=False)
    else:
        normalized_ids: list[int] = []
        for position, value in enumerate(raw_tick_ids):
            if not isinstance(value, Integral) or isinstance(value, (bool, np.bool_)):
                raise ValueError(
                    f"feature tick_id at row {position} is not an integer"
                )
            normalized_ids.append(int(value))
        tick_ids = np.asarray(normalized_ids, dtype=np.int64)
    expected_tick_ids = np.fromiter(
        (point.id for point in points), dtype=np.int64, count=len(points)
    )
    mismatch = _first_mismatch(tick_ids, expected_tick_ids)
    if mismatch is not None:
        raise ValueError(f"feature tick_id mismatch at row {mismatch}")

    timestamp_series = features["timestamp"]
    if isinstance(timestamp_series.dtype, pd.DatetimeTZDtype):
        feature_timestamp_ns = timestamp_series.array.asi8
    else:
        feature_timestamp_ns = np.fromiter(
            (
                _datetime_ns(_timestamp_utc(value, position))
                for position, value in enumerate(timestamp_series.to_numpy(copy=False))
            ),
            dtype=np.int64,
            count=len(features),
        )
    expected_timestamp_ns = np.fromiter(
        (_datetime_ns(point.timestamp) for point in points),
        dtype=np.int64,
        count=len(points),
    )
    mismatch = _first_mismatch(feature_timestamp_ns, expected_timestamp_ns)
    if mismatch is not None:
        raise ValueError(f"feature timestamp mismatch at row {mismatch}")

    bid = features["bid"].to_numpy(dtype=float, copy=False)
    ask = features["ask"].to_numpy(dtype=float, copy=False)
    mid = features["mid"].to_numpy(dtype=float, copy=False)
    expected_bid = np.fromiter(
        (point.bid for point in points), dtype=float, count=len(points)
    )
    expected_ask = np.fromiter(
        (point.ask for point in points), dtype=float, count=len(points)
    )
    for name, actual, expected in (
        ("bid", bid, expected_bid),
        ("ask", ask, expected_ask),
        ("mid", mid, (expected_bid + expected_ask) / 2.0),
    ):
        mismatch = _first_mismatch(actual, expected)
        if mismatch is not None:
            raise ValueError(f"feature {name} mismatch at row {mismatch}")


def _requested_kalman_columns(
    config: FreshFeatureBankConfig,
    selected_columns: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    selected = set(selected_columns)
    return {
        member.model_id: tuple(
            measurement
            for measurement in KALMAN_MEASUREMENT_COLUMNS
            if kalman_bank_column(member.model_id, measurement) in selected
        )
        for member in config.members
    }


def compute_fresh_feature_bank(
    ticks: Iterable[Tick],
    *,
    config: FreshFeatureBankConfig,
) -> pd.DataFrame:
    """Compute shared features once and requested Kalman models sequentially.

    Full-output mode computes every registered model.  Compact mode computes
    only models referenced by a selected family's columns.  One referenced
    model is used for the shared base call, so its Kalman state is reused
    instead of recomputed.  If a compact selection contains no Kalman column,
    the first member supplies the shared base and its unused state is dropped.
    """

    if not isinstance(config, FreshFeatureBankConfig):
        raise TypeError("config must be a FreshFeatureBankConfig")
    selected_columns = feature_bank_columns(config)
    requested_kalman = _requested_kalman_columns(config, selected_columns)
    required_model_ids = {
        model_id for model_id, columns in requested_kalman.items() if columns
    }
    primary = next(
        (
            member
            for member in config.members
            if member.model_id in required_model_ids
        ),
        config.members[0],
    )

    points = _materialize_ticks(ticks)
    all_kalman_output_columns = {
        kalman_bank_column(member.model_id, measurement)
        for member in config.members
        for measurement in KALMAN_MEASUREMENT_COLUMNS
    }
    base_columns = [
        column
        for column in selected_columns
        if column not in all_kalman_output_columns
    ]
    shared_output_columns = tuple(
        (*base_columns, *requested_kalman[primary.model_id])
    )
    shared_frame = compute_fresh_features(
        points,
        config=primary.feature_config,
        _output_columns=shared_output_columns,
        _ticks_are_validated=True,
    )

    kalman_outputs: dict[str, np.ndarray] = {}
    for measurement in requested_kalman[primary.model_id]:
        kalman_outputs[
            kalman_bank_column(primary.model_id, measurement)
        ] = shared_frame[measurement].to_numpy(dtype=float, copy=True)

    if points:
        timestamps = pd.DatetimeIndex(shared_frame["timestamp"])
        timestamp_ns = timestamps.astype("int64").to_numpy(dtype=np.int64)
        mid = shared_frame["mid"].to_numpy(dtype=float, copy=False)
        gaps = shared_frame["gap_detected"].to_numpy(dtype=bool, copy=False)
        for member in config.members:
            measurements = requested_kalman[member.model_id]
            if not measurements or member.model_id == primary.model_id:
                continue
            model = _constant_velocity_kalman(
                mid,
                timestamp_ns,
                gaps,
                float(member.feature_config.kalman_acceleration_variance),
                float(member.feature_config.kalman_measurement_variance),
                measurements,
            )
            for measurement in measurements:
                kalman_outputs[
                    kalman_bank_column(member.model_id, measurement)
                ] = model[measurement]
    else:
        for member in config.members:
            for measurement in requested_kalman[member.model_id]:
                kalman_outputs[
                    kalman_bank_column(member.model_id, measurement)
                ] = np.empty(0, dtype=float)

    result = shared_frame.loc[:, base_columns].copy()
    if kalman_outputs:
        result = pd.concat(
            [result, pd.DataFrame(kalman_outputs, index=result.index)], axis=1
        )
    result = result.loc[:, list(selected_columns)]
    preflight_feature_bank_bindings(
        points,
        result,
        _ticks_are_validated=True,
    )
    return result


__all__ = [
    "FEATURE_BANK_BINDING_COLUMNS",
    "KALMAN_MEASUREMENT_COLUMNS",
    "FreshFeatureBankConfig",
    "FreshFeatureBankOutputSelection",
    "FreshKalmanBankMember",
    "NamedFeatureFamily",
    "compute_fresh_feature_bank",
    "feature_bank_columns",
    "kalman_bank_column",
    "preflight_feature_bank_bindings",
]
