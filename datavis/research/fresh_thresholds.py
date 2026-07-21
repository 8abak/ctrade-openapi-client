"""Outcome-free, day-balanced threshold fitting for frozen signal rules.

Signal thresholds are learned only from causal feature values in explicitly
supplied training sessions.  Each requested quantile is computed exactly
within each session and the final threshold is the median session quantile.
This prevents high-tick-rate days from silently dominating a pooled rank and
requires no random sampling or tick shuffling.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Literal, Mapping, Sequence

import numpy as np
import pandas as pd


Transform = Literal["identity", "absolute", "positive"]
_FORBIDDEN = (
    "future",
    "label",
    "target",
    "outcome",
    "profit",
    "coverage",
    "trade_pnl",
    "mfe",
    "mae",
)


@dataclass(frozen=True, slots=True)
class QuantileMeasurementSpec:
    name: str
    column: str
    transform: Transform

    def __post_init__(self) -> None:
        for field_name in ("name", "column"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        lowered = self.column.casefold()
        if any(token in lowered for token in _FORBIDDEN):
            raise ValueError("measurement columns cannot contain outcome-like names")
        if self.transform not in ("identity", "absolute", "positive"):
            raise ValueError("unsupported measurement transform")


@dataclass(frozen=True, slots=True)
class FreshQuantileBankConfig:
    ranks: tuple[float, ...]
    minimum_finite_values_per_session: int
    minimum_eligible_sessions: int

    def __post_init__(self) -> None:
        try:
            ranks = tuple(float(value) for value in self.ranks)
        except (TypeError, ValueError) as exc:
            raise ValueError("ranks must be finite values strictly between zero and one") from exc
        if (
            not ranks
            or any(not math.isfinite(value) or not 0.0 < value < 1.0 for value in ranks)
            or tuple(sorted(set(ranks))) != ranks
        ):
            raise ValueError("ranks must be unique, sorted, and strictly between zero and one")
        object.__setattr__(self, "ranks", ranks)
        for name in (
            "minimum_finite_values_per_session",
            "minimum_eligible_sessions",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class QuantileThreshold:
    measurement: str
    rank: float
    value: float
    eligible_session_count: int
    finite_value_count: int
    minimum_session_quantile: float
    maximum_session_quantile: float


@dataclass(frozen=True, slots=True)
class FreshQuantileBank:
    config: FreshQuantileBankConfig
    training_session_anchors: tuple[str, ...]
    measurements: tuple[QuantileMeasurementSpec, ...]
    thresholds: tuple[QuantileThreshold, ...]
    bank_sha256: str

    def threshold(self, measurement: str, rank: float) -> float:
        selected_rank = float(rank)
        matches = [
            item
            for item in self.thresholds
            if item.measurement == measurement
            and math.isclose(item.rank, selected_rank, rel_tol=0.0, abs_tol=1e-12)
        ]
        if len(matches) != 1:
            raise KeyError((measurement, selected_rank))
        return matches[0].value


def _session_anchor(value: str) -> date:
    if not isinstance(value, str):
        raise ValueError("training session anchors must be ISO date strings")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("training session anchors must be ISO date strings") from exc
    if parsed.weekday() >= 5:
        raise ValueError("training session anchors must be weekdays")
    return parsed


def _transformed(values: np.ndarray, transform: Transform) -> np.ndarray:
    finite = values[np.isfinite(values)]
    if transform == "absolute":
        finite = np.abs(finite)
    elif transform == "positive":
        finite = finite[finite > 0.0]
    return finite


def _bank_hash(
    *,
    config: FreshQuantileBankConfig,
    anchors: Sequence[str],
    specs: Sequence[QuantileMeasurementSpec],
    thresholds: Sequence[QuantileThreshold],
) -> str:
    payload = {
        "schema": "fresh-session-balanced-quantiles/v1",
        "config": asdict(config),
        "trainingSessionAnchors": list(anchors),
        "measurements": [asdict(item) for item in specs],
        "thresholds": [asdict(item) for item in thresholds],
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class SessionBalancedQuantileFitter:
    """Incremental exact-within-session fitter for large chronological corpora."""

    def __init__(
        self,
        *,
        measurements: Sequence[QuantileMeasurementSpec],
        config: FreshQuantileBankConfig,
    ) -> None:
        if not isinstance(config, FreshQuantileBankConfig):
            raise TypeError("config must be FreshQuantileBankConfig")
        specs = tuple(measurements)
        if not specs or any(
            not isinstance(item, QuantileMeasurementSpec) for item in specs
        ):
            raise ValueError(
                "measurements must contain explicit QuantileMeasurementSpec values"
            )
        names = [item.name for item in specs]
        if len(names) != len(set(names)):
            raise ValueError("measurement names must be unique")
        self._config = config
        self._specs = specs
        self._anchors: list[str] = []
        self._last_anchor: date | None = None
        self._per_rank: dict[tuple[str, float], list[float]] = {
            (spec.name, rank): [] for spec in specs for rank in config.ranks
        }
        self._finite_counts = {spec.name: 0 for spec in specs}
        self._support_counts = {spec.name: 0 for spec in specs}
        self._maximum_session_support = {spec.name: 0 for spec in specs}
        self._frozen = False

    def add_session(self, anchor: str, frame: pd.DataFrame) -> None:
        if self._frozen:
            raise RuntimeError("quantile fitter has already been frozen")
        parsed = _session_anchor(anchor)
        if self._last_anchor is not None and parsed <= self._last_anchor:
            raise ValueError("training sessions must be unique and chronological")
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("every session value must be a pandas DataFrame")
        required = {
            "feature_ready",
            "gap_detected",
            *(item.column for item in self._specs),
        }
        missing = sorted(required.difference(frame.columns))
        if missing:
            raise ValueError(f"session {anchor} is missing columns: {missing}")
        ready = frame["feature_ready"].to_numpy(copy=False)
        gaps = frame["gap_detected"].to_numpy(copy=False)
        if any(not isinstance(value, (bool, np.bool_)) for value in ready) or any(
            not isinstance(value, (bool, np.bool_)) for value in gaps
        ):
            raise ValueError("feature_ready and gap_detected must be boolean")
        usable = np.asarray(ready, dtype=bool) & ~np.asarray(gaps, dtype=bool)
        for spec in self._specs:
            try:
                raw = frame[spec.column].to_numpy(dtype=float, copy=False)[usable]
            except (TypeError, ValueError) as exc:
                raise ValueError(f"measurement {spec.column} must be numeric") from exc
            if np.any(np.isinf(raw)):
                raise ValueError(f"measurement {spec.column} contains infinity")
            values = _transformed(raw, spec.transform)
            support = int(values.size)
            self._support_counts[spec.name] += support
            self._maximum_session_support[spec.name] = max(
                self._maximum_session_support[spec.name], support
            )
            if values.size < self._config.minimum_finite_values_per_session:
                continue
            self._finite_counts[spec.name] += int(values.size)
            quantiles = np.quantile(values, self._config.ranks, method="linear")
            for rank, value in zip(self._config.ranks, quantiles):
                self._per_rank[(spec.name, rank)].append(float(value))
        self._anchors.append(anchor)
        self._last_anchor = parsed

    def freeze(self) -> FreshQuantileBank:
        if self._frozen:
            raise RuntimeError("quantile fitter has already been frozen")
        self._frozen = True
        if not self._anchors:
            raise ValueError("at least one chronological training session is required")
        insufficient = []
        first_rank = self._config.ranks[0]
        for spec in self._specs:
            eligible_sessions = len(self._per_rank[(spec.name, first_rank)])
            if eligible_sessions < self._config.minimum_eligible_sessions:
                insufficient.append(
                    f"{spec.name!r}: {eligible_sessions} eligible sessions, "
                    f"{self._support_counts[spec.name]} total supporting values, "
                    f"{self._maximum_session_support[spec.name]} maximum in one session"
                )
        if insufficient:
            raise ValueError(
                "quantile support gate failed for " + "; ".join(insufficient)
            )
        thresholds: list[QuantileThreshold] = []
        for spec in self._specs:
            for rank in self._config.ranks:
                session_values = self._per_rank[(spec.name, rank)]
                value = float(np.median(np.asarray(session_values, dtype=float)))
                if not math.isfinite(value):
                    raise ValueError("fitted thresholds must be finite")
                thresholds.append(
                    QuantileThreshold(
                        measurement=spec.name,
                        rank=rank,
                        value=value,
                        eligible_session_count=len(session_values),
                        finite_value_count=self._finite_counts[spec.name],
                        minimum_session_quantile=min(session_values),
                        maximum_session_quantile=max(session_values),
                    )
                )
        anchors = tuple(self._anchors)
        digest = _bank_hash(
            config=self._config,
            anchors=anchors,
            specs=self._specs,
            thresholds=thresholds,
        )
        return FreshQuantileBank(
            config=self._config,
            training_session_anchors=anchors,
            measurements=self._specs,
            thresholds=tuple(thresholds),
            bank_sha256=digest,
        )


def fit_session_balanced_quantiles(
    frames: Mapping[str, pd.DataFrame],
    *,
    measurements: Sequence[QuantileMeasurementSpec],
    config: FreshQuantileBankConfig,
) -> FreshQuantileBank:
    """Fit exact per-session ranks, then take their cross-session median."""

    if not isinstance(frames, Mapping) or not frames:
        raise ValueError("frames must be a non-empty mapping of session anchors")
    fitter = SessionBalancedQuantileFitter(
        measurements=measurements,
        config=config,
    )
    for anchor, frame in frames.items():
        fitter.add_session(anchor, frame)
    return fitter.freeze()


def fresh_quantile_bank_payload(bank: FreshQuantileBank) -> dict[str, Any]:
    """Return the canonical JSON-compatible representation of a frozen bank."""

    if not isinstance(bank, FreshQuantileBank):
        raise TypeError("bank must be FreshQuantileBank")
    expected = _bank_hash(
        config=bank.config,
        anchors=bank.training_session_anchors,
        specs=bank.measurements,
        thresholds=bank.thresholds,
    )
    if bank.bank_sha256 != expected:
        raise ValueError("quantile bank hash does not match its contents")
    return {
        "schema": "fresh-session-balanced-quantiles/v1",
        "config": asdict(bank.config),
        "trainingSessionAnchors": list(bank.training_session_anchors),
        "measurements": [asdict(item) for item in bank.measurements],
        "thresholds": [asdict(item) for item in bank.thresholds],
        "bankSha256": bank.bank_sha256,
    }


def fresh_quantile_bank_from_payload(
    payload: Mapping[str, Any],
) -> FreshQuantileBank:
    """Reconstruct and hash-validate a persisted quantile bank."""

    if not isinstance(payload, Mapping):
        raise TypeError("quantile bank payload must be a mapping")
    expected_keys = {
        "schema",
        "config",
        "trainingSessionAnchors",
        "measurements",
        "thresholds",
        "bankSha256",
    }
    if set(payload) != expected_keys or payload.get("schema") != (
        "fresh-session-balanced-quantiles/v1"
    ):
        raise ValueError("quantile bank payload has an invalid schema")
    raw_config = payload["config"]
    raw_anchors = payload["trainingSessionAnchors"]
    raw_measurements = payload["measurements"]
    raw_thresholds = payload["thresholds"]
    if not isinstance(raw_config, Mapping):
        raise ValueError("quantile bank config must be a mapping")
    if not isinstance(raw_anchors, list):
        raise ValueError("quantile bank anchors must be a list")
    if not isinstance(raw_measurements, list) or not raw_measurements:
        raise ValueError("quantile bank measurements must be a non-empty list")
    if not isinstance(raw_thresholds, list) or not raw_thresholds:
        raise ValueError("quantile bank thresholds must be a non-empty list")
    try:
        config = FreshQuantileBankConfig(
            ranks=tuple(float(value) for value in raw_config["ranks"]),
            minimum_finite_values_per_session=int(
                raw_config["minimum_finite_values_per_session"]
            ),
            minimum_eligible_sessions=int(raw_config["minimum_eligible_sessions"]),
        )
        measurements = tuple(
            QuantileMeasurementSpec(
                name=str(item["name"]),
                column=str(item["column"]),
                transform=item["transform"],
            )
            for item in raw_measurements
        )
        thresholds = tuple(
            QuantileThreshold(
                measurement=str(item["measurement"]),
                rank=float(item["rank"]),
                value=float(item["value"]),
                eligible_session_count=int(item["eligible_session_count"]),
                finite_value_count=int(item["finite_value_count"]),
                minimum_session_quantile=float(item["minimum_session_quantile"]),
                maximum_session_quantile=float(item["maximum_session_quantile"]),
            )
            for item in raw_thresholds
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("quantile bank payload fields are invalid") from exc
    anchors = tuple(str(value) for value in raw_anchors)
    for anchor in anchors:
        _session_anchor(anchor)
    if tuple(sorted(anchors)) != anchors or len(set(anchors)) != len(anchors):
        raise ValueError("quantile bank anchors must be unique and chronological")
    names = tuple(item.name for item in measurements)
    if len(names) != len(set(names)):
        raise ValueError("quantile bank measurements must be unique")
    pairs = tuple((item.measurement, item.rank) for item in thresholds)
    expected_pairs = tuple(
        (measurement.name, rank)
        for measurement in measurements
        for rank in config.ranks
    )
    if pairs != expected_pairs:
        raise ValueError(
            "quantile bank thresholds must cover measurements and ranks in order"
        )
    claimed = str(payload["bankSha256"])
    actual = _bank_hash(
        config=config,
        anchors=anchors,
        specs=measurements,
        thresholds=thresholds,
    )
    if claimed != actual:
        raise ValueError("quantile bank hash does not match its contents")
    return FreshQuantileBank(
        config=config,
        training_session_anchors=anchors,
        measurements=measurements,
        thresholds=thresholds,
        bank_sha256=actual,
    )


__all__ = [
    "FreshQuantileBank",
    "FreshQuantileBankConfig",
    "QuantileMeasurementSpec",
    "QuantileThreshold",
    "SessionBalancedQuantileFitter",
    "fit_session_balanced_quantiles",
    "fresh_quantile_bank_from_payload",
    "fresh_quantile_bank_payload",
]
