"""Causal context enrichment and frozen event filters for fresh research.

The functions in this module consume already-frozen signal events and the
exact causal feature row on which each event was emitted.  They never accept
trade outcomes, future rows, labels, or P&L.  Regime boundaries are read from
one discovery-fitted, session-balanced quantile bank and are therefore frozen
before a later chronological window is evaluated.

An event keeps its original positional row, tick ID, timestamp, side, and
relative order.  Only a new ``context`` metadata mapping is attached.  The
broker-session anchor follows the 18:00--17:00 America/New_York schedule in
``fresh_sessions``; activity labels use civil Tokyo, London, and New York
hours with IANA daylight-saving conversion.
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from numbers import Integral, Real
from typing import Literal, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_sessions import assign_broker_timestamp
from datavis.research.fresh_thresholds import (
    FreshQuantileBank,
    QuantileMeasurementSpec,
)


FRESH_EVENT_FILTER_SCHEMA = "fresh-xauusd-event-filter/v1"
FRESH_EVENT_FILTER_VARIANT_BANK_SCHEMA = (
    "fresh-xauusd-event-filter-variant-bank/v1"
)
FRESH_REGIME_QUINTILE_RANKS = (0.20, 0.40, 0.60, 0.80)
MAXIMUM_DISCOVERY_CANDIDATES = 240
MAXIMUM_DISCOVERY_CANDIDATES_PER_FAMILY = 60

ActivityFilter = Literal[
    "all",
    "any-major-active",
    "opening-or-overlap",
]

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_TOKYO = ZoneInfo("Asia/Tokyo")
_LONDON = ZoneInfo("Europe/London")
_NEW_YORK = ZoneInfo("America/New_York")
_ACTIVITY_FILTERS = frozenset(
    {"all", "any-major-active", "opening-or-overlap"}
)
_BASE_COLUMNS = (
    "tick_id",
    "timestamp",
    "bid",
    "ask",
    "feature_ready",
    "gap_detected",
)


def _non_empty(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
    ):
        raise ValueError(f"{name} must be a non-empty, trimmed string")


def _rank(value: float | None, name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite rank in (0, 1)")
    if not math.isfinite(float(value)) or not 0.0 < float(value) < 1.0:
        raise ValueError(f"{name} must be a finite rank in (0, 1)")


def _measurement_name(dimension: str) -> str:
    return f"regime::{dimension}"


@dataclass(frozen=True, slots=True)
class FreshRegimeDefinition:
    """Exact causal columns used for four discovery-fitted regime labels."""

    volatility_column: str
    spread_column: str
    trend_column: str
    arrival_column: str

    def __post_init__(self) -> None:
        columns = (
            self.volatility_column,
            self.spread_column,
            self.trend_column,
            self.arrival_column,
        )
        if len(set(columns)) != len(columns):
            raise ValueError("regime measurement columns must be unique")
        # Constructing the specs applies the shared outcome-name guard.
        fresh_regime_quantile_measurements(self)


def fresh_regime_quantile_measurements(
    definition: FreshRegimeDefinition,
) -> tuple[QuantileMeasurementSpec, ...]:
    """Return the exact measurement specs needed for quintile fitting.

    Volatility and arrival rate use their positive magnitude, spread is used
    directly, and trend is deliberately direction-symmetric via absolute
    value.  These transformations are applied identically during fitting and
    event labelling.
    """

    if not isinstance(definition, FreshRegimeDefinition):
        raise TypeError("definition must be FreshRegimeDefinition")
    return (
        QuantileMeasurementSpec(
            _measurement_name("volatility"),
            definition.volatility_column,
            "positive",
        ),
        QuantileMeasurementSpec(
            _measurement_name("spread"),
            definition.spread_column,
            "identity",
        ),
        QuantileMeasurementSpec(
            _measurement_name("trend"),
            definition.trend_column,
            "absolute",
        ),
        QuantileMeasurementSpec(
            _measurement_name("arrival"),
            definition.arrival_column,
            "positive",
        ),
    )


@dataclass(frozen=True, slots=True)
class FreshEventFilterConfig:
    """A completely explicit activity and causal-regime event filter."""

    variant_id: str
    regime_definition: FreshRegimeDefinition
    activity_filter: ActivityFilter
    spread_ceiling_rank: float | None
    volatility_floor_rank: float | None

    def __post_init__(self) -> None:
        _non_empty(self.variant_id, "variant_id")
        if not isinstance(self.regime_definition, FreshRegimeDefinition):
            raise TypeError("regime_definition must be FreshRegimeDefinition")
        if self.activity_filter not in _ACTIVITY_FILTERS:
            raise ValueError(
                "activity_filter must be all, any-major-active, or "
                "opening-or-overlap"
            )
        _rank(self.spread_ceiling_rank, "spread_ceiling_rank")
        _rank(self.volatility_floor_rank, "volatility_floor_rank")


@dataclass(frozen=True, slots=True)
class FreshEventFilterResult:
    """Filtered events plus mutually exclusive rejection counts."""

    events: tuple[FrozenSignalEvent, ...]
    input_count: int
    rejected_activity_count: int
    rejected_spread_count: int
    rejected_volatility_count: int
    config_sha256: str
    quantile_bank_sha256: str

    @property
    def retained_count(self) -> int:
        return len(self.events)

    @property
    def rejected_count(self) -> int:
        return self.input_count - self.retained_count


@dataclass(frozen=True, slots=True)
class FreshEventFilterRequest:
    """One ordered event group and its frozen filter for batched enrichment."""

    events: tuple[FrozenSignalEvent, ...]
    config: FreshEventFilterConfig

    def __post_init__(self) -> None:
        try:
            selected = tuple(self.events)
        except TypeError as exc:
            raise TypeError("events must be an iterable of FrozenSignalEvent values") from exc
        for position, event in enumerate(selected):
            if not isinstance(event, FrozenSignalEvent):
                raise TypeError(f"events[{position}] is not a FrozenSignalEvent")
        if not isinstance(self.config, FreshEventFilterConfig):
            raise TypeError("config must be FreshEventFilterConfig")
        object.__setattr__(self, "events", selected)


@dataclass(frozen=True, slots=True)
class EventFilterVariantSource:
    """One already-registered entry eligible for bounded filter variants."""

    candidate_id: str
    family: str

    def __post_init__(self) -> None:
        _non_empty(self.candidate_id, "candidate_id")
        _non_empty(self.family, "family")


@dataclass(frozen=True, slots=True)
class FreshFilteredCandidateVariant:
    """One additional discovery candidate bound to a source entry."""

    candidate_id: str
    source_candidate_id: str
    family: str
    filter_config: FreshEventFilterConfig
    filter_config_sha256: str
    candidate_sha256: str


@dataclass(frozen=True, slots=True)
class FreshEventFilterVariantBank:
    """Deterministic, budget-audited post-discovery filter expansion."""

    schema: str
    quantile_bank_sha256: str
    already_registered_candidate_count: int
    registered_family_counts: tuple[tuple[str, int], ...]
    maximum_total_candidates: int
    maximum_candidates_per_family: int
    requested_additional_candidates: int
    variants: tuple[FreshFilteredCandidateVariant, ...]
    total_candidate_count: int
    final_family_counts: tuple[tuple[str, int], ...]
    variant_bank_sha256: str


@dataclass(frozen=True, slots=True)
class _PreparedRows:
    tick_ids: tuple[int, ...]
    timestamps: tuple[datetime, ...]
    timestamp_ns: np.ndarray
    bid: np.ndarray
    ask: np.ndarray
    ready: np.ndarray
    gaps: np.ndarray
    measurements: Mapping[str, np.ndarray]


@dataclass(frozen=True, slots=True)
class _Activity:
    label: str
    active_sessions: tuple[str, ...]
    opening_sessions: tuple[str, ...]

    @property
    def any_major_active(self) -> bool:
        return bool(self.active_sessions)

    @property
    def overlap(self) -> bool:
        return len(self.active_sessions) >= 2

    @property
    def opening_or_overlap(self) -> bool:
        return bool(self.opening_sessions) or self.overlap


def _bank_payload(bank: FreshQuantileBank) -> dict[str, object]:
    return {
        "schema": "fresh-session-balanced-quantiles/v1",
        "config": asdict(bank.config),
        "trainingSessionAnchors": list(bank.training_session_anchors),
        "measurements": [asdict(item) for item in bank.measurements],
        "thresholds": [asdict(item) for item in bank.thresholds],
    }


def _validate_bank(
    bank: FreshQuantileBank,
    definition: FreshRegimeDefinition,
    extra_ranks: Sequence[float | None] = (),
) -> None:
    if not isinstance(bank, FreshQuantileBank):
        raise TypeError("bank must be FreshQuantileBank")
    if (
        not isinstance(bank.bank_sha256, str)
        or _SHA256.fullmatch(bank.bank_sha256.lower()) is None
    ):
        raise ValueError("quantile bank must have a SHA-256 digest")
    if canonical_hash(_bank_payload(bank)) != bank.bank_sha256.lower():
        raise ValueError("quantile bank hash does not match its contents")

    actual: dict[str, QuantileMeasurementSpec] = {}
    for spec in bank.measurements:
        if spec.name in actual:
            raise ValueError("quantile bank measurement names must be unique")
        actual[spec.name] = spec
    required = fresh_regime_quantile_measurements(definition)
    for spec in required:
        if actual.get(spec.name) != spec:
            raise ValueError(
                "quantile bank is missing the exact regime measurement "
                f"{spec.name!r}"
            )

    ranks = {
        round(float(value), 12)
        for value in bank.config.ranks
        if math.isfinite(float(value))
    }
    wanted = (*FRESH_REGIME_QUINTILE_RANKS, *extra_ranks)
    missing = [
        float(value)
        for value in wanted
        if value is not None and round(float(value), 12) not in ranks
    ]
    if missing:
        raise ValueError(f"quantile bank is missing required ranks: {missing}")

    threshold_keys: set[tuple[str, float]] = set()
    for threshold in bank.thresholds:
        key = (threshold.measurement, round(float(threshold.rank), 12))
        if key in threshold_keys:
            raise ValueError("quantile bank contains duplicate thresholds")
        threshold_keys.add(key)
        numeric = (
            threshold.value,
            threshold.minimum_session_quantile,
            threshold.maximum_session_quantile,
        )
        if any(not math.isfinite(float(value)) for value in numeric):
            raise ValueError("quantile bank thresholds must be finite")
    for spec in required:
        for rank in wanted:
            if rank is None:
                continue
            key = (spec.name, round(float(rank), 12))
            if key not in threshold_keys:
                raise ValueError(f"quantile bank is missing threshold {key!r}")


def fresh_event_filter_config_fingerprint(
    config: FreshEventFilterConfig,
    bank: FreshQuantileBank,
) -> str:
    """Hash every filter choice together with its frozen threshold bank."""

    if not isinstance(config, FreshEventFilterConfig):
        raise TypeError("config must be FreshEventFilterConfig")
    _validate_bank(
        bank,
        config.regime_definition,
        (config.spread_ceiling_rank, config.volatility_floor_rank),
    )
    return canonical_hash(
        {
            "schema": FRESH_EVENT_FILTER_SCHEMA,
            "quantileBankSha256": bank.bank_sha256.lower(),
            "config": asdict(config),
        }
    )


def _aware_timestamp(value: object, position: int) -> tuple[datetime, int]:
    try:
        point = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"timestamp at row {position} is invalid") from exc
    if pd.isna(point) or point.tzinfo is None or point.utcoffset() is None:
        raise ValueError(f"timestamp at row {position} must be timezone-aware")
    return point.to_pydatetime(), int(point.tz_convert("UTC").value)


def _boolean_column(frame: pd.DataFrame, column: str) -> np.ndarray:
    raw = frame[column].to_numpy(copy=False)
    if any(not isinstance(value, (bool, np.bool_)) for value in raw):
        raise ValueError(f"{column} must contain only booleans")
    return np.asarray(raw, dtype=bool)


def _prepare_rows(
    features: pd.DataFrame,
    definition: FreshRegimeDefinition,
    *,
    row_limit: int,
) -> _PreparedRows:
    if not isinstance(features, pd.DataFrame):
        raise TypeError("features must be a pandas DataFrame")
    if not isinstance(row_limit, int) or isinstance(row_limit, bool) or row_limit < 0:
        raise ValueError("row_limit must be a non-negative integer")
    # Do not even validate feature values after the last requested event.  A
    # future malformed or outcome-bearing row therefore cannot influence an
    # earlier frozen decision.
    frame = features.iloc[:row_limit]
    specs = fresh_regime_quantile_measurements(definition)
    required = tuple(
        dict.fromkeys((*_BASE_COLUMNS, *(spec.column for spec in specs)))
    )
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"feature frame is missing columns: {', '.join(missing)}")

    tick_ids: list[int] = []
    timestamps: list[datetime] = []
    timestamp_ns = np.empty(len(frame), dtype=np.int64)
    previous: tuple[int, int] | None = None
    for position, (raw_id, raw_time) in enumerate(
        zip(
            frame["tick_id"].to_numpy(copy=False),
            frame["timestamp"].to_numpy(copy=False),
        )
    ):
        if not isinstance(raw_id, Integral) or isinstance(
            raw_id, (bool, np.bool_)
        ):
            raise ValueError(f"tick_id at row {position} must be an integer")
        tick_id = int(raw_id)
        timestamp, point_ns = _aware_timestamp(raw_time, position)
        key = (point_ns, tick_id)
        if previous is not None and key <= previous:
            raise ValueError(
                "feature rows must be strictly ordered by (timestamp, id)"
            )
        previous = key
        tick_ids.append(tick_id)
        timestamps.append(timestamp)
        timestamp_ns[position] = point_ns
    if len(set(tick_ids)) != len(tick_ids):
        raise ValueError("feature frame contains duplicate tick IDs")

    numeric: dict[str, np.ndarray] = {}
    for column in ("bid", "ask", *(spec.column for spec in specs)):
        try:
            values = frame[column].to_numpy(dtype=float, copy=True)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{column} must be numeric") from exc
        if np.any(np.isinf(values)):
            raise ValueError(f"{column} contains infinity")
        numeric[column] = values
    if np.any(~np.isfinite(numeric["bid"])) or np.any(
        ~np.isfinite(numeric["ask"])
    ):
        raise ValueError("bid and ask must be finite")
    if np.any(numeric["ask"] < numeric["bid"]):
        raise ValueError("ask must be greater than or equal to bid")

    return _PreparedRows(
        tick_ids=tuple(tick_ids),
        timestamps=tuple(timestamps),
        timestamp_ns=timestamp_ns,
        bid=numeric.pop("bid"),
        ask=numeric.pop("ask"),
        ready=_boolean_column(frame, "feature_ready"),
        gaps=_boolean_column(frame, "gap_detected"),
        measurements=numeric,
    )


def _validate_event_bindings(
    rows: _PreparedRows,
    events: Sequence[FrozenSignalEvent],
) -> None:
    previous_index = -1
    for position, event in enumerate(events):
        if not isinstance(event, FrozenSignalEvent):
            raise TypeError(f"events[{position}] is not a FrozenSignalEvent")
        if event.tick_index < previous_index:
            raise ValueError("events must be ordered by non-decreasing tick_index")
        previous_index = event.tick_index
        if event.tick_index >= len(rows.tick_ids):
            raise ValueError(f"event {position} points outside the feature frame")
        index = event.tick_index
        if event.tick_id != rows.tick_ids[index]:
            raise ValueError(f"event {position} tick_id does not match its row")
        _, event_ns = _aware_timestamp(event.timestamp, position)
        if event_ns != int(rows.timestamp_ns[index]):
            raise ValueError(f"event {position} timestamp does not match its row")
        if rows.gaps[index] or not rows.ready[index]:
            raise ValueError(f"event {position} is bound to an unusable feature row")
        if "context" in event.metadata:
            raise ValueError("event metadata already contains reserved context")


def _civil_state(
    timestamp: datetime,
    zone: ZoneInfo,
    start_hour: int,
    end_hour: int,
) -> tuple[bool, bool]:
    local = timestamp.astimezone(zone)
    seconds = (
        local.hour * 3_600
        + local.minute * 60
        + local.second
        + local.microsecond / 1_000_000
    )
    weekday = local.weekday() < 5
    active = weekday and start_hour * 3_600 <= seconds < end_hour * 3_600
    opening = (
        weekday
        and start_hour * 3_600 <= seconds < (start_hour + 1) * 3_600
    )
    return active, opening


def _market_activity(timestamp: datetime) -> _Activity:
    definitions = (
        ("tokyo", _TOKYO, 9, 18),
        ("london", _LONDON, 8, 17),
        ("new-york", _NEW_YORK, 8, 17),
    )
    active: list[str] = []
    opening: list[str] = []
    for name, zone, start, end in definitions:
        is_active, is_opening = _civil_state(timestamp, zone, start, end)
        if is_active:
            active.append(name)
        if is_opening:
            opening.append(name)

    if len(active) >= 2:
        label = "+".join(active) + "-overlap"
        if opening:
            label += "|" + "+".join(opening) + "-opening"
    elif opening:
        label = "+".join(opening) + "-opening"
    elif active:
        label = "+".join(active) + "-active"
    else:
        label = "off-major"
    return _Activity(label, tuple(active), tuple(opening))


def _transform(value: float, transform: str) -> float:
    if transform == "absolute":
        return abs(value)
    # A non-positive positive-transform value is causally below every fitted
    # positive quantile and is therefore assigned to quintile 1.
    return value


def _threshold(bank: FreshQuantileBank, measurement: str, rank: float) -> float:
    try:
        value = float(bank.threshold(measurement, rank))
    except KeyError as exc:  # pragma: no cover - guarded by bank validation
        raise ValueError(f"missing fitted threshold {(measurement, rank)!r}") from exc
    if not math.isfinite(value):  # pragma: no cover - guarded by validation
        raise ValueError("fitted threshold must be finite")
    return value


def _quintile(
    value: float,
    measurement: str,
    bank: FreshQuantileBank,
) -> int:
    boundaries = tuple(
        _threshold(bank, measurement, rank)
        for rank in FRESH_REGIME_QUINTILE_RANKS
    )
    if tuple(sorted(boundaries)) != boundaries:
        raise ValueError(f"quintile thresholds for {measurement!r} are not ordered")
    return int(np.searchsorted(boundaries, value, side="left")) + 1


def _activity_passes(activity: _Activity, policy: ActivityFilter) -> bool:
    if policy == "all":
        return True
    if policy == "any-major-active":
        return activity.any_major_active
    return activity.opening_or_overlap


def _enrich_and_filter_prepared(
    rows: _PreparedRows,
    selected_events: tuple[FrozenSignalEvent, ...],
    *,
    config: FreshEventFilterConfig,
    quantile_bank: FreshQuantileBank,
    config_sha: str,
) -> FreshEventFilterResult:
    specs = fresh_regime_quantile_measurements(config.regime_definition)
    specs_by_dimension = dict(
        zip(("volatility", "spread", "trend", "arrival"), specs)
    )
    spread_threshold = (
        None
        if config.spread_ceiling_rank is None
        else _threshold(
            quantile_bank,
            specs_by_dimension["spread"].name,
            config.spread_ceiling_rank,
        )
    )
    volatility_threshold = (
        None
        if config.volatility_floor_rank is None
        else _threshold(
            quantile_bank,
            specs_by_dimension["volatility"].name,
            config.volatility_floor_rank,
        )
    )

    retained: list[FrozenSignalEvent] = []
    rejected_activity = 0
    rejected_spread = 0
    rejected_volatility = 0
    for event in selected_events:
        index = event.tick_index
        timestamp = rows.timestamps[index]
        assignment = assign_broker_timestamp(timestamp)
        if (
            assignment.status != "session"
            or assignment.anchor is None
            or assignment.bounds is None
        ):
            raise ValueError(
                "signal events must be inside a scheduled broker session"
            )
        activity = _market_activity(timestamp)

        values: dict[str, float] = {}
        quintiles: dict[str, int] = {}
        regime_detail: dict[str, dict[str, object]] = {}
        for dimension, spec in specs_by_dimension.items():
            raw = float(rows.measurements[spec.column][index])
            if not math.isfinite(raw):
                raise ValueError(
                    f"event row {index} has no finite {dimension} measurement"
                )
            transformed = _transform(raw, spec.transform)
            quintile = _quintile(transformed, spec.name, quantile_bank)
            values[dimension] = transformed
            quintiles[dimension] = quintile
            regime_detail[dimension] = {
                "measurement": spec.name,
                "column": spec.column,
                "transform": spec.transform,
                "value": transformed,
                "quintile": f"q{quintile}",
                "quintileNumber": quintile,
            }

        if not _activity_passes(activity, config.activity_filter):
            rejected_activity += 1
            continue
        if spread_threshold is not None and values["spread"] > spread_threshold:
            rejected_spread += 1
            continue
        if (
            volatility_threshold is not None
            and values["volatility"] < volatility_threshold
        ):
            rejected_volatility += 1
            continue

        combined_regime = "|".join(
            f"{dimension}:q{quintiles[dimension]}"
            for dimension in ("volatility", "spread", "trend", "arrival")
        )
        context = {
            "schema": FRESH_EVENT_FILTER_SCHEMA,
            "day": assignment.anchor.isoformat(),
            "sessionAnchor": assignment.anchor.isoformat(),
            "timestampSydney": assignment.timestamp_sydney.isoformat(),
            "timestampNewYork": assignment.timestamp_new_york.isoformat(),
            "brokerStatus": assignment.status,
            "brokerSessionAnchorTimeZone": "America/New_York",
            "brokerSessionStartNewYork": assignment.bounds.start_new_york.isoformat(),
            "brokerSessionEndNewYork": assignment.bounds.end_new_york.isoformat(),
            "brokerSessionStartSydney": assignment.bounds.start_sydney.isoformat(),
            "brokerSessionEndSydney": assignment.bounds.end_sydney.isoformat(),
            "session": activity.label,
            "marketSession": activity.label,
            "activeMajorSessions": list(activity.active_sessions),
            "openingSessions": list(activity.opening_sessions),
            "majorSessionOverlap": activity.overlap,
            "regime": combined_regime,
            "regimes": regime_detail,
            "eventFilter": {
                "variantId": config.variant_id,
                "activityFilter": config.activity_filter,
                "spreadCeilingRank": config.spread_ceiling_rank,
                "spreadCeilingValue": spread_threshold,
                "volatilityFloorRank": config.volatility_floor_rank,
                "volatilityFloorValue": volatility_threshold,
                "configSha256": config_sha,
                "quantileBankSha256": quantile_bank.bank_sha256.lower(),
            },
        }
        metadata = dict(event.metadata)
        metadata["context"] = context
        retained.append(
            FrozenSignalEvent(
                tick_index=event.tick_index,
                tick_id=event.tick_id,
                timestamp=event.timestamp,
                side=event.side,
                metadata=metadata,
            )
        )

    return FreshEventFilterResult(
        events=tuple(retained),
        input_count=len(selected_events),
        rejected_activity_count=rejected_activity,
        rejected_spread_count=rejected_spread,
        rejected_volatility_count=rejected_volatility,
        config_sha256=config_sha,
        quantile_bank_sha256=quantile_bank.bank_sha256.lower(),
    )


def enrich_and_filter_frozen_events(
    features: pd.DataFrame,
    events: Sequence[FrozenSignalEvent],
    *,
    config: FreshEventFilterConfig,
    quantile_bank: FreshQuantileBank,
) -> FreshEventFilterResult:
    """Attach causal context and apply one frozen filter to exact signal rows.

    Rejection counts use fixed precedence: activity, then spread, then
    volatility.  This makes them mutually exclusive and exactly reconcilable
    with the retained count.
    """

    if not isinstance(config, FreshEventFilterConfig):
        raise TypeError("config must be FreshEventFilterConfig")
    selected_events = tuple(events)
    for position, event in enumerate(selected_events):
        if not isinstance(event, FrozenSignalEvent):
            raise TypeError(f"events[{position}] is not a FrozenSignalEvent")
    row_limit = (
        max(event.tick_index for event in selected_events) + 1
        if selected_events
        else 0
    )
    config_sha = fresh_event_filter_config_fingerprint(config, quantile_bank)
    rows = _prepare_rows(
        features,
        config.regime_definition,
        row_limit=row_limit,
    )
    _validate_event_bindings(rows, selected_events)
    return _enrich_and_filter_prepared(
        rows,
        selected_events,
        config=config,
        quantile_bank=quantile_bank,
        config_sha=config_sha,
    )


def enrich_and_filter_frozen_event_batch(
    features: pd.DataFrame,
    requests: Sequence[FreshEventFilterRequest],
    *,
    quantile_bank: FreshQuantileBank,
) -> tuple[FreshEventFilterResult, ...]:
    """Enrich ordered request groups after preparing their shared rows once.

    Every result is identical to an independent scalar call and occupies the
    same position as its request.  Preparation stops immediately after the
    latest event requested by the whole batch, so appended feature rows remain
    causally invisible.  All requests must use one regime definition because
    their prepared measurement arrays are shared.
    """

    try:
        selected_requests = tuple(requests)
    except TypeError as exc:
        raise TypeError(
            "requests must be an iterable of FreshEventFilterRequest values"
        ) from exc
    for position, request in enumerate(selected_requests):
        if not isinstance(request, FreshEventFilterRequest):
            raise TypeError(
                f"requests[{position}] is not a FreshEventFilterRequest"
            )
    if not selected_requests:
        return ()

    definition = selected_requests[0].config.regime_definition
    if any(
        request.config.regime_definition != definition
        for request in selected_requests[1:]
    ):
        raise ValueError("all batch requests must use the same regime definition")

    config_hashes: dict[FreshEventFilterConfig, str] = {}
    for request in selected_requests:
        if request.config not in config_hashes:
            config_hashes[request.config] = fresh_event_filter_config_fingerprint(
                request.config, quantile_bank
            )

    last_index = max(
        (
            event.tick_index
            for request in selected_requests
            for event in request.events
        ),
        default=-1,
    )
    rows = _prepare_rows(features, definition, row_limit=last_index + 1)

    results: list[FreshEventFilterResult] = []
    for request in selected_requests:
        _validate_event_bindings(rows, request.events)
        results.append(
            _enrich_and_filter_prepared(
                rows,
                request.events,
                config=request.config,
                quantile_bank=quantile_bank,
                config_sha=config_hashes[request.config],
            )
        )
    return tuple(results)


def _filter_catalog(
    definition: FreshRegimeDefinition,
) -> tuple[FreshEventFilterConfig, ...]:
    """Return a fixed low-complexity catalogue; never outcome-ranked."""

    choices = (
        ("major-active", "any-major-active", None, None),
        ("opening-overlap", "opening-or-overlap", None, None),
        ("spread-q4-ceiling", "all", 0.80, None),
        ("volatility-q1-floor", "all", None, 0.20),
        ("active-spread-q4", "any-major-active", 0.80, None),
        ("active-volatility-q1", "any-major-active", None, 0.20),
        ("opening-spread-q4", "opening-or-overlap", 0.80, None),
        ("opening-volatility-q1", "opening-or-overlap", None, 0.20),
        ("spread-q4-volatility-q1", "all", 0.80, 0.20),
        ("active-spread-q4-volatility-q1", "any-major-active", 0.80, 0.20),
        ("opening-spread-q4-volatility-q1", "opening-or-overlap", 0.80, 0.20),
        ("spread-q3-ceiling", "all", 0.60, None),
        ("volatility-q2-floor", "all", None, 0.40),
        ("active-spread-q3", "any-major-active", 0.60, None),
        ("active-volatility-q2", "any-major-active", None, 0.40),
        ("opening-spread-q3", "opening-or-overlap", 0.60, None),
        ("opening-volatility-q2", "opening-or-overlap", None, 0.40),
        ("spread-q3-volatility-q2", "all", 0.60, 0.40),
    )
    return tuple(
        FreshEventFilterConfig(
            variant_id=variant_id,
            regime_definition=definition,
            activity_filter=activity,  # type: ignore[arg-type]
            spread_ceiling_rank=spread_rank,
            volatility_floor_rank=volatility_rank,
        )
        for variant_id, activity, spread_rank, volatility_rank in choices
    )


def derive_bounded_post_discovery_variant_bank(
    quantile_bank: FreshQuantileBank,
    *,
    regime_definition: FreshRegimeDefinition,
    source_candidates: Sequence[EventFilterVariantSource],
    already_registered_candidate_count: int,
    registered_family_counts: Mapping[str, int],
    requested_additional_candidates: int,
    maximum_total_candidates: int = MAXIMUM_DISCOVERY_CANDIDATES,
    maximum_candidates_per_family: int = (
        MAXIMUM_DISCOVERY_CANDIDATES_PER_FAMILY
    ),
) -> FreshEventFilterVariantBank:
    """Expand selected entries without exceeding total or family budgets.

    The function accepts candidate identities and counts, but deliberately has
    no parameter for scores or outcomes.  Catalogue rounds are applied evenly
    across sources in sorted identity order; a family at its cap is skipped.
    Each returned record consumes exactly one additional discovery-candidate
    slot.
    """

    _validate_bank(quantile_bank, regime_definition, FRESH_REGIME_QUINTILE_RANKS)
    for name, value, allow_zero in (
        (
            "already_registered_candidate_count",
            already_registered_candidate_count,
            True,
        ),
        ("requested_additional_candidates", requested_additional_candidates, True),
        ("maximum_total_candidates", maximum_total_candidates, False),
        (
            "maximum_candidates_per_family",
            maximum_candidates_per_family,
            False,
        ),
    ):
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < (0 if allow_zero else 1)
        ):
            qualifier = "non-negative" if allow_zero else "positive"
            raise ValueError(f"{name} must be a {qualifier} integer")
    if already_registered_candidate_count > maximum_total_candidates:
        raise ValueError("registered candidates already exceed the total budget")
    if not isinstance(registered_family_counts, Mapping):
        raise TypeError("registered_family_counts must be a mapping")

    family_counts: dict[str, int] = {}
    for family, count in registered_family_counts.items():
        _non_empty(family, "registered family")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("registered family counts must be non-negative integers")
        if count > maximum_candidates_per_family:
            raise ValueError("a registered family already exceeds its budget")
        family_counts[family] = count
    if sum(family_counts.values()) != already_registered_candidate_count:
        raise ValueError(
            "registered family counts must sum to the registered total"
        )

    sources = tuple(source_candidates)
    if any(not isinstance(source, EventFilterVariantSource) for source in sources):
        raise TypeError(
            "source_candidates must contain EventFilterVariantSource values"
        )
    source_ids = [source.candidate_id for source in sources]
    if len(source_ids) != len(set(source_ids)):
        raise ValueError("source candidate IDs must be unique")
    for source in sources:
        if family_counts.get(source.family, 0) <= 0:
            raise ValueError(
                "every source family must have an already-registered candidate"
            )
    ordered_sources = tuple(sorted(sources, key=lambda item: (item.family, item.candidate_id)))

    total_room = maximum_total_candidates - already_registered_candidate_count
    additional_limit = min(requested_additional_candidates, total_room)
    variants: list[FreshFilteredCandidateVariant] = []
    for config in _filter_catalog(regime_definition):
        config_sha = fresh_event_filter_config_fingerprint(config, quantile_bank)
        for source in ordered_sources:
            if len(variants) >= additional_limit:
                break
            if family_counts[source.family] >= maximum_candidates_per_family:
                continue
            candidate_id = (
                f"{source.candidate_id}::event-filter::{config.variant_id}"
            )
            candidate_sha = canonical_hash(
                {
                    "kind": "fresh-filtered-entry-candidate",
                    "sourceCandidateId": source.candidate_id,
                    "family": source.family,
                    "candidateId": candidate_id,
                    "eventFilterSha256": config_sha,
                }
            )
            variants.append(
                FreshFilteredCandidateVariant(
                    candidate_id=candidate_id,
                    source_candidate_id=source.candidate_id,
                    family=source.family,
                    filter_config=config,
                    filter_config_sha256=config_sha,
                    candidate_sha256=candidate_sha,
                )
            )
            family_counts[source.family] += 1
        if len(variants) >= additional_limit:
            break

    registered_counts = tuple(sorted(registered_family_counts.items()))
    final_counts = tuple(sorted(family_counts.items()))
    total = already_registered_candidate_count + len(variants)
    payload = {
        "schema": FRESH_EVENT_FILTER_VARIANT_BANK_SCHEMA,
        "quantileBankSha256": quantile_bank.bank_sha256.lower(),
        "alreadyRegisteredCandidateCount": already_registered_candidate_count,
        "registeredFamilyCounts": [list(item) for item in registered_counts],
        "maximumTotalCandidates": maximum_total_candidates,
        "maximumCandidatesPerFamily": maximum_candidates_per_family,
        "requestedAdditionalCandidates": requested_additional_candidates,
        "variants": [
            {
                "candidateId": item.candidate_id,
                "sourceCandidateId": item.source_candidate_id,
                "family": item.family,
                "filterConfig": asdict(item.filter_config),
                "filterConfigSha256": item.filter_config_sha256,
                "candidateSha256": item.candidate_sha256,
            }
            for item in variants
        ],
        "totalCandidateCount": total,
        "finalFamilyCounts": [list(item) for item in final_counts],
    }
    return FreshEventFilterVariantBank(
        schema=FRESH_EVENT_FILTER_VARIANT_BANK_SCHEMA,
        quantile_bank_sha256=quantile_bank.bank_sha256.lower(),
        already_registered_candidate_count=already_registered_candidate_count,
        registered_family_counts=registered_counts,
        maximum_total_candidates=maximum_total_candidates,
        maximum_candidates_per_family=maximum_candidates_per_family,
        requested_additional_candidates=requested_additional_candidates,
        variants=tuple(variants),
        total_candidate_count=total,
        final_family_counts=final_counts,
        variant_bank_sha256=canonical_hash(payload),
    )


__all__ = [
    "FRESH_EVENT_FILTER_SCHEMA",
    "FRESH_EVENT_FILTER_VARIANT_BANK_SCHEMA",
    "FRESH_REGIME_QUINTILE_RANKS",
    "MAXIMUM_DISCOVERY_CANDIDATES",
    "MAXIMUM_DISCOVERY_CANDIDATES_PER_FAMILY",
    "EventFilterVariantSource",
    "FreshEventFilterConfig",
    "FreshEventFilterRequest",
    "FreshEventFilterResult",
    "FreshEventFilterVariantBank",
    "FreshFilteredCandidateVariant",
    "FreshRegimeDefinition",
    "derive_bounded_post_discovery_variant_bank",
    "enrich_and_filter_frozen_event_batch",
    "enrich_and_filter_frozen_events",
    "fresh_event_filter_config_fingerprint",
    "fresh_regime_quantile_measurements",
]
