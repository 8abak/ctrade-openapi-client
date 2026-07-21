"""Exact session tapes and aggregation helpers for chronological research.

This module contains no strategy selection.  It is the narrow bridge between
the audited database stream and the feature/diagnostic/replay layers.  Every
loaded session must match the strategy-neutral normalized-corpus fingerprint
frozen before outcomes are inspected.
"""

from __future__ import annotations

import math
from collections import Counter
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Callable, Mapping, Protocol

import pandas as pd

from datavis.research.fresh_data import FreshDataConfig
from datavis.research.fresh_decisions import CausalDecisionFeatureRow
from datavis.research.fresh_entry_diagnostics import (
    EntryDiagnosticRejection,
    FilledEntryDiagnostic,
    FreshEntryDiagnosticsResult,
)
from datavis.research.fresh_exits import VolatilityRow
from datavis.research.fresh_inventory import scan_and_fingerprint_db_session
from datavis.research.fresh_sessions import BrokerSessionBounds, broker_session_bounds
from datavis.research.ticks import Tick


class NamedCursorConnection(Protocol):
    autocommit: bool

    def cursor(self, *, name: str) -> Any: ...


ConnectionContextFactory = Callable[
    [], AbstractContextManager[NamedCursorConnection]
]


@dataclass(frozen=True, slots=True)
class FrozenSessionCorpusBinding:
    session_anchor: str
    normalized_quote_count: int
    normalized_sha256: str
    eligible: bool

    def __post_init__(self) -> None:
        parsed = date.fromisoformat(self.session_anchor)
        if parsed.weekday() >= 5 or parsed.isoformat() != self.session_anchor:
            raise ValueError("session_anchor must be a canonical weekday ISO date")
        if (
            not isinstance(self.normalized_quote_count, int)
            or isinstance(self.normalized_quote_count, bool)
            or self.normalized_quote_count < 0
        ):
            raise ValueError("normalized_quote_count must be a non-negative integer")
        if (
            not isinstance(self.normalized_sha256, str)
            or len(self.normalized_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.normalized_sha256)
        ):
            raise ValueError("normalized_sha256 must be a lowercase SHA-256 digest")
        if not isinstance(self.eligible, bool):
            raise ValueError("eligible must be boolean")


@dataclass(frozen=True, slots=True)
class FreshSessionTape:
    anchor: str
    bounds: BrokerSessionBounds
    ticks: tuple[Tick, ...]
    normalized_sha256: str

    def __post_init__(self) -> None:
        parsed = date.fromisoformat(self.anchor)
        if self.bounds.anchor != parsed:
            raise ValueError("session tape bounds do not match its anchor")
        if not self.ticks:
            raise ValueError("an eligible session tape cannot be empty")
        previous: tuple[Any, int] | None = None
        seen_ids: set[int] = set()
        for position, tick in enumerate(self.ticks):
            if not isinstance(tick, Tick):
                raise TypeError(f"ticks[{position}] must be Tick")
            if not self.bounds.contains(tick.timestamp):
                raise ValueError("session tape contains a tick outside its bounds")
            if tick.id in seen_ids:
                raise ValueError(f"duplicate tick id in session tape: {tick.id}")
            seen_ids.add(tick.id)
            key = (tick.timestamp, tick.id)
            if previous is not None and key <= previous:
                raise ValueError("session ticks must be strictly ordered")
            previous = key


class FreshSessionSource(Protocol):
    def load_session(self, session_anchor: str) -> FreshSessionTape: ...


def corpus_bindings_from_manifest(
    corpus_manifest: Mapping[str, Any],
) -> dict[str, FrozenSessionCorpusBinding]:
    if not isinstance(corpus_manifest, Mapping):
        raise TypeError("corpus_manifest must be a mapping")
    raw_sessions = corpus_manifest.get("sessions")
    if not isinstance(raw_sessions, list) or not raw_sessions:
        raise ValueError("corpus manifest must contain session bindings")
    output: dict[str, FrozenSessionCorpusBinding] = {}
    for raw in raw_sessions:
        if not isinstance(raw, Mapping):
            raise ValueError("corpus session bindings must be mappings")
        binding = FrozenSessionCorpusBinding(
            session_anchor=str(raw.get("sessionAnchor")),
            normalized_quote_count=int(raw.get("normalizedQuoteCount")),
            normalized_sha256=str(raw.get("normalizedSha256")),
            eligible=raw.get("eligible"),
        )
        if binding.session_anchor in output:
            raise ValueError("corpus session anchors must be unique")
        output[binding.session_anchor] = binding
    if tuple(output) != tuple(sorted(output)):
        raise ValueError("corpus session bindings must be chronological")
    return output


class FreshDbSessionSource:
    """Load one exact eligible session and verify its frozen corpus binding."""

    def __init__(
        self,
        *,
        connection_context_factory: ConnectionContextFactory,
        data_config: FreshDataConfig,
        corpus_manifest: Mapping[str, Any],
    ) -> None:
        if not callable(connection_context_factory):
            raise TypeError("connection_context_factory must be callable")
        if not isinstance(data_config, FreshDataConfig):
            raise TypeError("data_config must be FreshDataConfig")
        self._connection_context_factory = connection_context_factory
        self._data_config = data_config
        self._bindings = corpus_bindings_from_manifest(corpus_manifest)

    def load_session(self, session_anchor: str) -> FreshSessionTape:
        binding = self._bindings.get(session_anchor)
        if binding is None:
            raise PermissionError("session is not bound by the frozen corpus")
        if not binding.eligible:
            raise PermissionError("an ineligible session cannot be used for outcomes")
        ticks: list[Tick] = []
        with self._connection_context_factory() as connection:
            scanned = scan_and_fingerprint_db_session(
                connection,
                session_anchor,
                config=self._data_config,
                cursor_name=f"fresh_research_{session_anchor.replace('-', '')}",
                on_tick=lambda assigned: ticks.append(
                    Tick(
                        id=assigned.tick.id,
                        timestamp=assigned.tick.timestamp_utc,
                        bid=float(assigned.tick.bid),
                        ask=float(assigned.tick.ask),
                    )
                ),
            )
        if not scanned.inventory.is_complete:
            raise RuntimeError("a frozen eligible session failed live source QC")
        if scanned.inventory.normalized_quote_count != binding.normalized_quote_count:
            raise RuntimeError("session quote count differs from the frozen corpus")
        if scanned.normalized_sha256 != binding.normalized_sha256:
            raise RuntimeError("session fingerprint differs from the frozen corpus")
        return FreshSessionTape(
            anchor=session_anchor,
            bounds=broker_session_bounds(session_anchor),
            ticks=tuple(ticks),
            normalized_sha256=scanned.normalized_sha256,
        )


def combine_entry_diagnostics(
    results: tuple[FreshEntryDiagnosticsResult, ...],
) -> FreshEntryDiagnosticsResult:
    """Concatenate per-session diagnostics and rebuild global event positions."""

    diagnostics: list[FilledEntryDiagnostic] = []
    rejections: list[EntryDiagnosticRejection] = []
    reasons: Counter[str] = Counter()
    offset = 0
    for result in results:
        if not isinstance(result, FreshEntryDiagnosticsResult):
            raise TypeError("results must contain FreshEntryDiagnosticsResult values")
        for item in result.diagnostics:
            diagnostics.append(replace(item, event_position=item.event_position + offset))
        for item in result.rejections:
            rejections.append(replace(item, event_position=item.event_position + offset))
        reasons.update(result.rejected_reason_counts)
        offset += result.event_count
    ordered_diagnostics = tuple(sorted(diagnostics, key=lambda item: item.event_position))
    ordered_rejections = tuple(sorted(rejections, key=lambda item: item.event_position))
    return FreshEntryDiagnosticsResult(
        diagnostics=ordered_diagnostics,
        rejections=ordered_rejections,
        rejected_reason_counts=dict(sorted(reasons.items())),
        event_count=offset,
    )


def decision_feature_rows(
    features: pd.DataFrame,
    *,
    velocity_column: str,
    acceleration_column: str,
) -> tuple[CausalDecisionFeatureRow, ...]:
    """Bind weakening measurements to every exact feature/tick row."""

    required = {
        "tick_id",
        "timestamp",
        "bid",
        "ask",
        velocity_column,
        acceleration_column,
    }
    missing = sorted(required.difference(features.columns))
    if missing:
        raise ValueError(f"decision features are missing columns: {missing}")
    tick_ids = features["tick_id"].to_numpy(copy=False)
    timestamps = features["timestamp"].to_numpy(copy=False)
    bids = features["bid"].to_numpy(dtype=float, copy=False)
    asks = features["ask"].to_numpy(dtype=float, copy=False)
    velocities = features[velocity_column].to_numpy(dtype=float, copy=False)
    accelerations = features[acceleration_column].to_numpy(
        dtype=float, copy=False
    )
    output: list[CausalDecisionFeatureRow] = []
    for index in range(len(features)):
        velocity = float(velocities[index])
        acceleration = float(accelerations[index])
        output.append(
            CausalDecisionFeatureRow(
                tick_index=index,
                tick_id=int(tick_ids[index]),
                timestamp=pd.Timestamp(timestamps[index]).to_pydatetime(),
                bid=float(bids[index]),
                ask=float(asks[index]),
                velocity=velocity if math.isfinite(velocity) else None,
                acceleration=acceleration if math.isfinite(acceleration) else None,
            )
        )
    return tuple(output)


def volatility_rows(
    features: pd.DataFrame,
    *,
    column: str,
) -> tuple[VolatilityRow, ...]:
    """Bind one causal volatility column to every exact feature row."""

    required = {"tick_id", "timestamp", column}
    missing = sorted(required.difference(features.columns))
    if missing:
        raise ValueError(f"volatility features are missing columns: {missing}")
    tick_ids = features["tick_id"].to_numpy(copy=False)
    timestamps = features["timestamp"].to_numpy(copy=False)
    values = features[column].to_numpy(dtype=float, copy=False)
    output: list[VolatilityRow] = []
    for index in range(len(features)):
        numeric = float(values[index])
        output.append(
            VolatilityRow(
                tick_index=index,
                tick_id=int(tick_ids[index]),
                timestamp=pd.Timestamp(timestamps[index]).to_pydatetime(),
                value=numeric if math.isfinite(numeric) and numeric > 0.0 else None,
            )
        )
    return tuple(output)


__all__ = [
    "ConnectionContextFactory",
    "FreshDbSessionSource",
    "FrozenSessionCorpusBinding",
    "FreshSessionSource",
    "FreshSessionTape",
    "combine_entry_diagnostics",
    "corpus_bindings_from_manifest",
    "decision_feature_rows",
    "volatility_rows",
]
