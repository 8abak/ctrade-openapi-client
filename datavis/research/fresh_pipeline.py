"""Registered end-to-end runner for the fresh causal XAUUSD study.

This module is intentionally a thin composition layer.  All price features,
signals, fills, diagnostics, exits, scoring, split locks, and holdout locks are
implemented in the separately tested ``fresh_*`` modules.  The runner streams
one frozen broker session at a time and never materialises a cross-session tick
frame.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

from datavis.research.fresh_bootstrap import (
    build_fresh_source_bootstrap,
    registered_fresh_bootstrap_config,
    write_fresh_source_bootstrap,
)
from datavis.research.fresh_candidate_grid import (
    FRESH_CANDIDATE_QUANTILE_RANKS,
    FreshCandidate,
    FreshCandidateGrid,
    build_fresh_candidate_grid,
    fresh_candidate_quantile_measurements,
)
from datavis.research.fresh_decisions import (
    BoundDecisionFeatureRows,
    FrozenSignalDecisionSource,
)
from datavis.research.fresh_entry_diagnostics import (
    DiagnosticBoundary,
    EntrySchedulingConfig,
    FreshEntryDiagnosticsResult,
    FrozenSignalEvent,
    evaluate_frozen_entries,
)
from datavis.research.fresh_event_filters import (
    FRESH_REGIME_QUINTILE_RANKS,
    EventFilterVariantSource,
    FreshEventFilterConfig,
    FreshEventFilterRequest,
    FreshEventFilterVariantBank,
    FreshRegimeDefinition,
    derive_bounded_post_discovery_variant_bank,
    enrich_and_filter_frozen_event_batch,
    fresh_event_filter_config_fingerprint,
    fresh_regime_quantile_measurements,
)
from datavis.research.fresh_exit_grid import (
    FRESH_EXIT_QUANTILE_RANKS,
    FRESH_EXIT_VOLATILITY_COLUMN,
    FreshExitVariant,
    build_fresh_exit_grid,
    fresh_exit_quantile_measurements,
)
from datavis.research.fresh_exits import (
    BoundVolatilityRows,
    FreshProtectiveExitPolicy,
)
from datavis.research.fresh_feature_bank import (
    FreshFeatureBankConfig,
    FreshFeatureBankOutputSelection,
    FreshKalmanBankMember,
    NamedFeatureFamily,
    compute_fresh_feature_bank,
)
from datavis.research.fresh_preregistration import (
    authorize_registered_holdout,
    build_fresh_implementation_manifest,
    build_fresh_preregistration_v2,
    entry_barrier_diagnostic_configs_from_preregistration,
    feature_configs_from_preregistration,
    replay_execution_configs_from_preregistration,
    required_fresh_implementation_files,
)
from datavis.research.fresh_recovery import (
    RUN14_ENTRY_BANK_FILE_SHA256,
    RUN14_LEDGER_SHA256,
    RUN14_RUN_ID,
    build_run14_recovery_contract,
    load_run14_recovery_bundle,
    run_run14_recovery_equivalence_preflight,
)
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_replay import (
    ReplayBoundary,
    _prepare_replay_tape,
    run_fresh_replay,
)
from datavis.research.fresh_scoring import (
    EntryScoreReport,
    GateResult,
    SliceDimensions,
    TradeScoreReport,
    build_candidate_scorecard,
    evaluate_entry_gate,
    score_entry_diagnostics,
    score_trade_records,
    scoring_config_from_preregistration,
)
from datavis.research.fresh_search import (
    CandidateEvaluation,
    EntryCandidateSpec,
    EvaluationContext,
    FreshChronologicalSearch,
    FreshSearchBudgets,
    FreshSearchCallbacks,
    FrozenEntryCandidate,
    FrozenResearchWindow,
    FrozenStrategyCandidate,
    StageRunResult,
    StrategyCandidateSpec,
)
from datavis.research.fresh_session_eval import (
    FreshDbSessionSource,
    FreshSessionTape,
    combine_entry_diagnostics,
    decision_feature_rows,
    volatility_rows,
)
from datavis.research.fresh_signals import (
    FreshSignalConfig,
    generate_frozen_signal_events,
    signal_required_columns,
)
from datavis.research.fresh_spool import KeyedObjectSpool
from datavis.research.fresh_thresholds import (
    FreshQuantileBank,
    FreshQuantileBankConfig,
    QuantileMeasurementSpec,
    SessionBalancedQuantileFitter,
    fresh_quantile_bank_from_payload,
    fresh_quantile_bank_payload,
)


FRESH_PIPELINE_SCHEMA = "fresh-xauusd-chronological-run/v1"
REFERENCE_SCENARIO_ID = "reference-provisional"
BASELINE_EVENTS_PER_SIDE_PER_SESSION = 200
BASELINE_MINIMUM_UPLIFT = 0.02
BASELINE_CLUSTER_CONFIDENCE = 0.90
BASELINE_BOOTSTRAP_REPLICATES = 2_000
SESSION_CLOSE_SAFETY_MS = 62_000
LATER_SENSITIVITY_STAGES = frozenset(("walk_forward_3", "validation", "holdout"))

ProgressCallback = Callable[[Mapping[str, Any]], None]


@dataclass(frozen=True, slots=True)
class _EntryRuntime:
    candidate_id: str
    family: str
    source: FreshCandidate
    event_filter: FreshEventFilterConfig
    entry_variant: str
    robustness_group: str


@dataclass(frozen=True, slots=True)
class _EntryEdgeSummary:
    expected_barrier_pnl_per_fill: float | None
    median_mae_before_coverage: float | None
    median_mfe_horizon: float | None
    p90_restricted_coverage_ms: float | None
    failure_to_cover_60s: float | None
    coverage_10_cluster_interval: tuple[float, float] | None
    coverage_30_cluster_interval: tuple[float, float] | None
    baseline_coverage_10: float | None
    baseline_coverage_30: float | None
    uplift_10: float | None
    uplift_30: float | None
    uplift_10_cluster_interval: tuple[float, float] | None
    uplift_30_cluster_interval: tuple[float, float] | None
    baseline_gate_passed: bool


def _parameter_neighbourhood_audit(
    members: Sequence[tuple[str, float, bool, float | None, float | None, str]],
    *,
    minimum_valid_neighbor_fraction: float,
    minimum_positive_expectancy_neighbor_fraction: float,
    minimum_neighbor_expectancy_retention: float,
    maximum_absolute_coverage_30_drop: float | None,
) -> dict[str, Any]:
    """Evaluate one center plus its two registered adjacent rank variants."""

    selected = tuple(members)
    offsets = tuple(sorted(item[1] for item in selected))
    if offsets != (-0.05, 0.0, 0.05):
        raise ValueError(
            "a parameter neighbourhood must contain rank offsets -0.05, 0, 0.05"
        )
    center = next(item for item in selected if item[1] == 0.0)
    neighbors = tuple(item for item in selected if item[1] != 0.0)
    valid_fraction = sum(item[2] for item in neighbors) / len(neighbors)
    positive_fraction = sum(
        item[3] is not None and item[3] > 0.0 for item in neighbors
    ) / len(neighbors)
    neighbor_expectancies = [
        float(item[3]) for item in neighbors if item[3] is not None
    ]
    center_expectancy = center[3]
    retention = (
        float(min(neighbor_expectancies) / center_expectancy)
        if len(neighbor_expectancies) == len(neighbors)
        and center_expectancy is not None
        and center_expectancy > 0.0
        else None
    )
    neighbor_coverages = [float(item[4]) for item in neighbors if item[4] is not None]
    center_coverage = center[4]
    parameter_signatures = tuple(item[5] for item in selected)
    parameters_distinct = len(set(parameter_signatures)) == len(parameter_signatures)
    coverage_required = maximum_absolute_coverage_30_drop is not None
    coverage_drop = (
        max(abs(value - center_coverage) for value in neighbor_coverages)
        if coverage_required
        and len(neighbor_coverages) == len(neighbors)
        and center_coverage is not None
        else None
    )
    checks = {
        "centerHardGatePassed": center[2],
        "validNeighborFractionPassed": (
            valid_fraction >= minimum_valid_neighbor_fraction
        ),
        "positiveExpectancyNeighborFractionPassed": (
            positive_fraction >= minimum_positive_expectancy_neighbor_fraction
        ),
        "minimumExpectancyRetentionPassed": (
            retention is not None and retention >= minimum_neighbor_expectancy_retention
        ),
        "parametersDistinctAcrossRanks": parameters_distinct,
        "coverage30DropPassed": (
            not coverage_required
            or (
                coverage_drop is not None
                and maximum_absolute_coverage_30_drop is not None
                and coverage_drop <= maximum_absolute_coverage_30_drop
            )
        ),
    }
    return {
        "centerCandidateId": center[0],
        "evaluatedCount": len(selected),
        "adjacentNeighborCount": len(neighbors),
        "validNeighborFraction": valid_fraction,
        "minimumValidNeighborFraction": minimum_valid_neighbor_fraction,
        "positiveExpectancyNeighborFraction": positive_fraction,
        "minimumPositiveExpectancyNeighborFraction": (
            minimum_positive_expectancy_neighbor_fraction
        ),
        "centerExpectancy": center_expectancy,
        "minimumNeighborExpectancy": (
            float(min(neighbor_expectancies))
            if len(neighbor_expectancies) == len(neighbors)
            else None
        ),
        "minimumNeighborExpectancyRetention": retention,
        "requiredMinimumNeighborExpectancyRetention": (
            minimum_neighbor_expectancy_retention
        ),
        "centerCoverage30": center_coverage,
        "coverage30DropRequired": coverage_required,
        "maximumAbsoluteCoverage30Drop": coverage_drop,
        "maximumAllowedAbsoluteCoverage30Drop": (maximum_absolute_coverage_30_drop),
        "checks": checks,
        "passed": all(checks.values()),
    }


def _write_new_json(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to overwrite immutable artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (
        json.dumps(
            dict(payload),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ordered_specs(
    groups: Iterable[Iterable[QuantileMeasurementSpec]],
) -> tuple[QuantileMeasurementSpec, ...]:
    by_name: dict[str, QuantileMeasurementSpec] = {}
    for group in groups:
        for spec in group:
            existing = by_name.get(spec.name)
            if existing is not None and existing != spec:
                raise ValueError(f"quantile measurement {spec.name!r} is ambiguous")
            by_name[spec.name] = spec
    return tuple(by_name[name] for name in sorted(by_name))


def _context_anchors(context: EvaluationContext) -> tuple[str, ...]:
    selected: list[str] = []
    by_role = {window.role: window for window in context.windows}
    for role in context.evaluation_roles:
        window = by_role.get(role)
        if window is None:
            raise ValueError(f"evaluation context is missing role {role!r}")
        selected.extend(window.session_anchors)
    if not selected or tuple(selected) != tuple(sorted(selected)):
        raise ValueError("evaluation sessions must be non-empty and chronological")
    return tuple(selected)


def _events_by_candidate(
    events: Iterable[FrozenSignalEvent],
) -> dict[str, tuple[FrozenSignalEvent, ...]]:
    grouped: dict[str, list[FrozenSignalEvent]] = defaultdict(list)
    for event in events:
        identifier = event.metadata.get("candidate_id")
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("every signal event must identify its source candidate")
        grouped[identifier].append(event)
    return {key: tuple(value) for key, value in grouped.items()}


def _before_close(
    events: Iterable[FrozenSignalEvent], tape: FreshSessionTape
) -> tuple[FrozenSignalEvent, ...]:
    cutoff = tape.bounds.end_utc - timedelta(milliseconds=SESSION_CLOSE_SAFETY_MS)
    return tuple(event for event in events if event.timestamp < cutoff)


def _baseline_events(
    frame: pd.DataFrame,
    tape: FreshSessionTape,
    *,
    maximum_per_side: int = BASELINE_EVENTS_PER_SIDE_PER_SESSION,
) -> tuple[FrozenSignalEvent, ...]:
    if maximum_per_side <= 0:
        raise ValueError("maximum_per_side must be positive")
    ready = frame["feature_ready"].to_numpy(dtype=bool, copy=False)
    gaps = frame["gap_detected"].to_numpy(dtype=bool, copy=False)
    timestamps = pd.to_datetime(frame["timestamp"], utc=True)
    cutoff = tape.bounds.end_utc - timedelta(milliseconds=SESSION_CLOSE_SAFETY_MS)
    usable = np.flatnonzero(
        ready
        & ~gaps
        & (timestamps < pd.Timestamp(cutoff)).to_numpy(dtype=bool, copy=False)
    )
    if usable.size > maximum_per_side:
        positions = np.linspace(0, usable.size - 1, maximum_per_side, dtype=int)
        usable = usable[np.unique(positions)]
    tick_ids = frame["tick_id"].to_numpy(copy=False)
    raw_timestamps = frame["timestamp"].to_numpy(copy=False)
    output: list[FrozenSignalEvent] = []
    for index in usable.tolist():
        timestamp = pd.Timestamp(raw_timestamps[index]).to_pydatetime()
        for side in ("long", "short"):
            output.append(
                FrozenSignalEvent(
                    tick_index=int(index),
                    tick_id=int(tick_ids[index]),
                    timestamp=timestamp,
                    side=side,
                    metadata={
                        "candidate_id": "eligible-tick-direction-baseline",
                        "family": "eligible-tick-baseline",
                        "sampling": "uniform-index-stratified-by-session-and-side",
                    },
                )
            )
    return tuple(output)


def _diagnose(
    tape: FreshSessionTape,
    events: Sequence[FrozenSignalEvent],
    *,
    config: Any,
) -> FreshEntryDiagnosticsResult:
    if not isinstance(tape, FreshSessionTape):
        raise TypeError("trusted diagnostics require a FreshSessionTape")
    return evaluate_frozen_entries(
        tape.ticks,
        events,
        config=config,
        boundary=DiagnosticBoundary(
            start=tape.bounds.start_utc,
            end=tape.bounds.end_utc,
            name=tape.anchor,
            end_reason="session_end",
            input_complete_through_end=True,
        ),
        scheduling=EntrySchedulingConfig(mode="independent", cooldown_ms=0),
        _trusted_validated_ticks=True,
    )


def _replay_session(
    tape: FreshSessionTape,
    decisions: Any,
    *,
    config: Any,
    prepared_replay_tape: Any,
) -> Any:
    if not isinstance(tape, FreshSessionTape):
        raise TypeError("trusted replay requires a FreshSessionTape")
    return run_fresh_replay(
        tape.ticks,
        decisions,
        config=config,
        boundary=ReplayBoundary(
            start=tape.bounds.start_utc,
            end=tape.bounds.end_utc,
            name=tape.anchor,
            input_complete_through_end=True,
        ),
        _trusted_validated_ticks=True,
        _prepared_replay_tape=prepared_replay_tape,
    )


def _coverage_count(
    result: FreshEntryDiagnosticsResult, checkpoint: int, side: str | None = None
) -> tuple[int, int]:
    attribute = f"cost_covered_by_{checkpoint}s"
    selected = [
        item for item in result.diagnostics if side is None or item.event.side == side
    ]
    return (
        sum(not item.censored and bool(getattr(item, attribute)) for item in selected),
        len(selected),
    )


def _entry_barrier_value(item: Any) -> float:
    """Return the registered equal-barrier outcome with censors as failures."""

    if item.censored:
        return 0.0
    if item.first_barrier_hit == "profit":
        return 0.25
    if item.first_barrier_hit == "loss":
        return -0.25
    return 0.0


def _restricted_coverage_ms(item: Any) -> float:
    """Assign the full diagnostic horizon to censors and uncovered fills."""

    if item.censored or item.time_to_cost_coverage_ms is None:
        return 60_000.0
    return float(item.time_to_cost_coverage_ms)


def _scenario_ids_for_stage(
    execution_ids: Sequence[str],
    required_stress_ids: Sequence[str],
    *,
    stage: str,
) -> tuple[str, ...]:
    """Use selection scenarios in research and report all sensitivities later."""

    registered = tuple(execution_ids)
    if len(registered) != len(set(registered)):
        raise ValueError("execution scenario ids must be unique")
    core = (REFERENCE_SCENARIO_ID, *tuple(required_stress_ids))
    if len(core) != len(set(core)) or any(item not in registered for item in core):
        raise ValueError("reference and required stress scenarios must be registered")
    if stage in LATER_SENSITIVITY_STAGES:
        return registered
    return core


def _derive_event_filter_bank(
    bank: FreshQuantileBank,
    grid: FreshCandidateGrid,
    regime_definition: FreshRegimeDefinition,
) -> FreshEventFilterVariantBank:
    """Build the complete outcome-blind filter expansion used by preflight and search."""

    family_counts = Counter(item.family for item in grid.candidates)
    return derive_bounded_post_discovery_variant_bank(
        bank,
        regime_definition=regime_definition,
        source_candidates=tuple(
            EventFilterVariantSource(
                candidate_id=item.config.candidate_id,
                family=item.family,
                robustness_group=item.neighbourhood_id,
            )
            for item in grid.candidates
        ),
        already_registered_candidate_count=len(grid.candidates),
        registered_family_counts=family_counts,
        requested_additional_candidates=240 - len(grid.candidates),
    )


def _research_state_binding(
    state_directory: str | Path,
    split_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive durable ledger and holdout-lock paths from frozen window identity."""

    state_root = Path(state_directory).expanduser().resolve()
    claimed_split_sha = split_manifest.get("manifestSha256")
    if not isinstance(claimed_split_sha, str):
        raise ValueError("split manifest has no SHA-256 identity")
    split_body = {
        key: value for key, value in split_manifest.items() if key != "manifestSha256"
    }
    if canonical_hash(split_body) != claimed_split_sha:
        raise ValueError("split manifest hash is invalid")
    windows = split_manifest.get("windows")
    if not isinstance(windows, Mapping) or not isinstance(
        windows.get("holdout"), Mapping
    ):
        raise ValueError("split manifest has no holdout window")
    role_order = (
        "discovery",
        "walk_forward_1",
        "walk_forward_2",
        "walk_forward_3",
        "validation",
        "holdout",
    )
    if set(windows) != set(role_order) or any(
        not isinstance(windows[role], Mapping) for role in role_order
    ):
        raise ValueError("split manifest does not contain every research window")
    research_window_set_sha = canonical_hash(
        [canonical_hash(windows[role]) for role in role_order]
    )
    holdout_window_sha = canonical_hash(windows["holdout"])
    study_directory = state_root / "studies" / research_window_set_sha
    ledger_path = study_directory / "fresh_experiment_ledger_v1.jsonl"
    holdout_path = (
        state_root
        / "holdouts"
        / holdout_window_sha
        / "fresh_holdout_authorization_v1.json"
    )
    return {
        "schema": "fresh-xauusd-durable-research-state/v1",
        "studyId": "xauusd-fresh-causal-acceleration-v2",
        "splitManifestSha256": claimed_split_sha,
        "researchWindowSetSha256": research_window_set_sha,
        "holdoutWindowSha256": holdout_window_sha,
        "stateDirectory": str(state_root),
        "experimentLedgerPath": str(ledger_path),
        "holdoutAuthorizationRegistryPath": str(holdout_path),
    }


def _snapshot_new_file(source: Path, destination: Path) -> None:
    """Copy one durable audit file into the artifact set without overwriting."""

    if not source.is_file():
        raise FileNotFoundError(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".fresh-snapshot-",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with (
            source.open("rb") as reader,
            os.fdopen(descriptor, "wb", closefd=True) as writer,
        ):
            descriptor = -1
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        os.link(temporary, destination)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _cluster_entry_edge(
    candidate_results: Sequence[FreshEntryDiagnosticsResult],
    baseline_results: Sequence[FreshEntryDiagnosticsResult],
    *,
    seed_text: str,
) -> dict[str, Any]:
    if len(candidate_results) != len(baseline_results) or not candidate_results:
        raise ValueError("candidate and baseline sessions must align")
    rows: list[dict[str, float]] = []
    for candidate, baseline in zip(candidate_results, baseline_results):
        row: dict[str, float] = {}
        for checkpoint in (10, 30):
            successes, count = _coverage_count(candidate, checkpoint)
            expected = 0.0
            expected_count = 0
            for side in ("long", "short"):
                base_success, base_count = _coverage_count(baseline, checkpoint, side)
                _, candidate_side_count = _coverage_count(candidate, checkpoint, side)
                if candidate_side_count and base_count:
                    expected += candidate_side_count * base_success / base_count
                    expected_count += candidate_side_count
            row[f"success{checkpoint}"] = float(successes)
            row[f"count{checkpoint}"] = float(count)
            row[f"baselineExpected{checkpoint}"] = expected
            row[f"baselineCount{checkpoint}"] = float(expected_count)
        rows.append(row)

    digest = hashlib.sha256(seed_text.encode("utf-8")).digest()
    rng = np.random.default_rng(int.from_bytes(digest[:8], "big"))
    samples: dict[int, list[tuple[float, float]]] = {10: [], 30: []}
    size = len(rows)
    for _ in range(BASELINE_BOOTSTRAP_REPLICATES):
        selected = rng.integers(0, size, size=size)
        for checkpoint in (10, 30):
            count = sum(rows[index][f"count{checkpoint}"] for index in selected)
            baseline_count = sum(
                rows[index][f"baselineCount{checkpoint}"] for index in selected
            )
            if count <= 0.0 or baseline_count <= 0.0:
                continue
            coverage = (
                sum(rows[index][f"success{checkpoint}"] for index in selected) / count
            )
            baseline = (
                sum(rows[index][f"baselineExpected{checkpoint}"] for index in selected)
                / baseline_count
            )
            samples[checkpoint].append((coverage, coverage - baseline))

    alpha = (1.0 - BASELINE_CLUSTER_CONFIDENCE) / 2.0
    output: dict[str, Any] = {}
    for checkpoint in (10, 30):
        count = sum(row[f"count{checkpoint}"] for row in rows)
        success = sum(row[f"success{checkpoint}"] for row in rows)
        baseline_count = sum(row[f"baselineCount{checkpoint}"] for row in rows)
        expected = sum(row[f"baselineExpected{checkpoint}"] for row in rows)
        coverage = success / count if count else None
        baseline = expected / baseline_count if baseline_count else None
        uplift = (
            coverage - baseline
            if coverage is not None and baseline is not None
            else None
        )
        draws = samples[checkpoint]
        coverage_interval = (
            tuple(
                float(value)
                for value in np.quantile(
                    np.asarray([item[0] for item in draws]),
                    (alpha, 1.0 - alpha),
                    method="linear",
                )
            )
            if draws
            else None
        )
        uplift_interval = (
            tuple(
                float(value)
                for value in np.quantile(
                    np.asarray([item[1] for item in draws]),
                    (alpha, 1.0 - alpha),
                    method="linear",
                )
            )
            if draws
            else None
        )
        output[str(checkpoint)] = {
            "coverage": coverage,
            "coverageInterval": coverage_interval,
            "baseline": baseline,
            "uplift": uplift,
            "upliftInterval": uplift_interval,
        }
    return output


def _entry_edge_summary(
    candidate_results: Sequence[FreshEntryDiagnosticsResult],
    baseline_results: Sequence[FreshEntryDiagnosticsResult],
    *,
    seed_text: str,
) -> _EntryEdgeSummary:
    combined = combine_entry_diagnostics(tuple(candidate_results))
    fills = combined.diagnostics
    barrier_values = [_entry_barrier_value(item) for item in fills]
    restricted = [_restricted_coverage_ms(item) for item in fills]
    clustered = _cluster_entry_edge(
        candidate_results, baseline_results, seed_text=seed_text
    )
    ten = clustered["10"]
    thirty = clustered["30"]
    uplift_10_interval = ten["upliftInterval"]
    uplift_30_interval = thirty["upliftInterval"]
    baseline_gate = bool(
        ten["uplift"] is not None
        and thirty["uplift"] is not None
        and ten["uplift"] >= BASELINE_MINIMUM_UPLIFT
        and thirty["uplift"] >= BASELINE_MINIMUM_UPLIFT
        and uplift_10_interval is not None
        and uplift_30_interval is not None
        and uplift_10_interval[0] > 0.0
        and uplift_30_interval[0] > 0.0
    )
    covered_60, count_60 = _coverage_count(combined, 60)
    return _EntryEdgeSummary(
        expected_barrier_pnl_per_fill=(
            float(math.fsum(barrier_values) / len(barrier_values))
            if barrier_values
            else None
        ),
        median_mae_before_coverage=(
            float(median(item.mae_before_coverage_per_unit for item in fills))
            if fills
            else None
        ),
        median_mfe_horizon=(
            float(median(item.mfe_horizon_per_unit for item in fills))
            if fills
            else None
        ),
        p90_restricted_coverage_ms=(
            float(np.quantile(np.asarray(restricted), 0.90, method="linear"))
            if restricted
            else None
        ),
        failure_to_cover_60s=(1.0 - covered_60 / count_60 if count_60 else None),
        coverage_10_cluster_interval=ten["coverageInterval"],
        coverage_30_cluster_interval=thirty["coverageInterval"],
        baseline_coverage_10=ten["baseline"],
        baseline_coverage_30=thirty["baseline"],
        uplift_10=ten["uplift"],
        uplift_30=thirty["uplift"],
        uplift_10_cluster_interval=uplift_10_interval,
        uplift_30_cluster_interval=uplift_30_interval,
        baseline_gate_passed=baseline_gate,
    )


def _entry_rank_score(report: EntryScoreReport, edge: _EntryEdgeSummary) -> float:
    metrics = report.overall
    coverage_10 = metrics.coverage_probability(10) or 0.0
    coverage_30 = metrics.coverage_probability(30) or 0.0
    coverage_60 = metrics.coverage_probability(60) or 0.0
    barrier = metrics.barrier_profit_first_rate or 0.0
    speed = (
        1.0 - min(metrics.restricted_median_coverage_milliseconds / 60_000.0, 1.0)
        if metrics.restricted_median_coverage_milliseconds is not None
        else 0.0
    )
    fill = metrics.fill_rate or 0.0
    active = metrics.active_session_fraction or 0.0
    mae = abs(edge.median_mae_before_coverage or 0.0)
    mae_quality = 1.0 / (1.0 + mae)
    return float(
        0.25 * coverage_10
        + 0.25 * coverage_30
        + 0.10 * coverage_60
        + 0.15 * barrier
        + 0.10 * speed
        + 0.05 * fill
        + 0.05 * active
        + 0.05 * mae_quality
    )


def _compact_entry_slices(report: EntryScoreReport) -> dict[str, Any]:
    def compact(items: Sequence[tuple[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "label": label,
                "filledCount": metrics.filled_count,
                "coverage10": metrics.coverage_probability(10),
                "coverage30": metrics.coverage_probability(30),
                "coverage60": metrics.coverage_probability(60),
                "barrierProfitFirstRate": metrics.barrier_profit_first_rate,
            }
            for label, metrics in items
        ]

    return {
        "overall": asdict(report.overall),
        "byDay": compact(report.by_day),
        "bySide": compact(report.by_side),
        "byMarketSession": compact(report.by_market_session),
        "byRegime": compact(report.by_regime),
    }


def _compact_trade_slices(report: TradeScoreReport) -> dict[str, Any]:
    def compact(items: Sequence[tuple[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "label": label,
                "tradeCount": metrics.trade_count,
                "winRate": metrics.win_rate,
                "netPnl": metrics.net_pnl,
                "expectancy": metrics.expectancy,
                "profitFactor": metrics.profit_factor,
                "maximumDrawdown": metrics.maximum_drawdown,
            }
            for label, metrics in items
        ]

    return {
        "overall": asdict(report.overall),
        "byDay": compact(report.by_day),
        "bySide": compact(report.by_side),
        "byMarketSession": compact(report.by_market_session),
        "byRegime": compact(report.by_regime),
    }


def _bound_discovery_session_count(preregistration: Mapping[str, Any]) -> int:
    policy = preregistration.get("chronologicalWindowPolicy")
    if not isinstance(policy, Mapping):
        raise ValueError("preregistration has no chronological window policy")
    count = policy.get("discovery_sessions")
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        raise ValueError("preregistration has an invalid discovery-session count")
    return count


class RegisteredFreshResearchPipeline:
    """Compose the frozen study and expose callbacks to the protocol engine."""

    def __init__(
        self,
        *,
        repository_root: str | Path,
        output_directory: str | Path,
        connection_context_factory: Any,
        corpus_manifest: Mapping[str, Any],
        split_manifest: Mapping[str, Any],
        preregistration: Mapping[str, Any],
        progress: ProgressCallback | None = None,
        verify_preregistration_implementation_files: bool = True,
    ) -> None:
        self.repository_root = Path(repository_root).resolve()
        self.output = Path(output_directory).resolve()
        self.corpus_manifest = dict(corpus_manifest)
        self.split_manifest = dict(split_manifest)
        self.preregistration = dict(preregistration)
        self.progress = progress
        self.verify_preregistration_implementation_files = (
            verify_preregistration_implementation_files
        )
        self.regime_definition = FreshRegimeDefinition(
            volatility_column="1s_bollinger_std",
            spread_column="spread",
            trend_column="10s_mid_speed",
            arrival_column="1s_arrival_rate",
        )
        bootstrap = registered_fresh_bootstrap_config()
        self.source = FreshDbSessionSource(
            connection_context_factory=connection_context_factory,
            data_config=bootstrap.data_config,
            corpus_manifest=self.corpus_manifest,
        )
        feature_configs = feature_configs_from_preregistration(
            self.preregistration,
            verify_current_implementation_files=(
                self.verify_preregistration_implementation_files
            ),
        )
        models = self.preregistration["features"]["kalmanModelBank"]
        if len(feature_configs) != len(models):
            raise ValueError("registered Kalman model bank is inconsistent")
        self.members = tuple(
            FreshKalmanBankMember(model_id=str(model["id"]), feature_config=config)
            for model, config in zip(models, feature_configs)
        )
        self.model_ids = tuple(member.model_id for member in self.members)
        self.scoring = scoring_config_from_preregistration(
            self.preregistration,
            verify_current_implementation_files=(
                self.verify_preregistration_implementation_files
            ),
        )
        barrier_configs = entry_barrier_diagnostic_configs_from_preregistration(
            self.preregistration,
            scenario_id=REFERENCE_SCENARIO_ID,
            verify_current_implementation_files=(
                self.verify_preregistration_implementation_files
            ),
        )
        self.entry_diagnostic_config = barrier_configs[
            f"{REFERENCE_SCENARIO_ID}:profit-0.25:loss-0.25"
        ]
        self.executions = replay_execution_configs_from_preregistration(
            self.preregistration,
            verify_current_implementation_files=(
                self.verify_preregistration_implementation_files
            ),
        )
        scenario_policy = self.preregistration["execution"]["scenarioEvaluationPolicy"]
        self.exit_search_scenario_ids = tuple(
            str(item) for item in scenario_policy["exitSearchScenarioIds"]
        )
        self.later_full_scenario_ids = tuple(
            str(item) for item in scenario_policy["laterFullStrategyScenarioIds"]
        )
        registered_scenarios = tuple(self.executions)
        expected_exit_search = _scenario_ids_for_stage(
            registered_scenarios,
            self.scoring.required_stress_scenario_ids,
            stage="exit_search",
        )
        expected_later = _scenario_ids_for_stage(
            registered_scenarios,
            self.scoring.required_stress_scenario_ids,
            stage="validation",
        )
        hard_gate_ids = tuple(
            str(item) for item in scenario_policy["hardGateScenarioIds"]
        )
        sensitivity_ids = tuple(
            str(item) for item in scenario_policy["sensitivityOnlyScenarioIds"]
        )
        diagnostic_ids = tuple(
            str(item) for item in scenario_policy["diagnosticOnlyScenarioIds"]
        )
        extras = tuple(
            item for item in expected_later if item not in expected_exit_search
        )
        if (
            self.exit_search_scenario_ids != expected_exit_search
            or self.later_full_scenario_ids != expected_later
            or hard_gate_ids != expected_exit_search
            or len((*sensitivity_ids, *diagnostic_ids))
            != len(set((*sensitivity_ids, *diagnostic_ids)))
            or set((*sensitivity_ids, *diagnostic_ids)) != set(extras)
        ):
            raise ValueError(
                "registered execution scenario evaluation policy is inconsistent"
            )
        self.dimensions = SliceDimensions(
            day_metadata_path="context.day",
            market_session_metadata_path="context.marketSession",
            regime_metadata_path="context.regime",
        )
        self.quantile_bank: FreshQuantileBank | None = None
        self.threshold_preflight: dict[str, Any] | None = None
        self.entry_runtime: dict[str, _EntryRuntime] = {}
        self.exit_runtime: dict[str, FreshExitVariant] = {}
        self.stage_results: list[StageRunResult] = []
        self.recovery_contract: Mapping[str, Any] | None = None
        self.recovery_implementation_manifest: Mapping[str, Any] | None = None
        self.recovery_batch_result_path: Path | None = None

    def _emit(self, **payload: Any) -> None:
        if self.progress is not None:
            self.progress(payload)

    def _features(self, tape: FreshSessionTape, columns: Iterable[str]) -> pd.DataFrame:
        selected = tuple(dict.fromkeys(str(column) for column in columns))
        family = NamedFeatureFamily("pipeline", selected)
        config = FreshFeatureBankConfig(
            members=self.members,
            output_selection=FreshFeatureBankOutputSelection(
                include_all_columns=False,
                candidate_families=(family,),
                selected_candidate_families=("pipeline",),
            ),
        )
        return compute_fresh_feature_bank(tape.ticks, config=config)

    def fit_thresholds(self, context: EvaluationContext) -> Mapping[str, Any]:
        anchors = _context_anchors(context)
        expected_count = _bound_discovery_session_count(self.preregistration)
        if context.stage != "discovery" or len(anchors) != expected_count:
            raise PermissionError("thresholds may be fitted only on discovery")
        measurements = _ordered_specs(
            (
                fresh_candidate_quantile_measurements(kalman_model_ids=self.model_ids),
                fresh_exit_quantile_measurements(),
                fresh_regime_quantile_measurements(self.regime_definition),
            )
        )
        ranks = tuple(
            sorted(
                set(FRESH_CANDIDATE_QUANTILE_RANKS)
                | set(FRESH_EXIT_QUANTILE_RANKS)
                | set(FRESH_REGIME_QUINTILE_RANKS)
            )
        )
        fitter = SessionBalancedQuantileFitter(
            measurements=measurements,
            config=FreshQuantileBankConfig(
                ranks=ranks,
                minimum_finite_values_per_session=1_000,
                minimum_eligible_sessions=40,
            ),
        )
        columns = tuple(dict.fromkeys(spec.column for spec in measurements))
        for ordinal, anchor in enumerate(anchors, start=1):
            tape = self.source.load_session(anchor)
            frame = self._features(tape, columns)
            fitter.add_session(anchor, frame)
            self._emit(
                stage="threshold_fit",
                sessionOrdinal=ordinal,
                sessionCount=len(anchors),
                sessionAnchor=anchor,
            )
            del frame, tape
        bank = fitter.freeze()
        self.quantile_bank = bank
        payload = fresh_quantile_bank_payload(bank)
        _write_new_json(self.output / "fresh_quantile_bank_v1.json", payload)
        entry_grid = build_fresh_candidate_grid(bank, kalman_model_ids=self.model_ids)
        filter_bank = _derive_event_filter_bank(
            bank, entry_grid, self.regime_definition
        )
        exit_grid = build_fresh_exit_grid(bank, execution_configs=self.executions)
        preflight = {
            "schema": "fresh-xauusd-threshold-domain-preflight/v1",
            "quantileBankSha256": bank.bank_sha256,
            "candidateGridSha256": entry_grid.grid_sha256,
            "baseCandidateCount": len(entry_grid.candidates),
            "eventFilterVariantBankSha256": filter_bank.variant_bank_sha256,
            "eventFilterVariantCount": len(filter_bank.variants),
            "totalRuntimeEntryCount": filter_bank.total_candidate_count,
            "exitGridSha256": exit_grid.grid_sha256,
            "exitVariantCount": len(exit_grid.variants),
            "executionScenariosSha256": exit_grid.execution_scenarios_sha256,
            "allRegisteredThresholdDomainsResolved": True,
        }
        if preflight["totalRuntimeEntryCount"] != 240:
            raise ValueError("threshold preflight did not resolve 240 entry candidates")
        self.threshold_preflight = preflight
        _write_new_json(
            self.output / "fresh_threshold_domain_preflight_v1.json",
            preflight,
        )
        return payload

    def build_entry_candidates(
        self, payload: Mapping[str, Any], context: EvaluationContext
    ) -> Iterable[EntryCandidateSpec]:
        if context.stage != "discovery":
            raise PermissionError("entry grid may be built only in discovery")
        bank = fresh_quantile_bank_from_payload(payload)
        self.quantile_bank = bank
        grid = build_fresh_candidate_grid(bank, kalman_model_ids=self.model_ids)
        base_filter = FreshEventFilterConfig(
            variant_id="all-unfiltered",
            regime_definition=self.regime_definition,
            activity_filter="all",
            spread_ceiling_rank=None,
            volatility_floor_rank=None,
        )
        variants = _derive_event_filter_bank(bank, grid, self.regime_definition)
        preflight = self.threshold_preflight
        if preflight is None or any(
            (
                preflight["quantileBankSha256"] != bank.bank_sha256,
                preflight["candidateGridSha256"] != grid.grid_sha256,
                preflight["eventFilterVariantBankSha256"]
                != variants.variant_bank_sha256,
                preflight["totalRuntimeEntryCount"] != variants.total_candidate_count,
            )
        ):
            raise RuntimeError("runtime entry bank differs from threshold preflight")
        by_source = {item.config.candidate_id: item for item in grid.candidates}
        runtimes: list[_EntryRuntime] = []
        for item in grid.candidates:
            runtimes.append(
                _EntryRuntime(
                    candidate_id=item.config.candidate_id,
                    family=item.family,
                    source=item,
                    event_filter=base_filter,
                    entry_variant="base-all-market",
                    robustness_group=f"{item.neighbourhood_id}::all-unfiltered",
                )
            )
        for variant in variants.variants:
            source = by_source[variant.source_candidate_id]
            runtimes.append(
                _EntryRuntime(
                    candidate_id=variant.candidate_id,
                    family=variant.family,
                    source=source,
                    event_filter=variant.filter_config,
                    entry_variant=f"event-filter:{variant.filter_config.variant_id}",
                    robustness_group=(
                        f"{source.neighbourhood_id}::{variant.filter_config.variant_id}"
                    ),
                )
            )
        if len(runtimes) != 240:
            raise AssertionError("the frozen entry bank must contain 240 candidates")
        self.entry_runtime = {item.candidate_id: item for item in runtimes}
        artifact = {
            "schema": "fresh-xauusd-runtime-entry-bank/v1",
            "quantileBankSha256": bank.bank_sha256,
            "candidateGridSha256": grid.grid_sha256,
            "filterVariantBankSha256": variants.variant_bank_sha256,
            "candidateCount": len(runtimes),
            "candidates": [
                {
                    "candidateId": item.candidate_id,
                    "family": item.family,
                    "sourceCandidateId": item.source.config.candidate_id,
                    "sourceConfig": asdict(item.source.config),
                    "sourceConfigSha256": item.source.config_sha256,
                    "eventFilter": asdict(item.event_filter),
                    "eventFilterSha256": fresh_event_filter_config_fingerprint(
                        item.event_filter, bank
                    ),
                    "entryVariant": item.entry_variant,
                    "robustnessGroup": item.robustness_group,
                }
                for item in runtimes
            ],
        }
        _write_new_json(self.output / "fresh_entry_bank_v1.json", artifact)
        for item in runtimes:
            yield EntryCandidateSpec(
                candidate_id=item.candidate_id,
                family=item.family,
                config={
                    "schema": "fresh-xauusd-entry-runtime/v1",
                    "sourceCandidateId": item.source.config.candidate_id,
                    "sourceSignalConfig": asdict(item.source.config),
                    "sourceSignalConfigSha256": item.source.config_sha256,
                    "eventFilter": asdict(item.event_filter),
                    "eventFilterSha256": fresh_event_filter_config_fingerprint(
                        item.event_filter, bank
                    ),
                    "robustnessGroup": item.robustness_group,
                    "quantileBankSha256": bank.bank_sha256,
                    "sessionCloseSafetyMilliseconds": SESSION_CLOSE_SAFETY_MS,
                    "baseline": {
                        "eventsPerSidePerSession": (
                            BASELINE_EVENTS_PER_SIDE_PER_SESSION
                        ),
                        "minimumCoverageUplift": BASELINE_MINIMUM_UPLIFT,
                        "clusterConfidence": BASELINE_CLUSTER_CONFIDENCE,
                        "bootstrapReplicates": BASELINE_BOOTSTRAP_REPLICATES,
                    },
                },
                entry_variant=item.entry_variant,
            )

    def _entry_session_batch_materialized(
        self,
        runtimes: Sequence[_EntryRuntime],
        anchors: Sequence[str],
        *,
        stage: str,
    ) -> tuple[
        dict[str, list[FreshEntryDiagnosticsResult]],
        dict[str, list[FreshEntryDiagnosticsResult]],
    ]:
        """Materialized reference implementation retained for equivalence tests."""

        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        source_configs: dict[str, FreshSignalConfig] = {}
        for runtime in runtimes:
            source_configs[runtime.source.config.candidate_id] = runtime.source.config
        columns = list(
            dict.fromkeys(
                column
                for config in source_configs.values()
                for column in signal_required_columns(config)
            )
        )
        columns.extend(
            spec.column
            for spec in fresh_regime_quantile_measurements(self.regime_definition)
        )
        candidate_results: dict[str, list[FreshEntryDiagnosticsResult]] = {
            item.candidate_id: [] for item in runtimes
        }
        filters: dict[str, FreshEventFilterConfig] = {}
        for item in runtimes:
            fingerprint = fresh_event_filter_config_fingerprint(
                item.event_filter, self.quantile_bank
            )
            filters[fingerprint] = item.event_filter
        baseline_results: dict[str, list[FreshEntryDiagnosticsResult]] = {
            fingerprint: [] for fingerprint in filters
        }
        for ordinal, anchor in enumerate(anchors, start=1):
            tape = self.source.load_session(anchor)
            frame = self._features(tape, columns)
            raw_events = generate_frozen_signal_events(
                frame, configs=tuple(source_configs.values()), engine="batch"
            )
            grouped = _events_by_candidate(raw_events)
            raw_baseline = _baseline_events(frame, tape)
            filter_items = tuple(filters.items())
            filter_requests = tuple(
                FreshEventFilterRequest(raw_baseline, event_filter)
                for _, event_filter in filter_items
            )
            runtime_requests = tuple(
                FreshEventFilterRequest(
                    grouped.get(runtime.source.config.candidate_id, ()),
                    runtime.event_filter,
                )
                for runtime in runtimes
            )
            filtered_batch = enrich_and_filter_frozen_event_batch(
                frame,
                (*filter_requests, *runtime_requests),
                quantile_bank=self.quantile_bank,
            )
            filter_results = filtered_batch[: len(filter_requests)]
            runtime_results = filtered_batch[len(filter_requests) :]
            for (fingerprint, _), filtered in zip(filter_items, filter_results):
                baseline_results[fingerprint].append(
                    _diagnose(
                        tape,
                        _before_close(filtered.events, tape),
                        config=self.entry_diagnostic_config,
                    )
                )
            for runtime, filtered in zip(runtimes, runtime_results):
                candidate_results[runtime.candidate_id].append(
                    _diagnose(
                        tape,
                        _before_close(filtered.events, tape),
                        config=self.entry_diagnostic_config,
                    )
                )
            self._emit(
                stage=stage,
                sessionOrdinal=ordinal,
                sessionCount=len(anchors),
                sessionAnchor=anchor,
            )
            del frame, tape, raw_events, grouped, raw_baseline
        expanded_baselines: dict[str, list[FreshEntryDiagnosticsResult]] = {}
        for runtime in runtimes:
            fingerprint = fresh_event_filter_config_fingerprint(
                runtime.event_filter, self.quantile_bank
            )
            expanded_baselines[runtime.candidate_id] = baseline_results[fingerprint]
        return candidate_results, expanded_baselines

    @staticmethod
    def _candidate_spool_key(candidate_id: str) -> str:
        return f"candidate\x00{candidate_id}"

    @staticmethod
    def _baseline_spool_key(filter_sha256: str) -> str:
        return f"baseline\x00{filter_sha256}"

    def _append_entry_session_to_spool(
        self,
        *,
        spool: KeyedObjectSpool[FreshEntryDiagnosticsResult],
        anchor: str,
        ordinal: int,
        session_count: int,
        stage: str,
        columns: Sequence[str],
        source_configs: Sequence[FreshSignalConfig],
        filter_items: Sequence[tuple[str, FreshEventFilterConfig]],
        runtimes: Sequence[_EntryRuntime],
    ) -> None:
        """Process one complete session without returning a live session object."""

        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        tape = self.source.load_session(anchor)
        frame = self._features(tape, columns)
        raw_events = generate_frozen_signal_events(
            frame, configs=tuple(source_configs), engine="batch"
        )
        grouped = _events_by_candidate(raw_events)
        raw_baseline = _baseline_events(frame, tape)
        filter_requests = tuple(
            FreshEventFilterRequest(raw_baseline, event_filter)
            for _, event_filter in filter_items
        )
        runtime_requests = tuple(
            FreshEventFilterRequest(
                grouped.get(runtime.source.config.candidate_id, ()),
                runtime.event_filter,
            )
            for runtime in runtimes
        )
        filtered_batch = enrich_and_filter_frozen_event_batch(
            frame,
            (*filter_requests, *runtime_requests),
            quantile_bank=self.quantile_bank,
        )
        filter_results = filtered_batch[: len(filter_requests)]
        runtime_results = filtered_batch[len(filter_requests) :]
        for (fingerprint, _), filtered in zip(filter_items, filter_results):
            spool.append(
                self._baseline_spool_key(fingerprint),
                _diagnose(
                    tape,
                    _before_close(filtered.events, tape),
                    config=self.entry_diagnostic_config,
                ),
            )
        for runtime, filtered in zip(runtimes, runtime_results):
            spool.append(
                self._candidate_spool_key(runtime.candidate_id),
                _diagnose(
                    tape,
                    _before_close(filtered.events, tape),
                    config=self.entry_diagnostic_config,
                ),
            )
        self._emit(
            stage=stage,
            sessionOrdinal=ordinal,
            sessionCount=session_count,
            sessionAnchor=anchor,
        )

    @contextmanager
    def _entry_session_spool(
        self,
        runtimes: Sequence[_EntryRuntime],
        anchors: Sequence[str],
        *,
        stage: str,
    ) -> Iterator[
        tuple[
            KeyedObjectSpool[FreshEntryDiagnosticsResult],
            Mapping[str, str],
        ]
    ]:
        """Spill every session result, retaining no cross-session event graph."""

        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        source_configs: dict[str, FreshSignalConfig] = {}
        filters: dict[str, FreshEventFilterConfig] = {}
        baseline_by_candidate: dict[str, str] = {}
        for runtime in runtimes:
            source_configs[runtime.source.config.candidate_id] = runtime.source.config
            fingerprint = fresh_event_filter_config_fingerprint(
                runtime.event_filter, self.quantile_bank
            )
            filters[fingerprint] = runtime.event_filter
            baseline_by_candidate[runtime.candidate_id] = fingerprint
        columns = list(
            dict.fromkeys(
                column
                for config in source_configs.values()
                for column in signal_required_columns(config)
            )
        )
        columns.extend(
            spec.column
            for spec in fresh_regime_quantile_measurements(self.regime_definition)
        )
        filter_items = tuple(filters.items())
        with KeyedObjectSpool[FreshEntryDiagnosticsResult](self.output) as spool:
            for runtime in runtimes:
                spool.register_key(self._candidate_spool_key(runtime.candidate_id))
            for fingerprint, _ in filter_items:
                spool.register_key(self._baseline_spool_key(fingerprint))
            for ordinal, anchor in enumerate(anchors, start=1):
                self._append_entry_session_to_spool(
                    spool=spool,
                    anchor=anchor,
                    ordinal=ordinal,
                    session_count=len(anchors),
                    stage=stage,
                    columns=columns,
                    source_configs=tuple(source_configs.values()),
                    filter_items=filter_items,
                    runtimes=runtimes,
                )
            expected_count = len(anchors)
            if any(count != expected_count for _, count in spool.inventory):
                raise RuntimeError("entry spool session inventory is incomplete")
            yield spool, baseline_by_candidate

    def _entry_provisional_evaluation(
        self,
        *,
        candidate: FrozenEntryCandidate,
        context: EvaluationContext,
        anchors: Sequence[str],
        session_results: Sequence[FreshEntryDiagnosticsResult],
        baseline_results: Sequence[FreshEntryDiagnosticsResult],
    ) -> tuple[EntryScoreReport, GateResult, _EntryEdgeSummary]:
        combined = combine_entry_diagnostics(tuple(session_results))
        report = score_entry_diagnostics(
            combined,
            config=self.scoring.entry_metrics,
            dimensions=self.dimensions,
            evaluated_sessions=anchors,
        )
        gate = evaluate_entry_gate(
            report.overall,
            minimum_sample=self.scoring.minimum_sample,
            thresholds=self.scoring.entry_gate,
        )
        edge = _entry_edge_summary(
            session_results,
            baseline_results,
            seed_text=(
                f"{context.stage}:{candidate.entry_sha256}:"
                f"{canonical_hash(list(anchors))}"
            ),
        )
        return report, gate, edge

    def score_entries_batch(
        self,
        candidates: tuple[FrozenEntryCandidate, ...],
        context: EvaluationContext,
    ) -> Mapping[str, CandidateEvaluation]:
        anchors = _context_anchors(context)
        runtimes = tuple(self.entry_runtime[item.candidate_id] for item in candidates)
        provisional: dict[
            str, tuple[EntryScoreReport, GateResult, _EntryEdgeSummary]
        ] = {}
        with self._entry_session_spool(runtimes, anchors, stage=context.stage) as (
            spool,
            baseline_by_candidate,
        ):
            for candidate in candidates:
                with spool.load(
                    self._candidate_spool_key(candidate.candidate_id)
                ) as loaded:
                    session_results = tuple(loaded)
                if len(session_results) != len(anchors):
                    raise RuntimeError("candidate spool session count changed")
                baseline_fingerprint = baseline_by_candidate[candidate.candidate_id]
                with spool.load(
                    self._baseline_spool_key(baseline_fingerprint)
                ) as loaded:
                    baseline_results = tuple(loaded)
                if len(baseline_results) != len(anchors):
                    raise RuntimeError("baseline spool session count changed")
                provisional[candidate.candidate_id] = (
                    self._entry_provisional_evaluation(
                        candidate=candidate,
                        context=context,
                        anchors=anchors,
                        session_results=session_results,
                        baseline_results=baseline_results,
                    )
                )
                del session_results, baseline_results

        return self._finalize_entry_evaluations(
            candidates=candidates,
            context=context,
            provisional=provisional,
        )

    def score_entries_batch_materialized_reference(
        self,
        candidates: tuple[FrozenEntryCandidate, ...],
        context: EvaluationContext,
    ) -> Mapping[str, CandidateEvaluation]:
        """Test oracle for proving the disk-spooled scorer is outcome-equivalent."""

        anchors = _context_anchors(context)
        runtimes = tuple(self.entry_runtime[item.candidate_id] for item in candidates)
        session_results, baselines = self._entry_session_batch_materialized(
            runtimes, anchors, stage=context.stage
        )
        provisional = {
            candidate.candidate_id: self._entry_provisional_evaluation(
                candidate=candidate,
                context=context,
                anchors=anchors,
                session_results=session_results[candidate.candidate_id],
                baseline_results=baselines[candidate.candidate_id],
            )
            for candidate in candidates
        }
        return self._finalize_entry_evaluations(
            candidates=candidates,
            context=context,
            provisional=provisional,
        )

    def _finalize_entry_evaluations(
        self,
        *,
        candidates: Sequence[FrozenEntryCandidate],
        context: EvaluationContext,
        provisional: Mapping[
            str, tuple[EntryScoreReport, GateResult, _EntryEdgeSummary]
        ],
    ) -> Mapping[str, CandidateEvaluation]:
        grouped_candidates: dict[str, list[FrozenEntryCandidate]] = defaultdict(list)
        for candidate in candidates:
            runtime = self.entry_runtime[candidate.candidate_id]
            grouped_candidates[runtime.robustness_group].append(candidate)
        neighbourhood_spec = self.preregistration["robustnessAndGates"][
            "parameterNeighborhood"
        ]
        neighbourhood_audits: dict[str, dict[str, Any]] = {}
        if context.stage == "discovery":
            for group, members in grouped_candidates.items():
                neighbourhood_audits[group] = _parameter_neighbourhood_audit(
                    tuple(
                        (
                            candidate.candidate_id,
                            self.entry_runtime[
                                candidate.candidate_id
                            ].source.rank_offset,
                            bool(
                                provisional[candidate.candidate_id][1].passed
                                and provisional[candidate.candidate_id][
                                    2
                                ].baseline_gate_passed
                            ),
                            provisional[candidate.candidate_id][
                                2
                            ].expected_barrier_pnl_per_fill,
                            provisional[candidate.candidate_id][
                                0
                            ].overall.coverage_probability(30),
                            canonical_hash(
                                [
                                    (
                                        item.parameter,
                                        item.final_value,
                                    )
                                    for item in self.entry_runtime[
                                        candidate.candidate_id
                                    ].source.threshold_provenance
                                ]
                            ),
                        )
                        for candidate in members
                    ),
                    minimum_valid_neighbor_fraction=float(
                        neighbourhood_spec["minimumValidNeighborFraction"]
                    ),
                    minimum_positive_expectancy_neighbor_fraction=float(
                        neighbourhood_spec["minimumPositiveExpectancyNeighborFraction"]
                    ),
                    minimum_neighbor_expectancy_retention=float(
                        neighbourhood_spec["minimumNeighborExpectancyRetention"]
                    ),
                    maximum_absolute_coverage_30_drop=float(
                        neighbourhood_spec["maximumAbsoluteCoverage30SecondDrop"]
                    ),
                )

        output: dict[str, CandidateEvaluation] = {}
        for candidate in candidates:
            runtime = self.entry_runtime[candidate.candidate_id]
            report, gate, edge = provisional[candidate.candidate_id]
            neighbourhood_required = context.stage == "discovery"
            neighbourhood = neighbourhood_audits.get(
                runtime.robustness_group,
                {
                    "centerCandidateId": candidate.candidate_id,
                    "evaluatedCount": 1,
                    "passed": True,
                },
            )
            neighbourhood_passed = bool(neighbourhood["passed"])
            is_center = runtime.source.rank_offset == 0.0
            passed = bool(
                gate.passed
                and edge.baseline_gate_passed
                and (not neighbourhood_required or (is_center and neighbourhood_passed))
            )
            output[candidate.candidate_id] = CandidateEvaluation(
                identity_sha256=candidate.entry_sha256,
                passed=passed,
                metrics={
                    "entry": _compact_entry_slices(report),
                    "registeredGate": asdict(gate),
                    "entryEdge": asdict(edge),
                    "parameterNeighbourhood": {
                        **neighbourhood,
                        "group": runtime.robustness_group,
                        "requiredDuringStage": neighbourhood_required,
                        "candidateIsCenter": is_center,
                    },
                },
                leakage_checks={
                    "sessionCorpusFingerprintVerifiedBeforeFeatures": True,
                    "featureCalculationPrefixCausal": True,
                    "signalsUseCurrentOrEarlierRowsOnly": True,
                    "strictlyLaterBidAskFill": True,
                    "holdoutRolePresent": "holdout" in context.evaluation_roles,
                },
                score=_entry_rank_score(report, edge),
            )
        return output

    def build_exit_variants(
        self,
        entries: tuple[FrozenEntryCandidate, ...],
        context: EvaluationContext,
    ) -> Iterable[StrategyCandidateSpec]:
        if context.stage != "exit_search" or len(entries) != 1:
            raise PermissionError(
                "exit variants require exactly one promoted frozen entry"
            )
        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        selected_entry = entries[0]
        grid = build_fresh_exit_grid(
            self.quantile_bank, execution_configs=self.executions
        )
        preflight = self.threshold_preflight
        if preflight is None or any(
            (
                preflight["quantileBankSha256"] != self.quantile_bank.bank_sha256,
                preflight["exitGridSha256"] != grid.grid_sha256,
                preflight["executionScenariosSha256"]
                != grid.execution_scenarios_sha256,
            )
        ):
            raise RuntimeError("runtime exit bank differs from threshold preflight")
        self.exit_runtime = {}
        artifact = {
            "schema": "fresh-xauusd-runtime-exit-bank/v1",
            "selectedEntryCandidateId": selected_entry.candidate_id,
            "selectedEntrySha256": selected_entry.entry_sha256,
            "exitGridSha256": grid.grid_sha256,
            "executionScenariosSha256": grid.execution_scenarios_sha256,
            "variantCount": len(grid.variants),
            "variants": [asdict(item) for item in grid.variants],
        }
        _write_new_json(self.output / "fresh_exit_bank_v1.json", artifact)
        for variant in grid.variants:
            strategy_id = f"{selected_entry.candidate_id}::{variant.variant_id}"
            self.exit_runtime[strategy_id] = variant
            yield StrategyCandidateSpec(
                strategy_id=strategy_id,
                entry_candidate_id=selected_entry.candidate_id,
                exit_config={
                    "schema": "fresh-xauusd-exit-runtime/v1",
                    "variant": asdict(variant),
                    "variantSha256": variant.variant_sha256,
                    "exitGridSha256": grid.grid_sha256,
                },
                execution_config={
                    "schema": "fresh-xauusd-execution-scenarios/v1",
                    "scenarioIds": [
                        item.scenario_id for item in grid.execution_scenarios
                    ],
                    "scenarioConfigSha256": {
                        item.scenario_id: item.config_sha256
                        for item in grid.execution_scenarios
                    },
                    "selectionScenario": REFERENCE_SCENARIO_ID,
                    "requiredStressScenarioIds": list(
                        self.scoring.required_stress_scenario_ids
                    ),
                    "scenarioEvaluationPolicy": dict(
                        self.preregistration["execution"]["scenarioEvaluationPolicy"]
                    ),
                },
                exit_variant=variant.variant_id,
            )

    def score_strategies_batch(
        self,
        candidates: tuple[FrozenStrategyCandidate, ...],
        context: EvaluationContext,
    ) -> Mapping[str, CandidateEvaluation]:
        with KeyedObjectSpool[tuple[Any, ...]](self.output) as trade_spool:
            return self._score_strategies_batch_spooled(
                candidates, context, trade_spool
            )

    @staticmethod
    def _strategy_trade_spool_key(strategy_id: str, scenario_id: str) -> str:
        return f"strategy-trades\x00{strategy_id}\x00{scenario_id}"

    def _score_strategies_batch_spooled(
        self,
        candidates: tuple[FrozenStrategyCandidate, ...],
        context: EvaluationContext,
        trade_spool: KeyedObjectSpool[tuple[Any, ...]],
    ) -> Mapping[str, CandidateEvaluation]:
        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        anchors = _context_anchors(context)
        runtimes = {
            candidate.entry.candidate_id: self.entry_runtime[
                candidate.entry.candidate_id
            ]
            for candidate in candidates
        }
        source_configs = {
            item.source.config.candidate_id: item.source.config
            for item in runtimes.values()
        }
        columns = list(
            dict.fromkeys(
                column
                for config in source_configs.values()
                for column in signal_required_columns(config)
            )
        )
        columns.extend(
            (
                "1s_mid_speed",
                "1s_mid_acceleration",
                FRESH_EXIT_VOLATILITY_COLUMN,
                *(
                    spec.column
                    for spec in fresh_regime_quantile_measurements(
                        self.regime_definition
                    )
                ),
            )
        )
        scenario_ids = (
            self.later_full_scenario_ids
            if context.stage in LATER_SENSITIVITY_STAGES
            else self.exit_search_scenario_ids
        )
        for candidate in candidates:
            for scenario_id in scenario_ids:
                trade_spool.register_key(
                    self._strategy_trade_spool_key(candidate.strategy_id, scenario_id)
                )
        censors: dict[str, Counter[str]] = {
            candidate.strategy_id: Counter() for candidate in candidates
        }
        complete: dict[str, dict[str, bool]] = {
            candidate.strategy_id: {scenario: True for scenario in scenario_ids}
            for candidate in candidates
        }
        entry_results: dict[str, list[FreshEntryDiagnosticsResult]] = {
            identifier: [] for identifier in runtimes
        }
        filter_baselines: dict[str, list[FreshEntryDiagnosticsResult]] = {
            identifier: [] for identifier in runtimes
        }

        for ordinal, anchor in enumerate(anchors, start=1):
            tape = self.source.load_session(anchor)
            frame = self._features(tape, columns)
            raw_events = generate_frozen_signal_events(
                frame, configs=tuple(source_configs.values()), engine="batch"
            )
            grouped = _events_by_candidate(raw_events)
            raw_baseline = _baseline_events(frame, tape)
            events_by_entry: dict[str, tuple[FrozenSignalEvent, ...]] = {}
            baseline_by_filter: dict[str, FreshEntryDiagnosticsResult] = {}
            runtime_items = tuple(runtimes.items())
            filters_by_sha: dict[str, FreshEventFilterConfig] = {}
            for _, runtime in runtime_items:
                filter_sha = fresh_event_filter_config_fingerprint(
                    runtime.event_filter, self.quantile_bank
                )
                filters_by_sha.setdefault(filter_sha, runtime.event_filter)
            filter_items = tuple(filters_by_sha.items())
            filtered_batch = enrich_and_filter_frozen_event_batch(
                frame,
                (
                    *(
                        FreshEventFilterRequest(
                            grouped.get(runtime.source.config.candidate_id, ()),
                            runtime.event_filter,
                        )
                        for _, runtime in runtime_items
                    ),
                    *(
                        FreshEventFilterRequest(raw_baseline, event_filter)
                        for _, event_filter in filter_items
                    ),
                ),
                quantile_bank=self.quantile_bank,
            )
            runtime_results = filtered_batch[: len(runtime_items)]
            filter_results = filtered_batch[len(runtime_items) :]
            for (identifier, _), filtered in zip(runtime_items, runtime_results):
                events = filtered.events
                events = _before_close(events, tape)
                if len({event.tick_index for event in events}) != len(events):
                    raise ValueError(
                        "a frozen entry emitted both directions on one tick"
                    )
                events_by_entry[identifier] = events
                entry_results[identifier].append(
                    _diagnose(tape, events, config=self.entry_diagnostic_config)
                )
            for (filter_sha, _), filtered in zip(filter_items, filter_results):
                baseline_by_filter[filter_sha] = _diagnose(
                    tape,
                    _before_close(filtered.events, tape),
                    config=self.entry_diagnostic_config,
                )
            for identifier, runtime in runtime_items:
                filter_sha = fresh_event_filter_config_fingerprint(
                    runtime.event_filter, self.quantile_bank
                )
                filter_baselines[identifier].append(baseline_by_filter[filter_sha])

            feature_rows = BoundDecisionFeatureRows(
                tape.ticks,
                decision_feature_rows(
                    frame,
                    velocity_column="1s_mid_speed",
                    acceleration_column="1s_mid_acceleration",
                ),
            )
            volatility_feature_rows = BoundVolatilityRows(
                tape.ticks,
                volatility_rows(frame, column=FRESH_EXIT_VOLATILITY_COLUMN),
            )
            prepared_by_gap: dict[int, Any] = {}
            prepared_by_scenario: dict[str, Any] = {}
            for scenario_id in scenario_ids:
                execution = self.executions[scenario_id]
                gap_ms = execution.maximum_intertick_gap_ms
                if gap_ms not in prepared_by_gap:
                    prepared_by_gap[gap_ms] = _prepare_replay_tape(
                        tape.ticks,
                        maximum_intertick_gap_ms=gap_ms,
                        _trusted_validated_ticks=True,
                    )
                prepared_by_scenario[scenario_id] = prepared_by_gap[gap_ms]
            for candidate in candidates:
                variant = self.exit_runtime[candidate.strategy_id]
                events = events_by_entry[candidate.entry.candidate_id]
                for scenario_id in scenario_ids:
                    execution = self.executions[scenario_id]
                    source = FrozenSignalDecisionSource(
                        events,
                        feature_rows=feature_rows,
                        weakening=variant.weakening,
                        execution=execution,
                        source_metadata={
                            "frozenStrategyId": candidate.strategy_id,
                            "frozenStrategySha256": candidate.strategy_sha256,
                            "frozenEntryCandidateId": candidate.entry.candidate_id,
                        },
                    )
                    volatility = (
                        volatility_feature_rows.cursor()
                        if variant.policy.requires_volatility
                        else None
                    )
                    policy = FreshProtectiveExitPolicy(
                        source,
                        config=variant.policy,
                        execution=execution,
                        volatility=volatility,
                    )
                    replay = _replay_session(
                        tape,
                        policy,
                        config=execution,
                        prepared_replay_tape=prepared_by_scenario[scenario_id],
                    )
                    trade_spool.append(
                        self._strategy_trade_spool_key(
                            candidate.strategy_id, scenario_id
                        ),
                        tuple(replay.trades),
                    )
                    censors[candidate.strategy_id][scenario_id] += len(replay.censors)
                    complete[candidate.strategy_id][scenario_id] &= bool(
                        replay.boundary_reached
                        and not replay.halted
                        and not replay.censors
                    )
            self._emit(
                stage=context.stage,
                sessionOrdinal=ordinal,
                sessionCount=len(anchors),
                sessionAnchor=anchor,
            )
            del frame, tape, raw_events, grouped, raw_baseline

        if any(count != len(anchors) for _, count in trade_spool.inventory):
            raise RuntimeError("strategy trade spool session inventory is incomplete")
        entry_summaries: dict[
            tuple[str, str], tuple[EntryScoreReport, _EntryEdgeSummary]
        ] = {}
        for candidate in candidates:
            entry_id = candidate.entry.candidate_id
            summary_key = (entry_id, candidate.entry.entry_sha256)
            if summary_key in entry_summaries:
                continue
            combined_entry = combine_entry_diagnostics(tuple(entry_results[entry_id]))
            entry_report = score_entry_diagnostics(
                combined_entry,
                config=self.scoring.entry_metrics,
                dimensions=self.dimensions,
                evaluated_sessions=anchors,
            )
            entry_edge = _entry_edge_summary(
                entry_results[entry_id],
                filter_baselines[entry_id],
                seed_text=f"{context.stage}:{candidate.entry.entry_sha256}",
            )
            entry_summaries[summary_key] = (entry_report, entry_edge)

        provisional: dict[
            str,
            tuple[CandidateEvaluation, bool, TradeScoreReport, EntryScoreReport],
        ] = {}
        for candidate in candidates:
            entry_id = candidate.entry.candidate_id
            entry_report, entry_edge = entry_summaries[
                (entry_id, candidate.entry.entry_sha256)
            ]
            reports: dict[str, TradeScoreReport] = {}
            for scenario_id in scenario_ids:
                censor_count = int(censors[candidate.strategy_id][scenario_id])
                with trade_spool.load(
                    self._strategy_trade_spool_key(candidate.strategy_id, scenario_id)
                ) as trade_batches:
                    reports[scenario_id] = score_trade_records(
                        (
                            trade
                            for session_trades in trade_batches
                            for trade in session_trades
                        ),
                        config=self.scoring.trade_metrics,
                        dimensions=self.dimensions,
                        evaluated_sessions=anchors,
                        replay_censor_count=censor_count,
                        profitability_valid=bool(
                            complete[candidate.strategy_id][scenario_id]
                            and censor_count == 0
                        ),
                    )
            reference = reports.pop(REFERENCE_SCENARIO_ID)
            required_reports = {
                scenario_id: reports[scenario_id]
                for scenario_id in self.scoring.required_stress_scenario_ids
            }
            sensitivity_reports = {
                scenario_id: report
                for scenario_id, report in reports.items()
                if scenario_id not in required_reports
            }
            scorecard = build_candidate_scorecard(
                entry_report,
                reference,
                required_reports,
                config=self.scoring,
            )
            passed = bool(
                scorecard.full_gate.passed and entry_edge.baseline_gate_passed
            )
            evaluation = CandidateEvaluation(
                identity_sha256=candidate.strategy_sha256,
                passed=passed,
                metrics={
                    "entry": _compact_entry_slices(entry_report),
                    "entryEdge": asdict(entry_edge),
                    "reference": _compact_trade_slices(reference),
                    "stresses": {
                        key: _compact_trade_slices(value)
                        for key, value in sorted(required_reports.items())
                    },
                    "sensitivities": {
                        key: _compact_trade_slices(value)
                        for key, value in sorted(sensitivity_reports.items())
                    },
                    "registeredEntryGate": asdict(scorecard.entry_gate),
                    "registeredFullGate": asdict(scorecard.full_gate),
                    "balancedScore": asdict(scorecard.balanced_score),
                },
                leakage_checks={
                    "entryDefinitionUnchangedDuringExitSearch": True,
                    "sessionCorpusFingerprintVerifiedBeforeReplay": True,
                    "causalFeatureRowsBoundToEveryReplayTick": True,
                    "stopsTriggerOnObservedExecutableQuote": True,
                    "allFillsUseStrictlyLaterObservedQuotes": True,
                },
                score=scorecard.balanced_score.score,
            )
            provisional[candidate.strategy_id] = (
                evaluation,
                passed,
                reference,
                entry_report,
            )

        grouped_strategies: dict[str, list[FrozenStrategyCandidate]] = defaultdict(list)
        for candidate in candidates:
            variant = self.exit_runtime[candidate.strategy_id]
            group = "::".join(
                (
                    variant.stop_structure_id,
                    variant.management_structure_id,
                    variant.invalidation_structure_id,
                )
            )
            grouped_strategies[group].append(candidate)

        neighbourhood_spec = self.preregistration["robustnessAndGates"][
            "parameterNeighborhood"
        ]
        neighbourhood_audits: dict[str, dict[str, Any]] = {}
        if context.stage == "exit_search":
            for group, members in grouped_strategies.items():
                neighbourhood_audits[group] = _parameter_neighbourhood_audit(
                    tuple(
                        (
                            candidate.strategy_id,
                            self.exit_runtime[candidate.strategy_id].rank_offset,
                            provisional[candidate.strategy_id][1],
                            provisional[candidate.strategy_id][2].overall.expectancy,
                            None,
                            canonical_hash(
                                {
                                    "policy": asdict(
                                        self.exit_runtime[candidate.strategy_id].policy
                                    ),
                                    "weakening": (
                                        asdict(
                                            self.exit_runtime[
                                                candidate.strategy_id
                                            ].weakening
                                        )
                                        if self.exit_runtime[
                                            candidate.strategy_id
                                        ].weakening
                                        is not None
                                        else None
                                    ),
                                }
                            ),
                        )
                        for candidate in members
                    ),
                    minimum_valid_neighbor_fraction=float(
                        neighbourhood_spec["minimumValidNeighborFraction"]
                    ),
                    minimum_positive_expectancy_neighbor_fraction=float(
                        neighbourhood_spec["minimumPositiveExpectancyNeighborFraction"]
                    ),
                    minimum_neighbor_expectancy_retention=float(
                        neighbourhood_spec["minimumNeighborExpectancyRetention"]
                    ),
                    maximum_absolute_coverage_30_drop=None,
                )

        output: dict[str, CandidateEvaluation] = {}
        for candidate in candidates:
            evaluation, base_passed, _, _ = provisional[candidate.strategy_id]
            variant = self.exit_runtime[candidate.strategy_id]
            group = "::".join(
                (
                    variant.stop_structure_id,
                    variant.management_structure_id,
                    variant.invalidation_structure_id,
                )
            )
            required = context.stage == "exit_search"
            neighbourhood = neighbourhood_audits.get(
                group,
                {
                    "centerCandidateId": candidate.strategy_id,
                    "evaluatedCount": 1,
                    "passed": True,
                },
            )
            neighbourhood_passed = bool(neighbourhood["passed"])
            is_center = variant.rank_offset == 0.0
            metrics = dict(evaluation.metrics)
            metrics["exitParameterNeighbourhood"] = {
                **neighbourhood,
                "group": group,
                "requiredDuringStage": required,
                "candidateIsCenter": is_center,
            }
            output[candidate.strategy_id] = CandidateEvaluation(
                identity_sha256=evaluation.identity_sha256,
                passed=bool(
                    base_passed
                    and (not required or (is_center and neighbourhood_passed))
                ),
                metrics=metrics,
                leakage_checks=evaluation.leakage_checks,
                score=evaluation.score,
            )
        return output

    def authorize_holdout(
        self,
        winner: FrozenStrategyCandidate,
        records: tuple[Mapping[str, Any], ...],
        explicit: bool,
    ) -> Mapping[str, Any]:
        if explicit is not True:
            raise PermissionError("holdout authorization must be explicit")
        matching_wf3 = [
            item
            for item in records
            if item.get("role") == "walk_forward_3"
            and item.get("frozenStrategySha256") == winner.strategy_sha256
            and item.get("gatePassed") is True
        ]
        matching_validation = [
            item
            for item in records
            if item.get("role") == "validation"
            and item.get("frozenStrategySha256") == winner.strategy_sha256
            and item.get("gatePassed") is True
        ]
        if len(matching_wf3) != 1 or len(matching_validation) != 1:
            raise PermissionError(
                "holdout requires exact passed WF3 and validation records"
            )
        frozen = {
            "schema": "fresh-xauusd-final-strategy/v1",
            "strategyId": winner.strategy_id,
            "strategySha256": winner.strategy_sha256,
            "entryCandidateId": winner.entry.candidate_id,
            "entrySha256": winner.entry.entry_sha256,
            "entryConfig": winner.entry.config,
            "exitConfig": winner.exit_config,
            "executionConfig": winner.execution_config,
            "noPostHoldoutTuning": True,
        }
        _write_new_json(self.output / "fresh_final_strategy_frozen_v1.json", frozen)
        return authorize_registered_holdout(
            preregistration=self.preregistration,
            split_manifest=self.split_manifest,
            frozen_strategy_sha256=winner.strategy_sha256,
            walk_forward_3_record_number=int(matching_wf3[0]["recordNumber"]),
            validation_record_number=int(matching_validation[0]["recordNumber"]),
            explicit_holdout_authorization=True,
            verify_current_implementation_files=(
                self.verify_preregistration_implementation_files
            ),
            infrastructure_recovery_contract=self.recovery_contract,
            recovery_implementation_manifest=self.recovery_implementation_manifest,
            recovery_batch_result_path=self.recovery_batch_result_path,
        )

    @staticmethod
    def _unused_signal_generator(
        _candidate: FrozenEntryCandidate, _context: EvaluationContext
    ) -> None:
        raise RuntimeError("batched entry scorer is required")

    @staticmethod
    def _unused_entry_scorer(
        _candidate: FrozenEntryCandidate,
        _context: EvaluationContext,
        _signals: Any,
    ) -> CandidateEvaluation:
        raise RuntimeError("batched entry scorer is required")

    @staticmethod
    def _unused_scenario_runner(
        _candidate: FrozenStrategyCandidate,
        _context: EvaluationContext,
        _signals: Any,
    ) -> None:
        raise RuntimeError("batched strategy scorer is required")

    @staticmethod
    def _unused_strategy_scorer(
        _candidate: FrozenStrategyCandidate,
        _context: EvaluationContext,
        _results: Any,
    ) -> CandidateEvaluation:
        raise RuntimeError("batched strategy scorer is required")

    def _search_budgets(self) -> FreshSearchBudgets:
        budgets = self.preregistration["candidateSearch"]["budgets"]
        return FreshSearchBudgets(
            discovery_distinct_candidates=int(budgets["discoveryDistinctCandidates"]),
            discovery_per_family_maximum=int(budgets["discoveryPerFamilyMaximum"]),
            walk_forward_1_frozen_candidates=int(
                budgets["walkForward1FrozenCandidates"]
            ),
            walk_forward_2_frozen_candidates=int(
                budgets["walkForward2FrozenCandidates"]
            ),
            exit_variants_after_entry_gate=int(budgets["exitVariantsAfterEntryGate"]),
            walk_forward_3_full_strategies=int(budgets["walkForward3FullStrategies"]),
            validation_full_strategies=int(budgets["validationFullStrategies"]),
            holdout_full_strategies=int(budgets["holdoutFullStrategies"]),
            exit_search_frozen_entries=int(budgets["exitSearchFrozenEntries"]),
        )

    def _search_callbacks(self) -> FreshSearchCallbacks:
        return FreshSearchCallbacks(
            fit_thresholds=self.fit_thresholds,
            build_entry_candidates=self.build_entry_candidates,
            generate_signals=self._unused_signal_generator,
            score_entry=self._unused_entry_scorer,
            build_exit_variants=self.build_exit_variants,
            run_execution_scenarios=self._unused_scenario_runner,
            score_strategy=self._unused_strategy_scorer,
            authorize_holdout=self.authorize_holdout,
            score_entries_batch=self.score_entries_batch,
            score_strategies_batch=self.score_strategies_batch,
        )

    def build_search(self) -> FreshChronologicalSearch:
        return FreshChronologicalSearch(
            split_manifest=self.split_manifest,
            ledger_path=self.preregistration["sourceBindings"]["experimentLedgerPath"],
            budgets=self._search_budgets(),
            callbacks=self._search_callbacks(),
            preregistration_sha256=str(self.preregistration["preregistrationSha256"]),
        )

    def resume_incomplete_discovery_search(
        self,
        *,
        entry_specs: Sequence[EntryCandidateSpec],
        recovery_audit: Mapping[str, Any],
        recovery_batch_result_path: str | Path,
    ) -> FreshChronologicalSearch:
        if self.quantile_bank is None:
            raise RuntimeError("recovery quantile bank has not been restored")
        return FreshChronologicalSearch.resume_incomplete_discovery(
            split_manifest=self.split_manifest,
            ledger_path=self.preregistration["sourceBindings"]["experimentLedgerPath"],
            budgets=self._search_budgets(),
            callbacks=self._search_callbacks(),
            preregistration_sha256=str(self.preregistration["preregistrationSha256"]),
            threshold_bank=fresh_quantile_bank_payload(self.quantile_bank),
            entry_specs=entry_specs,
            recovery_audit=recovery_audit,
            recovery_batch_result_path=recovery_batch_result_path,
        )


def _strongest_record(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    stage_order = {
        "discovery": 0,
        "walk_forward_1": 1,
        "walk_forward_2": 2,
        "exit_search": 3,
        "walk_forward_3": 4,
        "validation": 5,
        "holdout": 6,
    }
    eligible = [
        item
        for item in records
        if item.get("stage") in stage_order
        and isinstance(item.get("balancedScore"), (int, float))
        and not isinstance(item.get("balancedScore"), bool)
    ]
    if not eligible:
        return None
    furthest = max(stage_order[str(item["stage"])] for item in eligible)
    selected = [
        item for item in eligible if stage_order[str(item["stage"])] == furthest
    ]
    return min(
        selected,
        key=lambda item: (-float(item["balancedScore"]), str(item["candidateId"])),
    )


def run_registered_fresh_research(
    connection_context_factory: Any,
    *,
    repository_root: str | Path,
    output_directory: str | Path,
    research_state_directory: str | Path,
    progress: ProgressCallback | None = None,
    resume_artifact_directory: str | Path | None = None,
) -> dict[str, Any]:
    """Run the frozen study or the sole audited continuation of run 14."""

    root = Path(repository_root).resolve()
    output = Path(output_directory).resolve()
    state_root = Path(research_state_directory).expanduser().resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("fresh research output directory must be empty")
    if state_root == output or output in state_root.parents:
        raise ValueError("durable research state cannot be inside temporary output")
    output.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)

    recovery_used = resume_artifact_directory is not None
    recovery_manifest: Mapping[str, Any] | None = None
    recovery_equivalence_evidence: Mapping[str, Any] | None = None
    recovery_bundle = None
    if recovery_used:
        recovery_bundle = load_run14_recovery_bundle(resume_artifact_directory)
        bootstrap = {
            "inventory": recovery_bundle.inventory,
            "corpus": recovery_bundle.corpus,
            "split": recovery_bundle.split,
        }
    else:
        bootstrap = build_fresh_source_bootstrap(
            connection_context_factory,
            config=registered_fresh_bootstrap_config(),
            on_progress=progress,
        )
        write_fresh_source_bootstrap(output, bootstrap)

    state_binding = _research_state_binding(state_root, bootstrap["split"])
    ledger = Path(state_binding["experimentLedgerPath"])
    holdout_registry = Path(state_binding["holdoutAuthorizationRegistryPath"])
    for durable_path in (ledger, holdout_registry):
        if durable_path.is_symlink():
            raise PermissionError("durable research state cannot be a symbolic link")
    if holdout_registry.exists():
        raise PermissionError("this frozen holdout window has already been consumed")
    ledger.parent.mkdir(parents=True, exist_ok=True)
    holdout_registry.parent.mkdir(parents=True, exist_ok=True)

    if recovery_used:
        if recovery_bundle is None:
            raise RuntimeError("run-14 recovery bundle was not loaded")
        if state_binding != recovery_bundle.state_binding:
            raise PermissionError("durable run-14 state binding changed")
        if not ledger.is_file() or _file_sha256(ledger) != RUN14_LEDGER_SHA256:
            raise PermissionError(
                "the original durable run-14 ledger was not preserved byte-for-byte"
            )
        for source_name, destination_name in (
            ("fresh_source_inventory_v1.json", "fresh_source_inventory_v1.json"),
            ("fresh_corpus_manifest_v1.json", "fresh_corpus_manifest_v1.json"),
            ("fresh_split_manifest_v2.json", "fresh_split_manifest_v2.json"),
            (
                "fresh_research_state_binding_v1.json",
                "fresh_research_state_binding_v1.json",
            ),
            (
                "fresh_implementation_manifest_v1.json",
                "fresh_implementation_manifest_v1.json",
            ),
            ("fresh_preregistration_v2.json", "fresh_preregistration_v2.json"),
            ("fresh_quantile_bank_v1.json", "fresh_quantile_bank_v1.json"),
            (
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            ),
            ("server-run.log", "run14_server-run.log"),
            ("remote-exit-status.txt", "run14_remote-exit-status.txt"),
        ):
            _snapshot_new_file(
                recovery_bundle.paths[source_name], output / destination_name
            )
        implementation = recovery_bundle.implementation
        preregistration = recovery_bundle.preregistration
        recovery_manifest = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=(
                *required_fresh_implementation_files(),
                "datavis/research/fresh_recovery.py",
                "datavis/research/fresh_spool.py",
                "test_fresh_pipeline.py",
                "test_fresh_preregistration.py",
                "test_fresh_recovery.py",
                "test_fresh_search.py",
                "test_fresh_spool.py",
            ),
        )
        _write_new_json(
            output / "fresh_recovery_implementation_manifest_v1.json",
            recovery_manifest,
        )
        recovery_equivalence_evidence = run_run14_recovery_equivalence_preflight(
            root,
            resume_artifact_directory,
            recovery_implementation_manifest=recovery_manifest,
        )
        if progress is not None:
            progress(
                {
                    "stage": "recovery_equivalence_preflight",
                    "status": "passed",
                    "testModuleCount": len(
                        recovery_equivalence_evidence["testModules"]
                    ),
                    "evidenceSha256": canonical_hash(recovery_equivalence_evidence),
                }
            )
    else:
        if ledger.exists() and ledger.stat().st_size:
            raise PermissionError(
                "this frozen split already has a durable experiment ledger"
            )
        _write_new_json(output / "fresh_research_state_binding_v1.json", state_binding)
        implementation = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=required_fresh_implementation_files(),
        )
        _write_new_json(
            output / "fresh_implementation_manifest_v1.json", implementation
        )
        preregistration = build_fresh_preregistration_v2(
            split_manifest=bootstrap["split"],
            corpus_manifest_sha256=str(bootstrap["corpus"]["corpusManifestSha256"]),
            protocol_code_identifier=(
                "fresh-pipeline-v1:" + implementation["manifestSha256"]
            ),
            implementation_manifest=implementation,
            experiment_ledger_path=ledger,
            holdout_authorization_registry_path=holdout_registry,
        )
        _write_new_json(output / "fresh_preregistration_v2.json", preregistration)

    pipeline = RegisteredFreshResearchPipeline(
        repository_root=root,
        output_directory=output,
        connection_context_factory=connection_context_factory,
        corpus_manifest=bootstrap["corpus"],
        split_manifest=bootstrap["split"],
        preregistration=preregistration,
        progress=progress,
        verify_preregistration_implementation_files=not recovery_used,
    )
    if recovery_used:
        if (
            recovery_bundle is None
            or recovery_manifest is None
            or recovery_equivalence_evidence is None
        ):
            raise RuntimeError("run-14 recovery identities were not frozen")
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            recovery_bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(recovery_bundle.threshold_preflight)
        discovery_window = bootstrap["split"]["windows"]["discovery"]
        recovery_context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(discovery_window["sessionAnchors"]),
                    window_sha256=canonical_hash(discovery_window),
                ),
            ),
        )
        entry_specs = tuple(
            pipeline.build_entry_candidates(
                recovery_bundle.quantile_bank, recovery_context
            )
        )
        entry_bank_path = output / "fresh_entry_bank_v1.json"
        if _file_sha256(entry_bank_path) != RUN14_ENTRY_BANK_FILE_SHA256:
            raise PermissionError("reconstructed run-14 entry-bank bytes changed")
        recovery_audit, recovery_contract = build_run14_recovery_contract(
            recovery_bundle,
            entry_specs=entry_specs,
            recovery_implementation_manifest=recovery_manifest,
            generated_entry_bank_path=entry_bank_path,
            equivalence_evidence=recovery_equivalence_evidence,
        )
        _write_new_json(output / "fresh_recovery_contract_v1.json", recovery_contract)
        pipeline.recovery_contract = recovery_contract
        pipeline.recovery_implementation_manifest = recovery_manifest
        pipeline.recovery_batch_result_path = (
            output / "fresh_recovery_discovery_batch_v1.json"
        )
        search = pipeline.resume_incomplete_discovery_search(
            entry_specs=entry_specs,
            recovery_audit=recovery_audit,
            recovery_batch_result_path=(pipeline.recovery_batch_result_path),
        )
        operations = (
            search.resume_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    else:
        search = pipeline.build_search()
        operations = (
            search.run_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    for operation in operations:
        result = operation()
        pipeline.stage_results.append(result)
        if progress is not None:
            progress(
                {
                    "stage": result.stage,
                    "evaluatedCount": len(result.evaluated_ids),
                    "promotedCount": len(result.promoted_ids),
                    "studyFailed": result.study_failed,
                }
            )
        if result.study_failed:
            break
    if pipeline.stage_results and pipeline.stage_results[-1].stage == "validation":
        if not pipeline.stage_results[-1].study_failed:
            search.authorize_holdout(explicit_holdout_authorization=True)
            holdout = search.run_holdout()
            pipeline.stage_results.append(holdout)
            if progress is not None:
                progress(
                    {
                        "stage": holdout.stage,
                        "evaluatedCount": len(holdout.evaluated_ids),
                        "promotedCount": len(holdout.promoted_ids),
                        "studyFailed": holdout.study_failed,
                    }
                )

    _snapshot_new_file(ledger, output / "fresh_experiment_ledger_v1.jsonl")
    if holdout_registry.is_file():
        _snapshot_new_file(
            holdout_registry,
            output / "fresh_holdout_authorization_v1.json",
        )
    records = search.audit_records
    strongest = _strongest_record(records)
    terminal_stage = pipeline.stage_results[-1] if pipeline.stage_results else None
    validated = bool(
        terminal_stage is not None
        and terminal_stage.stage == "holdout"
        and not terminal_stage.study_failed
    )
    summary = {
        "schema": FRESH_PIPELINE_SCHEMA,
        "status": (
            "validated_holdout_pass"
            if validated
            else "no_robust_setup_survived_frozen_validation"
        ),
        "preregistrationSha256": preregistration["preregistrationSha256"],
        "implementationManifestSha256": implementation["manifestSha256"],
        "recoveryUsed": recovery_used,
        "recoveryOriginalRunId": RUN14_RUN_ID if recovery_used else None,
        "recoveryImplementationManifestSha256": (
            recovery_manifest["manifestSha256"]
            if recovery_manifest is not None
            else None
        ),
        "splitManifestSha256": bootstrap["split"]["manifestSha256"],
        "corpusManifestSha256": bootstrap["corpus"]["corpusManifestSha256"],
        "holdoutOpened": any(
            item.stage == "holdout" for item in pipeline.stage_results
        ),
        "stageResults": [asdict(item) for item in pipeline.stage_results],
        "strongestRecord": dict(strongest) if strongest is not None else None,
        "artifactFiles": sorted(
            path.name for path in output.iterdir() if path.is_file()
        ),
    }
    _write_new_json(output / "fresh_run_summary_v1.json", summary)
    return summary


__all__ = [
    "BASELINE_BOOTSTRAP_REPLICATES",
    "BASELINE_CLUSTER_CONFIDENCE",
    "BASELINE_EVENTS_PER_SIDE_PER_SESSION",
    "BASELINE_MINIMUM_UPLIFT",
    "FRESH_PIPELINE_SCHEMA",
    "RegisteredFreshResearchPipeline",
    "run_registered_fresh_research",
]
