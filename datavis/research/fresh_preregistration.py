"""Outcome-blind preregistration for the fresh XAUUSD acceleration study.

This module contains research governance, measurement banks, and validation
gates.  It deliberately contains no fitted signal threshold and never opens a
tick file.  A preregistration is deterministic: it binds the already-frozen
chronological split and corpus manifest, then hashes every rule that is allowed
to influence later candidate selection.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from datavis.research.fresh_entry_diagnostics import EntryDiagnosticConfig
from datavis.research.fresh_features import FreshFeatureConfig
from datavis.research.fresh_protocol import (
    FreshWindowPolicy,
    authorize_evaluation,
    canonical_hash,
)
from datavis.research.fresh_replay import FreshExecutionConfig
from datavis.research.fresh_sessions import SessionAuditConfig


PREREGISTRATION_SCHEMA = "fresh-xauusd-acceleration-preregistration/v2"
IMPLEMENTATION_MANIFEST_SCHEMA = "fresh-xauusd-implementation-manifest/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ROLE_ORDER = (
    "discovery",
    "walk_forward_1",
    "walk_forward_2",
    "walk_forward_3",
    "validation",
    "holdout",
)
FRESH_V2_WINDOW_POLICY = FreshWindowPolicy(
    discovery_sessions=46,
    walk_forward_sessions=(12, 12, 12),
    validation_sessions=21,
    holdout_sessions=36,
)
_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "datavis/db.py",
        "datavis/research/fresh_bootstrap.py",
        "datavis/research/fresh_candidate_grid.py",
        "datavis/research/fresh_data.py",
        "datavis/research/fresh_db_source.py",
        "datavis/research/fresh_decisions.py",
        "datavis/research/fresh_entry_diagnostics.py",
        "datavis/research/fresh_event_filters.py",
        "datavis/research/fresh_exit_grid.py",
        "datavis/research/fresh_exits.py",
        "datavis/research/fresh_feature_bank.py",
        "datavis/research/fresh_features.py",
        "datavis/research/fresh_inventory.py",
        "datavis/research/fresh_pipeline.py",
        "datavis/research/fresh_pipeline_cli.py",
        "datavis/research/fresh_preregistration.py",
        "datavis/research/fresh_protocol.py",
        "datavis/research/fresh_replay.py",
        "datavis/research/fresh_scoring.py",
        "datavis/research/fresh_search.py",
        "datavis/research/fresh_session_eval.py",
        "datavis/research/fresh_sessions.py",
        "datavis/research/fresh_signals.py",
        "datavis/research/fresh_thresholds.py",
        "datavis/research/ticks.py",
    }
)


def required_fresh_implementation_files() -> tuple[str, ...]:
    """Return the complete sorted code-state binding for the registered run."""

    return tuple(sorted(_REQUIRED_IMPLEMENTATION_FILES))


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value.lower()) is None:
        raise ValueError(f"{name} must be a SHA-256 hex digest")
    return value.lower()


def _registered_file_path(value: str | Path, name: str) -> str:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise ValueError(f"{name} must be a non-empty file path")
    selected = Path(value).expanduser().resolve()
    if selected.exists() and selected.is_dir():
        raise ValueError(f"{name} must name a file, not a directory")
    return str(selected)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_fresh_implementation_manifest(
    *,
    repository_root: str | Path,
    relative_paths: Sequence[str | Path],
) -> dict[str, Any]:
    """Hash the actual implementation bytes used by the registered protocol."""

    root = Path(repository_root).expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise ValueError("repository_root must be a real directory")
    normalized: list[str] = []
    for raw_path in relative_paths:
        if not isinstance(raw_path, (str, Path)):
            raise ValueError("implementation paths must be relative file paths")
        relative = Path(raw_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("implementation paths must stay inside repository_root")
        full = (root / relative).resolve()
        try:
            resolved_relative = full.relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError("implementation paths must stay inside repository_root") from exc
        if not full.is_file() or full.is_symlink():
            raise ValueError(f"implementation file is unavailable: {resolved_relative}")
        normalized.append(resolved_relative)
    if len(normalized) != len(set(normalized)) or not normalized:
        raise ValueError("implementation paths must be non-empty and unique")
    if not _REQUIRED_IMPLEMENTATION_FILES.issubset(normalized):
        missing = sorted(_REQUIRED_IMPLEMENTATION_FILES.difference(normalized))
        raise ValueError(f"implementation manifest is missing required files: {missing}")
    files = [
        {
            "path": relative,
            "sha256": _file_sha256(root / Path(relative)),
        }
        for relative in sorted(normalized)
    ]
    body = {
        "schema": IMPLEMENTATION_MANIFEST_SCHEMA,
        "repositoryRoot": str(root),
        "files": files,
    }
    return {**body, "manifestSha256": canonical_hash(body)}


def validate_fresh_implementation_manifest(
    manifest: Mapping[str, Any],
    *,
    verify_current_files: bool = True,
) -> str:
    """Validate the manifest hash and, by default, every current file byte."""

    if not isinstance(manifest, Mapping):
        raise ValueError("implementation_manifest must be a mapping")
    body = copy.deepcopy(dict(manifest))
    claimed = _sha256(str(body.pop("manifestSha256", "")), "manifestSha256")
    if set(body) != {"schema", "repositoryRoot", "files"} or body.get(
        "schema"
    ) != IMPLEMENTATION_MANIFEST_SCHEMA:
        raise ValueError("implementation manifest has an invalid schema")
    if canonical_hash(body) != claimed:
        raise ValueError("implementation manifest hash does not match")
    root = Path(str(body["repositoryRoot"])).expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise ValueError("implementation repository root is unavailable")
    files = body.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("implementation manifest must contain files")
    paths: list[str] = []
    for record in files:
        if not isinstance(record, Mapping) or set(record) != {"path", "sha256"}:
            raise ValueError("implementation file records are invalid")
        path = record.get("path")
        if not isinstance(path, str):
            raise ValueError("implementation paths must be strings")
        relative = Path(path)
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != path:
            raise ValueError("implementation paths must be canonical and relative")
        expected_sha = _sha256(str(record.get("sha256", "")), "file sha256")
        full = (root / relative).resolve()
        try:
            full.relative_to(root)
        except ValueError as exc:
            raise ValueError("implementation path escapes repository root") from exc
        if verify_current_files and (
            not full.is_file() or full.is_symlink() or _file_sha256(full) != expected_sha
        ):
            raise ValueError(f"implementation bytes changed: {path}")
        paths.append(path)
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise ValueError("implementation files must be unique and sorted")
    if not _REQUIRED_IMPLEMENTATION_FILES.issubset(paths):
        raise ValueError("implementation manifest omits required protocol files")
    return claimed


def _manifest_body_and_hash(split_manifest: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    if not isinstance(split_manifest, Mapping):
        raise ValueError("split_manifest must be a mapping")
    body = copy.deepcopy(dict(split_manifest))
    claimed = _sha256(str(body.pop("manifestSha256", "")), "manifestSha256")
    if canonical_hash(body) != claimed:
        raise ValueError("split manifest hash does not match its contents")
    return body, claimed


def _validate_split(split_manifest: Mapping[str, Any]) -> str:
    """Reject a split that differs from the outcome-blind 139-session policy."""

    body, digest = _manifest_body_and_hash(split_manifest)
    expected_top_level = {
        "schemaVersion",
        "policy",
        "inventorySha256",
        "sessionCount",
        "excludedSessionsBeforeOutcomeInspection",
        "windows",
        "assignments",
        "sessionSchedule",
        "holdoutPolicy",
    }
    if set(body) != expected_top_level or body.get("schemaVersion") != 2:
        raise ValueError("split manifest does not have the exact v2 schema")
    _sha256(str(body.get("inventorySha256", "")), "inventorySha256")
    expected_policy = asdict(FRESH_V2_WINDOW_POLICY)
    # Tuple values become arrays after a JSON round trip.  Their canonical JSON
    # is intentionally identical, so persisted manifests remain valid.
    if canonical_hash(body.get("policy")) != canonical_hash(expected_policy):
        raise ValueError("split manifest does not use the fresh v2 window policy")
    if body.get("sessionCount") != FRESH_V2_WINDOW_POLICY.required_sessions:
        raise ValueError("split manifest must contain exactly 139 eligible sessions")

    schedule = body.get("sessionSchedule")
    expected_schedule = {
        "timezone": "America/New_York",
        "start": "previous-calendar-day 18:00",
        "end": "anchor-calendar-day 17:00",
        "interval": "half-open",
        "sydneyConversion": "IANA Australia/Sydney",
    }
    if schedule != expected_schedule:
        raise ValueError("split manifest does not use the frozen DST-aware schedule")
    if body.get("holdoutPolicy") != {
        "maximumFrozenCandidates": 1,
        "explicitAuthorizationRequired": True,
        "repeatEvaluationForbidden": True,
    }:
        raise ValueError("split manifest does not protect a single final holdout")
    exclusions = body.get("excludedSessionsBeforeOutcomeInspection")
    if (
        not isinstance(exclusions, list)
        or not exclusions
        or any(
            not isinstance(record, Mapping)
            or not isinstance(record.get("reason"), str)
            or not record["reason"].strip()
            for record in exclusions
        )
    ):
        raise ValueError("split exclusions must contain explicit pre-outcome QC reasons")
    excluded_anchors: list[date] = []
    for record in exclusions:
        raw_anchor = record.get("sessionAnchor")
        if not isinstance(raw_anchor, str):
            raise ValueError("every split exclusion must identify an ISO sessionAnchor")
        try:
            excluded_anchor = date.fromisoformat(raw_anchor)
        except ValueError as exc:
            raise ValueError("every split exclusion must identify an ISO sessionAnchor") from exc
        if excluded_anchor.weekday() >= 5:
            raise ValueError("excluded session anchors must be Monday through Friday")
        if excluded_anchors and excluded_anchor <= excluded_anchors[-1]:
            raise ValueError("excluded session anchors must be unique and chronological")
        excluded_anchors.append(excluded_anchor)

    windows = body.get("windows")
    assignments = body.get("assignments")
    if not isinstance(windows, Mapping) or not isinstance(assignments, list):
        raise ValueError("split manifest windows and assignments are required")
    if set(windows) != set(_ROLE_ORDER):
        raise ValueError("split manifest must contain exactly the six registered roles")
    expected_counts = (
        FRESH_V2_WINDOW_POLICY.discovery_sessions,
        *FRESH_V2_WINDOW_POLICY.walk_forward_sessions,
        FRESH_V2_WINDOW_POLICY.validation_sessions,
        FRESH_V2_WINDOW_POLICY.holdout_sessions,
    )
    flattened: list[tuple[str, str]] = []
    prior_anchor: date | None = None
    for role, expected_count in zip(_ROLE_ORDER, expected_counts):
        window = windows.get(role)
        if not isinstance(window, Mapping):
            raise ValueError(f"split manifest is missing {role}")
        anchors = window.get("sessionAnchors")
        if (
            window.get("role") != role
            or window.get("sessionCount") != expected_count
            or not isinstance(anchors, list)
            or len(anchors) != expected_count
        ):
            raise ValueError(f"split manifest has an invalid {role} window")
        if window.get("firstSessionAnchor") != anchors[0] or window.get(
            "lastSessionAnchor"
        ) != anchors[-1]:
            raise ValueError(f"split manifest has inconsistent {role} bounds")
        for anchor in anchors:
            if not isinstance(anchor, str):
                raise ValueError("split anchors must be ISO weekday dates")
            try:
                parsed_anchor = date.fromisoformat(anchor)
            except ValueError as exc:
                raise ValueError("split anchors must be ISO weekday dates") from exc
            if parsed_anchor.weekday() >= 5:
                raise ValueError("split anchors must be Monday through Friday")
            if prior_anchor is not None and parsed_anchor <= prior_anchor:
                raise ValueError("split anchors must be unique and strictly chronological")
            prior_anchor = parsed_anchor
            flattened.append((anchor, role))

    assignment_pairs = [
        (record.get("sessionAnchor"), record.get("role"))
        for record in assignments
        if isinstance(record, Mapping)
        and record.get("classification") == "new_york_maintenance_schedule"
    ]
    if assignment_pairs != flattened or len(assignments) != len(flattened):
        raise ValueError("split assignments do not exactly match chronological windows")
    eligible_dates = {date.fromisoformat(anchor) for anchor, _ in flattened}
    if eligible_dates.intersection(excluded_anchors):
        raise ValueError("excluded and eligible session anchors must be disjoint")
    return digest


def _session_and_data_specification() -> dict[str, Any]:
    return {
        "symbol": "XAUUSD",
        "quoteFields": ["id", "symbol", "timestamp", "bid", "ask"],
        "inputValidation": {
            "id": "non-negative integer, globally unique across ordered source files",
            "symbol": "non-empty and exactly XAUUSD",
            "timestamp": "timezone-aware ISO timestamp convertible to UTC",
            "bidAsk": "numeric, finite, strictly positive, and ask >= bid",
            "lockedQuote": "ask == bid is valid and separately counted",
            "invalidIdentity": "fail the corpus scan immediately",
            "invalidBidAsk": "record the row, do not emit it, and mark its broker session ineligible",
            "extraColumns": "ignored",
            "rowSorting": "forbidden; disorder is a fatal corpus error",
        },
        "sortKey": ["timestamp", "id"],
        "sortRequirement": "strictly-increasing",
        "equalTimestampPolicy": "retain distinct quotes in ascending id order",
        "duplicatePolicy": {
            "key": ["symbol", "timestamp", "bid", "ask"],
            "retention": "lowest-id observation",
            "priceRoundingBeforeDedupe": False,
        },
        "brokerSession": {
            "anchorDays": "Monday through Friday",
            "timezone": "America/New_York",
            "start": "previous-calendar-day 18:00:00",
            "end": "anchor-calendar-day 17:00:00",
            "interval": "half-open [start,end)",
            "sydneyDisplayTimezone": "Australia/Sydney",
            "dstConversion": "IANA zoneinfo independently at each boundary",
            "fixedSydneyClockPartitionForbidden": True,
            "weekendFragmentReattachmentForbidden": True,
        },
        "completenessAudit": {
            "openToleranceSeconds": 120.0,
            "normalCloseToleranceSeconds": 120.0,
            "fridayCloseToleranceSeconds": 600.0,
            "unexpectedGapSeconds": 300.0,
            "excludeWhen": [
                "open boundary is outside tolerance",
                "close boundary is outside the applicable tolerance",
                "an in-session interquote gap exceeds 300 seconds",
                "the requested batch contains a quote outside the session",
                "the session is empty or partial",
            ],
            "exclusionsFrozenBeforePriceOutcomeInspection": True,
            "missingIds": "record and audit; exclude only when they create a failed time-gap or boundary test",
        },
        "feedGapHandling": {
            "featureSegmentGapMilliseconds": 5_000,
            "crossGapFeatureState": "reset",
            "pendingEntryAcrossGap": "reject",
            "openPositionAcrossGap": "censor-and-halt; profitability invalid",
            "flatRearmMilliseconds": 5_000,
        },
    }


def _feature_specification() -> dict[str, Any]:
    horizons = [0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
    kalman_q = [0.04, 0.16, 0.64]
    kalman_r = [0.01, 0.04, 0.16]
    kalman_bank = [
        {
            "id": f"kalman-q{q:g}-r{r:g}",
            "accelerationVariance": q,
            "measurementVariance": r,
        }
        for q in kalman_q
        for r in kalman_r
    ]
    return {
        "calculationMode": "sequential/prefix-invariant",
        "featureTimestamp": "decision quote timestamp",
        "permittedInformation": "current quote and earlier rows in the same gap-bounded segment",
        "forbidden": [
            "centered windows",
            "future-confirmed extrema or zigzags",
            "back-filled session outcomes",
            "full-sample normalization",
            "future quote arrival information",
        ],
        "rawTimeHorizonsSeconds": horizons,
        "ewmaHalfLivesSeconds": horizons,
        "maximumIntertickGapMilliseconds": 5_000,
        "bollingerWidthStandardDeviations": 2.0,
        "kalmanModelBank": kalman_bank,
        "kalmanInitialization": "first mid in each gap-bounded segment; zero velocity; no earlier segment state",
        "measurementBanks": {
            "quote": ["bid", "ask", "mid", "observed spread"],
            "kinematics": [
                "causal displacement",
                "elapsed-time velocity",
                "acceleration from three causal subintervals",
                "jerk from four causal anchors",
            ],
            "arrivalAndPath": [
                "tick count",
                "arrival rate",
                "directional persistence",
                "path efficiency/straightness",
                "path noise",
            ],
            "bidAskPressure": [
                "bid displacement",
                "ask displacement",
                "translation coherence",
                "translation pressure",
            ],
            "trend": [
                "EWMA price, elapsed-time slope, price separation",
                "adjacent EWMA separation",
                "Kalman price, velocity, innovation, velocity change, uncertainty",
            ],
            "rangeAndVolatility": [
                "causal rolling range and range position",
                "spread mean/std/z-score/ratio/range position",
                "Bollinger mean/std/z-score/position",
            ],
            "calendar": [
                "Tokyo, London, and New York civil-session flags",
                "opening-hour flags",
                "major overlap flags",
            ],
        },
        "civilSessionDefinitions": {
            "Tokyo": {
                "timezone": "Asia/Tokyo",
                "weekdayInterval": "09:00-18:00 half-open",
                "openingHour": "09:00-10:00 half-open",
            },
            "London": {
                "timezone": "Europe/London",
                "weekdayInterval": "08:00-17:00 half-open",
                "openingHour": "08:00-09:00 half-open",
            },
            "NewYork": {
                "timezone": "America/New_York",
                "weekdayInterval": "08:00-17:00 half-open",
                "openingHour": "08:00-09:00 half-open",
            },
            "overlap": "logical intersection at each UTC timestamp after independent IANA conversion",
            "sydneyReporting": "convert every timestamp with IANA Australia/Sydney; never apply a fixed offset",
            "holidayCalendarClaimed": False,
            "treatment": "candidate features only unless unseen performance supports a frozen filter or risk adjustment",
        },
        "allowedCurrentRowDerivations": [
            "side-align by multiplying a signed measurement by +1 for long or -1 for short",
            "difference or finite ratio between already-ready causal horizons",
            "normalize by a current causal spread, standard deviation, range, or uncertainty",
            "lag a causal measurement by at least one observed row",
        ],
        "supportResistanceAndPivotRules": {
            "levelWindowSeconds": horizons,
            "levelMustExcludeCurrentQuote": True,
            "levelSource": "highest/lowest executable quote observed before the decision row in the current segment",
            "pivot": "state transition confirmed by the current row using only a prior causal extreme and current/prior velocities",
            "futureConfirmationForbidden": True,
            "retrospectiveZigzagForbidden": True,
        },
        "readiness": {
            "allFeaturesUsedByARuleMustBeReady": True,
            "noMissingValueImputationFromFuture": True,
            "crossGapWarmupRequired": True,
        },
        "prefixInvarianceAudit": {
            "required": True,
            "method": "features on every tested prefix must equal the corresponding rows from the longer causal run",
            "numericTolerance": {"relative": 1e-10, "absolute": 1e-12},
        },
    }


def _execution_specification() -> dict[str, Any]:
    common = {
        "maximumEntryLagMs": 1_000,
        "maximumExitLagMs": 1_000,
        "maximumIntertickGapMs": 5_000,
        "diagnosticHorizonMs": 60_000,
        "actualFillDeadlineMs": 60_000,
        "cooldownMs": 0,
        "postGapRearmMs": 5_000,
        "quantity": 1.0,
        "pnlClassificationTolerance": 1e-12,
    }
    scenarios = [
        {
            "id": "mechanics-zero-friction",
            "classification": "diagnostic-only; cannot support a profitability claim",
            "entryLatencyMs": 0,
            "exitLatencyMs": 0,
            "entrySlippagePerUnit": 0.0,
            "exitSlippagePerUnit": 0.0,
            "entryCommissionPerUnit": 0.0,
            "exitCommissionPerUnit": 0.0,
            **common,
        },
        {
            "id": "low-friction",
            "classification": "sensitivity",
            "entryLatencyMs": 100,
            "exitLatencyMs": 100,
            "entrySlippagePerUnit": 0.01,
            "exitSlippagePerUnit": 0.01,
            "entryCommissionPerUnit": 0.0,
            "exitCommissionPerUnit": 0.0,
            **common,
        },
        {
            "id": "reference-provisional",
            "classification": "selection and validation reference",
            "entryLatencyMs": 250,
            "exitLatencyMs": 250,
            "entrySlippagePerUnit": 0.02,
            "exitSlippagePerUnit": 0.02,
            "entryCommissionPerUnit": 0.035,
            "exitCommissionPerUnit": 0.035,
            **common,
        },
        {
            "id": "latency-stress",
            "classification": "required robustness stress",
            "entryLatencyMs": 500,
            "exitLatencyMs": 500,
            "entrySlippagePerUnit": 0.02,
            "exitSlippagePerUnit": 0.02,
            "entryCommissionPerUnit": 0.035,
            "exitCommissionPerUnit": 0.035,
            **common,
        },
        {
            "id": "friction-stress",
            "classification": "required robustness stress",
            "entryLatencyMs": 250,
            "exitLatencyMs": 250,
            "entrySlippagePerUnit": 0.05,
            "exitSlippagePerUnit": 0.05,
            "entryCommissionPerUnit": 0.07,
            "exitCommissionPerUnit": 0.07,
            **common,
        },
    ]
    return {
        "priceConvention": {
            "longEntry": "ask",
            "longMarkAndExit": "bid",
            "shortEntry": "bid",
            "shortMarkAndExit": "ask",
            "observedSpreadAlwaysIncluded": True,
        },
        "fillConvention": {
            "decisionRowCanFill": False,
            "eligibleFill": "first strictly later (timestamp,id) quote at or after decision timestamp plus latency",
            "equalTimestampHigherIdIsLater": True,
            "maximumReadyToFillLagMs": 1_000,
            "missingEligibleQuote": "reject entry or censor open position; never invent a fill",
            "gapThroughFill": "use the first actually observed eligible executable quote, never the requested stop price",
        },
        "stopTriggerConvention": {
            "long": "current bid less than or equal to stop",
            "short": "current ask greater than or equal to stop",
            "triggerQuoteIsNotFillQuote": True,
            "fill": "causal later-quote exit using the active execution scenario",
        },
        "breakEvenConvention": {
            "definition": "net executable P&L per unit is at least zero after observed spread, both commissions, and both slippage offsets",
            "stopBeyondEntry": "activation level must cover the frozen costs; realized gap/latency slippage can still produce a loss",
        },
        "scenarioUnit": "XAUUSD quote-price units per one normalized underlying unit",
        "scenarios": scenarios,
        "profitabilityClaimScenario": "reference-provisional",
        "requiredStressScenarioIds": ["latency-stress", "friction-stress"],
        "calibrationLimitation": {
            "status": "broker latency, slippage distribution, and commission schedule were unavailable at preregistration",
            "rule": "documented broker terms may replace provisional costs only before any price outcome evaluation; replacement changes the preregistration hash",
            "liveClaimForbiddenUntilMeasured": True,
        },
    }


def _entry_diagnostics_specification() -> dict[str, Any]:
    return {
        "observationInterval": "half-open [fill,fill+60 seconds)",
        "coverageCheckpointsSeconds": [1, 2, 5, 10, 20, 30, 60],
        "strictSixtySecondLimit": True,
        "firstCoverageDefinition": "first executable mark with net P&L per unit >= 0 after all frozen costs",
        "requiredMetrics": [
            "coverage probability at every checkpoint",
            "restricted time to coverage with uncovered entries assigned 60 seconds",
            "median first-coverage time among covered entries",
            "MAE and MFE in net executable P&L per unit",
            "entry efficiency = max(MFE,0) / (max(MFE,0) + max(-MAE,0)); undefined when the denominator is zero",
            "decision-to-fill and ready-to-fill lag",
            "decision spread and fill spread",
            "censor and rejection reason counts",
        ],
        "netBarrierDistancesPerUnit": [0.1, 0.25, 0.5, 1.0],
        "barrierPairing": "all equal and asymmetric profit/loss pairs; first hit only; quote at deadline excluded",
        "reportSlices": [
            "session anchor/day",
            "side",
            "Tokyo/London/New York/overlap/opening-hour state",
            "causal volatility quintile learned on the training window",
            "causal spread quintile learned on the training window",
            "trend family and maturity bin learned on the training window",
        ],
        "entryEdgeMode": {
            "scheduling": "independent events for diagnosis",
            "profitExitOptimizationForbidden": True,
            "duplicateSignals": "retain for diagnostic clustering report, then separately report causal non-overlapping schedule",
        },
        "metricDenominators": {
            "signalCount": "all frozen feature-ready events emitted before scheduling",
            "fillRate": "filled entries / signal count under the named execution scenario",
            "coverageProbability": "covered filled entries / all filled entries; censored or not-covered fills remain failures",
            "barrierProfitFirstRate": "profit-first filled entries / all filled entries; no-hit, loss-first, and censored fills are not successes",
            "rejections": "reported against all frozen signals by reason and never silently removed",
            "primaryGateScenario": "reference-provisional",
        },
        "deployableSchedule": {
            "mode": "one position or pending order at a time",
            "cooldownBankMilliseconds": [0, 1_000, 3_000, 5_000, 10_000, 30_000, 60_000],
            "cooldownSelection": "candidate parameter learned only in consumed research windows; no inherited three-minute value",
        },
    }


def _candidate_and_search_specification() -> dict[str, Any]:
    return {
        "candidateFamilies": [
            {
                "id": "trend-acceleration",
                "requiredStructure": "established causal trend plus renewed side-aligned velocity/acceleration and coherent bid/ask translation",
            },
            {
                "id": "pullback-resumption",
                "requiredStructure": "established trend, smaller opposite causal movement, then same-trend acceleration resumes",
            },
            {
                "id": "countertrend-pivot",
                "requiredStructure": "causal pullback decelerates and changes state at a prior/current-row pivot without future confirmation",
            },
            {
                "id": "compression-expansion-breakout",
                "requiredStructure": "causal compression followed by range/arrival expansion through a level fixed before the decision quote",
            },
            {
                "id": "quote-translation-pressure",
                "requiredStructure": "coherent executable-side translation, rising arrival pressure, and directional persistence",
            },
        ],
        "allowedRuleComplexity": {
            "maximumAtomicConditions": 6,
            "maximumStateTransitions": 3,
            "maximumInteractionDepth": 2,
            "sessionFilterOptional": True,
            "directionSpecificThresholds": "allowed only if both directions meet sample gates independently in research data",
        },
        "thresholdLearning": {
            "source": "empirical ranks or finite values from the currently permitted training window only",
            "manualOutcomeLabelInspectionForbidden": True,
            "sameWindowRetuningAfterEvaluationForbidden": True,
            "frozenRuleRequiredBeforeNextWindow": True,
            "fullSampleScalingForbidden": True,
        },
        "budgets": {
            "discoveryDistinctCandidates": 240,
            "discoveryPerFamilyMaximum": 60,
            "walkForward1FrozenCandidates": 24,
            "walkForward2FrozenCandidates": 8,
            "exitVariantsAfterEntryGate": 96,
            "walkForward3FullStrategies": 3,
            "validationFullStrategies": 1,
            "holdoutFullStrategies": 1,
        },
        "stageSequence": [
            {
                "role": "discovery",
                "use": "generate and fit entry families; diagnose immediate entry edge only",
            },
            {
                "role": "walk_forward_1",
                "use": "unseen entry-edge screen; outcomes become research data after scoring",
            },
            {
                "role": "walk_forward_2",
                "use": "unseen confirmation of revised frozen entries; freeze promoted entries",
            },
            {
                "role": "consumed discovery through walk_forward_2",
                "use": "search exits only for entries that passed the entry gates",
            },
            {
                "role": "walk_forward_3",
                "use": "unseen full-strategy test and predeclared selection of at most one unchanged strategy",
            },
            {
                "role": "validation",
                "use": "unseen test of exactly one strategy; no change is permitted afterward",
            },
            {
                "role": "holdout",
                "use": "one authorized evaluation of the exact validation strategy",
            },
        ],
        "failurePolicy": "a failed frozen candidate may inform a different candidate only in a later still-unused window; no failed strategy proceeds by pooled-window rescue",
    }


def _exit_specification() -> dict[str, Any]:
    return {
        "entryMustPassBeforeExitSearch": True,
        "entryDefinitionFrozenDuringExitSearch": True,
        "permittedInitialStops": [
            "fixed executable price distance",
            "multiple of current causal spread",
            "multiple of current causal volatility/noise",
            "prior causal structure level excluding current quote",
        ],
        "permittedInvalidation": [
            "time stop when cost coverage or expected acceleration does not arrive",
            "side-aligned velocity/acceleration reversal",
            "causal structure invalidation",
        ],
        "permittedProfitManagement": [
            "true executable break-even then beyond-cost stop",
            "fixed-distance trailing stop",
            "causal-volatility trailing stop",
            "prior causal structure trailing stop",
            "acceleration/deceleration trailing exit",
        ],
        "partialProfitTaking": "not registered in v2 because the audited replay supports full-position exits only",
        "strictActualFillDeadlineMilliseconds": 60_000,
        "candidateMaximumHoldingUpperBoundMilliseconds": 58_000,
        "deadlineCompatibilityRule": "maximum holding + exit latency + maximum exit lag must be <= 60000 milliseconds",
        "allExitDecisionsUseLaterQuoteFills": True,
        "selectionData": "only discovery, walk_forward_1, and walk_forward_2 after their entry outcomes have been consumed",
        "walkForward3OutcomeCannotTuneExit": True,
    }


def _robustness_and_gates_specification() -> dict[str, Any]:
    return {
        "minimumSample": {
            "filledTradesPerSession": 3,
            "absoluteFilledTrades": 30,
            "activeSessionFraction": 0.60,
            "rule": "require max(absoluteFilledTrades, filledTradesPerSession * evaluated session count)",
        },
        "entryPromotionGates": {
            "scenario": "reference-provisional",
            "fillRateMinimum": 0.95,
            "costCoverage10Seconds": 0.50,
            "costCoverage30Seconds": 0.60,
            "costCoverage60Seconds": 0.65,
            "restrictedMedianCoverageMillisecondsMaximum": 30_000,
            "censoredFractionMaximum": 0.02,
            "equalBarrierDistancePerUnit": 0.25,
            "equalBarrierProfitFirstRateMinimum": 0.52,
            "requiredIndependentlyOn": ["walk_forward_1", "walk_forward_2"],
        },
        "fullStrategyGates": {
            "referenceNetPnlStrictlyPositive": True,
            "referenceExpectancyStrictlyPositive": True,
            "referenceProfitFactorMinimum": 1.10,
            "positiveSessionFractionMinimum": 0.50,
            "maximumDrawdownToGrossProfitMaximum": 0.65,
            "largestTradeShareOfGrossProfitMaximum": 0.25,
            "largestSessionShareOfGrossProfitMaximum": 0.40,
            "loss95ToMedianAbsoluteLossMaximum": 3.0,
            "requiredStressNetPnlStrictlyPositive": True,
            "requiredStressProfitFactorMinimum": 1.0,
            "fullReplayCensorCountMaximum": 0,
            "profitabilityValidRequired": True,
            "entryPromotionGatesStillRequired": True,
            "requiredIndependentlyOn": ["walk_forward_3", "validation", "holdout"],
        },
        "balancedScore": {
            "range": [-1.0, 1.0],
            "definitions": {
                "clip": "clip(x,a,b)=min(max(x,a),b)",
                "expectancyScaledByMedianAbsoluteTradePnl": "clip(expectancy / median(abs(trade net P&L)), -1, 1); invalid when denominator is zero",
                "coverageProbabilityAndSpeed": "0.75 * sum(checkpoint weight * (2 * coverage probability - 1)) + 0.25 * (1 - 2 * restricted median coverage milliseconds / 60000)",
                "profitFactorCappedAtTwo": "clip(profit factor - 1, -1, 1)",
                "inverseDrawdownToGrossProfit": "1 - 2 * clip(maximum drawdown / gross profit, 0, 1); invalid when gross profit is zero",
                "positiveSessionFraction": "2 * positive session fraction - 1",
                "requiredStressPassFraction": "2 * fraction of required stress scenarios with positive expectancy and profit factor >= 1 - 1",
                "inverseLargestSessionProfitConcentration": "1 - 2 * clip(largest positive session contribution / gross profit, 0, 1)",
                "tradeCountAdequacy": "2 * min(1, filled trade count / minimum required filled trade count) - 1",
            },
            "weights": {
                "expectancyScaledByMedianAbsoluteTradePnl": 0.25,
                "coverageProbabilityAndSpeed": 0.20,
                "profitFactorCappedAtTwo": 0.15,
                "inverseDrawdownToGrossProfit": 0.12,
                "positiveSessionFraction": 0.10,
                "requiredStressPassFraction": 0.08,
                "inverseLargestSessionProfitConcentration": 0.05,
                "tradeCountAdequacy": 0.05,
            },
            "coverageSubscore": {
                "probabilityWeights": {
                    "2s": 0.15,
                    "5s": 0.25,
                    "10s": 0.25,
                    "30s": 0.20,
                    "60s": 0.15,
                },
                "probabilityShare": 0.75,
                "restrictedMedianSpeedShare": 0.25,
            },
            "hardGatesOverrideScore": True,
            "tieBreakOrder": [
                "higher minimum per-window expectancy",
                "higher minimum stress expectancy",
                "lower maximum drawdown",
                "simpler rule",
                "lexicographically smaller candidate id",
            ],
        },
        "parameterNeighborhood": {
            "continuousThresholdRankPerturbation": [-0.05, 0.05],
            "rankBoundaryRule": "add each perturbation, clip to [0,1], then deduplicate",
            "timeAndKalmanParameters": "each immediately adjacent registered bank value",
            "structuralAblation": "remove each optional condition one at a time",
            "minimumValidNeighborFraction": 0.70,
            "minimumPositiveExpectancyNeighborFraction": 0.70,
            "medianNeighborExpectancyRetention": 0.75,
            "maximumAbsoluteCoverage30SecondDrop": 0.07,
            "evaluatedOnlyOnConsumedResearchWindows": True,
            "validationOrHoldoutNeighborhoodTuningForbidden": True,
        },
        "stability": {
            "chronologicalWindowsReportedSeparately": True,
            "directionsSessionsDaysAndRegimesReportedSeparately": True,
            "pooledProfitCannotRescueAFailedRequiredWindow": True,
            "outlierConcentrationGatesRequired": True,
        },
    }


def _holdout_specification() -> dict[str, Any]:
    return {
        "maximumCandidates": 1,
        "explicitAuthorizationRequired": True,
        "prerequisites": [
            "one full strategy passed every walk_forward_3 gate",
            "the identical strategy passed every validation gate",
            "entry, exit, risk, feature, session, and execution hashes are frozen",
            "no holdout outcome has previously been requested or revealed",
        ],
        "singleEvaluation": True,
        "repeatAfterFailureForbidden": True,
        "parameterOrCodeChangeAfterAuthorization": "new research study; current holdout remains consumed",
        "failureReport": "No robust profitable setup has yet survived unseen validation.",
    }


def _preregistration_body(source_bindings: Mapping[str, Any]) -> dict[str, Any]:
    """Return the one canonical v2 body around validated source bindings."""

    return {
        "schema": PREREGISTRATION_SCHEMA,
        "studyId": "xauusd-fresh-causal-acceleration-v2",
        "outcomeBlindDeclaration": {
            "strategyThresholdsFitted": False,
            "tickPriceValuesRequiredByBuilder": False,
            "holdoutOutcomeAccessed": False,
            "allNumericSignalThresholdsMustBeLearnedLaterFromPermittedResearchWindows": True,
        },
        "sourceBindings": dict(source_bindings),
        "chronologicalWindowPolicy": {
            **asdict(FRESH_V2_WINDOW_POLICY),
            "totalEligibleSessions": FRESH_V2_WINDOW_POLICY.required_sessions,
            "roles": list(_ROLE_ORDER),
            "randomTickShuffleForbidden": True,
        },
        "sessionAndData": _session_and_data_specification(),
        "features": _feature_specification(),
        "execution": _execution_specification(),
        "entryDiagnostics": _entry_diagnostics_specification(),
        "candidateSearch": _candidate_and_search_specification(),
        "exitResearch": _exit_specification(),
        "robustnessAndGates": _robustness_and_gates_specification(),
        "holdout": _holdout_specification(),
        "auditRequirements": {
            "appendOnlyExperimentLedger": True,
            "frozenCandidateHashBeforeEveryUnseenWindow": True,
            "prefixInvarianceAndBidAskExecutionTestsRequired": True,
            "rejectionCensorAndLeakageReasonsRequired": True,
            "finiteMetricCheckRequired": True,
            "finalReportSeparatesDevelopmentWalkForwardValidationHoldout": True,
            "implementationManifest": "sorted repository-relative paths and SHA-256 of every feature, signal, replay, exit, scoring, and configuration file; its canonical SHA-256 is source-bound",
            "humanCodeIdentifierIsNotAContentHash": True,
        },
    }


def build_fresh_preregistration_v2(
    *,
    split_manifest: Mapping[str, Any],
    corpus_manifest_sha256: str,
    protocol_code_identifier: str,
    implementation_manifest: Mapping[str, Any],
    experiment_ledger_path: str | Path,
    holdout_authorization_registry_path: str | Path,
) -> dict[str, Any]:
    """Build and hash the deterministic preregistration without reading prices."""

    split_sha = _validate_split(split_manifest)
    corpus_sha = _sha256(corpus_manifest_sha256, "corpus_manifest_sha256")
    implementation_sha = validate_fresh_implementation_manifest(
        implementation_manifest,
        verify_current_files=True,
    )
    if not isinstance(protocol_code_identifier, str) or not protocol_code_identifier.strip():
        raise ValueError("protocol_code_identifier must be non-empty")
    inventory_sha = _sha256(
        str(split_manifest.get("inventorySha256", "")), "inventorySha256"
    )
    code_id = protocol_code_identifier.strip()
    ledger_path = _registered_file_path(
        experiment_ledger_path, "experiment_ledger_path"
    )
    authorization_path = _registered_file_path(
        holdout_authorization_registry_path,
        "holdout_authorization_registry_path",
    )
    if ledger_path == authorization_path:
        raise ValueError("experiment ledger and holdout registry must be different files")
    body = _preregistration_body(
        {
            "splitManifestSha256": split_sha,
            "inventorySha256": inventory_sha,
            "corpusManifestSha256": corpus_sha,
            "protocolCodeIdentifier": code_id,
            "protocolCodeIdentifierSha256": hashlib.sha256(code_id.encode("utf-8")).hexdigest(),
            "implementationManifestSha256": implementation_sha,
            "implementationManifest": copy.deepcopy(dict(implementation_manifest)),
            "experimentLedgerPath": ledger_path,
            "holdoutAuthorizationRegistryPath": authorization_path,
        }
    )
    return {**body, "preregistrationSha256": canonical_hash(body)}


def validate_fresh_preregistration_v2(preregistration: Mapping[str, Any]) -> str:
    """Return the registered digest or reject a modified/non-v2 document."""

    if not isinstance(preregistration, Mapping):
        raise ValueError("preregistration must be a mapping")
    body = copy.deepcopy(dict(preregistration))
    claimed = _sha256(
        str(body.pop("preregistrationSha256", "")), "preregistrationSha256"
    )
    if body.get("schema") != PREREGISTRATION_SCHEMA:
        raise ValueError("unsupported preregistration schema")
    if canonical_hash(body) != claimed:
        raise ValueError("preregistration hash does not match its contents")
    source = body.get("sourceBindings")
    expected_source_keys = {
        "splitManifestSha256",
        "inventorySha256",
        "corpusManifestSha256",
        "protocolCodeIdentifier",
        "protocolCodeIdentifierSha256",
        "implementationManifestSha256",
        "implementationManifest",
        "experimentLedgerPath",
        "holdoutAuthorizationRegistryPath",
    }
    if not isinstance(source, Mapping) or set(source) != expected_source_keys:
        raise ValueError("preregistration source bindings are incomplete")
    normalized_source = {
        "splitManifestSha256": _sha256(
            str(source["splitManifestSha256"]), "splitManifestSha256"
        ),
        "inventorySha256": _sha256(
            str(source["inventorySha256"]), "inventorySha256"
        ),
        "corpusManifestSha256": _sha256(
            str(source["corpusManifestSha256"]), "corpusManifestSha256"
        ),
        "protocolCodeIdentifier": str(source["protocolCodeIdentifier"]),
        "protocolCodeIdentifierSha256": _sha256(
            str(source["protocolCodeIdentifierSha256"]),
            "protocolCodeIdentifierSha256",
        ),
        "implementationManifestSha256": _sha256(
            str(source["implementationManifestSha256"]),
            "implementationManifestSha256",
        ),
        "implementationManifest": copy.deepcopy(source["implementationManifest"]),
        "experimentLedgerPath": _registered_file_path(
            source["experimentLedgerPath"], "experimentLedgerPath"
        ),
        "holdoutAuthorizationRegistryPath": _registered_file_path(
            source["holdoutAuthorizationRegistryPath"],
            "holdoutAuthorizationRegistryPath",
        ),
    }
    code_id = normalized_source["protocolCodeIdentifier"]
    if not code_id or code_id.strip() != code_id:
        raise ValueError("protocolCodeIdentifier must be non-empty and trimmed")
    if hashlib.sha256(code_id.encode("utf-8")).hexdigest() != normalized_source[
        "protocolCodeIdentifierSha256"
    ]:
        raise ValueError("protocol code identifier hash does not match")
    if (
        normalized_source["experimentLedgerPath"]
        == normalized_source["holdoutAuthorizationRegistryPath"]
    ):
        raise ValueError("experiment ledger and holdout registry must differ")
    actual_implementation_sha = validate_fresh_implementation_manifest(
        normalized_source["implementationManifest"],
        verify_current_files=True,
    )
    if actual_implementation_sha != normalized_source["implementationManifestSha256"]:
        raise ValueError("implementation manifest binding does not match")
    if canonical_hash(body) != canonical_hash(_preregistration_body(normalized_source)):
        raise ValueError("document differs from the canonical v2 preregistration")
    weights = body["robustnessAndGates"]["balancedScore"]["weights"]
    if not math.isclose(sum(float(value) for value in weights.values()), 1.0):
        raise ValueError("balanced score weights must sum to one")
    return claimed


def session_audit_config_from_preregistration(
    preregistration: Mapping[str, Any],
) -> SessionAuditConfig:
    """Materialize every session-QC field explicitly; use no class defaults."""

    validate_fresh_preregistration_v2(preregistration)
    spec = preregistration["sessionAndData"]["completenessAudit"]
    return SessionAuditConfig(
        open_tolerance_seconds=float(spec["openToleranceSeconds"]),
        close_tolerance_seconds=float(spec["normalCloseToleranceSeconds"]),
        friday_close_tolerance_seconds=float(spec["fridayCloseToleranceSeconds"]),
        unexpected_gap_seconds=float(spec["unexpectedGapSeconds"]),
    )


def feature_configs_from_preregistration(
    preregistration: Mapping[str, Any],
) -> tuple[FreshFeatureConfig, ...]:
    """Materialize the registered measurement bank with all fields explicit."""

    validate_fresh_preregistration_v2(preregistration)
    spec = preregistration["features"]
    return tuple(
        FreshFeatureConfig(
            horizons_seconds=tuple(float(value) for value in spec["rawTimeHorizonsSeconds"]),
            maximum_intertick_gap_ms=int(spec["maximumIntertickGapMilliseconds"]),
            ewma_half_lives_seconds=tuple(
                float(value) for value in spec["ewmaHalfLivesSeconds"]
            ),
            kalman_acceleration_variance=float(model["accelerationVariance"]),
            kalman_measurement_variance=float(model["measurementVariance"]),
            bollinger_width=float(spec["bollingerWidthStandardDeviations"]),
        )
        for model in spec["kalmanModelBank"]
    )


def entry_diagnostic_configs_from_preregistration(
    preregistration: Mapping[str, Any],
) -> dict[str, EntryDiagnosticConfig]:
    """Materialize registered execution scenarios with no inherited values."""

    validate_fresh_preregistration_v2(preregistration)
    configs: dict[str, EntryDiagnosticConfig] = {}
    for scenario in preregistration["execution"]["scenarios"]:
        identifier = str(scenario["id"])
        if identifier in configs:
            raise ValueError("execution scenario ids must be unique")
        configs[identifier] = EntryDiagnosticConfig(
            entry_latency_ms=int(scenario["entryLatencyMs"]),
            maximum_entry_lag_ms=int(scenario["maximumEntryLagMs"]),
            maximum_intertick_gap_ms=int(scenario["maximumIntertickGapMs"]),
            diagnostic_horizon_ms=int(scenario["diagnosticHorizonMs"]),
            quantity=float(scenario["quantity"]),
            entry_slippage_per_unit=float(scenario["entrySlippagePerUnit"]),
            exit_slippage_per_unit=float(scenario["exitSlippagePerUnit"]),
            entry_commission_per_unit=float(scenario["entryCommissionPerUnit"]),
            exit_commission_per_unit=float(scenario["exitCommissionPerUnit"]),
            profit_barrier_net_per_unit=None,
            loss_barrier_net_per_unit=None,
        )
    return configs


def replay_execution_configs_from_preregistration(
    preregistration: Mapping[str, Any],
) -> dict[str, FreshExecutionConfig]:
    """Materialize full-replay execution scenarios with all fields explicit."""

    validate_fresh_preregistration_v2(preregistration)
    configs: dict[str, FreshExecutionConfig] = {}
    for scenario in preregistration["execution"]["scenarios"]:
        identifier = str(scenario["id"])
        if identifier in configs:
            raise ValueError("execution scenario ids must be unique")
        entry_slippage = float(scenario["entrySlippagePerUnit"])
        exit_slippage = float(scenario["exitSlippagePerUnit"])
        entry_commission = float(scenario["entryCommissionPerUnit"])
        exit_commission = float(scenario["exitCommissionPerUnit"])
        if entry_slippage != exit_slippage or entry_commission != exit_commission:
            raise ValueError("fresh replay requires symmetric per-side costs")
        configs[identifier] = FreshExecutionConfig(
            entry_latency_ms=int(scenario["entryLatencyMs"]),
            exit_latency_ms=int(scenario["exitLatencyMs"]),
            maximum_entry_lag_ms=int(scenario["maximumEntryLagMs"]),
            maximum_exit_lag_ms=int(scenario["maximumExitLagMs"]),
            maximum_intertick_gap_ms=int(scenario["maximumIntertickGapMs"]),
            actual_fill_deadline_ms=int(scenario["actualFillDeadlineMs"]),
            cooldown_ms=int(scenario["cooldownMs"]),
            post_gap_rearm_ms=int(scenario["postGapRearmMs"]),
            quantity=float(scenario["quantity"]),
            slippage_per_side=entry_slippage,
            commission_per_unit_per_side=entry_commission,
            pnl_classification_tolerance=float(
                scenario["pnlClassificationTolerance"]
            ),
        )
    return configs


def entry_barrier_diagnostic_configs_from_preregistration(
    preregistration: Mapping[str, Any],
    *,
    scenario_id: str,
) -> dict[str, EntryDiagnosticConfig]:
    """Materialize all 16 registered profit/loss barrier pairs for one scenario."""

    base_configs = entry_diagnostic_configs_from_preregistration(preregistration)
    if scenario_id not in base_configs:
        raise ValueError(f"unknown execution scenario: {scenario_id!r}")
    base = base_configs[scenario_id]
    distances = tuple(
        float(value)
        for value in preregistration["entryDiagnostics"][
            "netBarrierDistancesPerUnit"
        ]
    )
    configs: dict[str, EntryDiagnosticConfig] = {}
    for profit in distances:
        for loss in distances:
            identifier = f"{scenario_id}:profit-{profit:g}:loss-{loss:g}"
            configs[identifier] = EntryDiagnosticConfig(
                entry_latency_ms=base.entry_latency_ms,
                maximum_entry_lag_ms=base.maximum_entry_lag_ms,
                maximum_intertick_gap_ms=base.maximum_intertick_gap_ms,
                diagnostic_horizon_ms=base.diagnostic_horizon_ms,
                quantity=base.quantity,
                entry_slippage_per_unit=base.entry_slippage_per_unit,
                exit_slippage_per_unit=base.exit_slippage_per_unit,
                entry_commission_per_unit=base.entry_commission_per_unit,
                exit_commission_per_unit=base.exit_commission_per_unit,
                profit_barrier_net_per_unit=profit,
                loss_barrier_net_per_unit=loss,
            )
    return configs


def replay_execution_config_for_candidate(
    preregistration: Mapping[str, Any],
    *,
    scenario_id: str,
    cooldown_ms: int,
) -> FreshExecutionConfig:
    """Return one registered scenario with a registered deployable cooldown."""

    configs = replay_execution_configs_from_preregistration(preregistration)
    if scenario_id not in configs:
        raise ValueError(f"unknown execution scenario: {scenario_id!r}")
    registered = preregistration["entryDiagnostics"]["deployableSchedule"][
        "cooldownBankMilliseconds"
    ]
    if (
        not isinstance(cooldown_ms, int)
        or isinstance(cooldown_ms, bool)
        or cooldown_ms not in registered
    ):
        raise ValueError("cooldown_ms is not in the registered candidate bank")
    base = configs[scenario_id]
    return FreshExecutionConfig(
        entry_latency_ms=base.entry_latency_ms,
        exit_latency_ms=base.exit_latency_ms,
        maximum_entry_lag_ms=base.maximum_entry_lag_ms,
        maximum_exit_lag_ms=base.maximum_exit_lag_ms,
        maximum_intertick_gap_ms=base.maximum_intertick_gap_ms,
        actual_fill_deadline_ms=base.actual_fill_deadline_ms,
        cooldown_ms=cooldown_ms,
        post_gap_rearm_ms=base.post_gap_rearm_ms,
        quantity=base.quantity,
        slippage_per_side=base.slippage_per_side,
        commission_per_unit_per_side=base.commission_per_unit_per_side,
        pnl_classification_tolerance=base.pnl_classification_tolerance,
    )


def _load_verified_ledger(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path).expanduser().resolve()
    if source.is_symlink() or not source.is_file():
        raise PermissionError("the preregistered experiment ledger is unavailable")
    records: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise PermissionError("experiment ledger contains a blank record")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PermissionError("experiment ledger is not valid JSONL") from exc
            if not isinstance(record, dict):
                raise PermissionError("experiment ledger records must be objects")
            claimed_number = record.pop("recordNumber", None)
            claimed_sha = record.pop("recordSha256", None)
            if claimed_number != line_number:
                raise PermissionError("experiment ledger numbering is not contiguous")
            if (
                not isinstance(claimed_sha, str)
                or _SHA256.fullmatch(claimed_sha.lower()) is None
                or canonical_hash(record) != claimed_sha.lower()
            ):
                raise PermissionError("experiment ledger record hash is invalid")
            records.append(
                {
                    "recordNumber": claimed_number,
                    "recordSha256": claimed_sha.lower(),
                    **record,
                }
            )
    return records


def _passed_same_strategy_record(
    record: Mapping[str, Any],
    *,
    role: str,
    strategy_sha: str,
    prereg_sha: str,
) -> bool:
    return all(
        record.get(key) == value
        for key, value in {
            "role": role,
            "outcomesRevealed": True,
            "gatePassed": True,
            "frozenStrategySha256": strategy_sha,
            "preregistrationSha256": prereg_sha,
        }.items()
    )


def authorize_registered_holdout(
    *,
    preregistration: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    frozen_strategy_sha256: str,
    walk_forward_3_record_number: int,
    validation_record_number: int,
    explicit_holdout_authorization: bool = False,
) -> dict[str, Any]:
    """Atomically reserve the sole holdout after verified identical validation.

    The registry file is created with ``O_EXCL``.  It is intentionally never
    overwritten: even a crash after reservation conservatively consumes the
    authorization and requires human audit rather than silently trying again.
    """

    prereg_sha = validate_fresh_preregistration_v2(preregistration)
    strategy_sha = _sha256(frozen_strategy_sha256, "frozen_strategy_sha256")
    if _validate_split(split_manifest) != preregistration["sourceBindings"][
        "splitManifestSha256"
    ]:
        raise PermissionError("split manifest differs from the preregistered split")
    if any(
        not isinstance(number, int) or isinstance(number, bool) or number <= 0
        for number in (walk_forward_3_record_number, validation_record_number)
    ):
        raise PermissionError("walk-forward and validation record numbers are required")
    records = _load_verified_ledger(
        preregistration["sourceBindings"]["experimentLedgerPath"]
    )
    outcome_records = [
        record for record in records if record.get("outcomesRevealed") is True
    ]
    research_role_order = {role: index for index, role in enumerate(_ROLE_ORDER[:-1])}
    if any(record.get("role") not in research_role_order for record in outcome_records):
        raise PermissionError("outcome-revealing ledger records contain an unknown role")
    outcome_role_indexes = [
        research_role_order[str(record["role"])] for record in outcome_records
    ]
    if outcome_role_indexes != sorted(outcome_role_indexes):
        raise PermissionError("outcome-revealing ledger roles are not chronological")
    validation_records = [
        record
        for record in outcome_records
        if record.get("role") == "validation"
    ]
    if len(validation_records) != 1:
        raise PermissionError("the ledger must contain exactly one validation outcome")
    walk_forward_3_records = [
        record
        for record in outcome_records
        if record.get("role") == "walk_forward_3"
    ]
    if not walk_forward_3_records or len(walk_forward_3_records) > 3:
        raise PermissionError("walk_forward_3 outcome count violates the registered budget")
    if validation_records[0]["recordNumber"] != validation_record_number:
        raise PermissionError("the nominated record is not the sole validation outcome")
    if validation_record_number != len(records):
        raise PermissionError("validation must be the final experiment-ledger record")
    by_number = {record["recordNumber"]: record for record in records}
    walk_forward_3_evidence = by_number.get(walk_forward_3_record_number)
    validation_evidence = by_number.get(validation_record_number)
    if (
        walk_forward_3_evidence is None
        or not _passed_same_strategy_record(
            walk_forward_3_evidence,
            role="walk_forward_3",
            strategy_sha=strategy_sha,
            prereg_sha=prereg_sha,
        )
    ):
        raise PermissionError(
            "holdout requires a passed walk_forward_3 record for the identical strategy"
        )
    if (
        validation_evidence is None
        or not _passed_same_strategy_record(
            validation_evidence,
            role="validation",
            strategy_sha=strategy_sha,
            prereg_sha=prereg_sha,
        )
    ):
        raise PermissionError(
            "holdout requires a passed validation record for the identical strategy"
        )
    if walk_forward_3_record_number >= validation_record_number:
        raise PermissionError("walk_forward_3 evidence must precede validation evidence")
    base = authorize_evaluation(
        "holdout",
        split_manifest=split_manifest,
        access_records=records,
        frozen_strategy_sha256=strategy_sha,
        explicit_holdout_authorization=explicit_holdout_authorization,
    )
    payload = {
        key: value for key, value in base.items() if key != "authorizationSha256"
    }
    payload["preregistrationSha256"] = prereg_sha
    payload["walkForward3EvidenceSha256"] = canonical_hash(
        walk_forward_3_evidence
    )
    payload["validationEvidenceSha256"] = canonical_hash(validation_evidence)
    authorization = {**payload, "authorizationSha256": canonical_hash(payload)}

    destination = Path(
        preregistration["sourceBindings"]["holdoutAuthorizationRegistryPath"]
    ).expanduser().resolve()
    if destination.exists() or destination.is_symlink():
        raise PermissionError("holdout authorization has already been reserved")
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            authorization,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
    )
    try:
        descriptor = os.open(destination, flags, 0o600)
    except FileExistsError as exc:
        raise PermissionError(
            "holdout authorization has already been reserved"
        ) from exc
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return authorization


__all__ = [
    "FRESH_V2_WINDOW_POLICY",
    "IMPLEMENTATION_MANIFEST_SCHEMA",
    "PREREGISTRATION_SCHEMA",
    "authorize_registered_holdout",
    "build_fresh_preregistration_v2",
    "build_fresh_implementation_manifest",
    "entry_barrier_diagnostic_configs_from_preregistration",
    "entry_diagnostic_configs_from_preregistration",
    "feature_configs_from_preregistration",
    "required_fresh_implementation_files",
    "replay_execution_configs_from_preregistration",
    "replay_execution_config_for_candidate",
    "session_audit_config_from_preregistration",
    "validate_fresh_preregistration_v2",
    "validate_fresh_implementation_manifest",
]
