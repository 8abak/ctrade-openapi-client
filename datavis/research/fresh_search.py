"""Chronological, data-source-agnostic orchestration for fresh XAUUSD research.

The engine in this module owns *research protocol state*, not market data.  It
never opens a database, reads a tick file, computes a signal, or chooses a
trading threshold.  Those operations are supplied as callbacks.  Its job is to
make otherwise easy research mistakes mechanically difficult:

* outcome windows are consumed exactly once and strictly chronologically;
* entry and full-strategy definitions are canonicalised before evaluation;
* only candidates that pass the current gate can move forward;
* an exit search cannot silently change the frozen entry definition;
* validation evaluates exactly one strategy; and
* the holdout can be authorised and attempted once, for that exact strategy.

The append-only experiment ledger is written through
``fresh_protocol.append_fresh_record`` so the resulting records can also be
used by the stronger preregistered holdout authorisation workflow.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from datavis.research.fresh_protocol import (
    append_fresh_record,
    authorize_evaluation,
    canonical_hash,
)

_ROLE_ORDER = (
    "discovery",
    "walk_forward_1",
    "walk_forward_2",
    "walk_forward_3",
    "validation",
    "holdout",
)
_SHA256 = re.compile(r"[0-9a-f]{64}")


class FreshSearchStage(str, Enum):
    """One-way lifecycle of a single, fresh chronological study."""

    NEW = "new"
    DISCOVERY_RESUME_AUTHORIZED = "discovery_resume_authorized"
    DISCOVERY_COMPLETE = "discovery_complete"
    WALK_FORWARD_1_COMPLETE = "walk_forward_1_complete"
    WALK_FORWARD_2_COMPLETE = "walk_forward_2_complete"
    EXIT_SEARCH_COMPLETE = "exit_search_complete"
    WALK_FORWARD_3_COMPLETE = "walk_forward_3_complete"
    VALIDATION_COMPLETE = "validation_complete"
    HOLDOUT_AUTHORIZED = "holdout_authorized"
    HOLDOUT_COMPLETE = "holdout_complete"
    FAILED = "failed"


class FreshSearchProtocolError(RuntimeError):
    """Raised when an operation would violate chronological research state."""


class FrozenIdentityError(FreshSearchProtocolError):
    """Raised when a callback reports results for a different definition."""


def _canonical_json(value: Any, name: str) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite and JSON-compatible") from exc


def _json_clone(value: Any, name: str) -> Any:
    return json.loads(_canonical_json(value, name))


def _non_empty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{name} must be a non-empty, trimmed string")
    return value


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value.lower()) is None:
        raise ValueError(f"{name} must be a SHA-256 hex digest")
    return value.lower()


def _load_verified_records(path: Path) -> list[dict[str, Any]]:
    """Load numbered ledger records while verifying every canonical digest."""

    if path.is_symlink() or not path.is_file():
        raise PermissionError("the recovery ledger is unavailable")
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise PermissionError("the recovery ledger contains a blank record")
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PermissionError("the recovery ledger is not valid JSONL") from exc
            if not isinstance(raw, dict):
                raise PermissionError("recovery ledger records must be objects")
            record = dict(raw)
            claimed_number = record.pop("recordNumber", None)
            claimed_sha = record.pop("recordSha256", None)
            if claimed_number != line_number:
                raise PermissionError("recovery ledger numbering is not contiguous")
            if (
                not isinstance(claimed_sha, str)
                or _SHA256.fullmatch(claimed_sha.lower()) is None
                or canonical_hash(record) != claimed_sha.lower()
            ):
                raise PermissionError("a recovery ledger record hash is invalid")
            records.append(
                {
                    "recordNumber": claimed_number,
                    "recordSha256": claimed_sha.lower(),
                    **record,
                }
            )
    return records


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _evaluation_payload(value: "CandidateEvaluation") -> dict[str, Any]:
    return {
        "identitySha256": value.identity_sha256,
        "passed": value.passed,
        "metrics": _json_clone(value.metrics, "recovery evaluation metrics"),
        "leakageChecks": _json_clone(
            value.leakage_checks, "recovery evaluation leakage checks"
        ),
        "score": value.score,
    }


@dataclass(frozen=True, slots=True)
class FreshSearchBudgets:
    """Frozen maximum candidate counts at every selection boundary."""

    discovery_distinct_candidates: int
    discovery_per_family_maximum: int
    walk_forward_1_frozen_candidates: int
    walk_forward_2_frozen_candidates: int
    exit_variants_after_entry_gate: int
    walk_forward_3_full_strategies: int
    validation_full_strategies: int
    holdout_full_strategies: int
    exit_search_frozen_entries: int = 1

    def __post_init__(self) -> None:
        values = (
            tuple(self.__dict__.values())
            if hasattr(self, "__dict__")
            else (
                self.discovery_distinct_candidates,
                self.discovery_per_family_maximum,
                self.walk_forward_1_frozen_candidates,
                self.walk_forward_2_frozen_candidates,
                self.exit_variants_after_entry_gate,
                self.walk_forward_3_full_strategies,
                self.validation_full_strategies,
                self.holdout_full_strategies,
                self.exit_search_frozen_entries,
            )
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in values
        ):
            raise ValueError("all research budgets must be positive integers")
        if self.walk_forward_1_frozen_candidates > self.discovery_distinct_candidates:
            raise ValueError("walk-forward 1 budget cannot exceed discovery budget")
        if (
            self.walk_forward_2_frozen_candidates
            > self.walk_forward_1_frozen_candidates
        ):
            raise ValueError(
                "walk-forward 2 budget cannot exceed walk-forward 1 budget"
            )
        if self.exit_search_frozen_entries > self.walk_forward_2_frozen_candidates:
            raise ValueError(
                "exit-search frozen-entry budget cannot exceed walk-forward 2 budget"
            )
        if self.exit_search_frozen_entries != 1:
            raise ValueError(
                "the current protocol requires exactly one frozen entry for exit search"
            )
        if self.walk_forward_3_full_strategies > self.exit_variants_after_entry_gate:
            raise ValueError("walk-forward 3 budget cannot exceed exit-search budget")
        if self.validation_full_strategies != 1:
            raise ValueError("validation must evaluate exactly one full strategy")
        if self.holdout_full_strategies != 1:
            raise ValueError("holdout must evaluate exactly one full strategy")


@dataclass(frozen=True, slots=True)
class FrozenResearchWindow:
    """Strategy-neutral view of one exact window in the split manifest."""

    role: str
    session_anchors: tuple[str, ...]
    window_sha256: str

    def __post_init__(self) -> None:
        if self.role not in _ROLE_ORDER:
            raise ValueError(f"unknown research role {self.role!r}")
        if not self.session_anchors:
            raise ValueError("a research window must contain at least one session")
        _sha256(self.window_sha256, "window_sha256")


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """Exact training/evaluation roles made visible to a callback."""

    stage: str
    training_roles: tuple[str, ...]
    evaluation_roles: tuple[str, ...]
    windows: tuple[FrozenResearchWindow, ...]

    def __post_init__(self) -> None:
        _non_empty(self.stage, "stage")
        if not self.training_roles or not self.evaluation_roles or not self.windows:
            raise ValueError("evaluation context roles and windows cannot be empty")
        if any(role not in _ROLE_ORDER for role in self.training_roles):
            raise ValueError("training_roles contains an unknown role")
        if any(role not in _ROLE_ORDER for role in self.evaluation_roles):
            raise ValueError("evaluation_roles contains an unknown role")


@dataclass(frozen=True, slots=True)
class EntryCandidateSpec:
    """Outcome-blind entry definition returned by the discovery callback."""

    candidate_id: str
    family: str
    config: Mapping[str, Any]
    entry_variant: str

    def __post_init__(self) -> None:
        _non_empty(self.candidate_id, "candidate_id")
        _non_empty(self.family, "family")
        _non_empty(self.entry_variant, "entry_variant")
        if not isinstance(self.config, Mapping):
            raise ValueError("entry candidate config must be a mapping")
        _canonical_json(self.config, "entry candidate config")


@dataclass(frozen=True, slots=True)
class StrategyCandidateSpec:
    """Exit and execution definition bound to one promoted frozen entry."""

    strategy_id: str
    entry_candidate_id: str
    exit_config: Mapping[str, Any]
    execution_config: Mapping[str, Any]
    exit_variant: str

    def __post_init__(self) -> None:
        _non_empty(self.strategy_id, "strategy_id")
        _non_empty(self.entry_candidate_id, "entry_candidate_id")
        _non_empty(self.exit_variant, "exit_variant")
        if not isinstance(self.exit_config, Mapping):
            raise ValueError("exit_config must be a mapping")
        if not isinstance(self.execution_config, Mapping):
            raise ValueError("execution_config must be a mapping")
        _canonical_json(self.exit_config, "exit_config")
        _canonical_json(self.execution_config, "execution_config")


@dataclass(frozen=True, slots=True)
class FrozenEntryCandidate:
    """Canonical entry identity; config access always returns a detached copy."""

    candidate_id: str
    family: str
    entry_variant: str
    threshold_bank_sha256: str
    entry_sha256: str
    _config_json: str

    @property
    def config(self) -> dict[str, Any]:
        return json.loads(self._config_json)

    @classmethod
    def freeze(
        cls,
        spec: EntryCandidateSpec,
        *,
        threshold_bank_sha256: str,
    ) -> FrozenEntryCandidate:
        if not isinstance(spec, EntryCandidateSpec):
            raise ValueError("entry candidates must be EntryCandidateSpec values")
        threshold_sha = _sha256(threshold_bank_sha256, "threshold_bank_sha256")
        config_json = _canonical_json(spec.config, "entry candidate config")
        identity = {
            "kind": "fresh-entry-candidate",
            "candidateId": spec.candidate_id,
            "family": spec.family,
            "entryVariant": spec.entry_variant,
            "thresholdBankSha256": threshold_sha,
            "config": json.loads(config_json),
        }
        return cls(
            candidate_id=spec.candidate_id,
            family=spec.family,
            entry_variant=spec.entry_variant,
            threshold_bank_sha256=threshold_sha,
            entry_sha256=canonical_hash(identity),
            _config_json=config_json,
        )


@dataclass(frozen=True, slots=True)
class FrozenStrategyCandidate:
    """Canonical full-strategy identity tied to the exact frozen entry hash."""

    strategy_id: str
    entry: FrozenEntryCandidate
    exit_variant: str
    strategy_sha256: str
    _exit_config_json: str
    _execution_config_json: str

    @property
    def exit_config(self) -> dict[str, Any]:
        return json.loads(self._exit_config_json)

    @property
    def execution_config(self) -> dict[str, Any]:
        return json.loads(self._execution_config_json)

    @classmethod
    def freeze(
        cls,
        spec: StrategyCandidateSpec,
        *,
        entries_by_id: Mapping[str, FrozenEntryCandidate],
    ) -> FrozenStrategyCandidate:
        if not isinstance(spec, StrategyCandidateSpec):
            raise ValueError("strategy candidates must be StrategyCandidateSpec values")
        entry = entries_by_id.get(spec.entry_candidate_id)
        if entry is None:
            raise ValueError(
                f"strategy {spec.strategy_id!r} references an unpromoted entry"
            )
        exit_json = _canonical_json(spec.exit_config, "exit_config")
        execution_json = _canonical_json(spec.execution_config, "execution_config")
        identity = {
            "kind": "fresh-full-strategy",
            "strategyId": spec.strategy_id,
            "entryCandidateId": entry.candidate_id,
            "entrySha256": entry.entry_sha256,
            "exitVariant": spec.exit_variant,
            "exitConfig": json.loads(exit_json),
            "executionConfig": json.loads(execution_json),
        }
        return cls(
            strategy_id=spec.strategy_id,
            entry=entry,
            exit_variant=spec.exit_variant,
            strategy_sha256=canonical_hash(identity),
            _exit_config_json=exit_json,
            _execution_config_json=execution_json,
        )


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    """Gate/scoring callback result explicitly bound to a frozen identity."""

    identity_sha256: str
    passed: bool
    metrics: Mapping[str, Any]
    leakage_checks: Mapping[str, Any]
    score: float | None

    def __post_init__(self) -> None:
        _sha256(self.identity_sha256, "identity_sha256")
        if not isinstance(self.passed, bool):
            raise ValueError("passed must be boolean")
        if not isinstance(self.metrics, Mapping):
            raise ValueError("metrics must be a mapping")
        if not isinstance(self.leakage_checks, Mapping):
            raise ValueError("leakage_checks must be a mapping")
        _canonical_json(self.metrics, "metrics")
        _canonical_json(self.leakage_checks, "leakage_checks")
        if self.score is not None and (
            isinstance(self.score, bool)
            or not isinstance(self.score, (int, float))
            or not math.isfinite(float(self.score))
        ):
            raise ValueError("score must be finite or None")


@dataclass(frozen=True, slots=True)
class StageRunResult:
    """Immutable summary of one completed stage."""

    stage: str
    evaluated_ids: tuple[str, ...]
    promoted_ids: tuple[str, ...]
    ledger_record_numbers: tuple[int, ...]
    study_failed: bool


ThresholdFitter = Callable[[EvaluationContext], Mapping[str, Any]]
EntryBuilder = Callable[
    [Mapping[str, Any], EvaluationContext], Iterable[EntryCandidateSpec]
]
SignalGenerator = Callable[[FrozenEntryCandidate, EvaluationContext], Any]
EntryScorer = Callable[
    [FrozenEntryCandidate, EvaluationContext, Any], CandidateEvaluation
]
ExitBuilder = Callable[
    [tuple[FrozenEntryCandidate, ...], EvaluationContext],
    Iterable[StrategyCandidateSpec],
]
ScenarioRunner = Callable[[FrozenStrategyCandidate, EvaluationContext, Any], Any]
StrategyScorer = Callable[
    [FrozenStrategyCandidate, EvaluationContext, Any], CandidateEvaluation
]
BatchEntryScorer = Callable[
    [tuple[FrozenEntryCandidate, ...], EvaluationContext],
    Mapping[str, CandidateEvaluation],
]
BatchStrategyScorer = Callable[
    [tuple[FrozenStrategyCandidate, ...], EvaluationContext],
    Mapping[str, CandidateEvaluation],
]
HoldoutAuthorizer = Callable[
    [FrozenStrategyCandidate, tuple[Mapping[str, Any], ...], bool],
    Mapping[str, Any],
]


@dataclass(frozen=True, slots=True)
class FreshSearchCallbacks:
    """All data-, feature-, signal-, execution-, and score-specific hooks."""

    fit_thresholds: ThresholdFitter
    build_entry_candidates: EntryBuilder
    generate_signals: SignalGenerator
    score_entry: EntryScorer
    build_exit_variants: ExitBuilder
    run_execution_scenarios: ScenarioRunner
    score_strategy: StrategyScorer
    authorize_holdout: HoldoutAuthorizer | None = None
    score_entries_batch: BatchEntryScorer | None = None
    score_strategies_batch: BatchStrategyScorer | None = None

    def __post_init__(self) -> None:
        required = (
            self.fit_thresholds,
            self.build_entry_candidates,
            self.generate_signals,
            self.score_entry,
            self.build_exit_variants,
            self.run_execution_scenarios,
            self.score_strategy,
        )
        if any(not callable(callback) for callback in required):
            raise ValueError("all required research callbacks must be callable")
        if self.authorize_holdout is not None and not callable(self.authorize_holdout):
            raise ValueError("authorize_holdout must be callable or None")
        if self.score_entries_batch is not None and not callable(
            self.score_entries_batch
        ):
            raise ValueError("score_entries_batch must be callable or None")
        if self.score_strategies_batch is not None and not callable(
            self.score_strategies_batch
        ):
            raise ValueError("score_strategies_batch must be callable or None")


def _freeze_windows(
    split_manifest: Mapping[str, Any],
) -> dict[str, FrozenResearchWindow]:
    if not isinstance(split_manifest, Mapping):
        raise ValueError("split_manifest must be a mapping")
    materialized = _json_clone(split_manifest, "split_manifest")
    claimed_manifest_sha = materialized.get("manifestSha256")
    if claimed_manifest_sha is not None:
        claimed = _sha256(claimed_manifest_sha, "manifestSha256")
        body = {
            key: value for key, value in materialized.items() if key != "manifestSha256"
        }
        if canonical_hash(body) != claimed:
            raise ValueError("split manifest hash is invalid")
    windows = materialized.get("windows")
    if not isinstance(windows, Mapping) or set(windows) != set(_ROLE_ORDER):
        raise ValueError("split manifest must contain each research role exactly once")

    frozen: dict[str, FrozenResearchWindow] = {}
    previous_anchor: date | None = None
    seen: set[str] = set()
    for role in _ROLE_ORDER:
        window = windows[role]
        if not isinstance(window, Mapping) or window.get("role") != role:
            raise ValueError(f"split window {role!r} has the wrong role binding")
        anchors = window.get("sessionAnchors")
        if (
            not isinstance(anchors, Sequence)
            or isinstance(anchors, (str, bytes))
            or not anchors
        ):
            raise ValueError(f"split window {role!r} has no session anchors")
        parsed_anchors: list[str] = []
        for raw in anchors:
            if not isinstance(raw, str):
                raise ValueError("session anchors must be ISO dates")
            try:
                parsed = date.fromisoformat(raw)
            except ValueError as exc:
                raise ValueError("session anchors must be ISO dates") from exc
            if previous_anchor is not None and parsed <= previous_anchor:
                raise ValueError("split windows must be globally chronological")
            if raw in seen:
                raise ValueError("a session anchor cannot occur in multiple windows")
            parsed_anchors.append(parsed.isoformat())
            seen.add(raw)
            previous_anchor = parsed
        if window.get("sessionCount") != len(parsed_anchors):
            raise ValueError(f"split window {role!r} has an invalid session count")
        if window.get("firstSessionAnchor") != parsed_anchors[0]:
            raise ValueError(f"split window {role!r} has an invalid first anchor")
        if window.get("lastSessionAnchor") != parsed_anchors[-1]:
            raise ValueError(f"split window {role!r} has an invalid last anchor")
        frozen[role] = FrozenResearchWindow(
            role=role,
            session_anchors=tuple(parsed_anchors),
            window_sha256=canonical_hash(window),
        )
    return frozen


class FreshChronologicalSearch:
    """State machine for one complete, non-reusable chronological search."""

    def __init__(
        self,
        *,
        split_manifest: Mapping[str, Any],
        ledger_path: str | Path,
        budgets: FreshSearchBudgets,
        callbacks: FreshSearchCallbacks,
        preregistration_sha256: str | None = None,
        _allow_existing_ledger_for_recovery: bool = False,
    ) -> None:
        if not isinstance(budgets, FreshSearchBudgets):
            raise ValueError("budgets must be FreshSearchBudgets")
        if not isinstance(callbacks, FreshSearchCallbacks):
            raise ValueError("callbacks must be FreshSearchCallbacks")
        selected_ledger = Path(ledger_path).expanduser()
        if selected_ledger.is_symlink():
            raise PermissionError("the experiment ledger cannot be a symbolic link")
        destination = selected_ledger.resolve()
        if (
            destination.exists()
            and destination.stat().st_size
            and not _allow_existing_ledger_for_recovery
        ):
            raise ValueError("a fresh search requires an empty experiment ledger")
        self._split_manifest = _json_clone(split_manifest, "split_manifest")
        self._windows = _freeze_windows(self._split_manifest)
        self._ledger_path = destination
        self._budgets = budgets
        self._callbacks = callbacks
        self._preregistration_sha256 = (
            _sha256(preregistration_sha256, "preregistration_sha256")
            if preregistration_sha256 is not None
            else None
        )
        self._stage = FreshSearchStage.NEW
        self._consumed_roles: list[str] = []
        self._records: list[dict[str, Any]] = []
        self._entry_pool: tuple[FrozenEntryCandidate, ...] = ()
        self._strategy_pool: tuple[FrozenStrategyCandidate, ...] = ()
        self._validation_winner: FrozenStrategyCandidate | None = None
        self._threshold_bank_json: str | None = None
        self._threshold_bank_sha256: str | None = None
        self._holdout_authorization: dict[str, Any] | None = None
        self._holdout_attempted = False
        self._resume_discovery_candidates: tuple[FrozenEntryCandidate, ...] = ()
        self._recovery_audit: dict[str, Any] | None = None
        self._recovery_batch_result_path: Path | None = None

    @classmethod
    def resume_incomplete_discovery(
        cls,
        *,
        split_manifest: Mapping[str, Any],
        ledger_path: str | Path,
        budgets: FreshSearchBudgets,
        callbacks: FreshSearchCallbacks,
        preregistration_sha256: str,
        threshold_bank: Mapping[str, Any],
        entry_specs: Sequence[EntryCandidateSpec],
        recovery_audit: Mapping[str, Any],
        recovery_batch_result_path: str | Path,
    ) -> "FreshChronologicalSearch":
        """Authorize one audited continuation of an exact incomplete batch.

        This is deliberately narrower than generic checkpointing.  It accepts
        only a frozen two-record terminal prefix produced when discovery was
        interrupted before a single candidate result was recorded.
        """

        selected_ledger = Path(ledger_path).expanduser()
        if selected_ledger.is_symlink():
            raise PermissionError("the recovery ledger cannot be a symbolic link")
        destination = selected_ledger.resolve()
        records = _load_verified_records(destination)
        if len(records) != 2:
            raise PermissionError(
                "recovery requires the exact two-record ledger prefix"
            )
        stage_access, batch_access = records
        expected_stage = {
            "recordNumber": 1,
            "recordKind": "stage-window-access",
            "candidateId": "protocol-stage-access::discovery",
            "stage": "discovery",
            "role": "discovery",
            "status": "window_access_started",
            "outcomesRevealed": True,
        }
        expected_batch = {
            "recordNumber": 2,
            "recordKind": "batch-window-access",
            "candidateId": "protocol-batch-access::entry::discovery",
            "stage": "discovery",
            "role": "discovery",
            "status": "batch_access_started",
            "outcomesRevealed": True,
        }
        if any(stage_access.get(key) != value for key, value in expected_stage.items()):
            raise PermissionError(
                "the discovery stage-access record is not recoverable"
            )
        if any(batch_access.get(key) != value for key, value in expected_batch.items()):
            raise PermissionError(
                "the discovery batch-access record is not recoverable"
            )
        prereg_sha = _sha256(preregistration_sha256, "preregistration_sha256")
        if any(record.get("preregistrationSha256") != prereg_sha for record in records):
            raise PermissionError("recovery ledger preregistration identity changed")
        if any(
            record.get("recordKind")
            not in ("stage-window-access", "batch-window-access")
            for record in records
        ):
            raise PermissionError(
                "candidate outcomes already exist in the recovery ledger"
            )

        audit = _json_clone(recovery_audit, "recovery audit")
        required_audit = {
            "schema",
            "recoveryAttemptId",
            "recoveryAttempt",
            "maximumRecoveryAttempts",
            "originalRunId",
            "originalCommitSha",
            "ledgerPrefixSha256",
            "originalRecordSha256",
            "candidateOutcomeRecordCount",
            "laterRoleRecordCount",
            "holdoutAuthorizationPresent",
            "oomEvidence",
            "identity",
            "permittedProcedure",
        }
        if set(audit) != required_audit:
            raise PermissionError("the recovery audit has an unexpected schema")
        if (
            audit["schema"] != "fresh-xauusd-infrastructure-recovery/v1"
            or audit["recoveryAttempt"] != 1
            or audit["maximumRecoveryAttempts"] != 1
            or audit["candidateOutcomeRecordCount"] != 0
            or audit["laterRoleRecordCount"] != 0
            or audit["holdoutAuthorizationPresent"] is not False
        ):
            raise PermissionError(
                "the recovery audit is not a one-time untouched continuation"
            )
        ledger_prefix_sha = _sha256(
            str(audit["ledgerPrefixSha256"]), "ledgerPrefixSha256"
        )
        if _file_sha256(destination) != ledger_prefix_sha:
            raise PermissionError("the durable ledger differs from the audited prefix")
        claimed_record_sha = audit["originalRecordSha256"]
        if claimed_record_sha != [
            stage_access["recordSha256"],
            batch_access["recordSha256"],
        ]:
            raise PermissionError(
                "recovery audit record hashes do not match the ledger"
            )

        threshold_json = _canonical_json(threshold_bank, "threshold bank")
        threshold_sha = canonical_hash(json.loads(threshold_json))
        specs = tuple(entry_specs)
        if not specs or len(specs) > budgets.discovery_distinct_candidates:
            raise PermissionError(
                "recovery entry candidate count violates the frozen budget"
            )
        candidates = tuple(
            FrozenEntryCandidate.freeze(spec, threshold_bank_sha256=threshold_sha)
            for spec in specs
        )
        candidate_ids = [candidate.candidate_id for candidate in candidates]
        candidate_sha = [candidate.entry_sha256 for candidate in candidates]
        parameters = batch_access.get("parameters")
        if not isinstance(parameters, Mapping) or (
            parameters.get("candidateIds") != candidate_ids
            or parameters.get("candidateSha256") != candidate_sha
        ):
            raise PermissionError(
                "reconstructed candidates differ from the started batch"
            )
        if len(candidate_ids) != len(set(candidate_ids)):
            raise PermissionError("recovery candidates are not unique")
        family_counts: dict[str, int] = {}
        for candidate in candidates:
            family_counts[candidate.family] = family_counts.get(candidate.family, 0) + 1
        if any(
            count > budgets.discovery_per_family_maximum
            for count in family_counts.values()
        ):
            raise PermissionError("recovery candidate family budget changed")

        identity = audit["identity"]
        if not isinstance(identity, Mapping):
            raise PermissionError("recovery identity must be a mapping")
        expected_identity = {
            "splitManifestSha256": split_manifest.get("manifestSha256"),
            "preregistrationSha256": prereg_sha,
            "thresholdBankSha256": threshold_sha,
            "orderedCandidateSequenceSha256": canonical_hash(
                [
                    {
                        "candidateId": candidate.candidate_id,
                        "entrySha256": candidate.entry_sha256,
                    }
                    for candidate in candidates
                ]
            ),
            "candidateCount": len(candidates),
        }
        if any(identity.get(key) != value for key, value in expected_identity.items()):
            raise PermissionError("recovery identities differ from the frozen batch")

        search = cls(
            split_manifest=split_manifest,
            ledger_path=destination,
            budgets=budgets,
            callbacks=callbacks,
            preregistration_sha256=prereg_sha,
            _allow_existing_ledger_for_recovery=True,
        )
        search._records = [
            _json_clone(record, "recovery ledger record") for record in records
        ]
        search._consumed_roles = ["discovery"]
        search._threshold_bank_json = threshold_json
        search._threshold_bank_sha256 = threshold_sha
        search._resume_discovery_candidates = candidates
        search._recovery_audit = audit
        selected_batch_destination = Path(recovery_batch_result_path).expanduser()
        if (
            selected_batch_destination.is_symlink()
            or selected_batch_destination.parent.is_symlink()
        ):
            raise PermissionError("the recovery batch-result artifact is unsafe")
        batch_destination = selected_batch_destination.resolve()
        if batch_destination.exists() or batch_destination.is_symlink():
            raise PermissionError("the recovery batch-result artifact already exists")
        if batch_destination.parent.is_symlink():
            raise PermissionError("the recovery batch-result directory is unsafe")
        search._recovery_batch_result_path = batch_destination
        context = search._context(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
        )
        search._append_resume_protocol_record(
            status="resume_eligibility_audit",
            context=context,
            parameters={
                "recoveryAttemptId": audit["recoveryAttemptId"],
                "ledgerPrefixSha256": ledger_prefix_sha,
                "originalRecordSha256": claimed_record_sha,
                "oomEvidence": audit["oomEvidence"],
                "candidateOutcomeRecordCount": 0,
                "laterRoleRecordCount": 0,
                "holdoutAuthorizationPresent": False,
            },
            leakage_checks={
                "originalLedgerPreserved": True,
                "zeroCandidateOutcomesBeforeResume": True,
                "laterRolesUntouched": True,
                "holdoutAuthorizationAbsent": True,
            },
        )
        search._append_resume_protocol_record(
            status="resume_authorized",
            context=context,
            parameters={
                key: audit[key]
                for key in (
                    "recoveryAttemptId",
                    "recoveryAttempt",
                    "maximumRecoveryAttempts",
                    "originalRunId",
                    "originalCommitSha",
                    "permittedProcedure",
                )
            },
            leakage_checks={
                "purelyMechanicalRecoveryOnly": True,
                "candidateThresholdAndGateChangesForbidden": True,
                "allOriginalCandidatesRequired": True,
            },
        )
        search._append_resume_protocol_record(
            status="resume_identity_verified",
            context=context,
            parameters=dict(identity),
            metrics={"candidateCount": len(candidates)},
            leakage_checks={
                "splitIdentityVerified": True,
                "thresholdIdentityVerified": True,
                "orderedCandidateIdentityVerified": True,
                "originalPreregistrationIdentityVerified": True,
            },
        )
        search._stage = FreshSearchStage.DISCOVERY_RESUME_AUTHORIZED
        return search

    @property
    def stage(self) -> FreshSearchStage:
        return self._stage

    @property
    def consumed_roles(self) -> tuple[str, ...]:
        return tuple(self._consumed_roles)

    @property
    def entry_candidates(self) -> tuple[FrozenEntryCandidate, ...]:
        return self._entry_pool

    @property
    def strategy_candidates(self) -> tuple[FrozenStrategyCandidate, ...]:
        return self._strategy_pool

    @property
    def validation_winner(self) -> FrozenStrategyCandidate | None:
        return self._validation_winner

    @property
    def holdout_authorization(self) -> dict[str, Any] | None:
        if self._holdout_authorization is None:
            return None
        return _json_clone(self._holdout_authorization, "holdout authorization")

    @property
    def audit_records(self) -> tuple[dict[str, Any], ...]:
        return tuple(_json_clone(record, "audit record") for record in self._records)

    def _require_stage(self, expected: FreshSearchStage) -> None:
        if self._stage is not expected:
            raise FreshSearchProtocolError(
                f"operation requires stage {expected.value!r}; current stage is "
                f"{self._stage.value!r}"
            )

    def _consume_role(self, role: str) -> None:
        expected_index = len(self._consumed_roles)
        if expected_index >= len(_ROLE_ORDER) or _ROLE_ORDER[expected_index] != role:
            raise FreshSearchProtocolError(
                f"role {role!r} cannot be evaluated after {self._consumed_roles!r}"
            )
        # Consume before any callback: an exception must never turn an unseen
        # outcome window into a silently reusable tuning window.
        self._consumed_roles.append(role)

    def _context(
        self,
        *,
        stage: str,
        training_roles: Sequence[str],
        evaluation_roles: Sequence[str],
    ) -> EvaluationContext:
        roles: list[str] = []
        for role in (*training_roles, *evaluation_roles):
            if role not in roles:
                roles.append(role)
        return EvaluationContext(
            stage=stage,
            training_roles=tuple(training_roles),
            evaluation_roles=tuple(evaluation_roles),
            windows=tuple(self._windows[role] for role in roles),
        )

    @staticmethod
    def _rank_passed(
        items: Sequence[tuple[Any, CandidateEvaluation]],
        *,
        identifier: Callable[[Any], str],
        limit: int,
    ) -> tuple[Any, ...]:
        passed = [item for item in items if item[1].passed]

        def key(item: tuple[Any, CandidateEvaluation]) -> tuple[Any, ...]:
            candidate, result = item
            score_missing = result.score is None
            score = float(result.score) if result.score is not None else float("-inf")
            return (score_missing, -score, identifier(candidate))

        return tuple(candidate for candidate, _ in sorted(passed, key=key)[:limit])

    def _append_record(
        self,
        *,
        candidate_id: str,
        family: str,
        stage: str,
        role: str | None,
        context: EvaluationContext,
        parameters: Mapping[str, Any],
        entry_variant: str,
        exit_variant: str,
        identity_sha256: str,
        evaluation: CandidateEvaluation | None,
        outcomes_revealed: bool,
        status: str,
        frozen_entry_sha256: str,
        frozen_strategy_sha256: str | None = None,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "candidateId": candidate_id,
            "family": family,
            "stage": stage,
            "trainingWindow": "+".join(context.training_roles),
            "evaluationWindow": "+".join(context.evaluation_roles),
            "parameters": _json_clone(parameters, "ledger parameters"),
            "entryVariant": entry_variant,
            "exitVariant": exit_variant,
            "metrics": (
                _json_clone(evaluation.metrics, "evaluation metrics")
                if evaluation is not None
                else {}
            ),
            "status": status,
            "leakageChecks": (
                _json_clone(evaluation.leakage_checks, "leakage checks")
                if evaluation is not None
                else {"callbackCompleted": False}
            ),
            "role": role,
            "outcomesRevealed": outcomes_revealed,
            "gatePassed": evaluation.passed if evaluation is not None else False,
            "identitySha256": identity_sha256,
            "frozenEntrySha256": frozen_entry_sha256,
            "frozenStrategySha256": frozen_strategy_sha256,
            "windowSha256": canonical_hash(
                [window.window_sha256 for window in context.windows]
            ),
        }
        if evaluation is not None:
            record["balancedScore"] = evaluation.score
        if self._preregistration_sha256 is not None:
            record["preregistrationSha256"] = self._preregistration_sha256
        enriched = append_fresh_record(self._ledger_path, record)
        self._records.append(enriched)
        return enriched

    def _append_batch_access_record(
        self,
        *,
        kind: str,
        status: str,
        context: EvaluationContext,
        role: str | None,
        candidate_ids: Sequence[str],
        candidate_sha256: Sequence[str],
        error_type: str | None = None,
    ) -> dict[str, Any]:
        """Durably record batch-window access around an outcome callback.

        Every marker is outcome-revealing.  In particular, the start marker is
        fsynced before callback invocation so abrupt termination cannot make the
        window reusable.  Candidate records remain the authoritative promotion
        and holdout evidence; these protocol records only consume access.
        """

        ids = tuple(candidate_ids)
        digests = tuple(candidate_sha256)
        if kind not in ("entry", "strategy"):
            raise ValueError("batch access kind must be entry or strategy")
        if status not in (
            "batch_access_started",
            "batch_access_completed",
            "batch_access_error",
        ):
            raise ValueError("unsupported batch access status")
        if len(ids) != len(digests) or not ids:
            raise ValueError("batch access candidates must be non-empty and aligned")
        if len(ids) != len(set(ids)):
            raise ValueError("batch access candidate ids must be unique")
        normalized_digests = tuple(
            _sha256(value, f"batch candidate digest {index}")
            for index, value in enumerate(digests)
        )
        if error_type is not None:
            _non_empty(error_type, "error_type")
        if (status == "batch_access_error") != (error_type is not None):
            raise ValueError("only batch access errors may identify an error type")

        identity_payload = {
            "kind": "fresh-batch-window-access",
            "batchKind": kind,
            "status": status,
            "stage": context.stage,
            "trainingRoles": list(context.training_roles),
            "evaluationRoles": list(context.evaluation_roles),
            "candidateIds": list(ids),
            "candidateSha256": list(normalized_digests),
            "errorType": error_type,
        }
        identity_sha256 = canonical_hash(identity_payload)
        record: dict[str, Any] = {
            "recordKind": "batch-window-access",
            "candidateId": f"protocol-batch-access::{kind}::{context.stage}",
            "family": "protocol-window-access",
            "stage": context.stage,
            "trainingWindow": "+".join(context.training_roles),
            "evaluationWindow": "+".join(context.evaluation_roles),
            "parameters": identity_payload,
            "entryVariant": "batch-window-access",
            "exitVariant": "batch-window-access",
            "metrics": {
                "candidateCount": len(ids),
                "errorType": error_type,
            },
            "status": status,
            "leakageChecks": {
                "durableBeforeCallback": status == "batch_access_started",
                "callbackCompleted": status == "batch_access_completed",
                "callbackErrored": status == "batch_access_error",
            },
            "role": role,
            "outcomesRevealed": True,
            "gatePassed": False,
            "identitySha256": identity_sha256,
            "windowSha256": canonical_hash(
                [window.window_sha256 for window in context.windows]
            ),
        }
        if self._preregistration_sha256 is not None:
            record["preregistrationSha256"] = self._preregistration_sha256
        enriched = append_fresh_record(self._ledger_path, record)
        self._records.append(enriched)
        return enriched

    def _append_resume_protocol_record(
        self,
        *,
        status: str,
        context: EvaluationContext,
        parameters: Mapping[str, Any],
        leakage_checks: Mapping[str, Any],
        metrics: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append one durable, outcome-revealing infrastructure-recovery record."""

        allowed = {
            "resume_eligibility_audit",
            "resume_authorized",
            "resume_identity_verified",
            "batch_resume_started",
            "batch_resume_completed",
            "batch_resume_error",
            "resume_stage_completed",
        }
        if status not in allowed:
            raise ValueError("unsupported infrastructure recovery status")
        if self._recovery_audit is None:
            raise FreshSearchProtocolError("no infrastructure recovery is authorized")
        payload = {
            "kind": "fresh-infrastructure-recovery",
            "status": status,
            "stage": context.stage,
            "recoveryAttemptId": self._recovery_audit["recoveryAttemptId"],
            "parameters": _json_clone(parameters, "recovery protocol parameters"),
        }
        record: dict[str, Any] = {
            "recordKind": "infrastructure-resume",
            "candidateId": f"protocol-infrastructure-resume::{status}",
            "family": "protocol-infrastructure-recovery",
            "stage": context.stage,
            "trainingWindow": "+".join(context.training_roles),
            "evaluationWindow": "+".join(context.evaluation_roles),
            "parameters": payload,
            "entryVariant": "infrastructure-resume",
            "exitVariant": "infrastructure-resume",
            "metrics": _json_clone(metrics or {}, "recovery protocol metrics"),
            "status": status,
            "leakageChecks": _json_clone(
                leakage_checks, "recovery protocol leakage checks"
            ),
            "role": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "identitySha256": canonical_hash(payload),
            "windowSha256": canonical_hash(
                [window.window_sha256 for window in context.windows]
            ),
        }
        if self._preregistration_sha256 is not None:
            record["preregistrationSha256"] = self._preregistration_sha256
        enriched = append_fresh_record(self._ledger_path, record)
        self._records.append(enriched)
        return enriched

    def _seal_recovery_batch_result(
        self,
        *,
        candidates: Sequence[FrozenEntryCandidate],
        results: Mapping[str, CandidateEvaluation],
    ) -> tuple[str, str]:
        """Create the immutable, complete discovery result before ledger promotion."""

        if self._recovery_audit is None or self._recovery_batch_result_path is None:
            raise FreshSearchProtocolError("recovery batch sealing is unavailable")
        ordered_results = [
            {
                "candidateId": candidate.candidate_id,
                "entrySha256": candidate.entry_sha256,
                "evaluation": _evaluation_payload(results[candidate.candidate_id]),
            }
            for candidate in candidates
        ]
        body = {
            "schema": "fresh-xauusd-recovery-discovery-batch/v1",
            "recoveryAttemptId": self._recovery_audit["recoveryAttemptId"],
            "preregistrationSha256": self._preregistration_sha256,
            "candidateCount": len(ordered_results),
            "orderedResults": ordered_results,
        }
        batch_sha = canonical_hash(body)
        document = {**body, "batchResultSha256": batch_sha}
        encoded = (
            json.dumps(
                document,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        destination = self._recovery_batch_result_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() or destination.is_symlink():
            raise PermissionError("the recovery batch-result artifact already exists")
        temporary = destination.with_name(destination.name + ".tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary.unlink(missing_ok=True)
            raise
        file_sha = _file_sha256(destination)
        if destination.read_bytes() != encoded:
            raise FreshSearchProtocolError("the sealed recovery batch artifact changed")
        return batch_sha, file_sha

    def _append_stage_window_access_record(
        self,
        *,
        context: EvaluationContext,
        role: str,
        purpose: str,
    ) -> dict[str, Any]:
        """Fsync a role-consumption marker before any stage-level data callback."""

        _non_empty(purpose, "stage access purpose")
        identity_payload = {
            "kind": "fresh-stage-window-access",
            "status": "window_access_started",
            "stage": context.stage,
            "role": role,
            "purpose": purpose,
            "trainingRoles": list(context.training_roles),
            "evaluationRoles": list(context.evaluation_roles),
        }
        record: dict[str, Any] = {
            "recordKind": "stage-window-access",
            "candidateId": f"protocol-stage-access::{context.stage}",
            "family": "protocol-window-access",
            "stage": context.stage,
            "trainingWindow": "+".join(context.training_roles),
            "evaluationWindow": "+".join(context.evaluation_roles),
            "parameters": identity_payload,
            "entryVariant": "stage-window-access",
            "exitVariant": "stage-window-access",
            "metrics": {"purpose": purpose},
            "status": "window_access_started",
            "leakageChecks": {
                "durableBeforeCallback": True,
                "windowConsumedBeforeCallback": True,
            },
            "role": role,
            "outcomesRevealed": True,
            "gatePassed": False,
            "identitySha256": canonical_hash(identity_payload),
            "windowSha256": canonical_hash(
                [window.window_sha256 for window in context.windows]
            ),
        }
        if self._preregistration_sha256 is not None:
            record["preregistrationSha256"] = self._preregistration_sha256
        enriched = append_fresh_record(self._ledger_path, record)
        self._records.append(enriched)
        return enriched

    def _evaluate_entries(
        self,
        *,
        candidates: Sequence[FrozenEntryCandidate],
        context: EvaluationContext,
        role: str,
    ) -> tuple[list[tuple[FrozenEntryCandidate, CandidateEvaluation]], list[int]]:
        selected = tuple(candidates)
        evaluated: list[tuple[FrozenEntryCandidate, CandidateEvaluation]] = []
        record_numbers: list[int] = []
        batch_results: Mapping[str, CandidateEvaluation] | None = None
        if self._callbacks.score_entries_batch is not None:
            started = self._append_batch_access_record(
                kind="entry",
                status="batch_access_started",
                context=context,
                role=role,
                candidate_ids=tuple(candidate.candidate_id for candidate in selected),
                candidate_sha256=tuple(
                    candidate.entry_sha256 for candidate in selected
                ),
            )
            record_numbers.append(int(started["recordNumber"]))
            try:
                batch_results = self._callbacks.score_entries_batch(selected, context)
                if not isinstance(batch_results, Mapping):
                    raise ValueError(
                        "score_entries_batch must return a candidate-id mapping"
                    )
                expected_ids = {candidate.candidate_id for candidate in selected}
                if set(batch_results) != expected_ids:
                    raise ValueError(
                        "score_entries_batch must return every requested candidate "
                        "exactly once"
                    )
            except Exception as exc:
                failed = self._append_batch_access_record(
                    kind="entry",
                    status="batch_access_error",
                    context=context,
                    role=role,
                    candidate_ids=tuple(
                        candidate.candidate_id for candidate in selected
                    ),
                    candidate_sha256=tuple(
                        candidate.entry_sha256 for candidate in selected
                    ),
                    error_type=type(exc).__name__,
                )
                record_numbers.append(int(failed["recordNumber"]))
                self._stage = FreshSearchStage.FAILED
                raise
            completed = self._append_batch_access_record(
                kind="entry",
                status="batch_access_completed",
                context=context,
                role=role,
                candidate_ids=tuple(candidate.candidate_id for candidate in selected),
                candidate_sha256=tuple(
                    candidate.entry_sha256 for candidate in selected
                ),
            )
            record_numbers.append(int(completed["recordNumber"]))
        for candidate in selected:
            try:
                if batch_results is None:
                    signals = self._callbacks.generate_signals(candidate, context)
                    result = self._callbacks.score_entry(candidate, context, signals)
                else:
                    result = batch_results[candidate.candidate_id]
                if not isinstance(result, CandidateEvaluation):
                    raise ValueError("entry scorer must return CandidateEvaluation")
                if result.identity_sha256 != candidate.entry_sha256:
                    raise FrozenIdentityError(
                        f"entry result for {candidate.candidate_id!r} has a different "
                        "frozen identity"
                    )
            except Exception:
                record = self._append_record(
                    candidate_id=candidate.candidate_id,
                    family=candidate.family,
                    stage=context.stage,
                    role=role,
                    context=context,
                    parameters={
                        "entryConfig": candidate.config,
                        "thresholdBankSha256": candidate.threshold_bank_sha256,
                    },
                    entry_variant=candidate.entry_variant,
                    exit_variant="entry-edge-only",
                    identity_sha256=candidate.entry_sha256,
                    evaluation=None,
                    outcomes_revealed=True,
                    status="evaluation_error",
                    frozen_entry_sha256=candidate.entry_sha256,
                )
                record_numbers.append(int(record["recordNumber"]))
                self._stage = FreshSearchStage.FAILED
                raise
            record = self._append_record(
                candidate_id=candidate.candidate_id,
                family=candidate.family,
                stage=context.stage,
                role=role,
                context=context,
                parameters={
                    "entryConfig": candidate.config,
                    "thresholdBankSha256": candidate.threshold_bank_sha256,
                },
                entry_variant=candidate.entry_variant,
                exit_variant="entry-edge-only",
                identity_sha256=candidate.entry_sha256,
                evaluation=result,
                outcomes_revealed=True,
                status="passed" if result.passed else "rejected",
                frozen_entry_sha256=candidate.entry_sha256,
            )
            record_numbers.append(int(record["recordNumber"]))
            evaluated.append((candidate, result))
        return evaluated, record_numbers

    def _evaluate_strategies(
        self,
        *,
        candidates: Sequence[FrozenStrategyCandidate],
        context: EvaluationContext,
        role: str | None,
        outcomes_revealed: bool,
    ) -> tuple[list[tuple[FrozenStrategyCandidate, CandidateEvaluation]], list[int]]:
        selected = tuple(candidates)
        evaluated: list[tuple[FrozenStrategyCandidate, CandidateEvaluation]] = []
        record_numbers: list[int] = []
        batch_results: Mapping[str, CandidateEvaluation] | None = None
        if self._callbacks.score_strategies_batch is not None:
            started = self._append_batch_access_record(
                kind="strategy",
                status="batch_access_started",
                context=context,
                role=role,
                candidate_ids=tuple(candidate.strategy_id for candidate in selected),
                candidate_sha256=tuple(
                    candidate.strategy_sha256 for candidate in selected
                ),
            )
            record_numbers.append(int(started["recordNumber"]))
            try:
                batch_results = self._callbacks.score_strategies_batch(
                    selected, context
                )
                if not isinstance(batch_results, Mapping):
                    raise ValueError(
                        "score_strategies_batch must return a strategy-id mapping"
                    )
                expected_ids = {candidate.strategy_id for candidate in selected}
                if set(batch_results) != expected_ids:
                    raise ValueError(
                        "score_strategies_batch must return every requested strategy "
                        "exactly once"
                    )
            except Exception as exc:
                failed = self._append_batch_access_record(
                    kind="strategy",
                    status="batch_access_error",
                    context=context,
                    role=role,
                    candidate_ids=tuple(
                        candidate.strategy_id for candidate in selected
                    ),
                    candidate_sha256=tuple(
                        candidate.strategy_sha256 for candidate in selected
                    ),
                    error_type=type(exc).__name__,
                )
                record_numbers.append(int(failed["recordNumber"]))
                self._stage = FreshSearchStage.FAILED
                raise
            completed = self._append_batch_access_record(
                kind="strategy",
                status="batch_access_completed",
                context=context,
                role=role,
                candidate_ids=tuple(candidate.strategy_id for candidate in selected),
                candidate_sha256=tuple(
                    candidate.strategy_sha256 for candidate in selected
                ),
            )
            record_numbers.append(int(completed["recordNumber"]))
        for candidate in selected:
            try:
                if batch_results is None:
                    signals = self._callbacks.generate_signals(candidate.entry, context)
                    scenario_results = self._callbacks.run_execution_scenarios(
                        candidate, context, signals
                    )
                    result = self._callbacks.score_strategy(
                        candidate, context, scenario_results
                    )
                else:
                    result = batch_results[candidate.strategy_id]
                if not isinstance(result, CandidateEvaluation):
                    raise ValueError("strategy scorer must return CandidateEvaluation")
                if result.identity_sha256 != candidate.strategy_sha256:
                    raise FrozenIdentityError(
                        f"strategy result for {candidate.strategy_id!r} has a "
                        "different frozen identity"
                    )
            except Exception:
                record = self._append_record(
                    candidate_id=candidate.strategy_id,
                    family=candidate.entry.family,
                    stage=context.stage,
                    role=role,
                    context=context,
                    parameters={
                        "entryCandidateId": candidate.entry.candidate_id,
                        "entryConfig": candidate.entry.config,
                        "exitConfig": candidate.exit_config,
                        "executionConfig": candidate.execution_config,
                    },
                    entry_variant=candidate.entry.entry_variant,
                    exit_variant=candidate.exit_variant,
                    identity_sha256=candidate.strategy_sha256,
                    evaluation=None,
                    outcomes_revealed=outcomes_revealed,
                    status="evaluation_error",
                    frozen_entry_sha256=candidate.entry.entry_sha256,
                    frozen_strategy_sha256=candidate.strategy_sha256,
                )
                record_numbers.append(int(record["recordNumber"]))
                self._stage = FreshSearchStage.FAILED
                raise
            record = self._append_record(
                candidate_id=candidate.strategy_id,
                family=candidate.entry.family,
                stage=context.stage,
                role=role,
                context=context,
                parameters={
                    "entryCandidateId": candidate.entry.candidate_id,
                    "entryConfig": candidate.entry.config,
                    "exitConfig": candidate.exit_config,
                    "executionConfig": candidate.execution_config,
                },
                entry_variant=candidate.entry.entry_variant,
                exit_variant=candidate.exit_variant,
                identity_sha256=candidate.strategy_sha256,
                evaluation=result,
                outcomes_revealed=outcomes_revealed,
                status="passed" if result.passed else "rejected",
                frozen_entry_sha256=candidate.entry.entry_sha256,
                frozen_strategy_sha256=candidate.strategy_sha256,
            )
            record_numbers.append(int(record["recordNumber"]))
            evaluated.append((candidate, result))
        return evaluated, record_numbers

    def run_discovery(self) -> StageRunResult:
        self._require_stage(FreshSearchStage.NEW)
        self._consume_role("discovery")
        context = self._context(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
        )
        access = self._append_stage_window_access_record(
            context=context,
            role="discovery",
            purpose="fit thresholds and construct the frozen entry bank",
        )
        numbers = [int(access["recordNumber"])]
        try:
            thresholds = self._callbacks.fit_thresholds(context)
            if not isinstance(thresholds, Mapping):
                raise ValueError("fit_thresholds must return a mapping")
            self._threshold_bank_json = _canonical_json(thresholds, "threshold bank")
            self._threshold_bank_sha256 = canonical_hash(
                json.loads(self._threshold_bank_json)
            )
            specs = tuple(
                self._callbacks.build_entry_candidates(
                    json.loads(self._threshold_bank_json), context
                )
            )
            if not specs:
                raise ValueError("discovery must produce at least one entry candidate")
            if len(specs) > self._budgets.discovery_distinct_candidates:
                raise ValueError("discovery candidate budget exceeded")
            candidates = tuple(
                FrozenEntryCandidate.freeze(
                    spec, threshold_bank_sha256=self._threshold_bank_sha256
                )
                for spec in specs
            )
            identifiers = [candidate.candidate_id for candidate in candidates]
            if len(identifiers) != len(set(identifiers)):
                raise ValueError("discovery candidate ids must be unique")
            family_counts: dict[str, int] = {}
            for candidate in candidates:
                family_counts[candidate.family] = (
                    family_counts.get(candidate.family, 0) + 1
                )
            if any(
                count > self._budgets.discovery_per_family_maximum
                for count in family_counts.values()
            ):
                raise ValueError("discovery per-family candidate budget exceeded")
            evaluated, evaluation_numbers = self._evaluate_entries(
                candidates=candidates, context=context, role="discovery"
            )
            numbers.extend(evaluation_numbers)
        except Exception:
            self._stage = FreshSearchStage.FAILED
            raise
        self._entry_pool = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.candidate_id,
            limit=self._budgets.walk_forward_1_frozen_candidates,
        )
        failed = not self._entry_pool
        self._stage = (
            FreshSearchStage.FAILED if failed else FreshSearchStage.DISCOVERY_COMPLETE
        )
        return StageRunResult(
            stage="discovery",
            evaluated_ids=tuple(candidate.candidate_id for candidate, _ in evaluated),
            promoted_ids=tuple(
                candidate.candidate_id for candidate in self._entry_pool
            ),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def run_frozen_discovery(
        self,
        *,
        threshold_bank: Mapping[str, Any],
        entry_specs: Sequence[EntryCandidateSpec],
    ) -> StageRunResult:
        """Evaluate an immutable outcome-blind bank in a new empty study.

        This is not recovery of an existing ledger.  It starts a separately
        preregistered study whose lineage already binds the imported threshold
        and entry-bank artifacts.  The normal stage/batch access records are
        written to the new ledger before any candidate outcome is evaluated.
        """

        self._require_stage(FreshSearchStage.NEW)
        self._consume_role("discovery")
        context = self._context(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
        )
        access = self._append_stage_window_access_record(
            context=context,
            role="discovery",
            purpose=(
                "evaluate the immutable outcome-blind predecessor discovery "
                "threshold and entry bank in the new study"
            ),
        )
        numbers = [int(access["recordNumber"])]
        try:
            if not isinstance(threshold_bank, Mapping):
                raise ValueError("threshold_bank must be a mapping")
            self._threshold_bank_json = _canonical_json(
                threshold_bank,
                "threshold bank",
            )
            self._threshold_bank_sha256 = canonical_hash(
                json.loads(self._threshold_bank_json)
            )
            specs = tuple(entry_specs)
            if not specs or any(
                not isinstance(spec, EntryCandidateSpec) for spec in specs
            ):
                raise ValueError(
                    "entry_specs must contain frozen entry candidate specs"
                )
            if len(specs) > self._budgets.discovery_distinct_candidates:
                raise ValueError("discovery candidate budget exceeded")
            candidates = tuple(
                FrozenEntryCandidate.freeze(
                    spec,
                    threshold_bank_sha256=self._threshold_bank_sha256,
                )
                for spec in specs
            )
            identifiers = [candidate.candidate_id for candidate in candidates]
            if len(identifiers) != len(set(identifiers)):
                raise ValueError("discovery candidate ids must be unique")
            family_counts: dict[str, int] = {}
            for candidate in candidates:
                family_counts[candidate.family] = (
                    family_counts.get(candidate.family, 0) + 1
                )
            if any(
                count > self._budgets.discovery_per_family_maximum
                for count in family_counts.values()
            ):
                raise ValueError("discovery per-family candidate budget exceeded")
            evaluated, evaluation_numbers = self._evaluate_entries(
                candidates=candidates,
                context=context,
                role="discovery",
            )
            numbers.extend(evaluation_numbers)
        except Exception:
            self._stage = FreshSearchStage.FAILED
            raise
        self._entry_pool = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.candidate_id,
            limit=self._budgets.walk_forward_1_frozen_candidates,
        )
        failed = not self._entry_pool
        self._stage = (
            FreshSearchStage.FAILED
            if failed
            else FreshSearchStage.DISCOVERY_COMPLETE
        )
        return StageRunResult(
            stage="discovery",
            evaluated_ids=tuple(
                candidate.candidate_id for candidate, _ in evaluated
            ),
            promoted_ids=tuple(
                candidate.candidate_id for candidate in self._entry_pool
            ),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def resume_discovery(self) -> StageRunResult:
        """Recompute and seal the exact incomplete discovery batch once.

        The original run revealed no candidate result.  This method therefore
        calls the registered batch scorer for every originally frozen candidate,
        validates the complete mapping, seals one immutable result artifact, and
        only then appends individual candidate outcomes in their original order.
        """

        self._require_stage(FreshSearchStage.DISCOVERY_RESUME_AUTHORIZED)
        candidates = self._resume_discovery_candidates
        audit = self._recovery_audit
        batch_scorer = self._callbacks.score_entries_batch
        if not candidates or audit is None or batch_scorer is None:
            self._stage = FreshSearchStage.FAILED
            raise FreshSearchProtocolError(
                "recovery requires the exact frozen candidate batch scorer"
            )
        context = self._context(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
        )
        numbers = [
            int(record["recordNumber"])
            for record in self._records
            if record.get("recordKind") == "infrastructure-resume"
        ]
        started = self._append_resume_protocol_record(
            status="batch_resume_started",
            context=context,
            parameters={
                "recoveryAttemptId": audit["recoveryAttemptId"],
                "originalBatchAccessRecordNumber": 2,
                "originalBatchAccessRecordSha256": self._records[1]["recordSha256"],
                "candidateCount": len(candidates),
                "orderedCandidateSequenceSha256": audit["identity"][
                    "orderedCandidateSequenceSha256"
                ],
            },
            leakage_checks={
                "durableBeforeCallback": True,
                "recomputedFromBatchStart": True,
                "partialMetricsUnavailable": True,
            },
            metrics={"candidateCount": len(candidates)},
        )
        numbers.append(int(started["recordNumber"]))
        try:
            raw_results = batch_scorer(candidates, context)
            if not isinstance(raw_results, Mapping):
                raise ValueError(
                    "recovery batch scorer must return a candidate-id mapping"
                )
            expected_ids = [candidate.candidate_id for candidate in candidates]
            if set(raw_results) != set(expected_ids) or len(raw_results) != len(
                expected_ids
            ):
                raise ValueError(
                    "recovery batch scorer must return every original candidate "
                    "exactly once"
                )
            results: dict[str, CandidateEvaluation] = {}
            for candidate in candidates:
                result = raw_results[candidate.candidate_id]
                if not isinstance(result, CandidateEvaluation):
                    raise ValueError(
                        "recovery entry scorer must return CandidateEvaluation"
                    )
                if result.identity_sha256 != candidate.entry_sha256:
                    raise FrozenIdentityError(
                        f"recovery result for {candidate.candidate_id!r} changed "
                        "the frozen identity"
                    )
                results[candidate.candidate_id] = result
            batch_sha, batch_file_sha = self._seal_recovery_batch_result(
                candidates=candidates,
                results=results,
            )
        except Exception as exc:
            failed = self._append_resume_protocol_record(
                status="batch_resume_error",
                context=context,
                parameters={
                    "recoveryAttemptId": audit["recoveryAttemptId"],
                    "errorType": type(exc).__name__,
                    "candidateOutcomesAppended": 0,
                },
                leakage_checks={
                    "recoveryAttemptConsumed": True,
                    "candidatePromotionForbidden": True,
                },
                metrics={"errorType": type(exc).__name__},
            )
            numbers.append(int(failed["recordNumber"]))
            self._stage = FreshSearchStage.FAILED
            raise

        completed = self._append_resume_protocol_record(
            status="batch_resume_completed",
            context=context,
            parameters={
                "recoveryAttemptId": audit["recoveryAttemptId"],
                "batchResultSha256": batch_sha,
                "batchResultFileSha256": batch_file_sha,
                "candidateCount": len(candidates),
                "orderedCandidateSequenceSha256": audit["identity"][
                    "orderedCandidateSequenceSha256"
                ],
            },
            leakage_checks={
                "completeBatchSealedBeforeCandidateRecords": True,
                "allOriginalCandidatesPresent": True,
                "originalOrderPreserved": True,
            },
            metrics={"candidateCount": len(candidates)},
        )
        numbers.append(int(completed["recordNumber"]))

        evaluated: list[tuple[FrozenEntryCandidate, CandidateEvaluation]] = []
        try:
            for candidate in candidates:
                result = results[candidate.candidate_id]
                record = self._append_record(
                    candidate_id=candidate.candidate_id,
                    family=candidate.family,
                    stage="discovery",
                    role="discovery",
                    context=context,
                    parameters={
                        "entryConfig": candidate.config,
                        "thresholdBankSha256": candidate.threshold_bank_sha256,
                        "recoveryAttemptId": audit["recoveryAttemptId"],
                        "sealedBatchResultSha256": batch_sha,
                    },
                    entry_variant=candidate.entry_variant,
                    exit_variant="entry-edge-only",
                    identity_sha256=candidate.entry_sha256,
                    evaluation=result,
                    outcomes_revealed=True,
                    status="passed" if result.passed else "rejected",
                    frozen_entry_sha256=candidate.entry_sha256,
                )
                numbers.append(int(record["recordNumber"]))
                evaluated.append((candidate, result))
            self._entry_pool = self._rank_passed(
                evaluated,
                identifier=lambda candidate: candidate.candidate_id,
                limit=self._budgets.walk_forward_1_frozen_candidates,
            )
            failed = not self._entry_pool
            stage_record = self._append_resume_protocol_record(
                status="resume_stage_completed",
                context=context,
                parameters={
                    "recoveryAttemptId": audit["recoveryAttemptId"],
                    "batchResultSha256": batch_sha,
                    "candidateCount": len(candidates),
                    "candidateOutcomeRecordCount": len(evaluated),
                    "promotedCandidateIds": [
                        candidate.candidate_id for candidate in self._entry_pool
                    ],
                },
                leakage_checks={
                    "candidateRecordCountExact": len(evaluated) == len(candidates),
                    "promotionAfterCompleteBatchOnly": True,
                    "recoveryAttemptConsumed": True,
                },
                metrics={
                    "candidateCount": len(candidates),
                    "promotedCandidateCount": len(self._entry_pool),
                    "studyFailed": failed,
                },
            )
            numbers.append(int(stage_record["recordNumber"]))
        except Exception:
            self._stage = FreshSearchStage.FAILED
            raise

        self._stage = (
            FreshSearchStage.FAILED if failed else FreshSearchStage.DISCOVERY_COMPLETE
        )
        self._resume_discovery_candidates = ()
        return StageRunResult(
            stage="discovery",
            evaluated_ids=tuple(candidate.candidate_id for candidate, _ in evaluated),
            promoted_ids=tuple(
                candidate.candidate_id for candidate in self._entry_pool
            ),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def _run_entry_test(
        self,
        *,
        required_stage: FreshSearchStage,
        role: str,
        next_stage: FreshSearchStage,
        promotion_limit: int,
    ) -> StageRunResult:
        self._require_stage(required_stage)
        self._consume_role(role)
        context = self._context(
            stage=role,
            training_roles=tuple(_ROLE_ORDER[: _ROLE_ORDER.index(role)]),
            evaluation_roles=(role,),
        )
        evaluated, numbers = self._evaluate_entries(
            candidates=self._entry_pool, context=context, role=role
        )
        self._entry_pool = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.candidate_id,
            limit=promotion_limit,
        )
        failed = not self._entry_pool
        self._stage = FreshSearchStage.FAILED if failed else next_stage
        return StageRunResult(
            stage=role,
            evaluated_ids=tuple(candidate.candidate_id for candidate, _ in evaluated),
            promoted_ids=tuple(
                candidate.candidate_id for candidate in self._entry_pool
            ),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def run_walk_forward_1(self) -> StageRunResult:
        return self._run_entry_test(
            required_stage=FreshSearchStage.DISCOVERY_COMPLETE,
            role="walk_forward_1",
            next_stage=FreshSearchStage.WALK_FORWARD_1_COMPLETE,
            promotion_limit=self._budgets.walk_forward_2_frozen_candidates,
        )

    def run_walk_forward_2(self) -> StageRunResult:
        return self._run_entry_test(
            required_stage=FreshSearchStage.WALK_FORWARD_1_COMPLETE,
            role="walk_forward_2",
            next_stage=FreshSearchStage.WALK_FORWARD_2_COMPLETE,
            promotion_limit=self._budgets.exit_search_frozen_entries,
        )

    def run_exit_search(self) -> StageRunResult:
        self._require_stage(FreshSearchStage.WALK_FORWARD_2_COMPLETE)
        context = self._context(
            stage="exit_search",
            training_roles=("discovery", "walk_forward_1", "walk_forward_2"),
            evaluation_roles=("discovery", "walk_forward_1", "walk_forward_2"),
        )
        specs = tuple(self._callbacks.build_exit_variants(self._entry_pool, context))
        if not specs:
            raise ValueError("exit search must produce at least one strategy candidate")
        if len(specs) > self._budgets.exit_variants_after_entry_gate:
            raise ValueError("exit-search strategy budget exceeded")
        entries_by_id = {entry.candidate_id: entry for entry in self._entry_pool}
        candidates = tuple(
            FrozenStrategyCandidate.freeze(spec, entries_by_id=entries_by_id)
            for spec in specs
        )
        identifiers = [candidate.strategy_id for candidate in candidates]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("strategy ids must be unique")
        evaluated, numbers = self._evaluate_strategies(
            candidates=candidates,
            context=context,
            # Exit search reuses already-consumed discovery through WF2.  Bind
            # the single ledger role to the latest consumed role while the
            # evaluationWindow records the complete reused prefix.
            role="walk_forward_2",
            outcomes_revealed=True,
        )
        self._strategy_pool = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.strategy_id,
            limit=self._budgets.walk_forward_3_full_strategies,
        )
        failed = not self._strategy_pool
        self._stage = (
            FreshSearchStage.FAILED if failed else FreshSearchStage.EXIT_SEARCH_COMPLETE
        )
        return StageRunResult(
            stage="exit_search",
            evaluated_ids=tuple(candidate.strategy_id for candidate, _ in evaluated),
            promoted_ids=tuple(
                candidate.strategy_id for candidate in self._strategy_pool
            ),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def run_walk_forward_3(self) -> StageRunResult:
        self._require_stage(FreshSearchStage.EXIT_SEARCH_COMPLETE)
        self._consume_role("walk_forward_3")
        context = self._context(
            stage="walk_forward_3",
            training_roles=("discovery", "walk_forward_1", "walk_forward_2"),
            evaluation_roles=("walk_forward_3",),
        )
        evaluated, numbers = self._evaluate_strategies(
            candidates=self._strategy_pool,
            context=context,
            role="walk_forward_3",
            outcomes_revealed=True,
        )
        self._strategy_pool = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.strategy_id,
            limit=self._budgets.validation_full_strategies,
        )
        failed = len(self._strategy_pool) != 1
        self._stage = (
            FreshSearchStage.FAILED
            if failed
            else FreshSearchStage.WALK_FORWARD_3_COMPLETE
        )
        return StageRunResult(
            stage="walk_forward_3",
            evaluated_ids=tuple(candidate.strategy_id for candidate, _ in evaluated),
            promoted_ids=tuple(
                candidate.strategy_id for candidate in self._strategy_pool
            ),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def run_validation(self) -> StageRunResult:
        self._require_stage(FreshSearchStage.WALK_FORWARD_3_COMPLETE)
        if len(self._strategy_pool) != 1:
            raise FreshSearchProtocolError("validation requires exactly one strategy")
        self._consume_role("validation")
        context = self._context(
            stage="validation",
            training_roles=(
                "discovery",
                "walk_forward_1",
                "walk_forward_2",
                "walk_forward_3",
            ),
            evaluation_roles=("validation",),
        )
        evaluated, numbers = self._evaluate_strategies(
            candidates=self._strategy_pool,
            context=context,
            role="validation",
            outcomes_revealed=True,
        )
        passed = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.strategy_id,
            limit=1,
        )
        failed = len(passed) != 1
        self._strategy_pool = passed
        self._validation_winner = passed[0] if passed else None
        self._stage = (
            FreshSearchStage.FAILED if failed else FreshSearchStage.VALIDATION_COMPLETE
        )
        return StageRunResult(
            stage="validation",
            evaluated_ids=tuple(candidate.strategy_id for candidate, _ in evaluated),
            promoted_ids=tuple(candidate.strategy_id for candidate in passed),
            ledger_record_numbers=tuple(numbers),
            study_failed=failed,
        )

    def authorize_holdout(
        self, *, explicit_holdout_authorization: bool
    ) -> dict[str, Any]:
        self._require_stage(FreshSearchStage.VALIDATION_COMPLETE)
        if explicit_holdout_authorization is not True:
            raise PermissionError("holdout requires explicit authorization")
        if self._validation_winner is None or len(self._strategy_pool) != 1:
            raise FreshSearchProtocolError(
                "holdout requires one exact passed validation winner"
            )
        winner = self._validation_winner
        if self._callbacks.authorize_holdout is None:
            authorization = authorize_evaluation(
                "holdout",
                split_manifest=self._split_manifest,
                access_records=self._records,
                frozen_strategy_sha256=winner.strategy_sha256,
                explicit_holdout_authorization=True,
            )
        else:
            authorization = self._callbacks.authorize_holdout(
                winner, tuple(self.audit_records), True
            )
        if not isinstance(authorization, Mapping):
            raise PermissionError("holdout authorizer must return a mapping")
        frozen = _json_clone(authorization, "holdout authorization")
        if frozen.get("role") != "holdout":
            raise PermissionError("holdout authorization has the wrong role")
        if frozen.get("frozenStrategySha256") != winner.strategy_sha256:
            raise PermissionError(
                "holdout authorization is not bound to the validation winner"
            )
        if frozen.get("outcomesRevealed") is not False:
            raise PermissionError("holdout authorization must precede outcome access")
        self._holdout_authorization = frozen
        self._stage = FreshSearchStage.HOLDOUT_AUTHORIZED
        return _json_clone(frozen, "holdout authorization")

    def run_holdout(self) -> StageRunResult:
        self._require_stage(FreshSearchStage.HOLDOUT_AUTHORIZED)
        if self._holdout_attempted:
            raise FreshSearchProtocolError(
                "holdout evaluation has already been attempted"
            )
        if self._validation_winner is None or self._holdout_authorization is None:
            raise FreshSearchProtocolError("holdout is not bound and authorized")
        winner = self._validation_winner
        if (
            self._holdout_authorization.get("frozenStrategySha256")
            != winner.strategy_sha256
        ):
            raise FrozenIdentityError(
                "holdout winner identity changed after authorization"
            )
        self._holdout_attempted = True
        self._consume_role("holdout")
        context = self._context(
            stage="holdout",
            training_roles=(
                "discovery",
                "walk_forward_1",
                "walk_forward_2",
                "walk_forward_3",
                "validation",
            ),
            evaluation_roles=("holdout",),
        )
        try:
            evaluated, numbers = self._evaluate_strategies(
                candidates=(winner,),
                context=context,
                role="holdout",
                outcomes_revealed=True,
            )
        finally:
            # Even an exception after authorisation consumes the one holdout.
            self._stage = FreshSearchStage.HOLDOUT_COMPLETE
        passed = tuple(candidate for candidate, result in evaluated if result.passed)
        return StageRunResult(
            stage="holdout",
            evaluated_ids=(winner.strategy_id,),
            promoted_ids=tuple(candidate.strategy_id for candidate in passed),
            ledger_record_numbers=tuple(numbers),
            study_failed=not bool(passed),
        )


__all__ = [
    "BatchEntryScorer",
    "BatchStrategyScorer",
    "CandidateEvaluation",
    "EntryCandidateSpec",
    "EvaluationContext",
    "FreshChronologicalSearch",
    "FreshSearchBudgets",
    "FreshSearchCallbacks",
    "FreshSearchProtocolError",
    "FreshSearchStage",
    "FrozenEntryCandidate",
    "FrozenIdentityError",
    "FrozenResearchWindow",
    "FrozenStrategyCandidate",
    "StageRunResult",
    "StrategyCandidateSpec",
]
