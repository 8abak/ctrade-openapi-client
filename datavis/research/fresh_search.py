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

import json
import math
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

    def __post_init__(self) -> None:
        values = tuple(self.__dict__.values()) if hasattr(self, "__dict__") else (
            self.discovery_distinct_candidates,
            self.discovery_per_family_maximum,
            self.walk_forward_1_frozen_candidates,
            self.walk_forward_2_frozen_candidates,
            self.exit_variants_after_entry_gate,
            self.walk_forward_3_full_strategies,
            self.validation_full_strategies,
            self.holdout_full_strategies,
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
            raise ValueError("walk-forward 2 budget cannot exceed walk-forward 1 budget")
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
ScenarioRunner = Callable[
    [FrozenStrategyCandidate, EvaluationContext, Any], Any
]
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
        if self.authorize_holdout is not None and not callable(
            self.authorize_holdout
        ):
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
        body = {key: value for key, value in materialized.items() if key != "manifestSha256"}
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
    ) -> None:
        if not isinstance(budgets, FreshSearchBudgets):
            raise ValueError("budgets must be FreshSearchBudgets")
        if not isinstance(callbacks, FreshSearchCallbacks):
            raise ValueError("callbacks must be FreshSearchCallbacks")
        destination = Path(ledger_path).expanduser().resolve()
        if destination.exists() and destination.stat().st_size:
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
            except Exception:
                self._stage = FreshSearchStage.FAILED
                raise
        for candidate in selected:
            try:
                if batch_results is None:
                    signals = self._callbacks.generate_signals(candidate, context)
                    result = self._callbacks.score_entry(candidate, context, signals)
                else:
                    result = batch_results[candidate.candidate_id]
                if not isinstance(result, CandidateEvaluation):
                    raise ValueError(
                        "entry scorer must return CandidateEvaluation"
                    )
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
            except Exception:
                self._stage = FreshSearchStage.FAILED
                raise
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
                    raise ValueError(
                        "strategy scorer must return CandidateEvaluation"
                    )
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
                family_counts[candidate.family] = family_counts.get(candidate.family, 0) + 1
            if any(
                count > self._budgets.discovery_per_family_maximum
                for count in family_counts.values()
            ):
                raise ValueError("discovery per-family candidate budget exceeded")
            evaluated, numbers = self._evaluate_entries(
                candidates=candidates, context=context, role="discovery"
            )
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
            evaluated_ids=tuple(candidate.candidate_id for candidate, _ in evaluated),
            promoted_ids=tuple(candidate.candidate_id for candidate in self._entry_pool),
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
            promoted_ids=tuple(candidate.candidate_id for candidate in self._entry_pool),
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
            promotion_limit=self._budgets.walk_forward_2_frozen_candidates,
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
            role=None,
            outcomes_revealed=False,
        )
        self._strategy_pool = self._rank_passed(
            evaluated,
            identifier=lambda candidate: candidate.strategy_id,
            limit=self._budgets.walk_forward_3_full_strategies,
        )
        failed = not self._strategy_pool
        self._stage = (
            FreshSearchStage.FAILED
            if failed
            else FreshSearchStage.EXIT_SEARCH_COMPLETE
        )
        return StageRunResult(
            stage="exit_search",
            evaluated_ids=tuple(candidate.strategy_id for candidate, _ in evaluated),
            promoted_ids=tuple(candidate.strategy_id for candidate in self._strategy_pool),
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
            promoted_ids=tuple(candidate.strategy_id for candidate in self._strategy_pool),
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
            raise FreshSearchProtocolError("holdout evaluation has already been attempted")
        if self._validation_winner is None or self._holdout_authorization is None:
            raise FreshSearchProtocolError("holdout is not bound and authorized")
        winner = self._validation_winner
        if self._holdout_authorization.get("frozenStrategySha256") != winner.strategy_sha256:
            raise FrozenIdentityError("holdout winner identity changed after authorization")
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
