from __future__ import annotations

import hashlib
import json
import unittest
import uuid
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from datavis.research.fresh_protocol import (
    FreshWindowPolicy,
    authorize_evaluation,
    build_fresh_split_manifest,
    canonical_hash,
)
from datavis.research.fresh_search import (
    CandidateEvaluation,
    EntryCandidateSpec,
    FreshChronologicalSearch,
    FreshSearchBudgets,
    FreshSearchCallbacks,
    FreshSearchProtocolError,
    FreshSearchStage,
    FrozenIdentityError,
    StrategyCandidateSpec,
)


def _weekday_anchors(count: int) -> list[str]:
    result: list[str] = []
    current = date(2026, 1, 2)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _split_manifest() -> dict[str, Any]:
    return build_fresh_split_manifest(
        _weekday_anchors(6),
        inventory_sha256="a" * 64,
        excluded_sessions=[{"sessionAnchor": "2026-01-01", "reason": "partial"}],
        policy=FreshWindowPolicy(
            discovery_sessions=1,
            walk_forward_sessions=(1, 1, 1),
            validation_sessions=1,
            holdout_sessions=1,
        ),
    )


BUDGETS = FreshSearchBudgets(
    discovery_distinct_candidates=2,
    discovery_per_family_maximum=2,
    walk_forward_1_frozen_candidates=2,
    walk_forward_2_frozen_candidates=2,
    exit_variants_after_entry_gate=2,
    walk_forward_3_full_strategies=2,
    validation_full_strategies=1,
    holdout_full_strategies=1,
    exit_search_frozen_entries=1,
)


def _entry_specs() -> tuple[EntryCandidateSpec, ...]:
    return (
        EntryCandidateSpec(
            candidate_id="entry-a",
            family="trend-acceleration",
            config={"velocityQuantile": 0.8},
            entry_variant="onset-a",
        ),
        EntryCandidateSpec(
            candidate_id="entry-b",
            family="trend-acceleration",
            config={"velocityQuantile": 0.9},
            entry_variant="onset-b",
        ),
    )


def _successful_entry_batch(candidates, context):
    if context.stage != "discovery":
        raise AssertionError("the discovery batch used a later outcome window")
    return {
        candidate.candidate_id: CandidateEvaluation(
            identity_sha256=candidate.entry_sha256,
            passed=True,
            metrics={
                "entryEdge": (2.0 if candidate.candidate_id == "entry-b" else 1.0)
            },
            leakage_checks={"prefixInvariant": True},
            score=2.0 if candidate.candidate_id == "entry-b" else 1.0,
        )
        for candidate in candidates
    }


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _recovery_audit(
    ledger: Path,
    *,
    split_manifest: dict[str, Any],
    threshold_bank: dict[str, Any],
    preregistration_sha256: str,
) -> dict[str, Any]:
    records = [json.loads(line) for line in ledger.read_text().splitlines()]
    batch_parameters = records[1]["parameters"]
    ordered_identity = [
        {"candidateId": candidate_id, "entrySha256": entry_sha256}
        for candidate_id, entry_sha256 in zip(
            batch_parameters["candidateIds"],
            batch_parameters["candidateSha256"],
        )
    ]
    return {
        "schema": "fresh-xauusd-infrastructure-recovery/v1",
        "recoveryAttemptId": "run-14-oom-attempt-1",
        "recoveryAttempt": 1,
        "maximumRecoveryAttempts": 1,
        "originalRunId": 14,
        "originalCommitSha": "c" * 40,
        "ledgerPrefixSha256": _file_sha256(ledger),
        "originalRecordSha256": [
            records[0]["recordSha256"],
            records[1]["recordSha256"],
        ],
        "candidateOutcomeRecordCount": 0,
        "laterRoleRecordCount": 0,
        "holdoutAuthorizationPresent": False,
        "oomEvidence": {"exitStatus": 137, "candidateOutcomesWritten": 0},
        "identity": {
            "splitManifestSha256": split_manifest["manifestSha256"],
            "preregistrationSha256": preregistration_sha256,
            "thresholdBankSha256": canonical_hash(threshold_bank),
            "orderedCandidateSequenceSha256": canonical_hash(ordered_identity),
            "candidateCount": len(ordered_identity),
        },
        "permittedProcedure": {
            "kind": "recompute-complete-discovery-batch-from-start",
            "candidateChangesAllowed": False,
        },
    }


class _Harness:
    def __init__(self) -> None:
        self.mutable_entry_config = {"velocityQuantile": 0.8}
        self.threshold_calls = 0
        self.builder_calls = 0
        self.signal_stages: list[str] = []
        self.scenario_stages: list[str] = []
        self.fail_entries_on: set[str] = set()
        self.fail_strategies_on: set[str] = set()
        self.wrong_entry_identity_on: set[str] = set()
        self.wrong_strategy_identity_on: set[str] = set()
        self.raise_strategy_on: set[str] = set()

    def fit_thresholds(self, context):
        self.threshold_calls += 1
        self.assert_context(context.stage, "discovery")
        return {"velocity": {"q80": 1.25}}

    def build_entries(self, thresholds, context):
        self.builder_calls += 1
        if thresholds["velocity"]["q80"] != 1.25:
            raise AssertionError("threshold bank changed")
        self.assert_context(context.stage, "discovery")
        return (
            EntryCandidateSpec(
                candidate_id="entry-a",
                family="trend-acceleration",
                config=self.mutable_entry_config,
                entry_variant="onset-a",
            ),
            EntryCandidateSpec(
                candidate_id="entry-b",
                family="trend-acceleration",
                config={"velocityQuantile": 0.9},
                entry_variant="onset-b",
            ),
        )

    def generate_signals(self, candidate, context):
        self.signal_stages.append(context.stage)
        return {"entrySha256": candidate.entry_sha256}

    def score_entry(self, candidate, context, signals):
        if signals["entrySha256"] != candidate.entry_sha256:
            raise AssertionError("wrong signals")
        identity = (
            "f" * 64
            if context.stage in self.wrong_entry_identity_on
            else candidate.entry_sha256
        )
        passed = context.stage not in self.fail_entries_on
        score = 2.0 if candidate.candidate_id == "entry-b" else 1.0
        return CandidateEvaluation(
            identity_sha256=identity,
            passed=passed,
            metrics={"entryEdge": score},
            leakage_checks={"prefixInvariant": True},
            score=score,
        )

    def build_exits(self, entries, context):
        self.assert_context(context.stage, "exit_search")
        if len(entries) != 1 or entries[0].candidate_id != "entry-b":
            raise AssertionError("exit search received an unpromoted entry")
        return (
            StrategyCandidateSpec(
                strategy_id="strategy-a",
                entry_candidate_id="entry-b",
                exit_config={"trail": 0.2},
                execution_config={"scenario": "reference"},
                exit_variant="fixed-trail",
            ),
            StrategyCandidateSpec(
                strategy_id="strategy-b",
                entry_candidate_id="entry-b",
                exit_config={"trail": 0.3},
                execution_config={"scenario": "reference"},
                exit_variant="volatility-trail",
            ),
        )

    def run_scenarios(self, strategy, context, signals):
        self.scenario_stages.append(context.stage)
        if signals["entrySha256"] != strategy.entry.entry_sha256:
            raise AssertionError("strategy used different entry signals")
        if context.stage in self.raise_strategy_on:
            raise RuntimeError("deliberate callback failure")
        return {"strategySha256": strategy.strategy_sha256}

    def score_strategy(self, strategy, context, results):
        if results["strategySha256"] != strategy.strategy_sha256:
            raise AssertionError("wrong scenario results")
        identity = (
            "e" * 64
            if context.stage in self.wrong_strategy_identity_on
            else strategy.strategy_sha256
        )
        passed = context.stage not in self.fail_strategies_on
        score = 2.0 if strategy.strategy_id == "strategy-b" else 1.0
        return CandidateEvaluation(
            identity_sha256=identity,
            passed=passed,
            metrics={"netPnl": score, "profitFactor": 1.2},
            leakage_checks={"causalReplay": True},
            score=score,
        )

    @staticmethod
    def assert_context(actual: str, expected: str) -> None:
        if actual != expected:
            raise AssertionError(f"expected {expected}, received {actual}")

    def callbacks(self, *, authorizer=None) -> FreshSearchCallbacks:
        return FreshSearchCallbacks(
            fit_thresholds=self.fit_thresholds,
            build_entry_candidates=self.build_entries,
            generate_signals=self.generate_signals,
            score_entry=self.score_entry,
            build_exit_variants=self.build_exits,
            run_execution_scenarios=self.run_scenarios,
            score_strategy=self.score_strategy,
            authorize_holdout=authorizer,
        )


class FreshSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = Path(__file__).resolve().parent / "artifacts" / "test-fresh-search"
        directory.mkdir(parents=True, exist_ok=True)
        self.ledger = directory / f"{uuid.uuid4().hex}-ledger.jsonl"
        self.addCleanup(self.ledger.unlink, missing_ok=True)
        self.addCleanup(
            self.ledger.with_name(self.ledger.name + ".lock").unlink,
            missing_ok=True,
        )

    def managed_path(self, suffix: str) -> Path:
        destination = self.ledger.with_name(f"{uuid.uuid4().hex}-{suffix}")
        self.addCleanup(destination.unlink, missing_ok=True)
        self.addCleanup(
            destination.with_name(destination.name + ".tmp").unlink,
            missing_ok=True,
        )
        self.addCleanup(
            destination.with_name(destination.name + ".lock").unlink,
            missing_ok=True,
        )
        return destination

    @staticmethod
    def callbacks_with_entry_batch(harness: _Harness, scorer):
        return replace(harness.callbacks(), score_entries_batch=scorer)

    def create_abrupt_discovery_prefix(
        self,
        *,
        ledger: Path | None = None,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        tuple[EntryCandidateSpec, ...],
        dict[str, Any],
        _Harness,
        list[tuple[str, ...]],
    ]:
        destination = ledger or self.ledger
        manifest = _split_manifest()
        threshold_bank = {"velocity": {"q80": 1.25}}
        preregistration_sha256 = "b" * 64
        harness = _Harness()
        abrupt_calls: list[tuple[str, ...]] = []

        def abrupt_batch(candidates, context):
            self.assertEqual(context.stage, "discovery")
            abrupt_calls.append(
                tuple(candidate.candidate_id for candidate in candidates)
            )
            raise SystemExit("simulated OOM termination")

        engine = FreshChronologicalSearch(
            split_manifest=manifest,
            ledger_path=destination,
            budgets=BUDGETS,
            callbacks=self.callbacks_with_entry_batch(harness, abrupt_batch),
            preregistration_sha256=preregistration_sha256,
        )
        with self.assertRaisesRegex(SystemExit, "OOM termination"):
            engine.run_discovery()

        records = [json.loads(line) for line in destination.read_text().splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(
            [record["status"] for record in records],
            ["window_access_started", "batch_access_started"],
        )
        specs = _entry_specs()
        audit = _recovery_audit(
            destination,
            split_manifest=manifest,
            threshold_bank=threshold_bank,
            preregistration_sha256=preregistration_sha256,
        )
        return manifest, threshold_bank, specs, audit, harness, abrupt_calls

    def engine(
        self, harness: _Harness | None = None
    ) -> tuple[FreshChronologicalSearch, _Harness]:
        selected = harness or _Harness()
        return (
            FreshChronologicalSearch(
                split_manifest=_split_manifest(),
                ledger_path=self.ledger,
                budgets=BUDGETS,
                callbacks=selected.callbacks(),
                preregistration_sha256="b" * 64,
            ),
            selected,
        )

    @staticmethod
    def progress_to_validation(engine: FreshChronologicalSearch) -> None:
        engine.run_discovery()
        engine.run_walk_forward_1()
        engine.run_walk_forward_2()
        engine.run_exit_search()
        engine.run_walk_forward_3()
        engine.run_validation()

    def test_complete_path_is_chronological_frozen_and_audited(self):
        engine, harness = self.engine()
        discovery = engine.run_discovery()
        self.assertEqual(discovery.promoted_ids, ("entry-b", "entry-a"))
        engine.run_walk_forward_1()
        self.assertEqual(
            tuple(candidate.candidate_id for candidate in engine.entry_candidates),
            ("entry-b", "entry-a"),
        )
        engine.run_walk_forward_2()
        engine.run_exit_search()
        engine.run_walk_forward_3()
        validation = engine.run_validation()
        self.assertEqual(validation.promoted_ids, ("strategy-b",))
        winner_sha = engine.validation_winner.strategy_sha256

        authorization = engine.authorize_holdout(explicit_holdout_authorization=True)
        self.assertEqual(authorization["frozenStrategySha256"], winner_sha)
        result = engine.run_holdout()

        self.assertEqual(engine.stage, FreshSearchStage.HOLDOUT_COMPLETE)
        self.assertEqual(result.promoted_ids, ("strategy-b",))
        self.assertEqual(
            engine.consumed_roles,
            (
                "discovery",
                "walk_forward_1",
                "walk_forward_2",
                "walk_forward_3",
                "validation",
                "holdout",
            ),
        )
        self.assertEqual(harness.threshold_calls, 1)
        self.assertEqual(harness.builder_calls, 1)
        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(len(records), 13)
        self.assertEqual(
            [record["recordNumber"] for record in records],
            list(range(1, len(records) + 1)),
        )
        self.assertEqual(records[-1]["role"], "holdout")
        self.assertEqual(records[-1]["frozenStrategySha256"], winner_sha)
        self.assertTrue(records[-1]["outcomesRevealed"])

    def test_walk_forward_2_freezes_exact_explicit_exit_search_entry_count(self):
        engine, _ = self.engine()
        engine.run_discovery()
        engine.run_walk_forward_1()
        self.assertEqual(
            tuple(candidate.candidate_id for candidate in engine.entry_candidates),
            ("entry-b", "entry-a"),
        )

        result = engine.run_walk_forward_2()

        self.assertEqual(result.evaluated_ids, ("entry-b", "entry-a"))
        self.assertEqual(result.promoted_ids, ("entry-b",))
        self.assertEqual(
            tuple(candidate.candidate_id for candidate in engine.entry_candidates),
            ("entry-b",),
        )
        engine.run_exit_search()

    def test_current_protocol_rejects_multiple_exit_search_entries(self):
        with self.assertRaisesRegex(ValueError, "exactly one frozen entry"):
            FreshSearchBudgets(
                discovery_distinct_candidates=2,
                discovery_per_family_maximum=2,
                walk_forward_1_frozen_candidates=2,
                walk_forward_2_frozen_candidates=2,
                exit_variants_after_entry_gate=2,
                walk_forward_3_full_strategies=2,
                validation_full_strategies=1,
                holdout_full_strategies=1,
                exit_search_frozen_entries=2,
            )

    def test_stage_skipping_and_reusing_unseen_window_are_rejected(self):
        engine, _ = self.engine()
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_walk_forward_1()
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_validation()
        engine.run_discovery()
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_discovery()
        engine.run_walk_forward_1()
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_walk_forward_1()
        self.assertEqual(engine.consumed_roles, ("discovery", "walk_forward_1"))

    def test_candidate_config_is_detached_and_cannot_be_tuned_after_discovery(self):
        engine, harness = self.engine()
        engine.run_discovery()
        frozen = next(
            candidate
            for candidate in engine.entry_candidates
            if candidate.candidate_id == "entry-a"
        )
        original_sha = frozen.entry_sha256
        harness.mutable_entry_config["velocityQuantile"] = 0.1
        returned_copy = frozen.config
        returned_copy["velocityQuantile"] = 0.2
        self.assertEqual(frozen.config, {"velocityQuantile": 0.8})
        self.assertEqual(frozen.entry_sha256, original_sha)
        engine.run_walk_forward_1()
        self.assertEqual(harness.builder_calls, 1)

    def test_identity_mismatch_consumes_test_window_and_prevents_retry(self):
        harness = _Harness()
        harness.wrong_entry_identity_on.add("walk_forward_1")
        engine, _ = self.engine(harness)
        engine.run_discovery()
        with self.assertRaises(FrozenIdentityError):
            engine.run_walk_forward_1()
        self.assertEqual(engine.stage, FreshSearchStage.FAILED)
        self.assertEqual(engine.consumed_roles, ("discovery", "walk_forward_1"))
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_walk_forward_1()
        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(records[-1]["status"], "evaluation_error")
        self.assertFalse(records[-1]["gatePassed"])

    def test_failed_gate_cannot_be_rescued_by_later_stage(self):
        harness = _Harness()
        harness.fail_entries_on.add("walk_forward_2")
        engine, _ = self.engine(harness)
        engine.run_discovery()
        engine.run_walk_forward_1()
        result = engine.run_walk_forward_2()
        self.assertTrue(result.study_failed)
        self.assertEqual(engine.stage, FreshSearchStage.FAILED)
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_exit_search()

    def test_holdout_is_blocked_until_exact_validation_winner_is_authorized(self):
        engine, _ = self.engine()
        with self.assertRaises(FreshSearchProtocolError):
            engine.authorize_holdout(explicit_holdout_authorization=True)
        engine.run_discovery()
        engine.run_walk_forward_1()
        engine.run_walk_forward_2()
        engine.run_exit_search()
        engine.run_walk_forward_3()
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_holdout()
        engine.run_validation()
        with self.assertRaises(PermissionError):
            engine.authorize_holdout(explicit_holdout_authorization=False)

        def wrong_authorizer(strategy, records, explicit):
            self.assertTrue(explicit)
            self.assertTrue(records)
            return {
                "role": "holdout",
                "frozenStrategySha256": "c" * 64,
                "outcomesRevealed": False,
            }

        # A separate engine demonstrates that an external preregistration
        # authorizer must bind the exact winner hash.
        other_ledger = self.ledger.with_name(f"{uuid.uuid4().hex}-ledger.jsonl")
        self.addCleanup(other_ledger.unlink, missing_ok=True)
        harness = _Harness()
        other = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=other_ledger,
            budgets=BUDGETS,
            callbacks=harness.callbacks(authorizer=wrong_authorizer),
        )
        self.progress_to_validation(other)
        with self.assertRaisesRegex(PermissionError, "validation winner"):
            other.authorize_holdout(explicit_holdout_authorization=True)

    def test_holdout_is_exactly_once_even_when_execution_callback_crashes(self):
        harness = _Harness()
        engine, _ = self.engine(harness)
        self.progress_to_validation(engine)
        engine.authorize_holdout(explicit_holdout_authorization=True)
        harness.raise_strategy_on.add("holdout")
        with self.assertRaisesRegex(RuntimeError, "deliberate"):
            engine.run_holdout()
        self.assertEqual(engine.stage, FreshSearchStage.HOLDOUT_COMPLETE)
        self.assertEqual(engine.consumed_roles[-1], "holdout")
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_holdout()

    def test_validation_identity_change_blocks_holdout(self):
        harness = _Harness()
        harness.wrong_strategy_identity_on.add("validation")
        engine, _ = self.engine(harness)
        engine.run_discovery()
        engine.run_walk_forward_1()
        engine.run_walk_forward_2()
        engine.run_exit_search()
        engine.run_walk_forward_3()
        with self.assertRaises(FrozenIdentityError):
            engine.run_validation()
        self.assertEqual(engine.stage, FreshSearchStage.FAILED)
        self.assertEqual(engine.consumed_roles[-1], "validation")
        with self.assertRaises(FreshSearchProtocolError):
            engine.authorize_holdout(explicit_holdout_authorization=True)

    def test_split_manifest_tampering_or_nonchronology_is_rejected(self):
        manifest = _split_manifest()
        tampered = json.loads(json.dumps(manifest))
        tampered["windows"]["holdout"]["sessionAnchors"] = ["2025-01-01"]
        with self.assertRaisesRegex(ValueError, "manifest hash"):
            FreshChronologicalSearch(
                split_manifest=tampered,
                ledger_path=self.ledger,
                budgets=BUDGETS,
                callbacks=_Harness().callbacks(),
            )

        unhashed = json.loads(json.dumps(manifest))
        unhashed.pop("manifestSha256")
        validation_anchor = unhashed["windows"]["validation"]["sessionAnchors"][0]
        unhashed["windows"]["holdout"].update(
            sessionAnchors=[validation_anchor],
            firstSessionAnchor=validation_anchor,
            lastSessionAnchor=validation_anchor,
        )
        with self.assertRaisesRegex(ValueError, "chronological"):
            FreshChronologicalSearch(
                split_manifest=unhashed,
                ledger_path=self.ledger,
                budgets=BUDGETS,
                callbacks=_Harness().callbacks(),
            )

    def test_optional_batch_scorers_run_once_and_preserve_frozen_identities(self):
        harness = _Harness()
        entry_calls: list[tuple[str, ...]] = []
        strategy_calls: list[tuple[str, ...]] = []

        def batch_entries(candidates, context):
            entry_calls.append(
                tuple(candidate.candidate_id for candidate in candidates)
            )
            return {
                candidate.candidate_id: CandidateEvaluation(
                    identity_sha256=candidate.entry_sha256,
                    passed=True,
                    metrics={"stage": context.stage},
                    leakage_checks={"batched": True},
                    score=2.0 if candidate.candidate_id == "entry-b" else 1.0,
                )
                for candidate in candidates
            }

        def batch_strategies(candidates, context):
            strategy_calls.append(
                tuple(candidate.strategy_id for candidate in candidates)
            )
            return {
                candidate.strategy_id: CandidateEvaluation(
                    identity_sha256=candidate.strategy_sha256,
                    passed=True,
                    metrics={"stage": context.stage},
                    leakage_checks={"batched": True},
                    score=2.0 if candidate.strategy_id == "strategy-b" else 1.0,
                )
                for candidate in candidates
            }

        base = harness.callbacks()
        callbacks = FreshSearchCallbacks(
            fit_thresholds=base.fit_thresholds,
            build_entry_candidates=base.build_entry_candidates,
            generate_signals=lambda *_: (_ for _ in ()).throw(
                AssertionError("individual signal path must not run")
            ),
            score_entry=base.score_entry,
            build_exit_variants=base.build_exit_variants,
            run_execution_scenarios=lambda *_: (_ for _ in ()).throw(
                AssertionError("individual strategy path must not run")
            ),
            score_strategy=base.score_strategy,
            score_entries_batch=batch_entries,
            score_strategies_batch=batch_strategies,
        )
        engine = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=callbacks,
        )
        self.progress_to_validation(engine)
        self.assertEqual(len(entry_calls), 3)
        self.assertEqual(len(strategy_calls), 3)
        self.assertEqual(entry_calls[0], ("entry-a", "entry-b"))

    def test_batch_entry_failure_is_durably_marked_before_and_after_callback(self):
        harness = _Harness()
        base = harness.callbacks()

        def fail_batch(candidates, context):
            self.assertEqual(context.stage, "discovery")
            self.assertEqual(
                tuple(candidate.candidate_id for candidate in candidates),
                ("entry-a", "entry-b"),
            )
            raise RuntimeError("deliberate batch entry failure")

        callbacks = FreshSearchCallbacks(
            fit_thresholds=base.fit_thresholds,
            build_entry_candidates=base.build_entry_candidates,
            generate_signals=base.generate_signals,
            score_entry=base.score_entry,
            build_exit_variants=base.build_exit_variants,
            run_execution_scenarios=base.run_execution_scenarios,
            score_strategy=base.score_strategy,
            score_entries_batch=fail_batch,
        )
        engine = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=callbacks,
        )

        with self.assertRaisesRegex(RuntimeError, "batch entry failure"):
            engine.run_discovery()

        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(
            [record["status"] for record in records],
            [
                "window_access_started",
                "batch_access_started",
                "batch_access_error",
            ],
        )
        self.assertTrue(all(record["outcomesRevealed"] for record in records))
        self.assertEqual(records[0]["recordKind"], "stage-window-access")
        self.assertEqual(records[1]["recordKind"], "batch-window-access")
        self.assertEqual(records[2]["role"], "discovery")
        self.assertEqual(records[2]["metrics"]["errorType"], "RuntimeError")
        self.assertEqual(engine.stage, FreshSearchStage.FAILED)
        self.assertEqual(engine.consumed_roles, ("discovery",))

    def test_batch_strategy_failure_durably_consumes_unseen_window(self):
        harness = _Harness()
        base = harness.callbacks()

        def batch_strategies(candidates, context):
            if context.stage == "walk_forward_3":
                raise RuntimeError("deliberate batch strategy failure")
            return {
                candidate.strategy_id: CandidateEvaluation(
                    identity_sha256=candidate.strategy_sha256,
                    passed=True,
                    metrics={"stage": context.stage},
                    leakage_checks={"batched": True},
                    score=2.0 if candidate.strategy_id == "strategy-b" else 1.0,
                )
                for candidate in candidates
            }

        callbacks = FreshSearchCallbacks(
            fit_thresholds=base.fit_thresholds,
            build_entry_candidates=base.build_entry_candidates,
            generate_signals=base.generate_signals,
            score_entry=base.score_entry,
            build_exit_variants=base.build_exit_variants,
            run_execution_scenarios=base.run_execution_scenarios,
            score_strategy=base.score_strategy,
            score_strategies_batch=batch_strategies,
        )
        engine = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=callbacks,
        )
        engine.run_discovery()
        engine.run_walk_forward_1()
        engine.run_walk_forward_2()
        engine.run_exit_search()

        with self.assertRaisesRegex(RuntimeError, "batch strategy failure"):
            engine.run_walk_forward_3()

        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(
            [record["status"] for record in records[-2:]],
            ["batch_access_started", "batch_access_error"],
        )
        self.assertTrue(records[-2]["outcomesRevealed"])
        self.assertTrue(records[-1]["outcomesRevealed"])
        self.assertEqual(records[-1]["role"], "walk_forward_3")
        self.assertEqual(records[-1]["metrics"]["errorType"], "RuntimeError")
        self.assertEqual(engine.stage, FreshSearchStage.FAILED)
        self.assertEqual(engine.consumed_roles[-1], "walk_forward_3")
        with self.assertRaises(FreshSearchProtocolError):
            engine.run_walk_forward_3()

    def test_abrupt_batch_stop_durably_consumes_the_window(self):
        harness = _Harness()
        base = harness.callbacks()

        def abrupt_stop(_candidates, _context):
            raise SystemExit("simulated abrupt stop")

        callbacks = FreshSearchCallbacks(
            fit_thresholds=base.fit_thresholds,
            build_entry_candidates=base.build_entry_candidates,
            generate_signals=base.generate_signals,
            score_entry=base.score_entry,
            build_exit_variants=base.build_exit_variants,
            run_execution_scenarios=base.run_execution_scenarios,
            score_strategy=base.score_strategy,
            score_entries_batch=abrupt_stop,
        )
        engine = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=callbacks,
        )

        with self.assertRaisesRegex(SystemExit, "abrupt stop"):
            engine.run_discovery()

        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(
            [record["status"] for record in records],
            ["window_access_started", "batch_access_started"],
        )
        self.assertTrue(all(record["outcomesRevealed"] for record in records))
        with self.assertRaisesRegex(PermissionError, "already been consumed"):
            authorize_evaluation(
                "discovery",
                split_manifest=_split_manifest(),
                access_records=records,
            )

    def test_audited_resume_matches_an_uninterrupted_discovery_batch(self):
        normal_ledger = self.managed_path("normal-ledger.jsonl")
        normal_harness = _Harness()
        normal_calls: list[tuple[str, ...]] = []

        def normal_batch(candidates, context):
            normal_calls.append(
                tuple(candidate.candidate_id for candidate in candidates)
            )
            return _successful_entry_batch(candidates, context)

        normal = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=normal_ledger,
            budgets=BUDGETS,
            callbacks=self.callbacks_with_entry_batch(normal_harness, normal_batch),
            preregistration_sha256="b" * 64,
        )
        uninterrupted = normal.run_discovery()

        (
            manifest,
            threshold_bank,
            specs,
            audit,
            recovery_harness,
            abrupt_calls,
        ) = self.create_abrupt_discovery_prefix()
        resumed_calls: list[tuple[str, ...]] = []

        def resumed_batch(candidates, context):
            resumed_calls.append(
                tuple(candidate.candidate_id for candidate in candidates)
            )
            return _successful_entry_batch(candidates, context)

        batch_result = self.managed_path("recovery-batch.json")
        resumed = FreshChronologicalSearch.resume_incomplete_discovery(
            split_manifest=manifest,
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=self.callbacks_with_entry_batch(recovery_harness, resumed_batch),
            preregistration_sha256="b" * 64,
            threshold_bank=threshold_bank,
            entry_specs=specs,
            recovery_audit=audit,
            recovery_batch_result_path=batch_result,
        )

        # Reconstruction is identity-only: it must not refit or rebuild using
        # outcomes which the failed process had already opened.
        self.assertEqual(recovery_harness.threshold_calls, 1)
        self.assertEqual(recovery_harness.builder_calls, 1)
        self.assertEqual(resumed.stage, FreshSearchStage.DISCOVERY_RESUME_AUTHORIZED)
        recovered = resumed.resume_discovery()
        self.assertEqual(recovery_harness.threshold_calls, 1)
        self.assertEqual(recovery_harness.builder_calls, 1)

        self.assertEqual(abrupt_calls, [("entry-a", "entry-b")])
        self.assertEqual(resumed_calls, abrupt_calls)
        self.assertEqual(normal_calls, abrupt_calls)
        self.assertEqual(recovered.evaluated_ids, uninterrupted.evaluated_ids)
        self.assertEqual(recovered.promoted_ids, uninterrupted.promoted_ids)
        self.assertEqual(recovered.promoted_ids, ("entry-b", "entry-a"))
        self.assertEqual(resumed.stage, FreshSearchStage.DISCOVERY_COMPLETE)
        self.assertEqual(resumed.consumed_roles, ("discovery",))
        self.assertEqual(
            tuple(candidate.entry_sha256 for candidate in resumed.entry_candidates),
            tuple(candidate.entry_sha256 for candidate in normal.entry_candidates),
        )

        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(
            [record["status"] for record in records],
            [
                "window_access_started",
                "batch_access_started",
                "resume_eligibility_audit",
                "resume_authorized",
                "resume_identity_verified",
                "batch_resume_started",
                "batch_resume_completed",
                "passed",
                "passed",
                "resume_stage_completed",
            ],
        )
        recovery_records = [
            record
            for record in records
            if record.get("recordKind") == "infrastructure-resume"
        ]
        self.assertEqual(
            [record["status"] for record in recovery_records],
            [
                "resume_eligibility_audit",
                "resume_authorized",
                "resume_identity_verified",
                "batch_resume_started",
                "batch_resume_completed",
                "resume_stage_completed",
            ],
        )
        self.assertTrue(all(record["outcomesRevealed"] for record in recovery_records))
        self.assertTrue(all(not record["gatePassed"] for record in recovery_records))

        normal_records = [
            record
            for record in (
                json.loads(line) for line in normal_ledger.read_text().splitlines()
            )
            if record["candidateId"] in {"entry-a", "entry-b"}
        ]
        recovered_records = [
            record
            for record in records
            if record["candidateId"] in {"entry-a", "entry-b"}
        ]
        self.assertEqual(
            [record["candidateId"] for record in recovered_records],
            ["entry-a", "entry-b"],
        )
        ignored = {"recordNumber", "recordSha256", "parameters"}
        self.assertEqual(
            [
                {key: value for key, value in record.items() if key not in ignored}
                for record in recovered_records
            ],
            [
                {key: value for key, value in record.items() if key not in ignored}
                for record in normal_records
            ],
        )
        for normal_record, recovered_record in zip(normal_records, recovered_records):
            self.assertEqual(
                recovered_record["parameters"]["entryConfig"],
                normal_record["parameters"]["entryConfig"],
            )
            self.assertEqual(
                recovered_record["parameters"]["thresholdBankSha256"],
                normal_record["parameters"]["thresholdBankSha256"],
            )

        artifact_bytes = batch_result.read_bytes()
        artifact = json.loads(artifact_bytes)
        artifact_body = dict(artifact)
        claimed_batch_sha = artifact_body.pop("batchResultSha256")
        self.assertEqual(canonical_hash(artifact_body), claimed_batch_sha)
        self.assertEqual(
            [item["candidateId"] for item in artifact["orderedResults"]],
            ["entry-a", "entry-b"],
        )
        completed = next(
            record
            for record in recovery_records
            if record["status"] == "batch_resume_completed"
        )
        completed_parameters = completed["parameters"]["parameters"]
        self.assertEqual(completed_parameters["batchResultSha256"], claimed_batch_sha)
        self.assertEqual(
            completed_parameters["batchResultFileSha256"],
            hashlib.sha256(artifact_bytes).hexdigest(),
        )

        # Completion is one-way and cannot rewrite the sealed result.
        with self.assertRaises(FreshSearchProtocolError):
            resumed.resume_discovery()
        self.assertEqual(batch_result.read_bytes(), artifact_bytes)

    def test_resume_identity_changes_fail_without_appending_to_the_prefix(self):
        (
            manifest,
            threshold_bank,
            specs,
            audit,
            harness,
            _,
        ) = self.create_abrupt_discovery_prefix()
        prefix = self.ledger.read_bytes()
        changed_manifest = json.loads(json.dumps(manifest))
        changed_manifest["inventorySha256"] = "d" * 64
        manifest_body = {
            key: value
            for key, value in changed_manifest.items()
            if key != "manifestSha256"
        }
        changed_manifest["manifestSha256"] = canonical_hash(manifest_body)
        changed_audit = json.loads(json.dumps(audit))
        changed_audit["identity"]["candidateCount"] = 3

        cases = (
            (
                "split manifest",
                {"split_manifest": changed_manifest},
                "recovery identities",
            ),
            (
                "preregistration",
                {"preregistration_sha256": "e" * 64},
                "preregistration identity",
            ),
            (
                "threshold bank",
                {"threshold_bank": {"velocity": {"q80": 1.250001}}},
                "reconstructed candidates",
            ),
            (
                "candidate order",
                {"entry_specs": tuple(reversed(specs))},
                "reconstructed candidates",
            ),
            (
                "audited identity",
                {"recovery_audit": changed_audit},
                "recovery identities",
            ),
        )
        for label, override, expected_error in cases:
            with self.subTest(label=label):
                arguments = {
                    "split_manifest": manifest,
                    "ledger_path": self.ledger,
                    "budgets": BUDGETS,
                    "callbacks": self.callbacks_with_entry_batch(
                        harness, _successful_entry_batch
                    ),
                    "preregistration_sha256": "b" * 64,
                    "threshold_bank": threshold_bank,
                    "entry_specs": specs,
                    "recovery_audit": audit,
                    "recovery_batch_result_path": self.managed_path(
                        f"{label.replace(' ', '-')}-batch.json"
                    ),
                }
                arguments.update(override)
                with self.assertRaisesRegex(PermissionError, expected_error):
                    FreshChronologicalSearch.resume_incomplete_discovery(**arguments)
                self.assertEqual(self.ledger.read_bytes(), prefix)
        self.assertEqual(harness.threshold_calls, 1)
        self.assertEqual(harness.builder_calls, 1)

    def test_semantically_tampered_two_record_prefix_is_not_recoverable(self):
        (
            manifest,
            threshold_bank,
            specs,
            _,
            harness,
            _,
        ) = self.create_abrupt_discovery_prefix()
        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        records[0]["status"] = "window_access_completed"
        body = {
            key: value
            for key, value in records[0].items()
            if key not in {"recordNumber", "recordSha256"}
        }
        records[0]["recordSha256"] = canonical_hash(body)
        self.ledger.write_text(
            "".join(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
        audit = _recovery_audit(
            self.ledger,
            split_manifest=manifest,
            threshold_bank=threshold_bank,
            preregistration_sha256="b" * 64,
        )

        with self.assertRaisesRegex(PermissionError, "stage-access record"):
            FreshChronologicalSearch.resume_incomplete_discovery(
                split_manifest=manifest,
                ledger_path=self.ledger,
                budgets=BUDGETS,
                callbacks=self.callbacks_with_entry_batch(
                    harness, _successful_entry_batch
                ),
                preregistration_sha256="b" * 64,
                threshold_bank=threshold_bank,
                entry_specs=specs,
                recovery_audit=audit,
                recovery_batch_result_path=self.managed_path(
                    "tampered-prefix-batch.json"
                ),
            )

    def test_existing_recovery_batch_artifact_fails_closed(self):
        (
            manifest,
            threshold_bank,
            specs,
            audit,
            harness,
            _,
        ) = self.create_abrupt_discovery_prefix()
        prefix = self.ledger.read_bytes()
        batch_result = self.managed_path("existing-recovery-batch.json")
        sentinel = b"do-not-overwrite\n"
        batch_result.write_bytes(sentinel)

        with self.assertRaisesRegex(PermissionError, "already exists"):
            FreshChronologicalSearch.resume_incomplete_discovery(
                split_manifest=manifest,
                ledger_path=self.ledger,
                budgets=BUDGETS,
                callbacks=self.callbacks_with_entry_batch(
                    harness, _successful_entry_batch
                ),
                preregistration_sha256="b" * 64,
                threshold_bank=threshold_bank,
                entry_specs=specs,
                recovery_audit=audit,
                recovery_batch_result_path=batch_result,
            )
        self.assertEqual(self.ledger.read_bytes(), prefix)
        self.assertEqual(batch_result.read_bytes(), sentinel)

    def test_bad_recovery_batch_is_terminal_and_cannot_be_retried(self):
        def missing_result(candidates, context):
            results = _successful_entry_batch(candidates, context)
            results.pop("entry-b")
            return results

        def changed_identity(candidates, context):
            results = _successful_entry_batch(candidates, context)
            original = results["entry-b"]
            results["entry-b"] = CandidateEvaluation(
                identity_sha256="f" * 64,
                passed=original.passed,
                metrics=original.metrics,
                leakage_checks=original.leakage_checks,
                score=original.score,
            )
            return results

        for label, scorer, error_type in (
            ("missing-result", missing_result, ValueError),
            ("changed-identity", changed_identity, FrozenIdentityError),
        ):
            with self.subTest(label=label):
                ledger = self.managed_path(f"{label}-ledger.jsonl")
                (
                    manifest,
                    threshold_bank,
                    specs,
                    audit,
                    harness,
                    _,
                ) = self.create_abrupt_discovery_prefix(ledger=ledger)
                batch_result = self.managed_path(f"{label}-batch.json")
                callbacks = self.callbacks_with_entry_batch(harness, scorer)
                resumed = FreshChronologicalSearch.resume_incomplete_discovery(
                    split_manifest=manifest,
                    ledger_path=ledger,
                    budgets=BUDGETS,
                    callbacks=callbacks,
                    preregistration_sha256="b" * 64,
                    threshold_bank=threshold_bank,
                    entry_specs=specs,
                    recovery_audit=audit,
                    recovery_batch_result_path=batch_result,
                )

                with self.assertRaises(error_type):
                    resumed.resume_discovery()
                self.assertEqual(resumed.stage, FreshSearchStage.FAILED)
                self.assertFalse(batch_result.exists())
                records = [json.loads(line) for line in ledger.read_text().splitlines()]
                self.assertEqual(
                    [record["status"] for record in records[-2:]],
                    ["batch_resume_started", "batch_resume_error"],
                )
                self.assertEqual(
                    [
                        record["candidateId"]
                        for record in records
                        if record["candidateId"] in {"entry-a", "entry-b"}
                    ],
                    [],
                )
                self.assertEqual(
                    records[-1]["parameters"]["parameters"][
                        "candidateOutcomesAppended"
                    ],
                    0,
                )

                # Both the in-memory state and the durable ledger make the
                # single audited attempt terminal.
                with self.assertRaises(FreshSearchProtocolError):
                    resumed.resume_discovery()
                with self.assertRaisesRegex(PermissionError, "two-record ledger"):
                    FreshChronologicalSearch.resume_incomplete_discovery(
                        split_manifest=manifest,
                        ledger_path=ledger,
                        budgets=BUDGETS,
                        callbacks=callbacks,
                        preregistration_sha256="b" * 64,
                        threshold_bank=threshold_bank,
                        entry_specs=specs,
                        recovery_audit=audit,
                        recovery_batch_result_path=batch_result,
                    )

    def test_threshold_fit_is_durably_consumed_before_the_callback(self):
        harness = _Harness()
        base = harness.callbacks()

        def abrupt_fit(_context):
            raise SystemExit("simulated threshold-fit stop")

        callbacks = FreshSearchCallbacks(
            fit_thresholds=abrupt_fit,
            build_entry_candidates=base.build_entry_candidates,
            generate_signals=base.generate_signals,
            score_entry=base.score_entry,
            build_exit_variants=base.build_exit_variants,
            run_execution_scenarios=base.run_execution_scenarios,
            score_strategy=base.score_strategy,
        )
        engine = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=callbacks,
        )

        with self.assertRaisesRegex(SystemExit, "threshold-fit stop"):
            engine.run_discovery()

        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["recordKind"], "stage-window-access")
        self.assertEqual(records[0]["status"], "window_access_started")
        self.assertTrue(records[0]["outcomesRevealed"])

    def test_exit_search_truthfully_records_reused_consumed_outcomes(self):
        engine, _ = self.engine()
        engine.run_discovery()
        engine.run_walk_forward_1()
        engine.run_walk_forward_2()
        engine.run_exit_search()

        records = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        exit_records = [
            record for record in records if record["stage"] == "exit_search"
        ]
        self.assertEqual(len(exit_records), 2)
        self.assertTrue(all(record["outcomesRevealed"] for record in exit_records))
        self.assertTrue(
            all(record["role"] == "walk_forward_2" for record in exit_records)
        )
        self.assertTrue(
            all(
                record["evaluationWindow"] == "discovery+walk_forward_1+walk_forward_2"
                for record in exit_records
            )
        )

    def test_batch_result_must_cover_every_requested_identity(self):
        harness = _Harness()
        base = harness.callbacks()
        callbacks = FreshSearchCallbacks(
            fit_thresholds=base.fit_thresholds,
            build_entry_candidates=base.build_entry_candidates,
            generate_signals=base.generate_signals,
            score_entry=base.score_entry,
            build_exit_variants=base.build_exit_variants,
            run_execution_scenarios=base.run_execution_scenarios,
            score_strategy=base.score_strategy,
            score_entries_batch=lambda candidates, context: {},
        )
        engine = FreshChronologicalSearch(
            split_manifest=_split_manifest(),
            ledger_path=self.ledger,
            budgets=BUDGETS,
            callbacks=callbacks,
        )
        with self.assertRaisesRegex(ValueError, "every requested candidate"):
            engine.run_discovery()
        self.assertEqual(engine.stage, FreshSearchStage.FAILED)


if __name__ == "__main__":
    unittest.main()
