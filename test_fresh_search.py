from __future__ import annotations

import json
import unittest
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from datavis.research.fresh_protocol import (
    FreshWindowPolicy,
    build_fresh_split_manifest,
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
    walk_forward_2_frozen_candidates=1,
    exit_variants_after_entry_gate=2,
    walk_forward_3_full_strategies=2,
    validation_full_strategies=1,
    holdout_full_strategies=1,
)


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

    def engine(self, harness: _Harness | None = None) -> tuple[FreshChronologicalSearch, _Harness]:
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
            ("entry-b",),
        )
        engine.run_walk_forward_2()
        engine.run_exit_search()
        engine.run_walk_forward_3()
        validation = engine.run_validation()
        self.assertEqual(validation.promoted_ids, ("strategy-b",))
        winner_sha = engine.validation_winner.strategy_sha256

        authorization = engine.authorize_holdout(
            explicit_holdout_authorization=True
        )
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
        self.assertEqual([record["recordNumber"] for record in records], list(range(1, 12)))
        self.assertEqual(records[-1]["role"], "holdout")
        self.assertEqual(records[-1]["frozenStrategySha256"], winner_sha)
        self.assertTrue(records[-1]["outcomesRevealed"])

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
            entry_calls.append(tuple(candidate.candidate_id for candidate in candidates))
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
            strategy_calls.append(tuple(candidate.strategy_id for candidate in candidates))
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
