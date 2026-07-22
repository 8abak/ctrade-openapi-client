from __future__ import annotations

import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from datavis.research.fresh_pipeline import (
    BASELINE_MINIMUM_UPLIFT,
    SESSION_CLOSE_SAFETY_MS,
    _baseline_events,
    _bound_discovery_session_count,
    _cluster_entry_edge,
    _diagnose,
    _entry_barrier_value,
    _parameter_neighbourhood_audit,
    _replay_session,
    _research_state_binding,
    _restricted_coverage_ms,
    _scenario_ids_for_stage,
    _snapshot_new_file,
    _strongest_record,
)
from datavis.research.fresh_pipeline_cli import main
from datavis.research.fresh_preregistration import (
    required_fresh_implementation_files,
)
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_session_eval import FreshSessionTape
from datavis.research.fresh_sessions import broker_session_bounds
from datavis.research.ticks import Tick


def result(*, successes_10: int, successes_30: int, count: int):
    diagnostics = []
    for position in range(count):
        diagnostics.append(
            SimpleNamespace(
                event=SimpleNamespace(side="long"),
                censored=False,
                cost_covered_by_10s=position < successes_10,
                cost_covered_by_30s=position < successes_30,
            )
        )
    return SimpleNamespace(diagnostics=tuple(diagnostics))


class FreshPipelineTests(unittest.TestCase):
    def test_durable_snapshot_is_complete_and_never_overwritten(self):
        root = Path(__file__).resolve().parent / "artifacts" / "test-fresh-protocol"
        root.mkdir(parents=True, exist_ok=True)
        token = uuid.uuid4().hex
        source = root / f"{token}-durable.jsonl"
        destination = root / f"{token}-artifact.jsonl"
        self.addCleanup(source.unlink, missing_ok=True)
        self.addCleanup(destination.unlink, missing_ok=True)
        source.write_bytes(b"one\ntwo\n")

        _snapshot_new_file(source, destination)

        self.assertEqual(destination.read_bytes(), source.read_bytes())
        self.assertEqual(list(root.glob(".fresh-snapshot-*.tmp")), [])
        with self.assertRaises(FileExistsError):
            _snapshot_new_file(source, destination)
        self.assertEqual(destination.read_bytes(), b"one\ntwo\n")
        self.assertEqual(list(root.glob(".fresh-snapshot-*.tmp")), [])

    def test_durable_state_paths_are_bound_to_split_and_holdout_identity(self):
        roles = (
            "discovery",
            "walk_forward_1",
            "walk_forward_2",
            "walk_forward_3",
            "validation",
            "holdout",
        )
        body = {
            "schemaVersion": "test",
            "windows": {
                role: {
                    "role": role,
                    "sessionAnchors": [f"2026-07-{ordinal + 1:02d}"],
                }
                for ordinal, role in enumerate(roles)
            },
        }
        split = {**body, "manifestSha256": canonical_hash(body)}

        first = _research_state_binding("durable-state", split)
        second = _research_state_binding("durable-state", split)

        self.assertEqual(first, second)
        self.assertIn(
            first["researchWindowSetSha256"], first["experimentLedgerPath"]
        )
        self.assertIn(
            first["holdoutWindowSha256"],
            first["holdoutAuthorizationRegistryPath"],
        )

    def test_later_stages_report_all_registered_execution_sensitivities(self):
        registered = (
            "mechanics-zero-friction",
            "low-friction",
            "reference-provisional",
            "latency-stress",
            "friction-stress",
        )
        required = ("latency-stress", "friction-stress")

        self.assertEqual(
            _scenario_ids_for_stage(
                registered, required, stage="exit_search"
            ),
            ("reference-provisional", *required),
        )
        self.assertEqual(
            _scenario_ids_for_stage(
                registered, required, stage="validation"
            ),
            registered,
        )

    def test_censored_fills_cannot_count_as_coverage_or_barrier_success(self):
        diagnostic = result(successes_10=1, successes_30=1, count=1)
        diagnostic.diagnostics[0].censored = True
        diagnostic.diagnostics[0].first_barrier_hit = "profit"
        diagnostic.diagnostics[0].time_to_cost_coverage_ms = 250.0

        clustered = _cluster_entry_edge(
            [diagnostic],
            [result(successes_10=0, successes_30=0, count=1)],
            seed_text="censored",
        )

        self.assertEqual(clustered["10"]["coverage"], 0.0)
        self.assertEqual(clustered["30"]["coverage"], 0.0)
        self.assertEqual(_entry_barrier_value(diagnostic.diagnostics[0]), 0.0)
        self.assertEqual(_restricted_coverage_ms(diagnostic.diagnostics[0]), 60_000.0)

    def test_parameter_neighbourhood_requires_center_and_both_neighbors(self):
        passing = (
            ("minus", -0.05, True, 0.08, 0.61, "minus-parameters"),
            ("center", 0.0, True, 0.10, 0.62, "center-parameters"),
            ("plus", 0.05, True, 0.09, 0.60, "plus-parameters"),
        )

        def audit(members):
            return _parameter_neighbourhood_audit(
                members,
                minimum_valid_neighbor_fraction=0.70,
                minimum_positive_expectancy_neighbor_fraction=0.70,
                minimum_neighbor_expectancy_retention=0.75,
                maximum_absolute_coverage_30_drop=0.07,
            )

        self.assertTrue(audit(passing)["passed"])
        center_failed = list(passing)
        center_failed[1] = (
            "center",
            0.0,
            False,
            0.10,
            0.62,
            "center-parameters",
        )
        self.assertFalse(audit(center_failed)["passed"])
        one_neighbor_failed = list(passing)
        one_neighbor_failed[0] = (
            "minus",
            -0.05,
            False,
            0.08,
            0.61,
            "minus-parameters",
        )
        self.assertFalse(audit(one_neighbor_failed)["passed"])
        retention_failed = list(passing)
        retention_failed[0] = (
            "minus",
            -0.05,
            True,
            0.01,
            0.61,
            "minus-parameters",
        )
        retention_failed[2] = (
            "plus",
            0.05,
            True,
            0.20,
            0.60,
            "plus-parameters",
        )
        self.assertFalse(audit(retention_failed)["passed"])
        coverage_failed = list(passing)
        coverage_failed[2] = (
            "plus",
            0.05,
            True,
            0.09,
            0.54,
            "plus-parameters",
        )
        self.assertFalse(audit(coverage_failed)["passed"])

        exit_members = tuple(
            (identifier, offset, passed, expectancy, None, signature)
            for identifier, offset, passed, expectancy, _, signature in passing
        )
        self.assertTrue(
            _parameter_neighbourhood_audit(
                exit_members,
                minimum_valid_neighbor_fraction=0.70,
                minimum_positive_expectancy_neighbor_fraction=0.70,
                minimum_neighbor_expectancy_retention=0.75,
                maximum_absolute_coverage_30_drop=None,
            )["passed"]
        )
        degenerate = list(passing)
        degenerate[0] = (*degenerate[0][:-1], "center-parameters")
        self.assertFalse(audit(degenerate)["passed"])

    def test_strongest_record_uses_the_registered_lexical_tie_break(self):
        records = (
            {"stage": "validation", "balancedScore": 0.5, "candidateId": "z"},
            {"stage": "validation", "balancedScore": 0.5, "candidateId": "a"},
        )

        self.assertEqual(_strongest_record(records)["candidateId"], "a")

    def test_discovery_count_comes_from_bound_preregistration_policy(self):
        preregistration = {
            "chronologicalWindowPolicy": {"discovery_sessions": 40}
        }
        self.assertEqual(_bound_discovery_session_count(preregistration), 40)
        with self.assertRaisesRegex(ValueError, "discovery-session count"):
            _bound_discovery_session_count(
                {"chronologicalWindowPolicy": {"discovery_sessions": 0}}
            )

    def test_direction_matched_cluster_uplift_is_deterministic(self):
        candidates = [result(successes_10=8, successes_30=9, count=10) for _ in range(12)]
        baselines = [result(successes_10=4, successes_30=5, count=10) for _ in range(12)]
        first = _cluster_entry_edge(candidates, baselines, seed_text="frozen")
        second = _cluster_entry_edge(candidates, baselines, seed_text="frozen")
        self.assertEqual(first, second)
        self.assertGreater(first["10"]["uplift"], BASELINE_MINIMUM_UPLIFT)
        self.assertGreater(first["10"]["upliftInterval"][0], 0.0)
        self.assertGreater(first["30"]["upliftInterval"][0], 0.0)

    def test_baseline_is_stratified_and_excludes_session_close_buffer(self):
        bounds = broker_session_bounds("2026-01-02")
        inside = bounds.end_utc - pd.Timedelta(milliseconds=SESSION_CLOSE_SAFETY_MS + 1)
        excluded = bounds.end_utc - pd.Timedelta(milliseconds=SESSION_CLOSE_SAFETY_MS)
        ticks = (
            Tick(1, inside, 100.0, 100.2),
            Tick(2, excluded, 100.1, 100.3),
        )
        tape = FreshSessionTape("2026-01-02", bounds, ticks, "a" * 64)
        frame = pd.DataFrame(
            {
                "tick_id": [1, 2],
                "timestamp": pd.to_datetime([item.timestamp for item in ticks], utc=True),
                "feature_ready": [True, True],
                "gap_detected": [False, False],
            }
        )
        events = _baseline_events(frame, tape)
        self.assertEqual([(item.tick_id, item.side) for item in events], [(1, "long"), (1, "short")])

    def test_diagnostics_trust_only_the_validated_session_tuple(self):
        bounds = broker_session_bounds("2026-01-02")
        ticks = (Tick(1, bounds.start_utc, 100.0, 100.2),)
        tape = FreshSessionTape("2026-01-02", bounds, ticks, "a" * 64)
        sentinel = object()

        with patch(
            "datavis.research.fresh_pipeline.evaluate_frozen_entries",
            return_value=sentinel,
        ) as evaluate:
            actual = _diagnose(tape, (), config=SimpleNamespace())

        self.assertIs(actual, sentinel)
        self.assertIs(evaluate.call_args.args[0], tape.ticks)
        self.assertTrue(evaluate.call_args.kwargs["_trusted_validated_ticks"])

        with self.assertRaisesRegex(TypeError, "FreshSessionTape"):
            _diagnose(SimpleNamespace(ticks=ticks), (), config=SimpleNamespace())

    def test_replay_trusts_only_the_validated_session_tuple(self):
        bounds = broker_session_bounds("2026-01-02")
        ticks = (Tick(1, bounds.start_utc, 100.0, 100.2),)
        tape = FreshSessionTape("2026-01-02", bounds, ticks, "a" * 64)
        sentinel = object()
        decisions = SimpleNamespace()
        config = SimpleNamespace()
        prepared = SimpleNamespace()

        with patch(
            "datavis.research.fresh_pipeline.run_fresh_replay",
            return_value=sentinel,
        ) as replay:
            actual = _replay_session(
                tape,
                decisions,
                config=config,
                prepared_replay_tape=prepared,
            )

        self.assertIs(actual, sentinel)
        self.assertIs(replay.call_args.args[0], tape.ticks)
        self.assertIs(replay.call_args.args[1], decisions)
        self.assertTrue(replay.call_args.kwargs["_trusted_validated_ticks"])
        self.assertIs(
            replay.call_args.kwargs["_prepared_replay_tape"], prepared
        )
        self.assertEqual(replay.call_args.kwargs["boundary"].name, tape.anchor)
        with self.assertRaisesRegex(TypeError, "FreshSessionTape"):
            _replay_session(
                SimpleNamespace(ticks=ticks),
                decisions,
                config=config,
                prepared_replay_tape=prepared,
            )

    def test_manifest_binding_covers_the_complete_runner(self):
        paths = required_fresh_implementation_files()
        self.assertIn("datavis/research/fresh_pipeline.py", paths)
        self.assertIn("datavis/research/fresh_scoring.py", paths)
        self.assertIn("datavis/research/fresh_candidate_grid.py", paths)

    def test_cli_requires_explicit_execute(self):
        with self.assertRaisesRegex(SystemExit, "without --execute"):
            main(["--output-dir", "unused"])
        with self.assertRaisesRegex(SystemExit, "durable --research-state-dir"):
            main(["--output-dir", "unused", "--execute"])


if __name__ == "__main__":
    unittest.main()
