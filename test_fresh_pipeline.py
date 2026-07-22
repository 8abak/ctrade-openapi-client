from __future__ import annotations

import gc
import shutil
import unittest
import uuid
import weakref
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from datavis.research.fresh_pipeline import (
    BASELINE_MINIMUM_UPLIFT,
    SESSION_CLOSE_SAFETY_MS,
    RegisteredFreshResearchPipeline,
    _EntryRuntime,
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
from datavis.research.fresh_entry_diagnostics import (
    EntryDiagnosticRejection,
    FilledEntryDiagnostic,
    FreshEntryDiagnosticsResult,
    FrozenSignalEvent,
)
from datavis.research.fresh_event_filters import (
    FreshEventFilterConfig,
    FreshRegimeDefinition,
)
from datavis.research.fresh_pipeline_cli import main
from datavis.research.fresh_preregistration import (
    required_fresh_implementation_files,
)
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_scoring import (
    EntryMetricConfig,
    EntryPromotionThresholds,
    MinimumSampleThresholds,
    SliceDimensions,
)
from datavis.research.fresh_search import (
    EntryCandidateSpec,
    EvaluationContext,
    FreshChronologicalSearch,
    FrozenEntryCandidate,
    FrozenResearchWindow,
)
from datavis.research.fresh_session_eval import FreshSessionTape
from datavis.research.fresh_sessions import broker_session_bounds
from datavis.research.fresh_spool import KeyedObjectSpool, SPOOL_DIRECTORY_PREFIX
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


@dataclass
class _TrackedDiagnostic:
    label: str


class _InjectedPipelineFailure(Exception):
    pass


class _AuditedPipelineSpool(KeyedObjectSpool):
    instances = []

    @classmethod
    def __class_getitem__(cls, _item):
        return cls

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_keys = []
        self.active_loads = 0
        self.maximum_active_loads = 0
        self.created_directory = None
        self.__class__.instances.append(self)

    def __enter__(self):
        entered = super().__enter__()
        self.created_directory = self.directory
        return entered

    @contextmanager
    def load(self, key):
        self.loaded_keys.append(key)
        self.active_loads += 1
        self.maximum_active_loads = max(self.maximum_active_loads, self.active_loads)
        try:
            with super().load(key) as values:
                yield values
        finally:
            self.active_loads -= 1


class _SyntheticSessionSource:
    def __init__(self, tapes, on_load=None):
        self._tapes = dict(tapes)
        self._on_load = on_load

    def load_session(self, anchor):
        if self._on_load is not None:
            self._on_load(anchor)
        return self._tapes[anchor]


class FreshPipelineTests(unittest.TestCase):
    def _spool_test_output(self):
        output = Path(__file__).resolve().parent / (
            f"fresh-pipeline-spool-test-{uuid.uuid4().hex}"
        )
        output.mkdir()
        self.addCleanup(shutil.rmtree, output, True)
        return output

    @staticmethod
    def _synthetic_tape(anchor, identifier_offset):
        bounds = broker_session_bounds(anchor)
        timestamp = bounds.start_utc + pd.Timedelta(hours=1)
        ticks = (
            Tick(identifier_offset + 1, timestamp, 2_050.0, 2_050.2),
            Tick(identifier_offset + 2, timestamp, 2_050.0, 2_050.2),
            Tick(
                identifier_offset + 3,
                timestamp + pd.Timedelta(milliseconds=1),
                2_050.3,
                2_050.5,
            ),
        )
        return FreshSessionTape(anchor, bounds, ticks, "a" * 64)

    @staticmethod
    def _entry_context(anchor):
        return {
            "day": anchor,
            "marketSession": "london-opening",
            "regime": "volatility-q4|spread-q2",
        }

    @classmethod
    def _filled_diagnostic(cls, position, event, outcome):
        covered = outcome == "profit"
        censored = outcome == "censored"
        coverage_ms = 500.0 if covered else None
        fill_timestamp = event.timestamp + timedelta(milliseconds=1)
        observation_end = event.timestamp + timedelta(seconds=60)
        return FilledEntryDiagnostic(
            event_position=position,
            event=event,
            fill_tick_index=event.tick_index + 1,
            fill_tick_id=event.tick_id + 10_000,
            fill_timestamp=fill_timestamp,
            ready_timestamp=event.timestamp,
            expires_timestamp=event.timestamp + timedelta(seconds=1),
            decision_to_fill_ms=1.0,
            ready_to_fill_lag_ms=1.0,
            decision_spread=0.2,
            fill_spread=0.2,
            entry_quote_price=2_050.2,
            entry_fill_price=2_050.2,
            initial_executable_quote_price=2_050.0,
            initial_executable_fill_price=2_050.0,
            explicit_round_trip_cost_per_unit=0.0,
            initial_net_pnl_per_unit=-0.2,
            initial_net_pnl=-0.2,
            break_even_executable_quote_price=2_050.2,
            cost_coverage_tick_index=(event.tick_index + 2 if covered else None),
            cost_coverage_tick_id=(event.tick_id + 20_000 if covered else None),
            cost_coverage_timestamp=(
                event.timestamp + timedelta(milliseconds=500) if covered else None
            ),
            time_to_cost_coverage_ms=coverage_ms,
            decision_to_cost_coverage_ms=coverage_ms,
            cost_covered_by_1s=covered,
            cost_covered_by_2s=covered,
            cost_covered_by_5s=covered,
            cost_covered_by_10s=covered,
            cost_covered_by_20s=covered,
            cost_covered_by_30s=covered,
            cost_covered_by_60s=covered,
            observed_quote_count=2,
            observation_end_timestamp=observation_end,
            observation_end_reason=("fold_end" if censored else "horizon_end"),
            scheduling_release_timestamp=observation_end,
            horizon_complete=not censored,
            censored=censored,
            mae_before_coverage_per_unit=-0.1,
            mfe_before_coverage_per_unit=0.3 if covered else 0.0,
            mae_horizon_per_unit=-0.25,
            mfe_horizon_per_unit=0.4 if covered else 0.0,
            mae_before_coverage=-0.1,
            mfe_before_coverage=0.3 if covered else 0.0,
            mae_horizon=-0.25,
            mfe_horizon=0.4 if covered else 0.0,
            entry_efficiency=0.75 if covered else 0.0,
            profit_barrier_hit=covered,
            profit_barrier_first_hit_ms=250.0 if covered else None,
            loss_barrier_hit=outcome == "loss",
            loss_barrier_first_hit_ms=250.0 if outcome == "loss" else None,
            first_barrier_hit=(
                "profit" if covered else "loss" if outcome == "loss" else None
            ),
            first_barrier_hit_tick_id=(
                event.tick_id + 30_000 if outcome != "censored" else None
            ),
            first_barrier_hit_timestamp=(
                event.timestamp + timedelta(milliseconds=250)
                if outcome != "censored"
                else None
            ),
            first_barrier_hit_ms=(250.0 if outcome != "censored" else None),
        )

    @classmethod
    def _synthetic_diagnostics(cls, tape, events, observed):
        selected = tuple(events)
        label = (
            selected[0].metadata.get("candidate_id", "baseline")
            if selected
            else "empty"
        )
        observed.append(
            (
                tape.anchor,
                label,
                tuple(
                    (
                        event.tick_id,
                        event.timestamp,
                        tape.ticks[event.tick_index].bid,
                        tape.ticks[event.tick_index].ask,
                    )
                    for event in selected
                ),
            )
        )
        diagnostics = []
        rejections = []
        reasons = {}
        for position, event in enumerate(selected):
            if label == "active" and position == 1 and tape.anchor.endswith("02"):
                reason = "entry_lag_exceeded"
                rejections.append(
                    EntryDiagnosticRejection(
                        event_position=position,
                        event=event,
                        reason=reason,
                        observed_timestamp=event.timestamp,
                        ready_timestamp=event.timestamp,
                        expires_timestamp=event.timestamp + timedelta(seconds=1),
                        scheduling_release_timestamp=event.timestamp,
                    )
                )
                reasons[reason] = reasons.get(reason, 0) + 1
                continue
            if label == "baseline":
                outcome = "loss"
            elif label == "active" and position == 1:
                outcome = "censored"
            else:
                outcome = "profit"
            diagnostics.append(cls._filled_diagnostic(position, event, outcome))
        return FreshEntryDiagnosticsResult(
            diagnostics=tuple(diagnostics),
            rejections=tuple(rejections),
            rejected_reason_counts=reasons,
            event_count=len(selected),
        )

    def _synthetic_entry_pipeline(self, *, on_load=None):
        output = self._spool_test_output()
        anchors = ("2026-01-02", "2026-01-05")
        tapes = {
            anchor: self._synthetic_tape(anchor, ordinal * 100)
            for ordinal, anchor in enumerate(anchors, start=1)
        }
        pipeline = object.__new__(RegisteredFreshResearchPipeline)
        pipeline.output = output
        pipeline.quantile_bank = SimpleNamespace(bank_sha256="9" * 64)
        pipeline.regime_definition = FreshRegimeDefinition(
            volatility_column="volatility",
            spread_column="spread",
            trend_column="trend",
            arrival_column="arrival",
        )
        pipeline.source = _SyntheticSessionSource(tapes, on_load=on_load)
        pipeline.entry_diagnostic_config = SimpleNamespace()
        pipeline.dimensions = SliceDimensions(
            day_metadata_path="context.day",
            market_session_metadata_path="context.marketSession",
            regime_metadata_path="context.regime",
        )
        pipeline.scoring = SimpleNamespace(
            entry_metrics=EntryMetricConfig(
                coverage_checkpoints_seconds=(1, 2, 5, 10, 20, 30, 60),
                restricted_uncovered_milliseconds=60_000,
                profit_barrier_net_per_unit=0.25,
                loss_barrier_net_per_unit=0.25,
            ),
            minimum_sample=MinimumSampleThresholds(
                filled_trades_per_session=1,
                absolute_filled_trades=1,
                active_session_fraction_minimum=0.0,
            ),
            entry_gate=EntryPromotionThresholds(
                fill_rate_minimum=0.0,
                coverage_10_seconds_minimum=0.0,
                coverage_30_seconds_minimum=0.0,
                coverage_60_seconds_minimum=0.0,
                restricted_median_coverage_milliseconds_maximum=60_000.0,
                censored_fraction_maximum=1.0,
                equal_barrier_distance_per_unit=0.25,
                equal_barrier_profit_first_rate_minimum=0.0,
            ),
        )
        pipeline.preregistration = {
            "robustnessAndGates": {
                "parameterNeighborhood": {
                    "minimumValidNeighborFraction": 0.0,
                    "minimumPositiveExpectancyNeighborFraction": 0.0,
                    "minimumNeighborExpectancyRetention": 0.0,
                    "maximumAbsoluteCoverage30SecondDrop": 1.0,
                }
            }
        }
        pipeline._emit = lambda **_payload: None

        def features(tape, _columns):
            frame = pd.DataFrame(
                {
                    "tick_id": [tick.id for tick in tape.ticks],
                    "timestamp": pd.to_datetime(
                        [tick.timestamp for tick in tape.ticks], utc=True
                    ),
                    "bid": [tick.bid for tick in tape.ticks],
                    "ask": [tick.ask for tick in tape.ticks],
                }
            )
            frame.attrs["anchor"] = tape.anchor
            return frame

        pipeline._features = features
        event_filter = FreshEventFilterConfig(
            variant_id="all",
            regime_definition=pipeline.regime_definition,
            activity_filter="all",
            spread_ceiling_rank=None,
            volatility_floor_rank=None,
        )
        runtimes = []
        candidates = []
        for candidate_id in ("active", "empty"):
            source = SimpleNamespace(
                config=SimpleNamespace(candidate_id=candidate_id),
                rank_offset=0.0,
                threshold_provenance=(),
            )
            runtime = _EntryRuntime(
                candidate_id=candidate_id,
                family="synthetic",
                source=source,
                event_filter=event_filter,
                entry_variant="synthetic",
                robustness_group=candidate_id,
            )
            runtimes.append(runtime)
            candidates.append(
                FrozenEntryCandidate.freeze(
                    EntryCandidateSpec(
                        candidate_id=candidate_id,
                        family="synthetic",
                        config={"candidate": candidate_id},
                        entry_variant="synthetic",
                    ),
                    threshold_bank_sha256="9" * 64,
                )
            )
        pipeline.entry_runtime = {runtime.candidate_id: runtime for runtime in runtimes}
        window = FrozenResearchWindow(
            role="walk_forward_1",
            session_anchors=anchors,
            window_sha256="8" * 64,
        )
        context = EvaluationContext(
            stage="walk_forward_1",
            training_roles=("discovery",),
            evaluation_roles=("walk_forward_1",),
            windows=(window,),
        )
        return pipeline, tuple(runtimes), tuple(candidates), context

    @contextmanager
    def _synthetic_entry_dependencies(self, observed, *, diagnose=None):
        def generate(frame, *, configs, engine):
            self.assertEqual(engine, "batch")
            anchor = frame.attrs["anchor"]
            output = []
            for config in configs:
                if config.candidate_id == "empty":
                    continue
                for index, side in ((0, "long"), (1, "short")):
                    output.append(
                        FrozenSignalEvent(
                            tick_index=index,
                            tick_id=int(frame.iloc[index]["tick_id"]),
                            timestamp=frame.iloc[index]["timestamp"].to_pydatetime(),
                            side=side,
                            metadata={
                                "candidate_id": config.candidate_id,
                                "context": self._entry_context(anchor),
                            },
                        )
                    )
            return tuple(output)

        def baseline(_frame, tape):
            return tuple(
                FrozenSignalEvent(
                    tick_index=index,
                    tick_id=tape.ticks[index].id,
                    timestamp=tape.ticks[index].timestamp,
                    side=side,
                    metadata={"context": self._entry_context(tape.anchor)},
                )
                for index, side in ((0, "long"), (1, "short"))
            )

        def filtered(_frame, requests, *, quantile_bank):
            self.assertIsNotNone(quantile_bank)
            return tuple(SimpleNamespace(events=request.events) for request in requests)

        diagnostic = diagnose or (
            lambda tape, events, *, config: self._synthetic_diagnostics(
                tape, events, observed
            )
        )
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline.signal_required_columns",
                    return_value=(),
                )
            )
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline.fresh_regime_quantile_measurements",
                    return_value=(),
                )
            )
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline.generate_frozen_signal_events",
                    side_effect=generate,
                )
            )
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline._baseline_events",
                    side_effect=baseline,
                )
            )
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline.enrich_and_filter_frozen_event_batch",
                    side_effect=filtered,
                )
            )
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline.fresh_event_filter_config_fingerprint",
                    side_effect=lambda config, _bank: config.variant_id,
                )
            )
            stack.enter_context(
                patch(
                    "datavis.research.fresh_pipeline._diagnose",
                    side_effect=diagnostic,
                )
            )
            yield

    @staticmethod
    def _evaluation_hash(evaluation):
        return canonical_hash(asdict(evaluation))

    @staticmethod
    def _spool_directories(output):
        return tuple(output.glob(f"{SPOOL_DIRECTORY_PREFIX}*"))

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
        self.assertIn(first["researchWindowSetSha256"], first["experimentLedgerPath"])
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
            _scenario_ids_for_stage(registered, required, stage="exit_search"),
            ("reference-provisional", *required),
        )
        self.assertEqual(
            _scenario_ids_for_stage(registered, required, stage="validation"),
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
        preregistration = {"chronologicalWindowPolicy": {"discovery_sessions": 40}}
        self.assertEqual(_bound_discovery_session_count(preregistration), 40)
        with self.assertRaisesRegex(ValueError, "discovery-session count"):
            _bound_discovery_session_count(
                {"chronologicalWindowPolicy": {"discovery_sessions": 0}}
            )

    def test_direction_matched_cluster_uplift_is_deterministic(self):
        candidates = [
            result(successes_10=8, successes_30=9, count=10) for _ in range(12)
        ]
        baselines = [
            result(successes_10=4, successes_30=5, count=10) for _ in range(12)
        ]
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
                "timestamp": pd.to_datetime(
                    [item.timestamp for item in ticks], utc=True
                ),
                "feature_ready": [True, True],
                "gap_detected": [False, False],
            }
        )
        events = _baseline_events(frame, tape)
        self.assertEqual(
            [(item.tick_id, item.side) for item in events], [(1, "long"), (1, "short")]
        )

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
        self.assertIs(replay.call_args.kwargs["_prepared_replay_tape"], prepared)
        self.assertEqual(replay.call_args.kwargs["boundary"].name, tape.anchor)
        with self.assertRaisesRegex(TypeError, "FreshSessionTape"):
            _replay_session(
                SimpleNamespace(ticks=ticks),
                decisions,
                config=config,
                prepared_replay_tape=prepared,
            )

    def test_spooled_entry_scores_are_exactly_materialized_equivalent(self):
        pipeline, _runtimes, candidates, context = self._synthetic_entry_pipeline()
        materialized_observed = []
        with self._synthetic_entry_dependencies(materialized_observed):
            materialized = pipeline.score_entries_batch_materialized_reference(
                candidates, context
            )

        _AuditedPipelineSpool.instances = []
        spooled_observed = []
        with (
            self._synthetic_entry_dependencies(spooled_observed),
            patch(
                "datavis.research.fresh_pipeline.KeyedObjectSpool",
                _AuditedPipelineSpool,
            ),
        ):
            spooled = pipeline.score_entries_batch(candidates, context)

        self.assertEqual(tuple(materialized), tuple(spooled))
        for candidate in candidates:
            expected = materialized[candidate.candidate_id]
            actual = spooled[candidate.candidate_id]
            self.assertEqual(actual, expected)
            self.assertEqual(
                self._evaluation_hash(actual), self._evaluation_hash(expected)
            )

        def promoted(evaluations):
            return tuple(
                item.candidate_id
                for item in FreshChronologicalSearch._rank_passed(
                    tuple(
                        (candidate, evaluations[candidate.candidate_id])
                        for candidate in candidates
                    ),
                    identifier=lambda item: item.candidate_id,
                    limit=len(candidates),
                )
            )

        self.assertEqual(promoted(spooled), promoted(materialized))
        self.assertEqual(promoted(spooled), ("active",))
        active_metrics = spooled["active"].metrics["entry"]["overall"]
        empty_metrics = spooled["empty"].metrics["entry"]["overall"]
        self.assertEqual(active_metrics["signal_count"], 4)
        self.assertEqual(active_metrics["filled_count"], 3)
        self.assertEqual(active_metrics["rejected_count"], 1)
        self.assertEqual(active_metrics["censored_count"], 1)
        self.assertEqual(empty_metrics["signal_count"], 0)
        self.assertEqual(empty_metrics["filled_count"], 0)

        active_observations = [item for item in spooled_observed if item[1] == "active"]
        self.assertEqual(len(active_observations), 2)
        for _anchor, _label, events in active_observations:
            self.assertEqual(len(events), 2)
            self.assertNotEqual(events[0][0], events[1][0])
            self.assertEqual(events[0][1:], events[1][1:])
        self.assertEqual(spooled_observed, materialized_observed)

        self.assertEqual(len(_AuditedPipelineSpool.instances), 1)
        spool = _AuditedPipelineSpool.instances[0]
        self.assertEqual(spool.maximum_active_loads, 1)
        self.assertEqual(
            spool.loaded_keys,
            [
                pipeline._candidate_spool_key("active"),
                pipeline._baseline_spool_key("all"),
                pipeline._candidate_spool_key("empty"),
                pipeline._baseline_spool_key("all"),
            ],
        )
        self.assertFalse(spool.created_directory.exists())
        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_spooled_pipeline_drops_prior_session_diagnostics(self):
        weak_references = []

        def before_load(anchor):
            if anchor == "2026-01-05":
                gc.collect()
                self.assertTrue(weak_references)
                self.assertTrue(
                    all(reference() is None for reference in weak_references)
                )

        pipeline, runtimes, _candidates, context = self._synthetic_entry_pipeline(
            on_load=before_load
        )

        def diagnose(tape, events, *, config):
            del events, config
            diagnostic = _TrackedDiagnostic(tape.anchor)
            weak_references.append(weakref.ref(diagnostic))
            return diagnostic

        with self._synthetic_entry_dependencies([], diagnose=diagnose):
            with pipeline._entry_session_spool(
                runtimes,
                context.windows[0].session_anchors,
                stage="retention-audit",
            ) as (spool, _baseline_by_candidate):
                self.assertEqual({count for _key, count in spool.inventory}, {2})
                gc.collect()
                self.assertTrue(
                    all(reference() is None for reference in weak_references)
                )

        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_spooled_pipeline_cleans_up_after_processing_failure(self):
        pipeline, _runtimes, candidates, context = self._synthetic_entry_pipeline()
        _AuditedPipelineSpool.instances = []
        original = pipeline._append_entry_session_to_spool
        calls = 0

        def fail_on_second_session(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise _InjectedPipelineFailure("processing failed")
            return original(**kwargs)

        with (
            self._synthetic_entry_dependencies([]),
            patch(
                "datavis.research.fresh_pipeline.KeyedObjectSpool",
                _AuditedPipelineSpool,
            ),
            patch.object(
                pipeline,
                "_append_entry_session_to_spool",
                side_effect=fail_on_second_session,
            ),
        ):
            with self.assertRaisesRegex(_InjectedPipelineFailure, "processing failed"):
                pipeline.score_entries_batch(candidates, context)

        self.assertEqual(calls, 2)
        self.assertEqual(len(_AuditedPipelineSpool.instances), 1)
        self.assertFalse(_AuditedPipelineSpool.instances[0].created_directory.exists())
        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_spooled_pipeline_cleans_up_after_scoring_failure(self):
        pipeline, _runtimes, candidates, context = self._synthetic_entry_pipeline()
        _AuditedPipelineSpool.instances = []
        with (
            self._synthetic_entry_dependencies([]),
            patch(
                "datavis.research.fresh_pipeline.KeyedObjectSpool",
                _AuditedPipelineSpool,
            ),
            patch.object(
                pipeline,
                "_entry_provisional_evaluation",
                side_effect=_InjectedPipelineFailure("scoring failed"),
            ),
        ):
            with self.assertRaisesRegex(_InjectedPipelineFailure, "scoring failed"):
                pipeline.score_entries_batch(candidates, context)

        self.assertEqual(len(_AuditedPipelineSpool.instances), 1)
        self.assertFalse(_AuditedPipelineSpool.instances[0].created_directory.exists())
        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_spooled_pipeline_cleans_up_after_serialization_failure(self):
        pipeline, _runtimes, candidates, context = self._synthetic_entry_pipeline()
        _AuditedPipelineSpool.instances = []
        with (
            self._synthetic_entry_dependencies([]),
            patch(
                "datavis.research.fresh_pipeline.KeyedObjectSpool",
                _AuditedPipelineSpool,
            ),
            patch(
                "datavis.research.fresh_spool.pickle.dumps",
                side_effect=_InjectedPipelineFailure("serialization failed"),
            ),
        ):
            with self.assertRaisesRegex(
                _InjectedPipelineFailure, "serialization failed"
            ):
                pipeline.score_entries_batch(candidates, context)

        self.assertEqual(len(_AuditedPipelineSpool.instances), 1)
        self.assertFalse(_AuditedPipelineSpool.instances[0].created_directory.exists())
        self.assertEqual(self._spool_directories(pipeline.output), ())

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
