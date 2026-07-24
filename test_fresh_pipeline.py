from __future__ import annotations

import gc
import random
import shutil
import unittest
import uuid
import weakref
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from datavis.research.fresh_pipeline import (
    BASELINE_MINIMUM_UPLIFT,
    SESSION_CLOSE_SAFETY_MS,
    RegisteredFreshResearchPipeline,
    _StreamingEntryEdgeReducer,
    _EntryRuntime,
    _baseline_coverage_summary,
    _baseline_events,
    _bound_discovery_session_count,
    _cluster_entry_edge,
    _diagnose,
    _entry_barrier_value,
    _entry_edge_summary,
    _parameter_neighbourhood_audit,
    _replay_session,
    _research_state_binding,
    _research_state_binding_v3,
    _research_state_binding_v4,
    _restricted_coverage_ms,
    _scenario_ids_for_stage,
    _snapshot_new_file,
    _strongest_record,
    run_registered_fresh_research,
)
from datavis.research.fresh_numeric_spool import (
    NUMERIC_SPOOL_DIRECTORY_PREFIX,
    FloatSeriesSpool,
)
from datavis.research.fresh_entry_diagnostics import (
    EntryDiagnosticRejection,
    FilledEntryDiagnostic,
    FreshEntryDiagnosticsResult,
    FrozenSignalEvent,
    prepare_entry_diagnostic_tape,
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
    StageRunResult,
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
        pipeline.spool_directory = output
        pipeline.spool_maximum_bytes = 8 * 1024 * 1024
        pipeline.event_spool_maximum_bytes = 4 * 1024 * 1024
        pipeline.numeric_spool_maximum_bytes = 8 * 1024 * 1024
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
    def _synthetic_entry_dependencies(
        self,
        observed,
        *,
        diagnose=None,
        filter_request_sizes=None,
        prepared_diagnostics=None,
    ):
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

        def generate_groups(frame, *, configs, engine):
            for candidate_config in configs:
                yield (
                    candidate_config.candidate_id,
                    generate(
                        frame,
                        configs=(candidate_config,),
                        engine=engine,
                    ),
                )

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
            if filter_request_sizes is not None:
                filter_request_sizes.append(len(requests))
            return tuple(SimpleNamespace(events=request.events) for request in requests)

        class SyntheticFilterEvaluator:
            def __init__(self, frame, *, regime_definition, row_limit):
                del regime_definition, row_limit
                self.frame = frame

            def enrich_and_filter(self, requests, *, quantile_bank):
                return filtered(
                    self.frame,
                    requests,
                    quantile_bank=quantile_bank,
                )

        diagnostic = diagnose or (
            lambda tape, events, *, config: self._synthetic_diagnostics(
                tape, events, observed
            )
        )

        def diagnose_compatibility(
            tape,
            events,
            *,
            config,
            prepared_tape=None,
        ):
            if prepared_diagnostics is not None:
                prepared_diagnostics.append(prepared_tape is not None)
            return diagnostic(tape, events, config=config)

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
                    "datavis.research.fresh_pipeline.iter_frozen_signal_event_groups",
                    side_effect=generate_groups,
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
                    "datavis.research.fresh_pipeline.FreshEventFilterBatchEvaluator",
                    SyntheticFilterEvaluator,
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
                    side_effect=diagnose_compatibility,
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

    def test_v3_uses_a_separate_ledger_and_the_identical_global_holdout_lock(self):
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
        predecessor = _research_state_binding("durable-state", split)
        research_sha = predecessor["researchWindowSetSha256"]
        lineage_body = {
            "schema": "fresh-xauusd-study-lineage/v1",
            "studyId": "xauusd-fresh-causal-acceleration-v3",
            "predecessorStudyId": "xauusd-fresh-causal-acceleration-v2",
            "predecessorPreregistrationSha256": (
                "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf"
            ),
            "predecessorTerminalLedgerSha256": (
                "209d80249abd3082df7b50b55c845b71c18401f0c9d2c61a25f5c66e4de28c40"
            ),
            "splitManifestSha256": split["manifestSha256"],
            "researchWindowSetSha256": research_sha,
            "scientificSpecificationSha256": (
                "fef6b1a4898aaeb4ce33ad96ea270f0211448357399d94f76051b01c9dabcbd8"
            ),
        }
        expected_lineage = canonical_hash(lineage_body)
        provenance = {
            "predecessorLedgerSha256": (
                "209d80249abd3082df7b50b55c845b71c18401f0c9d2c61a25f5c66e4de28c40"
            ),
            "predecessorPreregistrationSha256": (
                "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf"
            ),
            "predecessorLineageTerminal": True,
            "candidateOutcomeRecordCount": 0,
            "transientCandidateComputationsRecovered": False,
            "batchResultSealed": False,
        }
        with patch(
            "datavis.research.fresh_pipeline.FRESH_V3_STUDY_LINEAGE_SHA256",
            expected_lineage,
        ):
            restarted = _research_state_binding_v3(
                "durable-state",
                split,
                provenance,
            )

        self.assertEqual(
            restarted["schema"],
            "fresh-xauusd-durable-research-state/v2",
        )
        self.assertEqual(restarted["studyLineage"], lineage_body)
        self.assertEqual(restarted["studyLineageSha256"], expected_lineage)
        self.assertNotEqual(
            restarted["experimentLedgerPath"],
            predecessor["experimentLedgerPath"],
        )
        self.assertEqual(
            restarted["predecessorExperimentLedgerPath"],
            predecessor["experimentLedgerPath"],
        )
        self.assertEqual(
            restarted["holdoutAuthorizationRegistryPath"],
            predecessor["holdoutAuthorizationRegistryPath"],
        )

    def test_v4_uses_a_new_lineage_ledger_and_the_identical_global_holdout_lock(
        self,
    ):
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
        predecessor = _research_state_binding("durable-state", split)
        scientific_sha = "f" * 64
        predecessor_lineage_sha = "d" * 64
        lineage_body = {
            "schema": "fresh-xauusd-study-lineage/v1",
            "studyId": "xauusd-fresh-causal-acceleration-v4",
            "predecessorStudyId": "xauusd-fresh-causal-acceleration-v3",
            "predecessorPreregistrationSha256": "a" * 64,
            "predecessorTerminalLedgerSha256": "b" * 64,
            "splitManifestSha256": split["manifestSha256"],
            "researchWindowSetSha256": predecessor["researchWindowSetSha256"],
            "scientificSpecificationSha256": scientific_sha,
        }
        expected_lineage = canonical_hash(lineage_body)
        provenance = {
            "studyId": "xauusd-fresh-causal-acceleration-v4",
            "studyLineageSha256": expected_lineage,
            "predecessorLedgerSha256": "b" * 64,
            "predecessorPreregistrationSha256": "a" * 64,
            "predecessorLineageTerminal": True,
            "candidateOutcomeRecordCount": 0,
            "laterWindowOutcomeRecordCount": 0,
            "transientSpoolsRecovered": False,
            "transientCandidateComputationsRecovered": False,
            "partialCandidateResultsImported": False,
            "batchResultSealed": False,
            "restartPolicy": {
                "recomputeFromDiscoverySessionOrdinal": 1,
                "discardTransientSpools": True,
                "discardPartialCandidateComputations": True,
                "importCandidateResults": False,
            },
        }
        predecessor_state_binding = {
            "schema": "fresh-xauusd-durable-research-state/v2",
            "studyId": "xauusd-fresh-causal-acceleration-v3",
            "studyLineageSha256": predecessor_lineage_sha,
            "splitManifestSha256": predecessor["splitManifestSha256"],
            "researchWindowSetSha256": predecessor[
                "researchWindowSetSha256"
            ],
            "holdoutWindowSha256": predecessor["holdoutWindowSha256"],
            "stateDirectory": predecessor["stateDirectory"],
            "experimentLedgerPath": str(
                Path(predecessor["stateDirectory"])
                / "studies"
                / predecessor["researchWindowSetSha256"]
                / "lineages"
                / predecessor_lineage_sha
                / "fresh_experiment_ledger_v1.jsonl"
            ),
            "holdoutAuthorizationRegistryPath": predecessor[
                "holdoutAuthorizationRegistryPath"
            ],
        }
        with (
            patch(
                "datavis.research.fresh_pipeline.RUN17_LEDGER_SHA256",
                "b" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN17_PREREGISTRATION_SHA256",
                "a" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN17_STUDY_LINEAGE_SHA256",
                expected_lineage,
            ),
            patch(
                "datavis.research.fresh_pipeline.FRESH_V3_STUDY_LINEAGE_SHA256",
                predecessor_lineage_sha,
            ),
            patch(
                "datavis.research.fresh_pipeline.canonical_fresh_v4_study_lineage",
                return_value=lineage_body,
            ),
            patch(
                "datavis.research.fresh_pipeline.fresh_v4_scientific_specification_sha256",
                return_value=scientific_sha,
            ),
        ):
            restarted = _research_state_binding_v4(
                "durable-state",
                split,
                provenance,
                predecessor_state_binding,
            )
            with self.assertRaisesRegex(PermissionError, "exact terminal v3"):
                _research_state_binding_v4(
                    "alternate-durable-state",
                    split,
                    provenance,
                    predecessor_state_binding,
                )

        self.assertEqual(
            restarted["schema"],
            "fresh-xauusd-durable-research-state/v3",
        )
        self.assertEqual(restarted["studyLineage"], lineage_body)
        self.assertEqual(restarted["studyLineageSha256"], expected_lineage)
        self.assertIn(expected_lineage, restarted["experimentLedgerPath"])
        self.assertIn(
            predecessor_lineage_sha,
            restarted["predecessorExperimentLedgerPath"],
        )
        self.assertEqual(
            restarted["holdoutAuthorizationRegistryPath"],
            predecessor["holdoutAuthorizationRegistryPath"],
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

    def test_streaming_entry_edge_is_exactly_randomized_materialized_equivalent(
        self,
    ):
        rng = random.Random(20260725)
        candidate_results = []
        baseline_results = []
        timestamp = broker_session_bounds("2026-01-02").start_utc
        tick_id = 1
        for session_index in range(12):
            candidate_diagnostics = []
            baseline_diagnostics = []
            candidate_count = 0 if session_index == 0 else rng.randrange(1, 25)
            baseline_count = rng.randrange(2, 12)
            for label, count, destination in (
                ("candidate", candidate_count, candidate_diagnostics),
                ("baseline", baseline_count, baseline_diagnostics),
            ):
                for position in range(count):
                    event = FrozenSignalEvent(
                        tick_index=position,
                        tick_id=tick_id,
                        timestamp=timestamp + timedelta(milliseconds=tick_id),
                        side=rng.choice(("long", "short")),
                        metadata={
                            "candidate_id": label,
                            "context": self._entry_context(
                                f"2026-01-{session_index + 2:02d}"
                            ),
                        },
                    )
                    tick_id += 1
                    destination.append(
                        self._filled_diagnostic(
                            position,
                            event,
                            rng.choice(("profit", "loss", "censored")),
                        )
                    )
            candidate_results.append(
                FreshEntryDiagnosticsResult(
                    diagnostics=tuple(candidate_diagnostics),
                    rejections=(),
                    rejected_reason_counts={},
                    event_count=len(candidate_diagnostics),
                )
            )
            baseline_results.append(
                FreshEntryDiagnosticsResult(
                    diagnostics=tuple(baseline_diagnostics),
                    rejections=(),
                    rejected_reason_counts={},
                    event_count=len(baseline_diagnostics),
                )
            )

        seed_text = "randomized-exact-edge"
        expected = _entry_edge_summary(
            candidate_results,
            baseline_results,
            seed_text=seed_text,
        )
        with FloatSeriesSpool(
            self._spool_test_output(),
            maximum_bytes=64 * 1024 * 1024,
        ) as values:
            reducer = _StreamingEntryEdgeReducer(
                values=values,
                key_prefix="edge",
                seed_text=seed_text,
            )
            for candidate, baseline in zip(candidate_results, baseline_results):
                reducer.add_session(
                    candidate,
                    _baseline_coverage_summary(baseline),
                )
            actual = reducer.finish()

        self.assertEqual(actual, expected)

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

        prepared = prepare_entry_diagnostic_tape(
            tape.ticks,
            _trusted_validated_ticks=True,
        )
        with patch(
            "datavis.research.fresh_pipeline.evaluate_prepared_frozen_entries",
            return_value=sentinel,
        ) as evaluate_prepared:
            actual = _diagnose(
                tape,
                (),
                config=SimpleNamespace(),
                prepared_tape=prepared,
            )

        self.assertIs(actual, sentinel)
        self.assertIs(evaluate_prepared.call_args.args[0], prepared)
        self.assertEqual(evaluate_prepared.call_args.kwargs["boundary"].name, tape.anchor)
        self.assertEqual(
            evaluate_prepared.call_args.kwargs["scheduling"].mode,
            "independent",
        )

        other_tape = self._synthetic_tape("2026-01-05", 100)
        with self.assertRaisesRegex(TypeError, "same FreshSessionTape tuple"):
            _diagnose(
                tape,
                (),
                config=SimpleNamespace(),
                prepared_tape=prepare_entry_diagnostic_tape(
                    other_tape.ticks,
                    _trusted_validated_ticks=True,
                ),
            )

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
        numeric_spool_caps = []

        def bounded_numeric_spool(parent_directory, *, maximum_bytes):
            numeric_spool_caps.append(maximum_bytes)
            return FloatSeriesSpool(
                parent_directory,
                maximum_bytes=maximum_bytes,
            )

        with (
            self._synthetic_entry_dependencies(spooled_observed),
            patch(
                "datavis.research.fresh_pipeline.KeyedObjectSpool",
                _AuditedPipelineSpool,
            ),
            patch(
                "datavis.research.fresh_pipeline.FloatSeriesSpool",
                side_effect=bounded_numeric_spool,
            ),
            patch(
                "datavis.research.fresh_pipeline.combine_entry_diagnostics",
                side_effect=AssertionError(
                    "production spooled scoring materialized diagnostics"
                ),
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
        self.assertEqual(
            numeric_spool_caps,
            [pipeline.numeric_spool_maximum_bytes] * len(candidates),
        )
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

        self.assertEqual(len(_AuditedPipelineSpool.instances), 3)
        spool = _AuditedPipelineSpool.instances[0]
        self.assertEqual(spool.maximum_active_loads, 1)
        self.assertEqual(
            spool.loaded_keys,
            [
                pipeline._baseline_spool_key("all"),
                pipeline._candidate_spool_key("active"),
                pipeline._candidate_spool_key("empty"),
            ],
        )
        self.assertTrue(
            all(
                not audited.created_directory.exists()
                for audited in _AuditedPipelineSpool.instances
            )
        )
        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_external_spool_directory_does_not_change_scores_or_touch_output(self):
        pipeline, _runtimes, candidates, context = self._synthetic_entry_pipeline()
        with self._synthetic_entry_dependencies([]):
            expected = pipeline.score_entries_batch_materialized_reference(
                candidates,
                context,
            )

        scratch = self._spool_test_output()
        pipeline.spool_directory = scratch
        _AuditedPipelineSpool.instances = []
        with (
            self._synthetic_entry_dependencies([]),
            patch(
                "datavis.research.fresh_pipeline.KeyedObjectSpool",
                _AuditedPipelineSpool,
            ),
        ):
            actual = pipeline.score_entries_batch(candidates, context)

        self.assertEqual(actual, expected)
        self.assertEqual(self._spool_directories(pipeline.output), ())
        self.assertEqual(self._spool_directories(scratch), ())
        self.assertTrue(_AuditedPipelineSpool.instances)
        outer = _AuditedPipelineSpool.instances[0]
        self.assertEqual(outer.created_directory.parent, scratch)
        self.assertEqual(outer.maximum_bytes, pipeline.spool_maximum_bytes)

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

    def test_streamed_pipeline_enriches_only_one_result_at_a_time(self):
        pipeline, _runtimes, candidates, context = self._synthetic_entry_pipeline()
        request_sizes = []
        prepared_usage = []

        with (
            self._synthetic_entry_dependencies(
                [],
                filter_request_sizes=request_sizes,
                prepared_diagnostics=prepared_usage,
            ),
            patch(
                "datavis.research.fresh_pipeline.prepare_entry_diagnostic_tape",
                wraps=prepare_entry_diagnostic_tape,
            ) as prepare,
        ):
            pipeline.score_entries_batch(candidates, context)

        self.assertEqual(request_sizes, [1, 1, 1, 1, 1, 1])
        self.assertEqual(prepare.call_count, 2)
        self.assertTrue(prepared_usage)
        self.assertTrue(all(prepared_usage))
        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_streamed_pipeline_releases_last_first_pass_group_before_filtering(self):
        pipeline, runtimes, _candidates, context = self._synthetic_entry_pipeline()
        marker_references = []

        def generate_groups(frame, *, configs, engine):
            self.assertEqual(engine, "batch")
            previous_reference = None
            for candidate_config in configs:
                if previous_reference is not None:
                    gc.collect()
                    self.assertIsNone(previous_reference())
                marker = _TrackedDiagnostic(candidate_config.candidate_id)
                previous_reference = weakref.ref(marker)
                marker_references.append(previous_reference)
                row = frame.iloc[0]
                yield (
                    candidate_config.candidate_id,
                    (
                        FrozenSignalEvent(
                            tick_index=0,
                            tick_id=int(row["tick_id"]),
                            timestamp=row["timestamp"].to_pydatetime(),
                            side="long",
                            metadata={
                                "candidate_id": candidate_config.candidate_id,
                                "context": self._entry_context(frame.attrs["anchor"]),
                                "retention_marker": marker,
                            },
                        ),
                    ),
                )
                del marker
            gc.collect()
            if previous_reference is not None:
                self.assertIsNone(previous_reference())

        class RetentionAuditedEvaluator:
            def __init__(
                inner_self,
                frame,
                *,
                regime_definition,
                row_limit,
            ):
                del regime_definition, row_limit
                gc.collect()
                self.assertTrue(marker_references)
                self.assertTrue(
                    all(reference() is None for reference in marker_references)
                )
                inner_self.frame = frame

            def enrich_and_filter(
                inner_self,
                requests,
                *,
                quantile_bank,
            ):
                del inner_self, quantile_bank
                return tuple(
                    SimpleNamespace(events=request.events)
                    for request in requests
                )

        with (
            self._synthetic_entry_dependencies([]),
            patch(
                "datavis.research.fresh_pipeline.iter_frozen_signal_event_groups",
                side_effect=generate_groups,
            ),
            patch(
                "datavis.research.fresh_pipeline.FreshEventFilterBatchEvaluator",
                RetentionAuditedEvaluator,
            ),
        ):
            with pipeline._entry_session_spool(
                runtimes,
                context.windows[0].session_anchors,
                stage="first-pass-retention-audit",
            ) as (spool, _baseline_by_candidate):
                self.assertEqual({count for _key, count in spool.inventory}, {2})

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
        self.assertEqual(len(_AuditedPipelineSpool.instances), 2)
        self.assertTrue(
            all(
                not audited.created_directory.exists()
                for audited in _AuditedPipelineSpool.instances
            )
        )
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
            patch(
                "datavis.research.fresh_pipeline."
                "EntryDiagnosticSessionScorer.add_session",
                side_effect=_InjectedPipelineFailure("scoring failed"),
            ),
        ):
            with self.assertRaisesRegex(_InjectedPipelineFailure, "scoring failed"):
                pipeline.score_entries_batch(candidates, context)

        self.assertEqual(len(_AuditedPipelineSpool.instances), 3)
        self.assertTrue(
            all(
                not audited.created_directory.exists()
                for audited in _AuditedPipelineSpool.instances
            )
        )
        self.assertEqual(self._spool_directories(pipeline.output), ())
        self.assertEqual(
            tuple(
                pipeline.output.glob(f"{NUMERIC_SPOOL_DIRECTORY_PREFIX}*")
            ),
            (),
        )

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
                "datavis.research.fresh_spool._CompressedPickleWriter.write",
                side_effect=_InjectedPipelineFailure("serialization failed"),
            ),
        ):
            with self.assertRaisesRegex(
                _InjectedPipelineFailure, "serialization failed"
            ):
                pipeline.score_entries_batch(candidates, context)

        self.assertEqual(len(_AuditedPipelineSpool.instances), 2)
        self.assertTrue(
            all(
                not audited.created_directory.exists()
                for audited in _AuditedPipelineSpool.instances
            )
        )
        self.assertEqual(self._spool_directories(pipeline.output), ())

    def test_manifest_binding_covers_the_complete_runner(self):
        paths = required_fresh_implementation_files()
        self.assertIn("datavis/research/fresh_pipeline.py", paths)
        self.assertIn("datavis/research/fresh_scoring.py", paths)
        self.assertIn("datavis/research/fresh_candidate_grid.py", paths)

    def test_v4_orchestration_recomputes_frozen_discovery_from_session_one(self):
        repository = Path(__file__).resolve().parent
        output = self._spool_test_output()
        state = self._spool_test_output()
        scratch = self._spool_test_output()
        predecessor_ledger = state / "v3" / "fresh_experiment_ledger_v1.jsonl"
        predecessor_ledger.parent.mkdir()
        predecessor_ledger.write_text("terminal-v3\n", encoding="utf-8")
        ledger = state / "v4" / "fresh_experiment_ledger_v1.jsonl"
        holdout = state / "holdout" / "fresh_holdout_authorization_v1.json"
        entry_bank_sha = "e" * 64
        predecessor_ledger_sha = "b" * 64
        discovery_window = {
            "role": "discovery",
            "sessionAnchors": ["2026-01-02"],
        }
        split = {
            "manifestSha256": "s" * 64,
            "windows": {"discovery": discovery_window},
        }
        provenance = {
            "predecessorRunId": 30000411128,
            "reusedOutcomeBlindInputs": {
                "fresh_entry_bank_v1.json": entry_bank_sha,
            },
        }
        required_bundle_paths = {
            name: repository / name
            for name in (
                "fresh_source_inventory_v1.json",
                "fresh_corpus_manifest_v1.json",
                "fresh_split_manifest_v2.json",
                "fresh_research_state_binding_v2.json",
                "fresh_experiment_ledger_v1.jsonl",
                "fresh_preregistration_v3.json",
                "fresh_implementation_manifest_v1.json",
                "fresh_quantile_bank_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            )
        }
        bundle = SimpleNamespace(
            inventory={"inventorySha256": "i" * 64},
            corpus={"corpusManifestSha256": "c" * 64},
            split=split,
            provenance=provenance,
            predecessor_state_binding={"stateDirectory": str(state)},
            quantile_bank={"bankSha256": "q" * 64},
            threshold_preflight={"allRegisteredThresholdDomainsResolved": True},
            paths=required_bundle_paths,
        )
        state_binding = {
            "studyId": "xauusd-fresh-causal-acceleration-v4",
            "studyLineageSha256": "a" * 64,
            "experimentLedgerPath": str(ledger),
            "predecessorExperimentLedgerPath": str(predecessor_ledger),
            "holdoutAuthorizationRegistryPath": str(holdout),
        }
        discovery_result = StageRunResult(
            stage="discovery",
            evaluated_ids=("candidate",),
            promoted_ids=(),
            ledger_record_numbers=(1,),
            study_failed=True,
        )
        frozen_discovery = MagicMock(return_value=discovery_result)
        search = SimpleNamespace(
            run_frozen_discovery=frozen_discovery,
            run_walk_forward_1=MagicMock(),
            run_walk_forward_2=MagicMock(),
            run_exit_search=MagicMock(),
            run_walk_forward_3=MagicMock(),
            run_validation=MagicMock(),
            audit_records=(),
        )
        build_entries = MagicMock(return_value=("entry-spec",))
        build_search = MagicMock(return_value=search)
        pipeline = SimpleNamespace(
            quantile_bank=None,
            threshold_preflight=None,
            stage_results=[],
            build_entry_candidates=build_entries,
            build_search=build_search,
        )
        implementation = {"manifestSha256": "m" * 64}
        preregistration = {"preregistrationSha256": "p" * 64}

        with (
            patch(
                "datavis.research.fresh_pipeline.load_fresh_v4_restart_bundle",
                return_value=bundle,
            ) as load_bundle,
            patch(
                "datavis.research.fresh_pipeline._research_state_binding_v4",
                return_value=state_binding,
            ) as bind_state,
            patch(
                "datavis.research.fresh_pipeline._snapshot_new_file",
            ),
            patch(
                "datavis.research.fresh_pipeline._file_sha256",
                side_effect=(predecessor_ledger_sha, entry_bank_sha),
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN17_LEDGER_SHA256",
                predecessor_ledger_sha,
            ),
            patch(
                "datavis.research.fresh_pipeline.build_fresh_implementation_manifest",
                return_value=implementation,
            ),
            patch(
                "datavis.research.fresh_pipeline.build_fresh_preregistration_v4",
                return_value=preregistration,
            ) as build_preregistration,
            patch(
                "datavis.research.fresh_pipeline.fresh_quantile_bank_from_payload",
                return_value="bound-quantile-bank",
            ),
            patch(
                "datavis.research.fresh_pipeline.RegisteredFreshResearchPipeline",
                return_value=pipeline,
            ) as construct_pipeline,
        ):
            summary = run_registered_fresh_research(
                lambda: None,
                repository_root=repository,
                output_directory=output,
                research_state_directory=state,
                scratch_directory=scratch,
                infrastructure_restart_v4_artifact_directory="run17",
            )

        load_bundle.assert_called_once_with("run17")
        bind_state.assert_called_once_with(
            state.resolve(),
            split,
            provenance,
            bundle.predecessor_state_binding,
        )
        construct_pipeline.assert_called_once()
        self.assertEqual(
            construct_pipeline.call_args.kwargs["spool_directory"],
            scratch.resolve(),
        )
        build_preregistration.assert_called_once()
        self.assertIs(
            build_preregistration.call_args.kwargs[
                "infrastructure_restart_provenance"
            ],
            provenance,
        )
        self.assertEqual(
            build_entries.call_args.args[1].windows[0].session_anchors,
            ("2026-01-02",),
        )
        frozen_discovery.assert_called_once_with(
            threshold_bank=bundle.quantile_bank,
            entry_specs=("entry-spec",),
        )
        for operation in (
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        ):
            operation.assert_not_called()
        self.assertEqual(summary["infrastructureRestartVersion"], 4)
        self.assertEqual(summary["predecessorRunId"], 30000411128)
        self.assertFalse(summary["holdoutOpened"])

    def test_research_scratch_must_not_overlap_output_or_durable_state(self):
        repository = Path(__file__).resolve().parent
        state = self._spool_test_output()
        scratch_parent = self._spool_test_output()
        output_inside_scratch = scratch_parent / "output"
        output_inside_scratch.mkdir()

        with self.assertRaisesRegex(ValueError, "scratch must be separate"):
            run_registered_fresh_research(
                lambda: None,
                repository_root=repository,
                output_directory=output_inside_scratch,
                research_state_directory=state,
                scratch_directory=scratch_parent,
            )

        output = self._spool_test_output()
        with self.assertRaisesRegex(ValueError, "scratch must be separate"):
            run_registered_fresh_research(
                lambda: None,
                repository_root=repository,
                output_directory=output,
                research_state_directory=state,
                scratch_directory=state,
            )

    def test_research_scratch_must_be_empty_before_outcome_access(self):
        repository = Path(__file__).resolve().parent
        output = self._spool_test_output()
        state = self._spool_test_output()
        scratch = self._spool_test_output()
        marker = scratch / "unexpected"
        marker.write_text("not empty", encoding="utf-8")

        with self.assertRaisesRegex(FileExistsError, "scratch directory must be empty"):
            run_registered_fresh_research(
                lambda: None,
                repository_root=repository,
                output_directory=output,
                research_state_directory=state,
                scratch_directory=scratch,
            )

    def test_v4_restart_requires_an_explicit_separate_scratch(self):
        repository = Path(__file__).resolve().parent
        output = self._spool_test_output()
        state = self._spool_test_output()

        with self.assertRaisesRegex(ValueError, "explicit separate scratch"):
            run_registered_fresh_research(
                lambda: None,
                repository_root=repository,
                output_directory=output,
                research_state_directory=state,
                infrastructure_restart_v4_artifact_directory="unused",
            )
        with self.assertRaisesRegex(ValueError, "explicit separate scratch"):
            run_registered_fresh_research(
                lambda: None,
                repository_root=repository,
                output_directory=output,
                research_state_directory=state,
                scratch_directory=output,
                infrastructure_restart_v4_artifact_directory="unused",
            )

    def test_cli_requires_explicit_execute(self):
        with self.assertRaisesRegex(SystemExit, "without --execute"):
            main(["--output-dir", "unused"])
        with self.assertRaisesRegex(SystemExit, "durable --research-state-dir"):
            main(["--output-dir", "unused", "--execute"])

    def test_cli_passes_separate_scratch_directory(self):
        with patch(
            "datavis.research.fresh_pipeline_cli.run_registered_fresh_research",
            return_value={"status": "complete", "holdoutOpened": False},
        ) as run:
            self.assertEqual(
                main(
                    [
                        "--output-dir",
                        "output",
                        "--scratch-dir",
                        "scratch",
                        "--restart-v4-artifact-dir",
                        "restart-v4",
                        "--research-state-dir",
                        "state",
                        "--execute",
                    ]
                ),
                0,
            )
        self.assertEqual(run.call_args.kwargs["scratch_directory"], "scratch")
        self.assertEqual(
            run.call_args.kwargs[
                "infrastructure_restart_v4_artifact_directory"
            ],
            "restart-v4",
        )


if __name__ == "__main__":
    unittest.main()
