"""Focused, outcome-independent tests for the detached-v5 terminal auditor."""

from __future__ import annotations

import copy
from dataclasses import asdict
import gzip
import hashlib
import io
import json
import tarfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from datavis.research import fresh_terminal_audit as audit
from datavis.research.fresh_pipeline import _EntryEdgeSummary, _entry_rank_score
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_scoring import (
    EntryMetrics,
    EntryScoreReport,
    TradeMetrics,
    evaluate_entry_gate,
    scoring_config_from_preregistration,
)
from datavis.research.fresh_search import (
    EntryCandidateSpec,
    FrozenEntryCandidate,
    FrozenStrategyCandidate,
    StrategyCandidateSpec,
)
from test_fresh_preregistration_v5 import _build_v5, _split_manifest


def _launch_receipt() -> dict:
    run_id = audit.FROZEN_RUN_ID
    attempt = audit.FROZEN_RUN_ATTEMPT
    return {
        "schema": audit.RECEIPT_SCHEMA,
        "kind": "launch_ready",
        "status": "running",
        "processExitStatus": None,
        "githubRunId": run_id,
        "githubRunAttempt": attempt,
        "branch": audit.FROZEN_RUN_BRANCH,
        "commitSha": audit.FROZEN_RUN_COMMIT,
        "studyLineageSha256": audit.RUN19_V5_STUDY_LINEAGE_SHA256,
        "run19ArtifactId": 8585919266,
        "run19TerminalArchiveSha256": (
            "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
        ),
        "controllerSha256": audit.FROZEN_CONTROLLER_SHA256,
        "controllerPid": 486270,
        "controllerStartTicks": 1069712502,
        "pipelinePid": 486543,
        "pipelineStartTicks": 1069715129,
        "paths": {
            "worktree": "/tmp/fresh-xauusd-worktree.E4Jrbc",
            "output": "/tmp/fresh-xauusd-output.eVrX3i",
            "scratch": (
                "/home/ec2-user/.local/state/datavis/"
                f"fresh-xauusd-scratch-v1/run.{run_id}.{attempt}.QrG3VH"
            ),
            "restart": "/tmp/fresh-xauusd-restart.KJ622R",
            "transfer": "/tmp/fresh-xauusd-transfer.SEMXw4",
            "terminalArchive": (
                "/home/ec2-user/.local/state/datavis/"
                f"fresh-xauusd-artifacts-v1/fresh-xauusd-{run_id}-{attempt}.tgz"
            ),
            "serverLog": "/tmp/fresh-xauusd-run.c5EeE6.log",
        },
        "terminalArchive": None,
    }


def _enrich(bodies: list[dict]) -> list[dict]:
    return [
        {
            "recordNumber": number,
            "recordSha256": canonical_hash(body),
            **body,
        }
        for number, body in enumerate(bodies, start=1)
    ]


def _ledger_bytes(records: list[dict]) -> bytes:
    return b"".join(audit._canonical_compact_bytes(item) for item in records)


def _json_round_trip(value: object) -> object:
    return json.loads(json.dumps(value, allow_nan=False))


def _empty_entry_metrics(session_count: int) -> EntryMetrics:
    return EntryMetrics(
        signal_count=0,
        filled_count=0,
        rejected_count=0,
        censored_count=0,
        fill_rate=None,
        censored_fraction=None,
        coverage_probabilities=tuple(
            (checkpoint, None) for checkpoint in (1, 2, 5, 10, 20, 30, 60)
        ),
        restricted_median_coverage_milliseconds=None,
        median_covered_time_milliseconds=None,
        barrier_profit_first_count=0,
        barrier_loss_first_count=0,
        barrier_no_hit_count=0,
        barrier_profit_first_rate=None,
        rejection_reason_counts=(),
        censor_reason_counts=(),
        evaluated_session_count=session_count,
        active_session_count=0,
        active_session_fraction=0.0,
        profit_barrier_net_per_unit=0.25,
        loss_barrier_net_per_unit=0.25,
    )


def _compact_empty_entry(anchors: tuple[str, ...]) -> dict:
    empty_slice = {
        "filledCount": 0,
        "coverage10": None,
        "coverage30": None,
        "coverage60": None,
        "barrierProfitFirstRate": None,
    }
    return {
        "overall": _json_round_trip(asdict(_empty_entry_metrics(len(anchors)))),
        "byDay": [
            {"label": anchor, **empty_slice} for anchor in sorted(anchors)
        ],
        "bySide": [],
        "byMarketSession": [],
        "byRegime": [],
    }


def _entry_edge_empty() -> _EntryEdgeSummary:
    return _EntryEdgeSummary(
        expected_barrier_pnl_per_fill=None,
        median_mae_before_coverage=None,
        median_mfe_horizon=None,
        p90_restricted_coverage_ms=None,
        failure_to_cover_60s=None,
        coverage_10_cluster_interval=None,
        coverage_30_cluster_interval=None,
        baseline_coverage_10=None,
        baseline_coverage_30=None,
        uplift_10=None,
        uplift_30=None,
        uplift_10_cluster_interval=None,
        uplift_30_cluster_interval=None,
        baseline_gate_passed=False,
    )


class FreshTerminalAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.preregistration = _build_v5()
        cls.split = _split_manifest()
        cls.preregistration_sha = cls.preregistration["preregistrationSha256"]

    def _terminal_receipt(self, exit_status: int = 17) -> tuple[bytes, bytes]:
        launch = _launch_receipt()
        launch_raw = audit._canonical_compact_bytes(launch)
        self.assertEqual(len(launch_raw), audit.FROZEN_LAUNCH_RECEIPT_SIZE)
        self.assertEqual(
            hashlib.sha256(launch_raw).hexdigest(),
            audit.FROZEN_LAUNCH_RECEIPT_SHA256,
        )
        terminal = copy.deepcopy(launch)
        terminal.update(
            {
                "kind": "terminal",
                "status": "failed",
                "processExitStatus": exit_status,
                "terminalArchive": {
                    "size": 1234,
                    "sha256": "f" * 64,
                    "device": 2049,
                    "inode": 987654,
                },
            }
        )
        return launch_raw, audit._canonical_compact_bytes(terminal)

    def _frozen_discovery(self) -> audit._FrozenAuditInputs:
        entries: list[FrozenEntryCandidate] = []
        rows: dict[str, dict] = {}
        sources: dict[str, object] = {}
        for ordinal in range(240):
            candidate_id = f"entry-{ordinal:03d}-rank-base-frozen"
            family = f"family-{ordinal // 60}"
            candidate = FrozenEntryCandidate.freeze(
                EntryCandidateSpec(
                    candidate_id=candidate_id,
                    family=family,
                    config={"ordinal": ordinal},
                    entry_variant="synthetic-entry",
                ),
                threshold_bank_sha256="a" * 64,
            )
            entries.append(candidate)
            rows[candidate_id] = {
                "robustnessGroup": f"group-{ordinal // 3}",
                "sourceConfigSha256": f"{ordinal:064x}",
            }
            sources[candidate_id] = SimpleNamespace(
                rank_offset=0.0,
                threshold_provenance=(
                    SimpleNamespace(parameter="x", final_value=ordinal),
                ),
            )
        return audit._FrozenAuditInputs(
            entries=tuple(entries),
            entries_by_id={item.candidate_id: item for item in entries},
            entry_bank_by_id=rows,
            entry_source_by_id=sources,
            strategies=(),
            strategies_by_id={},
            exit_variant_by_strategy_id={},
        )

    def _discovery_ledger(
        self,
        frozen: audit._FrozenAuditInputs,
    ) -> list[dict]:
        training, evaluation, role, kind, window_sha = audit._stage_context(
            self.split,
            "discovery",
        )
        identifiers = [item.candidate_id for item in frozen.entries]
        identities = [item.entry_sha256 for item in frozen.entries]
        bodies = [
            audit._expected_stage_access_record(
                stage="discovery",
                training=training,
                evaluation=evaluation,
                role=role,
                window_sha=window_sha,
                preregistration_sha=self.preregistration_sha,
            ),
            audit._expected_batch_access_record(
                kind=kind,
                status="batch_access_started",
                stage="discovery",
                training=training,
                evaluation=evaluation,
                role=role,
                candidate_ids=identifiers,
                candidate_sha256=identities,
                window_sha=window_sha,
                preregistration_sha=self.preregistration_sha,
            ),
            audit._expected_batch_access_record(
                kind=kind,
                status="batch_access_completed",
                stage="discovery",
                training=training,
                evaluation=evaluation,
                role=role,
                candidate_ids=identifiers,
                candidate_sha256=identities,
                window_sha=window_sha,
                preregistration_sha=self.preregistration_sha,
            ),
            *[
                {"stage": "discovery", "candidateId": identifier}
                for identifier in identifiers
            ],
        ]
        return _enrich(bodies)

    def _verify_discovery_with_mocked_metrics(
        self,
        *,
        passed_scores: dict[str, float],
        promoted: list[str],
        failed: bool,
    ) -> tuple[str, bool]:
        frozen = self._frozen_discovery()
        records = self._discovery_ledger(frozen)
        identifiers = [item.candidate_id for item in frozen.entries]
        summary = {
            "status": "no_robust_setup_survived_frozen_validation",
            "holdoutOpened": False,
            "strongestRecord": None,
            "stageResults": [
                {
                    "stage": "discovery",
                    "evaluated_ids": identifiers,
                    "promoted_ids": promoted,
                    "ledger_record_numbers": list(
                        range(1, len(records) + 1)
                    ),
                    "study_failed": failed,
                }
            ],
        }

        def fake_audit(_record: dict, *, candidate: object, **_kwargs: object) -> dict:
            candidate_id = candidate.candidate_id
            return {
                "candidateId": candidate_id,
                "score": passed_scores.get(candidate_id, 0.0),
            }

        def fake_finalize(items: list[dict], **_kwargs: object) -> list[dict]:
            return [
                {
                    **item,
                    "passed": item["candidateId"] in passed_scores,
                }
                for item in items
            ]

        with (
            patch.object(
                audit,
                "_audit_entry_candidate_record",
                side_effect=fake_audit,
            ),
            patch.object(
                audit,
                "_finalize_candidate_neighbourhoods",
                side_effect=fake_finalize,
            ),
        ):
            return audit._verify_stage_and_ledger(
                summary,
                records,
                self.preregistration_sha,
                split=self.split,
                preregistration=self.preregistration,
                frozen=frozen,
            )

    def test_process_failure_is_not_reported_as_strategy_no_pass(self) -> None:
        launch_raw, terminal_raw = self._terminal_receipt()
        _launch, terminal, _archive = audit._verify_receipts(
            launch_raw, terminal_raw
        )
        result = audit._infrastructure_failure_report(
            terminal["processExitStatus"]
        )
        self.assertEqual(result["status"], "infrastructure_failure")
        self.assertFalse(result["scientificConclusionAvailable"])
        self.assertNotIn("no_robust_setup", json.dumps(result))

    def test_terminal_receipt_tampering_is_rejected_before_classification(self) -> None:
        launch_raw, terminal_raw = self._terminal_receipt()
        terminal = json.loads(terminal_raw)
        terminal["commitSha"] = "0" * 40
        with self.assertRaisesRegex(audit.FreshTerminalAuditError, "changed"):
            audit._verify_receipts(
                launch_raw,
                audit._canonical_compact_bytes(terminal),
            )

    def test_adoption_is_pinned_to_only_the_accepted_r1_r2_executions(self) -> None:
        launch = _launch_receipt()
        archive_name = (
            f"fresh-xauusd-{audit.FROZEN_RUN_ID}-{audit.FROZEN_RUN_ATTEMPT}.tgz"
        )
        identities = {
            "fresh-xauusd-v5-launch-receipt.json": (10, "a" * 64),
            "fresh-xauusd-v5-terminal-receipt.json": (20, "b" * 64),
            archive_name: (30, "c" * 64),
        }
        adoption_run, adoption_attempt, adoption_commit = next(
            iter(audit.FROZEN_ADOPTION_EXECUTIONS)
        )
        manifest = {
            "schema": audit.ADOPTION_SCHEMA,
            "source": {
                "githubRunId": audit.FROZEN_RUN_ID,
                "githubRunAttempt": audit.FROZEN_RUN_ATTEMPT,
                "commitSha": audit.FROZEN_RUN_COMMIT,
                "launchArtifactId": audit.FROZEN_LAUNCH_ARTIFACT_ID,
                "launchArtifactDigest": audit.FROZEN_LAUNCH_ARTIFACT_DIGEST,
                "launchArtifactSize": audit.FROZEN_LAUNCH_ARTIFACT_SIZE,
            },
            "adoption": {
                "githubRunId": adoption_run,
                "githubRunAttempt": adoption_attempt,
                "commitSha": adoption_commit,
                "remoteMutation": False,
            },
            "members": [
                {"name": name, "size": size, "sha256": sha}
                for name, (size, sha) in identities.items()
            ],
        }
        raw = audit._canonical_compact_bytes(manifest)
        audit._verify_adoption_manifest(raw, manifest, identities, launch)

        changed = copy.deepcopy(manifest)
        changed["adoption"]["githubRunId"] += 99
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "not pinned",
        ):
            audit._verify_adoption_manifest(
                audit._canonical_compact_bytes(changed),
                changed,
                identities,
                launch,
            )
        type_confused = copy.deepcopy(manifest)
        type_confused["source"]["githubRunAttempt"] = True
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "source identity",
        ):
            audit._verify_adoption_manifest(
                audit._canonical_compact_bytes(type_confused),
                type_confused,
                identities,
                launch,
            )

    def test_unknown_and_resume_record_kinds_are_rejected(self) -> None:
        for kind in ("infrastructure-resume", "future-protocol-kind"):
            with self.subTest(kind=kind):
                with self.assertRaises(audit.FreshTerminalAuditError):
                    audit._candidate_records([{"recordKind": kind}])

    def test_clean_scientific_no_pass_requires_a_terminal_failed_gate(self) -> None:
        terminal, validated = self._verify_discovery_with_mocked_metrics(
            passed_scores={},
            promoted=[],
            failed=True,
        )
        self.assertEqual(terminal, "discovery")
        self.assertFalse(validated)

        first_id = self._frozen_discovery().entries[0].candidate_id
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "stopped before",
        ):
            self._verify_discovery_with_mocked_metrics(
                passed_scores={first_id: 0.5},
                promoted=[first_id],
                failed=False,
            )

    def test_promotions_must_equal_exact_production_ranking(self) -> None:
        frozen = self._frozen_discovery()
        first = frozen.entries[0].candidate_id
        second = frozen.entries[1].candidate_id
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "promotion ranking",
        ):
            self._verify_discovery_with_mocked_metrics(
                passed_scores={first: 0.1, second: 0.9},
                promoted=[first, second],
                failed=False,
            )

    def test_empty_metrics_cannot_be_promoted_by_claimed_gate_passed(self) -> None:
        candidate = FrozenEntryCandidate.freeze(
            EntryCandidateSpec(
                candidate_id="entry-test-rank-base-frozen",
                family="synthetic",
                config={"causal": True},
                entry_variant="synthetic-entry",
            ),
            threshold_bank_sha256="a" * 64,
        )
        stage = "walk_forward_1"
        training, evaluation, role, _kind, window_sha = audit._stage_context(
            self.split,
            stage,
        )
        anchors = audit._evaluation_anchors(self.split, stage)
        entry = _empty_entry_metrics(len(anchors))
        report = EntryScoreReport(entry, (), (), (), ())
        edge = _entry_edge_empty()
        scoring = scoring_config_from_preregistration(self.preregistration)
        gate = evaluate_entry_gate(
            entry,
            minimum_sample=scoring.minimum_sample,
            thresholds=scoring.entry_gate,
        )
        score = _entry_rank_score(report, edge)
        group = "synthetic-group"
        neighbourhood = {
            "centerCandidateId": candidate.candidate_id,
            "evaluatedCount": 1,
            "passed": True,
            "group": group,
            "requiredDuringStage": False,
            "candidateIsCenter": True,
        }
        record = {
            "recordNumber": 1,
            "recordSha256": "0" * 64,
            "candidateId": candidate.candidate_id,
            "family": candidate.family,
            "stage": stage,
            "trainingWindow": "+".join(training),
            "evaluationWindow": "+".join(evaluation),
            "parameters": {
                "entryConfig": candidate.config,
                "thresholdBankSha256": candidate.threshold_bank_sha256,
            },
            "entryVariant": candidate.entry_variant,
            "exitVariant": "entry-edge-only",
            "metrics": {
                "entry": _compact_empty_entry(anchors),
                "registeredGate": _json_round_trip(asdict(gate)),
                "entryEdge": _json_round_trip(asdict(edge)),
                "parameterNeighbourhood": neighbourhood,
            },
            "status": "passed",
            "leakageChecks": dict(audit._ENTRY_LEAKAGE_CHECKS),
            "role": role,
            "outcomesRevealed": True,
            "gatePassed": True,
            "identitySha256": candidate.entry_sha256,
            "frozenEntrySha256": candidate.entry_sha256,
            "frozenStrategySha256": None,
            "windowSha256": window_sha,
            "balancedScore": score,
            "preregistrationSha256": self.preregistration_sha,
        }
        provisional = audit._audit_entry_candidate_record(
            record,
            candidate=candidate,
            bank_row={"robustnessGroup": group},
            source_candidate=SimpleNamespace(
                rank_offset=0.0,
                threshold_provenance=(
                    SimpleNamespace(parameter="x", final_value=1.0),
                ),
            ),
            stage=stage,
            split=self.split,
            preregistration=self.preregistration,
            preregistration_sha=self.preregistration_sha,
            scoring=scoring,
        )
        self.assertFalse(provisional["basePassed"])
        for changed in (
            {**copy.deepcopy(record), "outcomesRevealed": 1},
            {
                **copy.deepcopy(record),
                "leakageChecks": {
                    key: int(value)
                    for key, value in audit._ENTRY_LEAKAGE_CHECKS.items()
                },
            },
            {
                **copy.deepcopy(record),
                "parameters": {
                    **record["parameters"],
                    "entryConfig": {"causal": 1},
                },
            },
        ):
            with self.assertRaises(audit.FreshTerminalAuditError):
                audit._audit_entry_candidate_record(
                    changed,
                    candidate=candidate,
                    bank_row={"robustnessGroup": group},
                    source_candidate=SimpleNamespace(
                        rank_offset=0.0,
                        threshold_provenance=(
                            SimpleNamespace(parameter="x", final_value=1.0),
                        ),
                    ),
                    stage=stage,
                    split=self.split,
                    preregistration=self.preregistration,
                    preregistration_sha=self.preregistration_sha,
                    scoring=scoring,
                )
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "pass status",
        ):
            audit._finalize_candidate_neighbourhoods(
                [provisional],
                stage=stage,
                preregistration=self.preregistration,
            )

    def test_aggregate_metric_inconsistencies_are_rejected(self) -> None:
        entry = EntryMetrics(
            signal_count=30,
            filled_count=30,
            rejected_count=0,
            censored_count=0,
            fill_rate=1.0,
            censored_fraction=0.0,
            coverage_probabilities=(
                (1, 0.2),
                (2, 0.3),
                (5, 0.4),
                (10, 0.5),
                (20, 17 / 30),
                (30, 0.6),
                (60, 20 / 30),
            ),
            restricted_median_coverage_milliseconds=20_000.0,
            median_covered_time_milliseconds=10_000.0,
            barrier_profit_first_count=16,
            barrier_loss_first_count=14,
            barrier_no_hit_count=0,
            barrier_profit_first_rate=16 / 30,
            rejection_reason_counts=(),
            censor_reason_counts=(),
            evaluated_session_count=10,
            active_session_count=6,
            active_session_fraction=0.6,
            profit_barrier_net_per_unit=0.25,
            loss_barrier_net_per_unit=0.25,
        )
        payload = _json_round_trip(asdict(entry))
        parsed = audit._entry_metrics(payload, expected_sessions=10)
        self.assertEqual(parsed.filled_count, 30)
        payload["coverage_probabilities"][5][1] = 0.615
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "integer outcomes",
        ):
            audit._entry_metrics(payload, expected_sessions=10)

        sessions = tuple(
            (f"2025-01-{day:02d}", 1.0 if day <= 6 else 0.0)
            for day in range(1, 11)
        )
        trade = TradeMetrics(
            trade_count=30,
            win_count=18,
            loss_count=12,
            flat_count=0,
            win_rate=0.6,
            net_pnl=6.0,
            expectancy=0.2,
            gross_profit=18.0,
            gross_loss=12.0,
            profit_factor=1.5,
            maximum_drawdown=3.0,
            maximum_drawdown_to_gross_profit=1 / 6,
            median_absolute_trade_pnl=1.0,
            loss_95_absolute=1.0,
            median_absolute_loss=1.0,
            loss_95_to_median_absolute_loss=1.0,
            largest_trade_share_of_gross_profit=1 / 18,
            positive_trade_profit_hhi=1 / 18,
            largest_session_share_of_gross_profit=1 / 18,
            positive_session_profit_hhi=1 / 54,
            evaluated_session_count=10,
            active_session_count=10,
            active_session_fraction=1.0,
            positive_session_count=6,
            positive_session_fraction=0.6,
            session_net_pnl=sessions,
            replay_censor_count=0,
            profitability_valid=True,
        )
        trade_payload = _json_round_trip(asdict(trade))
        anchors = tuple(label for label, _ in sessions)
        parsed_trade = audit._trade_metrics(
            trade_payload,
            expected_anchors=anchors,
            pnl_tolerance=1e-12,
        )
        self.assertTrue(parsed_trade.profitability_valid)
        trade_payload["loss_95_to_median_absolute_loss"] = 2.0
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "loss-tail",
        ):
            audit._trade_metrics(
                trade_payload,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        impossible_tail = _json_round_trip(asdict(trade))
        impossible_tail["loss_95_absolute"] = 0.5
        impossible_tail["loss_95_to_median_absolute_loss"] = 0.5
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "loss-tail magnitudes",
        ):
            audit._trade_metrics(
                impossible_tail,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        impossible_drawdown = _json_round_trip(asdict(trade))
        impossible_drawdown["maximum_drawdown"] = 0.5
        impossible_drawdown["maximum_drawdown_to_gross_profit"] = 0.5 / 18
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "loss-tail magnitudes",
        ):
            audit._trade_metrics(
                impossible_drawdown,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        impossible_median = _json_round_trip(asdict(trade))
        impossible_median["median_absolute_loss"] = 2.0
        impossible_median["loss_95_absolute"] = 2.0
        impossible_median["loss_95_to_median_absolute_loss"] = 1.0
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "loss-tail magnitudes",
        ):
            audit._trade_metrics(
                impossible_median,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        impossible_classification = _json_round_trip(asdict(trade))
        impossible_classification["gross_loss"] = 12e-12
        impossible_classification["net_pnl"] = 18.0 - 12e-12
        impossible_classification["expectancy"] = (
            impossible_classification["net_pnl"] / 30
        )
        impossible_classification["profit_factor"] = 18.0 / 12e-12
        impossible_classification["maximum_drawdown"] = 1e-12
        impossible_classification["maximum_drawdown_to_gross_profit"] = (
            1e-12 / 18.0
        )
        impossible_classification["loss_95_absolute"] = 1e-12
        impossible_classification["median_absolute_loss"] = 1e-12
        impossible_classification["loss_95_to_median_absolute_loss"] = 1.0
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "counts disagree",
        ):
            audit._trade_metrics(
                impossible_classification,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        offsetting_sessions = _json_round_trip(asdict(trade))
        offsetting_sessions["session_net_pnl"] = [
            [anchors[0], 10.0],
            [anchors[1], 10.0],
            [anchors[2], -14.0],
            *[[anchor, 0.0] for anchor in anchors[3:]],
        ]
        offsetting_sessions["positive_session_count"] = 2
        offsetting_sessions["positive_session_fraction"] = 0.2
        offsetting_sessions["largest_session_share_of_gross_profit"] = 10 / 18
        offsetting_sessions["positive_session_profit_hhi"] = 2 * (10 / 18) ** 2
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "session P&L magnitudes",
        ):
            audit._trade_metrics(
                offsetting_sessions,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        understated_drawdown = _json_round_trip(asdict(trade))
        understated_drawdown["session_net_pnl"] = [
            [anchors[0], 10.0],
            [anchors[1], -10.0],
            [anchors[2], 6.0],
            *[[anchor, 0.0] for anchor in anchors[3:]],
        ]
        understated_drawdown["positive_session_count"] = 2
        understated_drawdown["positive_session_fraction"] = 0.2
        understated_drawdown["largest_session_share_of_gross_profit"] = 10 / 18
        understated_drawdown["positive_session_profit_hhi"] = (
            (10 / 18) ** 2 + (6 / 18) ** 2
        )
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "chronological session drawdown",
        ):
            audit._trade_metrics(
                understated_drawdown,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )
        censored = _json_round_trip(asdict(trade))
        censored["replay_censor_count"] = 1
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "profitability-valid",
        ):
            audit._trade_metrics(
                censored,
                expected_anchors=anchors,
                pnl_tolerance=1e-12,
            )

    def test_holdout_artifact_is_the_exact_reconstructed_strategy(self) -> None:
        entry = FrozenEntryCandidate.freeze(
            EntryCandidateSpec(
                candidate_id="entry-rank-base-frozen",
                family="synthetic",
                config={"signal": "causal"},
                entry_variant="entry",
            ),
            threshold_bank_sha256="a" * 64,
        )
        strategy_candidate = FrozenStrategyCandidate.freeze(
            StrategyCandidateSpec(
                strategy_id="winner",
                entry_candidate_id=entry.candidate_id,
                exit_config={"stop": "observed-quote"},
                execution_config={"fills": "strictly-later"},
                exit_variant="exit",
            ),
            entries_by_id={entry.candidate_id: entry},
        )
        frozen = audit._FrozenAuditInputs(
            entries=(entry,),
            entries_by_id={entry.candidate_id: entry},
            entry_bank_by_id={entry.candidate_id: {}},
            entry_source_by_id={entry.candidate_id: object()},
            strategies=(strategy_candidate,),
            strategies_by_id={
                strategy_candidate.strategy_id: strategy_candidate
            },
            exit_variant_by_strategy_id={strategy_candidate.strategy_id: {}},
        )
        strategy = {
            "schema": "fresh-xauusd-final-strategy/v1",
            "strategyId": strategy_candidate.strategy_id,
            "strategySha256": strategy_candidate.strategy_sha256,
            "entryCandidateId": entry.candidate_id,
            "entrySha256": entry.entry_sha256,
            "entryConfig": entry.config,
            "exitConfig": strategy_candidate.exit_config,
            "executionConfig": strategy_candidate.execution_config,
            "noPostHoldoutTuning": True,
        }
        records: list[dict] = []
        for number, role in enumerate(
            ("walk_forward_3", "validation", "holdout"),
            start=1,
        ):
            records.append(
                {
                    "recordNumber": number,
                    "recordSha256": f"{number:064x}",
                    "candidateId": strategy_candidate.strategy_id,
                    "role": role,
                    "gatePassed": True,
                    "frozenStrategySha256": strategy_candidate.strategy_sha256,
                    "frozenEntrySha256": entry.entry_sha256,
                    "identitySha256": strategy_candidate.strategy_sha256,
                    "parameters": {
                        "entryConfig": entry.config,
                        "exitConfig": strategy_candidate.exit_config,
                        "executionConfig": strategy_candidate.execution_config,
                    },
                    "leakageChecks": dict(audit._STRATEGY_LEAKAGE_CHECKS),
                }
            )
        authorization_body = {
            "schemaVersion": 2,
            "role": "holdout",
            "window": self.split["windows"]["holdout"],
            "splitManifestSha256": self.split["manifestSha256"],
            "frozenStrategySha256": strategy_candidate.strategy_sha256,
            "outcomesRevealed": False,
            "preregistrationSha256": self.preregistration_sha,
            "walkForward3EvidenceSha256": canonical_hash(records[0]),
            "validationEvidenceSha256": canonical_hash(records[1]),
        }
        authorization = {
            **authorization_body,
            "authorizationSha256": canonical_hash(authorization_body),
        }
        selected = audit._verify_holdout_evidence(
            split=self.split,
            preregistration_sha=self.preregistration_sha,
            records=records,
            authorization=authorization,
            strategy=strategy,
            validated=True,
            frozen=frozen,
        )
        self.assertEqual(selected["role"], "holdout")

        changed = copy.deepcopy(strategy)
        changed["exitConfig"]["stop"] = "invented"
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "final strategy",
        ):
            audit._verify_holdout_evidence(
                split=self.split,
                preregistration_sha=self.preregistration_sha,
                records=records,
                authorization=authorization,
                strategy=changed,
                validated=True,
                frozen=frozen,
            )
        type_confused = copy.deepcopy(strategy)
        type_confused["noPostHoldoutTuning"] = 1
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "final strategy",
        ):
            audit._verify_holdout_evidence(
                split=self.split,
                preregistration_sha=self.preregistration_sha,
                records=records,
                authorization=authorization,
                strategy=type_confused,
                validated=True,
                frozen=frozen,
            )

    def test_strategy_entry_evidence_and_winner_selection_are_exact(self) -> None:
        first = {
            "record": {
                "metrics": {
                    "entry": {"overall": {"filled_count": 10}},
                    "entryEdge": {"uplift_10": 0.1},
                }
            }
        }
        second = copy.deepcopy(first)
        audit._verify_strategy_stage_entry_invariance(
            [first, second],
            stage="exit_search",
        )
        second["record"]["metrics"]["entryEdge"]["uplift_10"] = 0.2
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "entry evidence changed",
        ):
            audit._verify_strategy_stage_entry_invariance(
                [first, second],
                stage="exit_search",
            )

        records = [
            {
                "candidateId": "entry",
                "frozenEntrySha256": "e" * 64,
                "frozenStrategySha256": None,
            },
            {
                "candidateId": "losing-exit",
                "frozenEntrySha256": "e" * 64,
                "frozenStrategySha256": "l" * 64,
            },
            {
                "candidateId": "winner",
                "frozenEntrySha256": "e" * 64,
                "frozenStrategySha256": "s" * 64,
            },
        ]
        selected = audit._winner_candidate_records(
            records,
            {
                "entrySha256": "e" * 64,
                "strategySha256": "s" * 64,
            },
        )
        self.assertEqual(
            [record["candidateId"] for record in selected],
            ["entry", "winner"],
        )

    def test_runtime_and_launch_implementation_are_exactly_pinned(self) -> None:
        expected = {
            "schema": "fresh-xauusd-implementation-manifest/v1",
            "repositoryRoot": "/tmp/fresh-xauusd-worktree.E4Jrbc",
            "files": [
                {
                    "path": path,
                    "sha256": audit.FROZEN_V5_IMPLEMENTATION_FILE_SHA256[path],
                }
                for path in sorted(audit.FROZEN_V5_IMPLEMENTATION_FILE_SHA256)
            ],
            "manifestSha256": audit.FROZEN_V5_IMPLEMENTATION_MANIFEST_SHA256,
        }
        audit._verify_frozen_launch_implementation(
            expected,
            launch_worktree="/tmp/fresh-xauusd-worktree.E4Jrbc",
        )

        def canonical_linux_sha(path, *, maximum_bytes):
            raw = path.read_bytes()
            if len(raw) > maximum_bytes:
                raise audit.FreshTerminalAuditError(
                    "test source exceeded its byte bound"
                )
            return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()

        with patch.object(
            audit,
            "_sha256_file",
            side_effect=canonical_linux_sha,
        ):
            audit._verify_local_frozen_scientific_runtime()
        self.assertEqual(
            audit.FROZEN_V5_IMPLEMENTATION_FILE_SHA256["datavis/db.py"],
            "e26524b82902441a2750311ad5ac5e6c31cb1e6140f2e9770470b058eebc3330",
        )
        self.assertEqual(
            audit.FROZEN_LOCAL_RUNTIME_CLOSURE_SHA256["datavis/db.py"],
            "7f3c8dc45ceed968ec4c935752ba85b3b172fb538fb1ff63de4baa1fcec48999",
        )

        changed = copy.deepcopy(expected)
        changed["files"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "exact frozen",
        ):
            audit._verify_frozen_launch_implementation(
                changed,
                launch_worktree="/tmp/fresh-xauusd-worktree.E4Jrbc",
            )

    def test_archive_root_headers_and_json_ambiguity_are_bounded(self) -> None:
        raw_archive = io.BytesIO()
        with tarfile.open(fileobj=raw_archive, mode="w:gz") as output:
            for _ in range(audit.MAX_ARCHIVE_MEMBERS + 1):
                root = tarfile.TarInfo(".")
                root.type = tarfile.DIRTYPE
                output.addfile(root)
        raw_archive.seek(0)
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "member-count",
        ):
            audit._archive_members(raw_archive)

        for raw in (b'{"a":1,"a":2}\n', b'{"a":NaN}\n'):
            with self.subTest(raw=raw):
                with self.assertRaises(audit.FreshTerminalAuditError):
                    audit._json_object(raw, "synthetic")

    def test_pax_metadata_cannot_bypass_decompressed_stream_bound(self) -> None:
        raw_archive = io.BytesIO()
        with tarfile.open(fileobj=raw_archive, mode="w:gz") as output:
            pax = tarfile.TarInfo("././@PaxHeader")
            pax.type = tarfile.XHDTYPE
            payload = b"a" * 4_096
            pax.size = len(payload)
            output.addfile(pax, io.BytesIO(payload))
        raw_archive.seek(0)

        with (
            patch.object(audit, "MAX_TAR_STREAM_BYTES", 1_024),
            self.assertRaisesRegex(
                audit.FreshTerminalAuditError,
                "decompressed tar stream",
            ),
        ):
            audit._archive_members(raw_archive)

    def test_physical_tar_contract_rejects_hidden_and_ambiguous_data(self) -> None:
        for member_type in (
            tarfile.XHDTYPE,
            tarfile.XGLTYPE,
            tarfile.GNUTYPE_LONGNAME,
            tarfile.GNUTYPE_LONGLINK,
            tarfile.GNUTYPE_SPARSE,
        ):
            with self.subTest(member_type=member_type):
                raw_archive = io.BytesIO()
                with tarfile.open(fileobj=raw_archive, mode="w:gz") as output:
                    hidden = tarfile.TarInfo("././@HiddenHeader")
                    hidden.type = member_type
                    payload = b"x\n"
                    hidden.size = len(payload)
                    output.addfile(hidden, io.BytesIO(payload))
                raw_archive.seek(0)
                with self.assertRaisesRegex(
                    audit.FreshTerminalAuditError,
                    "unsupported physical header",
                ):
                    audit._archive_members(raw_archive)

        plain = io.BytesIO()
        with tarfile.open(fileobj=plain, mode="w:") as output:
            member = tarfile.TarInfo("remote-exit-status.txt")
            payload = b"0\n"
            member.size = len(payload)
            output.addfile(member, io.BytesIO(payload))
        valid_tar = plain.getvalue()
        valid_gzip = gzip.compress(valid_tar)

        for suffix in (gzip.compress(valid_tar), b"\0", b"junk"):
            with self.subTest(suffix=suffix[:4]):
                with self.assertRaisesRegex(
                    audit.FreshTerminalAuditError,
                    "trailing gzip data",
                ):
                    audit._archive_members(io.BytesIO(valid_gzip + suffix))

        bad_padding = bytearray(valid_tar)
        bad_padding[tarfile.BLOCKSIZE + len(payload)] = 1
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "padding is nonzero",
        ):
            audit._archive_members(
                io.BytesIO(gzip.compress(bytes(bad_padding)))
            )

        huge = tarfile.TarInfo("remote-exit-status.txt")
        huge.size = audit.MAX_RECEIPT_BYTES + 1
        huge_tar = huge.tobuf() + b"\0" * (2 * tarfile.BLOCKSIZE)
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "size is unsafe",
        ):
            audit._archive_members(io.BytesIO(gzip.compress(huge_tar)))

        one_zero = (
            valid_tar[
                : 2 * tarfile.BLOCKSIZE
            ]
            + b"\0" * tarfile.BLOCKSIZE
        )
        with self.assertRaisesRegex(
            audit.FreshTerminalAuditError,
            "two-block terminator",
        ):
            audit._archive_members(io.BytesIO(gzip.compress(one_zero)))

        bundle, members = audit._archive_members(io.BytesIO(valid_gzip))
        try:
            self.assertEqual(
                audit._read_member(
                    bundle,
                    members,
                    "remote-exit-status.txt",
                ),
                payload,
            )
        finally:
            bundle.close()

    def test_ledger_hashes_are_recomputed_and_record_count_is_bounded(self) -> None:
        body = {"stage": "discovery", "candidateId": "candidate"}
        record = _enrich([body])[0]
        verified = audit._verified_ledger(_ledger_bytes([record]))
        self.assertEqual(verified[0]["candidateId"], "candidate")

        tampered = bytearray(_ledger_bytes([record]))
        tampered[tampered.index(b"candidate")] = ord("C")
        with self.assertRaises(audit.FreshTerminalAuditError):
            audit._verified_ledger(bytes(tampered))

        for invalid_number in (True, 1.0):
            with self.subTest(recordNumber=invalid_number):
                changed = copy.deepcopy(record)
                changed["recordNumber"] = invalid_number
                with self.assertRaises(audit.FreshTerminalAuditError):
                    audit._verified_ledger(_ledger_bytes([changed]))


if __name__ == "__main__":
    unittest.main()
