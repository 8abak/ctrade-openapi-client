from __future__ import annotations

import copy
import json
import unittest
import uuid
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

from datavis.research.fresh_preregistration import (
    FRESH_V2_WINDOW_POLICY,
    PREREGISTRATION_SCHEMA,
    authorize_registered_holdout,
    build_fresh_implementation_manifest,
    build_fresh_preregistration_v2,
    entry_barrier_diagnostic_configs_from_preregistration,
    entry_diagnostic_configs_from_preregistration,
    feature_configs_from_preregistration,
    required_fresh_implementation_files,
    replay_execution_config_for_candidate,
    replay_execution_configs_from_preregistration,
    session_audit_config_from_preregistration,
    validate_fresh_preregistration_v2,
)
from datavis.research.fresh_protocol import (
    append_fresh_record,
    build_fresh_split_manifest,
    canonical_hash,
)


TEST_ARTIFACTS = Path(__file__).resolve().parent / "artifacts" / "test-fresh-preregistration"
DEFAULT_LEDGER = TEST_ARTIFACTS / "unused-ledger.jsonl"
DEFAULT_REGISTRY = TEST_ARTIFACTS / "unused-holdout-authorization.json"
REPOSITORY_ROOT = Path(__file__).resolve().parent
IMPLEMENTATION_FILES = required_fresh_implementation_files()


def implementation_manifest() -> dict:
    return build_fresh_implementation_manifest(
        repository_root=REPOSITORY_ROOT,
        relative_paths=IMPLEMENTATION_FILES,
    )


def weekday_anchors(count: int, start: date = date(2025, 1, 2)) -> list[str]:
    anchors: list[str] = []
    cursor = start
    while len(anchors) < count:
        if cursor.weekday() < 5:
            anchors.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return anchors


def split_manifest(inventory: str = "a" * 64) -> dict:
    return build_fresh_split_manifest(
        weekday_anchors(FRESH_V2_WINDOW_POLICY.required_sessions),
        inventory_sha256=inventory,
        excluded_sessions=[
            {
                "sessionAnchor": "2025-01-01",
                "reason": "partial source coverage established before outcome inspection",
            }
        ],
        policy=FRESH_V2_WINDOW_POLICY,
    )


def preregistration(
    *,
    ledger_path: Path = DEFAULT_LEDGER,
    registry_path: Path = DEFAULT_REGISTRY,
) -> tuple[dict, dict]:
    split = split_manifest()
    prereg = build_fresh_preregistration_v2(
        split_manifest=split,
        corpus_manifest_sha256="b" * 64,
        protocol_code_identifier="tree:causal-protocol-v2",
        implementation_manifest=implementation_manifest(),
        experiment_ledger_path=ledger_path,
        holdout_authorization_registry_path=registry_path,
    )
    return split, prereg


def rehash_manifest(manifest: dict) -> dict:
    body = copy.deepcopy(manifest)
    body.pop("manifestSha256", None)
    body["manifestSha256"] = canonical_hash(body)
    return body


def build_with_split(split: dict) -> dict:
    return build_fresh_preregistration_v2(
        split_manifest=split,
        corpus_manifest_sha256="b" * 64,
        protocol_code_identifier="code",
        implementation_manifest=implementation_manifest(),
        experiment_ledger_path=DEFAULT_LEDGER,
        holdout_authorization_registry_path=DEFAULT_REGISTRY,
    )


def passed_evidence(prereg: dict, strategy_sha: str, role: str) -> dict:
    return {
        "candidateId": "fresh-final-1",
        "family": "trend-acceleration",
        "stage": role,
        "trainingWindow": "consumed-research",
        "evaluationWindow": role,
        "parameters": {"frozen": True},
        "entryVariant": "frozen-entry",
        "exitVariant": "frozen-exit",
        "metrics": {"allRegisteredGatesPassed": True},
        "status": "promoted",
        "leakageChecks": {"prefixInvariant": True},
        "role": role,
        "outcomesRevealed": True,
        "gatePassed": True,
        "frozenStrategySha256": strategy_sha,
        "preregistrationSha256": prereg["preregistrationSha256"],
    }


class FreshPreregistrationTests(unittest.TestCase):
    def test_builder_is_deterministic_json_safe_and_binds_source_hashes(self):
        split, first = preregistration()
        second = build_fresh_preregistration_v2(
            split_manifest=json.loads(json.dumps(split)),
            corpus_manifest_sha256="B" * 64,
            protocol_code_identifier="tree:causal-protocol-v2",
            implementation_manifest=implementation_manifest(),
            experiment_ledger_path=DEFAULT_LEDGER,
            holdout_authorization_registry_path=DEFAULT_REGISTRY,
        )
        self.assertEqual(first, second)
        self.assertEqual(first["schema"], PREREGISTRATION_SCHEMA)
        self.assertEqual(first["chronologicalWindowPolicy"]["totalEligibleSessions"], 139)
        self.assertEqual(first["sourceBindings"]["inventorySha256"], "a" * 64)
        self.assertEqual(first["sourceBindings"]["corpusManifestSha256"], "b" * 64)
        self.assertEqual(
            first["sourceBindings"]["implementationManifestSha256"],
            first["sourceBindings"]["implementationManifest"]["manifestSha256"],
        )
        self.assertEqual(
            validate_fresh_preregistration_v2(first),
            first["preregistrationSha256"],
        )
        persisted = json.loads(json.dumps(first, allow_nan=False, sort_keys=True))
        self.assertEqual(
            validate_fresh_preregistration_v2(persisted),
            first["preregistrationSha256"],
        )

    def test_split_must_be_exact_hash_valid_dst_aware_and_139_sessions(self):
        split = split_manifest()

        tampered = copy.deepcopy(split)
        tampered["windows"]["holdout"]["sessionAnchors"][0] = "2099-01-01"
        with self.assertRaisesRegex(ValueError, "hash"):
            build_with_split(tampered)

        wrong_schedule = copy.deepcopy(split)
        wrong_schedule["sessionSchedule"]["timezone"] = "Australia/Sydney"
        wrong_schedule = rehash_manifest(wrong_schedule)
        with self.assertRaisesRegex(ValueError, "DST-aware"):
            build_with_split(wrong_schedule)

        wrong_policy = copy.deepcopy(split)
        wrong_policy["policy"]["holdout_sessions"] = 35
        wrong_policy = rehash_manifest(wrong_policy)
        with self.assertRaisesRegex(ValueError, "window policy"):
            build_with_split(wrong_policy)

        non_iso = copy.deepcopy(split)
        old_anchor = non_iso["windows"]["discovery"]["sessionAnchors"][0]
        non_iso["windows"]["discovery"]["sessionAnchors"][0] = "x000"
        non_iso["windows"]["discovery"]["firstSessionAnchor"] = "x000"
        non_iso["assignments"][0]["sessionAnchor"] = "x000"
        non_iso = rehash_manifest(non_iso)
        with self.assertRaisesRegex(ValueError, "ISO weekday"):
            build_with_split(non_iso)

        overlapping_exclusion = copy.deepcopy(split)
        overlapping_exclusion["excludedSessionsBeforeOutcomeInspection"][0][
            "sessionAnchor"
        ] = old_anchor
        overlapping_exclusion = rehash_manifest(overlapping_exclusion)
        with self.assertRaisesRegex(ValueError, "disjoint"):
            build_with_split(overlapping_exclusion)

    def test_qc_and_feature_banks_materialize_without_defaults(self):
        _, prereg = preregistration()
        audit = session_audit_config_from_preregistration(prereg)
        self.assertEqual(
            asdict(audit),
            {
                "open_tolerance_seconds": 120.0,
                "close_tolerance_seconds": 120.0,
                "friday_close_tolerance_seconds": 600.0,
                "unexpected_gap_seconds": 300.0,
            },
        )

        configs = feature_configs_from_preregistration(prereg)
        self.assertEqual(len(configs), 9)
        self.assertEqual(
            configs[0].horizons_seconds,
            (0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
        )
        self.assertEqual(configs[0].ewma_half_lives_seconds, configs[0].horizons_seconds)
        self.assertEqual(configs[0].maximum_intertick_gap_ms, 5_000)
        self.assertEqual(
            {
                (item.kalman_acceleration_variance, item.kalman_measurement_variance)
                for item in configs
            },
            {
                (q, r)
                for q in (0.04, 0.16, 0.64)
                for r in (0.01, 0.04, 0.16)
            },
        )
        self.assertTrue(
            prereg["features"]["supportResistanceAndPivotRules"][
                "futureConfirmationForbidden"
            ]
        )

    def test_execution_scenarios_are_explicit_later_quote_and_costed(self):
        _, prereg = preregistration()
        configs = entry_diagnostic_configs_from_preregistration(prereg)
        self.assertEqual(
            list(configs),
            [
                "mechanics-zero-friction",
                "low-friction",
                "reference-provisional",
                "latency-stress",
                "friction-stress",
            ],
        )
        reference = configs["reference-provisional"]
        self.assertEqual(reference.entry_latency_ms, 250)
        self.assertEqual(reference.maximum_entry_lag_ms, 1_000)
        self.assertEqual(reference.diagnostic_horizon_ms, 60_000)
        self.assertEqual(reference.entry_slippage_per_unit, 0.02)
        self.assertEqual(reference.exit_slippage_per_unit, 0.02)
        self.assertEqual(reference.entry_commission_per_unit, 0.035)
        self.assertEqual(reference.exit_commission_per_unit, 0.035)
        convention = prereg["execution"]["fillConvention"]
        self.assertFalse(convention["decisionRowCanFill"])
        self.assertTrue(convention["equalTimestampHigherIdIsLater"])
        self.assertTrue(prereg["execution"]["priceConvention"]["observedSpreadAlwaysIncluded"])
        self.assertTrue(
            prereg["execution"]["calibrationLimitation"]["liveClaimForbiddenUntilMeasured"]
        )
        replay = replay_execution_configs_from_preregistration(prereg)
        replay_reference = replay["reference-provisional"]
        self.assertEqual(replay_reference.entry_latency_ms, 250)
        self.assertEqual(replay_reference.exit_latency_ms, 250)
        self.assertEqual(replay_reference.maximum_exit_lag_ms, 1_000)
        self.assertEqual(replay_reference.actual_fill_deadline_ms, 60_000)
        self.assertEqual(replay_reference.post_gap_rearm_ms, 5_000)
        self.assertEqual(replay_reference.slippage_per_side, 0.02)
        self.assertEqual(replay_reference.commission_per_unit_per_side, 0.035)
        barriers = entry_barrier_diagnostic_configs_from_preregistration(
            prereg, scenario_id="reference-provisional"
        )
        self.assertEqual(len(barriers), 16)
        equal_quarter = barriers[
            "reference-provisional:profit-0.25:loss-0.25"
        ]
        self.assertEqual(equal_quarter.profit_barrier_net_per_unit, 0.25)
        self.assertEqual(equal_quarter.loss_barrier_net_per_unit, 0.25)
        candidate_execution = replay_execution_config_for_candidate(
            prereg, scenario_id="reference-provisional", cooldown_ms=10_000
        )
        self.assertEqual(candidate_execution.cooldown_ms, 10_000)
        with self.assertRaisesRegex(ValueError, "candidate bank"):
            replay_execution_config_for_candidate(
                prereg, scenario_id="reference-provisional", cooldown_ms=180_000
            )

    def test_search_exit_robustness_and_holdout_rules_are_frozen(self):
        _, prereg = preregistration()
        budgets = prereg["candidateSearch"]["budgets"]
        self.assertEqual(
            budgets,
            {
                "discoveryDistinctCandidates": 240,
                "discoveryPerFamilyMaximum": 60,
                "walkForward1FrozenCandidates": 24,
                "walkForward2FrozenCandidates": 8,
                "exitVariantsAfterEntryGate": 96,
                "walkForward3FullStrategies": 3,
                "validationFullStrategies": 1,
                "holdoutFullStrategies": 1,
            },
        )
        self.assertTrue(prereg["exitResearch"]["entryMustPassBeforeExitSearch"])
        self.assertTrue(prereg["exitResearch"]["entryDefinitionFrozenDuringExitSearch"])
        self.assertEqual(
            prereg["exitResearch"]["candidateMaximumHoldingUpperBoundMilliseconds"],
            58_000,
        )
        self.assertEqual(
            prereg["entryDiagnostics"]["coverageCheckpointsSeconds"],
            [1, 2, 5, 10, 20, 30, 60],
        )
        neighborhood = prereg["robustnessAndGates"]["parameterNeighborhood"]
        self.assertEqual(neighborhood["continuousThresholdRankPerturbation"], [-0.05, 0.05])
        self.assertEqual(neighborhood["minimumPositiveExpectancyNeighborFraction"], 0.70)
        self.assertTrue(prereg["holdout"]["singleEvaluation"])
        self.assertEqual(prereg["holdout"]["maximumCandidates"], 1)
        self.assertEqual(
            prereg["robustnessAndGates"]["fullStrategyGates"][
                "fullReplayCensorCountMaximum"
            ],
            0,
        )
        self.assertIn("not registered", prereg["exitResearch"]["partialProfitTaking"])

    def test_tampering_is_detected(self):
        _, prereg = preregistration()
        modified = copy.deepcopy(prereg)
        modified["robustnessAndGates"]["fullStrategyGates"][
            "referenceProfitFactorMinimum"
        ] = 1.0
        with self.assertRaisesRegex(ValueError, "hash"):
            validate_fresh_preregistration_v2(modified)

        rehashed_invalid = copy.deepcopy(prereg)
        rehashed_invalid["execution"]["priceConvention"]["longEntry"] = "bid"
        rehashed_invalid.pop("preregistrationSha256")
        rehashed_invalid["preregistrationSha256"] = canonical_hash(rehashed_invalid)
        with self.assertRaisesRegex(ValueError, "canonical v2"):
            validate_fresh_preregistration_v2(rehashed_invalid)

    def test_holdout_requires_same_passed_validation_and_is_single_use(self):
        token = uuid.uuid4().hex
        ledger_path = TEST_ARTIFACTS / f"{token}-ledger.jsonl"
        registry_path = TEST_ARTIFACTS / f"{token}-authorization.json"
        self.addCleanup(ledger_path.unlink, missing_ok=True)
        self.addCleanup(ledger_path.with_name(ledger_path.name + ".lock").unlink, missing_ok=True)
        self.addCleanup(registry_path.unlink, missing_ok=True)
        split, prereg = preregistration(
            ledger_path=ledger_path,
            registry_path=registry_path,
        )
        strategy_sha = "c" * 64
        walk_forward_3 = append_fresh_record(
            ledger_path, passed_evidence(prereg, strategy_sha, "walk_forward_3")
        )
        validation = append_fresh_record(
            ledger_path, passed_evidence(prereg, strategy_sha, "validation")
        )

        with self.assertRaisesRegex(PermissionError, "validation outcome"):
            authorize_registered_holdout(
                preregistration=prereg,
                split_manifest=split,
                frozen_strategy_sha256=strategy_sha,
                walk_forward_3_record_number=walk_forward_3["recordNumber"],
                validation_record_number=walk_forward_3["recordNumber"],
                explicit_holdout_authorization=True,
            )

        with self.assertRaises(PermissionError):
            authorize_registered_holdout(
                preregistration=prereg,
                split_manifest=split,
                frozen_strategy_sha256=strategy_sha,
                walk_forward_3_record_number=walk_forward_3["recordNumber"],
                validation_record_number=validation["recordNumber"],
            )

        authorization = authorize_registered_holdout(
            preregistration=prereg,
            split_manifest=split,
            frozen_strategy_sha256=strategy_sha,
            walk_forward_3_record_number=walk_forward_3["recordNumber"],
            validation_record_number=validation["recordNumber"],
            explicit_holdout_authorization=True,
        )
        self.assertEqual(authorization["role"], "holdout")
        self.assertEqual(authorization["frozenStrategySha256"], strategy_sha)
        self.assertEqual(
            authorization["preregistrationSha256"], prereg["preregistrationSha256"]
        )
        self.assertEqual(
            authorization["authorizationSha256"],
            canonical_hash(
                {
                    key: value
                    for key, value in authorization.items()
                    if key != "authorizationSha256"
                }
            ),
        )
        self.assertEqual(json.loads(registry_path.read_text()), authorization)

        with self.assertRaisesRegex(PermissionError, "already been reserved"):
            authorize_registered_holdout(
                preregistration=prereg,
                split_manifest=split,
                frozen_strategy_sha256=strategy_sha,
                walk_forward_3_record_number=walk_forward_3["recordNumber"],
                validation_record_number=validation["recordNumber"],
                explicit_holdout_authorization=True,
            )

    def test_holdout_rejects_repeated_validation_and_post_validation_records(self):
        strategy_sha = "c" * 64

        def paths(label: str) -> tuple[Path, Path]:
            token = f"{uuid.uuid4().hex}-{label}"
            ledger = TEST_ARTIFACTS / f"{token}-ledger.jsonl"
            registry = TEST_ARTIFACTS / f"{token}-authorization.json"
            self.addCleanup(ledger.unlink, missing_ok=True)
            self.addCleanup(ledger.with_name(ledger.name + ".lock").unlink, missing_ok=True)
            self.addCleanup(registry.unlink, missing_ok=True)
            return ledger, registry

        duplicate_ledger, duplicate_registry = paths("duplicate")
        split, duplicate_prereg = preregistration(
            ledger_path=duplicate_ledger,
            registry_path=duplicate_registry,
        )
        wf3 = append_fresh_record(
            duplicate_ledger,
            passed_evidence(duplicate_prereg, strategy_sha, "walk_forward_3"),
        )
        first_validation = append_fresh_record(
            duplicate_ledger,
            passed_evidence(duplicate_prereg, strategy_sha, "validation"),
        )
        append_fresh_record(
            duplicate_ledger,
            passed_evidence(duplicate_prereg, strategy_sha, "validation"),
        )
        with self.assertRaisesRegex(PermissionError, "exactly one validation"):
            authorize_registered_holdout(
                preregistration=duplicate_prereg,
                split_manifest=split,
                frozen_strategy_sha256=strategy_sha,
                walk_forward_3_record_number=wf3["recordNumber"],
                validation_record_number=first_validation["recordNumber"],
                explicit_holdout_authorization=True,
            )

        post_ledger, post_registry = paths("post")
        split, post_prereg = preregistration(
            ledger_path=post_ledger,
            registry_path=post_registry,
        )
        wf3 = append_fresh_record(
            post_ledger,
            passed_evidence(post_prereg, strategy_sha, "walk_forward_3"),
        )
        validation = append_fresh_record(
            post_ledger,
            passed_evidence(post_prereg, strategy_sha, "validation"),
        )
        administrative = passed_evidence(post_prereg, strategy_sha, "validation")
        administrative.update(
            {
                "candidateId": "post-validation-change",
                "outcomesRevealed": False,
                "gatePassed": False,
                "status": "audit",
            }
        )
        append_fresh_record(post_ledger, administrative)
        with self.assertRaisesRegex(PermissionError, "final experiment-ledger record"):
            authorize_registered_holdout(
                preregistration=post_prereg,
                split_manifest=split,
                frozen_strategy_sha256=strategy_sha,
                walk_forward_3_record_number=wf3["recordNumber"],
                validation_record_number=validation["recordNumber"],
                explicit_holdout_authorization=True,
            )


if __name__ == "__main__":
    unittest.main()
