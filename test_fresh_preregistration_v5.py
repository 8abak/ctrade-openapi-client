from __future__ import annotations

import copy
import json
import unittest
from datetime import date, timedelta
from pathlib import Path

from datavis.research.fresh_preregistration import (
    FRESH_V2_WINDOW_POLICY,
    PREREGISTRATION_V5_SCHEMA,
    build_fresh_implementation_manifest,
    build_fresh_preregistration_v2,
    build_fresh_preregistration_v5,
    feature_configs_from_preregistration,
    fresh_v3_scientific_specification_sha256,
    fresh_v4_scientific_specification_sha256,
    fresh_v5_scientific_specification_sha256,
    required_fresh_implementation_files,
    required_fresh_v4_implementation_files,
    required_fresh_v5_implementation_files,
    validate_fresh_preregistration,
    validate_fresh_preregistration_v5,
)
from datavis.research.fresh_protocol import (
    build_fresh_split_manifest,
    canonical_hash,
)
from datavis.research.fresh_restart_v5 import (
    FRESH_V5_STUDY_ID,
    RUN19_SCIENTIFIC_SPECIFICATION_SHA256,
    canonical_fresh_v5_restart_provenance,
)


ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts" / "test-fresh-preregistration-v5"
LEDGER = ARTIFACTS / "unused-v5-ledger.jsonl"
HOLDOUT = ARTIFACTS / "unused-holdout-authorization.json"
SCIENTIFIC_KEYS = (
    "chronologicalWindowPolicy",
    "sessionAndData",
    "features",
    "execution",
    "entryDiagnostics",
    "candidateSearch",
    "exitResearch",
    "robustnessAndGates",
    "holdout",
)


def _weekday_anchors(count: int) -> list[str]:
    anchors: list[str] = []
    cursor = date(2025, 1, 2)
    while len(anchors) < count:
        if cursor.weekday() < 5:
            anchors.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return anchors


def _split_manifest() -> dict:
    return build_fresh_split_manifest(
        _weekday_anchors(FRESH_V2_WINDOW_POLICY.required_sessions),
        inventory_sha256="a" * 64,
        excluded_sessions=[
            {
                "sessionAnchor": "2025-01-01",
                "reason": (
                    "partial source coverage established before outcome inspection"
                ),
            }
        ],
        policy=FRESH_V2_WINDOW_POLICY,
    )


def _implementation_manifest(
    paths: tuple[str, ...] | None = None,
) -> dict:
    return build_fresh_implementation_manifest(
        repository_root=ROOT,
        relative_paths=(
            required_fresh_v5_implementation_files()
            if paths is None
            else paths
        ),
    )


def _build_v5(
    *,
    implementation_manifest: dict | None = None,
    provenance: dict | None = None,
) -> dict:
    return build_fresh_preregistration_v5(
        split_manifest=_split_manifest(),
        corpus_manifest_sha256="b" * 64,
        protocol_code_identifier="tree:causal-protocol-v5",
        implementation_manifest=(
            _implementation_manifest()
            if implementation_manifest is None
            else implementation_manifest
        ),
        experiment_ledger_path=LEDGER,
        holdout_authorization_registry_path=HOLDOUT,
        infrastructure_restart_provenance=(
            canonical_fresh_v5_restart_provenance()
            if provenance is None
            else provenance
        ),
    )


def _rehash_preregistration(value: dict) -> dict:
    selected = copy.deepcopy(value)
    body = copy.deepcopy(selected)
    body.pop("preregistrationSha256", None)
    selected["preregistrationSha256"] = canonical_hash(body)
    return selected


def _rehash_provenance(value: dict) -> dict:
    selected = copy.deepcopy(value)
    body = copy.deepcopy(selected)
    body.pop("provenanceSha256", None)
    selected["provenanceSha256"] = canonical_hash(body)
    return selected


class FreshPreregistrationV5Tests(unittest.TestCase):
    def test_v5_deterministic_round_trip_and_v2_v3_v4_v5_science(self) -> None:
        manifest = _implementation_manifest()
        split = _split_manifest()
        provenance = canonical_fresh_v5_restart_provenance()
        v5 = build_fresh_preregistration_v5(
            split_manifest=split,
            corpus_manifest_sha256="b" * 64,
            protocol_code_identifier="tree:causal-protocol-v5",
            implementation_manifest=manifest,
            experiment_ledger_path=LEDGER,
            holdout_authorization_registry_path=HOLDOUT,
            infrastructure_restart_provenance=provenance,
        )
        duplicate = build_fresh_preregistration_v5(
            split_manifest=json.loads(json.dumps(split)),
            corpus_manifest_sha256="B" * 64,
            protocol_code_identifier="tree:causal-protocol-v5",
            implementation_manifest=copy.deepcopy(manifest),
            experiment_ledger_path=LEDGER,
            holdout_authorization_registry_path=HOLDOUT,
            infrastructure_restart_provenance=json.loads(
                json.dumps(provenance)
            ),
        )
        v2 = build_fresh_preregistration_v2(
            split_manifest=split,
            corpus_manifest_sha256="b" * 64,
            protocol_code_identifier="tree:causal-protocol-v5",
            implementation_manifest=manifest,
            experiment_ledger_path=LEDGER,
            holdout_authorization_registry_path=HOLDOUT,
        )
        v2_science = {
            key: v2[key]
            for key in SCIENTIFIC_KEYS
        }
        science_sha256 = canonical_hash(v2_science)

        self.assertEqual(v5, duplicate)
        self.assertEqual(v5["schema"], PREREGISTRATION_V5_SCHEMA)
        self.assertEqual(v5["studyId"], FRESH_V5_STUDY_ID)
        self.assertEqual(v5["infrastructureRestart"], provenance)
        self.assertEqual(
            {key: v5[key] for key in SCIENTIFIC_KEYS},
            v2_science,
        )
        self.assertEqual(
            {
                science_sha256,
                fresh_v3_scientific_specification_sha256(),
                fresh_v4_scientific_specification_sha256(),
                fresh_v5_scientific_specification_sha256(),
                RUN19_SCIENTIFIC_SPECIFICATION_SHA256,
            },
            {science_sha256},
        )
        self.assertEqual(
            validate_fresh_preregistration_v5(v5),
            v5["preregistrationSha256"],
        )

    def test_v5_rejects_self_consistent_provenance_tampering(self) -> None:
        mutations = (
            lambda value: value["restartPolicy"].__setitem__(
                "recomputeFromDiscoverySessionOrdinal", 2
            ),
            lambda value: value.__setitem__(
                "candidateOutcomeRecordCount", 1
            ),
            lambda value: value.__setitem__(
                "scientificDefinitionsChanged", True
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                tampered = _build_v5()
                mutate(tampered["infrastructureRestart"])
                tampered["infrastructureRestart"] = _rehash_provenance(
                    tampered["infrastructureRestart"]
                )
                tampered = _rehash_preregistration(tampered)

                with self.assertRaisesRegex(
                    ValueError,
                    "v5 infrastructure restart provenance changed",
                ):
                    validate_fresh_preregistration_v5(tampered)

    def test_v5_rejects_rehashed_scientific_rule_changes(self) -> None:
        tampered = _build_v5()
        tampered["candidateSearch"]["budgets"][
            "discoveryDistinctCandidates"
        ] = 239
        tampered = _rehash_preregistration(tampered)

        with self.assertRaisesRegex(ValueError, "canonical v2"):
            validate_fresh_preregistration_v5(tampered)

    def test_v5_requires_all_restart_generations_and_spool(self) -> None:
        required = required_fresh_v5_implementation_files()
        self.assertTrue(
            set(required_fresh_implementation_files()).issubset(required)
        )
        self.assertTrue(
            set(required_fresh_v4_implementation_files()).issubset(required)
        )
        required_lineage_files = (
            "datavis/research/fresh_restart.py",
            "datavis/research/fresh_restart_v4.py",
            "datavis/research/fresh_restart_v5.py",
            "datavis/research/fresh_spool.py",
        )
        for path in required_lineage_files:
            self.assertIn(path, required)

        for missing in required_lineage_files:
            with self.subTest(missing=missing):
                manifest = _implementation_manifest(
                    tuple(path for path in required if path != missing)
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "v5 implementation manifest omits restart or spool code",
                ):
                    _build_v5(implementation_manifest=manifest)

    def test_generic_validator_and_config_dispatch_accept_v5(self) -> None:
        v5 = _build_v5()
        serialized = json.loads(json.dumps(v5, sort_keys=True))

        self.assertEqual(
            validate_fresh_preregistration(serialized),
            v5["preregistrationSha256"],
        )
        configs = feature_configs_from_preregistration(serialized)
        self.assertEqual(len(configs), 9)
        self.assertEqual(
            configs[0].horizons_seconds,
            (0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
        )


if __name__ == "__main__":
    unittest.main()
