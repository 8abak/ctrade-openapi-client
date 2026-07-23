from __future__ import annotations

import copy
import json
import unittest
from datetime import date, timedelta
from pathlib import Path

from datavis.research.fresh_preregistration import (
    FRESH_V2_WINDOW_POLICY,
    PREREGISTRATION_V4_SCHEMA,
    build_fresh_implementation_manifest,
    build_fresh_preregistration_v2,
    build_fresh_preregistration_v4,
    feature_configs_from_preregistration,
    fresh_v3_scientific_specification_sha256,
    fresh_v4_scientific_specification_sha256,
    required_fresh_implementation_files,
    required_fresh_v4_implementation_files,
    validate_fresh_preregistration,
    validate_fresh_preregistration_v4,
)
from datavis.research.fresh_protocol import (
    build_fresh_split_manifest,
    canonical_hash,
)
from datavis.research.fresh_restart_v4 import (
    FRESH_V4_STUDY_ID,
    RUN17_SCIENTIFIC_SPECIFICATION_SHA256,
    canonical_fresh_v4_restart_provenance,
)


ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts" / "test-fresh-preregistration-v4"
LEDGER = ARTIFACTS / "unused-v4-ledger.jsonl"
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
            required_fresh_v4_implementation_files()
            if paths is None
            else paths
        ),
    )


def _build_v4(
    *,
    implementation_manifest: dict | None = None,
    provenance: dict | None = None,
) -> dict:
    return build_fresh_preregistration_v4(
        split_manifest=_split_manifest(),
        corpus_manifest_sha256="b" * 64,
        protocol_code_identifier="tree:causal-protocol-v4",
        implementation_manifest=(
            _implementation_manifest()
            if implementation_manifest is None
            else implementation_manifest
        ),
        experiment_ledger_path=LEDGER,
        holdout_authorization_registry_path=HOLDOUT,
        infrastructure_restart_provenance=(
            canonical_fresh_v4_restart_provenance()
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


class FreshPreregistrationV4Tests(unittest.TestCase):
    def test_v4_round_trip_binds_exact_run17_and_unchanged_science(self) -> None:
        manifest = _implementation_manifest()
        split = _split_manifest()
        v4 = build_fresh_preregistration_v4(
            split_manifest=split,
            corpus_manifest_sha256="b" * 64,
            protocol_code_identifier="tree:causal-protocol-v4",
            implementation_manifest=manifest,
            experiment_ledger_path=LEDGER,
            holdout_authorization_registry_path=HOLDOUT,
            infrastructure_restart_provenance=(
                canonical_fresh_v4_restart_provenance()
            ),
        )
        duplicate = build_fresh_preregistration_v4(
            split_manifest=json.loads(json.dumps(split)),
            corpus_manifest_sha256="B" * 64,
            protocol_code_identifier="tree:causal-protocol-v4",
            implementation_manifest=copy.deepcopy(manifest),
            experiment_ledger_path=LEDGER,
            holdout_authorization_registry_path=HOLDOUT,
            infrastructure_restart_provenance=json.loads(
                json.dumps(canonical_fresh_v4_restart_provenance())
            ),
        )
        v2 = build_fresh_preregistration_v2(
            split_manifest=split,
            corpus_manifest_sha256="b" * 64,
            protocol_code_identifier="tree:causal-protocol-v4",
            implementation_manifest=manifest,
            experiment_ledger_path=LEDGER,
            holdout_authorization_registry_path=HOLDOUT,
        )

        self.assertEqual(v4, duplicate)
        self.assertEqual(v4["schema"], PREREGISTRATION_V4_SCHEMA)
        self.assertEqual(v4["studyId"], FRESH_V4_STUDY_ID)
        self.assertEqual(
            v4["infrastructureRestart"],
            canonical_fresh_v4_restart_provenance(),
        )
        self.assertEqual(
            {key: v4[key] for key in SCIENTIFIC_KEYS},
            {key: v2[key] for key in SCIENTIFIC_KEYS},
        )
        self.assertEqual(
            fresh_v4_scientific_specification_sha256(),
            fresh_v3_scientific_specification_sha256(),
        )
        self.assertEqual(
            fresh_v4_scientific_specification_sha256(),
            RUN17_SCIENTIFIC_SPECIFICATION_SHA256,
        )
        self.assertEqual(
            validate_fresh_preregistration_v4(v4),
            v4["preregistrationSha256"],
        )
        self.assertEqual(
            validate_fresh_preregistration(
                json.loads(json.dumps(v4, sort_keys=True))
            ),
            v4["preregistrationSha256"],
        )
        self.assertEqual(len(feature_configs_from_preregistration(v4)), 9)
        declaration = v4["outcomeBlindDeclaration"]
        self.assertTrue(declaration["thresholdsInheritedByteForByte"])
        self.assertEqual(
            declaration["candidateOutcomeRecordCountAvailableToBuilder"],
            0,
        )
        self.assertFalse(declaration["partialCandidateResultsRecovered"])
        self.assertFalse(
            declaration["thresholdOrCandidateRetuningPermitted"]
        )

    def test_exact_run17_provenance_rejects_self_consistent_tampering(self) -> None:
        v4 = _build_v4()
        mutations = (
            lambda value: value["infrastructureRestart"]["restartPolicy"].__setitem__(
                "recomputeFromDiscoverySessionOrdinal", 11
            ),
            lambda value: value["infrastructureRestart"].__setitem__(
                "candidateOutcomeRecordCount", 1
            ),
            lambda value: value["infrastructureRestart"].__setitem__(
                "scientificDefinitionsChanged", True
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                tampered = copy.deepcopy(v4)
                mutate(tampered)
                tampered["infrastructureRestart"] = _rehash_provenance(
                    tampered["infrastructureRestart"]
                )
                tampered = _rehash_preregistration(tampered)
                with self.assertRaisesRegex(
                    ValueError,
                    "v4 infrastructure restart provenance changed",
                ):
                    validate_fresh_preregistration_v4(tampered)

        missing_digest = copy.deepcopy(v4)
        missing_digest["infrastructureRestart"].pop("provenanceSha256")
        missing_digest = _rehash_preregistration(missing_digest)
        with self.assertRaisesRegex(
            ValueError,
            "v4 infrastructure restart provenance changed",
        ):
            validate_fresh_preregistration_v4(missing_digest)

    def test_v4_rejects_rehashed_scientific_rule_changes(self) -> None:
        tampered = _build_v4()
        tampered["candidateSearch"]["budgets"][
            "discoveryDistinctCandidates"
        ] = 239
        tampered = _rehash_preregistration(tampered)

        with self.assertRaisesRegex(ValueError, "canonical v2"):
            validate_fresh_preregistration_v4(tampered)

    def test_v4_requires_restart_v4_and_spool_in_manifest(self) -> None:
        required = required_fresh_v4_implementation_files()
        self.assertTrue(
            set(required_fresh_implementation_files()).issubset(required)
        )
        self.assertIn(
            "datavis/research/fresh_restart_v4.py",
            required,
        )
        self.assertIn("datavis/research/fresh_spool.py", required)

        for missing in (
            "datavis/research/fresh_restart_v4.py",
            "datavis/research/fresh_spool.py",
        ):
            with self.subTest(missing=missing):
                manifest = _implementation_manifest(
                    tuple(path for path in required if path != missing)
                )
                with self.assertRaisesRegex(
                    ValueError,
                    "v4 implementation manifest omits restart or spool code",
                ):
                    _build_v4(implementation_manifest=manifest)

    def test_v4_validator_rechecks_required_implementation_files(self) -> None:
        v4 = _build_v4()
        source = v4["sourceBindings"]
        manifest = source["implementationManifest"]
        manifest["files"] = [
            item
            for item in manifest["files"]
            if item["path"] != "datavis/research/fresh_restart_v4.py"
        ]
        manifest_body = copy.deepcopy(manifest)
        manifest_body.pop("manifestSha256")
        manifest["manifestSha256"] = canonical_hash(manifest_body)
        source["implementationManifestSha256"] = manifest["manifestSha256"]
        v4 = _rehash_preregistration(v4)

        with self.assertRaisesRegex(
            ValueError,
            "v4 implementation manifest omits restart or spool code",
        ):
            validate_fresh_preregistration_v4(
                v4,
                verify_current_implementation_files=False,
            )


if __name__ == "__main__":
    unittest.main()
