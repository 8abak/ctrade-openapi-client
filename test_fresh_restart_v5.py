from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path
from unittest import mock

from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_restart_v5 import (
    FRESH_V5_STUDY_ID,
    RUN19_ADOPTION_ARTIFACT_NAME,
    RUN19_ADOPTION_GITHUB_ARTIFACT_ID,
    RUN19_ADOPTION_GITHUB_RUN_ID,
    RUN19_ARCHIVE_SHA256,
    RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH,
    RUN19_GITHUB_COMMIT_SHA,
    RUN19_GITHUB_JOB_ID,
    RUN19_GITHUB_RUN_ATTEMPT,
    RUN19_GITHUB_RUN_ID,
    RUN19_IMPLEMENTATION_MANIFEST_SHA256,
    RUN19_INHERITED_SCIENTIFIC_IDENTITIES,
    RUN19_LEDGER_SHA256,
    RUN19_MEMBER_FILE_SHA256,
    RUN19_ORDERED_LEDGER_RECORD_SHA256,
    RUN19_PREREGISTRATION_SHA256,
    RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256,
    RUN19_SCIENTIFIC_SPECIFICATION_SHA256,
    RUN19_STUDY_LINEAGE_SHA256,
    RUN19_TERMINAL_ARCHIVE_NAME,
    RUN19_V5_PROVENANCE_SHA256,
    RUN19_V5_STUDY_LINEAGE_SHA256,
    RUN19_ZIP_SHA256,
    _EXPECTED_DISCOVERY_ANCHORS,
    _V4_RESTART_PROVENANCE_SHA256,
    _canonical_v4_lineage,
    _expected_v4_state_binding,
    _file_sha256,
    _read_json,
    _validate_run19_ledger,
    _validate_run19_progress_log,
    _validate_run19_scientific_evidence,
    canonical_fresh_v5_restart_provenance,
    canonical_fresh_v5_study_lineage,
    default_run19_terminal_artifact_path,
    load_fresh_v5_restart_bundle,
)


def _ledger_records() -> tuple[dict, ...]:
    records = (
        {
            "recordNumber": 1,
            "recordSha256": RUN19_ORDERED_LEDGER_RECORD_SHA256[0],
            "recordKind": "stage-window-access",
            "status": "window_access_started",
            "candidateId": "protocol-stage-access::discovery",
            "role": "discovery",
            "stage": "discovery",
            "trainingWindow": "discovery",
            "evaluationWindow": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "preregistrationSha256": RUN19_PREREGISTRATION_SHA256,
            "metrics": {"purpose": "frozen discovery"},
            "leakageChecks": {"durableBeforeCallback": True},
        },
        {
            "recordNumber": 2,
            "recordSha256": RUN19_ORDERED_LEDGER_RECORD_SHA256[1],
            "recordKind": "batch-window-access",
            "status": "batch_access_started",
            "candidateId": "protocol-batch-access::entry::discovery",
            "role": "discovery",
            "stage": "discovery",
            "trainingWindow": "discovery",
            "evaluationWindow": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "preregistrationSha256": RUN19_PREREGISTRATION_SHA256,
            "metrics": {"candidateCount": 240, "errorType": None},
            "leakageChecks": {
                "callbackCompleted": False,
                "callbackErrored": False,
                "durableBeforeCallback": True,
            },
        },
        {
            "recordNumber": 3,
            "recordSha256": RUN19_ORDERED_LEDGER_RECORD_SHA256[2],
            "recordKind": "batch-window-access",
            "status": "batch_access_error",
            "candidateId": "protocol-batch-access::entry::discovery",
            "role": "discovery",
            "stage": "discovery",
            "trainingWindow": "discovery",
            "evaluationWindow": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "preregistrationSha256": RUN19_PREREGISTRATION_SHA256,
            "metrics": {
                "candidateCount": 240,
                "errorType": "BrokenPipeError",
            },
            "leakageChecks": {
                "callbackCompleted": False,
                "callbackErrored": True,
                "durableBeforeCallback": False,
            },
        },
    )
    return tuple(copy.deepcopy(record) for record in records)


def _progress_text() -> str:
    rows = [
        json.dumps(
            {
                "sessionAnchor": anchor,
                "sessionCount": 40,
                "sessionOrdinal": ordinal,
                "stage": "discovery",
            },
            sort_keys=True,
        )
        for ordinal, anchor in enumerate(
            _EXPECTED_DISCOVERY_ANCHORS,
            start=1,
        )
    ]
    return "\n".join(rows) + "\n"


def _scientific_evidence() -> dict:
    identity = dict(RUN19_INHERITED_SCIENTIFIC_IDENTITIES)
    state = _expected_v4_state_binding()
    source = {
        "inventorySha256": identity["inventorySha256"],
        "corpusManifestSha256": identity["corpusManifestSha256"],
        "splitManifestSha256": identity["splitManifestSha256"],
        "implementationManifestSha256": (
            RUN19_IMPLEMENTATION_MANIFEST_SHA256
        ),
        "experimentLedgerPath": state["experimentLedgerPath"],
        "holdoutAuthorizationRegistryPath": (
            state["holdoutAuthorizationRegistryPath"]
        ),
    }
    return {
        "inventory": {"inventorySha256": identity["inventorySha256"]},
        "corpus": {
            "inventorySha256": identity["inventorySha256"],
            "corpusManifestSha256": identity["corpusManifestSha256"],
        },
        "split": {
            "inventorySha256": identity["inventorySha256"],
            "manifestSha256": identity["splitManifestSha256"],
        },
        "predecessor_preregistration": {
            "schema": "fresh-xauusd-acceleration-preregistration/v4",
            "studyId": "xauusd-fresh-causal-acceleration-v4",
            "preregistrationSha256": RUN19_PREREGISTRATION_SHA256,
            "sourceBindings": source,
            "infrastructureRestart": {
                "provenanceSha256": _V4_RESTART_PROVENANCE_SHA256,
                "studyLineage": _canonical_v4_lineage(),
                "studyLineageSha256": RUN19_STUDY_LINEAGE_SHA256,
                "scientificSpecificationSha256": (
                    RUN19_SCIENTIFIC_SPECIFICATION_SHA256
                ),
                "inheritedScientificIdentities": identity,
                "reusedOutcomeBlindInputs": dict(
                    RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256
                ),
                "candidateOutcomeRecordCount": 0,
                "laterWindowOutcomeRecordCount": 0,
                "batchResultSealed": False,
                "holdoutAuthorizationPresent": False,
            },
        },
        "predecessor_implementation_manifest": {
            "manifestSha256": RUN19_IMPLEMENTATION_MANIFEST_SHA256,
        },
        "predecessor_state_binding": state,
        "quantile_bank": {
            "bankSha256": identity["quantileBankSha256"],
        },
        "threshold_preflight": {
            "schema": "fresh-xauusd-threshold-domain-preflight/v1",
            "allRegisteredThresholdDomainsResolved": True,
            "baseCandidateCount": 93,
            "candidateGridSha256": identity["candidateGridSha256"],
            "eventFilterVariantBankSha256": identity[
                "eventFilterVariantBankSha256"
            ],
            "eventFilterVariantCount": 147,
            "executionScenariosSha256": identity[
                "executionScenariosSha256"
            ],
            "exitGridSha256": identity["exitGridSha256"],
            "exitVariantCount": 72,
            "quantileBankSha256": identity["quantileBankSha256"],
            "totalRuntimeEntryCount": identity["candidateCount"],
        },
        "entry_bank": {
            "candidateCount": identity["candidateCount"],
            "candidateGridSha256": identity["candidateGridSha256"],
            "filterVariantBankSha256": identity[
                "eventFilterVariantBankSha256"
            ],
            "quantileBankSha256": identity["quantileBankSha256"],
        },
    }


class FreshRestartV5EvidenceTests(unittest.TestCase):
    def test_run19_original_and_adopter_metadata_are_exact(self) -> None:
        self.assertEqual(RUN19_GITHUB_RUN_ID, 30042880650)
        self.assertEqual(RUN19_GITHUB_RUN_ATTEMPT, 1)
        self.assertEqual(RUN19_GITHUB_JOB_ID, 89326866829)
        self.assertEqual(
            RUN19_GITHUB_COMMIT_SHA,
            "48ef503cbb01d53629bd1156b5d95e1396b412fb",
        )
        self.assertEqual(RUN19_ADOPTION_GITHUB_RUN_ID, 30065029441)
        self.assertEqual(RUN19_ADOPTION_GITHUB_ARTIFACT_ID, 8585919266)
        self.assertEqual(
            RUN19_ADOPTION_ARTIFACT_NAME,
            "fresh-xauusd-run19-adopted-30065029441-1",
        )
        self.assertEqual(
            RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH,
            "artifacts/run19-adopted-30065029441-1/terminal",
        )
        self.assertEqual(
            RUN19_TERMINAL_ARCHIVE_NAME,
            "fresh-xauusd-30042880650-1.tgz",
        )
        self.assertEqual(
            RUN19_ZIP_SHA256,
            "7bd36760bda3fd4250be3221d144be4f3a4f0f7b94b7d445f5c1278796b33a1c",
        )
        self.assertEqual(
            RUN19_ARCHIVE_SHA256,
            "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9",
        )
        self.assertEqual(
            RUN19_LEDGER_SHA256,
            "ac627bd986c044b12049f717eb3fc664321c08c169fd6a829a5fc8d51144c7b4",
        )

    def test_exact_six_reusable_files_within_exact_sixteen_members(
        self,
    ) -> None:
        self.assertEqual(len(RUN19_MEMBER_FILE_SHA256), 16)
        self.assertEqual(len(RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256), 6)
        self.assertEqual(
            set(RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256),
            {
                "fresh_source_inventory_v1.json",
                "fresh_corpus_manifest_v1.json",
                "fresh_split_manifest_v2.json",
                "fresh_quantile_bank_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_entry_bank_v1.json",
            },
        )
        for name, digest in (
            RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256.items()
        ):
            self.assertEqual(digest, RUN19_MEMBER_FILE_SHA256[name])
        self.assertNotIn(
            "fresh_holdout_authorization_v1.json",
            RUN19_MEMBER_FILE_SHA256,
        )
        self.assertNotIn(
            "fresh_exit_bank_v1.json",
            RUN19_MEMBER_FILE_SHA256,
        )

    def test_v5_lineage_is_canonical_and_v4_predecessor_is_pinned(
        self,
    ) -> None:
        lineage = canonical_fresh_v5_study_lineage()
        self.assertEqual(FRESH_V5_STUDY_ID, lineage["studyId"])
        self.assertEqual(
            lineage["predecessorStudyId"],
            "xauusd-fresh-causal-acceleration-v4",
        )
        self.assertEqual(
            lineage["predecessorTerminalLedgerSha256"],
            RUN19_LEDGER_SHA256,
        )
        self.assertEqual(
            RUN19_STUDY_LINEAGE_SHA256,
            "aa894a42147c5b5436490470ea81b630e1d899bd3b079fa800715820c89eb928",
        )
        self.assertEqual(
            canonical_hash(lineage),
            RUN19_V5_STUDY_LINEAGE_SHA256,
        )
        self.assertEqual(
            RUN19_V5_STUDY_LINEAGE_SHA256,
            "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473",
        )

    def test_restart_provenance_is_canonical_and_returns_fresh_copies(
        self,
    ) -> None:
        first = canonical_fresh_v5_restart_provenance()
        second = canonical_fresh_v5_restart_provenance()
        body = dict(first)
        claimed = body.pop("provenanceSha256")
        self.assertEqual(claimed, canonical_hash(body))
        self.assertEqual(claimed, RUN19_V5_PROVENANCE_SHA256)
        self.assertEqual(
            RUN19_V5_PROVENANCE_SHA256,
            "a04a46ac13c7ae4046b65e78966ee7f23734265f102e5db719d74938620d9274",
        )
        self.assertEqual(first, second)
        self.assertIsNot(first, second)
        self.assertEqual(first["predecessorExitStatus"], 120)
        self.assertEqual(
            first["predecessorFailure"]["errorType"],
            "BrokenPipeError",
        )
        self.assertEqual(first["completedDiscoverySessionCount"], 37)
        self.assertEqual(first["candidateOutcomeRecordCount"], 0)
        self.assertEqual(first["laterWindowOutcomeRecordCount"], 0)
        self.assertFalse(first["batchResultSealed"])
        self.assertFalse(first["holdoutAuthorizationPresent"])
        self.assertTrue(
            first["restartPolicy"]["discardPartialCandidateComputations"]
        )
        self.assertFalse(first["restartPolicy"]["importCandidateResults"])
        self.assertEqual(
            first["restartPolicy"]["recomputeFromDiscoverySessionOrdinal"],
            1,
        )
        first["restartPolicy"]["recomputeFromDiscoverySessionOrdinal"] = 38
        self.assertEqual(
            canonical_fresh_v5_restart_provenance()["restartPolicy"][
                "recomputeFromDiscoverySessionOrdinal"
            ],
            1,
        )

    def test_terminal_ledger_accepts_only_exact_three_records(self) -> None:
        _validate_run19_ledger(_ledger_records())
        mutations = (
            (0, "recordNumber", 2),
            (0, "recordSha256", "0" * 64),
            (0, "role", "walk_forward_1"),
            (1, "status", "batch_access_completed"),
            (1, "outcomesRevealed", False),
            (2, "recordKind", "candidate-outcome"),
            (2, "stage", "holdout"),
            (2, "preregistrationSha256", "f" * 64),
        )
        for index, field, value in mutations:
            with self.subTest(index=index, field=field):
                changed = copy.deepcopy(_ledger_records())
                changed[index][field] = value
                with self.assertRaises(PermissionError):
                    _validate_run19_ledger(changed)

    def test_terminal_ledger_rejects_sealed_or_other_failure(self) -> None:
        for index, section, key, value in (
            (1, "metrics", "candidateCount", 239),
            (2, "metrics", "errorType", "OSError"),
            (2, "leakageChecks", "callbackErrored", False),
        ):
            with self.subTest(index=index, key=key):
                changed = copy.deepcopy(_ledger_records())
                changed[index][section][key] = value
                with self.assertRaises(PermissionError):
                    _validate_run19_ledger(changed)

    def test_progress_requires_exactly_thirty_seven_rows(self) -> None:
        path = Path.cwd() / ".fresh-v5-progress-test.log"
        try:
            path.write_text(_progress_text(), encoding="utf-8")
            _validate_run19_progress_log(path)

            lines = _progress_text().splitlines()
            lines[36] = lines[36].replace(
                '"sessionOrdinal": 37',
                '"sessionOrdinal": 38',
            )
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaises(PermissionError):
                _validate_run19_progress_log(path)

            path.write_text(
                _progress_text() + '{"unexpected":true}\n',
                encoding="utf-8",
            )
            with self.assertRaises(PermissionError):
                _validate_run19_progress_log(path)
        finally:
            path.unlink(missing_ok=True)

    def test_progress_and_json_readers_reject_duplicate_keys(self) -> None:
        progress = Path.cwd() / ".fresh-v5-duplicate-progress-test.log"
        document = Path.cwd() / ".fresh-v5-duplicate-json-test.json"
        try:
            lines = _progress_text().splitlines()
            lines[0] = (
                '{"sessionAnchor":"2026-01-02","sessionCount":40,'
                '"sessionOrdinal":1,"stage":"discovery","stage":"holdout"}'
            )
            progress.write_text(
                "\n".join(lines) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(PermissionError):
                _validate_run19_progress_log(progress)

            document.write_text(
                '{"sealed":false,"sealed":true}',
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                _read_json(document)
        finally:
            progress.unlink(missing_ok=True)
            document.unlink(missing_ok=True)

    def test_scientific_evidence_accepts_only_frozen_v4_identity(
        self,
    ) -> None:
        _validate_run19_scientific_evidence(**_scientific_evidence())
        mutations = (
            (
                "predecessor_preregistration",
                "preregistrationSha256",
                "0" * 64,
            ),
            (
                "predecessor_implementation_manifest",
                "manifestSha256",
                "0" * 64,
            ),
            (
                "predecessor_state_binding",
                "studyLineageSha256",
                "0" * 64,
            ),
            ("inventory", "inventorySha256", "0" * 64),
            ("quantile_bank", "bankSha256", "0" * 64),
            ("entry_bank", "candidateCount", 241),
        )
        for section, key, value in mutations:
            with self.subTest(section=section, key=key):
                changed = _scientific_evidence()
                changed[section][key] = value
                with self.assertRaises(PermissionError):
                    _validate_run19_scientific_evidence(**changed)

    def test_loader_rejects_changed_envelope_identity_before_io(self) -> None:
        missing = Path.cwd() / ".missing-run19-payload"
        with self.assertRaisesRegex(PermissionError, "GitHub ZIP"):
            load_fresh_v5_restart_bundle(
                missing,
                github_zip_sha256="0" * 64,
            )
        with self.assertRaisesRegex(PermissionError, "nested TGZ"):
            load_fresh_v5_restart_bundle(
                missing,
                nested_tgz_sha256="0" * 64,
            )

    def test_extracted_run19_fixture_and_tampering(self) -> None:
        fixture = default_run19_terminal_artifact_path()
        if not fixture.is_dir():
            self.skipTest("adopted immutable Run-19 fixture is unavailable")
        self.assertEqual(
            fixture.as_posix().split("/platform/")[-1],
            RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH,
        )
        archive = fixture.parent / RUN19_TERMINAL_ARCHIVE_NAME
        self.assertEqual(
            hashlib.sha256(archive.read_bytes()).hexdigest(),
            RUN19_ARCHIVE_SHA256,
        )

        bundle = load_fresh_v5_restart_bundle()
        self.assertEqual(bundle.root, fixture.resolve())
        self.assertEqual(len(bundle.paths), 16)
        self.assertEqual(len(bundle.ledger_records), 3)
        self.assertEqual(
            bundle.provenance,
            canonical_fresh_v5_restart_provenance(),
        )

        def changed_digest(path: Path) -> str:
            if path.name == "fresh_entry_bank_v1.json":
                return "0" * 64
            return _file_sha256(path)

        with mock.patch(
            "datavis.research.fresh_restart_v5._file_sha256",
            side_effect=changed_digest,
        ):
            with self.assertRaisesRegex(PermissionError, "file changed"):
                load_fresh_v5_restart_bundle(fixture)

        with mock.patch(
            "datavis.research.fresh_restart_v5._FORBIDDEN_TERMINAL_MEMBERS",
            frozenset({"fresh_entry_bank_v1.json"}),
        ):
            with self.assertRaisesRegex(
                PermissionError,
                "forbidden result state",
            ):
                load_fresh_v5_restart_bundle(fixture)


if __name__ == "__main__":
    unittest.main()
