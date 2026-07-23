from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path

from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_restart_v4 import (
    FRESH_V4_STUDY_ID,
    RUN17_ARCHIVE_SHA256,
    RUN17_ARTIFACT_NAME,
    RUN17_GITHUB_ARTIFACT_ID,
    RUN17_GITHUB_COMMIT_SHA,
    RUN17_GITHUB_JOB_ID,
    RUN17_GITHUB_RUN_ATTEMPT,
    RUN17_GITHUB_RUN_ID,
    RUN17_IMPLEMENTATION_MANIFEST_SHA256,
    RUN17_INHERITED_SCIENTIFIC_IDENTITIES,
    RUN17_LEDGER_SHA256,
    RUN17_MEMBER_FILE_SHA256,
    RUN17_ORDERED_LEDGER_RECORD_SHA256,
    RUN17_PREREGISTRATION_SHA256,
    RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256,
    RUN17_SCIENTIFIC_SPECIFICATION_SHA256,
    RUN17_STUDY_LINEAGE_SHA256,
    RUN17_V4_PROVENANCE_SHA256,
    RUN17_ZIP_SHA256,
    _expected_v3_state_binding,
    _read_json,
    _validate_run17_ledger,
    _validate_run17_progress_log,
    _validate_run17_scientific_evidence,
    canonical_fresh_v4_restart_provenance,
    canonical_fresh_v4_study_lineage,
    load_fresh_v4_restart_bundle,
)


def _ledger_records() -> tuple[dict, ...]:
    records = (
        {
            "recordNumber": 1,
            "recordSha256": RUN17_ORDERED_LEDGER_RECORD_SHA256[0],
            "recordKind": "stage-window-access",
            "status": "window_access_started",
            "candidateId": "protocol-stage-access::discovery",
            "role": "discovery",
            "stage": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "preregistrationSha256": RUN17_PREREGISTRATION_SHA256,
            "metrics": {"purpose": "frozen discovery"},
            "leakageChecks": {"durableBeforeCallback": True},
        },
        {
            "recordNumber": 2,
            "recordSha256": RUN17_ORDERED_LEDGER_RECORD_SHA256[1],
            "recordKind": "batch-window-access",
            "status": "batch_access_started",
            "candidateId": "protocol-batch-access::entry::discovery",
            "role": "discovery",
            "stage": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "preregistrationSha256": RUN17_PREREGISTRATION_SHA256,
            "metrics": {"candidateCount": 240, "errorType": None},
            "leakageChecks": {
                "callbackCompleted": False,
                "callbackErrored": False,
                "durableBeforeCallback": True,
            },
        },
        {
            "recordNumber": 3,
            "recordSha256": RUN17_ORDERED_LEDGER_RECORD_SHA256[2],
            "recordKind": "batch-window-access",
            "status": "batch_access_error",
            "candidateId": "protocol-batch-access::entry::discovery",
            "role": "discovery",
            "stage": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "preregistrationSha256": RUN17_PREREGISTRATION_SHA256,
            "metrics": {"candidateCount": 240, "errorType": "OSError"},
            "leakageChecks": {
                "callbackCompleted": False,
                "callbackErrored": True,
                "durableBeforeCallback": False,
            },
        },
    )
    return tuple(copy.deepcopy(record) for record in records)


def _progress_text() -> str:
    anchors = (
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
        "2026-01-12",
        "2026-01-14",
        "2026-01-15",
        "2026-01-16",
    )
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
        for ordinal, anchor in enumerate(anchors, start=1)
    ]
    rows.extend(
        (
            "Traceback (most recent call last):",
            '  File "fresh_pipeline_cli.py", line 62, in main',
            "    run_v3_discovery()",
            "    score_entries_batch()",
            "    _entry_session_spool()",
            "    _append_entry_session_to_spool()",
            '  File "fresh_spool.py", line 46, in _write_all',
            "OSError: [Errno 28] No space left on device",
        )
    )
    return "\n".join(rows) + "\n"


def _scientific_evidence() -> dict:
    identity = dict(RUN17_INHERITED_SCIENTIFIC_IDENTITIES)
    state = _expected_v3_state_binding()
    source = {
        "inventorySha256": identity["inventorySha256"],
        "corpusManifestSha256": identity["corpusManifestSha256"],
        "splitManifestSha256": identity["splitManifestSha256"],
        "implementationManifestSha256": (
            RUN17_IMPLEMENTATION_MANIFEST_SHA256
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
            "schema": "fresh-xauusd-acceleration-preregistration/v3",
            "studyId": "xauusd-fresh-causal-acceleration-v3",
            "preregistrationSha256": RUN17_PREREGISTRATION_SHA256,
            "sourceBindings": source,
            "infrastructureRestart": {
                "scientificSpecificationSha256": (
                    RUN17_SCIENTIFIC_SPECIFICATION_SHA256
                ),
                "inheritedScientificIdentities": identity,
                "reusedOutcomeBlindInputs": dict(
                    RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256
                ),
                "candidateOutcomeRecordCount": 0,
                "laterWindowOutcomeRecordCount": 0,
                "batchResultSealed": False,
                "holdoutAuthorizationPresent": False,
            },
        },
        "predecessor_implementation_manifest": {
            "manifestSha256": RUN17_IMPLEMENTATION_MANIFEST_SHA256,
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


class FreshRestartV4EvidenceTests(unittest.TestCase):
    def test_run17_external_metadata_and_envelopes_are_exact(self) -> None:
        self.assertEqual(RUN17_GITHUB_RUN_ID, 30000411128)
        self.assertEqual(RUN17_GITHUB_RUN_ATTEMPT, 1)
        self.assertEqual(RUN17_GITHUB_JOB_ID, 89184009635)
        self.assertEqual(RUN17_GITHUB_ARTIFACT_ID, 8562091360)
        self.assertEqual(
            RUN17_GITHUB_COMMIT_SHA,
            "50d3b60da902e86e416669b82922ab4d7436ef32",
        )
        self.assertEqual(RUN17_ARTIFACT_NAME, "fresh-xauusd-30000411128-1")
        self.assertEqual(
            RUN17_ZIP_SHA256,
            "5a54b2bd7670d06234e4f1efab9566dcbf8b4b2a9392fd8238860f5eb0852490",
        )
        self.assertEqual(
            RUN17_ARCHIVE_SHA256,
            "13f3c091ecb54d58f1d467d9ce0022617658f80a1a7fa38f4c78c33a9c865ada",
        )
        self.assertEqual(
            RUN17_LEDGER_SHA256,
            "222bd02635243ce554ef666db3faf2e5008fd60aac08d92023db69b2fd52ac9f",
        )

    def test_exact_six_reusable_files_within_exact_sixteen_members(
        self,
    ) -> None:
        self.assertEqual(len(RUN17_MEMBER_FILE_SHA256), 16)
        self.assertEqual(len(RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256), 6)
        self.assertEqual(
            set(RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256),
            {
                "fresh_source_inventory_v1.json",
                "fresh_corpus_manifest_v1.json",
                "fresh_split_manifest_v2.json",
                "fresh_quantile_bank_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_entry_bank_v1.json",
            },
        )
        for name, digest in RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256.items():
            self.assertEqual(digest, RUN17_MEMBER_FILE_SHA256[name])
        self.assertNotIn(
            "fresh_holdout_authorization_v1.json",
            RUN17_MEMBER_FILE_SHA256,
        )

    def test_v4_lineage_is_the_canonical_new_study_identity(self) -> None:
        lineage = canonical_fresh_v4_study_lineage()
        self.assertEqual(FRESH_V4_STUDY_ID, lineage["studyId"])
        self.assertEqual(
            lineage["predecessorStudyId"],
            "xauusd-fresh-causal-acceleration-v3",
        )
        self.assertEqual(canonical_hash(lineage), RUN17_STUDY_LINEAGE_SHA256)
        self.assertEqual(
            RUN17_STUDY_LINEAGE_SHA256,
            "aa894a42147c5b5436490470ea81b630e1d899bd3b079fa800715820c89eb928",
        )

    def test_restart_provenance_is_canonical_and_returns_fresh_copies(
        self,
    ) -> None:
        first = canonical_fresh_v4_restart_provenance()
        second = canonical_fresh_v4_restart_provenance()
        body = dict(first)
        claimed = body.pop("provenanceSha256")
        self.assertEqual(claimed, canonical_hash(body))
        self.assertEqual(claimed, RUN17_V4_PROVENANCE_SHA256)
        self.assertEqual(
            RUN17_V4_PROVENANCE_SHA256,
            "6c8d7eade6553209351e7117249c78d256c6dcd93a67aeac5b5ad19c01557237",
        )
        self.assertEqual(first, second)
        self.assertIsNot(first, second)
        self.assertTrue(first["restartPolicy"]["discardTransientSpools"])
        self.assertFalse(first["restartPolicy"]["importCandidateResults"])
        self.assertEqual(
            first["restartPolicy"]["recomputeFromDiscoverySessionOrdinal"],
            1,
        )
        self.assertEqual(first["candidateOutcomeRecordCount"], 0)
        self.assertEqual(first["laterWindowOutcomeRecordCount"], 0)
        self.assertFalse(first["batchResultSealed"])
        self.assertFalse(first["holdoutAuthorizationPresent"])
        first["restartPolicy"]["recomputeFromDiscoverySessionOrdinal"] = 11
        self.assertEqual(
            canonical_fresh_v4_restart_provenance()["restartPolicy"][
                "recomputeFromDiscoverySessionOrdinal"
            ],
            1,
        )

    def test_terminal_ledger_accepts_only_the_exact_three_records(
        self,
    ) -> None:
        _validate_run17_ledger(_ledger_records())
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
                    _validate_run17_ledger(changed)

    def test_terminal_ledger_rejects_sealed_or_non_enospc_batch(
        self,
    ) -> None:
        for index, section, key, value in (
            (1, "metrics", "candidateCount", 239),
            (2, "metrics", "errorType", None),
            (2, "leakageChecks", "callbackErrored", False),
        ):
            with self.subTest(index=index, key=key):
                changed = copy.deepcopy(_ledger_records())
                changed[index][section][key] = value
                with self.assertRaises(PermissionError):
                    _validate_run17_ledger(changed)

    def test_progress_requires_ten_rows_followed_by_enospc(self) -> None:
        path = Path.cwd() / ".fresh-v4-progress-test.log"
        try:
            path.write_text(_progress_text(), encoding="utf-8")
            _validate_run17_progress_log(path)

            lines = _progress_text().splitlines()
            lines[9] = lines[9].replace('"sessionOrdinal": 10', '"sessionOrdinal": 11')
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaises(PermissionError):
                _validate_run17_progress_log(path)

            lines = _progress_text().splitlines()
            lines[-1] = "OSError: unrelated"
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaises(PermissionError):
                _validate_run17_progress_log(path)
        finally:
            path.unlink(missing_ok=True)

    def test_progress_and_json_readers_reject_duplicate_keys(self) -> None:
        progress = Path.cwd() / ".fresh-v4-duplicate-progress-test.log"
        document = Path.cwd() / ".fresh-v4-duplicate-json-test.json"
        try:
            lines = _progress_text().splitlines()
            lines[0] = (
                '{"sessionAnchor":"2026-01-02","sessionCount":40,'
                '"sessionOrdinal":1,"stage":"discovery","stage":"holdout"}'
            )
            progress.write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaises(PermissionError):
                _validate_run17_progress_log(progress)

            document.write_text(
                '{"sealed":false,"sealed":true}',
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                _read_json(document)
        finally:
            progress.unlink(missing_ok=True)
            document.unlink(missing_ok=True)

    def test_scientific_evidence_accepts_only_v3_frozen_identity(
        self,
    ) -> None:
        _validate_run17_scientific_evidence(**_scientific_evidence())
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
                    _validate_run17_scientific_evidence(**changed)

    def test_loader_rejects_changed_envelope_identity_before_io(self) -> None:
        missing = Path.cwd() / ".missing-run17-payload"
        with self.assertRaisesRegex(PermissionError, "GitHub ZIP"):
            load_fresh_v4_restart_bundle(
                missing,
                github_zip_sha256="0" * 64,
            )
        with self.assertRaisesRegex(PermissionError, "nested TGZ"):
            load_fresh_v4_restart_bundle(
                missing,
                nested_tgz_sha256="0" * 64,
            )

    def test_local_immutable_run17_artifact_when_available(self) -> None:
        fixture = (
            Path(__file__).resolve().parent.parent
            / "run-artifacts"
            / "run-30000411128-audit"
            / "payload"
        )
        if not fixture.is_dir():
            self.skipTest("immutable run-17 artifact is not in this checkout")
        zip_path = fixture.parent / f"{RUN17_ARTIFACT_NAME}.zip"
        archive_path = (
            fixture.parent
            / "zip-extracted"
            / f"{RUN17_ARTIFACT_NAME}.tgz"
        )
        self.assertEqual(
            hashlib.sha256(zip_path.read_bytes()).hexdigest(),
            RUN17_ZIP_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            RUN17_ARCHIVE_SHA256,
        )
        bundle = load_fresh_v4_restart_bundle(
            fixture,
            github_zip_sha256=RUN17_ZIP_SHA256,
            nested_tgz_sha256=RUN17_ARCHIVE_SHA256,
        )
        self.assertEqual(len(bundle.paths), 16)
        self.assertEqual(len(bundle.ledger_records), 3)
        self.assertEqual(
            bundle.provenance,
            canonical_fresh_v4_restart_provenance(),
        )
        self.assertEqual(
            bundle.provenance["restartPolicy"][
                "recomputeFromDiscoverySessionOrdinal"
            ],
            1,
        )


if __name__ == "__main__":
    unittest.main()
