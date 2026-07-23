from __future__ import annotations

import copy
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tarfile
import textwrap
import unittest
import uuid
from pathlib import Path

from datavis.research.fresh_restart import (
    RUN16_GITHUB_ARTIFACT_ID,
    RUN16_GITHUB_COMMIT_SHA,
    RUN16_GITHUB_JOB_ID,
    RUN16_INHERITED_SCIENTIFIC_IDENTITIES,
    RUN16_ORDERED_LEDGER_RECORD_SHA256,
    RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256,
    RUN16_PREDECESSOR_PREREGISTRATION_SHA256,
    _read_json,
    _validate_run16_ledger,
    _validate_run16_progress_log,
    _validate_run16_scientific_evidence,
    load_fresh_v3_restart_bundle,
)
from datavis.research.fresh_restart_v4 import RUN17_ARCHIVE_SHA256


def _ledger_records() -> tuple[dict, ...]:
    kinds = (
        "stage-window-access",
        "batch-window-access",
        "infrastructure-resume",
        "infrastructure-resume",
        "infrastructure-resume",
        "infrastructure-resume",
        "infrastructure-resume",
    )
    statuses = (
        "window_access_started",
        "batch_access_started",
        "resume_eligibility_audit",
        "resume_authorized",
        "resume_identity_verified",
        "batch_resume_started",
        "batch_resume_error",
    )
    records = tuple(
        {
            "recordNumber": index,
            "recordSha256": digest,
            "recordKind": kind,
            "status": status,
            "role": "discovery",
        }
        for index, (digest, kind, status) in enumerate(
            zip(RUN16_ORDERED_LEDGER_RECORD_SHA256, kinds, statuses),
            start=1,
        )
    )
    records[-1]["metrics"] = {
        "batchResultSealed": False,
        "candidateOutcomesAppended": 0,
        "errorType": "ExternalProcessTermination",
        "externalExitStatus": 137,
    }
    records[-1]["leakageChecks"] = {"recoveryAttemptConsumed": True}
    return records


def _scientific_evidence() -> dict:
    identity = dict(RUN16_INHERITED_SCIENTIFIC_IDENTITIES)
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
            "studyId": "xauusd-fresh-causal-acceleration-v2",
            "preregistrationSha256": (
                RUN16_PREDECESSOR_PREREGISTRATION_SHA256
            ),
            "sourceBindings": {
                "inventorySha256": identity["inventorySha256"],
                "corpusManifestSha256": identity["corpusManifestSha256"],
                "splitManifestSha256": identity["splitManifestSha256"],
                "implementationManifestSha256": (
                    RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256
                ),
            },
        },
        "predecessor_implementation_manifest": {
            "manifestSha256": (
                RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256
            )
        },
        "predecessor_recovery_contract": {
            "schema": "fresh-xauusd-run14-recovery-contract/v1",
            "recoveryContractSha256": (
                "1a477c36992505b93b86fd205e61d5ed95d7a025abbd924b04c70675d7bab2d7"
            ),
            "audit": {
                "identity": identity,
                "candidateOutcomeRecordCount": 0,
                "laterRoleRecordCount": 0,
                "holdoutAuthorizationPresent": False,
                "maximumRecoveryAttempts": 1,
                "recoveryAttempt": 1,
            },
            "equivalenceEvidence": {
                "allRequiredTestsPassed": True,
                "completedBeforeRecoveryOutcomeAccess": True,
                "processExitCode": 0,
            },
        },
        "predecessor_state_binding": {
            "schema": "fresh-xauusd-durable-research-state/v1",
            "studyId": "xauusd-fresh-causal-acceleration-v2",
            "splitManifestSha256": identity["splitManifestSha256"],
            "researchWindowSetSha256": identity[
                "researchWindowSetSha256"
            ],
            "holdoutWindowSha256": identity["holdoutWindowSha256"],
        },
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


class FreshRestartEvidenceTests(unittest.TestCase):
    def test_workflow_extracts_only_the_exact_flat_run17_artifact(self) -> None:
        root = Path(__file__).resolve().parent
        workflow = (
            root / ".github/workflows/fresh-xauusd-research.yml"
        ).read_text(encoding="utf-8")
        invocation = (
            'python3 - "${restart_archive}" "${restart_directory}" <<\'PY\''
        )
        invocation_offset = workflow.index(invocation)
        script_start = workflow.index("          import hashlib\n", invocation_offset)
        script_end = workflow.index("\n          PY", script_start)
        extractor = textwrap.dedent(workflow[script_start:script_end])
        self.assertEqual(extractor.count(RUN17_ARCHIVE_SHA256), 1)
        allowed = {
            "fresh_corpus_manifest_v1.json",
            "fresh_entry_bank_v1.json",
            "fresh_experiment_ledger_v1.jsonl",
            "fresh_implementation_manifest_v1.json",
            "fresh_preregistration_v3.json",
            "fresh_quantile_bank_v1.json",
            "fresh_research_state_binding_v2.json",
            "fresh_source_inventory_v1.json",
            "fresh_split_manifest_v2.json",
            "fresh_threshold_domain_preflight_v1.json",
            "predecessor_fresh_experiment_ledger_v1.jsonl",
            "predecessor_fresh_implementation_manifest_v1.json",
            "predecessor_fresh_preregistration_v2.json",
            "predecessor_fresh_research_state_binding_v1.json",
            "remote-exit-status.txt",
            "server-run.log",
        }

        def archive_bytes(*, include_root: bool, extra_name: str | None = None) -> bytes:
            stream = io.BytesIO()
            with tarfile.open(fileobj=stream, mode="w:gz") as bundle:
                if include_root:
                    directory = tarfile.TarInfo(".")
                    directory.type = tarfile.DIRTYPE
                    directory.mode = 0o700
                    bundle.addfile(directory)
                for name in sorted(allowed):
                    payload = f"fixture:{name}\n".encode()
                    member = tarfile.TarInfo(f"./{name}")
                    member.mode = 0o600
                    member.size = len(payload)
                    bundle.addfile(member, io.BytesIO(payload))
                if extra_name is not None:
                    payload = b"unsafe\n"
                    member = tarfile.TarInfo(extra_name)
                    member.mode = 0o600
                    member.size = len(payload)
                    bundle.addfile(member, io.BytesIO(payload))
            return stream.getvalue()

        scratch = root / f".fresh-workflow-extractor-test-{uuid.uuid4().hex}"
        scratch.mkdir()
        self.addCleanup(shutil.rmtree, scratch, True)

        def run_extract(payload: bytes, label: str):
            archive = scratch / f"{label}.tgz"
            destination = scratch / label
            archive.write_bytes(payload)
            destination.mkdir()
            selected_script = extractor.replace(
                RUN17_ARCHIVE_SHA256,
                hashlib.sha256(payload).hexdigest(),
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    selected_script,
                    str(archive),
                    str(destination),
                ],
                cwd=root,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return completed, {path.name for path in destination.iterdir()}

        valid, names = run_extract(
            archive_bytes(include_root=True),
            "valid",
        )
        self.assertEqual(valid.returncode, 0, msg=valid.stderr)
        self.assertEqual(names, allowed)

        nested, _ = run_extract(
            archive_bytes(include_root=True, extra_name="./nested/evil"),
            "nested",
        )
        self.assertNotEqual(nested.returncode, 0)
        self.assertIn("unsafe run-17 archive member", nested.stderr)

        missing_root, _ = run_extract(
            archive_bytes(include_root=False),
            "missing-root",
        )
        self.assertNotEqual(missing_root.returncode, 0)
        self.assertIn("run-17 archive root member changed", missing_root.stderr)

    def test_run16_external_metadata_is_exact(self) -> None:
        self.assertEqual(RUN16_GITHUB_JOB_ID, 88917398289)
        self.assertEqual(RUN16_GITHUB_ARTIFACT_ID, 8533447491)
        self.assertEqual(
            RUN16_GITHUB_COMMIT_SHA,
            "740c149b145cbb26f26c6583d8cfd9861b6a8d0f",
        )
        self.assertEqual(
            RUN16_PREDECESSOR_PREREGISTRATION_SHA256,
            "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf",
        )

    def test_terminal_ledger_accepts_only_the_exact_ordered_evidence(
        self,
    ) -> None:
        records = _ledger_records()
        _validate_run16_ledger(records)
        mutations = (
            (0, "recordNumber", 2),
            (0, "recordSha256", "0" * 64),
            (1, "recordKind", "candidate-outcome"),
            (2, "status", "resume_authorized"),
            (3, "role", "walk_forward_1"),
        )
        for index, field, value in mutations:
            with self.subTest(index=index, field=field):
                changed = copy.deepcopy(records)
                changed[index][field] = value
                with self.assertRaises(PermissionError):
                    _validate_run16_ledger(changed)

    def test_terminal_ledger_rejects_claimed_sealed_or_recovered_results(
        self,
    ) -> None:
        for path, value in (
            (("metrics", "batchResultSealed"), True),
            (("metrics", "candidateOutcomesAppended"), 1),
            (("metrics", "externalExitStatus"), 0),
            (("leakageChecks", "recoveryAttemptConsumed"), False),
        ):
            with self.subTest(path=path):
                changed = copy.deepcopy(_ledger_records())
                changed[-1][path[0]][path[1]] = value
                with self.assertRaises(PermissionError):
                    _validate_run16_ledger(changed)

    def test_scientific_evidence_accepts_the_exact_inherited_identity(
        self,
    ) -> None:
        _validate_run16_scientific_evidence(**_scientific_evidence())

    def test_every_recovery_identity_is_tamper_evident(self) -> None:
        for key in RUN16_INHERITED_SCIENTIFIC_IDENTITIES:
            with self.subTest(key=key):
                evidence = _scientific_evidence()
                value = evidence["predecessor_recovery_contract"]["audit"][
                    "identity"
                ][key]
                evidence["predecessor_recovery_contract"]["audit"][
                    "identity"
                ][key] = value + 1 if isinstance(value, int) else "0" * 64
                with self.assertRaises(PermissionError):
                    _validate_run16_scientific_evidence(**evidence)

    def test_cross_file_identity_tampering_is_rejected(self) -> None:
        mutations = (
            ("inventory", "inventorySha256"),
            ("corpus", "corpusManifestSha256"),
            ("split", "manifestSha256"),
            (
                "predecessor_implementation_manifest",
                "manifestSha256",
            ),
            ("predecessor_state_binding", "holdoutWindowSha256"),
            ("quantile_bank", "bankSha256"),
            ("threshold_preflight", "totalRuntimeEntryCount"),
            ("entry_bank", "candidateGridSha256"),
        )
        for section, key in mutations:
            with self.subTest(section=section, key=key):
                evidence = _scientific_evidence()
                old = evidence[section][key]
                evidence[section][key] = (
                    old + 1 if isinstance(old, int) else "f" * 64
                )
                with self.assertRaises(PermissionError):
                    _validate_run16_scientific_evidence(**evidence)

    def test_progress_log_is_exact_and_duplicate_keys_are_rejected(
        self,
    ) -> None:
        identity = RUN16_INHERITED_SCIENTIFIC_IDENTITIES
        rows = [
            {
                "evidenceSha256": identity["equivalenceEvidenceSha256"],
                "stage": "recovery_equivalence_preflight",
                "status": "passed",
                "testModuleCount": 5,
            }
        ]
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
        rows.extend(
            {
                "sessionAnchor": anchor,
                "sessionCount": 40,
                "sessionOrdinal": ordinal,
                "stage": "discovery",
            }
            for ordinal, anchor in enumerate(anchors, start=1)
        )
        path = Path.cwd() / ".fresh-restart-progress-test.jsonl"
        try:
            path.write_text(
                "".join(
                    json.dumps(row, sort_keys=True) + "\n" for row in rows
                ),
                encoding="utf-8",
            )
            _validate_run16_progress_log(path)
            path.write_text(
                '{"stage":"discovery","stage":"holdout"}\n',
                encoding="utf-8",
            )
            with self.assertRaises(PermissionError):
                _validate_run16_progress_log(path)
        finally:
            path.unlink(missing_ok=True)

    def test_json_reader_rejects_duplicate_keys(self) -> None:
        path = Path.cwd() / ".fresh-restart-json-test.json"
        try:
            path.write_text('{"sealed":false,"sealed":true}', encoding="utf-8")
            with self.assertRaises(ValueError):
                _read_json(path)
        finally:
            path.unlink(missing_ok=True)

    def test_local_immutable_artifact_when_available(self) -> None:
        fixture = (
            Path(__file__).resolve().parent.parent
            / "run-artifacts"
            / "run-29918347818"
            / "extracted"
            / "payload"
        )
        if not fixture.is_dir():
            self.skipTest("immutable run-16 artifact is not in this checkout")
        bundle = load_fresh_v3_restart_bundle(fixture)
        self.assertEqual(len(bundle.ledger_records), 7)
        self.assertFalse(
            bundle.provenance["transientCandidateComputationsRecovered"]
        )
        self.assertEqual(
            bundle.provenance["predecessorOrderedLedgerRecordSha256"],
            list(RUN16_ORDERED_LEDGER_RECORD_SHA256),
        )


if __name__ == "__main__":
    unittest.main()
