from __future__ import annotations

import copy
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from datavis.research import fresh_recovery_terminal_audit as audit
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_recovery_v5 import (
    required_fresh_v5_recovery_implementation_files,
)


ROOT = Path(__file__).resolve().parent
REMOTE_ROOT = "/tmp/fresh-xauusd-v5-recovery-worktree.AuditFixture1"


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _manifest(
    files: dict[str, bytes],
    *,
    remote_root: str = REMOTE_ROOT,
) -> dict[str, object]:
    body = {
        "schema": audit.IMPLEMENTATION_MANIFEST_SCHEMA,
        "repositoryRoot": remote_root,
        "files": [
            {"path": path, "sha256": _sha256(files[path])}
            for path in audit.RECOVERY_IMPLEMENTATION_FILES
        ],
    }
    return {**body, "manifestSha256": canonical_hash(body)}


class RecoveryAuditGitFixture(unittest.TestCase):
    root: Path
    files: dict[str, bytes]
    commit: str
    manifest: dict[str, object]

    @classmethod
    def setUpClass(cls) -> None:
        cls.root = ROOT / f".fresh-recovery-audit-git-{uuid.uuid4().hex}"
        cls.root.mkdir(mode=0o777)
        cls.addClassCleanup(shutil.rmtree, cls.root, True)
        cls.files = {}
        for index, relative in enumerate(
            audit.RECOVERY_IMPLEMENTATION_FILES,
            start=1,
        ):
            raw = f"sealed launch blob {index}: {relative}\n".encode()
            cls.files[relative] = raw
            path = cls.root / Path(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        commands = (
            ("init",),
            ("config", "user.email", "audit@example.invalid"),
            ("config", "user.name", "Recovery Audit Fixture"),
            ("add", "--", "."),
            ("commit", "-m", "sealed recovery launch fixture"),
        )
        for command in commands:
            subprocess.run(
                ["git", "-C", str(cls.root), *command],
                check=True,
                capture_output=True,
            )
        cls.commit = subprocess.run(
            ["git", "-C", str(cls.root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        cls.manifest = _manifest(cls.files)

    def test_explicit_closure_matches_launch_protocol(self) -> None:
        self.assertEqual(len(audit.RECOVERY_IMPLEMENTATION_FILES), 49)
        self.assertEqual(
            audit.RECOVERY_IMPLEMENTATION_FILES,
            tuple(sorted(audit.RECOVERY_IMPLEMENTATION_FILES)),
        )
        self.assertEqual(
            audit.RECOVERY_IMPLEMENTATION_FILES,
            required_fresh_v5_recovery_implementation_files(),
        )

    def test_remote_manifest_is_verified_from_exact_git_blobs(self) -> None:
        verified = audit.verify_recovery_implementation_manifest_offline(
            self.manifest,
            launch_source_root=self.root,
            expected_launch_commit_sha=self.commit,
            recorded_remote_repository_root=REMOTE_ROOT,
        )
        self.assertEqual(verified.commit_sha, self.commit)
        self.assertEqual(
            verified.implementation_manifest_sha256,
            self.manifest["manifestSha256"],
        )
        self.assertEqual(
            verified.total_bytes,
            sum(len(raw) for raw in self.files.values()),
        )
        self.assertRegex(verified.closure_sha256, r"^[0-9a-f]{64}$")

    def test_manifest_tampering_and_wrong_commit_fail_closed(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["files"][0]["sha256"] = "0" * 64
        body = {
            key: value
            for key, value in changed.items()
            if key != "manifestSha256"
        }
        changed["manifestSha256"] = canonical_hash(body)
        with self.assertRaisesRegex(
            audit.FreshRecoveryTerminalAuditError,
            "differs from launch Git blob",
        ):
            audit.verify_recovery_implementation_manifest_offline(
                changed,
                launch_source_root=self.root,
                expected_launch_commit_sha=self.commit,
                recorded_remote_repository_root=REMOTE_ROOT,
            )
        with self.assertRaisesRegex(
            audit.FreshRecoveryTerminalAuditError,
            "triggering commit",
        ):
            audit.verify_recovery_implementation_manifest_offline(
                self.manifest,
                launch_source_root=self.root,
                expected_launch_commit_sha="0" * 40,
                recorded_remote_repository_root=REMOTE_ROOT,
            )

    def test_manifest_root_must_match_launch_receipt(self) -> None:
        with self.assertRaisesRegex(
            audit.FreshRecoveryTerminalAuditError,
            "differs from the launch receipt",
        ):
            audit.verify_recovery_implementation_manifest_offline(
                self.manifest,
                launch_source_root=self.root,
                expected_launch_commit_sha=self.commit,
                recorded_remote_repository_root=(
                    "/tmp/fresh-xauusd-v5-recovery-worktree.Other"
                ),
            )


class RecoveryAuditProtocolAdapterTests(unittest.TestCase):
    @staticmethod
    def _fake_cat_file_process():
        class FakeProcess:
            def __init__(self) -> None:
                self.stdin = io.BytesIO()
                self.stdout = io.BytesIO(
                    b"1" * 40 + b" blob 4\nDATA\n"
                )
                self.stderr = io.BytesIO()
                self.returncode = None

            def poll(self):
                return self.returncode

            def kill(self) -> None:
                self.returncode = -9

            def wait(self) -> int:
                if self.returncode is None:
                    self.returncode = 0
                return self.returncode

        return FakeProcess()

    def test_git_batch_pipes_close_on_success_and_error(self) -> None:
        tree = b"100644 blob " + b"1" * 40 + b"\tfile.py\0"
        success = self._fake_cat_file_process()
        with (
            patch.object(audit, "_run_git", return_value=tree),
            patch.object(
                audit.subprocess,
                "Popen",
                return_value=success,
            ),
        ):
            self.assertEqual(
                audit._git_blobs(
                    Path("/launch"),
                    "a" * 40,
                    ("file.py",),
                ),
                {"file.py": b"DATA"},
            )
        self.assertTrue(success.stdin.closed)
        self.assertTrue(success.stdout.closed)
        self.assertTrue(success.stderr.closed)

        failed = self._fake_cat_file_process()
        with (
            patch.object(audit, "_run_git", return_value=tree),
            patch.object(
                audit.subprocess,
                "Popen",
                return_value=failed,
            ),
            patch.object(audit, "MAX_SOURCE_CLOSURE_BYTES", 1),
            self.assertRaises(audit.FreshRecoveryTerminalAuditError),
        ):
            audit._git_blobs(
                Path("/launch"),
                "a" * 40,
                ("file.py",),
            )
        self.assertTrue(failed.stdin.closed)
        self.assertTrue(failed.stdout.closed)
        self.assertTrue(failed.stderr.closed)

    def test_only_live_path_resolution_is_replaced_and_restored(self) -> None:
        original_calls: list[object] = []

        def original_manifest_validator(_manifest):
            original_calls.append("unexpected")
            raise AssertionError("live-path verifier must be replaced")

        protocol = SimpleNamespace()
        protocol._validate_recovery_implementation_manifest = (
            original_manifest_validator
        )

        expected_proof = {
            "schema": audit.RECOVERY_PROOF_SCHEMA,
            "candidateOutcomeRecordCount": 240,
            "orderedCandidateSequenceSha256": (
                audit.ORDERED_CANDIDATE_SEQUENCE_SHA256
            ),
            "recoveryImplementationManifestSha256": "a" * 64,
        }

        def validate(**values):
            manifest_sha, root, files = (
                protocol._validate_recovery_implementation_manifest(
                    values["recovery_implementation_manifest"]
                )
            )
            self.assertEqual(manifest_sha, "a" * 64)
            self.assertEqual(root, Path("/verified-launch"))
            self.assertEqual(files, {"source.py": "b" * 64})
            return expected_proof

        protocol.validate_fresh_v5_recovery_for_holdout = validate
        manifest = {
            "schema": "fixture",
            "manifestSha256": "a" * 64,
        }
        source = audit.VerifiedRecoveryLaunchSource(
            commit_sha="c" * 40,
            recorded_remote_repository_root=REMOTE_ROOT,
            implementation_manifest_sha256="a" * 64,
            file_sha256={"source.py": "b" * 64},
            closure_sha256="d" * 64,
            total_bytes=10,
        )
        result = audit._invoke_protocol_validator(
            protocol,
            materialized_root=Path("/verified-launch"),
            manifest=manifest,
            verified_source=source,
            records=(),
            preregistration={},
            split_manifest={},
            recovery_contract={},
            sealed_batch_result_path=Path("/sealed-batch"),
        )
        self.assertEqual(result, expected_proof)
        self.assertIs(
            protocol._validate_recovery_implementation_manifest,
            original_manifest_validator,
        )
        self.assertEqual(original_calls, [])


class RecoveryAuditBundleTests(unittest.TestCase):
    root: Path
    source: audit.VerifiedRecoveryLaunchSource
    proof: dict[str, object]

    def setUp(self) -> None:
        self.root = ROOT / f".fresh-recovery-output-{uuid.uuid4().hex}"
        self.root.mkdir(mode=0o777)
        self.addCleanup(shutil.rmtree, self.root, True)
        original_hashes: dict[str, str] = {}
        for name in audit.ORIGINAL_V5_OUTPUT_SHA256:
            if name == "original_v5_remote-exit-status.txt":
                raw = b"137\n"
            elif name.endswith(".jsonl"):
                raw = b"{}\n"
            elif name.endswith(".json"):
                raw = b"{}\n"
            else:
                raw = b"frozen original log\n"
            (self.root / name).write_bytes(raw)
            original_hashes[name] = _sha256(raw)
        for name, raw in {
            "fresh_recovery_implementation_manifest_v1.json": b"{}\n",
            "fresh_recovery_contract_v1.json": json.dumps(
                {"schema": audit.RECOVERY_CONTRACT_SCHEMA},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n",
            "fresh_recovery_discovery_batch_v1.json": b"{}\n",
            "fresh_experiment_ledger_v1.jsonl": b"{}\n",
            "server-run.log": b"recovery log\n",
            "remote-exit-status.txt": b"0\n",
        }.items():
            (self.root / name).write_bytes(raw)
        self.source = audit.VerifiedRecoveryLaunchSource(
            commit_sha="c" * 40,
            recorded_remote_repository_root=REMOTE_ROOT,
            implementation_manifest_sha256="a" * 64,
            file_sha256={"source.py": "b" * 64},
            closure_sha256="d" * 64,
            total_bytes=10,
        )
        self.proof = {
            "schema": audit.RECOVERY_PROOF_SCHEMA,
            "candidateOutcomeRecordCount": 240,
            "orderedCandidateSequenceSha256": (
                audit.ORDERED_CANDIDATE_SEQUENCE_SHA256
            ),
            "recoveryImplementationManifestSha256": "a" * 64,
        }
        summary = {
            "schema": "fresh-xauusd-chronological-run/v1",
            "status": "no_robust_setup_survived_frozen_validation",
            "recoveryUsed": True,
            "recoveryVersion": "v5-same-lineage",
            "recoveryOriginalRunId": audit.ORIGINAL_RUN_ID,
            "recoveryImplementationManifestSha256": "a" * 64,
            "infrastructureRestartUsed": True,
            "infrastructureRestartVersion": 5,
            "studyId": audit.STUDY_ID,
            "studyLineageSha256": audit.STUDY_LINEAGE_SHA256,
            "splitManifestSha256": audit.SPLIT_MANIFEST_SHA256,
            "holdoutOpened": False,
        }
        summary["artifactFiles"] = sorted(
            path.name
            for path in self.root.iterdir()
            if path.name not in {"server-run.log", "remote-exit-status.txt"}
        )
        (self.root / "fresh_run_summary_v1.json").write_text(
            json.dumps(summary, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        self.original_hashes = original_hashes

    def _patches(self):
        return (
            patch.object(
                audit,
                "ORIGINAL_V5_OUTPUT_SHA256",
                self.original_hashes,
            ),
            patch.object(
                audit,
                "verify_recovery_implementation_manifest_offline",
                return_value=self.source,
            ),
            patch.object(audit, "_verify_preregistration_and_split"),
            patch.object(
                audit,
                "_verified_ledger",
                return_value=tuple({} for _ in range(248)),
            ),
            patch.object(
                audit,
                "_validate_with_launch_protocol",
                return_value=self.proof,
            ),
        )

    def test_gate_receipt_never_interprets_strategy_result(self) -> None:
        first, second, third, fourth, fifth = self._patches()
        with first, second, third, fourth, fifth:
            result = audit.audit_recovery_scientific_gate(
                self.root,
                launch_source_root=self.root,
                expected_launch_commit_sha="c" * 40,
                recorded_remote_repository_root=REMOTE_ROOT,
            )
        self.assertEqual(
            result["status"],
            "recovery_scientific_gate_verified",
        )
        self.assertFalse(result["scientificResultInterpreted"])
        self.assertFalse(result["finalStrategyConclusionAuthorized"])
        self.assertFalse(result["holdoutOpened"])
        self.assertTrue(result["duplicateTicksRemainVolume"])

    def test_nonzero_recovery_is_never_scientific_evidence(self) -> None:
        (self.root / "remote-exit-status.txt").write_bytes(b"137\n")
        with self.assertRaisesRegex(
            audit.FreshRecoveryTerminalAuditError,
            "infrastructure evidence",
        ):
            audit.audit_recovery_scientific_gate(
                self.root,
                launch_source_root=self.root,
                expected_launch_commit_sha="c" * 40,
                recorded_remote_repository_root=REMOTE_ROOT,
            )


class OriginalAuditorPreservationTests(unittest.TestCase):
    def test_original_v5_auditor_seals_are_unchanged(self) -> None:
        expected = {
            "datavis/research/fresh_terminal_audit.py": (
                "fa51cd73dde0e51d3854b6563f5fcdbb737aca70d8a86fbaaf87c543435c9050"
            ),
            "datavis/research/fresh_terminal_audit_bootstrap.py": (
                "b882794e0b5df18850b6c283229a4bcf833bb30b4aa7fd63a04184d86a6d9cde"
            ),
            ".github/workflows/fresh-xauusd-v5-terminal-audit.yml": (
                "4d3b1b9d3e9ae922a427ada5e264fcafa611891146a0e049a37c7c233313ebbb"
            ),
            ".github/research-v5-terminal-audit.txt": (
                "9c1388a2f7ea8f672c401b2d98dc5b270de38293cf9d7f01d4c241d7626562e7"
            ),
        }
        for relative, digest in expected.items():
            with self.subTest(path=relative):
                self.assertEqual(
                    hashlib.sha256((ROOT / relative).read_bytes()).hexdigest(),
                    digest,
                )


if __name__ == "__main__":
    unittest.main()
