"""Tests for pre-outcome V5 recovery terminal adoption."""

from __future__ import annotations

import base64
from contextlib import contextmanager
import copy
import hashlib
from importlib.util import module_from_spec, spec_from_file_location
import io
import json
from pathlib import Path
import shutil
import tarfile
import unittest
from unittest import mock
import uuid


ROOT = Path(__file__).resolve().parent
READER = (
    ROOT
    / ".github/scripts/fresh-xauusd-v5-recovery-terminal-reader.py"
)
WORKFLOW = (
    ROOT
    / ".github/workflows/"
    "fresh-xauusd-v5-recovery-terminal-adoption.yml"
)
TRIGGER_WORKFLOW = (
    ROOT
    / ".github/workflows/"
    "fresh-xauusd-v5-recovery-terminal-adoption-trigger.yml"
)
TRIGGER_MARKER = (
    ROOT / ".github/research-v5-recovery-adoption-launch.txt"
)
ORIGINAL_READER = (
    ROOT / ".github/scripts/fresh-xauusd-v5-terminal-reader.py"
)
ORIGINAL_WORKFLOW = (
    ROOT / ".github/workflows/fresh-xauusd-v5-terminal-adoption.yml"
)


def _load_reader():
    specification = spec_from_file_location(
        "fresh_v5_recovery_terminal_reader", READER
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("could not load recovery terminal reader")
    module = module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


reader = _load_reader()


RUN_ID = 40_123_456_789
RUN_ATTEMPT = 1
RUN_COMMIT = "a" * 40
CONTROLLER_SHA = "b" * 64


def _launch_receipt() -> dict:
    names = reader._receipt_names(RUN_ID, RUN_ATTEMPT)
    return {
        "schema": reader.RECEIPT_SCHEMA,
        "kind": "launch_ready",
        "status": "running",
        "processExitStatus": None,
        "githubRunId": RUN_ID,
        "githubRunAttempt": RUN_ATTEMPT,
        "branch": reader.RUN_BRANCH,
        "commitSha": RUN_COMMIT,
        "studyLineageSha256": reader.LINEAGE_SHA256,
        "recoveryAttemptId": reader.RECOVERY_ATTEMPT_ID,
        "sourceAdoptionRunId": reader.SOURCE_ADOPTION_RUN_ID,
        "sourceAdoptionArtifactId": reader.SOURCE_ADOPTION_ARTIFACT_ID,
        "sourceAdoptionArtifactDigest": (
            reader.SOURCE_ADOPTION_ARTIFACT_DIGEST
        ),
        "sourceTerminalArchiveSha256": (
            reader.SOURCE_TERMINAL_ARCHIVE_SHA256
        ),
        "controllerSha256": CONTROLLER_SHA,
        "controllerPid": 81001,
        "controllerStartTicks": 1_100_001,
        "pipelinePid": 81002,
        "pipelineStartTicks": 1_100_002,
        "paths": {
            "worktree": (
                "/tmp/fresh-xauusd-v5-recovery-worktree.AbCd1234"
            ),
            "output": "/tmp/fresh-xauusd-v5-recovery-output.EfGh5678",
            "scratch": (
                "/home/ec2-user/.local/state/datavis/"
                "fresh-xauusd-v5-recovery-scratch-v1/"
                f"attempt.{RUN_ID}.{RUN_ATTEMPT}.IjKl9012"
            ),
            "recovery": (
                "/tmp/fresh-xauusd-v5-recovery-input.MnOp3456"
            ),
            "state": str(reader.STATE_ROOT),
            "terminalArchive": str(
                reader.ARTIFACT_ROOT / names["archive"]
            ),
            "serverLog": (
                "/tmp/fresh-xauusd-v5-recovery.QrSt7890.log"
            ),
        },
        "terminalArchive": None,
    }


def _terminal_receipt(
    launch: dict,
    *,
    size: int = 1234,
    digest: str = "c" * 64,
    exit_status: int = 0,
) -> dict:
    terminal = copy.deepcopy(launch)
    terminal.update(
        {
            "kind": "terminal",
            "status": "succeeded" if exit_status == 0 else "failed",
            "processExitStatus": exit_status,
            "terminalArchive": {
                "size": size,
                "sha256": digest,
                "device": 2049,
                "inode": 123456,
            },
        }
    )
    return terminal


def _write_tar(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, mode="w:gz") as bundle:
        for name, raw in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(raw)
            info.mode = 0o600
            bundle.addfile(info, io.BytesIO(raw))


def _archive_fixture(directory: Path) -> tuple[Path, dict[str, bytes]]:
    members = {
        "fresh_experiment_ledger_v1.jsonl": b'{"record":"sealed"}\n',
        "remote-exit-status.txt": b"0\n",
        "server-run.log": b"terminal\n",
        "fresh_summary_v1.json": b'{"status":"complete"}\n',
    }
    path = directory / f"v5-recovery-{RUN_ID}-{RUN_ATTEMPT}.tgz"
    _write_tar(path, members)
    return path, members


@contextmanager
def _workspace_directory():
    path = ROOT / f"fresh-v5-adoption-fixture-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class RecoveryReceiptTests(unittest.TestCase):
    def test_runtime_launch_identity_and_terminal_are_exactly_bound(
        self,
    ) -> None:
        launch = _launch_receipt()
        paths = reader.validate_launch_receipt(
            launch,
            expected_run_id=RUN_ID,
            expected_run_attempt=RUN_ATTEMPT,
            expected_commit_sha=RUN_COMMIT,
            expected_controller_sha256=CONTROLLER_SHA,
        )
        self.assertEqual(
            paths["terminalArchive"].name,
            f"v5-recovery-{RUN_ID}-{RUN_ATTEMPT}.tgz",
        )
        terminal = _terminal_receipt(launch)
        identity = reader.validate_terminal_receipt(launch, terminal)
        self.assertEqual(identity["sha256"], "c" * 64)

        changed = copy.deepcopy(terminal)
        changed["commitSha"] = "d" * 40
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_terminal_receipt(launch, changed)
        changed = copy.deepcopy(terminal)
        changed["status"] = "failed"
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_terminal_receipt(launch, changed)

    def test_launch_rejects_unsafe_or_wrong_runtime_identity(self) -> None:
        launch = _launch_receipt()
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_launch_receipt(
                launch, expected_run_id=RUN_ID + 1
            )
        changed = copy.deepcopy(launch)
        changed["githubRunAttempt"] = True
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_launch_receipt(changed)
        changed = copy.deepcopy(launch)
        changed["sourceAdoptionArtifactId"] += 1
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_launch_receipt(changed)
        changed = copy.deepcopy(launch)
        changed["paths"]["output"] = "/tmp/../escaped"
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_launch_receipt(changed)
        changed = copy.deepcopy(launch)
        changed["paths"]["terminalArchive"] = (
            "/tmp/v5-recovery-terminal.tgz"
        )
        with self.assertRaises(reader.RecoveryAdoptionError):
            reader.validate_launch_receipt(changed)

    def test_controller_and_pipeline_commands_are_fully_constrained(
        self,
    ) -> None:
        launch = _launch_receipt()
        paths = reader.validate_launch_receipt(launch)
        transfer = "/tmp/fresh-xauusd-v5-recovery-transfer.UvWx1234"
        controller_arguments = [
            "python3",
            "-I",
            "-B",
            f"{transfer}/fresh-xauusd-v5-recovery-controller.py",
            "--branch",
            reader.RUN_BRANCH,
            "--commit-sha",
            RUN_COMMIT,
            "--run-id",
            str(RUN_ID),
            "--run-attempt",
            str(RUN_ATTEMPT),
            "--terminal-input",
            f"{transfer}/{reader.SOURCE_TERMINAL_ARCHIVE_NAME}",
            "--terminal-archive",
            str(paths["terminalArchive"]),
            "--ready-receipt",
            str(
                reader.LAUNCH_ROOT
                / f"v5-recovery-{RUN_ID}-{RUN_ATTEMPT}.ready.json"
            ),
            "--failure-receipt",
            str(
                reader.LAUNCH_ROOT
                / f"v5-recovery-{RUN_ID}-{RUN_ATTEMPT}.failure.json"
            ),
            "--terminal-receipt",
            str(
                reader.LAUNCH_ROOT
                / f"v5-recovery-{RUN_ID}-{RUN_ATTEMPT}.terminal.json"
            ),
            "--expected-controller-sha256",
            CONTROLLER_SHA,
        ]
        self.assertTrue(
            reader._controller_arguments_are_expected(
                controller_arguments, launch
            )
        )
        controller_arguments[-1] = "0" * 64
        self.assertFalse(
            reader._controller_arguments_are_expected(
                controller_arguments, launch
            )
        )

        pipeline_arguments = [
            str(paths["worktree"] / ".fresh-venv/bin/python"),
            "-m",
            reader.PIPELINE_MODULE,
            "--repository-root",
            str(paths["worktree"]),
            "--output-dir",
            str(paths["output"]),
            "--scratch-dir",
            str(paths["scratch"]),
            "--research-state-dir",
            str(paths["state"]),
            "--resume-v5-artifact-dir",
            str(paths["recovery"]),
            "--execute",
        ]
        self.assertTrue(
            reader._pipeline_arguments_are_expected(
                pipeline_arguments, paths
            )
        )
        pipeline_arguments[-1] = "--dry-run"
        self.assertFalse(
            reader._pipeline_arguments_are_expected(
                pipeline_arguments, paths
            )
        )

    def test_active_probe_returns_75_without_a_remote_write(self) -> None:
        launch = _launch_receipt()
        encoded = base64.b64encode(
            reader._canonical_json_bytes(launch)
        ).decode("ascii")
        with (
            mock.patch.object(
                reader, "_read_terminal_receipt", return_value=None
            ),
            mock.patch.object(
                reader,
                "_process_states",
                return_value=("active", "active"),
            ),
        ):
            self.assertEqual(reader.remote_probe(encoded, "0"), 75)


class RecoveryArchiveTests(unittest.TestCase):
    def test_archive_and_four_member_manifest_are_transport_exact(
        self,
    ) -> None:
        with _workspace_directory() as directory:
            archive, _ = _archive_fixture(directory)
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            launch = _launch_receipt()
            terminal = _terminal_receipt(
                launch,
                size=archive.stat().st_size,
                digest=digest,
            )
            launch_path = directory / (
                "fresh-xauusd-v5-recovery-launch-receipt.json"
            )
            terminal_path = directory / (
                "fresh-xauusd-v5-recovery-terminal-receipt.json"
            )
            launch_path.write_bytes(reader._canonical_json_bytes(launch))
            terminal_path.write_bytes(
                reader._canonical_json_bytes(terminal)
            )
            reader.verify_local_archive(
                archive, launch_path, terminal_path
            )
            manifest_path = directory / (
                "fresh-xauusd-v5-recovery-adoption-manifest.json"
            )
            reader.build_manifest(
                manifest_path,
                launch_path,
                terminal_path,
                archive,
                RUN_ID,
                RUN_ATTEMPT,
                RUN_COMMIT,
                9_123_456_789,
                f"sha256:{'d' * 64}",
                2048,
                41_123_456_789,
                1,
                "e" * 40,
            )
            manifest_raw = manifest_path.read_bytes()
            manifest = json.loads(manifest_raw)
            self.assertEqual(
                manifest_raw, reader._canonical_json_bytes(manifest)
            )
            self.assertEqual(manifest["schema"], reader.MANIFEST_SCHEMA)
            self.assertFalse(manifest["adoption"]["remoteMutation"])
            self.assertEqual(
                manifest["source"]["githubRunId"], RUN_ID
            )
            self.assertEqual(len(manifest["members"]), 3)
            self.assertEqual(
                len(
                    [
                        launch_path,
                        terminal_path,
                        archive,
                        manifest_path,
                    ]
                ),
                4,
            )
            cli_manifest = directory / "cli-manifest.json"
            self.assertEqual(
                reader.main(
                    [
                        "build-manifest",
                        str(cli_manifest),
                        str(launch_path),
                        str(terminal_path),
                        str(archive),
                        str(RUN_ID),
                        str(RUN_ATTEMPT),
                        RUN_COMMIT,
                        "9123456789",
                        f"sha256:{'d' * 64}",
                        "2048",
                        "41123456789",
                        "1",
                        "e" * 40,
                    ]
                ),
                0,
            )

    def test_archive_rejects_path_traversal(self) -> None:
        with _workspace_directory() as directory:
            archive = directory / (
                f"v5-recovery-{RUN_ID}-{RUN_ATTEMPT}.tgz"
            )
            members = {
                "fresh_experiment_ledger_v1.jsonl": b"{}\n",
                "remote-exit-status.txt": b"1\n",
                "server-run.log": b"failure\n",
                "../escape": b"forbidden\n",
            }
            _write_tar(archive, members)
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            launch = _launch_receipt()
            terminal = _terminal_receipt(
                launch,
                size=archive.stat().st_size,
                digest=digest,
                exit_status=1,
            )
            launch_path = directory / "launch.json"
            terminal_path = directory / "terminal.json"
            launch_path.write_bytes(reader._canonical_json_bytes(launch))
            terminal_path.write_bytes(
                reader._canonical_json_bytes(terminal)
            )
            with self.assertRaises(reader.RecoveryAdoptionError):
                reader.verify_local_archive(
                    archive, launch_path, terminal_path
                )


class RecoveryWorkflowTests(unittest.TestCase):
    def test_workflow_is_pre_outcome_and_rechecks_immutable_metadata(
        self,
    ) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        reader_digest = hashlib.sha256(READER.read_bytes()).hexdigest()
        self.assertIn(f"READER_SHA256: {reader_digest}", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("\n  push:", workflow)
        for input_name in (
            "launch_run_id:",
            "launch_run_attempt:",
            "launch_commit_sha:",
            "launch_artifact_id:",
            "launch_artifact_size:",
            "launch_artifact_digest:",
        ):
            self.assertIn(input_name, workflow)
        self.assertIn(
            "fresh-xauusd-v5-recovery-detached-launch.yml", workflow
        )
        self.assertIn("actions/runs/${LAUNCH_RUN_ID}", workflow)
        self.assertIn(
            "actions/artifacts/${LAUNCH_ARTIFACT_ID}", workflow
        )
        self.assertIn("api-before", workflow)
        self.assertIn("api-after", workflow)
        self.assertIn(
            "metadata changed during download", workflow
        )
        self.assertIn(
            "actions/download-artifact@"
            "d3f86a106a0bac45b974a628896c90dbdf5c8093",
            workflow,
        )
        self.assertIn(
            "actions/upload-artifact@"
            "ea165f8d65b6e75b540449e92b4886f43607fa02",
            workflow,
        )
        self.assertNotIn("__READER_SHA256__", workflow)
        self.assertNotIn(str(RUN_ID), workflow)

    def test_remote_commands_are_read_only_and_active_is_not_adopted(
        self,
    ) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("scp ", workflow)
        self.assertEqual(
            workflow.count("python3 -I -B - probe"), 1
        )
        self.assertEqual(
            workflow.count("python3 -I -B - stream"), 1
        )
        self.assertIn('if [[ "${probe_status}" -eq 75 ]]', workflow)
        self.assertIn("no remote state was changed", workflow)
        self.assertIn("ready=false", workflow)
        self.assertIn(
            "if: steps.adoption.outputs.ready == 'true'", workflow
        )
        self.assertIn("StrictHostKeyChecking=yes", workflow)
        self.assertNotIn("StrictHostKeyChecking=accept-new", workflow)
        self.assertIn("-o IdentitiesOnly=yes", workflow)
        self.assertIn("-o HostKeyAlgorithms=ssh-ed25519", workflow)

        source = READER.read_text(encoding="utf-8")
        remote = source[
            source.index("def _require_remote_read_flags")
            : source.index("def _regular_file_identity")
        ]
        self.assertIn("os.O_NOFOLLOW", remote)
        self.assertIn("os.O_NOATIME", remote)
        self.assertIn("os.O_PATH", remote)
        for mutation in (
            "os.O_WRONLY",
            "os.O_CREAT",
            "os.unlink",
            "os.remove",
            "os.rename",
            "os.replace",
            ".mkdir(",
            ".write_bytes(",
        ):
            self.assertNotIn(mutation, remote)

    def test_original_v5_adoption_sources_remain_separate(self) -> None:
        self.assertTrue(ORIGINAL_READER.is_file())
        self.assertTrue(ORIGINAL_WORKFLOW.is_file())
        self.assertNotEqual(READER, ORIGINAL_READER)
        self.assertNotEqual(WORKFLOW, ORIGINAL_WORKFLOW)
        self.assertNotIn(
            "fresh-xauusd-v5-recovery-terminal-reader",
            ORIGINAL_READER.read_text(encoding="utf-8"),
        )
        self.assertNotIn(
            "fresh-xauusd-v5-recovery-terminal-adoption",
            ORIGINAL_WORKFLOW.read_text(encoding="utf-8"),
        )

    def test_branch_push_trigger_calls_same_read_only_workflow(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        trigger = TRIGGER_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_call:", workflow)
        for input_name in (
            "launch_run_id",
            "launch_run_attempt",
            "launch_commit_sha",
            "launch_artifact_id",
            "launch_artifact_size",
            "launch_artifact_digest",
            "wait_minutes",
        ):
            self.assertGreaterEqual(workflow.count(f"      {input_name}:"), 2)
        self.assertIn(
            "branches:\n      - codex/xauusd-fresh-walkforward",
            trigger,
        )
        self.assertIn(
            "- .github/research-v5-recovery-adoption-launch.txt",
            trigger,
        )
        self.assertIn(
            "uses: ./.github/workflows/"
            "fresh-xauusd-v5-recovery-terminal-adoption.yml",
            trigger,
        )
        self.assertIn("    secrets: inherit", trigger)
        expected = {
            "launch_run_id": "30132173254",
            "launch_run_attempt": "1",
            "launch_commit_sha": (
                "72784c36b00223ca166b3f0d904ab4c93db7dcc1"
            ),
            "launch_artifact_id": "8611513967",
            "launch_artifact_size": "911",
            "launch_artifact_digest": (
                "sha256:"
                "192c9661393571a5b64730665191914ed682890a"
                "fc91ee271057999f49d64683"
            ),
            "wait_minutes": "0",
        }
        for name, value in expected.items():
            self.assertIn(f'{name}: "{value}"', trigger)
        self.assertEqual(
            TRIGGER_MARKER.read_text(encoding="utf-8"),
            (
                "fresh-xauusd-v5-recovery-adoption-"
                "30132173254-attempt-1-probe-0-v2\n"
            ),
        )


if __name__ == "__main__":
    unittest.main()
