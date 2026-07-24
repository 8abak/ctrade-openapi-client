from __future__ import annotations

import argparse
import ast
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent
CONTROLLER_PATH = (
    ROOT / ".github/scripts/fresh-xauusd-v5-monitor-controller.py"
)
REMOTE_PATH = ROOT / ".github/scripts/fresh-xauusd-v5-remote-monitor.py"
WORKFLOW_PATH = (
    ROOT / ".github/workflows/fresh-xauusd-v5-read-only-monitor.yml"
)
MARKER_PATH = ROOT / ".github/research-v5-monitor.txt"


def load_module(name: str, path: Path):
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


monitor = load_module("fresh_v5_monitor_controller", CONTROLLER_PATH)
remote = load_module("fresh_v5_remote_monitor", REMOTE_PATH)


def launch_receipt():
    return {
        "branch": "codex/xauusd-fresh-walkforward",
        "commitSha": "bc7c814876cc75a0fbe85ba824177ad8baccd5cf",
        "controllerPid": 486270,
        "controllerSha256": (
            "da57bce0f90890a8712edbb8cb9830054bfc5b2b3d544c2363420836b8b9ce3f"
        ),
        "controllerStartTicks": 1069712502,
        "githubRunAttempt": 1,
        "githubRunId": 30067832187,
        "kind": "launch_ready",
        "paths": {
            "output": "/tmp/fresh-xauusd-output.eVrX3i",
            "restart": "/tmp/fresh-xauusd-restart.KJ622R",
            "scratch": (
                "/home/ec2-user/.local/state/datavis/"
                "fresh-xauusd-scratch-v1/run.30067832187.1.QrG3VH"
            ),
            "serverLog": "/tmp/fresh-xauusd-run.c5EeE6.log",
            "terminalArchive": (
                "/home/ec2-user/.local/state/datavis/"
                "fresh-xauusd-artifacts-v1/"
                "fresh-xauusd-30067832187-1.tgz"
            ),
            "transfer": "/tmp/fresh-xauusd-transfer.SEMXw4",
            "worktree": "/tmp/fresh-xauusd-worktree.E4Jrbc",
        },
        "pipelinePid": 486543,
        "pipelineStartTicks": 1069715129,
        "processExitStatus": None,
        "run19ArtifactId": 8585919266,
        "run19TerminalArchiveSha256": (
            "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
        ),
        "schema": "fresh-xauusd-detached-research-receipt/v1",
        "status": "running",
        "studyLineageSha256": (
            "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
        ),
        "terminalArchive": None,
    }


def source_plan():
    return {
        "schema": monitor.PLAN_SCHEMA,
        "repository": monitor.REPOSITORY,
        "sourceRunId": monitor.SOURCE_RUN_ID,
        "sourceRunAttempt": monitor.SOURCE_ATTEMPT,
        "sourceCommitSha": monitor.SOURCE_COMMIT,
        "sourceBranch": monitor.SOURCE_BRANCH,
        "sourceWorkflow": monitor.SOURCE_WORKFLOW,
        "artifactId": monitor.SOURCE_ARTIFACT_ID,
        "artifactName": monitor.SOURCE_ARTIFACT_NAME,
        "artifactDigest": monitor.SOURCE_ARTIFACT_DIGEST,
        "artifactSize": monitor.SOURCE_ARTIFACT_SIZE,
        "archiveDownloadUrl": (
            "https://api.github.com/repos/8abak/ctrade-openapi-client/"
            "actions/artifacts/8586881858/zip"
        ),
    }


def process(pid, start, parent, group, session):
    return {
        "pid": pid,
        "state": "S",
        "parentPid": parent,
        "processGroup": group,
        "sessionId": session,
        "cpuTicks": 100,
        "startTicks": start,
        "threads": 1,
        "vmRssKiB": 1000,
        "vmSwapKiB": 0,
        "readBytes": 10,
        "writeBytes": 20,
        "commandSha256": "1" * 64,
        "commandVerified": True,
        "ownershipVerified": True,
    }


def filesystem_object(kind):
    return {
        "exists": True,
        "kind": kind,
        "device": 1,
        "inode": 2,
        "uid": 1000,
        "gid": 1000,
        "mode": 0o700 if kind == "directory" else 0o600,
        "size": 64,
        "mtimeNs": 1,
    }


def runtime_snapshot():
    receipt = launch_receipt()
    directory_names = {
        "worktree",
        "output",
        "scratch",
        "restart",
        "transfer",
        "stateRoot",
        "launchRoot",
        "claimRoot",
        "artifactRoot",
    }
    objects = {
        name: filesystem_object(
            "directory" if name in directory_names else "regular"
        )
        for name in monitor.OBJECT_KEYS
    }
    for name in monitor.OPTIONAL_OBJECTS:
        objects[name] = {"exists": False}
    volume = {
        "bytesAvailable": 10,
        "bytesTotal": 20,
        "inodesAvailable": 30,
        "inodesTotal": 40,
    }
    return {
        "schema": monitor.REMOTE_SNAPSHOT_SCHEMA,
        "scope": "process-and-filesystem-metadata-only",
        "repository": monitor.REPOSITORY,
        "sourceLaunch": {
            "runId": monitor.SOURCE_RUN_ID,
            "runAttempt": monitor.SOURCE_ATTEMPT,
            "commitSha": monitor.SOURCE_COMMIT,
            "branch": monitor.SOURCE_BRANCH,
            "studyLineageSha256": monitor.STUDY_LINEAGE_SHA256,
            "controllerSha256": monitor.CONTROLLER_SHA256,
            "launchReceiptSha256": monitor.SOURCE_RECEIPT_SHA256,
        },
        "capturedUtc": "2026-07-24T05:00:00+00:00",
        "sampleSeconds": 3.0,
        "initialLifecycle": "running",
        "lifecycle": "running",
        "processes": {
            "controller": process(
                receipt["controllerPid"],
                receipt["controllerStartTicks"],
                1,
                receipt["controllerPid"],
                receipt["controllerPid"],
            ),
            "pipeline": process(
                receipt["pipelinePid"],
                receipt["pipelineStartTicks"],
                receipt["controllerPid"],
                receipt["controllerPid"],
                receipt["controllerPid"],
            ),
            "controllerDelta": {
                "cpuTicks": 1,
                "readBytes": 2,
                "writeBytes": 3,
            },
            "pipelineDelta": {
                "cpuTicks": 1,
                "readBytes": 2,
                "writeBytes": 3,
            },
        },
        "filesystem": {
            "objects": objects,
            "volumes": {
                name: dict(volume)
                for name in ("output", "scratch", "state", "artifacts")
            },
        },
        "outcomeFilesOpened": False,
        "researchFileContentsRead": False,
        "remoteFilesystemWritesAttempted": False,
    }


class FrozenLaunchEvidenceTests(unittest.TestCase):
    def test_exact_launch_receipt_byte_identity_is_frozen(self):
        payload = launch_receipt()
        monitor.validate_launch_receipt(payload)
        raw = monitor.canonical_bytes(payload)
        self.assertEqual(len(raw), 1146)
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            monitor.SOURCE_RECEIPT_SHA256,
        )

    def test_source_artifact_metadata_is_exact(self):
        run = {
            "id": monitor.SOURCE_RUN_ID,
            "run_attempt": monitor.SOURCE_ATTEMPT,
            "head_sha": monitor.SOURCE_COMMIT,
            "head_branch": monitor.SOURCE_BRANCH,
            "event": "push",
            "status": "completed",
            "conclusion": "success",
            "path": monitor.SOURCE_WORKFLOW,
            "repository": {"full_name": monitor.REPOSITORY},
            "head_repository": {"full_name": monitor.REPOSITORY},
        }
        artifact = {
            "id": monitor.SOURCE_ARTIFACT_ID,
            "name": monitor.SOURCE_ARTIFACT_NAME,
            "size_in_bytes": monitor.SOURCE_ARTIFACT_SIZE,
            "digest": monitor.SOURCE_ARTIFACT_DIGEST,
            "expired": False,
            "archive_download_url": (
                "https://api.github.com/repos/8abak/"
                "ctrade-openapi-client/actions/artifacts/8586881858/zip"
            ),
            "workflow_run": {
                "id": monitor.SOURCE_RUN_ID,
                "head_sha": monitor.SOURCE_COMMIT,
                "head_branch": monitor.SOURCE_BRANCH,
            },
        }
        self.assertEqual(
            monitor.source_plan(
                run, {"total_count": 1, "artifacts": [artifact]}
            ),
            source_plan(),
        )
        changed = copy.deepcopy(artifact)
        changed["digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ValueError, "metadata changed"):
            monitor.source_plan(
                run, {"total_count": 1, "artifacts": [changed]}
            )

    def test_remote_argument_vector_is_derived_from_frozen_receipt(self):
        with (
            mock.patch.object(
                monitor,
                "read_json",
                return_value=launch_receipt(),
            ),
            mock.patch.object(
                monitor,
                "sha256_file",
                return_value=(monitor.SOURCE_RECEIPT_SHA256, 1146),
            ),
        ):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                monitor.command_remote_args(
                    argparse.Namespace(receipt="unused")
                )
        values = output.getvalue().splitlines()
        self.assertEqual(len(values), 18)
        self.assertEqual(values[0:2], ["30067832187", "1"])
        self.assertEqual(values[7:11], [
            "486270",
            "1069712502",
            "486543",
            "1069715129",
        ])
        self.assertEqual(
            values[11],
            "/tmp/fresh-xauusd-worktree.E4Jrbc",
        )


class SnapshotSchemaTests(unittest.TestCase):
    def test_running_metadata_snapshot_passes(self):
        snapshot = runtime_snapshot()
        self.assertIs(
            monitor.validate_remote_snapshot(
                snapshot,
                launch_receipt(),
                monitor.SOURCE_RECEIPT_SHA256,
            ),
            snapshot,
        )

    def test_snapshot_cannot_smuggle_extra_or_outcome_content(self):
        snapshot = runtime_snapshot()
        snapshot["scientificResult"] = {"pnl": 1}
        with self.assertRaisesRegex(ValueError, "keys changed"):
            monitor.validate_remote_snapshot(
                snapshot,
                launch_receipt(),
                monitor.SOURCE_RECEIPT_SHA256,
            )
        snapshot = runtime_snapshot()
        snapshot["outcomeFilesOpened"] = True
        with self.assertRaisesRegex(ValueError, "scope or source"):
            monitor.validate_remote_snapshot(
                snapshot,
                launch_receipt(),
                monitor.SOURCE_RECEIPT_SHA256,
            )

    def test_terminal_transition_requires_terminal_archive_metadata(self):
        snapshot = runtime_snapshot()
        snapshot["initialLifecycle"] = "finalizing_metadata_only"
        snapshot["lifecycle"] = "terminal_metadata_present"
        snapshot["processes"]["pipeline"] = None
        snapshot["processes"]["pipelineDelta"] = None
        snapshot["filesystem"]["objects"]["terminalReceipt"] = (
            filesystem_object("regular")
        )
        with self.assertRaisesRegex(ValueError, "terminal filesystem"):
            monitor.validate_remote_snapshot(
                snapshot,
                launch_receipt(),
                monitor.SOURCE_RECEIPT_SHA256,
            )
        snapshot["filesystem"]["objects"]["terminalArchive"] = (
            filesystem_object("regular")
        )
        monitor.validate_remote_snapshot(
            snapshot,
            launch_receipt(),
            monitor.SOURCE_RECEIPT_SHA256,
        )


class StaticReadOnlyBoundaryTests(unittest.TestCase):
    def test_remote_program_has_no_filesystem_mutation_calls(self):
        tree = ast.parse(REMOTE_PATH.read_text(encoding="utf-8"))
        forbidden_methods = {
            "chmod",
            "link",
            "mkdir",
            "remove",
            "rename",
            "replace",
            "rmdir",
            "symlink",
            "unlink",
            "write_bytes",
            "write_text",
        }
        observed = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in forbidden_methods
        }
        self.assertEqual(observed, set())
        source = REMOTE_PATH.read_text(encoding="utf-8")
        for forbidden in (
            "fresh_experiment_ledger",
            "fresh_holdout",
            "fresh_research_summary",
            "fresh_selected_candidate",
            "fresh_final_strategy",
            "server-run.log",
        ):
            self.assertNotIn(forbidden, source)

    def test_workflow_is_marker_triggered_and_host_pinned(self):
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "paths:\n      - .github/research-v5-monitor.txt",
            workflow,
        )
        self.assertNotIn("workflow_dispatch", workflow)
        self.assertIn("fetch-depth: 0", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn("StrictHostKeyChecking=yes", workflow)
        self.assertIn("HostKeyAlgorithms=ssh-ed25519", workflow)
        self.assertIn("IdentitiesOnly=yes", workflow)
        self.assertIn("8586881858/zip", workflow)
        self.assertNotIn("StrictHostKeyChecking=accept-new", workflow)
        self.assertNotIn("ssh-keyscan", workflow)
        self.assertNotIn("\n          scp ", workflow)
        self.assertNotIn("\n          rsync ", workflow)
        self.assertEqual(
            MARKER_PATH.read_bytes(),
            (
                b"fresh-xauusd-v5-read-only-monitor-source-"
                b"30067832187-attempt-1-r3\n"
            ),
        )


if __name__ == "__main__":
    unittest.main()
