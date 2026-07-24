"""Static and receipt-binding tests for detached v5 terminal adoption."""

from __future__ import annotations

import copy
import hashlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent
READER = ROOT / ".github/scripts/fresh-xauusd-v5-terminal-reader.py"
WORKFLOW = ROOT / ".github/workflows/fresh-xauusd-v5-terminal-adoption.yml"
MARKER = ROOT / ".github/research-v5-adoption-launch.txt"
KNOWN_HOSTS = ROOT / ".github/ssh/fresh-xauusd-ec2-known-hosts"


def _load_reader():
    specification = spec_from_file_location("fresh_v5_terminal_reader", READER)
    if specification is None or specification.loader is None:
        raise RuntimeError("could not load terminal adoption reader")
    module = module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


reader = _load_reader()


def _launch_receipt() -> dict:
    run_id = reader.RUN_ID
    attempt = reader.RUN_ATTEMPT
    return {
        "schema": reader.RECEIPT_SCHEMA,
        "kind": "launch_ready",
        "status": "running",
        "processExitStatus": None,
        "githubRunId": run_id,
        "githubRunAttempt": attempt,
        "branch": reader.RUN_BRANCH,
        "commitSha": reader.RUN_COMMIT,
        "studyLineageSha256": reader.LINEAGE_SHA256,
        "run19ArtifactId": reader.RUN19_ARTIFACT_ID,
        "run19TerminalArchiveSha256": reader.RUN19_ARCHIVE_SHA256,
        "controllerSha256": reader.CONTROLLER_SHA256,
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


class FreshV5TerminalAdoptionTests(unittest.TestCase):
    def test_launch_and_terminal_receipts_are_exactly_bound(self) -> None:
        launch = _launch_receipt()
        raw = reader._canonical_json_bytes(launch)
        self.assertEqual(len(raw), reader.LAUNCH_RECEIPT_SIZE)
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            reader.LAUNCH_RECEIPT_SHA256,
        )
        paths = reader.validate_launch_receipt(launch)
        self.assertEqual(
            paths["terminalArchive"].name,
            f"fresh-xauusd-{reader.RUN_ID}-{reader.RUN_ATTEMPT}.tgz",
        )
        terminal = copy.deepcopy(launch)
        terminal.update(
            {
                "kind": "terminal",
                "status": "succeeded",
                "processExitStatus": 0,
                "terminalArchive": {
                    "size": 123456,
                    "sha256": "a" * 64,
                    "device": 2049,
                    "inode": 987654,
                },
            }
        )
        archive = reader.validate_terminal_receipt(launch, terminal)
        self.assertEqual(archive["sha256"], "a" * 64)
        controller_arguments, pipeline_arguments = (
            reader._expected_process_arguments(launch, paths)
        )
        self.assertEqual(
            controller_arguments[3],
            "/tmp/fresh-xauusd-transfer.SEMXw4/"
            "fresh-xauusd-v5-controller.sh",
        )
        self.assertEqual(
            pipeline_arguments[0],
            "/tmp/fresh-xauusd-worktree.E4Jrbc/.fresh-venv/bin/python",
        )

        changed = copy.deepcopy(terminal)
        changed["commitSha"] = "0" * 40
        with self.assertRaises(reader.AdoptionError):
            reader.validate_terminal_receipt(launch, changed)
        changed = copy.deepcopy(terminal)
        changed["githubRunAttempt"] = True
        with self.assertRaises(reader.AdoptionError):
            reader.validate_terminal_receipt(launch, changed)

    def test_launch_rejects_boolean_attempt_and_changed_controller(self) -> None:
        launch = _launch_receipt()
        launch["githubRunAttempt"] = True
        with self.assertRaises(reader.AdoptionError):
            reader.validate_launch_receipt(launch)
        launch = _launch_receipt()
        launch["controllerSha256"] = "0" * 64
        with self.assertRaises(reader.AdoptionError):
            reader.validate_launch_receipt(launch)

    def test_workflow_is_frozen_and_remote_commands_are_read_only(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(hashlib.sha256(READER.read_bytes()).hexdigest(), workflow)
        self.assertIn(
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            workflow,
        )
        self.assertIn(".github/research-v5-adoption-launch.txt", workflow)
        self.assertNotIn("workflow_dispatch:", workflow)
        self.assertIn('WAIT_MINUTES: "350"', workflow)
        self.assertIn("timeout-minutes: 360", workflow)
        self.assertIn(reader.RUN_COMMIT, workflow)
        self.assertIn(
            'run.get("path")\n'
            '              != ".github/workflows/'
            'fresh-xauusd-v5-detached-launch.yml"',
            workflow,
        )
        self.assertIn("8abak/ctrade-openapi-client", workflow)
        self.assertIn(
            "/actions/runs/${V5_LAUNCH_RUN_ID}/artifacts?"
            "name=${V5_LAUNCH_ARTIFACT_NAME}&per_page=100",
            workflow,
        )
        self.assertIn(str(reader.LAUNCH_ARTIFACT_ID), workflow)
        self.assertIn(reader.LAUNCH_ARTIFACT_DIGEST, workflow)
        self.assertIn(reader.LAUNCH_RECEIPT_SHA256, workflow)
        self.assertIn(f'"{reader.LAUNCH_RECEIPT_SIZE}"', workflow)
        self.assertIn(reader.CONTROLLER_SHA256, workflow)
        self.assertIn("StrictHostKeyChecking=yes", workflow)
        self.assertNotIn("StrictHostKeyChecking=accept-new", workflow)
        self.assertIn('if [[ "${EC2_USER}" != "ec2-user" ]]', workflow)
        self.assertIn("-o IdentitiesOnly=yes", workflow)
        self.assertIn("-o HostKeyAlgorithms=ssh-ed25519", workflow)
        self.assertIn(
            "1ae003bab02effdbae4e43d5c2d8b566b65464b380fd31a4fad8e457b37770c8",
            workflow,
        )
        self.assertNotIn("scp ", workflow)
        self.assertEqual(workflow.count("python3 -B - probe"), 1)
        self.assertEqual(workflow.count("python3 -B - stream"), 1)
        self.assertNotIn("__REPLACE_", workflow)

        remote_reader = READER.read_text(encoding="utf-8")
        remote_section = remote_reader[
            remote_reader.index("def _require_remote_read_flags")
            : remote_reader.index("def verify_local_archive")
        ]
        self.assertIn("os.O_NOFOLLOW", remote_section)
        self.assertIn("os.O_NOATIME", remote_section)
        self.assertIn("os.O_PATH", remote_section)
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
            self.assertNotIn(mutation, remote_section)

    def test_marker_and_host_key_are_exact(self) -> None:
        self.assertEqual(
            MARKER.read_text(encoding="utf-8"),
            "fresh-xauusd-v5-terminal-adoption-30067832187-1-r2\n",
        )
        self.assertEqual(
            KNOWN_HOSTS.read_text(encoding="utf-8"),
            (
                "datavis.au,www.datavis.au,3.27.110.195,"
                "ec2-3-27-110-195.ap-southeast-2.compute.amazonaws.com "
                "ssh-ed25519 "
                "AAAAC3NzaC1lZDI1NTE5AAAAIJaLbSMHMPiqscPzaqqsOoa41AKxQseBtEWVngSLj6nk\n"
            ),
        )


if __name__ == "__main__":
    unittest.main()
