from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import unittest
from unittest.mock import patch
import uuid


ROOT = Path(__file__).resolve().parent
CONTROLLER_PATH = (
    ROOT / ".github/scripts/fresh-xauusd-v5-recovery-controller.py"
)
WORKFLOW_PATH = (
    ROOT / ".github/workflows/fresh-xauusd-v5-recovery-detached-launch.yml"
)
MARKER_PATH = ROOT / ".github/research-v5-recovery-launch.txt"


def _load_controller():
    specification = importlib.util.spec_from_file_location(
        "fresh_xauusd_v5_recovery_controller",
        CONTROLLER_PATH,
    )
    if specification is None or specification.loader is None:
        raise AssertionError("recovery controller cannot be loaded")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


controller = _load_controller()


class FreshV5RecoveryOrchestrationTests(unittest.TestCase):
    def _directory(self) -> Path:
        directory = ROOT / (
            f".fresh-v5-recovery-orchestration-test-{uuid.uuid4().hex}"
        )
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory, True)
        return directory

    def test_external_artifact_and_nested_archive_are_exactly_pinned(self) -> None:
        self.assertEqual(controller.ADOPTION_RUN_ID, 30_101_048_443)
        self.assertEqual(controller.ADOPTION_RUN_ATTEMPT, 1)
        self.assertEqual(controller.ADOPTION_JOB_ID, 89_506_876_763)
        self.assertEqual(controller.ADOPTION_ARTIFACT_ID, 8_608_015_979)
        self.assertEqual(controller.ADOPTION_ARTIFACT_SIZE, 127_602)
        self.assertEqual(
            controller.ADOPTION_ARTIFACT_DIGEST,
            "sha256:"
            "6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3",
        )
        self.assertEqual(controller.TERMINAL_ARCHIVE_SIZE, 125_470)
        self.assertEqual(
            controller.TERMINAL_ARCHIVE_SHA256,
            "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d",
        )
        self.assertEqual(len(controller.RECOVERY_MEMBERS), 16)

    def test_pipeline_invocation_is_resume_only_and_uses_separate_scratch(self) -> None:
        self.assertEqual(
            controller.MINIMUM_SCRATCH_BYTES,
            16 * 1024 * 1024 * 1024,
        )
        command = controller.pipeline_command(
            Path("/work/.venv/bin/python"),
            repository=Path("/work"),
            output=Path("/output"),
            scratch=Path("/durable/recovery-scratch/attempt.1"),
            state=Path("/durable/state"),
            recovery=Path("/tmp/recovery"),
        )
        normalized = tuple(value.replace("\\", "/") for value in command)
        self.assertEqual(
            normalized,
            (
                "/work/.venv/bin/python",
                "-m",
                "datavis.research.fresh_pipeline_cli",
                "--repository-root",
                "/work",
                "--output-dir",
                "/output",
                "--scratch-dir",
                "/durable/recovery-scratch/attempt.1",
                "--research-state-dir",
                "/durable/state",
                "--resume-v5-artifact-dir",
                "/tmp/recovery",
                "--execute",
            ),
        )
        self.assertNotIn("--restart-v5-artifact-dir", command)

    @unittest.skipIf(os.name == "nt", "claim uses POSIX directory descriptors")
    def test_recovery_claim_is_distinct_durable_and_one_shot(self) -> None:
        claim = self._directory() / "sole.claim"
        controller.write_execution_claim(
            claim,
            run_id=123,
            run_attempt=1,
            commit_sha="a" * 40,
        )
        payload = json.loads(claim.read_text(encoding="utf-8"))
        self.assertEqual(
            payload["schema"],
            "fresh-xauusd-v5-recovery-execution-claim/v1",
        )
        self.assertEqual(
            payload["recoveryAttemptId"],
            "v5-discovery-recovery-attempt-1",
        )
        self.assertEqual(
            payload["sourceAdoption"]["artifactId"],
            8_608_015_979,
        )
        with self.assertRaises(FileExistsError):
            controller.write_execution_claim(
                claim,
                run_id=124,
                run_attempt=1,
                commit_sha="b" * 40,
            )

    def test_safe_extractor_rejects_non_allowlisted_member(self) -> None:
        root = self._directory()
        archive = root / controller.TERMINAL_ARCHIVE_NAME
        with tarfile.open(archive, "w:gz") as bundle:
            directory = tarfile.TarInfo(".")
            directory.type = tarfile.DIRTYPE
            bundle.addfile(directory)
            payload = b"forbidden\n"
            member = tarfile.TarInfo("unexpected.bin")
            member.size = len(payload)
            bundle.addfile(member, io.BytesIO(payload))
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        destination = root / "out"
        destination.mkdir()
        with (
            patch.object(
                controller,
                "TERMINAL_ARCHIVE_SIZE",
                archive.stat().st_size,
            ),
            patch.object(
                controller,
                "TERMINAL_ARCHIVE_SHA256",
                digest,
            ),
        ):
            with self.assertRaisesRegex(
                controller.RecoveryControllerError,
                "unsafe terminal archive member",
            ):
                controller.safe_extract_recovery_archive(
                    archive,
                    destination,
                )

    def test_nonzero_finalizer_contract_uses_actual_status_and_exact_api(self) -> None:
        self.assertEqual(controller._portable_exit_status(-9), 137)
        self.assertEqual(controller._portable_exit_status(2), 2)
        completed = subprocess.CompletedProcess(args=(), returncode=0)
        with patch.object(
            controller.subprocess,
            "run",
            return_value=completed,
        ) as invoked:
            root = self._directory()
            log = root / "server.log"
            controller.invoke_failure_finalizer(
                Path("/work/.venv/bin/python"),
                Path("/durable/fresh_experiment_ledger_v1.jsonl"),
                exit_status=137,
                cwd=Path("/work"),
                environment={"A": "B"},
                log_path=log,
            )
            command = invoked.call_args.args[0]
            self.assertIn(
                "finalize_interrupted_fresh_v5_recovery",
                command[3],
            )
            self.assertEqual(
                [value.replace("\\", "/") for value in command[-2:]],
                ["/durable/fresh_experiment_ledger_v1.jsonl", "137"],
            )
            self.assertNotIn("--restart-v5-artifact-dir", command)

    def test_existing_snapshot_must_match_durable_source(self) -> None:
        root = self._directory()
        source = root / "source.jsonl"
        destination = root / "destination.jsonl"
        source.write_bytes(b"exact\n")
        destination.write_bytes(b"different\n")
        with self.assertRaisesRegex(
            controller.RecoveryControllerError,
            "differs from durable source",
        ):
            controller._copy_new(source, destination)
        self.assertEqual(destination.read_bytes(), b"different\n")
        destination.write_bytes(source.read_bytes())
        controller._copy_new(source, destination)

    def test_required_state_directory_is_never_created(self) -> None:
        missing = self._directory() / "missing-state"
        with self.assertRaises(FileNotFoundError):
            controller._existing_canonical_directory(missing)
        self.assertFalse(missing.exists())

    def test_workflow_pins_outer_provenance_and_nested_member_hashes(self) -> None:
        source = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertEqual(
            MARKER_PATH.read_text(encoding="utf-8"),
            "fresh-xauusd-v5-discovery-recovery-attempt-1-transport-2\n",
        )
        self.assertIn(
            '"fresh-xauusd-v5-discovery-recovery-attempt-1-transport-2")',
            source,
        )
        self.assertIn(
            ') | (\n'
            '            cd "${bundle}"\n'
            "            sha256sum --check --strict -\n"
            "          )",
            source,
        )
        self.assertNotIn(
            ") | sha256sum --check --strict -",
            source,
        )
        required = (
            'ADOPTION_RUN_ID: "30101048443"',
            'ADOPTION_ARTIFACT_ID: "8608015979"',
            "sha256:6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3",
            "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d",
            "7850a5ef41660c9192f4d5d57aedba1264101ebf4f6b93e3e0b043739092eee3",
            "c6e32cbbdaaa2b9d343eee2a2fc399804976a6493ad4f99776b5ad795c5c54a4",
            "0b82d250c2c62115223d4d7696695fed337bbc64b3fcb1950cb04a3504d92531",
            "fresh-xauusd-v5-recovery-controller.py",
            "fresh-xauusd-v5-recovery-scratch-v1",
            "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        )
        for value in required:
            with self.subTest(value=value):
                self.assertIn(value, source)
        self.assertNotIn("--restart-v5-artifact-dir", source)
        controller_source = CONTROLLER_PATH.read_text(encoding="utf-8")
        self.assertIn('"rev-parse",\n                    "FETCH_HEAD"', controller_source)
        self.assertIn("if fetched != args.commit_sha", controller_source)
        self.assertNotIn(
            "state_root = _new_directory(DURABLE_STATE_ROOT)",
            controller_source,
        )


if __name__ == "__main__":
    unittest.main()
