from __future__ import annotations

import copy
from contextlib import contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import stat
import unittest
import uuid
import warnings
import zipfile


ROOT = Path(__file__).resolve().parent
HELPER_PATH = (
    ROOT
    / ".github"
    / "scripts"
    / "fresh-xauusd-v5-terminal-audit-input.py"
)
WORKFLOW_PATH = (
    ROOT / ".github" / "workflows" / "fresh-xauusd-v5-terminal-audit.yml"
)
MARKER_PATH = ROOT / ".github" / "research-v5-terminal-audit.txt"
SPEC = importlib.util.spec_from_file_location("fresh_v5_audit_input", HELPER_PATH)
assert SPEC is not None and SPEC.loader is not None
audit_input = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit_input)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


@contextmanager
def _temporary_root():
    path = ROOT / f"fresh-v5-audit-workflow-test-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class ExternalMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.run_id = 30070000001
        self.run_attempt = 2
        self.commit = "a" * 40
        self.job_id = 89400000001
        self.artifact_id = 8587000001
        self.artifact_name = (
            f"fresh-xauusd-v5-terminal-adopted-"
            f"{self.run_id}-{self.run_attempt}"
        )
        self.artifact_size = 12345
        self.artifact_digest = f"sha256:{'b' * 64}"
        producer = {
            "id": self.run_id,
            "head_sha": self.commit,
            "head_branch": audit_input.BRANCH,
        }
        self.artifact = {
            "id": self.artifact_id,
            "name": self.artifact_name,
            "size_in_bytes": self.artifact_size,
            "digest": self.artifact_digest,
            "expired": False,
            "archive_download_url": (
                f"https://api.github.com/repos/{audit_input.REPOSITORY}/"
                f"actions/artifacts/{self.artifact_id}/zip"
            ),
            "workflow_run": producer,
        }
        self.run = {
            "id": self.run_id,
            "run_attempt": self.run_attempt,
            "head_sha": self.commit,
            "head_branch": audit_input.BRANCH,
            "event": "push",
            "status": "completed",
            "conclusion": "success",
            "path": audit_input.ADOPTION_WORKFLOW,
            "repository": {"full_name": audit_input.REPOSITORY},
            "head_repository": {"full_name": audit_input.REPOSITORY},
        }
        self.listing = {
            "total_count": 1,
            "artifacts": [copy.deepcopy(self.artifact)],
        }
        self.jobs = {
            "total_count": 1,
            "jobs": [
                {
                    "id": self.job_id,
                    "run_id": self.run_id,
                    "run_attempt": self.run_attempt,
                    "head_sha": self.commit,
                    "head_branch": audit_input.BRANCH,
                    "workflow_name": audit_input.ADOPTION_WORKFLOW_NAME,
                    "name": audit_input.ADOPTION_JOB_NAME,
                    "status": "completed",
                    "conclusion": "success",
                    "steps": [
                        {
                            "name": audit_input.ADOPTION_UPLOAD_STEP,
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ],
                }
            ],
        }

    def _validate(
        self,
        directory: Path,
        *,
        run: dict[str, object] | None = None,
        jobs: dict[str, object] | None = None,
    ) -> dict[str, object]:
        paths = {
            "artifact": directory / "artifact.json",
            "run": directory / "run.json",
            "listing": directory / "listing.json",
            "jobs": directory / "jobs.json",
        }
        _write_json(paths["artifact"], self.artifact)
        _write_json(paths["run"], self.run if run is None else run)
        _write_json(paths["listing"], self.listing)
        _write_json(paths["jobs"], self.jobs if jobs is None else jobs)
        return audit_input.validate_external_metadata(
            paths["artifact"],
            paths["run"],
            paths["listing"],
            paths["jobs"],
            artifact_id=self.artifact_id,
            artifact_name=self.artifact_name,
            artifact_size=self.artifact_size,
            artifact_digest=self.artifact_digest,
            run_id=self.run_id,
            run_attempt=self.run_attempt,
            commit=self.commit,
            job_id=self.job_id,
        )

    def test_exact_attempt_artifact_and_job_are_normalized(self) -> None:
        with _temporary_root() as temporary:
            payload = self._validate(temporary)
        self.assertEqual(payload["run"]["attempt"], self.run_attempt)
        self.assertEqual(payload["job"]["id"], self.job_id)
        self.assertEqual(payload["artifact"]["digest"], self.artifact_digest)

    def test_type_confused_attempt_is_rejected(self) -> None:
        bad_run = copy.deepcopy(self.run)
        bad_run["run_attempt"] = True
        with _temporary_root() as temporary:
            with self.assertRaises(audit_input.AuditInputError):
                self._validate(temporary, run=bad_run)

    def test_wrong_attempt_job_or_failed_upload_is_rejected(self) -> None:
        bad_jobs = copy.deepcopy(self.jobs)
        bad_jobs["jobs"][0]["run_attempt"] = self.run_attempt + 1
        with _temporary_root() as temporary:
            with self.assertRaises(audit_input.AuditInputError):
                self._validate(temporary, jobs=bad_jobs)
        bad_jobs = copy.deepcopy(self.jobs)
        bad_jobs["jobs"][0]["steps"][0]["conclusion"] = "failure"
        with _temporary_root() as temporary:
            with self.assertRaises(audit_input.AuditInputError):
                self._validate(temporary, jobs=bad_jobs)


class SafeZipTests(unittest.TestCase):
    def _member_payloads(
        self,
        *,
        adoption_run: int = 30070000001,
        adoption_attempt: int = 2,
        adoption_commit: str = "a" * 40,
    ) -> dict[str, bytes]:
        first_three = {
            audit_input.LAUNCH_RECEIPT_NAME: b'{"fixture":"launch"}\n',
            audit_input.TERMINAL_RECEIPT_NAME: b'{"fixture":"terminal"}\n',
            audit_input.TERMINAL_ARCHIVE_NAME: b"fixture-tar-gzip-bytes",
        }
        members = []
        for name in audit_input.MANIFEST_MEMBER_NAMES:
            raw = first_three[name]
            members.append(
                {
                    "name": name,
                    "size": len(raw),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            )
        manifest = {
            "schema": "fresh-xauusd-v5-terminal-adoption/v1",
            "source": {
                "githubRunId": audit_input.LAUNCH_RUN_ID,
                "githubRunAttempt": audit_input.LAUNCH_RUN_ATTEMPT,
                "commitSha": audit_input.LAUNCH_COMMIT,
                "launchArtifactId": audit_input.LAUNCH_ARTIFACT_ID,
                "launchArtifactDigest": audit_input.LAUNCH_ARTIFACT_DIGEST,
                "launchArtifactSize": audit_input.LAUNCH_ARTIFACT_SIZE,
            },
            "adoption": {
                "githubRunId": adoption_run,
                "githubRunAttempt": adoption_attempt,
                "commitSha": adoption_commit,
                "remoteMutation": False,
            },
            "members": members,
        }
        first_three[audit_input.ADOPTION_MANIFEST_NAME] = (
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        return first_three

    def _write_zip(
        self,
        path: Path,
        payloads: dict[str, bytes],
        *,
        symlink_name: str | None = None,
        duplicate_name: str | None = None,
        extra_name: str | None = None,
    ) -> None:
        with zipfile.ZipFile(
            path,
            "w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            for name, raw in payloads.items():
                if name == symlink_name:
                    info = zipfile.ZipInfo(name)
                    info.create_system = 3
                    info.external_attr = (stat.S_IFLNK | 0o777) << 16
                    archive.writestr(info, raw)
                else:
                    archive.writestr(name, raw)
            if duplicate_name is not None:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    archive.writestr(duplicate_name, b"duplicate")
            if extra_name is not None:
                archive.writestr(extra_name, b"unexpected")

    def _external_for_zip(
        self,
        zip_path: Path,
        *,
        run_id: int = 30070000001,
        run_attempt: int = 2,
        commit: str = "a" * 40,
    ) -> dict[str, object]:
        raw = zip_path.read_bytes()
        return {
            "schema": "fresh-xauusd-v5-external-adoption/v1",
            "run": {
                "id": run_id,
                "attempt": run_attempt,
                "commitSha": commit,
            },
            "artifact": {
                "size": len(raw),
                "digest": f"sha256:{hashlib.sha256(raw).hexdigest()}",
            },
        }

    def test_exact_four_file_bundle_is_extracted_and_bound(self) -> None:
        with _temporary_root() as root:
            archive = root / "artifact.zip"
            self._write_zip(archive, self._member_payloads())
            external = root / "external.json"
            _write_json(external, self._external_for_zip(archive))
            destination = root / "bundle"
            manifest = audit_input.extract_exact_bundle(
                archive,
                destination,
                external,
            )
            self.assertEqual(
                {entry["name"] for entry in manifest["members"]},
                set(audit_input.EXPECTED_NAMES),
            )
            self.assertEqual(
                {entry.name for entry in destination.iterdir()},
                set(audit_input.EXPECTED_NAMES),
            )

    def test_duplicate_unexpected_and_symlink_entries_are_rejected(self) -> None:
        cases = (
            {"duplicate_name": audit_input.LAUNCH_RECEIPT_NAME},
            {"extra_name": "../outside"},
            {"symlink_name": audit_input.TERMINAL_ARCHIVE_NAME},
        )
        for case in cases:
            with self.subTest(case=case):
                with _temporary_root() as root:
                    archive = root / "artifact.zip"
                    self._write_zip(
                        archive,
                        self._member_payloads(),
                        **case,
                    )
                    external = root / "external.json"
                    _write_json(external, self._external_for_zip(archive))
                    with self.assertRaises(audit_input.AuditInputError):
                        audit_input.extract_exact_bundle(
                            archive,
                            root / "bundle",
                            external,
                        )

    def test_oversized_small_member_and_manifest_run_mismatch_are_rejected(
        self,
    ) -> None:
        with _temporary_root() as root:
            payloads = self._member_payloads()
            payloads[audit_input.LAUNCH_RECEIPT_NAME] = (
                b"x" * (audit_input.MAX_SMALL_MEMBER_BYTES + 1)
            )
            archive = root / "oversized.zip"
            self._write_zip(archive, payloads)
            external = root / "external.json"
            _write_json(external, self._external_for_zip(archive))
            with self.assertRaises(audit_input.AuditInputError):
                audit_input.extract_exact_bundle(
                    archive,
                    root / "bundle",
                    external,
                )
        with _temporary_root() as root:
            archive = root / "mismatch.zip"
            self._write_zip(
                archive,
                self._member_payloads(adoption_run=30070000002),
            )
            external = root / "external.json"
            _write_json(external, self._external_for_zip(archive))
            with self.assertRaises(audit_input.AuditInputError):
                audit_input.extract_exact_bundle(
                    archive,
                    root / "bundle",
                    external,
                )


class ReceiptAndWorkflowTests(unittest.TestCase):
    def test_provenance_hashes_raw_json_stdout_without_rewriting_it(self) -> None:
        with _temporary_root() as root:
            receipt = root / "receipt.json"
            receipt.write_bytes(b'{"schema":"fixture-audit"}\n')
            external = root / "external.json"
            bundle = root / "bundle.json"
            digest = "c" * 64
            _write_json(
                external,
                {
                    "artifact": {
                        "size": 123,
                        "digest": f"sha256:{digest}",
                    }
                },
            )
            _write_json(
                bundle,
                {
                    "zip": {"size": 123, "sha256": digest},
                },
            )
            output = root / "provenance.json"
            payload = audit_input.build_provenance(
                receipt,
                external,
                bundle,
                output,
                audit_run_id=42,
                audit_run_attempt=1,
                audit_commit="d" * 40,
                audit_ref=f"refs/heads/{audit_input.BRANCH}",
                runtime={
                    "implementation": "CPython",
                    "version": "3.11.fixture",
                    "optimize": 0,
                    "numpy": audit_input.NUMPY_VERSION,
                    "pandas": audit_input.PANDAS_VERSION,
                },
            )
            self.assertEqual(
                payload["auditStdout"]["sha256"],
                hashlib.sha256(receipt.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8"))["schema"],
                "fresh-xauusd-v5-terminal-audit-provenance/v1",
            )

    def test_workflow_trigger_phase_and_seals_are_consistent(self) -> None:
        source = WORKFLOW_PATH.read_text(encoding="utf-8")
        armed = 'AUDIT_ARMED: "true"' in source
        if armed:
            self.assertIsNone(
                re.search(
                    r'^\s+[A-Z0-9_]+:\s*"__FILL_',
                    source,
                    flags=re.MULTILINE,
                )
            )
            values = {}
            for name in (
                "ADOPTION_RUN_ID",
                "ADOPTION_RUN_ATTEMPT",
                "ADOPTION_ARTIFACT_ID",
            ):
                match = re.search(
                    rf'^\s+{name}: "?([1-9][0-9]*)"?$',
                    source,
                    flags=re.MULTILINE,
                )
                self.assertIsNotNone(match)
                assert match is not None
                values[name] = match.group(1)
            expected_marker = (
                "fresh-xauusd-v5-terminal-audit-"
                f"{values['ADOPTION_RUN_ID']}-"
                f"{values['ADOPTION_RUN_ATTEMPT']}-"
                f"{values['ADOPTION_ARTIFACT_ID']}-r1\n"
            )
            if MARKER_PATH.exists():
                self.assertEqual(
                    MARKER_PATH.read_text(encoding="utf-8"),
                    expected_marker,
                )
        else:
            self.assertFalse(MARKER_PATH.exists())
            self.assertIn(
                'AUDIT_ARMED: "__FILL_WITH_LITERAL_true',
                source,
            )
            self.assertIn("ADOPTION_ARTIFACT_ID: \"__FILL_", source)
            self.assertIn("ADOPTION_WORKFLOW_BLOB: \"__FILL_", source)
        self.assertIn(audit_input.SEALED_COMMIT, source)
        self.assertIn(audit_input.BOOTSTRAP_SHA256, source)
        self.assertIn(
            "actions/runs/${ADOPTION_RUN_ID}/attempts/"
            "${ADOPTION_RUN_ATTEMPT}/jobs",
            source,
        )
        self.assertIn(
            "fresh-v5-external-adoption-before.json",
            source,
        )
        self.assertIn(
            "fresh-v5-external-adoption-after.json",
            source,
        )
        self.assertIn('rev-parse "HEAD:${path}"', source)
        self.assertIn(
            'python -I -B "${bootstrap}" "${bundle}" > "${receipt}"',
            source,
        )
        self.assertNotIn("workflow_dispatch:", source)
        self.assertNotIn("pull_request:", source)


if __name__ == "__main__":
    unittest.main()
