#!/usr/bin/env python3
"""Verify one externally adopted v5 bundle before sealed terminal audit.

This helper deliberately does not interpret any research outcome.  It binds an
artifact to one pinned GitHub Actions run/attempt/job, rechecks the immutable
artifact identity, and extracts a four-file allowlisted ZIP without using
``ZipFile.extract``.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import stat
import sys
from typing import Any, BinaryIO
import zipfile


REPOSITORY = "8abak/ctrade-openapi-client"
BRANCH = "codex/xauusd-fresh-walkforward"
ADOPTION_WORKFLOW = ".github/workflows/fresh-xauusd-v5-terminal-adoption.yml"
ADOPTION_WORKFLOW_NAME = "Fresh XAUUSD v5 terminal adoption"
ADOPTION_JOB_NAME = (
    "Retrieve one terminal archive without changing preserved remote state"
)
ADOPTION_UPLOAD_STEP = "Upload one immutable terminal adoption artifact"
LAUNCH_RUN_ID = 30067832187
LAUNCH_RUN_ATTEMPT = 1
LAUNCH_COMMIT = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
LAUNCH_ARTIFACT_ID = 8586881858
LAUNCH_ARTIFACT_SIZE = 801
LAUNCH_ARTIFACT_DIGEST = (
    "sha256:86f6a8b06e0fde6a5223099a1eda4a9ce6e2f6fdd6248dbef91bbc4395936e1e"
)
SEALED_COMMIT = "8b37ebee8c145080ea8dc7e27557a1aaa73300b5"
BOOTSTRAP_PATH = "datavis/research/fresh_terminal_audit_bootstrap.py"
BOOTSTRAP_SHA256 = (
    "bd08194866bc817555b7b25e9d30634c8d0912e4fff74f55e81be516e1a1fa0c"
)
PYTHON_SERIES = (3, 11)
NUMPY_VERSION = "2.0.2"
PANDAS_VERSION = "2.2.3"

LAUNCH_RECEIPT_NAME = "fresh-xauusd-v5-launch-receipt.json"
TERMINAL_RECEIPT_NAME = "fresh-xauusd-v5-terminal-receipt.json"
TERMINAL_ARCHIVE_NAME = (
    f"fresh-xauusd-{LAUNCH_RUN_ID}-{LAUNCH_RUN_ATTEMPT}.tgz"
)
ADOPTION_MANIFEST_NAME = "fresh-xauusd-v5-adoption-manifest.json"
EXPECTED_NAMES = (
    LAUNCH_RECEIPT_NAME,
    TERMINAL_RECEIPT_NAME,
    TERMINAL_ARCHIVE_NAME,
    ADOPTION_MANIFEST_NAME,
)
MANIFEST_MEMBER_NAMES = EXPECTED_NAMES[:3]

MAX_API_JSON_BYTES = 4 * 1024**2
MAX_SMALL_MEMBER_BYTES = 64 * 1024
MAX_TERMINAL_ARCHIVE_BYTES = 512 * 1024**2
MAX_ZIP_BYTES = MAX_TERMINAL_ARCHIVE_BYTES + 1024**2
MAX_EXPANDED_BYTES = MAX_TERMINAL_ARCHIVE_BYTES + 3 * MAX_SMALL_MEMBER_BYTES
MAX_COMPRESSION_RATIO = 200.0
MAX_AUDIT_RECEIPT_BYTES = 8 * 1024**2
CHUNK_BYTES = 1024 * 1024

SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}\Z")


class AuditInputError(RuntimeError):
    """Raised when an external audit input fails closed."""


def _reject_json_constant(value: str) -> None:
    raise AuditInputError(f"non-finite JSON constant is forbidden: {value}")


def _object_without_duplicates(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AuditInputError(f"duplicate JSON key is forbidden: {key}")
        result[key] = value
    return result


def _read_regular_bytes(path: Path, limit: int, label: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise AuditInputError(f"{label} cannot be opened safely") from error
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size < 0
            or metadata.st_size > limit
        ):
            raise AuditInputError(f"{label} is not a bounded regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(limit + 1)
        if len(raw) != metadata.st_size or len(raw) > limit:
            raise AuditInputError(f"{label} changed or exceeded its bound")
        return raw
    finally:
        os.close(descriptor)


def _decode_json(path: Path, limit: int, label: str) -> Any:
    raw = _read_regular_bytes(path, limit, label)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise AuditInputError(f"{label} is not UTF-8") from error
    try:
        return json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, TypeError) as error:
        raise AuditInputError(f"{label} is not strict JSON") from error


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise AuditInputError(f"{label} is not an object")
    return value


def _require_list(value: Any, label: str) -> list[Any]:
    if type(value) is not list:
        raise AuditInputError(f"{label} is not an array")
    return value


def _exact_keys(
    value: dict[str, Any],
    expected: set[str],
    label: str,
) -> None:
    if set(value) != expected:
        raise AuditInputError(f"{label} keys changed")


def _exact_int(value: Any, expected: int, label: str) -> None:
    if type(value) is not int or value != expected:
        raise AuditInputError(f"{label} changed")


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise AuditInputError(f"{label} is not a positive integer")
    return value


def _exact_string(value: Any, expected: str, label: str) -> None:
    if type(value) is not str or value != expected:
        raise AuditInputError(f"{label} changed")


def _exact_false(value: Any, label: str) -> None:
    if value is not False:
        raise AuditInputError(f"{label} changed")


def _parse_positive_integer(value: str, label: str) -> int:
    if not value.isascii() or not value.isdecimal():
        raise AuditInputError(f"{label} is not a decimal integer")
    parsed = int(value)
    if parsed <= 0 or str(parsed) != value:
        raise AuditInputError(f"{label} is not a canonical positive integer")
    return parsed


def _parse_commit(value: str, label: str) -> str:
    if COMMIT_PATTERN.fullmatch(value) is None:
        raise AuditInputError(f"{label} is not a lowercase commit SHA")
    return value


def _parse_digest(value: str, label: str) -> str:
    prefix = "sha256:"
    if not value.startswith(prefix):
        raise AuditInputError(f"{label} is not a SHA-256 digest")
    digest = value[len(prefix) :]
    if SHA256_PATTERN.fullmatch(digest) is None:
        raise AuditInputError(f"{label} is not a lowercase SHA-256 digest")
    return value


def _artifact_identity(
    artifact: dict[str, Any],
    *,
    artifact_id: int,
    artifact_name: str,
    artifact_size: int,
    artifact_digest: str,
    run_id: int,
    commit: str,
) -> None:
    _exact_int(artifact.get("id"), artifact_id, "artifact id")
    _exact_string(artifact.get("name"), artifact_name, "artifact name")
    _exact_int(
        artifact.get("size_in_bytes"),
        artifact_size,
        "artifact size",
    )
    _exact_string(
        artifact.get("digest"),
        artifact_digest,
        "artifact digest",
    )
    _exact_false(artifact.get("expired"), "artifact expiry")
    expected_download = (
        f"https://api.github.com/repos/{REPOSITORY}/"
        f"actions/artifacts/{artifact_id}/zip"
    )
    _exact_string(
        artifact.get("archive_download_url"),
        expected_download,
        "artifact download URL",
    )
    producer = _require_mapping(
        artifact.get("workflow_run"),
        "artifact workflow_run",
    )
    _exact_int(producer.get("id"), run_id, "artifact producer run id")
    _exact_string(
        producer.get("head_sha"),
        commit,
        "artifact producer commit",
    )
    _exact_string(
        producer.get("head_branch"),
        BRANCH,
        "artifact producer branch",
    )


def _validate_run(
    run: dict[str, Any],
    *,
    run_id: int,
    run_attempt: int,
    commit: str,
) -> None:
    _exact_int(run.get("id"), run_id, "adoption run id")
    _exact_int(
        run.get("run_attempt"),
        run_attempt,
        "adoption run attempt",
    )
    _exact_string(run.get("head_sha"), commit, "adoption run commit")
    _exact_string(run.get("head_branch"), BRANCH, "adoption run branch")
    _exact_string(run.get("event"), "push", "adoption run event")
    _exact_string(run.get("status"), "completed", "adoption run status")
    _exact_string(
        run.get("conclusion"),
        "success",
        "adoption run conclusion",
    )
    _exact_string(run.get("path"), ADOPTION_WORKFLOW, "adoption workflow")
    repository = _require_mapping(
        run.get("repository"),
        "adoption run repository",
    )
    head_repository = _require_mapping(
        run.get("head_repository"),
        "adoption run head repository",
    )
    _exact_string(
        repository.get("full_name"),
        REPOSITORY,
        "adoption run repository name",
    )
    _exact_string(
        head_repository.get("full_name"),
        REPOSITORY,
        "adoption run head repository name",
    )


def _validate_job(
    jobs_payload: dict[str, Any],
    *,
    run_id: int,
    run_attempt: int,
    commit: str,
    job_id: int,
) -> None:
    jobs = _require_list(jobs_payload.get("jobs"), "adoption jobs")
    _exact_int(jobs_payload.get("total_count"), 1, "adoption job count")
    if len(jobs) != 1:
        raise AuditInputError("adoption job listing changed")
    job = _require_mapping(jobs[0], "adoption job")
    _exact_int(job.get("id"), job_id, "adoption job id")
    _exact_int(job.get("run_id"), run_id, "adoption job run id")
    _exact_int(
        job.get("run_attempt"),
        run_attempt,
        "adoption job run attempt",
    )
    _exact_string(job.get("head_sha"), commit, "adoption job commit")
    _exact_string(
        job.get("head_branch"),
        BRANCH,
        "adoption job branch",
    )
    _exact_string(
        job.get("workflow_name"),
        ADOPTION_WORKFLOW_NAME,
        "adoption workflow name",
    )
    _exact_string(job.get("name"), ADOPTION_JOB_NAME, "adoption job name")
    _exact_string(job.get("status"), "completed", "adoption job status")
    _exact_string(
        job.get("conclusion"),
        "success",
        "adoption job conclusion",
    )
    steps = _require_list(job.get("steps"), "adoption job steps")
    matches = [
        _require_mapping(step, "adoption job step")
        for step in steps
        if type(step) is dict and step.get("name") == ADOPTION_UPLOAD_STEP
    ]
    if len(matches) != 1:
        raise AuditInputError("adoption upload step is not unique")
    upload = matches[0]
    _exact_string(
        upload.get("status"),
        "completed",
        "adoption upload step status",
    )
    _exact_string(
        upload.get("conclusion"),
        "success",
        "adoption upload step conclusion",
    )


def validate_external_metadata(
    artifact_path: Path,
    run_path: Path,
    listing_path: Path,
    jobs_path: Path,
    *,
    artifact_id: int,
    artifact_name: str,
    artifact_size: int,
    artifact_digest: str,
    run_id: int,
    run_attempt: int,
    commit: str,
    job_id: int,
) -> dict[str, Any]:
    """Validate exact external GitHub provenance and return normalized facts."""
    if artifact_size > MAX_ZIP_BYTES:
        raise AuditInputError("artifact size exceeds the audit ZIP bound")
    artifact = _require_mapping(
        _decode_json(
            artifact_path,
            MAX_API_JSON_BYTES,
            "artifact API response",
        ),
        "artifact API response",
    )
    run = _require_mapping(
        _decode_json(run_path, MAX_API_JSON_BYTES, "run API response"),
        "run API response",
    )
    listing = _require_mapping(
        _decode_json(
            listing_path,
            MAX_API_JSON_BYTES,
            "artifact listing API response",
        ),
        "artifact listing API response",
    )
    jobs = _require_mapping(
        _decode_json(jobs_path, MAX_API_JSON_BYTES, "jobs API response"),
        "jobs API response",
    )
    _validate_run(
        run,
        run_id=run_id,
        run_attempt=run_attempt,
        commit=commit,
    )
    _artifact_identity(
        artifact,
        artifact_id=artifact_id,
        artifact_name=artifact_name,
        artifact_size=artifact_size,
        artifact_digest=artifact_digest,
        run_id=run_id,
        commit=commit,
    )
    listed = _require_list(listing.get("artifacts"), "artifact listing")
    _exact_int(listing.get("total_count"), 1, "artifact listing count")
    if len(listed) != 1:
        raise AuditInputError("artifact listing is not unique")
    _artifact_identity(
        _require_mapping(listed[0], "listed artifact"),
        artifact_id=artifact_id,
        artifact_name=artifact_name,
        artifact_size=artifact_size,
        artifact_digest=artifact_digest,
        run_id=run_id,
        commit=commit,
    )
    _validate_job(
        jobs,
        run_id=run_id,
        run_attempt=run_attempt,
        commit=commit,
        job_id=job_id,
    )
    return {
        "schema": "fresh-xauusd-v5-external-adoption/v1",
        "repository": REPOSITORY,
        "workflow": ADOPTION_WORKFLOW,
        "workflowName": ADOPTION_WORKFLOW_NAME,
        "event": "push",
        "branch": BRANCH,
        "run": {
            "id": run_id,
            "attempt": run_attempt,
            "commitSha": commit,
            "status": "completed",
            "conclusion": "success",
        },
        "job": {
            "id": job_id,
            "name": ADOPTION_JOB_NAME,
            "status": "completed",
            "conclusion": "success",
            "uploadStep": ADOPTION_UPLOAD_STEP,
        },
        "artifact": {
            "id": artifact_id,
            "name": artifact_name,
            "size": artifact_size,
            "digest": artifact_digest,
            "expired": False,
        },
    }


def _write_new_json(path: Path, payload: dict[str, Any]) -> None:
    raw = (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        raise AuditInputError(f"refusing to replace {path.name}") from error
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _sha256_regular(path: Path, limit: int, label: str) -> tuple[int, str]:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise AuditInputError(f"{label} cannot be opened safely") from error
    digest = hashlib.sha256()
    total = 0
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size < 0
            or metadata.st_size > limit
        ):
            raise AuditInputError(f"{label} is not a bounded regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
                total += len(chunk)
                if total > limit:
                    raise AuditInputError(f"{label} exceeded its bound")
                digest.update(chunk)
        if total != metadata.st_size:
            raise AuditInputError(f"{label} changed while being hashed")
    finally:
        os.close(descriptor)
    return total, digest.hexdigest()


def _member_limit(name: str) -> int:
    if name == TERMINAL_ARCHIVE_NAME:
        return MAX_TERMINAL_ARCHIVE_BYTES
    return MAX_SMALL_MEMBER_BYTES


def _validate_zip_member(info: zipfile.ZipInfo) -> None:
    if info.orig_filename != info.filename or info.filename not in EXPECTED_NAMES:
        raise AuditInputError("ZIP contains a non-allowlisted or ambiguous path")
    if (
        "/" in info.filename
        or "\\" in info.filename
        or info.is_dir()
        or info.file_size <= 0
    ):
        raise AuditInputError("ZIP member is not a nonempty flat file")
    if info.flag_bits & 0x1:
        raise AuditInputError("encrypted ZIP members are forbidden")
    if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
        raise AuditInputError("unsupported ZIP compression method")
    unix_mode = (info.external_attr >> 16) & 0xFFFF
    file_type = stat.S_IFMT(unix_mode)
    if file_type not in {0, stat.S_IFREG}:
        raise AuditInputError("ZIP member is not a regular file")
    if info.external_attr & 0x10:
        raise AuditInputError("ZIP member has a directory attribute")
    limit = _member_limit(info.filename)
    if info.file_size > limit:
        raise AuditInputError(f"{info.filename} exceeds its expansion bound")
    if info.compress_size <= 0:
        raise AuditInputError("ZIP member has an invalid compressed size")
    ratio = info.file_size / info.compress_size
    if not math.isfinite(ratio) or ratio > MAX_COMPRESSION_RATIO:
        raise AuditInputError("ZIP member exceeds the compression-ratio bound")


def _copy_zip_member(
    source: BinaryIO,
    destination: Path,
    expected_size: int,
) -> tuple[int, str]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(destination, flags, 0o600)
    except OSError as error:
        raise AuditInputError("refusing to replace an extracted member") from error
    digest = hashlib.sha256()
    total = 0
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            while True:
                chunk = source.read(CHUNK_BYTES)
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_size:
                    raise AuditInputError("ZIP member exceeded declared size")
                output.write(chunk)
                digest.update(chunk)
            output.flush()
            os.fsync(output.fileno())
    finally:
        os.close(descriptor)
    if total != expected_size:
        raise AuditInputError("ZIP member did not match declared size")
    metadata = destination.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise AuditInputError("extracted member is not a regular file")
    return total, digest.hexdigest()


def _validate_adoption_manifest(
    bundle: Path,
    identities: dict[str, dict[str, Any]],
    external: dict[str, Any],
) -> None:
    manifest = _require_mapping(
        _decode_json(
            bundle / ADOPTION_MANIFEST_NAME,
            MAX_SMALL_MEMBER_BYTES,
            "adoption manifest",
        ),
        "adoption manifest",
    )
    _exact_keys(
        manifest,
        {"schema", "source", "adoption", "members"},
        "adoption manifest",
    )
    _exact_string(
        manifest.get("schema"),
        "fresh-xauusd-v5-terminal-adoption/v1",
        "adoption manifest schema",
    )
    source = _require_mapping(manifest.get("source"), "manifest source")
    expected_source = {
        "githubRunId": LAUNCH_RUN_ID,
        "githubRunAttempt": LAUNCH_RUN_ATTEMPT,
        "commitSha": LAUNCH_COMMIT,
        "launchArtifactId": LAUNCH_ARTIFACT_ID,
        "launchArtifactDigest": LAUNCH_ARTIFACT_DIGEST,
        "launchArtifactSize": LAUNCH_ARTIFACT_SIZE,
    }
    _exact_keys(source, set(expected_source), "manifest source")
    for key, expected in expected_source.items():
        value = source.get(key)
        if type(expected) is int:
            _exact_int(value, expected, f"manifest source {key}")
        else:
            _exact_string(value, expected, f"manifest source {key}")
    adoption = _require_mapping(
        manifest.get("adoption"),
        "manifest adoption",
    )
    _exact_keys(
        adoption,
        {
            "githubRunId",
            "githubRunAttempt",
            "commitSha",
            "remoteMutation",
        },
        "manifest adoption",
    )
    external_run = _require_mapping(external.get("run"), "external run")
    _exact_int(
        adoption.get("githubRunId"),
        _positive_int(external_run.get("id"), "external run id"),
        "manifest adoption run id",
    )
    _exact_int(
        adoption.get("githubRunAttempt"),
        _positive_int(external_run.get("attempt"), "external run attempt"),
        "manifest adoption run attempt",
    )
    external_commit = external_run.get("commitSha")
    if type(external_commit) is not str:
        raise AuditInputError("external run commit is invalid")
    _exact_string(
        adoption.get("commitSha"),
        external_commit,
        "manifest adoption commit",
    )
    _exact_false(adoption.get("remoteMutation"), "manifest remote mutation")
    members = _require_list(manifest.get("members"), "manifest members")
    if len(members) != len(MANIFEST_MEMBER_NAMES):
        raise AuditInputError("manifest member count changed")
    for index, expected_name in enumerate(MANIFEST_MEMBER_NAMES):
        member = _require_mapping(
            members[index],
            f"manifest member {index}",
        )
        _exact_keys(member, {"name", "size", "sha256"}, "manifest member")
        identity = identities[expected_name]
        _exact_string(
            member.get("name"),
            expected_name,
            "manifest member name",
        )
        _exact_int(
            member.get("size"),
            _positive_int(identity.get("size"), "extracted member size"),
            "manifest member size",
        )
        digest = identity.get("sha256")
        if type(digest) is not str:
            raise AuditInputError("extracted member digest is invalid")
        _exact_string(
            member.get("sha256"),
            digest,
            "manifest member digest",
        )


def extract_exact_bundle(
    zip_path: Path,
    destination: Path,
    external_path: Path,
) -> dict[str, Any]:
    """Verify and safely extract one exact four-member adoption ZIP."""
    external = _require_mapping(
        _decode_json(
            external_path,
            MAX_API_JSON_BYTES,
            "normalized external metadata",
        ),
        "normalized external metadata",
    )
    _exact_string(
        external.get("schema"),
        "fresh-xauusd-v5-external-adoption/v1",
        "external metadata schema",
    )
    artifact = _require_mapping(
        external.get("artifact"),
        "normalized external artifact",
    )
    expected_size = _positive_int(
        artifact.get("size"),
        "normalized artifact size",
    )
    expected_digest = artifact.get("digest")
    if type(expected_digest) is not str:
        raise AuditInputError("normalized artifact digest is invalid")
    _parse_digest(expected_digest, "normalized artifact digest")
    zip_size, zip_sha256 = _sha256_regular(
        zip_path,
        MAX_ZIP_BYTES,
        "adoption artifact ZIP",
    )
    _exact_int(zip_size, expected_size, "downloaded ZIP size")
    _exact_string(
        f"sha256:{zip_sha256}",
        expected_digest,
        "downloaded ZIP digest",
    )
    if destination.exists() or destination.is_symlink():
        raise AuditInputError("bundle destination already exists")
    try:
        destination.mkdir(mode=0o700 if os.name == "posix" else 0o777)
    except OSError as error:
        raise AuditInputError("bundle destination cannot be created") from error

    identities: dict[str, dict[str, Any]] = {}
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if (
                len(infos) != len(EXPECTED_NAMES)
                or len(set(names)) != len(names)
                or set(names) != set(EXPECTED_NAMES)
            ):
                raise AuditInputError(
                    "ZIP does not contain exactly four unique expected files"
                )
            expanded = 0
            for info in infos:
                _validate_zip_member(info)
                expanded += info.file_size
                if expanded > MAX_EXPANDED_BYTES:
                    raise AuditInputError("ZIP exceeds the aggregate bound")
            by_name = {info.filename: info for info in infos}
            for name in EXPECTED_NAMES:
                info = by_name[name]
                with archive.open(info, "r") as source:
                    size, digest = _copy_zip_member(
                        source,
                        destination / name,
                        info.file_size,
                    )
                identities[name] = {
                    "name": name,
                    "size": size,
                    "sha256": digest,
                }
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        if isinstance(error, AuditInputError):
            raise
        raise AuditInputError("adoption artifact is not a safe ZIP") from error

    extracted = {entry.name for entry in destination.iterdir()}
    if extracted != set(EXPECTED_NAMES):
        raise AuditInputError("bundle extraction produced unexpected files")
    _validate_adoption_manifest(destination, identities, external)
    return {
        "schema": "fresh-xauusd-v5-audit-bundle-input/v1",
        "zip": {
            "size": zip_size,
            "sha256": zip_sha256,
        },
        "members": [identities[name] for name in EXPECTED_NAMES],
    }


def _validate_runtime() -> dict[str, Any]:
    if (
        platform.python_implementation() != "CPython"
        or sys.version_info[:2] != PYTHON_SERIES
        or sys.flags.optimize != 0
    ):
        raise AuditInputError("audit workflow Python runtime changed")
    numpy_version = importlib.metadata.version("numpy")
    pandas_version = importlib.metadata.version("pandas")
    if numpy_version != NUMPY_VERSION or pandas_version != PANDAS_VERSION:
        raise AuditInputError("audit workflow dependency versions changed")
    return {
        "implementation": "CPython",
        "version": platform.python_version(),
        "optimize": 0,
        "numpy": numpy_version,
        "pandas": pandas_version,
    }


def build_provenance(
    receipt_path: Path,
    external_path: Path,
    bundle_manifest_path: Path,
    output_path: Path,
    *,
    audit_run_id: int,
    audit_run_attempt: int,
    audit_commit: str,
    audit_ref: str,
    runtime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind raw bootstrap stdout to its verified external/runtime inputs."""
    receipt_payload = _decode_json(
        receipt_path,
        MAX_AUDIT_RECEIPT_BYTES,
        "audit stdout receipt",
    )
    if type(receipt_payload) is not dict:
        raise AuditInputError("audit stdout receipt is not a JSON object")
    receipt_size, receipt_sha256 = _sha256_regular(
        receipt_path,
        MAX_AUDIT_RECEIPT_BYTES,
        "audit stdout receipt",
    )
    external = _require_mapping(
        _decode_json(
            external_path,
            MAX_API_JSON_BYTES,
            "normalized external metadata",
        ),
        "normalized external metadata",
    )
    bundle = _require_mapping(
        _decode_json(
            bundle_manifest_path,
            MAX_API_JSON_BYTES,
            "bundle input manifest",
        ),
        "bundle input manifest",
    )
    external_artifact = _require_mapping(
        external.get("artifact"),
        "external artifact",
    )
    bundle_zip = _require_mapping(bundle.get("zip"), "bundle ZIP")
    artifact_digest = external_artifact.get("digest")
    if type(artifact_digest) is not str:
        raise AuditInputError("external artifact digest is invalid")
    _exact_string(
        f"sha256:{bundle_zip.get('sha256')}",
        artifact_digest,
        "bundle/external digest binding",
    )
    _exact_int(
        bundle_zip.get("size"),
        _positive_int(external_artifact.get("size"), "external artifact size"),
        "bundle/external size binding",
    )
    if runtime is None:
        runtime = _validate_runtime()
    payload = {
        "schema": "fresh-xauusd-v5-terminal-audit-provenance/v1",
        "auditWorkflow": {
            "repository": REPOSITORY,
            "workflow": (
                ".github/workflows/fresh-xauusd-v5-terminal-audit.yml"
            ),
            "event": "push",
            "ref": audit_ref,
            "commitSha": audit_commit,
            "runId": audit_run_id,
            "runAttempt": audit_run_attempt,
        },
        "sealedAuditor": {
            "commitSha": SEALED_COMMIT,
            "bootstrapPath": BOOTSTRAP_PATH,
            "bootstrapSha256": BOOTSTRAP_SHA256,
        },
        "runtime": runtime,
        "externalAdoption": external,
        "bundleInput": bundle,
        "auditStdout": {
            "name": receipt_path.name,
            "size": receipt_size,
            "sha256": receipt_sha256,
        },
        "trustedBoundary": [
            "GitHub Actions service, API, TLS, and artifact storage",
            (
                "GitHub-hosted ubuntu-24.04 runner image and its system "
                "utilities"
            ),
            (
                "pinned actions/checkout, actions/setup-python, and "
                "actions/upload-artifact implementations"
            ),
            "CPython 3.11 binary and interpreter/site initialization",
            (
                "installed NumPy 2.0.2, pandas 2.2.3, and transitive "
                "distribution wheel contents"
            ),
        ],
    }
    _write_new_json(output_path, payload)
    return payload


def _validate_cli_identity(arguments: argparse.Namespace) -> dict[str, Any]:
    artifact_id = _parse_positive_integer(
        arguments.artifact_id,
        "artifact id",
    )
    artifact_size = _parse_positive_integer(
        arguments.artifact_size,
        "artifact size",
    )
    run_id = _parse_positive_integer(arguments.run_id, "run id")
    run_attempt = _parse_positive_integer(
        arguments.run_attempt,
        "run attempt",
    )
    job_id = _parse_positive_integer(arguments.job_id, "job id")
    commit = _parse_commit(arguments.commit, "adoption commit")
    digest = _parse_digest(arguments.artifact_digest, "artifact digest")
    expected_name = (
        f"fresh-xauusd-v5-terminal-adopted-{run_id}-{run_attempt}"
    )
    if arguments.artifact_name != expected_name:
        raise AuditInputError("artifact name is not exact for the run attempt")
    return {
        "artifact_id": artifact_id,
        "artifact_name": arguments.artifact_name,
        "artifact_size": artifact_size,
        "artifact_digest": digest,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "commit": commit,
        "job_id": job_id,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    metadata = subparsers.add_parser("validate-api")
    metadata.add_argument("artifact")
    metadata.add_argument("run")
    metadata.add_argument("listing")
    metadata.add_argument("jobs")
    metadata.add_argument("output")
    metadata.add_argument("--artifact-id", required=True)
    metadata.add_argument("--artifact-name", required=True)
    metadata.add_argument("--artifact-size", required=True)
    metadata.add_argument("--artifact-digest", required=True)
    metadata.add_argument("--run-id", required=True)
    metadata.add_argument("--run-attempt", required=True)
    metadata.add_argument("--commit", required=True)
    metadata.add_argument("--job-id", required=True)

    extract = subparsers.add_parser("extract")
    extract.add_argument("zip")
    extract.add_argument("destination")
    extract.add_argument("external")
    extract.add_argument("manifest")

    provenance = subparsers.add_parser("build-provenance")
    provenance.add_argument("receipt")
    provenance.add_argument("external")
    provenance.add_argument("bundle_manifest")
    provenance.add_argument("output")
    provenance.add_argument("--audit-run-id", required=True)
    provenance.add_argument("--audit-run-attempt", required=True)
    provenance.add_argument("--audit-commit", required=True)
    provenance.add_argument("--audit-ref", required=True)
    return parser


def main(arguments: list[str] | None = None) -> int:
    parsed = _parser().parse_args(arguments)
    if parsed.command == "validate-api":
        identity = _validate_cli_identity(parsed)
        payload = validate_external_metadata(
            Path(parsed.artifact),
            Path(parsed.run),
            Path(parsed.listing),
            Path(parsed.jobs),
            **identity,
        )
        _write_new_json(Path(parsed.output), payload)
        return 0
    if parsed.command == "extract":
        payload = extract_exact_bundle(
            Path(parsed.zip),
            Path(parsed.destination),
            Path(parsed.external),
        )
        _write_new_json(Path(parsed.manifest), payload)
        return 0
    if parsed.command == "build-provenance":
        build_provenance(
            Path(parsed.receipt),
            Path(parsed.external),
            Path(parsed.bundle_manifest),
            Path(parsed.output),
            audit_run_id=_parse_positive_integer(
                parsed.audit_run_id,
                "audit run id",
            ),
            audit_run_attempt=_parse_positive_integer(
                parsed.audit_run_attempt,
                "audit run attempt",
            ),
            audit_commit=_parse_commit(
                parsed.audit_commit,
                "audit commit",
            ),
            audit_ref=parsed.audit_ref,
        )
        return 0
    raise AuditInputError("unsupported command")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AuditInputError as error:
        print(f"AuditInputError: {error}", file=sys.stderr)
        raise SystemExit(1) from error
