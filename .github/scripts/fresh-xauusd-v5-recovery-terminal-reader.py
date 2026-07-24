#!/usr/bin/env python3
"""Read and transport one detached V5 recovery result without remote writes.

The recovery launch run and its artifact do not exist when this source is
reviewed.  Their identities are therefore supplied by the adoption workflow
and checked against GitHub before this reader accepts the launch receipt.
Only the already-sealed scientific lineage and source V5 adoption are fixed
here.

The ``probe`` and ``stream`` modes are sent to the research host over SSH on
standard input.  Their remote section uses descriptor-relative, no-follow,
no-atime reads and never creates, renames, removes, or writes a remote file.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import tarfile
import time
from typing import Any, BinaryIO, Mapping


RUN_BRANCH = "codex/xauusd-fresh-walkforward"
LINEAGE_SHA256 = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)
RECOVERY_ATTEMPT_ID = "v5-discovery-recovery-attempt-1"
SOURCE_ADOPTION_RUN_ID = 30_101_048_443
SOURCE_ADOPTION_ARTIFACT_ID = 8_608_015_979
SOURCE_ADOPTION_ARTIFACT_DIGEST = (
    "sha256:6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3"
)
SOURCE_TERMINAL_ARCHIVE_SHA256 = (
    "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d"
)
RECEIPT_SCHEMA = "fresh-xauusd-v5-recovery-detached-receipt/v1"
MANIFEST_SCHEMA = "fresh-xauusd-v5-recovery-terminal-adoption/v1"
PIPELINE_MODULE = "datavis.research.fresh_pipeline_cli"

LAUNCH_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-launch-v1"
)
ARTIFACT_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-artifacts-v1"
)
STATE_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
)
SCRATCH_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/"
    "fresh-xauusd-v5-recovery-scratch-v1"
)
SOURCE_TERMINAL_ARCHIVE_NAME = "fresh-xauusd-30067832187-1.tgz"

MAX_RECEIPT_BYTES = 64 * 1024
MAX_ARCHIVE_BYTES = 12 * 1024**3
MAX_ARCHIVE_MEMBERS = 100_000
MAX_EXPANDED_BYTES = 64 * 1024**3
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
ARTIFACT_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}\Z")
TEMP_SUFFIX_PATTERN = r"[A-Za-z0-9_-]{6,24}"

RECEIPT_KEYS = {
    "schema",
    "kind",
    "status",
    "processExitStatus",
    "githubRunId",
    "githubRunAttempt",
    "branch",
    "commitSha",
    "studyLineageSha256",
    "recoveryAttemptId",
    "sourceAdoptionRunId",
    "sourceAdoptionArtifactId",
    "sourceAdoptionArtifactDigest",
    "sourceTerminalArchiveSha256",
    "controllerSha256",
    "controllerPid",
    "controllerStartTicks",
    "pipelinePid",
    "pipelineStartTicks",
    "paths",
    "terminalArchive",
}
PATH_KEYS = {
    "worktree",
    "output",
    "scratch",
    "recovery",
    "state",
    "terminalArchive",
    "serverLog",
}
REQUIRED_ARCHIVE_MEMBERS = {
    "fresh_experiment_ledger_v1.jsonl",
    "remote-exit-status.txt",
    "server-run.log",
}


class RecoveryAdoptionError(RuntimeError):
    """Raised when recovery adoption cannot be proved safe and exact."""


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _decode_json_bytes(raw: bytes, label: str) -> dict[str, Any]:
    if not raw or len(raw) > MAX_RECEIPT_BYTES:
        raise RecoveryAdoptionError(
            f"{label} is empty or exceeds its size bound"
        )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RecoveryAdoptionError(
            f"{label} is not canonical JSON"
        ) from error
    if not isinstance(payload, dict):
        raise RecoveryAdoptionError(f"{label} is not a JSON object")
    if raw != _canonical_json_bytes(payload):
        raise RecoveryAdoptionError(f"{label} byte encoding is not canonical")
    return payload


def _decode_base64_receipt(
    value: str,
    label: str,
) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, base64.binascii.Error) as error:
        raise RecoveryAdoptionError(
            f"{label} is not valid base64"
        ) from error
    return raw, _decode_json_bytes(raw, label)


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RecoveryAdoptionError(f"{label} is not a positive integer")
    return value


def _canonical_path(value: Any, label: str) -> PurePosixPath:
    if not isinstance(value, str):
        raise RecoveryAdoptionError(f"{label} is not a string")
    parsed = PurePosixPath(value)
    if not parsed.is_absolute() or ".." in parsed.parts or str(parsed) != value:
        raise RecoveryAdoptionError(
            f"{label} is not a canonical absolute path"
        )
    return parsed


def _receipt_names(run_id: int, run_attempt: int) -> dict[str, str]:
    prefix = f"v5-recovery-{run_id}-{run_attempt}"
    return {
        "ready": f"{prefix}.ready.json",
        "failure": f"{prefix}.failure.json",
        "terminal": f"{prefix}.terminal.json",
        "archive": f"{prefix}.tgz",
    }


def validate_launch_receipt(
    payload: Mapping[str, Any],
    *,
    expected_run_id: int | None = None,
    expected_run_attempt: int | None = None,
    expected_commit_sha: str | None = None,
    expected_controller_sha256: str | None = None,
) -> dict[str, PurePosixPath]:
    """Validate fixed provenance, runtime identity, and every path shape."""

    if set(payload) != RECEIPT_KEYS:
        raise RecoveryAdoptionError("recovery launch receipt field set changed")
    run_id = _positive_integer(
        payload.get("githubRunId"), "recovery launch run id"
    )
    run_attempt = _positive_integer(
        payload.get("githubRunAttempt"), "recovery launch run attempt"
    )
    commit_sha = payload.get("commitSha")
    controller_sha = payload.get("controllerSha256")
    if (
        not isinstance(commit_sha, str)
        or COMMIT_PATTERN.fullmatch(commit_sha) is None
    ):
        raise RecoveryAdoptionError("recovery launch commit SHA is invalid")
    if (
        not isinstance(controller_sha, str)
        or SHA256_PATTERN.fullmatch(controller_sha) is None
    ):
        raise RecoveryAdoptionError("recovery controller digest is invalid")
    expected = {
        "schema": RECEIPT_SCHEMA,
        "kind": "launch_ready",
        "status": "running",
        "processExitStatus": None,
        "branch": RUN_BRANCH,
        "studyLineageSha256": LINEAGE_SHA256,
        "recoveryAttemptId": RECOVERY_ATTEMPT_ID,
        "sourceAdoptionRunId": SOURCE_ADOPTION_RUN_ID,
        "sourceAdoptionArtifactId": SOURCE_ADOPTION_ARTIFACT_ID,
        "sourceAdoptionArtifactDigest": SOURCE_ADOPTION_ARTIFACT_DIGEST,
        "sourceTerminalArchiveSha256": SOURCE_TERMINAL_ARCHIVE_SHA256,
        "terminalArchive": None,
    }
    for key, value in expected.items():
        if payload.get(key) != value or type(payload.get(key)) is not type(value):
            raise RecoveryAdoptionError(
                f"recovery launch receipt {key} changed"
            )
    runtime_expected = {
        "githubRunId": expected_run_id,
        "githubRunAttempt": expected_run_attempt,
        "commitSha": expected_commit_sha,
        "controllerSha256": expected_controller_sha256,
    }
    for key, value in runtime_expected.items():
        if value is not None and payload.get(key) != value:
            raise RecoveryAdoptionError(
                f"recovery launch receipt {key} differs from GitHub identity"
            )
    for key in (
        "controllerPid",
        "controllerStartTicks",
        "pipelinePid",
        "pipelineStartTicks",
    ):
        _positive_integer(payload.get(key), f"recovery launch {key}")

    path_values = payload.get("paths")
    if not isinstance(path_values, dict) or set(path_values) != PATH_KEYS:
        raise RecoveryAdoptionError(
            "recovery launch receipt path field set changed"
        )
    paths = {
        key: _canonical_path(
            path_values.get(key), f"recovery launch path {key}"
        )
        for key in PATH_KEYS
    }
    temporary_paths = {
        "worktree": "fresh-xauusd-v5-recovery-worktree",
        "output": "fresh-xauusd-v5-recovery-output",
        "recovery": "fresh-xauusd-v5-recovery-input",
    }
    for key, prefix in temporary_paths.items():
        if (
            paths[key].parent != PurePosixPath("/tmp")
            or re.fullmatch(
                rf"{re.escape(prefix)}\.{TEMP_SUFFIX_PATTERN}",
                paths[key].name,
            )
            is None
        ):
            raise RecoveryAdoptionError(
                f"recovery launch path {key} escaped its scope"
            )
    if (
        paths["serverLog"].parent != PurePosixPath("/tmp")
        or re.fullmatch(
            rf"fresh-xauusd-v5-recovery\.{TEMP_SUFFIX_PATTERN}\.log",
            paths["serverLog"].name,
        )
        is None
    ):
        raise RecoveryAdoptionError(
            "recovery launch server log escaped its scope"
        )
    if (
        paths["scratch"].parent != SCRATCH_ROOT
        or re.fullmatch(
            rf"attempt\.{run_id}\.{run_attempt}\.{TEMP_SUFFIX_PATTERN}",
            paths["scratch"].name,
        )
        is None
    ):
        raise RecoveryAdoptionError(
            "recovery launch scratch path escaped its scope"
        )
    names = _receipt_names(run_id, run_attempt)
    if (
        paths["state"] != STATE_ROOT
        or paths["terminalArchive"].parent != ARTIFACT_ROOT
        or paths["terminalArchive"].name != names["archive"]
    ):
        raise RecoveryAdoptionError(
            "recovery durable path binding changed"
        )
    return paths


def validate_terminal_receipt(
    launch: Mapping[str, Any],
    terminal: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind a terminal envelope exactly to the accepted launch envelope."""

    validate_launch_receipt(launch)
    if set(terminal) != RECEIPT_KEYS:
        raise RecoveryAdoptionError(
            "recovery terminal receipt field set changed"
        )
    for key in RECEIPT_KEYS - {
        "kind",
        "status",
        "processExitStatus",
        "terminalArchive",
    }:
        if (
            terminal.get(key) != launch.get(key)
            or type(terminal.get(key)) is not type(launch.get(key))
        ):
            raise RecoveryAdoptionError(
                f"recovery terminal receipt {key} changed from launch"
            )
    if terminal.get("kind") != "terminal":
        raise RecoveryAdoptionError("recovery receipt is not terminal")
    exit_status = terminal.get("processExitStatus")
    if (
        isinstance(exit_status, bool)
        or not isinstance(exit_status, int)
        or not 0 <= exit_status <= 255
    ):
        raise RecoveryAdoptionError(
            "recovery terminal process exit status is invalid"
        )
    expected_status = "succeeded" if exit_status == 0 else "failed"
    if terminal.get("status") != expected_status:
        raise RecoveryAdoptionError(
            "recovery terminal status disagrees with exit status"
        )
    archive = terminal.get("terminalArchive")
    if not isinstance(archive, dict) or set(archive) != {
        "size",
        "sha256",
        "device",
        "inode",
    }:
        raise RecoveryAdoptionError(
            "recovery terminal archive identity is incomplete"
        )
    size = _positive_integer(archive.get("size"), "recovery archive size")
    if size > MAX_ARCHIVE_BYTES:
        raise RecoveryAdoptionError("recovery archive exceeds its size bound")
    digest = archive.get("sha256")
    if (
        not isinstance(digest, str)
        or SHA256_PATTERN.fullmatch(digest) is None
    ):
        raise RecoveryAdoptionError("recovery archive digest is invalid")
    _positive_integer(archive.get("device"), "recovery archive device")
    _positive_integer(archive.get("inode"), "recovery archive inode")
    return dict(archive)


def _require_remote_read_flags() -> tuple[int, int]:
    required = ("O_NOFOLLOW", "O_NOATIME", "O_DIRECTORY", "O_PATH")
    missing = [name for name in required if not hasattr(os, name)]
    if missing:
        raise RecoveryAdoptionError(
            "remote kernel lacks required read-only flags: "
            + ", ".join(missing)
        )
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NOATIME
    directory_flags = os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW
    return file_flags, directory_flags


def _open_directory(path: PurePosixPath) -> int:
    _, directory_flags = _require_remote_read_flags()
    if not path.is_absolute() or ".." in path.parts:
        raise RecoveryAdoptionError(
            f"{path} is not a scoped absolute directory"
        )
    descriptor = os.open("/", directory_flags)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component, directory_flags, dir_fd=descriptor
            )
            os.close(descriptor)
            descriptor = next_descriptor
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise RecoveryAdoptionError(
                f"{path} is not an owned mode-0700 directory"
            )
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_regular_at(
    directory_descriptor: int,
    name: str,
    *,
    maximum_bytes: int,
) -> bytes:
    file_flags, _ = _require_remote_read_flags()
    descriptor = os.open(name, file_flags, dir_fd=directory_descriptor)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum_bytes
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
        ):
            raise RecoveryAdoptionError(
                f"{name} is not a bounded owned regular file"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(
                descriptor, min(65536, maximum_bytes + 1 - total)
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise RecoveryAdoptionError(
                    f"{name} exceeds its read bound"
                )
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if identity_before != identity_after or total != before.st_size:
            raise RecoveryAdoptionError(f"{name} changed while being read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _read_process_snapshot(pid: int) -> tuple[str, list[str], list[int]]:
    process = Path(f"/proc/{pid}")
    try:
        raw_stat = (process / "stat").read_text(encoding="ascii")
        raw_status = (process / "status").read_text(encoding="ascii")
        raw_arguments = (process / "cmdline").read_bytes()
        final_stat = (process / "stat").read_text(encoding="ascii")
    except FileNotFoundError:
        return "terminal", [], []
    stat_fields = raw_stat.rsplit(") ", 1)[1].split()
    final_fields = final_stat.rsplit(") ", 1)[1].split()
    if len(stat_fields) < 20 or len(final_fields) < 20:
        raise RecoveryAdoptionError(
            f"recorded process {pid} has malformed stat data"
        )
    if stat_fields[0] in {"X", "Z"} or final_fields[0] in {"X", "Z"}:
        return "terminal", [], []
    if stat_fields[19] != final_fields[19]:
        return "replaced", [], []
    uids: list[int] = []
    for line in raw_status.splitlines():
        key, separator, value = line.partition(":")
        if separator and key == "Uid":
            uids = [int(item) for item in value.split()]
            break
    arguments = [
        os.fsdecode(part) for part in raw_arguments.split(b"\0") if part
    ]
    return stat_fields[19], arguments, uids


def _controller_arguments_are_expected(
    arguments: list[str],
    launch: Mapping[str, Any],
) -> bool:
    run_id = int(launch["githubRunId"])
    run_attempt = int(launch["githubRunAttempt"])
    names = _receipt_names(run_id, run_attempt)
    if len(arguments) != 24 or Path(arguments[0]).name not in {
        "python3",
        "python3.11",
        "python3.12",
    }:
        return False
    controller = PurePosixPath(arguments[3])
    if (
        arguments[1:3] != ["-I", "-B"]
        or controller.name
        != "fresh-xauusd-v5-recovery-controller.py"
        or controller.parent.parent != PurePosixPath("/tmp")
        or re.fullmatch(
            rf"fresh-xauusd-v5-recovery-transfer\.{TEMP_SUFFIX_PATTERN}",
            controller.parent.name,
        )
        is None
    ):
        return False
    input_path = controller.parent / SOURCE_TERMINAL_ARCHIVE_NAME
    expected = [
        "--branch",
        RUN_BRANCH,
        "--commit-sha",
        str(launch["commitSha"]),
        "--run-id",
        str(run_id),
        "--run-attempt",
        str(run_attempt),
        "--terminal-input",
        str(input_path),
        "--terminal-archive",
        str(launch["paths"]["terminalArchive"]),
        "--ready-receipt",
        str(LAUNCH_ROOT / names["ready"]),
        "--failure-receipt",
        str(LAUNCH_ROOT / names["failure"]),
        "--terminal-receipt",
        str(LAUNCH_ROOT / names["terminal"]),
        "--expected-controller-sha256",
        str(launch["controllerSha256"]),
    ]
    return arguments[4:] == expected


def _pipeline_arguments_are_expected(
    arguments: list[str],
    paths: Mapping[str, PurePosixPath],
) -> bool:
    expected = [
        str(paths["worktree"] / ".fresh-venv/bin/python"),
        "-m",
        PIPELINE_MODULE,
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
    return arguments == expected


def _process_identity(
    pid: int,
    expected_start_ticks: int,
    validator: Any,
) -> str:
    snapshot, arguments, uids = _read_process_snapshot(pid)
    if snapshot in {"terminal", "replaced"}:
        return snapshot
    if int(snapshot) != expected_start_ticks:
        return "replaced"
    if uids[:2] != [os.getuid(), os.geteuid()]:
        raise RecoveryAdoptionError(
            f"recorded process {pid} ownership changed"
        )
    if not validator(arguments):
        raise RecoveryAdoptionError(
            f"recorded process {pid} command line changed"
        )
    return "active"


def _process_states(
    launch: Mapping[str, Any],
    paths: Mapping[str, PurePosixPath],
) -> tuple[str, str]:
    controller_state = _process_identity(
        int(launch["controllerPid"]),
        int(launch["controllerStartTicks"]),
        lambda arguments: _controller_arguments_are_expected(
            arguments, launch
        ),
    )
    pipeline_state = _process_identity(
        int(launch["pipelinePid"]),
        int(launch["pipelineStartTicks"]),
        lambda arguments: _pipeline_arguments_are_expected(arguments, paths),
    )
    return controller_state, pipeline_state


def _read_terminal_receipt(
    launch_raw: bytes,
    launch: Mapping[str, Any],
    launch_paths: Mapping[str, PurePosixPath],
) -> tuple[bytes, dict[str, Any], dict[str, Any]] | None:
    names = _receipt_names(
        int(launch["githubRunId"]), int(launch["githubRunAttempt"])
    )
    launch_descriptor = _open_directory(LAUNCH_ROOT)
    try:
        ready_raw = _read_regular_at(
            launch_descriptor,
            names["ready"],
            maximum_bytes=MAX_RECEIPT_BYTES,
        )
        if ready_raw != launch_raw:
            raise RecoveryAdoptionError(
                "remote recovery ready receipt differs from launch artifact"
            )
        try:
            os.stat(
                names["failure"],
                dir_fd=launch_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise RecoveryAdoptionError(
                "recovery failure receipt contradicts accepted ready receipt"
            )
        try:
            terminal_raw = _read_regular_at(
                launch_descriptor,
                names["terminal"],
                maximum_bytes=MAX_RECEIPT_BYTES,
            )
        except FileNotFoundError:
            return None
    finally:
        os.close(launch_descriptor)
    terminal = _decode_json_bytes(terminal_raw, "recovery terminal receipt")
    archive = validate_terminal_receipt(launch, terminal)
    if terminal["paths"] != {
        key: str(value) for key, value in launch_paths.items()
    }:
        raise RecoveryAdoptionError(
            "recovery terminal paths changed from launch"
        )
    return terminal_raw, terminal, archive


def _open_bound_archive(
    terminal: Mapping[str, Any],
    archive_identity: Mapping[str, Any],
) -> int:
    terminal_path = _canonical_path(
        terminal["paths"]["terminalArchive"],
        "recovery terminal archive path",
    )
    names = _receipt_names(
        int(terminal["githubRunId"]), int(terminal["githubRunAttempt"])
    )
    if (
        terminal_path.parent != ARTIFACT_ROOT
        or terminal_path.name != names["archive"]
    ):
        raise RecoveryAdoptionError(
            "recovery archive escaped the durable artifact root"
        )
    artifact_descriptor = _open_directory(ARTIFACT_ROOT)
    try:
        file_flags, _ = _require_remote_read_flags()
        descriptor = os.open(
            terminal_path.name,
            file_flags,
            dir_fd=artifact_descriptor,
        )
    finally:
        os.close(artifact_descriptor)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size != archive_identity["size"]
        or metadata.st_dev != archive_identity["device"]
        or metadata.st_ino != archive_identity["inode"]
        or metadata.st_size > MAX_ARCHIVE_BYTES
    ):
        os.close(descriptor)
        raise RecoveryAdoptionError(
            "opened recovery archive differs from terminal receipt"
        )
    return descriptor


def remote_probe(launch_b64: str, wait_seconds_value: str) -> int:
    """Return 75 while active, or emit the fully terminal receipt."""

    launch_raw, launch = _decode_base64_receipt(
        launch_b64, "recovery launch receipt"
    )
    launch_paths = validate_launch_receipt(launch)
    try:
        wait_seconds = int(wait_seconds_value)
    except ValueError as error:
        raise RecoveryAdoptionError(
            "wait duration is not an integer"
        ) from error
    if wait_seconds not in {0, 900, 3600, 10800, 21000}:
        raise RecoveryAdoptionError(
            "wait duration was not explicitly allowlisted"
        )
    deadline = time.monotonic() + wait_seconds
    while True:
        terminal_result = _read_terminal_receipt(
            launch_raw, launch, launch_paths
        )
        controller_state, pipeline_state = _process_states(
            launch, launch_paths
        )
        if "replaced" in {controller_state, pipeline_state}:
            raise RecoveryAdoptionError(
                "a recorded recovery process identity was replaced"
            )
        if terminal_result is not None:
            if controller_state != "active" and pipeline_state != "active":
                raw, terminal, archive = terminal_result
                descriptor = _open_bound_archive(terminal, archive)
                os.close(descriptor)
                sys.stdout.buffer.write(raw)
                sys.stdout.buffer.flush()
                return 0
        elif controller_state != "active":
            terminal_result = _read_terminal_receipt(
                launch_raw, launch, launch_paths
            )
            if terminal_result is None:
                raise RecoveryAdoptionError(
                    "recovery controller is terminal but no terminal "
                    "receipt exists"
                )
            continue
        if time.monotonic() >= deadline:
            return 75
        time.sleep(min(5.0, max(0.0, deadline - time.monotonic())))


def _stream_descriptor(
    descriptor: int,
    destination: BinaryIO,
    expected_sha256: str,
) -> None:
    before = os.fstat(descriptor)
    digest = hashlib.sha256()
    total = 0
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        destination.write(chunk)
        total += len(chunk)
    destination.flush()
    after = os.fstat(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after or total != before.st_size:
        raise RecoveryAdoptionError(
            "recovery archive changed while streaming"
        )
    if digest.hexdigest() != expected_sha256:
        raise RecoveryAdoptionError(
            "streamed recovery archive digest differs from terminal receipt"
        )


def remote_stream(launch_b64: str, terminal_b64: str) -> int:
    """Stream the exact archive bound by an accepted terminal receipt."""

    launch_raw, launch = _decode_base64_receipt(
        launch_b64, "recovery launch receipt"
    )
    expected_raw, expected_terminal = _decode_base64_receipt(
        terminal_b64, "recovery terminal receipt"
    )
    launch_paths = validate_launch_receipt(launch)
    archive_identity = validate_terminal_receipt(
        launch, expected_terminal
    )
    terminal_result = _read_terminal_receipt(
        launch_raw, launch, launch_paths
    )
    if terminal_result is None or terminal_result[0] != expected_raw:
        raise RecoveryAdoptionError(
            "recovery terminal receipt changed before archive streaming"
        )
    controller_state, pipeline_state = _process_states(
        launch, launch_paths
    )
    if "replaced" in {controller_state, pipeline_state}:
        raise RecoveryAdoptionError(
            "a recorded recovery process identity was replaced"
        )
    if controller_state == "active" or pipeline_state == "active":
        raise RecoveryAdoptionError(
            "recorded recovery processes are not terminal"
        )
    descriptor = _open_bound_archive(expected_terminal, archive_identity)
    try:
        _stream_descriptor(
            descriptor,
            sys.stdout.buffer,
            str(archive_identity["sha256"]),
        )
    finally:
        os.close(descriptor)
    return 0


def _regular_file_identity(path: Path) -> dict[str, Any]:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RecoveryAdoptionError(f"{path.name} is not a regular file")
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    final = path.lstat()
    if (
        total != metadata.st_size
        or metadata.st_dev != final.st_dev
        or metadata.st_ino != final.st_ino
        or metadata.st_size != final.st_size
        or metadata.st_mtime_ns != final.st_mtime_ns
    ):
        raise RecoveryAdoptionError(f"{path.name} changed while hashing")
    return {
        "name": path.name,
        "size": total,
        "sha256": digest.hexdigest(),
    }


def _read_local_receipt(path: Path, label: str) -> dict[str, Any]:
    identity = _regular_file_identity(path)
    if identity["size"] > MAX_RECEIPT_BYTES:
        raise RecoveryAdoptionError(f"{label} exceeds its size bound")
    return _decode_json_bytes(path.read_bytes(), label)


def verify_local_archive(
    archive_path: Path,
    launch_path: Path,
    terminal_path: Path,
) -> None:
    """Verify transport identity and safe flat recovery-output tar shape."""

    launch = _read_local_receipt(
        launch_path, "recovery launch receipt"
    )
    terminal = _read_local_receipt(
        terminal_path, "recovery terminal receipt"
    )
    validate_launch_receipt(launch)
    archive_identity = validate_terminal_receipt(launch, terminal)
    local_identity = _regular_file_identity(archive_path)
    if (
        local_identity["size"] != archive_identity["size"]
        or local_identity["sha256"] != archive_identity["sha256"]
    ):
        raise RecoveryAdoptionError(
            "retrieved recovery archive differs from terminal receipt"
        )

    seen: set[str] = set()
    expanded_bytes = 0
    with tarfile.open(archive_path, mode="r:gz") as bundle:
        for count, member in enumerate(bundle, start=1):
            if count > MAX_ARCHIVE_MEMBERS:
                raise RecoveryAdoptionError(
                    "retrieved recovery archive has too many members"
                )
            pure = PurePosixPath(member.name)
            if (
                not member.isfile()
                or pure.is_absolute()
                or len(pure.parts) != 1
                or ".." in pure.parts
                or "\\" in member.name
                or member.name in seen
            ):
                raise RecoveryAdoptionError(
                    "retrieved recovery archive has an unsafe member"
                )
            seen.add(member.name)
            expanded_bytes += member.size
            if expanded_bytes > MAX_EXPANDED_BYTES:
                raise RecoveryAdoptionError(
                    "retrieved recovery archive exceeds expansion bound"
                )
    if not REQUIRED_ARCHIVE_MEMBERS.issubset(seen):
        raise RecoveryAdoptionError(
            "retrieved recovery archive lacks required transport members"
        )


def _write_new_local(path: Path, raw: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def build_manifest(
    destination: Path,
    launch_path: Path,
    terminal_path: Path,
    archive_path: Path,
    expected_launch_run_id: int,
    expected_launch_run_attempt: int,
    expected_launch_commit_sha: str,
    launch_artifact_id: int,
    launch_artifact_digest: str,
    launch_artifact_size: int,
    adoption_run_id: int,
    adoption_run_attempt: int,
    adoption_commit_sha: str,
) -> None:
    """Create the fourth immutable transport member with full provenance."""

    expected_launch_run_id = _positive_integer(
        expected_launch_run_id, "recovery launch run id"
    )
    expected_launch_run_attempt = _positive_integer(
        expected_launch_run_attempt, "recovery launch run attempt"
    )
    if COMMIT_PATTERN.fullmatch(expected_launch_commit_sha) is None:
        raise RecoveryAdoptionError(
            "recovery launch commit SHA is invalid"
        )
    launch_artifact_id = _positive_integer(
        launch_artifact_id, "recovery launch artifact id"
    )
    launch_artifact_size = _positive_integer(
        launch_artifact_size, "recovery launch artifact size"
    )
    if ARTIFACT_DIGEST_PATTERN.fullmatch(launch_artifact_digest) is None:
        raise RecoveryAdoptionError(
            "recovery launch artifact digest is invalid"
        )
    _positive_integer(adoption_run_id, "recovery adoption run id")
    _positive_integer(
        adoption_run_attempt, "recovery adoption run attempt"
    )
    if COMMIT_PATTERN.fullmatch(adoption_commit_sha) is None:
        raise RecoveryAdoptionError(
            "recovery adoption commit SHA is invalid"
        )
    launch = _read_local_receipt(
        launch_path, "recovery launch receipt"
    )
    terminal = _read_local_receipt(
        terminal_path, "recovery terminal receipt"
    )
    validate_launch_receipt(
        launch,
        expected_run_id=expected_launch_run_id,
        expected_run_attempt=expected_launch_run_attempt,
        expected_commit_sha=expected_launch_commit_sha,
    )
    archive_identity = validate_terminal_receipt(launch, terminal)
    verify_local_archive(archive_path, launch_path, terminal_path)
    launch_run_id = int(launch["githubRunId"])
    launch_run_attempt = int(launch["githubRunAttempt"])
    payload = {
        "schema": MANIFEST_SCHEMA,
        "source": {
            "githubRunId": launch_run_id,
            "githubRunAttempt": launch_run_attempt,
            "commitSha": launch["commitSha"],
            "workflowPath": (
                ".github/workflows/"
                "fresh-xauusd-v5-recovery-detached-launch.yml"
            ),
            "launchArtifactId": launch_artifact_id,
            "launchArtifactName": (
                "fresh-xauusd-v5-recovery-launch-"
                f"{launch_run_id}-{launch_run_attempt}"
            ),
            "launchArtifactDigest": launch_artifact_digest,
            "launchArtifactSize": launch_artifact_size,
        },
        "adoption": {
            "githubRunId": adoption_run_id,
            "githubRunAttempt": adoption_run_attempt,
            "commitSha": adoption_commit_sha,
            "workflowPath": (
                ".github/workflows/"
                "fresh-xauusd-v5-recovery-terminal-adoption.yml"
            ),
            "remoteMutation": False,
        },
        "remoteArchiveIdentity": archive_identity,
        "members": [
            _regular_file_identity(launch_path),
            _regular_file_identity(terminal_path),
            _regular_file_identity(archive_path),
        ],
    }
    _write_new_local(destination, _canonical_json_bytes(payload))


def main(arguments: list[str]) -> int:
    if not arguments:
        raise RecoveryAdoptionError("mode is required")
    mode, *values = arguments
    if mode == "validate-launch" and len(values) == 5:
        payload = _read_local_receipt(
            Path(values[0]), "recovery launch receipt"
        )
        validate_launch_receipt(
            payload,
            expected_run_id=int(values[1]),
            expected_run_attempt=int(values[2]),
            expected_commit_sha=values[3],
            expected_controller_sha256=values[4],
        )
        return 0
    if mode == "probe" and len(values) == 2:
        return remote_probe(values[0], values[1])
    if mode == "stream" and len(values) == 2:
        return remote_stream(values[0], values[1])
    if mode == "verify-archive" and len(values) == 3:
        verify_local_archive(
            Path(values[0]), Path(values[1]), Path(values[2])
        )
        return 0
    if mode == "build-manifest" and len(values) == 13:
        build_manifest(
            Path(values[0]),
            Path(values[1]),
            Path(values[2]),
            Path(values[3]),
            int(values[4]),
            int(values[5]),
            values[6],
            int(values[7]),
            values[8],
            int(values[9]),
            int(values[10]),
            int(values[11]),
            values[12],
        )
        return 0
    raise RecoveryAdoptionError(f"invalid arguments for mode {mode!r}")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except RecoveryAdoptionError as error:
        print(f"recovery adoption refused: {error}", file=sys.stderr)
        raise SystemExit(1) from error
