#!/usr/bin/env python3
"""Validate and read one detached v5 terminal artifact without remote writes.

The ``probe`` and ``stream`` modes are sent to the research host over SSH on
standard input.  They deliberately use O_NOFOLLOW and O_NOATIME descriptor
opens so that adoption neither follows substituted links nor changes access
times on the preserved research state.
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
from typing import Any, BinaryIO


RUN_ID = 30067832187
RUN_ATTEMPT = 1
RUN_COMMIT = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
RUN_BRANCH = "codex/xauusd-fresh-walkforward"
LINEAGE_SHA256 = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)
RUN19_ARTIFACT_ID = 8585919266
RUN19_ARCHIVE_SHA256 = (
    "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
)
CONTROLLER_SHA256 = (
    "da57bce0f90890a8712edbb8cb9830054bfc5b2b3d544c2363420836b8b9ce3f"
)
LAUNCH_ARTIFACT_ID = 8586881858
LAUNCH_ARTIFACT_SIZE = 801
LAUNCH_ARTIFACT_DIGEST = (
    "sha256:86f6a8b06e0fde6a5223099a1eda4a9ce6e2f6fdd6248dbef91bbc4395936e1e"
)
LAUNCH_RECEIPT_SHA256 = (
    "c6e32cbbdaaa2b9d343eee2a2fc399804976a6493ad4f99776b5ad795c5c54a4"
)
LAUNCH_RECEIPT_SIZE = 1146
RECEIPT_SCHEMA = "fresh-xauusd-detached-research-receipt/v1"
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
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-scratch-v1"
)
TERMINAL_RECEIPT_NAME = f"v5-{RUN_ID}-{RUN_ATTEMPT}.terminal.json"
READY_RECEIPT_NAME = f"v5-{RUN_ID}-{RUN_ATTEMPT}.ready.json"
FAILURE_RECEIPT_NAME = f"v5-{RUN_ID}-{RUN_ATTEMPT}.failure.json"
ARCHIVE_NAME = f"fresh-xauusd-{RUN_ID}-{RUN_ATTEMPT}.tgz"
MAX_RECEIPT_BYTES = 64 * 1024
MAX_ARCHIVE_BYTES = 12 * 1024**3
MAX_ARCHIVE_MEMBERS = 100_000
MAX_EXPANDED_BYTES = 64 * 1024**3
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}\Z")

LAUNCH_KEYS = {
    "schema",
    "kind",
    "status",
    "processExitStatus",
    "githubRunId",
    "githubRunAttempt",
    "branch",
    "commitSha",
    "studyLineageSha256",
    "run19ArtifactId",
    "run19TerminalArchiveSha256",
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
    "restart",
    "transfer",
    "terminalArchive",
    "serverLog",
}


class AdoptionError(RuntimeError):
    """Raised when an adoption identity or safety check fails."""


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _decode_json_bytes(raw: bytes, label: str) -> dict[str, Any]:
    if not raw or len(raw) > MAX_RECEIPT_BYTES:
        raise AdoptionError(f"{label} is empty or exceeds its size bound")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AdoptionError(f"{label} is not canonical JSON") from error
    if not isinstance(payload, dict):
        raise AdoptionError(f"{label} is not a JSON object")
    if raw != _canonical_json_bytes(payload):
        raise AdoptionError(f"{label} byte encoding is not canonical")
    return payload


def _decode_base64_receipt(value: str, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, base64.binascii.Error) as error:
        raise AdoptionError(f"{label} is not valid base64") from error
    if label == "launch receipt" and (
        len(raw) != LAUNCH_RECEIPT_SIZE
        or hashlib.sha256(raw).hexdigest() != LAUNCH_RECEIPT_SHA256
    ):
        raise AdoptionError("launch receipt exact byte identity changed")
    return raw, _decode_json_bytes(raw, label)


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AdoptionError(f"{label} is not a positive integer")
    return value


def _canonical_path(value: Any, label: str) -> PurePosixPath:
    if not isinstance(value, str):
        raise AdoptionError(f"{label} is not a string")
    parsed = PurePosixPath(value)
    if not parsed.is_absolute() or ".." in parsed.parts or str(parsed) != value:
        raise AdoptionError(f"{label} is not a canonical absolute path")
    return parsed


def validate_launch_receipt(payload: dict[str, Any]) -> dict[str, PurePosixPath]:
    """Validate every fixed and path-shape field in the immutable launch receipt."""

    if set(payload) != LAUNCH_KEYS:
        raise AdoptionError("launch receipt field set changed")
    fixed = {
        "schema": RECEIPT_SCHEMA,
        "kind": "launch_ready",
        "status": "running",
        "processExitStatus": None,
        "githubRunId": RUN_ID,
        "githubRunAttempt": RUN_ATTEMPT,
        "branch": RUN_BRANCH,
        "commitSha": RUN_COMMIT,
        "studyLineageSha256": LINEAGE_SHA256,
        "run19ArtifactId": RUN19_ARTIFACT_ID,
        "run19TerminalArchiveSha256": RUN19_ARCHIVE_SHA256,
        "controllerSha256": CONTROLLER_SHA256,
        "terminalArchive": None,
    }
    for key, expected in fixed.items():
        if payload.get(key) != expected or type(payload.get(key)) is not type(
            expected
        ):
            raise AdoptionError(f"launch receipt {key} changed")
    for key in (
        "controllerPid",
        "controllerStartTicks",
        "pipelinePid",
        "pipelineStartTicks",
    ):
        _positive_integer(payload.get(key), f"launch receipt {key}")

    path_values = payload.get("paths")
    if not isinstance(path_values, dict) or set(path_values) != PATH_KEYS:
        raise AdoptionError("launch receipt path field set changed")
    paths = {
        key: _canonical_path(path_values.get(key), f"launch receipt path {key}")
        for key in PATH_KEYS
    }
    scoped_tmp = {
        "worktree": r"fresh-xauusd-worktree\.[A-Za-z0-9]{6}",
        "output": r"fresh-xauusd-output\.[A-Za-z0-9]{6}",
        "restart": r"fresh-xauusd-restart\.[A-Za-z0-9]{6}",
        "transfer": r"fresh-xauusd-transfer\.[A-Za-z0-9]{6}",
        "serverLog": r"fresh-xauusd-run\.[A-Za-z0-9]{6}\.log",
    }
    for key, pattern in scoped_tmp.items():
        if (
            paths[key].parent != PurePosixPath("/tmp")
            or re.fullmatch(pattern, paths[key].name) is None
        ):
            raise AdoptionError(f"launch receipt path {key} escaped its scope")
    if (
        paths["scratch"].parent != SCRATCH_ROOT
        or re.fullmatch(
            rf"run\.{RUN_ID}\.{RUN_ATTEMPT}\.[A-Za-z0-9]{{6}}",
            paths["scratch"].name,
        )
        is None
    ):
        raise AdoptionError("launch receipt scratch path changed")
    if (
        paths["terminalArchive"].parent != ARTIFACT_ROOT
        or paths["terminalArchive"].name != ARCHIVE_NAME
    ):
        raise AdoptionError("launch receipt terminal archive path changed")
    return paths


def validate_terminal_receipt(
    launch: dict[str, Any],
    terminal: dict[str, Any],
) -> dict[str, Any]:
    """Bind a terminal receipt exactly to its accepted launch receipt."""

    validate_launch_receipt(launch)
    if set(terminal) != LAUNCH_KEYS:
        raise AdoptionError("terminal receipt field set changed")
    for key in LAUNCH_KEYS - {
        "kind",
        "status",
        "processExitStatus",
        "terminalArchive",
    }:
        if (
            terminal.get(key) != launch.get(key)
            or type(terminal.get(key)) is not type(launch.get(key))
        ):
            raise AdoptionError(f"terminal receipt {key} changed from launch")
    if terminal.get("kind") != "terminal":
        raise AdoptionError("receipt is not terminal")
    exit_status = terminal.get("processExitStatus")
    if (
        isinstance(exit_status, bool)
        or not isinstance(exit_status, int)
        or not 0 <= exit_status <= 255
    ):
        raise AdoptionError("terminal process exit status is invalid")
    expected_status = "succeeded" if exit_status == 0 else "failed"
    if terminal.get("status") != expected_status:
        raise AdoptionError("terminal status and process exit status disagree")

    archive = terminal.get("terminalArchive")
    if not isinstance(archive, dict) or set(archive) != {
        "size",
        "sha256",
        "device",
        "inode",
    }:
        raise AdoptionError("terminal archive identity is incomplete")
    size = _positive_integer(archive.get("size"), "terminal archive size")
    if size > MAX_ARCHIVE_BYTES:
        raise AdoptionError("terminal archive exceeds its size bound")
    digest = archive.get("sha256")
    if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
        raise AdoptionError("terminal archive digest is invalid")
    _positive_integer(archive.get("device"), "terminal archive device")
    _positive_integer(archive.get("inode"), "terminal archive inode")
    return archive


def _require_remote_read_flags() -> tuple[int, int]:
    required = ("O_NOFOLLOW", "O_NOATIME", "O_DIRECTORY", "O_PATH")
    missing = [name for name in required if not hasattr(os, name)]
    if missing:
        raise AdoptionError(
            "remote kernel lacks required read-only flags: " + ", ".join(missing)
        )
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NOATIME
    # O_NOATIME on root-owned path components would fail for ec2-user.  O_PATH
    # performs descriptor-only traversal without reading directory contents or
    # updating their access times; O_NOATIME remains mandatory for owned files.
    directory_flags = os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW
    return file_flags, directory_flags


def _open_directory(path: PurePosixPath) -> int:
    _, directory_flags = _require_remote_read_flags()
    if not path.is_absolute() or ".." in path.parts:
        raise AdoptionError(f"{path} is not a scoped absolute directory")
    descriptor = os.open("/", directory_flags)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                directory_flags,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise AdoptionError(f"{path} is not a directory")
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
            raise AdoptionError(f"{name} is not a bounded owned regular file")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65536, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise AdoptionError(f"{name} exceeds its read bound")
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
            raise AdoptionError(f"{name} changed while being read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _process_identity(
    pid: int,
    expected_start_ticks: int,
    expected_arguments: tuple[str, ...],
) -> str:
    """Return active, terminal, or replaced for one recorded process."""

    process = Path(f"/proc/{pid}")
    try:
        raw_stat = (process / "stat").read_text(encoding="ascii")
    except FileNotFoundError:
        return "terminal"
    fields = raw_stat.rsplit(") ", 1)[1].split()
    if len(fields) < 20:
        raise AdoptionError(f"recorded process {pid} has malformed stat data")
    if int(fields[19]) != expected_start_ticks:
        return "replaced"
    if fields[0] in {"X", "Z"}:
        return "terminal"
    status_uids: list[int] | None = None
    try:
        status_lines = (process / "status").read_text(
            encoding="ascii"
        ).splitlines()
    except FileNotFoundError:
        return "terminal"
    for line in status_lines:
        key, separator, value = line.partition(":")
        if separator and key == "Uid":
            status_uids = [int(item) for item in value.split()]
            break
    try:
        arguments = tuple(
            os.fsdecode(part)
            for part in (process / "cmdline").read_bytes().split(b"\0")
            if part
        )
    except FileNotFoundError:
        return "terminal"
    try:
        final_stat = (process / "stat").read_text(encoding="ascii")
    except FileNotFoundError:
        return "terminal"
    final_fields = final_stat.rsplit(") ", 1)[1].split()
    if int(final_fields[19]) != expected_start_ticks:
        return "replaced"
    if final_fields[0] in {"X", "Z"}:
        return "terminal"
    if status_uids is None or status_uids[:2] != [os.getuid(), os.geteuid()]:
        raise AdoptionError(f"recorded process {pid} ownership changed")
    expected_executable = expected_arguments[0]
    executable_matches = (
        Path(arguments[0]).name == "bash"
        if arguments and expected_executable == "<bash>"
        else bool(arguments) and arguments[0] == expected_executable
    )
    if not executable_matches or arguments[1:] != expected_arguments[1:]:
        raise AdoptionError(f"recorded process {pid} command line changed")
    return "active"


def _expected_process_arguments(
    launch: dict[str, Any],
    paths: dict[str, PurePosixPath],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    launch_prefix = LAUNCH_ROOT / f"v5-{RUN_ID}-{RUN_ATTEMPT}"
    controller = paths["transfer"] / "fresh-xauusd-v5-controller.sh"
    restart_archive = paths["transfer"] / "run19-restart.tgz"
    controller_arguments = (
        "<bash>",
        "--noprofile",
        "--norc",
        str(controller),
        RUN_BRANCH,
        RUN_COMMIT,
        str(RUN_ID),
        str(RUN_ATTEMPT),
        str(restart_archive),
        str(paths["terminalArchive"]),
        str(launch["controllerSha256"]),
        str(PurePosixPath(f"{launch_prefix}.ready.json")),
        str(PurePosixPath(f"{launch_prefix}.failure.json")),
        str(PurePosixPath(f"{launch_prefix}.terminal.json")),
    )
    pipeline_arguments = (
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
        str(STATE_ROOT),
        "--restart-v5-artifact-dir",
        str(paths["restart"]),
        "--execute",
    )
    return controller_arguments, pipeline_arguments


def _process_states(
    launch: dict[str, Any],
    paths: dict[str, PurePosixPath],
) -> tuple[str, str]:
    controller_arguments, pipeline_arguments = _expected_process_arguments(
        launch, paths
    )
    controller_state = _process_identity(
        int(launch["controllerPid"]),
        int(launch["controllerStartTicks"]),
        controller_arguments,
    )
    pipeline_state = _process_identity(
        int(launch["pipelinePid"]),
        int(launch["pipelineStartTicks"]),
        pipeline_arguments,
    )
    return controller_state, pipeline_state


def _open_bound_archive(
    terminal: dict[str, Any],
    archive_identity: dict[str, Any],
) -> int:
    paths = terminal["paths"]
    terminal_path = _canonical_path(
        paths["terminalArchive"], "terminal archive path"
    )
    if terminal_path.parent != ARTIFACT_ROOT or terminal_path.name != ARCHIVE_NAME:
        raise AdoptionError("terminal archive path escaped the durable root")
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
        raise AdoptionError("opened archive does not match the terminal receipt")
    return descriptor


def _read_terminal_receipt(
    launch_raw: bytes,
    launch: dict[str, Any],
    launch_paths: dict[str, PurePosixPath],
) -> tuple[bytes, dict[str, Any], dict[str, Any]] | None:
    launch_descriptor = _open_directory(LAUNCH_ROOT)
    try:
        ready_raw = _read_regular_at(
            launch_descriptor,
            READY_RECEIPT_NAME,
            maximum_bytes=MAX_RECEIPT_BYTES,
        )
        if ready_raw != launch_raw:
            raise AdoptionError(
                "remote ready receipt differs from immutable launch artifact"
            )
        try:
            os.stat(
                FAILURE_RECEIPT_NAME,
                dir_fd=launch_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise AdoptionError(
                "failure receipt contradicts the accepted ready receipt"
            )
        try:
            raw = _read_regular_at(
                launch_descriptor,
                TERMINAL_RECEIPT_NAME,
                maximum_bytes=MAX_RECEIPT_BYTES,
            )
        except FileNotFoundError:
            return None
    finally:
        os.close(launch_descriptor)
    terminal = _decode_json_bytes(raw, "terminal receipt")
    archive = validate_terminal_receipt(launch, terminal)
    if terminal["paths"] != {
        key: str(value) for key, value in launch_paths.items()
    }:
        raise AdoptionError("terminal paths changed from accepted launch")
    return raw, terminal, archive


def remote_probe(launch_b64: str, wait_seconds_value: str) -> int:
    """Wait read-only for a terminal receipt and print it when fully terminal."""

    launch_raw, launch = _decode_base64_receipt(
        launch_b64, "launch receipt"
    )
    launch_paths = validate_launch_receipt(launch)
    try:
        wait_seconds = int(wait_seconds_value)
    except ValueError as error:
        raise AdoptionError("wait duration is not an integer") from error
    if wait_seconds not in {0, 900, 3600, 10800, 21000}:
        raise AdoptionError("wait duration was not explicitly allowlisted")
    deadline = time.monotonic() + wait_seconds
    while True:
        terminal_result = _read_terminal_receipt(
            launch_raw, launch, launch_paths
        )
        controller_state, pipeline_state = _process_states(launch, launch_paths)
        if terminal_result is not None:
            if controller_state != "active" and pipeline_state != "active":
                raw, _, archive = terminal_result
                descriptor = _open_bound_archive(terminal_result[1], archive)
                os.close(descriptor)
                sys.stdout.buffer.write(raw)
                sys.stdout.buffer.flush()
                return 0
        elif controller_state != "active":
            terminal_result = _read_terminal_receipt(
                launch_raw, launch, launch_paths
            )
            if terminal_result is None:
                raise AdoptionError(
                    "recorded controller is terminal but no terminal receipt exists"
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
        raise AdoptionError("terminal archive changed while streaming")
    if digest.hexdigest() != expected_sha256:
        raise AdoptionError("streamed archive digest differs from terminal receipt")


def remote_stream(launch_b64: str, terminal_b64: str) -> int:
    """Stream the exact descriptor named by an already accepted terminal receipt."""

    launch_raw, launch = _decode_base64_receipt(
        launch_b64, "launch receipt"
    )
    expected_terminal_raw, expected_terminal = _decode_base64_receipt(
        terminal_b64, "terminal receipt"
    )
    launch_paths = validate_launch_receipt(launch)
    archive_identity = validate_terminal_receipt(launch, expected_terminal)
    terminal_result = _read_terminal_receipt(
        launch_raw, launch, launch_paths
    )
    if terminal_result is None or terminal_result[0] != expected_terminal_raw:
        raise AdoptionError("terminal receipt changed before archive streaming")
    controller_state, pipeline_state = _process_states(launch, launch_paths)
    if controller_state == "active" or pipeline_state == "active":
        raise AdoptionError("recorded research processes are not terminal")
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


def verify_local_archive(archive_path: Path, terminal_path: Path) -> None:
    terminal_raw = terminal_path.read_bytes()
    terminal = _decode_json_bytes(terminal_raw, "terminal receipt")
    archive_identity = terminal.get("terminalArchive")
    if not isinstance(archive_identity, dict):
        raise AdoptionError("terminal receipt has no archive identity")
    metadata = archive_path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise AdoptionError("retrieved archive is not a regular file")
    if metadata.st_size != archive_identity.get("size"):
        raise AdoptionError("retrieved archive size differs from terminal receipt")
    digest = hashlib.sha256()
    with archive_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != archive_identity.get("sha256"):
        raise AdoptionError("retrieved archive digest differs from terminal receipt")

    seen: set[str] = set()
    expanded_bytes = 0
    with tarfile.open(archive_path, "r:gz") as bundle:
        for count, member in enumerate(bundle, start=1):
            if count > MAX_ARCHIVE_MEMBERS:
                raise AdoptionError("retrieved archive has too many members")
            if member.name in {".", "./"}:
                if not member.isdir() or "." in seen:
                    raise AdoptionError("retrieved archive has an invalid root member")
                seen.add(".")
                continue
            pure = PurePosixPath(member.name)
            normalized = str(pure)
            if (
                pure.is_absolute()
                or not pure.parts
                or ".." in pure.parts
                or "\\" in member.name
                or normalized in seen
            ):
                raise AdoptionError("retrieved archive has an unsafe member path")
            seen.add(normalized)
            if member.isfile():
                expanded_bytes += member.size
                if expanded_bytes > MAX_EXPANDED_BYTES:
                    raise AdoptionError("retrieved archive exceeds expansion bound")
            elif not member.isdir():
                raise AdoptionError("retrieved archive has a non-file member")


def build_manifest(
    destination: Path,
    launch_path: Path,
    terminal_path: Path,
    archive_path: Path,
    launch_artifact_id: int,
    launch_artifact_digest: str,
    launch_artifact_size: int,
    adoption_run_id: int,
    adoption_run_attempt: int,
    adoption_commit_sha: str,
) -> None:
    if (
        launch_artifact_id != LAUNCH_ARTIFACT_ID
        or launch_artifact_digest != LAUNCH_ARTIFACT_DIGEST
        or launch_artifact_size != LAUNCH_ARTIFACT_SIZE
    ):
        raise AdoptionError("launch artifact identity changed before manifest")
    _positive_integer(adoption_run_id, "adoption run id")
    _positive_integer(adoption_run_attempt, "adoption run attempt")
    if COMMIT_PATTERN.fullmatch(adoption_commit_sha) is None:
        raise AdoptionError("adoption commit SHA is invalid")
    launch_raw = launch_path.read_bytes()
    terminal_raw = terminal_path.read_bytes()
    if (
        len(launch_raw) != LAUNCH_RECEIPT_SIZE
        or hashlib.sha256(launch_raw).hexdigest() != LAUNCH_RECEIPT_SHA256
    ):
        raise AdoptionError("launch receipt bytes changed before manifest")
    launch = _decode_json_bytes(launch_raw, "launch receipt")
    terminal = _decode_json_bytes(terminal_raw, "terminal receipt")
    validate_terminal_receipt(launch, terminal)

    def identity(path: Path) -> dict[str, Any]:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise AdoptionError(f"{path.name} is not a regular file")
        digest_builder = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest_builder.update(chunk)
        digest = digest_builder.hexdigest()
        return {"name": path.name, "size": metadata.st_size, "sha256": digest}

    payload = {
        "schema": "fresh-xauusd-v5-terminal-adoption/v1",
        "source": {
            "githubRunId": RUN_ID,
            "githubRunAttempt": RUN_ATTEMPT,
            "commitSha": RUN_COMMIT,
            "launchArtifactId": launch_artifact_id,
            "launchArtifactDigest": launch_artifact_digest,
            "launchArtifactSize": launch_artifact_size,
        },
        "adoption": {
            "githubRunId": adoption_run_id,
            "githubRunAttempt": adoption_run_attempt,
            "commitSha": adoption_commit_sha,
            "remoteMutation": False,
        },
        "members": [
            identity(launch_path),
            identity(terminal_path),
            identity(archive_path),
        ],
    }
    destination.write_bytes(_canonical_json_bytes(payload))


def _read_local_receipt(path: Path, label: str) -> dict[str, Any]:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise AdoptionError(f"{label} is not a regular file")
    raw = path.read_bytes()
    if label == "launch receipt" and (
        len(raw) != LAUNCH_RECEIPT_SIZE
        or hashlib.sha256(raw).hexdigest() != LAUNCH_RECEIPT_SHA256
    ):
        raise AdoptionError("launch receipt exact byte identity changed")
    return _decode_json_bytes(raw, label)


def main(arguments: list[str]) -> int:
    if not arguments:
        raise AdoptionError("mode is required")
    mode, *values = arguments
    if mode == "validate-launch" and len(values) == 1:
        payload = _read_local_receipt(Path(values[0]), "launch receipt")
        validate_launch_receipt(payload)
        return 0
    if mode == "controller-sha" and len(values) == 1:
        payload = _read_local_receipt(Path(values[0]), "launch receipt")
        validate_launch_receipt(payload)
        print(payload["controllerSha256"])
        return 0
    if mode == "probe" and len(values) == 2:
        return remote_probe(values[0], values[1])
    if mode == "stream" and len(values) == 2:
        return remote_stream(values[0], values[1])
    if mode == "verify-archive" and len(values) == 2:
        verify_local_archive(Path(values[0]), Path(values[1]))
        return 0
    if mode == "build-manifest" and len(values) == 10:
        build_manifest(
            Path(values[0]),
            Path(values[1]),
            Path(values[2]),
            Path(values[3]),
            int(values[4]),
            values[5],
            int(values[6]),
            int(values[7]),
            int(values[8]),
            values[9],
        )
        return 0
    raise AdoptionError(f"invalid arguments for mode {mode!r}")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except AdoptionError as error:
        print(f"adoption refused: {error}", file=sys.stderr)
        raise SystemExit(1) from error
