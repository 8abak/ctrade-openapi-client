"""Outcome-blind adoption and retrieval for the surviving Run 19 study.

This module is intentionally pinned to the one already-running study.  It never
starts research, reads research outcomes, signals a process, or removes remote
state.  Its wait mode observes exact /proc identities until the original Bash
EXIT trap has committed its atomic archive.  Its stream mode then emits that
verified archive through an already-open, O_NOFOLLOW file descriptor.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tarfile
import time
from typing import Iterable, Optional, Sequence


RUN_ID = "30042880650"
RUN_ATTEMPT = "1"
RUN_SHA = "48ef503cbb01d53629bd1156b5d95e1396b412fb"
RUN_BRANCH = "codex/xauusd-fresh-walkforward"
PIPELINE_MODULE = "datavis.research.fresh_pipeline_cli"
ARCHIVE_NAME = f"fresh-xauusd-{RUN_ID}-{RUN_ATTEMPT}.tgz"
RESTART_ARCHIVE_NAME = "run17-restart.tgz"
RESTART_ARCHIVE_SHA256 = (
    "13f3c091ecb54d58f1d467d9ce0022617658f80a1a7fa38f4c78c33a9c865ada"
)
STATE_ROOT = Path(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
)
SCRATCH_ROOT = Path(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-scratch-v1"
)
SCRATCH_OWNER = (
    f"fresh-xauusd-scratch-owner-v1:{RUN_ID}:{RUN_ATTEMPT}:{RUN_SHA}"
)
TMP_NAME = re.compile(r"[A-Za-z0-9]{6}\Z")
SCRATCH_NAME = re.compile(
    rf"run\.{re.escape(RUN_ID)}\.{re.escape(RUN_ATTEMPT)}\."
    r"[A-Za-z0-9]{6}\Z"
)
HEARTBEAT_SECONDS = 300.0
FINALIZER_GRACE_SECONDS = 1800.0
MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAX_TAR_MEMBERS = 10000
MAX_TAR_EXPANDED_BYTES = 2 * 1024 * 1024 * 1024
EXIT_WAITING = 75
EXIT_FORENSIC_REQUIRED = 76


class AdoptionError(RuntimeError):
    """Run 19 cannot be adopted without violating a frozen invariant."""


def emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)


def command_arguments(pid: int) -> Optional[tuple[str, ...]]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    arguments = tuple(os.fsdecode(part) for part in raw.split(b"\0") if part)
    return arguments or None


def parse_process_stat(raw: str) -> dict:
    closing = raw.rfind(")")
    if closing < 0:
        raise AdoptionError("malformed process stat")
    fields = raw[closing + 2 :].split()
    if len(fields) <= 19:
        raise AdoptionError("short process stat")
    return {
        "state": fields[0],
        "ppid": int(fields[1]),
        "pgrp": int(fields[2]),
        "session": int(fields[3]),
        "start_ticks": int(fields[19]),
    }


def process_stat(pid: int) -> Optional[dict]:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    return parse_process_stat(raw)


def process_uid(pid: int) -> Optional[int]:
    try:
        return Path(f"/proc/{pid}").stat().st_uid
    except OSError:
        return None


def iter_processes() -> Iterable[tuple[int, tuple[str, ...]]]:
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        arguments = command_arguments(int(entry.name))
        if arguments:
            yield int(entry.name), arguments


def option_once(arguments: Sequence[str], option: str) -> Optional[str]:
    positions = [
        index for index, value in enumerate(arguments) if value == option
    ]
    if len(positions) != 1 or positions[0] + 1 >= len(arguments):
        return None
    return arguments[positions[0] + 1]


def is_target_pipeline(arguments: Sequence[str]) -> bool:
    return (
        len(arguments) == 14
        and arguments[1:3] == ("-m", PIPELINE_MODULE)
        and arguments[-1] == "--execute"
    )


def is_target_parent(arguments: Sequence[str]) -> bool:
    return (
        len(arguments) == 9
        and Path(arguments[0]).name == "bash"
        and arguments[1:7]
        == (
            "-s",
            "--",
            RUN_BRANCH,
            RUN_SHA,
            RUN_ID,
            RUN_ATTEMPT,
        )
    )


def lstat_real_directory(path: Path) -> Path:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise AdoptionError(f"not a real directory: {path}")
    return path.resolve(strict=True)


def validate_tmp_directory(path: Path, prefix: str) -> Path:
    real = lstat_real_directory(path)
    if (
        path != real
        or real.parent != Path("/tmp")
        or not real.name.startswith(prefix)
        or TMP_NAME.fullmatch(real.name[len(prefix) :]) is None
    ):
        raise AdoptionError(f"unscoped temporary directory: {path}")
    if real.stat().st_uid != os.getuid():
        raise AdoptionError(f"temporary directory owner changed: {path}")
    return real


def regular_file_metadata(path: Path) -> os.stat_result:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
    ):
        raise AdoptionError(f"unsafe regular file: {path}")
    return metadata


def descriptor_digest(path: Path) -> tuple[str, os.stat_result]:
    flags = os.O_RDONLY | os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
        ):
            raise AdoptionError(f"unsafe digest source: {path}")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise AdoptionError(f"file changed while hashing: {path}")
    return digest.hexdigest(), after


def validate_restart_archive(transfer: Path) -> None:
    restart = transfer / RESTART_ARCHIVE_NAME
    regular_file_metadata(restart)
    digest, _ = descriptor_digest(restart)
    if digest != RESTART_ARCHIVE_SHA256:
        raise AdoptionError("Run 17 restart archive digest changed")


def validate_transfer_paths(
    restart_value: str, archive_value: str
) -> tuple[Path, Path]:
    restart = Path(restart_value)
    archive = Path(archive_value)
    if (
        restart.name != RESTART_ARCHIVE_NAME
        or archive.name != ARCHIVE_NAME
        or restart.parent != archive.parent
    ):
        raise AdoptionError("parent shell transfer arguments changed")
    transfer = validate_tmp_directory(
        restart.parent, "fresh-xauusd-transfer."
    )
    validate_restart_archive(transfer)
    return transfer, transfer / ARCHIVE_NAME


def discover_transfer() -> tuple[Path, Path]:
    matches = []
    for candidate in Path("/tmp").glob("fresh-xauusd-transfer.*"):
        try:
            transfer = validate_tmp_directory(
                candidate, "fresh-xauusd-transfer."
            )
            validate_restart_archive(transfer)
        except (AdoptionError, FileNotFoundError, PermissionError, OSError):
            continue
        matches.append(transfer)
    if len(matches) != 1:
        raise AdoptionError(
            f"expected one Run 19 transfer directory, found {len(matches)}"
        )
    return matches[0], matches[0] / ARCHIVE_NAME


def validate_target_pipeline(
    pid: int, arguments: tuple[str, ...]
) -> dict:
    if not is_target_pipeline(arguments):
        raise AdoptionError("pipeline invocation shape changed")
    repository = option_once(arguments, "--repository-root")
    output = option_once(arguments, "--output-dir")
    scratch = option_once(arguments, "--scratch-dir")
    state = option_once(arguments, "--research-state-dir")
    restart = option_once(arguments, "--restart-v4-artifact-dir")
    if None in (repository, output, scratch, state, restart):
        raise AdoptionError("pipeline options are incomplete or duplicated")

    expected = (
        arguments[0],
        "-m",
        PIPELINE_MODULE,
        "--repository-root",
        repository,
        "--output-dir",
        output,
        "--scratch-dir",
        scratch,
        "--research-state-dir",
        state,
        "--restart-v4-artifact-dir",
        restart,
        "--execute",
    )
    if arguments != expected:
        raise AdoptionError("pipeline arguments were reordered or extended")

    repository_path = validate_tmp_directory(
        Path(repository), "fresh-xauusd-worktree."
    )
    output_path = validate_tmp_directory(
        Path(output), "fresh-xauusd-output."
    )
    restart_path = validate_tmp_directory(
        Path(restart), "fresh-xauusd-restart."
    )
    state_path = lstat_real_directory(Path(state))
    scratch_path = lstat_real_directory(Path(scratch))
    scratch_root = lstat_real_directory(SCRATCH_ROOT)
    if state_path != STATE_ROOT or Path(state) != state_path:
        raise AdoptionError("pipeline research state root changed")
    if (
        scratch_path.parent != scratch_root
        or SCRATCH_NAME.fullmatch(scratch_path.name) is None
        or Path(scratch) != scratch_path
    ):
        raise AdoptionError("pipeline scratch path changed")
    owner = scratch_root / f".{scratch_path.name}.owner"
    regular_file_metadata(owner)
    if owner.read_text(encoding="utf-8").strip() != SCRATCH_OWNER:
        raise AdoptionError("pipeline scratch owner changed")
    if process_uid(pid) != os.getuid():
        raise AdoptionError("pipeline process owner changed")
    cwd = Path(os.readlink(f"/proc/{pid}/cwd")).resolve(strict=True)
    if cwd != repository_path:
        raise AdoptionError("pipeline current directory changed")
    if arguments[0] != str(repository_path / ".fresh-venv/bin/python"):
        raise AdoptionError("pipeline interpreter path changed")
    head = subprocess.run(
        ["git", "-C", str(repository_path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != RUN_SHA:
        raise AdoptionError("pipeline worktree commit changed")
    identity = process_stat(pid)
    if identity is None or identity["state"] == "Z":
        raise AdoptionError("pipeline disappeared during validation")
    return {
        "pid": pid,
        "arguments": arguments,
        "stat": identity,
        "repository": repository_path,
        "output": output_path,
        "scratch": scratch_path,
        "state": state_path,
        "restart": restart_path,
    }


def validate_parent(
    pipeline: dict,
) -> tuple[Optional[dict], Path, Path]:
    parent_pid = pipeline["stat"]["ppid"]
    parent_arguments = command_arguments(parent_pid)
    parent_identity = process_stat(parent_pid)
    if (
        parent_arguments is None
        or parent_identity is None
        or parent_identity["state"] == "Z"
        or not is_target_parent(parent_arguments)
    ):
        transfer, archive = discover_transfer()
        return None, transfer, archive
    if process_uid(parent_pid) != os.getuid():
        raise AdoptionError("pipeline parent owner changed")
    if parent_identity["session"] != pipeline["stat"]["session"]:
        raise AdoptionError("pipeline parent session changed")
    parent_cwd = Path(os.readlink(f"/proc/{parent_pid}/cwd")).resolve(
        strict=True
    )
    if parent_cwd != pipeline["repository"]:
        raise AdoptionError("pipeline parent current directory changed")
    transfer, archive = validate_transfer_paths(
        parent_arguments[7], parent_arguments[8]
    )
    parent = {
        "pid": parent_pid,
        "arguments": parent_arguments,
        "stat": parent_identity,
    }
    validate_tee_sibling(pipeline, parent)
    return parent, transfer, archive


def validate_tee_sibling(pipeline: dict, parent: dict) -> None:
    siblings = []
    for pid, arguments in iter_processes():
        identity = process_stat(pid)
        if (
            identity is not None
            and identity["ppid"] == parent["pid"]
            and len(arguments) == 2
            and Path(arguments[0]).name == "tee"
        ):
            siblings.append((pid, arguments, identity))
    if not siblings:
        return
    if len(siblings) != 1:
        raise AdoptionError("ambiguous tee sibling set")
    tee_pid, tee_arguments, tee_identity = siblings[0]
    log_path = Path(tee_arguments[1])
    log_prefix = "fresh-xauusd-run."
    if (
        log_path.parent != Path("/tmp")
        or not log_path.name.startswith(log_prefix)
        or not log_path.name.endswith(".log")
        or TMP_NAME.fullmatch(log_path.name[len(log_prefix) : -4]) is None
        or process_uid(tee_pid) != os.getuid()
        or tee_identity["session"] != pipeline["stat"]["session"]
    ):
        raise AdoptionError("tee sibling identity changed")
    python_pipe = os.readlink(f"/proc/{pipeline['pid']}/fd/1")
    tee_pipe = os.readlink(f"/proc/{tee_pid}/fd/0")
    if not python_pipe.startswith("pipe:[") or python_pipe != tee_pipe:
        raise AdoptionError("pipeline-to-tee pipe identity changed")


def same_process(identity: Optional[dict]) -> bool:
    if identity is None:
        return False
    current_stat = process_stat(identity["pid"])
    current_arguments = command_arguments(identity["pid"])
    return (
        current_stat is not None
        and current_stat["state"] != "Z"
        and current_stat["start_ticks"] == identity["stat"]["start_ticks"]
        and current_arguments == identity["arguments"]
    )


def validate_tar(path: Path) -> None:
    count = 0
    expanded = 0
    seen = set()
    with tarfile.open(path, "r:gz") as bundle:
        for member in bundle:
            count += 1
            if count > MAX_TAR_MEMBERS:
                raise AdoptionError("archive has too many members")
            normalized = member.name.replace("\\", "/")
            parts = [part for part in normalized.split("/") if part not in ("", ".")]
            if normalized.startswith("/") or ".." in parts:
                raise AdoptionError("archive contains an unsafe path")
            key = "/".join(parts) or "."
            if key in seen:
                raise AdoptionError("archive contains a duplicate member")
            seen.add(key)
            if not (member.isdir() or member.isfile()):
                raise AdoptionError("archive contains a non-file member")
            if member.isfile():
                expanded += member.size
                if expanded > MAX_TAR_EXPANDED_BYTES:
                    raise AdoptionError("archive expands beyond its bound")


def validate_final_archive(path: Path) -> dict:
    if path.name != ARCHIVE_NAME:
        raise AdoptionError("final archive name changed")
    transfer = validate_tmp_directory(
        path.parent, "fresh-xauusd-transfer."
    )
    validate_restart_archive(transfer)
    before = regular_file_metadata(path)
    if before.st_size <= 0 or before.st_size > MAX_ARCHIVE_BYTES:
        raise AdoptionError("final archive size is outside its bound")
    digest, hashed = descriptor_digest(path)
    validate_tar(path)
    after = regular_file_metadata(path)
    stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(
        getattr(before, field) != getattr(hashed, field)
        or getattr(hashed, field) != getattr(after, field)
        for field in stable_fields
    ):
        raise AdoptionError("final archive changed during validation")
    return {
        "state": "ready",
        "archive": str(path),
        "sha256": digest,
        "size": after.st_size,
    }


def discover_identities() -> tuple[Optional[dict], Optional[dict], Path]:
    pipelines = [
        (pid, arguments)
        for pid, arguments in iter_processes()
        if is_target_pipeline(arguments)
    ]
    if len(pipelines) > 1:
        raise AdoptionError("multiple Run 19 pipeline processes found")
    if pipelines:
        pipeline = validate_target_pipeline(*pipelines[0])
        parent, _, archive = validate_parent(pipeline)
        return pipeline, parent, archive

    parents = [
        (pid, arguments)
        for pid, arguments in iter_processes()
        if is_target_parent(arguments)
    ]
    if len(parents) > 1:
        raise AdoptionError("multiple Run 19 parent shells found")
    if parents:
        parent_pid, parent_arguments = parents[0]
        parent_stat = process_stat(parent_pid)
        if (
            parent_stat is None
            or parent_stat["state"] == "Z"
            or process_uid(parent_pid) != os.getuid()
        ):
            raise AdoptionError("Run 19 parent identity is unstable")
        _, archive = validate_transfer_paths(
            parent_arguments[7], parent_arguments[8]
        )
        parent = {
            "pid": parent_pid,
            "arguments": parent_arguments,
            "stat": parent_stat,
        }
        return None, parent, archive

    _, archive = discover_transfer()
    return None, None, archive


def wait_for_archive(deadline_seconds: float) -> int:
    pipeline, parent, archive = discover_identities()
    started = time.monotonic()
    deadline = started + deadline_seconds
    next_heartbeat = started
    terminal_since = None

    emit(
        {
            "state": "adopted",
            "pipeline": "active" if pipeline is not None else "absent",
            "parent": "active" if parent is not None else "absent",
            "archive": str(archive),
        }
    )
    while True:
        now = time.monotonic()
        pipeline_active = same_process(pipeline)
        parent_active = same_process(parent)
        archive_present = archive.exists()

        if pipeline_active and archive_present:
            raise AdoptionError(
                "final archive appeared while the pipeline was still active"
            )
        if not pipeline_active and not parent_active:
            if terminal_since is None:
                terminal_since = now
            if archive_present:
                emit(validate_final_archive(archive))
                return 0
            if now - terminal_since >= FINALIZER_GRACE_SECONDS:
                emit(
                    {
                        "state": "forensic-recovery-required",
                        "reason": "original final archive did not appear",
                    }
                )
                return EXIT_FORENSIC_REQUIRED
        else:
            terminal_since = None

        if now >= deadline:
            emit(
                {
                    "state": "waiting",
                    "pipeline": "active" if pipeline_active else "absent",
                    "parent": "active" if parent_active else "absent",
                }
            )
            return EXIT_WAITING
        if now >= next_heartbeat:
            emit(
                {
                    "state": "waiting",
                    "elapsedSeconds": int(now - started),
                    "pipeline": "active" if pipeline_active else "absent",
                    "parent": "active" if parent_active else "absent",
                }
            )
            next_heartbeat = now + HEARTBEAT_SECONDS
        time.sleep(min(30.0, max(0.1, deadline - now)))


def target_processes_are_quiescent() -> bool:
    for _, arguments in iter_processes():
        if is_target_pipeline(arguments) or is_target_parent(arguments):
            return False
    return True


def stream_archive(path_value: str, expected_sha256: str) -> int:
    path = Path(path_value)
    if not target_processes_are_quiescent():
        raise AdoptionError("Run 19 is not quiescent during retrieval")
    validated = validate_final_archive(path)
    if validated["sha256"] != expected_sha256:
        raise AdoptionError("requested archive digest does not match")

    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    output = sys.stdout.buffer
    try:
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            output.write(chunk)
        output.flush()
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if digest.hexdigest() != expected_sha256:
        raise AdoptionError("archive changed while streaming")
    stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(
        getattr(before, field) != getattr(after, field)
        for field in stable_fields
    ):
        raise AdoptionError("archive metadata changed while streaming")
    return 0


def main(arguments: Sequence[str]) -> int:
    if len(arguments) == 2 and arguments[0] == "wait":
        try:
            deadline = float(arguments[1])
        except ValueError as error:
            raise AdoptionError("invalid adoption deadline") from error
        if deadline <= 0 or deadline > 19800:
            raise AdoptionError("adoption deadline is outside its bound")
        return wait_for_archive(deadline)
    if len(arguments) == 3 and arguments[0] == "stream":
        if re.fullmatch(r"[0-9a-f]{64}", arguments[2]) is None:
            raise AdoptionError("invalid requested digest")
        return stream_archive(arguments[1], arguments[2])
    raise AdoptionError("usage: wait SECONDS | stream ARCHIVE SHA256")


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except AdoptionError as error:
        print(f"run19-adoption-error: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1)

