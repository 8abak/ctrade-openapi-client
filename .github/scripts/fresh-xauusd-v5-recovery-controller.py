#!/usr/bin/env python3
"""One-shot detached controller for the sole audited V5 exit-137 recovery.

This file is operational orchestration, not part of the frozen V5 scientific
implementation.  It accepts only the exact nested terminal archive adopted by
GitHub run 30101048443, verifies the still-live durable ledger under the global
execution lock, and invokes the recovery CLI exactly once.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Any, Mapping, Sequence

try:
    import fcntl
except ModuleNotFoundError:  # pragma: no cover - import-only unit tests on Windows.
    fcntl = None  # type: ignore[assignment]


BRANCH = "codex/xauusd-fresh-walkforward"
STUDY_LINEAGE_SHA256 = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)
RECOVERY_ATTEMPT_ID = "v5-discovery-recovery-attempt-1"
ADOPTION_RUN_ID = 30_101_048_443
ADOPTION_RUN_ATTEMPT = 1
ADOPTION_COMMIT_SHA = "c730fd0a2c66426f995ac43f1d50035cf94265ff"
ADOPTION_JOB_ID = 89_506_876_763
ADOPTION_ARTIFACT_ID = 8_608_015_979
ADOPTION_ARTIFACT_NAME = "fresh-xauusd-v5-terminal-adopted-30101048443-1"
ADOPTION_ARTIFACT_SIZE = 127_602
ADOPTION_ARTIFACT_DIGEST = (
    "sha256:6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3"
)
TERMINAL_ARCHIVE_NAME = "fresh-xauusd-30067832187-1.tgz"
TERMINAL_ARCHIVE_SIZE = 125_470
TERMINAL_ARCHIVE_SHA256 = (
    "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d"
)
V5_LEDGER_SHA256 = (
    "e95e1739987cdb56315adcbb98b2e85198cb14a1d536a07282214d2ef359744d"
)
V5_LEDGER_SIZE = 32_058
PREDECESSOR_LEDGER_SHA256 = (
    "ac627bd986c044b12049f717eb3fc664321c08c169fd6a829a5fc8d51144c7b4"
)
PREDECESSOR_LEDGER_SIZE = 62_909

PRODUCTION_REPOSITORY = Path("/home/ec2-user/cTrade")
DURABLE_STATE_ROOT = Path(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
)
RECOVERY_SCRATCH_ROOT = Path(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-v5-recovery-scratch-v1"
)
LAUNCH_ROOT = Path(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-launch-v1"
)
ARTIFACT_ROOT = Path(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-artifacts-v1"
)
V5_LEDGER_PATH = DURABLE_STATE_ROOT / (
    "studies/0215cfa1ca0954bae1d6eaafbab44a62ddf663a356bb9f7978071afed4595371/"
    "lineages/6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473/"
    "fresh_experiment_ledger_v1.jsonl"
)
PREDECESSOR_LEDGER_PATH = DURABLE_STATE_ROOT / (
    "studies/0215cfa1ca0954bae1d6eaafbab44a62ddf663a356bb9f7978071afed4595371/"
    "lineages/aa894a42147c5b5436490470ea81b630e1d899bd3b079fa800715820c89eb928/"
    "fresh_experiment_ledger_v1.jsonl"
)
HOLDOUT_REGISTRY_PATH = DURABLE_STATE_ROOT / (
    "holdouts/8d599150987e32430a5d012b4973590bda56f7d548c42e7dad9714e2f0fe40b7/"
    "fresh_holdout_authorization_v1.json"
)

MINIMUM_SCRATCH_BYTES = 16 * 1024 * 1024 * 1024
MINIMUM_SCRATCH_INODES = 4096
HASH_CHUNK_BYTES = 1024 * 1024

RECOVERY_MEMBERS: Mapping[str, tuple[int, str]] = {
    "fresh_corpus_manifest_v1.json": (
        29_935,
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2",
    ),
    "fresh_entry_bank_v1.json": (
        386_844,
        "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c",
    ),
    "fresh_experiment_ledger_v1.jsonl": (
        V5_LEDGER_SIZE,
        V5_LEDGER_SHA256,
    ),
    "fresh_implementation_manifest_v1.json": (
        4_955,
        "45c240012263986409add7d9f478a4e8990d7403bd3ed38b4fbd403b8f15ea23",
    ),
    "fresh_preregistration_v5.json": (
        47_363,
        "06c25b8733de70b75f7ae07b136a3bfecba5bd264f0ecdaa1b153db6d0f190a6",
    ),
    "fresh_quantile_bank_v1.json": (
        141_368,
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46",
    ),
    "fresh_research_state_binding_v4.json": (
        2_412,
        "696408161ef88e94436ec1713960bc5f5ecb0c4394ea06e15643e10ed0f60567",
    ),
    "fresh_source_inventory_v1.json": (
        172_493,
        "ab1125638e76cd35517859b4e292abb3908a49d79fb92534c5fc2fd7a32e9ab8",
    ),
    "fresh_split_manifest_v2.json": (
        23_730,
        "b179c8e359b0ab998258a1bbbdac41e33b970d63e60131f3057f2dc224c1a0dc",
    ),
    "fresh_threshold_domain_preflight_v1.json": (
        704,
        "55d94106f9860676f9d42be8c7023de2bd7d7234ee812155fedd578eae6d98dc",
    ),
    "predecessor_fresh_experiment_ledger_v1.jsonl": (
        PREDECESSOR_LEDGER_SIZE,
        PREDECESSOR_LEDGER_SHA256,
    ),
    "predecessor_fresh_implementation_manifest_v1.json": (
        4_805,
        "d04bd2279c31922fc753b313f61b140a124c2fc7625227a5a0b9de29377ca1ee",
    ),
    "predecessor_fresh_preregistration_v4.json": (
        46_959,
        "fd203eed1ff5b1f407b6179b2fd18546106420a1d3ba50b7acddc65e090e0e87",
    ),
    "predecessor_fresh_research_state_binding_v3.json": (
        2_412,
        "62eacb704989a640478ab8a3d05a20cc91a0d69a3797d12dac330c9b3c606cee",
    ),
    "remote-exit-status.txt": (
        4,
        "e3b9c2844b5a5c2677b3a2279db2ec8487491dd9a23d6b22fac153391b3bb63c",
    ),
    "server-run.log": (
        3_831,
        "e99d19a11fea31762b6e49e85d4b24ca16a57dbd2be16862bce365ab6a9227d2",
    ),
}


class RecoveryControllerError(RuntimeError):
    """Fail-closed operational recovery error."""


def _pairs_without_duplicates(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RecoveryControllerError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _sha256_regular(path: Path, *, expected_size: int | None = None) -> str:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
    )
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RecoveryControllerError(f"not a regular file: {path}")
        if expected_size is not None and before.st_size != expected_size:
            raise RecoveryControllerError(f"file size changed: {path}")
        while True:
            chunk = os.read(descriptor, HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(getattr(before, key) != getattr(after, key) for key in identity):
        raise RecoveryControllerError(f"file changed while hashing: {path}")
    return digest.hexdigest()


def _read_strict_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_pairs_without_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(token)
            ),
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise RecoveryControllerError(f"invalid JSON file: {path}") from error
    if not isinstance(value, Mapping):
        raise RecoveryControllerError(f"JSON root is not an object: {path}")
    return value


def _new_directory(path: Path) -> Path:
    if path.is_symlink():
        raise RecoveryControllerError(f"refusing symlink directory: {path}")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)
    resolved = path.resolve(strict=True)
    if resolved != path or not resolved.is_dir():
        raise RecoveryControllerError(f"directory is not canonical: {path}")
    return resolved


def _existing_canonical_directory(path: Path) -> Path:
    if path.is_symlink():
        raise RecoveryControllerError(f"refusing symlink directory: {path}")
    resolved = path.resolve(strict=True)
    if resolved != path or not resolved.is_dir():
        raise RecoveryControllerError(
            f"required directory is not canonical: {path}"
        )
    return resolved


def _scoped_tmp(prefix: str) -> Path:
    value = Path(tempfile.mkdtemp(prefix=prefix, dir="/tmp"))
    resolved = value.resolve(strict=True)
    if resolved.parent != Path("/tmp") or not resolved.name.startswith(prefix):
        raise RecoveryControllerError("temporary directory escaped /tmp")
    return resolved


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":  # Windows cannot open directories as file descriptors.
        return
    directory_descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def safe_extract_recovery_archive(archive: Path, destination: Path) -> None:
    """Extract the exact 16-member V5 terminal archive without path traversal."""

    if (
        archive.name != TERMINAL_ARCHIVE_NAME
        or _sha256_regular(archive, expected_size=TERMINAL_ARCHIVE_SIZE)
        != TERMINAL_ARCHIVE_SHA256
    ):
        raise RecoveryControllerError("adopted V5 terminal archive changed")
    destination = destination.resolve(strict=True)
    seen: set[str] = set()
    root_seen = False
    with tarfile.open(archive, mode="r:gz") as bundle:
        for member in bundle:
            raw = member.name
            if raw == ".":
                if root_seen or not member.isdir():
                    raise RecoveryControllerError("terminal archive root changed")
                root_seen = True
                continue
            name = raw[2:] if raw.startswith("./") else raw
            pure = PurePosixPath(name)
            if (
                name not in RECOVERY_MEMBERS
                or name in seen
                or not member.isfile()
                or pure.name != name
                or pure.is_absolute()
                or ".." in pure.parts
            ):
                raise RecoveryControllerError("unsafe terminal archive member")
            expected_size, expected_sha = RECOVERY_MEMBERS[name]
            if member.size != expected_size:
                raise RecoveryControllerError(
                    f"terminal archive member size changed: {name}"
                )
            source = bundle.extractfile(member)
            if source is None:
                raise RecoveryControllerError(f"unreadable archive member: {name}")
            target = destination / name
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_BINARY", 0)
            )
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(target, flags, 0o600)
            digest = hashlib.sha256()
            total = 0
            with os.fdopen(descriptor, "wb") as writer:
                while True:
                    chunk = source.read(HASH_CHUNK_BYTES)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > expected_size:
                        raise RecoveryControllerError(
                            f"archive member exceeded size: {name}"
                        )
                    digest.update(chunk)
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            if total != expected_size or digest.hexdigest() != expected_sha:
                raise RecoveryControllerError(
                    f"terminal archive member digest changed: {name}"
                )
            seen.add(name)
    if not root_seen or seen != set(RECOVERY_MEMBERS):
        raise RecoveryControllerError("terminal archive member set changed")
    _fsync_directory(destination)


def validate_live_state_before_claim(
    recovery_directory: Path,
    *,
    durable_state_root: Path = DURABLE_STATE_ROOT,
) -> tuple[Path, Path]:
    """Recheck the exact live V5 prefix and untouched holdout under the lock."""

    if durable_state_root.is_symlink():
        raise RecoveryControllerError("durable state root is a symlink")
    root = durable_state_root.resolve(strict=True)
    if root != durable_state_root or root == Path("/tmp") or Path("/tmp") in root.parents:
        raise RecoveryControllerError("durable state root is unsafe")
    binding = _read_strict_json(
        recovery_directory / "fresh_research_state_binding_v4.json"
    )
    expected = {
        "stateDirectory": str(root),
        "experimentLedgerPath": str(V5_LEDGER_PATH),
        "predecessorExperimentLedgerPath": str(PREDECESSOR_LEDGER_PATH),
        "holdoutAuthorizationRegistryPath": str(HOLDOUT_REGISTRY_PATH),
        "studyLineageSha256": STUDY_LINEAGE_SHA256,
    }
    for key, value in expected.items():
        if binding.get(key) != value:
            raise RecoveryControllerError(f"V5 state binding changed: {key}")
    if (
        V5_LEDGER_PATH.is_symlink()
        or _sha256_regular(V5_LEDGER_PATH, expected_size=V5_LEDGER_SIZE)
        != V5_LEDGER_SHA256
    ):
        raise RecoveryControllerError("live V5 ledger prefix changed")
    if (
        PREDECESSOR_LEDGER_PATH.is_symlink()
        or _sha256_regular(
            PREDECESSOR_LEDGER_PATH,
            expected_size=PREDECESSOR_LEDGER_SIZE,
        )
        != PREDECESSOR_LEDGER_SHA256
    ):
        raise RecoveryControllerError("live predecessor ledger changed")
    if HOLDOUT_REGISTRY_PATH.exists() or HOLDOUT_REGISTRY_PATH.is_symlink():
        raise RecoveryControllerError("holdout authorization already exists")
    return V5_LEDGER_PATH, HOLDOUT_REGISTRY_PATH


def write_execution_claim(
    path: Path,
    *,
    run_id: int,
    run_attempt: int,
    commit_sha: str,
) -> None:
    """Create the distinct sole recovery claim with O_EXCL durability."""

    payload = {
        "schema": "fresh-xauusd-v5-recovery-execution-claim/v1",
        "githubRunId": run_id,
        "githubRunAttempt": run_attempt,
        "commitSha": commit_sha,
        "studyLineageSha256": STUDY_LINEAGE_SHA256,
        "recoveryAttemptId": RECOVERY_ATTEMPT_ID,
        "sourceAdoption": {
            "githubRunId": ADOPTION_RUN_ID,
            "githubRunAttempt": ADOPTION_RUN_ATTEMPT,
            "artifactId": ADOPTION_ARTIFACT_ID,
            "artifactDigest": ADOPTION_ARTIFACT_DIGEST,
            "terminalArchiveSha256": TERMINAL_ARCHIVE_SHA256,
        },
    }
    raw = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    directory_descriptor = os.open(
        path.parent,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_BINARY", 0)
        )
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path.name, flags, 0o600, dir_fd=directory_descriptor)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def pipeline_command(
    python: Path,
    *,
    repository: Path,
    output: Path,
    scratch: Path,
    state: Path,
    recovery: Path,
) -> tuple[str, ...]:
    return (
        str(python),
        "-m",
        "datavis.research.fresh_pipeline_cli",
        "--repository-root",
        str(repository),
        "--output-dir",
        str(output),
        "--scratch-dir",
        str(scratch),
        "--research-state-dir",
        str(state),
        "--resume-v5-artifact-dir",
        str(recovery),
        "--execute",
    )


def invoke_failure_finalizer(
    python: Path,
    ledger_path: Path,
    *,
    exit_status: int,
    cwd: Path,
    environment: Mapping[str, str],
    log_path: Path,
) -> None:
    """Invoke the required recovery finalizer; absence is a hard failure."""

    code = (
        "import sys\n"
        "from datavis.research.fresh_recovery_v5 import "
        "finalize_interrupted_fresh_v5_recovery\n"
        "result = finalize_interrupted_fresh_v5_recovery("
        "sys.argv[1], exit_status=int(sys.argv[2]))\n"
        "if type(result) is not bool:\n"
        "    raise SystemExit('V5 recovery finalizer returned a non-bool')\n"
        "print('v5-recovery-finalizer-appended=' + str(result).lower())\n"
    )
    with log_path.open("ab") as log:
        completed = subprocess.run(
            [
                str(python),
                "-B",
                "-c",
                code,
                str(ledger_path),
                str(exit_status),
            ],
            cwd=cwd,
            env=dict(environment),
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if completed.returncode != 0:
        raise RecoveryControllerError(
            "the mandatory V5 recovery finalizer failed closed"
        )


def _load_server_environment(base: Mapping[str, str]) -> dict[str, str]:
    command = (
        "set -Eeuo pipefail; "
        "set -a; eval \"$(sudo cat /etc/datavis.env)\"; set +a; env -0"
    )
    completed = subprocess.run(
        ["bash", "--noprofile", "--norc", "-c", command],
        check=True,
        capture_output=True,
    )
    result = dict(base)
    for item in completed.stdout.split(b"\0"):
        if not item:
            continue
        key, separator, value = item.partition(b"=")
        if not separator:
            raise RecoveryControllerError("server environment is malformed")
        result[os.fsdecode(key)] = os.fsdecode(value)
    return result


def _process_start_ticks(pid: int) -> int:
    raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    return int(raw.rsplit(") ", 1)[1].split()[19])


def _portable_exit_status(returncode: int) -> int:
    """Convert Python's negative signal return code to shell status form."""

    return 128 - returncode if returncode < 0 else returncode


def _pipeline_is_active() -> bool:
    completed = subprocess.run(
        ["pgrep", "-af", r"datavis[.]research[.]fresh_pipeline_cli"],
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.returncode == 0 and bool(completed.stdout.strip())


def _write_new_json(path: Path, payload: Mapping[str, Any]) -> None:
    raw = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
    )
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _copy_new(source: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        if destination.is_symlink() or not destination.is_file():
            raise RecoveryControllerError(
                f"existing snapshot is unsafe: {destination}"
            )
        source_size = source.stat().st_size
        if (
            destination.stat().st_size != source_size
            or _sha256_regular(source, expected_size=source_size)
            != _sha256_regular(destination, expected_size=source_size)
        ):
            raise RecoveryControllerError(
                f"existing snapshot differs from durable source: {destination}"
            )
        return
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
    )
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(destination, flags, 0o600)
    with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
        shutil.copyfileobj(reader, writer, HASH_CHUNK_BYTES)
        writer.flush()
        os.fsync(writer.fileno())


def _create_terminal_archive(output: Path, target: Path) -> Mapping[str, Any]:
    partial = Path(f"{target}.partial")
    if any(path.exists() or path.is_symlink() for path in (partial, target)):
        raise RecoveryControllerError("terminal archive path is not new")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
    )
    descriptor = os.open(partial, flags, 0o600)
    with os.fdopen(descriptor, "wb") as writer:
        with tarfile.open(fileobj=writer, mode="w:gz") as bundle:
            for child in sorted(output.iterdir(), key=lambda item: item.name):
                if child.is_symlink() or not child.is_file():
                    raise RecoveryControllerError(
                        f"terminal output is not flat: {child}"
                    )
                bundle.add(child, arcname=child.name, recursive=False)
        writer.flush()
        os.fsync(writer.fileno())
    os.link(partial, target)
    partial.unlink()
    _fsync_directory(target.parent)
    metadata = target.stat()
    return {
        "size": metadata.st_size,
        "sha256": _sha256_regular(target, expected_size=metadata.st_size),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
    }


def _receipt_payload(
    *,
    kind: str,
    status: str,
    arguments: argparse.Namespace,
    controller_sha: str,
    controller_start_ticks: int,
    worktree: Path | None,
    output: Path | None,
    scratch: Path | None,
    recovery: Path | None,
    log_path: Path | None,
    pipeline_pid: int | None,
    pipeline_start_ticks: int | None,
    process_exit_status: int | None,
    archive: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema": "fresh-xauusd-v5-recovery-detached-receipt/v1",
        "kind": kind,
        "status": status,
        "processExitStatus": process_exit_status,
        "githubRunId": arguments.run_id,
        "githubRunAttempt": arguments.run_attempt,
        "branch": arguments.branch,
        "commitSha": arguments.commit_sha,
        "studyLineageSha256": STUDY_LINEAGE_SHA256,
        "recoveryAttemptId": RECOVERY_ATTEMPT_ID,
        "sourceAdoptionRunId": ADOPTION_RUN_ID,
        "sourceAdoptionArtifactId": ADOPTION_ARTIFACT_ID,
        "sourceAdoptionArtifactDigest": ADOPTION_ARTIFACT_DIGEST,
        "sourceTerminalArchiveSha256": TERMINAL_ARCHIVE_SHA256,
        "controllerSha256": controller_sha,
        "controllerPid": os.getpid(),
        "controllerStartTicks": controller_start_ticks,
        "pipelinePid": pipeline_pid,
        "pipelineStartTicks": pipeline_start_ticks,
        "paths": {
            "worktree": str(worktree) if worktree else None,
            "output": str(output) if output else None,
            "scratch": str(scratch) if scratch else None,
            "recovery": str(recovery) if recovery else None,
            "state": str(DURABLE_STATE_ROOT),
            "terminalArchive": str(arguments.terminal_archive),
            "serverLog": str(log_path) if log_path else None,
        },
        "terminalArchive": dict(archive) if archive else None,
    }


def _parse_arguments(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--branch", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--run-attempt", required=True, type=int)
    parser.add_argument("--terminal-input", required=True, type=Path)
    parser.add_argument("--terminal-archive", required=True, type=Path)
    parser.add_argument("--ready-receipt", required=True, type=Path)
    parser.add_argument("--failure-receipt", required=True, type=Path)
    parser.add_argument("--terminal-receipt", required=True, type=Path)
    parser.add_argument("--expected-controller-sha256", required=True)
    return parser.parse_args(arguments)


def run(arguments: Sequence[str]) -> int:
    args = _parse_arguments(arguments)
    if (
        args.branch != BRANCH
        or re.fullmatch(r"[0-9a-f]{40}", args.commit_sha) is None
        or args.run_id <= 0
        or args.run_attempt <= 0
        or re.fullmatch(
            r"[0-9a-f]{64}", args.expected_controller_sha256
        )
        is None
    ):
        raise RecoveryControllerError("controller identity arguments changed")
    controller = Path(__file__).resolve(strict=True)
    controller_sha = _sha256_regular(controller)
    if controller_sha != args.expected_controller_sha256:
        raise RecoveryControllerError("controller digest changed")
    transfer = args.terminal_input.parent.resolve(strict=True)
    if (
        transfer.parent != Path("/tmp")
        or not transfer.name.startswith("fresh-xauusd-v5-recovery-transfer.")
        or args.terminal_input != transfer / TERMINAL_ARCHIVE_NAME
        or args.terminal_archive.parent != ARTIFACT_ROOT
    ):
        raise RecoveryControllerError("controller paths escaped their scope")

    launch_root = _new_directory(LAUNCH_ROOT)
    claims = _new_directory(launch_root / "claims")
    artifact_root = _new_directory(ARTIFACT_ROOT)
    if args.terminal_archive.parent != artifact_root:
        raise RecoveryControllerError("terminal artifact root changed")
    for receipt in (
        args.ready_receipt,
        args.failure_receipt,
        args.terminal_receipt,
    ):
        if (
            receipt.parent != launch_root
            or receipt.exists()
            or receipt.is_symlink()
        ):
            raise RecoveryControllerError("receipt path is not new and scoped")

    lock_descriptor = os.open(
        launch_root,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        if fcntl is None:
            raise RecoveryControllerError("POSIX file locking is unavailable")
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RecoveryControllerError(
                "another sealed research controller holds the global lock"
            ) from error
        if _pipeline_is_active():
            raise RecoveryControllerError("another research pipeline is active")

        controller_start = _process_start_ticks(os.getpid())
        worktree: Path | None = None
        output: Path | None = None
        recovery: Path | None = None
        scratch: Path | None = None
        log_path: Path | None = None
        pipeline_pid: int | None = None
        pipeline_start: int | None = None
        pipeline_status: int | None = None
        process: subprocess.Popen[bytes] | None = None
        log_writer: Any = None
        server_environment: Mapping[str, str] | None = None
        ledger_path: Path | None = None
        ready_written = False
        failure_finalizer_attempted = False
        archive_identity: Mapping[str, Any] | None = None
        terminal_status = 1
        try:
            worktree = _scoped_tmp("fresh-xauusd-v5-recovery-worktree.")
            output = _scoped_tmp("fresh-xauusd-v5-recovery-output.")
            recovery = _scoped_tmp("fresh-xauusd-v5-recovery-input.")
            log_descriptor, raw_log = tempfile.mkstemp(
                prefix="fresh-xauusd-v5-recovery.",
                suffix=".log",
                dir="/tmp",
            )
            os.close(log_descriptor)
            log_path = Path(raw_log).resolve(strict=True)
            safe_extract_recovery_archive(args.terminal_input, recovery)

            state_root = _existing_canonical_directory(DURABLE_STATE_ROOT)
            scratch_root = _new_directory(RECOVERY_SCRATCH_ROOT)
            if scratch_root == state_root or state_root in scratch_root.parents:
                raise RecoveryControllerError("recovery scratch overlaps state")
            scratch = Path(
                tempfile.mkdtemp(
                    prefix=f"attempt.{args.run_id}.{args.run_attempt}.",
                    dir=scratch_root,
                )
            ).resolve(strict=True)
            capacity = os.statvfs(scratch)
            free_bytes = capacity.f_bavail * capacity.f_frsize
            free_inodes = capacity.f_favail
            if (
                free_bytes < MINIMUM_SCRATCH_BYTES
                or free_inodes < MINIMUM_SCRATCH_INODES
            ):
                raise RecoveryControllerError(
                    "recovery scratch capacity is insufficient"
                )

            subprocess.run(
                [
                    "git",
                    "-C",
                    str(PRODUCTION_REPOSITORY),
                    "fetch",
                    "--no-tags",
                    "origin",
                    args.branch,
                ],
                check=True,
            )
            fetched = subprocess.run(
                [
                    "git",
                    "-C",
                    str(PRODUCTION_REPOSITORY),
                    "rev-parse",
                    "FETCH_HEAD",
                ],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            if fetched != args.commit_sha:
                raise RecoveryControllerError(
                    "fetched recovery ref is not the triggering commit"
                )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(PRODUCTION_REPOSITORY),
                    "cat-file",
                    "-e",
                    f"{args.commit_sha}^{{commit}}",
                ],
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(PRODUCTION_REPOSITORY),
                    "worktree",
                    "add",
                    "--detach",
                    str(worktree),
                    args.commit_sha,
                ],
                check=True,
            )
            head = subprocess.run(
                ["git", "-C", str(worktree), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            if head != args.commit_sha:
                raise RecoveryControllerError("detached worktree commit changed")

            venv = worktree / ".fresh-venv"
            subprocess.run(["python3", "-m", "venv", str(venv)], check=True)
            python = venv / "bin/python"
            subprocess.run(
                [
                    str(python),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--no-cache-dir",
                    "numpy==2.0.2",
                    "pandas==2.2.3",
                    "psycopg2-binary==2.9.10",
                    "python-dotenv==1.1.1",
                ],
                cwd=worktree,
                check=True,
            )
            server_environment = _load_server_environment(os.environ)
            preflight_environment = dict(server_environment)
            preflight_environment.update(
                {
                    "FRESH_V5_RECOVERY_ARTIFACT_DIR": str(recovery),
                    "FRESH_REQUIRE_V5_RECOVERY_FIXTURE": "1",
                }
            )
            module_query = (
                "from datavis.research.fresh_recovery_v5 import "
                "V5_RECOVERY_TEST_MODULES; print('\\n'.join("
                "V5_RECOVERY_TEST_MODULES))"
            )
            modules = subprocess.run(
                [str(python), "-B", "-c", module_query],
                cwd=worktree,
                env=preflight_environment,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.splitlines()
            if not modules:
                raise RecoveryControllerError("recovery preflight set is empty")
            subprocess.run(
                [str(python), "-m", "unittest", *modules],
                cwd=worktree,
                env=preflight_environment,
                check=True,
            )
            subprocess.run(
                [
                    str(python),
                    "-B",
                    "-c",
                    (
                        "import sys; from pathlib import Path; "
                        "from datavis.research.fresh_recovery_v5 import "
                        "load_fresh_v5_recovery_bundle; "
                        "load_fresh_v5_recovery_bundle(Path(sys.argv[1]))"
                    ),
                    str(recovery),
                ],
                cwd=worktree,
                env=preflight_environment,
                check=True,
            )
            if _pipeline_is_active():
                raise RecoveryControllerError(
                    "a pipeline appeared during recovery preflight"
                )

            ledger_path, _ = validate_live_state_before_claim(
                recovery,
                durable_state_root=state_root,
            )
            claim = claims / (
                f"v5-recovery-{STUDY_LINEAGE_SHA256}-"
                f"{RECOVERY_ATTEMPT_ID}.claim"
            )
            write_execution_claim(
                claim,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
                commit_sha=args.commit_sha,
            )

            command = pipeline_command(
                python,
                repository=worktree,
                output=output,
                scratch=scratch,
                state=state_root,
                recovery=recovery,
            )
            log_writer = log_path.open("ab")
            process = subprocess.Popen(
                command,
                cwd=worktree,
                env=server_environment,
                stdin=subprocess.DEVNULL,
                stdout=log_writer,
                stderr=subprocess.STDOUT,
            )
            pipeline_pid = process.pid
            pipeline_start = _process_start_ticks(process.pid)
            time.sleep(5)
            early_status = process.poll()
            if early_status is not None:
                pipeline_status = _portable_exit_status(early_status)
                raise RecoveryControllerError(
                    "recovery pipeline did not survive launch grace"
                )
            ready_payload = _receipt_payload(
                kind="launch_ready",
                status="running",
                arguments=args,
                controller_sha=controller_sha,
                controller_start_ticks=controller_start,
                worktree=worktree,
                output=output,
                scratch=scratch,
                recovery=recovery,
                log_path=log_path,
                pipeline_pid=pipeline_pid,
                pipeline_start_ticks=pipeline_start,
                process_exit_status=None,
                archive=None,
            )
            _write_new_json(args.ready_receipt, ready_payload)
            ready_written = True
            pipeline_status = _portable_exit_status(process.wait())
            log_writer.close()
            if pipeline_status != 0:
                failure_finalizer_attempted = True
                invoke_failure_finalizer(
                    python,
                    ledger_path,
                    exit_status=pipeline_status,
                    cwd=worktree,
                    environment=server_environment,
                    log_path=log_path,
                )
            terminal_status = pipeline_status
        except BaseException:
            if process is not None:
                if pipeline_status is None:
                    pipeline_status = _portable_exit_status(process.wait())
                if log_writer is not None and not log_writer.closed:
                    log_writer.close()
                terminal_status = pipeline_status
                if (
                    pipeline_status != 0
                    and not failure_finalizer_attempted
                    and ledger_path is not None
                    and server_environment is not None
                    and worktree is not None
                    and log_path is not None
                ):
                    failure_finalizer_attempted = True
                    invoke_failure_finalizer(
                        worktree / ".fresh-venv/bin/python",
                        ledger_path,
                        exit_status=pipeline_status,
                        cwd=worktree,
                        environment=server_environment,
                        log_path=log_path,
                    )
            raise
        finally:
            if output is not None and output.is_dir():
                if log_path is not None and log_path.is_file():
                    _copy_new(log_path, output / "server-run.log")
                if V5_LEDGER_PATH.is_file() and not V5_LEDGER_PATH.is_symlink():
                    _copy_new(
                        V5_LEDGER_PATH,
                        output / "fresh_experiment_ledger_v1.jsonl",
                    )
                if (
                    HOLDOUT_REGISTRY_PATH.is_file()
                    and not HOLDOUT_REGISTRY_PATH.is_symlink()
                ):
                    _copy_new(
                        HOLDOUT_REGISTRY_PATH,
                        output / "fresh_holdout_authorization_v1.json",
                    )
                exit_path = output / "remote-exit-status.txt"
                if not exit_path.exists() and not exit_path.is_symlink():
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
                    descriptor = os.open(exit_path, flags, 0o600)
                    with os.fdopen(descriptor, "w", encoding="ascii") as handle:
                        handle.write(f"{terminal_status}\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                archive_identity = _create_terminal_archive(
                    output, args.terminal_archive
                )
            terminal_payload = _receipt_payload(
                kind="terminal",
                status="succeeded" if terminal_status == 0 else "failed",
                arguments=args,
                controller_sha=controller_sha,
                controller_start_ticks=controller_start,
                worktree=worktree,
                output=output,
                scratch=scratch,
                recovery=recovery,
                log_path=log_path,
                pipeline_pid=pipeline_pid,
                pipeline_start_ticks=pipeline_start,
                process_exit_status=terminal_status,
                archive=archive_identity,
            )
            if not args.terminal_receipt.exists():
                _write_new_json(args.terminal_receipt, terminal_payload)
            if (
                not ready_written
                and not args.failure_receipt.exists()
                and not args.failure_receipt.is_symlink()
            ):
                failure_payload = dict(terminal_payload)
                failure_payload["kind"] = "failure"
                _write_new_json(args.failure_receipt, failure_payload)
        return terminal_status
    finally:
        os.close(lock_descriptor)


def main(arguments: Sequence[str]) -> int:
    try:
        return run(arguments)
    except BaseException as error:
        print(f"v5-recovery-controller-error: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
