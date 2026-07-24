"""Emit an outcome-blind metadata snapshot for one detached v5 process tree.

This file is streamed to ``python3 -B -`` over SSH.  It deliberately has no
filesystem write operations and never opens a research output, state, spool,
log, receipt, or archive file.  Only procfs process identity/resource fields
and filesystem metadata for preregistered paths are inspected.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time


SCHEMA = "fresh-xauusd-v5-runtime-metadata-snapshot/v1"
REPOSITORY = "8abak/ctrade-openapi-client"
EXPECTED_RUN_ID = 30067832187
EXPECTED_ATTEMPT = 1
EXPECTED_COMMIT = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
EXPECTED_BRANCH = "codex/xauusd-fresh-walkforward"
EXPECTED_LINEAGE = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)
EXPECTED_CONTROLLER_SHA = (
    "da57bce0f90890a8712edbb8cb9830054bfc5b2b3d544c2363420836b8b9ce3f"
)
EXPECTED_LAUNCH_RECEIPT_SHA = (
    "c6e32cbbdaaa2b9d343eee2a2fc399804976a6493ad4f99776b5ad795c5c54a4"
)
EXPECTED_CONTROLLER_PID = 486270
EXPECTED_CONTROLLER_START_TICKS = 1069712502
EXPECTED_PIPELINE_PID = 486543
EXPECTED_PIPELINE_START_TICKS = 1069715129
STATE_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
)
LAUNCH_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-launch-v1"
)
SCRATCH_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-scratch-v1"
)
ARTIFACT_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-artifacts-v1"
)
TARGET_MODULE = "datavis.research.fresh_pipeline_cli"
EXPECTED_PATHS = {
    "worktree": PurePosixPath("/tmp/fresh-xauusd-worktree.E4Jrbc"),
    "output": PurePosixPath("/tmp/fresh-xauusd-output.eVrX3i"),
    "scratch": PurePosixPath(
        "/home/ec2-user/.local/state/datavis/"
        "fresh-xauusd-scratch-v1/run.30067832187.1.QrG3VH"
    ),
    "restart": PurePosixPath("/tmp/fresh-xauusd-restart.KJ622R"),
    "transfer": PurePosixPath("/tmp/fresh-xauusd-transfer.SEMXw4"),
    "terminalArchive": PurePosixPath(
        "/home/ec2-user/.local/state/datavis/"
        "fresh-xauusd-artifacts-v1/fresh-xauusd-30067832187-1.tgz"
    ),
    "serverLog": PurePosixPath("/tmp/fresh-xauusd-run.c5EeE6.log"),
}


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def exact_int(value: str, label: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        fail(f"{label} is not a positive decimal integer")
    return int(value)


def canonical_path(value: str, label: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or str(path) != value
        or "\x00" in value
        or "\r" in value
        or "\n" in value
    ):
        fail(f"{label} is not a canonical absolute path")
    return path


def validate_scoped_paths(values: list[str]) -> dict[str, PurePosixPath]:
    names = (
        "worktree",
        "output",
        "scratch",
        "restart",
        "transfer",
        "terminalArchive",
        "serverLog",
    )
    if len(values) != len(names):
        fail("sealed path vector has the wrong length")
    paths = {
        name: canonical_path(value, name)
        for name, value in zip(names, values)
    }
    tmp_patterns = {
        "worktree": r"fresh-xauusd-worktree\.[A-Za-z0-9]{6}",
        "output": r"fresh-xauusd-output\.[A-Za-z0-9]{6}",
        "restart": r"fresh-xauusd-restart\.[A-Za-z0-9]{6}",
        "transfer": r"fresh-xauusd-transfer\.[A-Za-z0-9]{6}",
        "serverLog": r"fresh-xauusd-run\.[A-Za-z0-9]{6}\.log",
    }
    for name, pattern in tmp_patterns.items():
        path = paths[name]
        if (
            path.parent != PurePosixPath("/tmp")
            or re.fullmatch(pattern, path.name) is None
        ):
            fail(f"{name} escaped its preregistered scope")
    if (
        paths["scratch"].parent != SCRATCH_ROOT
        or re.fullmatch(
            rf"run\.{EXPECTED_RUN_ID}\.{EXPECTED_ATTEMPT}\."
            r"[A-Za-z0-9]{6}",
            paths["scratch"].name,
        )
        is None
    ):
        fail("scratch escaped its preregistered scope")
    expected_archive = (
        ARTIFACT_ROOT
        / f"fresh-xauusd-{EXPECTED_RUN_ID}-{EXPECTED_ATTEMPT}.tgz"
    )
    if paths["terminalArchive"] != expected_archive:
        fail("terminal archive path changed")
    if len(set(paths.values())) != len(paths):
        fail("receipt paths are not distinct")
    if paths != EXPECTED_PATHS:
        fail("receipt-bound runtime paths changed")
    return paths


def read_proc_text(path: Path, maximum_bytes: int) -> str:
    """Read a bounded procfs metadata pseudo-file."""

    with path.open("rb", buffering=0) as handle:
        data = handle.read(maximum_bytes + 1)
    if len(data) > maximum_bytes:
        fail(f"procfs metadata exceeded {maximum_bytes} bytes")
    return data.decode("ascii")


def process_snapshot(
    pid: int,
    expected_start_ticks: int,
    expected_arguments: list[str],
    role: str,
) -> dict[str, object] | None:
    process = Path("/proc") / str(pid)
    try:
        raw_stat = read_proc_text(process / "stat", 16 * 1024)
    except FileNotFoundError:
        return None
    fields = raw_stat.rsplit(") ", 1)
    if len(fields) != 2:
        fail(f"{role} proc stat is malformed")
    values = fields[1].split()
    if len(values) < 22:
        fail(f"{role} proc stat is incomplete")
    start_ticks = int(values[19])
    if start_ticks != expected_start_ticks:
        fail(f"{role} PID was reused or its identity changed")
    if values[0] in {"X", "Z"}:
        return None

    try:
        raw_command = (process / "cmdline").read_bytes()
    except FileNotFoundError:
        return None
    if len(raw_command) > 64 * 1024:
        fail(f"{role} command line is unexpectedly large")
    arguments = [
        os.fsdecode(item) for item in raw_command.split(b"\0") if item
    ]
    if arguments != expected_arguments:
        fail(f"{role} command line changed")

    status: dict[str, object] = {}
    try:
        raw_status = read_proc_text(process / "status", 256 * 1024)
        raw_io = read_proc_text(process / "io", 64 * 1024)
        final_stat = read_proc_text(process / "stat", 16 * 1024)
    except FileNotFoundError:
        return None
    final_fields = final_stat.rsplit(") ", 1)
    if len(final_fields) != 2:
        fail(f"{role} final proc stat is malformed")
    final_values = final_fields[1].split()
    if (
        len(final_values) < 22
        or int(final_values[19]) != expected_start_ticks
    ):
        fail(f"{role} identity changed while it was sampled")
    if final_values[0] in {"X", "Z"}:
        return None
    for line in raw_status.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        if key in {"Uid", "Gid"}:
            status[key] = [int(item) for item in value.split()]
        elif key in {"Threads", "VmRSS", "VmSwap"}:
            status[key] = int(value.split()[0])
    uid = status.get("Uid")
    gid = status.get("Gid")
    if (
        not isinstance(uid, list)
        or uid[:2] != [os.getuid(), os.geteuid()]
        or not isinstance(gid, list)
        or gid[:2] != [os.getgid(), os.getegid()]
    ):
        fail(f"{role} ownership changed")

    io_values: dict[str, int] = {}
    for line in raw_io.splitlines():
        key, separator, value = line.partition(":")
        if separator and key in {"read_bytes", "write_bytes"}:
            io_values[key] = int(value.strip())
    if set(io_values) != {"read_bytes", "write_bytes"}:
        fail(f"{role} proc IO metadata is incomplete")

    return {
        "pid": pid,
        "state": values[0],
        "parentPid": int(values[1]),
        "processGroup": int(values[2]),
        "sessionId": int(values[3]),
        "cpuTicks": int(values[11]) + int(values[12]),
        "startTicks": start_ticks,
        "threads": int(status.get("Threads", 0)),
        "vmRssKiB": int(status.get("VmRSS", 0)),
        "vmSwapKiB": int(status.get("VmSwap", 0)),
        "readBytes": io_values["read_bytes"],
        "writeBytes": io_values["write_bytes"],
        "commandSha256": hashlib.sha256(raw_command).hexdigest(),
        "commandVerified": True,
        "ownershipVerified": True,
    }


def metadata(
    path: PurePosixPath,
    expected_kind: str,
    *,
    required: bool,
) -> dict[str, object]:
    concrete = Path(str(path))
    try:
        item = concrete.lstat()
    except FileNotFoundError:
        if required:
            fail(f"required {expected_kind} is absent: {path}")
        return {"exists": False}
    if stat.S_ISLNK(item.st_mode):
        fail(f"symbolic link refused: {path}")
    if expected_kind == "directory":
        kind_matches = stat.S_ISDIR(item.st_mode)
    elif expected_kind == "regular":
        kind_matches = stat.S_ISREG(item.st_mode)
    else:
        raise AssertionError(expected_kind)
    if not kind_matches:
        fail(f"unexpected filesystem object type: {path}")
    try:
        resolved = concrete.resolve(strict=True)
    except OSError as error:
        fail(f"unable to resolve metadata path {path}: {error}")
    if str(resolved) != str(path):
        fail(f"metadata path contains a symbolic-link component: {path}")
    return {
        "exists": True,
        "kind": expected_kind,
        "device": item.st_dev,
        "inode": item.st_ino,
        "uid": item.st_uid,
        "gid": item.st_gid,
        "mode": stat.S_IMODE(item.st_mode),
        "size": item.st_size,
        "mtimeNs": item.st_mtime_ns,
    }


def volume_metadata(path: PurePosixPath) -> dict[str, int]:
    usage = os.statvfs(str(path))
    return {
        "bytesAvailable": usage.f_bavail * usage.f_frsize,
        "bytesTotal": usage.f_blocks * usage.f_frsize,
        "inodesAvailable": usage.f_favail,
        "inodesTotal": usage.f_files,
    }


def expected_commands(
    paths: dict[str, PurePosixPath],
    controller_sha: str,
) -> tuple[list[str], list[str]]:
    receipt_prefix = LAUNCH_ROOT / (
        f"v5-{EXPECTED_RUN_ID}-{EXPECTED_ATTEMPT}"
    )
    controller = paths["transfer"] / "fresh-xauusd-v5-controller.sh"
    controller_arguments = [
        "bash",
        "--noprofile",
        "--norc",
        str(controller),
        EXPECTED_BRANCH,
        EXPECTED_COMMIT,
        str(EXPECTED_RUN_ID),
        str(EXPECTED_ATTEMPT),
        str(paths["transfer"] / "run19-restart.tgz"),
        str(paths["terminalArchive"]),
        controller_sha,
        f"{receipt_prefix}.ready.json",
        f"{receipt_prefix}.failure.json",
        f"{receipt_prefix}.terminal.json",
    ]
    pipeline_arguments = [
        str(paths["worktree"] / ".fresh-venv/bin/python"),
        "-m",
        TARGET_MODULE,
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
    ]
    return controller_arguments, pipeline_arguments


def classify(
    controller: dict[str, object] | None,
    pipeline: dict[str, object] | None,
    terminal_exists: bool,
) -> str:
    if terminal_exists:
        if pipeline is not None:
            fail("terminal receipt exists while the pipeline identity is active")
        return "terminal_metadata_present"
    if controller is None:
        fail("controller disappeared without a terminal receipt")
    if pipeline is None:
        return "finalizing_metadata_only"
    if (
        pipeline["parentPid"] != controller["pid"]
        or controller["processGroup"] != controller["pid"]
        or controller["sessionId"] != controller["pid"]
        or pipeline["processGroup"] != controller["pid"]
        or pipeline["sessionId"] != controller["pid"]
    ):
        fail("detached process-tree relationship changed")
    return "running"


def process_pair(
    controller_pid: int,
    controller_start: int,
    pipeline_pid: int,
    pipeline_start: int,
    controller_arguments: list[str],
    pipeline_arguments: list[str],
    terminal_receipt: PurePosixPath,
) -> tuple[str, dict[str, object] | None, dict[str, object] | None]:
    terminal_before = metadata(
        terminal_receipt, "regular", required=False
    )
    controller = process_snapshot(
        controller_pid,
        controller_start,
        controller_arguments,
        "controller",
    )
    pipeline = process_snapshot(
        pipeline_pid,
        pipeline_start,
        pipeline_arguments,
        "pipeline",
    )
    terminal_after = metadata(
        terminal_receipt, "regular", required=False
    )
    state = classify(
        controller,
        pipeline,
        bool(terminal_before["exists"] or terminal_after["exists"]),
    )
    return state, controller, pipeline


def delta(
    first: dict[str, object] | None,
    second: dict[str, object] | None,
) -> dict[str, int] | None:
    if first is None or second is None:
        return None
    if (
        first["pid"] != second["pid"]
        or first["startTicks"] != second["startTicks"]
    ):
        fail("process identity changed during metadata sampling")
    return {
        "cpuTicks": int(second["cpuTicks"]) - int(first["cpuTicks"]),
        "readBytes": int(second["readBytes"]) - int(first["readBytes"]),
        "writeBytes": int(second["writeBytes"]) - int(first["writeBytes"]),
    }


def main() -> None:
    if len(sys.argv) != 19:
        fail("expected exactly 18 sealed monitor arguments")
    (
        run_id_value,
        attempt_value,
        commit_sha,
        branch,
        lineage_sha,
        controller_sha,
        launch_receipt_sha,
        controller_pid_value,
        controller_start_value,
        pipeline_pid_value,
        pipeline_start_value,
        *path_values,
    ) = sys.argv[1:]
    run_id = exact_int(run_id_value, "run id")
    attempt = exact_int(attempt_value, "run attempt")
    controller_pid = exact_int(controller_pid_value, "controller PID")
    controller_start = exact_int(
        controller_start_value, "controller start ticks"
    )
    pipeline_pid = exact_int(pipeline_pid_value, "pipeline PID")
    pipeline_start = exact_int(pipeline_start_value, "pipeline start ticks")
    if (
        run_id != EXPECTED_RUN_ID
        or attempt != EXPECTED_ATTEMPT
        or commit_sha != EXPECTED_COMMIT
        or branch != EXPECTED_BRANCH
        or lineage_sha != EXPECTED_LINEAGE
        or controller_sha != EXPECTED_CONTROLLER_SHA
        or launch_receipt_sha != EXPECTED_LAUNCH_RECEIPT_SHA
        or controller_pid != EXPECTED_CONTROLLER_PID
        or controller_start != EXPECTED_CONTROLLER_START_TICKS
        or pipeline_pid != EXPECTED_PIPELINE_PID
        or pipeline_start != EXPECTED_PIPELINE_START_TICKS
    ):
        fail("sealed launch identity changed")
    paths = validate_scoped_paths(path_values)
    controller_arguments, pipeline_arguments = expected_commands(
        paths, controller_sha
    )
    receipt_prefix = LAUNCH_ROOT / f"v5-{run_id}-{attempt}"
    ready_receipt = PurePosixPath(f"{receipt_prefix}.ready.json")
    failure_receipt = PurePosixPath(f"{receipt_prefix}.failure.json")
    terminal_receipt = PurePosixPath(f"{receipt_prefix}.terminal.json")
    claim = (
        LAUNCH_ROOT
        / "claims"
        / f"v5-{lineage_sha}.claim"
    )

    ready = metadata(ready_receipt, "regular", required=True)
    failure = metadata(failure_receipt, "regular", required=False)
    if failure["exists"]:
        fail("unexpected post-ready failure receipt exists")
    claim_data = metadata(claim, "regular", required=True)

    first_monotonic = time.monotonic()
    first_state, first_controller, first_pipeline = process_pair(
        controller_pid,
        controller_start,
        pipeline_pid,
        pipeline_start,
        controller_arguments,
        pipeline_arguments,
        terminal_receipt,
    )
    time.sleep(3)
    second_state, second_controller, second_pipeline = process_pair(
        controller_pid,
        controller_start,
        pipeline_pid,
        pipeline_start,
        controller_arguments,
        pipeline_arguments,
        terminal_receipt,
    )
    elapsed = time.monotonic() - first_monotonic
    allowed_transitions = {
        "running": {
            "running",
            "finalizing_metadata_only",
            "terminal_metadata_present",
        },
        "finalizing_metadata_only": {
            "finalizing_metadata_only",
            "terminal_metadata_present",
        },
        "terminal_metadata_present": {"terminal_metadata_present"},
    }
    if second_state not in allowed_transitions[first_state]:
        fail("runtime lifecycle regressed during metadata sampling")

    terminal = metadata(terminal_receipt, "regular", required=False)
    terminal_archive = metadata(
        paths["terminalArchive"],
        "regular",
        required=bool(terminal["exists"]),
    )
    partial_archive = metadata(
        PurePosixPath(f"{paths['terminalArchive']}.partial"),
        "regular",
        required=False,
    )
    if terminal["exists"] and partial_archive["exists"]:
        fail("terminal and partial archives coexist")
    if second_state == "running" and (
        terminal_archive["exists"] or partial_archive["exists"]
    ):
        fail("archive metadata appeared while pipeline remained active")

    objects = {
        "worktree": metadata(paths["worktree"], "directory", required=True),
        "output": metadata(paths["output"], "directory", required=True),
        "scratch": metadata(paths["scratch"], "directory", required=True),
        "restart": metadata(paths["restart"], "directory", required=True),
        "transfer": metadata(paths["transfer"], "directory", required=True),
        "stateRoot": metadata(STATE_ROOT, "directory", required=True),
        "launchRoot": metadata(LAUNCH_ROOT, "directory", required=True),
        "claimRoot": metadata(
            LAUNCH_ROOT / "claims", "directory", required=True
        ),
        "artifactRoot": metadata(
            ARTIFACT_ROOT, "directory", required=True
        ),
        "serverLog": metadata(paths["serverLog"], "regular", required=True),
        "readyReceipt": ready,
        "failureReceipt": failure,
        "terminalReceipt": terminal,
        "terminalArchive": terminal_archive,
        "partialTerminalArchive": partial_archive,
        "executionClaim": claim_data,
    }
    volumes = {
        "output": volume_metadata(paths["output"]),
        "scratch": volume_metadata(paths["scratch"]),
        "state": volume_metadata(STATE_ROOT),
        "artifacts": volume_metadata(ARTIFACT_ROOT),
    }
    snapshot = {
        "schema": SCHEMA,
        "scope": "process-and-filesystem-metadata-only",
        "repository": REPOSITORY,
        "sourceLaunch": {
            "runId": run_id,
            "runAttempt": attempt,
            "commitSha": commit_sha,
            "branch": branch,
            "studyLineageSha256": lineage_sha,
            "controllerSha256": controller_sha,
            "launchReceiptSha256": launch_receipt_sha,
        },
        "capturedUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "sampleSeconds": elapsed,
        "initialLifecycle": first_state,
        "lifecycle": second_state,
        "processes": {
            "controller": second_controller,
            "pipeline": second_pipeline,
            "controllerDelta": delta(first_controller, second_controller),
            "pipelineDelta": delta(first_pipeline, second_pipeline),
        },
        "filesystem": {
            "objects": objects,
            "volumes": volumes,
        },
        "outcomeFilesOpened": False,
        "researchFileContentsRead": False,
        "remoteFilesystemWritesAttempted": False,
    }
    json.dump(snapshot, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
