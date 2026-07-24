"""Verify and seal the detached v5 monitor's runner-side evidence.

The source launch artifact is selected by an exact GitHub run identity and
verified before its sole JSON receipt is extracted.  The remote snapshot is
then reduced to an allow-listed metadata schema before a new immutable monitor
receipt is written.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from typing import Any, Mapping
import zipfile


REPOSITORY = "8abak/ctrade-openapi-client"
SOURCE_RUN_ID = 30067832187
SOURCE_ATTEMPT = 1
SOURCE_COMMIT = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
SOURCE_BRANCH = "codex/xauusd-fresh-walkforward"
SOURCE_WORKFLOW = ".github/workflows/fresh-xauusd-v5-detached-launch.yml"
SOURCE_ARTIFACT_NAME = (
    "fresh-xauusd-detached-launch-30067832187-1"
)
SOURCE_ARTIFACT_ID = 8586881858
SOURCE_ARTIFACT_SIZE = 801
SOURCE_ARTIFACT_DIGEST = (
    "sha256:86f6a8b06e0fde6a5223099a1eda4a9ce6e2f6fdd6248dbef91bbc4395936e1e"
)
SOURCE_RECEIPT_NAME = "fresh-xauusd-v5-launch-receipt.json"
SOURCE_RECEIPT_SHA256 = (
    "c6e32cbbdaaa2b9d343eee2a2fc399804976a6493ad4f99776b5ad795c5c54a4"
)
STUDY_LINEAGE_SHA256 = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)
RUN19_ARTIFACT_ID = 8585919266
RUN19_ARCHIVE_SHA256 = (
    "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
)
CONTROLLER_SHA256 = (
    "da57bce0f90890a8712edbb8cb9830054bfc5b2b3d544c2363420836b8b9ce3f"
)
REMOTE_SNAPSHOT_SCHEMA = (
    "fresh-xauusd-v5-runtime-metadata-snapshot/v1"
)
MONITOR_RECEIPT_SCHEMA = "fresh-xauusd-read-only-monitor-receipt/v1"
PLAN_SCHEMA = "fresh-xauusd-launch-artifact-selection/v1"
MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_RECEIPT_BYTES = 64 * 1024
MAX_SNAPSHOT_BYTES = 128 * 1024


def object_pairs(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def read_json(path: Path, maximum_bytes: int) -> Mapping[str, Any]:
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum_bytes:
        raise ValueError(f"{path.name} is not a bounded regular file")
    raw = path.read_bytes()
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=object_pairs)
    if not isinstance(value, Mapping):
        raise ValueError(f"{path.name} must contain one JSON object")
    return value


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def write_exclusive(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def require_exact_keys(
    value: Mapping[str, Any],
    keys: set[str],
    label: str,
) -> None:
    if set(value) != keys:
        raise ValueError(f"{label} keys changed")


def positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} is not a positive integer")
    return value


def nonnegative_int(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} is not a nonnegative integer")
    return value


def sha256_file(path: Path, maximum_bytes: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > maximum_bytes:
                raise ValueError(f"{path.name} exceeds its size limit")
            digest.update(chunk)
    return digest.hexdigest(), size


def validate_source_run(run: Mapping[str, Any]) -> None:
    repository = run.get("repository")
    head_repository = run.get("head_repository")
    if (
        run.get("id") != SOURCE_RUN_ID
        or run.get("run_attempt") != SOURCE_ATTEMPT
        or run.get("head_sha") != SOURCE_COMMIT
        or run.get("head_branch") != SOURCE_BRANCH
        or run.get("event") != "push"
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or run.get("path") != SOURCE_WORKFLOW
        or not isinstance(repository, Mapping)
        or repository.get("full_name") != REPOSITORY
        or not isinstance(head_repository, Mapping)
        or head_repository.get("full_name") != REPOSITORY
    ):
        raise ValueError("source launch workflow identity changed")


def source_plan(
    run: Mapping[str, Any],
    listing: Mapping[str, Any],
) -> dict[str, Any]:
    validate_source_run(run)
    artifacts = listing.get("artifacts")
    if (
        listing.get("total_count") != 1
        or not isinstance(artifacts, list)
    ):
        raise ValueError("artifact listing is malformed")
    matches = [
        item
        for item in artifacts
        if isinstance(item, Mapping)
        and item.get("name") == SOURCE_ARTIFACT_NAME
    ]
    if len(matches) != 1:
        raise ValueError("expected exactly one immutable launch artifact")
    artifact = matches[0]
    artifact_id = positive_int(artifact.get("id"), "artifact id")
    size = positive_int(artifact.get("size_in_bytes"), "artifact size")
    digest = artifact.get("digest")
    workflow_run = artifact.get("workflow_run")
    expected_url = (
        f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/"
        f"{artifact_id}/zip"
    )
    if (
        artifact_id != SOURCE_ARTIFACT_ID
        or size != SOURCE_ARTIFACT_SIZE
        or size > MAX_ARTIFACT_BYTES
        or artifact.get("expired") is not False
        or digest != SOURCE_ARTIFACT_DIGEST
        or artifact.get("archive_download_url") != expected_url
        or not isinstance(workflow_run, Mapping)
        or workflow_run.get("id") != SOURCE_RUN_ID
        or workflow_run.get("head_sha") != SOURCE_COMMIT
        or workflow_run.get("head_branch") != SOURCE_BRANCH
    ):
        raise ValueError("immutable launch artifact metadata changed")
    return {
        "schema": PLAN_SCHEMA,
        "repository": REPOSITORY,
        "sourceRunId": SOURCE_RUN_ID,
        "sourceRunAttempt": SOURCE_ATTEMPT,
        "sourceCommitSha": SOURCE_COMMIT,
        "sourceBranch": SOURCE_BRANCH,
        "sourceWorkflow": SOURCE_WORKFLOW,
        "artifactId": artifact_id,
        "artifactName": SOURCE_ARTIFACT_NAME,
        "artifactDigest": digest,
        "artifactSize": size,
        "archiveDownloadUrl": expected_url,
    }


def validate_plan(plan: Mapping[str, Any]) -> None:
    require_exact_keys(
        plan,
        {
            "schema",
            "repository",
            "sourceRunId",
            "sourceRunAttempt",
            "sourceCommitSha",
            "sourceBranch",
            "sourceWorkflow",
            "artifactId",
            "artifactName",
            "artifactDigest",
            "artifactSize",
            "archiveDownloadUrl",
        },
        "source plan",
    )
    artifact_id = positive_int(plan.get("artifactId"), "artifact id")
    artifact_size = positive_int(plan.get("artifactSize"), "artifact size")
    digest = plan.get("artifactDigest")
    expected_url = (
        f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/"
        f"{artifact_id}/zip"
    )
    if (
        plan.get("schema") != PLAN_SCHEMA
        or plan.get("repository") != REPOSITORY
        or plan.get("sourceRunId") != SOURCE_RUN_ID
        or plan.get("sourceRunAttempt") != SOURCE_ATTEMPT
        or plan.get("sourceCommitSha") != SOURCE_COMMIT
        or plan.get("sourceBranch") != SOURCE_BRANCH
        or plan.get("sourceWorkflow") != SOURCE_WORKFLOW
        or plan.get("artifactName") != SOURCE_ARTIFACT_NAME
        or artifact_id != SOURCE_ARTIFACT_ID
        or artifact_size != SOURCE_ARTIFACT_SIZE
        or artifact_size > MAX_ARTIFACT_BYTES
        or digest != SOURCE_ARTIFACT_DIGEST
        or plan.get("archiveDownloadUrl") != expected_url
    ):
        raise ValueError("source artifact plan identity changed")


def scoped_paths(payload: Mapping[str, Any]) -> dict[str, str]:
    paths = payload.get("paths")
    if not isinstance(paths, Mapping):
        raise ValueError("launch receipt paths are missing")
    names = {
        "worktree",
        "output",
        "scratch",
        "restart",
        "transfer",
        "terminalArchive",
        "serverLog",
    }
    require_exact_keys(paths, names, "launch receipt paths")
    parsed: dict[str, PurePosixPath] = {}
    for name in names:
        value = paths.get(name)
        if not isinstance(value, str):
            raise ValueError(f"launch receipt path {name} changed")
        item = PurePosixPath(value)
        if (
            not item.is_absolute()
            or ".." in item.parts
            or "." in item.parts
            or str(item) != value
            or any(character in value for character in "\x00\r\n")
        ):
            raise ValueError(f"launch receipt path {name} is not canonical")
        parsed[name] = item
    tmp_patterns = {
        "worktree": r"fresh-xauusd-worktree\.[A-Za-z0-9]{6}",
        "output": r"fresh-xauusd-output\.[A-Za-z0-9]{6}",
        "restart": r"fresh-xauusd-restart\.[A-Za-z0-9]{6}",
        "transfer": r"fresh-xauusd-transfer\.[A-Za-z0-9]{6}",
        "serverLog": r"fresh-xauusd-run\.[A-Za-z0-9]{6}\.log",
    }
    for name, pattern in tmp_patterns.items():
        if (
            parsed[name].parent != PurePosixPath("/tmp")
            or re.fullmatch(pattern, parsed[name].name) is None
        ):
            raise ValueError(f"launch receipt path {name} escaped scope")
    scratch_root = PurePosixPath(
        "/home/ec2-user/.local/state/datavis/fresh-xauusd-scratch-v1"
    )
    if (
        parsed["scratch"].parent != scratch_root
        or re.fullmatch(
            rf"run\.{SOURCE_RUN_ID}\.{SOURCE_ATTEMPT}\."
            r"[A-Za-z0-9]{6}",
            parsed["scratch"].name,
        )
        is None
    ):
        raise ValueError("launch receipt scratch path changed")
    expected_archive = PurePosixPath(
        "/home/ec2-user/.local/state/datavis/"
        f"fresh-xauusd-artifacts-v1/fresh-xauusd-{SOURCE_RUN_ID}-"
        f"{SOURCE_ATTEMPT}.tgz"
    )
    if parsed["terminalArchive"] != expected_archive:
        raise ValueError("launch receipt terminal archive path changed")
    if len(set(parsed.values())) != len(parsed):
        raise ValueError("launch receipt paths are not distinct")
    return {name: str(parsed[name]) for name in names}


def validate_launch_receipt(payload: Mapping[str, Any]) -> dict[str, str]:
    require_exact_keys(
        payload,
        {
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
        },
        "launch receipt",
    )
    if (
        payload.get("schema")
        != "fresh-xauusd-detached-research-receipt/v1"
        or payload.get("kind") != "launch_ready"
        or payload.get("status") != "running"
        or payload.get("processExitStatus") is not None
        or payload.get("githubRunId") != SOURCE_RUN_ID
        or payload.get("githubRunAttempt") != SOURCE_ATTEMPT
        or payload.get("branch") != SOURCE_BRANCH
        or payload.get("commitSha") != SOURCE_COMMIT
        or payload.get("studyLineageSha256") != STUDY_LINEAGE_SHA256
        or payload.get("run19ArtifactId") != RUN19_ARTIFACT_ID
        or payload.get("run19TerminalArchiveSha256")
        != RUN19_ARCHIVE_SHA256
        or payload.get("controllerSha256") != CONTROLLER_SHA256
        or payload.get("terminalArchive") is not None
    ):
        raise ValueError("detached launch receipt identity changed")
    positive_int(payload.get("controllerPid"), "controller PID")
    positive_int(
        payload.get("controllerStartTicks"), "controller start ticks"
    )
    positive_int(payload.get("pipelinePid"), "pipeline PID")
    positive_int(
        payload.get("pipelineStartTicks"), "pipeline start ticks"
    )
    return scoped_paths(payload)


def verify_launch_artifact(
    plan: Mapping[str, Any],
    archive: Path,
) -> bytes:
    validate_plan(plan)
    archive_digest, archive_size = sha256_file(
        archive, MAX_ARTIFACT_BYTES
    )
    if (
        f"sha256:{archive_digest}" != plan["artifactDigest"]
        or archive_size != plan["artifactSize"]
    ):
        raise ValueError("downloaded launch artifact identity changed")
    with zipfile.ZipFile(archive, "r") as bundle:
        members = bundle.infolist()
        if len(members) != 1:
            raise ValueError("launch artifact member set changed")
        member = members[0]
        unix_mode = member.external_attr >> 16
        if (
            member.filename != SOURCE_RECEIPT_NAME
            or member.is_dir()
            or member.flag_bits & 0x1
            or member.file_size > MAX_RECEIPT_BYTES
            or member.compress_size > MAX_RECEIPT_BYTES
            or (
                unix_mode
                and not stat.S_ISREG(unix_mode)
            )
        ):
            raise ValueError("launch artifact receipt member changed")
        raw = bundle.read(member)
    payload = json.loads(raw.decode("utf-8"), object_pairs_hook=object_pairs)
    if not isinstance(payload, Mapping):
        raise ValueError("launch receipt is not a JSON object")
    validate_launch_receipt(payload)
    if raw != canonical_bytes(payload):
        raise ValueError("launch receipt is not canonical")
    if hashlib.sha256(raw).hexdigest() != SOURCE_RECEIPT_SHA256:
        raise ValueError("launch receipt byte identity changed")
    return raw


def validate_datetime(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"{label} is malformed")
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} has no timezone")
    return value


PROCESS_KEYS = {
    "pid",
    "state",
    "parentPid",
    "processGroup",
    "sessionId",
    "cpuTicks",
    "startTicks",
    "threads",
    "vmRssKiB",
    "vmSwapKiB",
    "readBytes",
    "writeBytes",
    "commandSha256",
    "commandVerified",
    "ownershipVerified",
}


def validate_process(
    value: Any,
    label: str,
    expected_pid: int,
    expected_start: int,
) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} process metadata is malformed")
    require_exact_keys(value, PROCESS_KEYS, f"{label} process metadata")
    for key in {
        "pid",
        "parentPid",
        "processGroup",
        "sessionId",
        "cpuTicks",
        "startTicks",
        "threads",
        "vmRssKiB",
        "vmSwapKiB",
        "readBytes",
        "writeBytes",
    }:
        nonnegative_int(value.get(key), f"{label} {key}")
    if (
        value.get("pid") != expected_pid
        or value.get("startTicks") != expected_start
        or value.get("state") not in {"R", "S", "D", "T", "t", "I", "W"}
        or not isinstance(value.get("commandSha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", value["commandSha256"]) is None
        or value.get("commandVerified") is not True
        or value.get("ownershipVerified") is not True
    ):
        raise ValueError(f"{label} sealed process identity changed")
    return value


def validate_delta(value: Any, label: str) -> Mapping[str, int] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} delta is malformed")
    require_exact_keys(
        value, {"cpuTicks", "readBytes", "writeBytes"}, f"{label} delta"
    )
    for key, item in value.items():
        nonnegative_int(item, f"{label} delta {key}")
    return value


OBJECT_KEYS = {
    "worktree",
    "output",
    "scratch",
    "restart",
    "transfer",
    "stateRoot",
    "launchRoot",
    "claimRoot",
    "artifactRoot",
    "serverLog",
    "readyReceipt",
    "failureReceipt",
    "terminalReceipt",
    "terminalArchive",
    "partialTerminalArchive",
    "executionClaim",
}
DIRECTORY_OBJECTS = {
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
OPTIONAL_OBJECTS = {
    "failureReceipt",
    "terminalReceipt",
    "terminalArchive",
    "partialTerminalArchive",
}


def validate_object(
    value: Any,
    name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"filesystem object {name} is malformed")
    if value == {"exists": False}:
        if name not in OPTIONAL_OBJECTS:
            raise ValueError(f"required filesystem object {name} is absent")
        return value
    require_exact_keys(
        value,
        {
            "exists",
            "kind",
            "device",
            "inode",
            "uid",
            "gid",
            "mode",
            "size",
            "mtimeNs",
        },
        f"filesystem object {name}",
    )
    expected_kind = "directory" if name in DIRECTORY_OBJECTS else "regular"
    if value.get("exists") is not True or value.get("kind") != expected_kind:
        raise ValueError(f"filesystem object {name} type changed")
    for key in {"device", "inode", "uid", "gid", "mode", "size", "mtimeNs"}:
        nonnegative_int(value.get(key), f"filesystem object {name} {key}")
    if value["mode"] > 0o7777:
        raise ValueError(f"filesystem object {name} mode changed")
    return value


def validate_volume(value: Any, name: str) -> Mapping[str, int]:
    if not isinstance(value, Mapping):
        raise ValueError(f"volume metadata {name} is malformed")
    require_exact_keys(
        value,
        {
            "bytesAvailable",
            "bytesTotal",
            "inodesAvailable",
            "inodesTotal",
        },
        f"volume metadata {name}",
    )
    for key, item in value.items():
        nonnegative_int(item, f"volume metadata {name} {key}")
    if (
        value["bytesAvailable"] > value["bytesTotal"]
        or value["inodesAvailable"] > value["inodesTotal"]
    ):
        raise ValueError(f"volume metadata {name} is inconsistent")
    return value


def validate_remote_snapshot(
    snapshot: Mapping[str, Any],
    launch_receipt: Mapping[str, Any],
    launch_receipt_sha: str,
) -> Mapping[str, Any]:
    require_exact_keys(
        snapshot,
        {
            "schema",
            "scope",
            "repository",
            "sourceLaunch",
            "capturedUtc",
            "sampleSeconds",
            "initialLifecycle",
            "lifecycle",
            "processes",
            "filesystem",
            "outcomeFilesOpened",
            "researchFileContentsRead",
            "remoteFilesystemWritesAttempted",
        },
        "remote metadata snapshot",
    )
    source = snapshot.get("sourceLaunch")
    if not isinstance(source, Mapping):
        raise ValueError("remote source identity is malformed")
    require_exact_keys(
        source,
        {
            "runId",
            "runAttempt",
            "commitSha",
            "branch",
            "studyLineageSha256",
            "controllerSha256",
            "launchReceiptSha256",
        },
        "remote source identity",
    )
    if (
        snapshot.get("schema") != REMOTE_SNAPSHOT_SCHEMA
        or snapshot.get("scope")
        != "process-and-filesystem-metadata-only"
        or snapshot.get("repository") != REPOSITORY
        or source
        != {
            "runId": SOURCE_RUN_ID,
            "runAttempt": SOURCE_ATTEMPT,
            "commitSha": SOURCE_COMMIT,
            "branch": SOURCE_BRANCH,
            "studyLineageSha256": STUDY_LINEAGE_SHA256,
            "controllerSha256": CONTROLLER_SHA256,
            "launchReceiptSha256": launch_receipt_sha,
        }
        or snapshot.get("outcomeFilesOpened") is not False
        or snapshot.get("researchFileContentsRead") is not False
        or snapshot.get("remoteFilesystemWritesAttempted") is not False
    ):
        raise ValueError("remote metadata scope or source identity changed")
    validate_datetime(snapshot.get("capturedUtc"), "snapshot timestamp")
    sample_seconds = snapshot.get("sampleSeconds")
    if (
        not isinstance(sample_seconds, (int, float))
        or isinstance(sample_seconds, bool)
        or not 2.0 <= sample_seconds <= 30.0
    ):
        raise ValueError("remote sample duration changed")
    states = {
        "running",
        "finalizing_metadata_only",
        "terminal_metadata_present",
    }
    initial = snapshot.get("initialLifecycle")
    lifecycle = snapshot.get("lifecycle")
    if initial not in states or lifecycle not in states:
        raise ValueError("remote lifecycle is invalid")
    allowed = {
        "running": states,
        "finalizing_metadata_only": {
            "finalizing_metadata_only",
            "terminal_metadata_present",
        },
        "terminal_metadata_present": {"terminal_metadata_present"},
    }
    if lifecycle not in allowed[initial]:
        raise ValueError("remote lifecycle regressed")

    processes = snapshot.get("processes")
    if not isinstance(processes, Mapping):
        raise ValueError("remote process metadata is malformed")
    require_exact_keys(
        processes,
        {
            "controller",
            "pipeline",
            "controllerDelta",
            "pipelineDelta",
        },
        "remote process metadata",
    )
    controller = validate_process(
        processes.get("controller"),
        "controller",
        launch_receipt["controllerPid"],
        launch_receipt["controllerStartTicks"],
    )
    pipeline = validate_process(
        processes.get("pipeline"),
        "pipeline",
        launch_receipt["pipelinePid"],
        launch_receipt["pipelineStartTicks"],
    )
    controller_delta = validate_delta(
        processes.get("controllerDelta"), "controller"
    )
    pipeline_delta = validate_delta(
        processes.get("pipelineDelta"), "pipeline"
    )
    if lifecycle == "running" and (
        controller is None
        or pipeline is None
        or controller_delta is None
        or pipeline_delta is None
    ):
        raise ValueError("running process metadata is incomplete")
    if lifecycle == "finalizing_metadata_only" and (
        controller is None or pipeline is not None
    ):
        raise ValueError("finalizing process metadata is inconsistent")
    if lifecycle == "terminal_metadata_present" and pipeline is not None:
        raise ValueError("terminal process metadata is inconsistent")

    filesystem = snapshot.get("filesystem")
    if not isinstance(filesystem, Mapping):
        raise ValueError("filesystem metadata is malformed")
    require_exact_keys(
        filesystem, {"objects", "volumes"}, "filesystem metadata"
    )
    objects = filesystem.get("objects")
    volumes = filesystem.get("volumes")
    if not isinstance(objects, Mapping) or not isinstance(volumes, Mapping):
        raise ValueError("filesystem metadata sections are malformed")
    require_exact_keys(objects, OBJECT_KEYS, "filesystem objects")
    require_exact_keys(
        volumes, {"output", "scratch", "state", "artifacts"}, "volumes"
    )
    checked_objects = {
        name: validate_object(value, name)
        for name, value in objects.items()
    }
    for name, value in volumes.items():
        validate_volume(value, name)
    if checked_objects["failureReceipt"]["exists"]:
        raise ValueError("unexpected failure receipt metadata exists")
    terminal_exists = checked_objects["terminalReceipt"]["exists"]
    archive_exists = checked_objects["terminalArchive"]["exists"]
    partial_exists = checked_objects["partialTerminalArchive"]["exists"]
    if lifecycle == "terminal_metadata_present":
        if not terminal_exists or not archive_exists or partial_exists:
            raise ValueError("terminal filesystem metadata is inconsistent")
    elif terminal_exists:
        raise ValueError("terminal receipt exists before terminal lifecycle")
    if lifecycle == "running" and (archive_exists or partial_exists):
        raise ValueError("archive metadata exists during running lifecycle")
    return snapshot


def command_resolve(args: argparse.Namespace) -> None:
    run = read_json(Path(args.run_json), MAX_RECEIPT_BYTES * 8)
    artifacts = read_json(
        Path(args.artifacts_json), MAX_RECEIPT_BYTES * 32
    )
    plan = source_plan(run, artifacts)
    sys.stdout.buffer.write(canonical_bytes(plan))


def command_verify_launch(args: argparse.Namespace) -> None:
    plan = read_json(Path(args.plan), MAX_RECEIPT_BYTES)
    raw = verify_launch_artifact(plan, Path(args.archive))
    write_exclusive(Path(args.receipt_output), raw)


def command_remote_args(args: argparse.Namespace) -> None:
    receipt_path = Path(args.receipt)
    receipt = read_json(receipt_path, MAX_RECEIPT_BYTES)
    paths = validate_launch_receipt(receipt)
    digest, _ = sha256_file(receipt_path, MAX_RECEIPT_BYTES)
    if digest != SOURCE_RECEIPT_SHA256:
        raise ValueError("launch receipt byte identity changed")
    values = (
        str(SOURCE_RUN_ID),
        str(SOURCE_ATTEMPT),
        SOURCE_COMMIT,
        SOURCE_BRANCH,
        STUDY_LINEAGE_SHA256,
        CONTROLLER_SHA256,
        digest,
        str(receipt["controllerPid"]),
        str(receipt["controllerStartTicks"]),
        str(receipt["pipelinePid"]),
        str(receipt["pipelineStartTicks"]),
        paths["worktree"],
        paths["output"],
        paths["scratch"],
        paths["restart"],
        paths["transfer"],
        paths["terminalArchive"],
        paths["serverLog"],
    )
    for value in values:
        if any(character in value for character in "\x00\r\n"):
            raise ValueError("remote argument contains a line separator")
        print(value)


def command_seal(args: argparse.Namespace) -> None:
    plan = read_json(Path(args.plan), MAX_RECEIPT_BYTES)
    validate_plan(plan)
    receipt_path = Path(args.launch_receipt)
    launch_receipt = read_json(receipt_path, MAX_RECEIPT_BYTES)
    validate_launch_receipt(launch_receipt)
    launch_receipt_sha, _ = sha256_file(
        receipt_path, MAX_RECEIPT_BYTES
    )
    if launch_receipt_sha != SOURCE_RECEIPT_SHA256:
        raise ValueError("launch receipt byte identity changed")
    snapshot_path = Path(args.snapshot)
    snapshot = read_json(snapshot_path, MAX_SNAPSHOT_BYTES)
    validate_remote_snapshot(
        snapshot, launch_receipt, launch_receipt_sha
    )
    snapshot_raw = snapshot_path.read_bytes()
    if snapshot_raw != canonical_bytes(snapshot):
        raise ValueError("remote metadata snapshot is not canonical")
    snapshot_sha = hashlib.sha256(snapshot_raw).hexdigest()
    monitor_run_id = positive_int(
        int(args.monitor_run_id), "monitor run id"
    )
    monitor_attempt = positive_int(
        int(args.monitor_attempt), "monitor run attempt"
    )
    if (
        re.fullmatch(r"[0-9a-f]{40}", args.monitor_commit) is None
        or args.monitor_branch != SOURCE_BRANCH
    ):
        raise ValueError("monitor workflow identity changed")
    artifact_digest = plan["artifactDigest"]
    receipt = {
        "schema": MONITOR_RECEIPT_SCHEMA,
        "scope": "process-and-filesystem-metadata-only",
        "sourceLaunch": {
            "repository": REPOSITORY,
            "runId": SOURCE_RUN_ID,
            "runAttempt": SOURCE_ATTEMPT,
            "commitSha": SOURCE_COMMIT,
            "branch": SOURCE_BRANCH,
            "artifactId": plan["artifactId"],
            "artifactName": SOURCE_ARTIFACT_NAME,
            "artifactDigest": artifact_digest,
            "artifactZipSha256": artifact_digest.removeprefix("sha256:"),
            "launchReceiptSha256": launch_receipt_sha,
            "studyLineageSha256": STUDY_LINEAGE_SHA256,
            "controllerSha256": CONTROLLER_SHA256,
        },
        "monitorWorkflow": {
            "runId": monitor_run_id,
            "runAttempt": monitor_attempt,
            "commitSha": args.monitor_commit,
            "branch": args.monitor_branch,
        },
        "observation": snapshot,
        "remoteSnapshotSha256": snapshot_sha,
        "outcomeFilesAccessed": False,
        "researchFileContentsRead": False,
        "remoteMutationAttempted": False,
    }
    write_exclusive(Path(args.output), canonical_bytes(receipt))


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    subparsers = result.add_subparsers(dest="command", required=True)

    resolve = subparsers.add_parser("resolve-source")
    resolve.add_argument("--run-json", required=True)
    resolve.add_argument("--artifacts-json", required=True)
    resolve.set_defaults(function=command_resolve)

    verify = subparsers.add_parser("verify-launch")
    verify.add_argument("--plan", required=True)
    verify.add_argument("--archive", required=True)
    verify.add_argument("--receipt-output", required=True)
    verify.set_defaults(function=command_verify_launch)

    remote = subparsers.add_parser("remote-args")
    remote.add_argument("--receipt", required=True)
    remote.set_defaults(function=command_remote_args)

    seal = subparsers.add_parser("seal")
    seal.add_argument("--plan", required=True)
    seal.add_argument("--launch-receipt", required=True)
    seal.add_argument("--snapshot", required=True)
    seal.add_argument("--monitor-run-id", required=True)
    seal.add_argument("--monitor-attempt", required=True)
    seal.add_argument("--monitor-commit", required=True)
    seal.add_argument("--monitor-branch", required=True)
    seal.add_argument("--output", required=True)
    seal.set_defaults(function=command_seal)
    return result


def main() -> None:
    arguments = parser().parse_args()
    arguments.function(arguments)


if __name__ == "__main__":
    main()
