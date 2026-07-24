"""Pre-outcome scientific gate for the sole detached V5 recovery.

This module intentionally does not know a recovery adoption run or artifact
identifier.  The transport layer supplies an already-adopted, flat terminal
output directory plus the exact Git commit that the recovery launch receipt
names.  The audit then:

* verifies every inherited V5 input byte that authorized the continuation;
* binds the recovery implementation manifest to Git *blob* bytes from that
  exact commit, independent of the manifest's ephemeral remote worktree path;
* materializes those verified blobs in an isolated temporary source tree and
  invokes that commit's own recovery-for-holdout validator; and
* verifies that any holdout authorization embeds the identical recovery proof.

No P&L, winning strategy, or final scientific conclusion is interpreted here.
That remains the responsibility of a separately sealed terminal scientific
auditor after transport provenance and the normal chronological ledger gates
have also passed.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import importlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Mapping, Sequence


AUDIT_SCHEMA = "fresh-xauusd-v5-recovery-scientific-gate-audit/v1"
IMPLEMENTATION_MANIFEST_SCHEMA = "fresh-xauusd-implementation-manifest/v1"
RECOVERY_CONTRACT_SCHEMA = "fresh-xauusd-v5-recovery-contract/v1"
RECOVERY_PROOF_SCHEMA = "fresh-xauusd-v5-holdout-recovery-proof/v1"
RECOVERY_ATTEMPT_ID = "v5-discovery-recovery-attempt-1"
RECOVERY_EQUIVALENCE_SCHEMA = (
    "fresh-xauusd-v5-recovery-equivalence-preflight/v1"
)
STUDY_ID = "xauusd-fresh-causal-acceleration-v5"
STUDY_LINEAGE_SHA256 = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)
SCIENTIFIC_SPECIFICATION_SHA256 = (
    "fef6b1a4898aaeb4ce33ad96ea270f0211448357399d94f76051b01c9dabcbd8"
)
ORIGINAL_RUN_ID = 30_067_832_187
ORIGINAL_RUN_ATTEMPT = 1
ORIGINAL_COMMIT_SHA = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
ORIGINAL_LAUNCH_ARTIFACT_ID = 8_586_881_858
ORIGINAL_ADOPTION_RUN_ID = 30_101_048_443
ORIGINAL_ADOPTION_RUN_ATTEMPT = 1
ORIGINAL_ADOPTION_JOB_ID = 89_506_876_763
ORIGINAL_ADOPTION_COMMIT_SHA = (
    "c730fd0a2c66426f995ac43f1d50035cf94265ff"
)
ORIGINAL_ADOPTION_ARTIFACT_ID = 8_608_015_979
ORIGINAL_ADOPTION_ARTIFACT_NAME = (
    "fresh-xauusd-v5-terminal-adopted-30101048443-1"
)
ORIGINAL_ADOPTION_ARTIFACT_SIZE = 127_602
ORIGINAL_ADOPTION_ARTIFACT_DIGEST = (
    "sha256:6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3"
)
ORIGINAL_TERMINAL_ARCHIVE_NAME = "fresh-xauusd-30067832187-1.tgz"
ORIGINAL_TERMINAL_ARCHIVE_SIZE = 125_470
ORIGINAL_TERMINAL_ARCHIVE_SHA256 = (
    "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d"
)
ORIGINAL_LEDGER_SHA256 = (
    "e95e1739987cdb56315adcbb98b2e85198cb14a1d536a07282214d2ef359744d"
)
ORIGINAL_LEDGER_RECORD_SHA256 = (
    "83b8e201bab95195526f3580c98e2f4494331df3ecc44c1586cba72ef4f95cb3",
    "f300211bd30a73842539bc8c2365c3eb3fcbd8e7216a968bd97f76dff4f151f1",
)
PREREGISTRATION_SHA256 = (
    "ef72f00de02a144ab67dd75012a711473bcd47824cd5ee787b07268a92b11c8c"
)
ORIGINAL_IMPLEMENTATION_MANIFEST_SHA256 = (
    "aadafdecc92cd4b1e3e1757a74c805bd6b119c8767d2a741ba4ef946bf645748"
)
SPLIT_MANIFEST_SHA256 = (
    "59a0df375a3b8934c14a355a4fc91bb9aade6ada88052d5096c4b9a29e2744bd"
)
HOLDOUT_WINDOW_SHA256 = (
    "8d599150987e32430a5d012b4973590bda56f7d548c42e7dad9714e2f0fe40b7"
)
ENTRY_BANK_FILE_SHA256 = (
    "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
)
ORDERED_CANDIDATE_SEQUENCE_SHA256 = (
    "d4163395adb43ec49a5f0e10df1fcc82bb698703d2462d735eed5b7ed40ba19c"
)

RECOVERY_TEST_MODULES = (
    "test_fresh_numeric_spool",
    "test_fresh_pipeline",
    "test_fresh_pipeline_v5",
    "test_fresh_preregistration",
    "test_fresh_preregistration_v5",
    "test_fresh_recovery_v5",
    "test_fresh_scoring",
    "test_fresh_search",
    "test_fresh_spool",
    "test_fresh_v5_recovery_orchestration",
)

RECOVERY_IMPLEMENTATION_FILES = (
    ".github/research-launch.txt",
    ".github/research-v5-recovery-launch.txt",
    ".github/scripts/fresh-xauusd-v5-recovery-controller.py",
    ".github/scripts/fresh-xauusd-v5-terminal-audit-input.py",
    ".github/ssh/fresh-xauusd-ec2-known-hosts",
    ".github/workflows/fresh-xauusd-research.yml",
    ".github/workflows/fresh-xauusd-v5-recovery-detached-launch.yml",
    "datavis/db.py",
    "datavis/research/__init__.py",
    "datavis/research/fresh_bootstrap.py",
    "datavis/research/fresh_candidate_grid.py",
    "datavis/research/fresh_data.py",
    "datavis/research/fresh_db_source.py",
    "datavis/research/fresh_decisions.py",
    "datavis/research/fresh_entry_diagnostics.py",
    "datavis/research/fresh_event_filters.py",
    "datavis/research/fresh_exit_grid.py",
    "datavis/research/fresh_exits.py",
    "datavis/research/fresh_feature_bank.py",
    "datavis/research/fresh_features.py",
    "datavis/research/fresh_inventory.py",
    "datavis/research/fresh_numeric_spool.py",
    "datavis/research/fresh_pipeline.py",
    "datavis/research/fresh_pipeline_cli.py",
    "datavis/research/fresh_preregistration.py",
    "datavis/research/fresh_protocol.py",
    "datavis/research/fresh_recovery_v5.py",
    "datavis/research/fresh_replay.py",
    "datavis/research/fresh_restart.py",
    "datavis/research/fresh_restart_v4.py",
    "datavis/research/fresh_restart_v5.py",
    "datavis/research/fresh_scoring.py",
    "datavis/research/fresh_search.py",
    "datavis/research/fresh_session_eval.py",
    "datavis/research/fresh_sessions.py",
    "datavis/research/fresh_signals.py",
    "datavis/research/fresh_spool.py",
    "datavis/research/fresh_thresholds.py",
    "datavis/research/ticks.py",
    "test_fresh_numeric_spool.py",
    "test_fresh_pipeline.py",
    "test_fresh_pipeline_v5.py",
    "test_fresh_preregistration.py",
    "test_fresh_preregistration_v5.py",
    "test_fresh_recovery_v5.py",
    "test_fresh_scoring.py",
    "test_fresh_search.py",
    "test_fresh_spool.py",
    "test_fresh_v5_recovery_orchestration.py",
)

ORIGINAL_V5_OUTPUT_SHA256 = {
    "fresh_corpus_manifest_v1.json": (
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2"
    ),
    "fresh_entry_bank_v1.json": ENTRY_BANK_FILE_SHA256,
    "original_v5_fresh_experiment_ledger_v1.jsonl": ORIGINAL_LEDGER_SHA256,
    "fresh_implementation_manifest_v1.json": (
        "45c240012263986409add7d9f478a4e8990d7403bd3ed38b4fbd403b8f15ea23"
    ),
    "fresh_preregistration_v5.json": (
        "06c25b8733de70b75f7ae07b136a3bfecba5bd264f0ecdaa1b153db6d0f190a6"
    ),
    "fresh_quantile_bank_v1.json": (
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46"
    ),
    "fresh_research_state_binding_v4.json": (
        "696408161ef88e94436ec1713960bc5f5ecb0c4394ea06e15643e10ed0f60567"
    ),
    "fresh_source_inventory_v1.json": (
        "ab1125638e76cd35517859b4e292abb3908a49d79fb92534c5fc2fd7a32e9ab8"
    ),
    "fresh_split_manifest_v2.json": (
        "b179c8e359b0ab998258a1bbbdac41e33b970d63e60131f3057f2dc224c1a0dc"
    ),
    "fresh_threshold_domain_preflight_v1.json": (
        "55d94106f9860676f9d42be8c7023de2bd7d7234ee812155fedd578eae6d98dc"
    ),
    "predecessor_fresh_experiment_ledger_v1.jsonl": (
        "ac627bd986c044b12049f717eb3fc664321c08c169fd6a829a5fc8d51144c7b4"
    ),
    "predecessor_fresh_implementation_manifest_v1.json": (
        "d04bd2279c31922fc753b313f61b140a124c2fc7625227a5a0b9de29377ca1ee"
    ),
    "predecessor_fresh_preregistration_v4.json": (
        "fd203eed1ff5b1f407b6179b2fd18546106420a1d3ba50b7acddc65e090e0e87"
    ),
    "predecessor_fresh_research_state_binding_v3.json": (
        "62eacb704989a640478ab8a3d05a20cc91a0d69a3797d12dac330c9b3c606cee"
    ),
    "original_v5_remote-exit-status.txt": (
        "e3b9c2844b5a5c2677b3a2279db2ec8487491dd9a23d6b22fac153391b3bb63c"
    ),
    "original_v5_server-run.log": (
        "e99d19a11fea31762b6e49e85d4b24ca16a57dbd2be16862bce365ab6a9227d2"
    ),
}

MANDATORY_OUTPUT_MEMBERS = frozenset(
    {
        *ORIGINAL_V5_OUTPUT_SHA256,
        "fresh_recovery_implementation_manifest_v1.json",
        "fresh_recovery_contract_v1.json",
        "fresh_recovery_discovery_batch_v1.json",
        "fresh_experiment_ledger_v1.jsonl",
        "fresh_run_summary_v1.json",
        "server-run.log",
        "remote-exit-status.txt",
    }
)
CONDITIONAL_OUTPUT_MEMBERS = frozenset(
    {
        "fresh_exit_bank_v1.json",
        "fresh_final_strategy_frozen_v1.json",
        "fresh_holdout_authorization_v1.json",
    }
)
ALLOWED_OUTPUT_MEMBERS = MANDATORY_OUTPUT_MEMBERS | CONDITIONAL_OUTPUT_MEMBERS

MAX_SMALL_JSON_BYTES = 16 * 1024**2
MAX_LEDGER_BYTES = 64 * 1024**2
MAX_BATCH_BYTES = 256 * 1024**2
MAX_LOG_BYTES = 32 * 1024**2
MAX_GIT_BLOB_BYTES = 16 * 1024**2
MAX_SOURCE_CLOSURE_BYTES = 256 * 1024**2
MAX_LEDGER_RECORDS = 4096
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}\Z")
REMOTE_WORKTREE_PATTERN = re.compile(
    r"/tmp/fresh-xauusd-v5-recovery-worktree\.[A-Za-z0-9_-]+\Z"
)


class FreshRecoveryTerminalAuditError(RuntimeError):
    """Raised when recovery provenance or scientific proof is inconsistent."""


@dataclass(frozen=True, slots=True)
class VerifiedRecoveryLaunchSource:
    commit_sha: str
    recorded_remote_repository_root: str
    implementation_manifest_sha256: str
    file_sha256: Mapping[str, str]
    closure_sha256: str
    total_bytes: int


def _canonical_hash(payload: Any) -> str:
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise FreshRecoveryTerminalAuditError(
            "payload is not finite canonical JSON"
        ) from error
    return hashlib.sha256(encoded).hexdigest()


def _strict_object(raw: bytes, label: str) -> dict[str, Any]:
    def pairs(values: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda item: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON value: {item}")
            ),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise FreshRecoveryTerminalAuditError(
            f"{label} is not strict JSON"
        ) from error
    if not isinstance(value, dict):
        raise FreshRecoveryTerminalAuditError(
            f"{label} is not a JSON object"
        )
    return value


def _stable_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _open_regular(path: Path, *, maximum_bytes: int, label: str) -> tuple[int, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise FreshRecoveryTerminalAuditError(
            f"{label} is unavailable"
        ) from error
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size < 0
        or metadata.st_size > maximum_bytes
    ):
        os.close(descriptor)
        raise FreshRecoveryTerminalAuditError(
            f"{label} is not a bounded regular file"
        )
    return descriptor, metadata


def _read_regular(path: Path, *, maximum_bytes: int, label: str) -> bytes:
    descriptor, before = _open_regular(
        path,
        maximum_bytes=maximum_bytes,
        label=label,
    )
    chunks: list[bytes] = []
    total = 0
    try:
        while True:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, maximum_bytes + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise FreshRecoveryTerminalAuditError(
                    f"{label} exceeds its read bound"
                )
        after = os.fstat(descriptor)
        if (
            _stable_identity(before) != _stable_identity(after)
            or total != before.st_size
        ):
            raise FreshRecoveryTerminalAuditError(
                f"{label} changed while being read"
            )
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _sha256_regular(path: Path, *, maximum_bytes: int, label: str) -> str:
    descriptor, before = _open_regular(
        path,
        maximum_bytes=maximum_bytes,
        label=label,
    )
    digest = hashlib.sha256()
    total = 0
    try:
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
        if (
            _stable_identity(before) != _stable_identity(after)
            or total != before.st_size
        ):
            raise FreshRecoveryTerminalAuditError(
                f"{label} changed while being hashed"
            )
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _read_json(path: Path, *, maximum_bytes: int, label: str) -> dict[str, Any]:
    raw = _read_regular(path, maximum_bytes=maximum_bytes, label=label)
    return _strict_object(raw, label)


def _verified_ledger(path: Path) -> tuple[dict[str, Any], ...]:
    raw = _read_regular(
        path,
        maximum_bytes=MAX_LEDGER_BYTES,
        label="recovery experiment ledger",
    )
    if not raw or not raw.endswith(b"\n"):
        raise FreshRecoveryTerminalAuditError(
            "recovery experiment ledger is not newline terminated"
        )
    records: list[dict[str, Any]] = []
    for number, line in enumerate(raw.splitlines(), start=1):
        if number > MAX_LEDGER_RECORDS or not line:
            raise FreshRecoveryTerminalAuditError(
                "recovery experiment ledger has an invalid record count"
            )
        record = _strict_object(line, f"ledger record {number}")
        body = copy.deepcopy(record)
        claimed_number = body.pop("recordNumber", None)
        claimed_sha = body.pop("recordSha256", None)
        if (
            claimed_number != number
            or not isinstance(claimed_sha, str)
            or SHA256_PATTERN.fullmatch(claimed_sha) is None
            or _canonical_hash(body) != claimed_sha
        ):
            raise FreshRecoveryTerminalAuditError(
                f"ledger record {number} has an invalid chain identity"
            )
        records.append(record)
    if len(records) < 248:
        raise FreshRecoveryTerminalAuditError(
            "the completed recovery ledger is too short"
        )
    if tuple(
        str(record.get("recordSha256")) for record in records[:2]
    ) != ORIGINAL_LEDGER_RECORD_SHA256:
        raise FreshRecoveryTerminalAuditError(
            "the original V5 ledger prefix changed"
        )
    return tuple(records)


def _run_git(
    root: Path,
    arguments: Sequence[str],
    *,
    maximum_bytes: int = 1024 * 1024,
) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=False,
        capture_output=True,
    )
    if (
        completed.returncode != 0
        or len(completed.stdout) > maximum_bytes
        or len(completed.stderr) > maximum_bytes
    ):
        raise FreshRecoveryTerminalAuditError(
            "the exact recovery launch Git object is unavailable"
        )
    return completed.stdout


def _git_blobs(
    root: Path,
    commit_sha: str,
    relative_paths: Sequence[str],
) -> dict[str, bytes]:
    requested = tuple(relative_paths)
    if not requested or len(requested) != len(set(requested)):
        raise FreshRecoveryTerminalAuditError(
            "launch source blob request is empty or duplicated"
        )
    tree = _run_git(
        root,
        ["ls-tree", "-r", "-z", commit_sha, "--", *requested],
        maximum_bytes=1024 * 1024,
    )
    records = [item for item in tree.split(b"\0") if item]
    identities: dict[str, str] = {}
    sizes: dict[str, int] = {}
    for record in records:
        metadata, separator, encoded_path = record.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8")
        except UnicodeDecodeError as error:
            raise FreshRecoveryTerminalAuditError(
                "launch source path is not UTF-8"
            ) from error
        if (
            not separator
            or len(fields) != 3
            or fields[0] not in {b"100644", b"100755"}
            or fields[1] != b"blob"
            or relative not in requested
            or relative in identities
        ):
            raise FreshRecoveryTerminalAuditError(
                f"launch source is not a regular Git blob: {relative}"
            )
        identities[relative] = fields[2].decode("ascii")
    if set(identities) != set(requested):
        raise FreshRecoveryTerminalAuditError(
            "launch commit does not contain the exact requested blob set"
        )

    process = subprocess.Popen(
        ["git", "-C", str(root), "cat-file", "--batch"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if (
        process.stdin is None
        or process.stdout is None
        or process.stderr is None
    ):
        process.kill()
        process.wait()
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        raise FreshRecoveryTerminalAuditError(
            "bounded Git blob reader could not be created"
        )
    try:
        for relative in requested:
            process.stdin.write(f"{commit_sha}:{relative}\n".encode("utf-8"))
        process.stdin.close()
        blobs: dict[str, bytes] = {}
        total = 0
        for relative in requested:
            header = process.stdout.readline(4097)
            if not header.endswith(b"\n") or len(header) > 4096:
                raise FreshRecoveryTerminalAuditError(
                    f"Git blob header is invalid: {relative}"
                )
            fields = header.rstrip(b"\n").split()
            if len(fields) != 3 or fields[1] != b"blob":
                raise FreshRecoveryTerminalAuditError(
                    f"Git object is not a blob: {relative}"
                )
            try:
                object_id = fields[0].decode("ascii")
                size = int(fields[2])
            except (UnicodeDecodeError, ValueError) as error:
                raise FreshRecoveryTerminalAuditError(
                    f"Git blob identity is invalid: {relative}"
                ) from error
            if (
                object_id != identities[relative]
                or not 0 <= size <= MAX_GIT_BLOB_BYTES
            ):
                raise FreshRecoveryTerminalAuditError(
                    f"Git blob size or identity changed: {relative}"
                )
            raw = process.stdout.read(size)
            if len(raw) != size or process.stdout.read(1) != b"\n":
                raise FreshRecoveryTerminalAuditError(
                    f"Git blob stream is truncated: {relative}"
                )
            total += size
            if total > MAX_SOURCE_CLOSURE_BYTES:
                raise FreshRecoveryTerminalAuditError(
                    "recovery source closure exceeds its total bound"
                )
            blobs[relative] = raw
            sizes[relative] = size
        if process.stdout.read(1):
            raise FreshRecoveryTerminalAuditError(
                "Git blob stream has unexpected trailing bytes"
            )
        stderr = process.stderr.read(1024 * 1024 + 1)
        returncode = process.wait()
        if returncode != 0 or len(stderr) > 1024 * 1024:
            raise FreshRecoveryTerminalAuditError(
                "the exact recovery launch Git blobs are unavailable"
            )
    except BaseException:
        if process.poll() is None:
            process.kill()
            process.wait()
        raise
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if not stream.closed:
                stream.close()
    if (
        tuple(blobs) != requested
        or any(len(blobs[path]) != sizes[path] for path in requested)
    ):
        raise FreshRecoveryTerminalAuditError(
            "the exact recovery launch Git blob set changed"
        )
    return blobs


def _manifest_sha256(manifest: Mapping[str, Any]) -> str:
    body = copy.deepcopy(dict(manifest))
    claimed = body.pop("manifestSha256", None)
    if (
        not isinstance(claimed, str)
        or SHA256_PATTERN.fullmatch(claimed) is None
        or set(body) != {"schema", "repositoryRoot", "files"}
        or body.get("schema") != IMPLEMENTATION_MANIFEST_SCHEMA
        or _canonical_hash(body) != claimed
    ):
        raise FreshRecoveryTerminalAuditError(
            "recovery implementation manifest identity is invalid"
        )
    return claimed


def verify_recovery_implementation_manifest_offline(
    manifest: Mapping[str, Any],
    *,
    launch_source_root: str | Path,
    expected_launch_commit_sha: str,
    recorded_remote_repository_root: str,
) -> VerifiedRecoveryLaunchSource:
    """Bind a remote-root manifest to exact Git blobs from the launch commit."""

    if (
        not isinstance(expected_launch_commit_sha, str)
        or COMMIT_PATTERN.fullmatch(expected_launch_commit_sha) is None
    ):
        raise FreshRecoveryTerminalAuditError(
            "recovery launch commit SHA is invalid"
        )
    if (
        not isinstance(recorded_remote_repository_root, str)
        or REMOTE_WORKTREE_PATTERN.fullmatch(
            recorded_remote_repository_root
        )
        is None
        or str(PurePosixPath(recorded_remote_repository_root))
        != recorded_remote_repository_root
    ):
        raise FreshRecoveryTerminalAuditError(
            "recorded recovery worktree path is invalid"
        )
    manifest_sha = _manifest_sha256(manifest)
    if manifest.get("repositoryRoot") != recorded_remote_repository_root:
        raise FreshRecoveryTerminalAuditError(
            "manifest root differs from the launch receipt"
        )
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise FreshRecoveryTerminalAuditError(
            "recovery implementation manifest has no file closure"
        )
    files: dict[str, str] = {}
    ordered_paths: list[str] = []
    for record in raw_files:
        if not isinstance(record, Mapping) or set(record) != {"path", "sha256"}:
            raise FreshRecoveryTerminalAuditError(
                "recovery implementation file record is invalid"
            )
        relative = record.get("path")
        digest = record.get("sha256")
        if (
            not isinstance(relative, str)
            or not isinstance(digest, str)
            or SHA256_PATTERN.fullmatch(digest) is None
            or PurePosixPath(relative).is_absolute()
            or ".." in PurePosixPath(relative).parts
            or str(PurePosixPath(relative)) != relative
            or relative in files
        ):
            raise FreshRecoveryTerminalAuditError(
                "recovery implementation path or digest is invalid"
            )
        ordered_paths.append(relative)
        files[relative] = digest
    if (
        tuple(ordered_paths) != RECOVERY_IMPLEMENTATION_FILES
        or tuple(sorted(files)) != RECOVERY_IMPLEMENTATION_FILES
    ):
        raise FreshRecoveryTerminalAuditError(
            "recovery implementation file closure changed"
        )

    selected = Path(launch_source_root).expanduser()
    if selected.is_symlink():
        raise FreshRecoveryTerminalAuditError(
            "launch source checkout cannot be a symlink"
        )
    root = selected.resolve()
    if not root.is_dir():
        raise FreshRecoveryTerminalAuditError(
            "launch source checkout is unavailable"
        )
    head = _run_git(root, ["rev-parse", "--verify", "HEAD^{commit}"]).decode(
        "ascii"
    ).strip()
    if head != expected_launch_commit_sha:
        raise FreshRecoveryTerminalAuditError(
            "launch source checkout is not the triggering commit"
        )

    actual_files: dict[str, str] = {}
    blobs = _git_blobs(
        root,
        expected_launch_commit_sha,
        RECOVERY_IMPLEMENTATION_FILES,
    )
    total_bytes = sum(len(raw) for raw in blobs.values())
    for relative in RECOVERY_IMPLEMENTATION_FILES:
        raw = blobs[relative]
        actual = hashlib.sha256(raw).hexdigest()
        if files[relative] != actual:
            raise FreshRecoveryTerminalAuditError(
                f"manifest differs from launch Git blob: {relative}"
            )
        actual_files[relative] = actual
    closure = {
        "schema": "fresh-xauusd-v5-recovery-launch-source-closure/v1",
        "commitSha": expected_launch_commit_sha,
        "files": [
            {"path": path, "sha256": actual_files[path]}
            for path in RECOVERY_IMPLEMENTATION_FILES
        ],
    }
    return VerifiedRecoveryLaunchSource(
        commit_sha=expected_launch_commit_sha,
        recorded_remote_repository_root=recorded_remote_repository_root,
        implementation_manifest_sha256=manifest_sha,
        file_sha256=actual_files,
        closure_sha256=_canonical_hash(closure),
        total_bytes=total_bytes,
    )


def _write_exclusive(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _assert_launch_protocol_constants(protocol: Any) -> None:
    expected = {
        "V5_ORIGINAL_GITHUB_RUN_ID": ORIGINAL_RUN_ID,
        "V5_ORIGINAL_GITHUB_RUN_ATTEMPT": ORIGINAL_RUN_ATTEMPT,
        "V5_ORIGINAL_GITHUB_COMMIT_SHA": ORIGINAL_COMMIT_SHA,
        "V5_LAUNCH_ARTIFACT_ID": ORIGINAL_LAUNCH_ARTIFACT_ID,
        "V5_ADOPTION_GITHUB_RUN_ID": ORIGINAL_ADOPTION_RUN_ID,
        "V5_ADOPTION_GITHUB_RUN_ATTEMPT": ORIGINAL_ADOPTION_RUN_ATTEMPT,
        "V5_ADOPTION_GITHUB_JOB_ID": ORIGINAL_ADOPTION_JOB_ID,
        "V5_ADOPTION_GITHUB_COMMIT_SHA": ORIGINAL_ADOPTION_COMMIT_SHA,
        "V5_ADOPTION_ARTIFACT_ID": ORIGINAL_ADOPTION_ARTIFACT_ID,
        "V5_ADOPTION_ARTIFACT_NAME": ORIGINAL_ADOPTION_ARTIFACT_NAME,
        "V5_ADOPTION_ARTIFACT_SIZE": ORIGINAL_ADOPTION_ARTIFACT_SIZE,
        "V5_ADOPTION_ARTIFACT_DIGEST": ORIGINAL_ADOPTION_ARTIFACT_DIGEST,
        "V5_TERMINAL_ARCHIVE_NAME": ORIGINAL_TERMINAL_ARCHIVE_NAME,
        "V5_TERMINAL_ARCHIVE_SIZE": ORIGINAL_TERMINAL_ARCHIVE_SIZE,
        "V5_TERMINAL_ARCHIVE_SHA256": ORIGINAL_TERMINAL_ARCHIVE_SHA256,
        "V5_LEDGER_SHA256": ORIGINAL_LEDGER_SHA256,
        "V5_LEDGER_RECORD_SHA256": ORIGINAL_LEDGER_RECORD_SHA256,
        "V5_PREREGISTRATION_SHA256": PREREGISTRATION_SHA256,
        "V5_IMPLEMENTATION_MANIFEST_SHA256": (
            ORIGINAL_IMPLEMENTATION_MANIFEST_SHA256
        ),
        "V5_SPLIT_MANIFEST_SHA256": SPLIT_MANIFEST_SHA256,
        "V5_HOLDOUT_WINDOW_SHA256": HOLDOUT_WINDOW_SHA256,
        "V5_ENTRY_BANK_FILE_SHA256": ENTRY_BANK_FILE_SHA256,
        "V5_ORDERED_CANDIDATE_SEQUENCE_SHA256": (
            ORDERED_CANDIDATE_SEQUENCE_SHA256
        ),
        "V5_SCIENTIFIC_SPECIFICATION_SHA256": (
            SCIENTIFIC_SPECIFICATION_SHA256
        ),
        "V5_RECOVERY_ATTEMPT_ID": RECOVERY_ATTEMPT_ID,
        "V5_RECOVERY_CONTRACT_SCHEMA": RECOVERY_CONTRACT_SCHEMA,
        "V5_RECOVERY_EQUIVALENCE_SCHEMA": RECOVERY_EQUIVALENCE_SCHEMA,
        "V5_RECOVERY_HOLDOUT_PROOF_SCHEMA": RECOVERY_PROOF_SCHEMA,
        "V5_RECOVERY_TEST_MODULES": RECOVERY_TEST_MODULES,
    }
    for name, value in expected.items():
        if getattr(protocol, name, None) != value:
            raise FreshRecoveryTerminalAuditError(
                f"launch recovery protocol constant changed: {name}"
            )
    if tuple(protocol.required_fresh_v5_recovery_implementation_files()) != (
        RECOVERY_IMPLEMENTATION_FILES
    ):
        raise FreshRecoveryTerminalAuditError(
            "launch recovery protocol source closure changed"
        )


def _invoke_protocol_validator(
    protocol: Any,
    *,
    materialized_root: Path,
    manifest: Mapping[str, Any],
    verified_source: VerifiedRecoveryLaunchSource,
    records: Sequence[Mapping[str, Any]],
    preregistration: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    recovery_contract: Mapping[str, Any],
    sealed_batch_result_path: Path,
) -> dict[str, Any]:
    """Replace only the live-path verifier with the stricter Git-blob seal."""

    original = getattr(
        protocol,
        "_validate_recovery_implementation_manifest",
        None,
    )
    if not callable(original):
        raise FreshRecoveryTerminalAuditError(
            "launch recovery manifest verifier is unavailable"
        )

    def offline_verifier(
        selected_manifest: Mapping[str, Any],
    ) -> tuple[str, Path, Mapping[str, str]]:
        if _canonical_hash(selected_manifest) != _canonical_hash(manifest):
            raise PermissionError(
                "recovery implementation manifest changed during audit"
            )
        return (
            verified_source.implementation_manifest_sha256,
            materialized_root,
            dict(verified_source.file_sha256),
        )

    protocol._validate_recovery_implementation_manifest = offline_verifier
    try:
        proof = protocol.validate_fresh_v5_recovery_for_holdout(
            records=records,
            preregistration=preregistration,
            preregistration_sha256=PREREGISTRATION_SHA256,
            split_manifest=split_manifest,
            split_manifest_sha256=SPLIT_MANIFEST_SHA256,
            recovery_contract=recovery_contract,
            recovery_implementation_manifest=manifest,
            sealed_batch_result_path=sealed_batch_result_path,
        )
    except (PermissionError, TypeError, ValueError) as error:
        raise FreshRecoveryTerminalAuditError(
            "the exact launch recovery validator rejected the evidence"
        ) from error
    finally:
        protocol._validate_recovery_implementation_manifest = original
    if (
        not isinstance(proof, dict)
        or proof.get("schema") != RECOVERY_PROOF_SCHEMA
        or proof.get("candidateOutcomeRecordCount") != 240
        or proof.get("orderedCandidateSequenceSha256")
        != ORDERED_CANDIDATE_SEQUENCE_SHA256
        or proof.get("recoveryImplementationManifestSha256")
        != verified_source.implementation_manifest_sha256
    ):
        raise FreshRecoveryTerminalAuditError(
            "launch recovery validator returned an invalid proof"
        )
    return copy.deepcopy(proof)


def _validate_with_launch_protocol(
    *,
    launch_source_root: Path,
    verified_source: VerifiedRecoveryLaunchSource,
    manifest: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    preregistration: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    recovery_contract: Mapping[str, Any],
    sealed_batch_result_path: Path,
) -> dict[str, Any]:
    preloaded = tuple(
        name for name in sys.modules if name == "datavis" or name.startswith("datavis.")
    )
    if preloaded:
        raise FreshRecoveryTerminalAuditError(
            "datavis modules were imported before launch-source verification"
        )
    with tempfile.TemporaryDirectory(
        prefix="fresh-v5-recovery-audit-source-"
    ) as raw_temporary:
        materialized = Path(raw_temporary).resolve()
        blobs = _git_blobs(
            launch_source_root,
            verified_source.commit_sha,
            RECOVERY_IMPLEMENTATION_FILES,
        )
        for relative in RECOVERY_IMPLEMENTATION_FILES:
            blob = blobs[relative]
            if hashlib.sha256(blob).hexdigest() != (
                verified_source.file_sha256[relative]
            ):
                raise FreshRecoveryTerminalAuditError(
                    "launch source changed before isolated validation"
                )
            _write_exclusive(materialized / PurePosixPath(relative), blob)
        prior_path = list(sys.path)
        sys.path.insert(0, str(materialized))
        imported_names: set[str] = set()
        try:
            protocol = importlib.import_module(
                "datavis.research.fresh_recovery_v5"
            )
            imported_names = {
                name
                for name in sys.modules
                if name == "datavis" or name.startswith("datavis.")
            }
            _assert_launch_protocol_constants(protocol)
            return _invoke_protocol_validator(
                protocol,
                materialized_root=materialized,
                manifest=manifest,
                verified_source=verified_source,
                records=records,
                preregistration=preregistration,
                split_manifest=split_manifest,
                recovery_contract=recovery_contract,
                sealed_batch_result_path=sealed_batch_result_path,
            )
        finally:
            sys.path[:] = prior_path
            for name in sorted(imported_names, reverse=True):
                sys.modules.pop(name, None)


def _verify_original_v5_inputs(root: Path) -> None:
    for name, expected in ORIGINAL_V5_OUTPUT_SHA256.items():
        maximum = (
            MAX_LOG_BYTES
            if name.endswith(".log")
            else (
                MAX_LEDGER_BYTES
                if name.endswith(".jsonl")
                else MAX_SMALL_JSON_BYTES
            )
        )
        actual = _sha256_regular(
            root / name,
            maximum_bytes=maximum,
            label=f"frozen original V5 member {name}",
        )
        if actual != expected:
            raise FreshRecoveryTerminalAuditError(
                f"frozen original V5 evidence changed: {name}"
            )
    original_status = _read_regular(
        root / "original_v5_remote-exit-status.txt",
        maximum_bytes=16,
        label="original V5 exit status",
    )
    if original_status != b"137\n":
        raise FreshRecoveryTerminalAuditError(
            "original V5 infrastructure status changed"
        )


def _semantic_digest(document: Mapping[str, Any], field: str, label: str) -> str:
    body = copy.deepcopy(dict(document))
    claimed = body.pop(field, None)
    if (
        not isinstance(claimed, str)
        or SHA256_PATTERN.fullmatch(claimed) is None
        or _canonical_hash(body) != claimed
    ):
        raise FreshRecoveryTerminalAuditError(
            f"{label} semantic identity is invalid"
        )
    return claimed


def _verify_preregistration_and_split(
    preregistration: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
) -> None:
    if (
        _semantic_digest(
            preregistration,
            "preregistrationSha256",
            "V5 preregistration",
        )
        != PREREGISTRATION_SHA256
        or _semantic_digest(
            split_manifest,
            "manifestSha256",
            "V5 split manifest",
        )
        != SPLIT_MANIFEST_SHA256
    ):
        raise FreshRecoveryTerminalAuditError(
            "the frozen V5 preregistration or split changed"
        )
    session_and_data = preregistration.get("sessionAndData")
    repeated = (
        session_and_data.get("repeatedQuotePolicy")
        if isinstance(session_and_data, Mapping)
        else None
    )
    if repeated != {
        "diagnosticKey": ["symbol", "timestamp", "bid", "ask"],
        "retention": "every observation with a unique database id",
        "interpretation": "one additional tick-volume event, never a defect",
        "priceRoundingBeforeComparison": False,
    }:
        raise FreshRecoveryTerminalAuditError(
            "duplicate-tick volume semantics changed"
        )


def _verify_summary(
    root: Path,
    *,
    summary: Mapping[str, Any],
    source: VerifiedRecoveryLaunchSource,
    proof: Mapping[str, Any],
) -> tuple[bool, bool]:
    authorization_path = root / "fresh_holdout_authorization_v1.json"
    strategy_path = root / "fresh_final_strategy_frozen_v1.json"
    holdout_present = authorization_path.exists() or authorization_path.is_symlink()
    strategy_present = strategy_path.exists() or strategy_path.is_symlink()
    if holdout_present != strategy_present:
        raise FreshRecoveryTerminalAuditError(
            "holdout authorization and frozen strategy disagree"
        )
    if (
        summary.get("schema") != "fresh-xauusd-chronological-run/v1"
        or summary.get("recoveryUsed") is not True
        or summary.get("recoveryVersion") != "v5-same-lineage"
        or summary.get("recoveryOriginalRunId") != ORIGINAL_RUN_ID
        or summary.get("recoveryImplementationManifestSha256")
        != source.implementation_manifest_sha256
        or summary.get("infrastructureRestartUsed") is not True
        or summary.get("infrastructureRestartVersion") != 5
        or summary.get("studyId") != STUDY_ID
        or summary.get("studyLineageSha256") != STUDY_LINEAGE_SHA256
        or summary.get("splitManifestSha256") != SPLIT_MANIFEST_SHA256
        or summary.get("holdoutOpened") is not holdout_present
        or summary.get("status")
        not in {
            "validated_holdout_pass",
            "no_robust_setup_survived_frozen_validation",
        }
    ):
        raise FreshRecoveryTerminalAuditError(
            "recovery run summary identity changed"
        )
    if (
        summary.get("status") == "validated_holdout_pass"
        and not holdout_present
    ):
        raise FreshRecoveryTerminalAuditError(
            "validated status exists without holdout access"
        )
    expected_artifacts = sorted(
        {
            path.name
            for path in root.iterdir()
            if path.is_file()
            and path.name
            not in {
                "fresh_run_summary_v1.json",
                "server-run.log",
                "remote-exit-status.txt",
            }
        }
    )
    if summary.get("artifactFiles") != expected_artifacts:
        raise FreshRecoveryTerminalAuditError(
            "recovery run summary artifact inventory changed"
        )
    if holdout_present:
        authorization = _read_json(
            authorization_path,
            maximum_bytes=MAX_SMALL_JSON_BYTES,
            label="holdout authorization",
        )
        if authorization.get("infrastructureRecoveryProof") != proof:
            raise FreshRecoveryTerminalAuditError(
                "holdout authorization recovery proof changed"
            )
    return holdout_present, strategy_present


def audit_recovery_scientific_gate(
    terminal_output_directory: str | Path,
    *,
    launch_source_root: str | Path,
    expected_launch_commit_sha: str,
    recorded_remote_repository_root: str,
) -> dict[str, Any]:
    """Verify recovery eligibility without interpreting strategy outcomes."""

    selected = Path(terminal_output_directory).expanduser()
    if selected.is_symlink():
        raise FreshRecoveryTerminalAuditError(
            "terminal output directory cannot be a symlink"
        )
    root = selected.resolve()
    if not root.is_dir():
        raise FreshRecoveryTerminalAuditError(
            "terminal output directory is unavailable"
        )
    children = tuple(root.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in children):
        raise FreshRecoveryTerminalAuditError(
            "terminal output contains a non-regular member"
        )
    names = {path.name for path in children}
    if (
        not MANDATORY_OUTPUT_MEMBERS.issubset(names)
        or not names.issubset(ALLOWED_OUTPUT_MEMBERS)
    ):
        raise FreshRecoveryTerminalAuditError(
            "terminal output member set changed"
        )
    if _read_regular(
        root / "remote-exit-status.txt",
        maximum_bytes=16,
        label="recovery exit status",
    ) != b"0\n":
        raise FreshRecoveryTerminalAuditError(
            "a nonzero recovery is infrastructure evidence, not science"
        )
    _verify_original_v5_inputs(root)

    manifest = _read_json(
        root / "fresh_recovery_implementation_manifest_v1.json",
        maximum_bytes=MAX_SMALL_JSON_BYTES,
        label="recovery implementation manifest",
    )
    source = verify_recovery_implementation_manifest_offline(
        manifest,
        launch_source_root=launch_source_root,
        expected_launch_commit_sha=expected_launch_commit_sha,
        recorded_remote_repository_root=recorded_remote_repository_root,
    )
    preregistration = _read_json(
        root / "fresh_preregistration_v5.json",
        maximum_bytes=MAX_SMALL_JSON_BYTES,
        label="V5 preregistration",
    )
    split_manifest = _read_json(
        root / "fresh_split_manifest_v2.json",
        maximum_bytes=MAX_SMALL_JSON_BYTES,
        label="V5 split manifest",
    )
    _verify_preregistration_and_split(preregistration, split_manifest)
    contract = _read_json(
        root / "fresh_recovery_contract_v1.json",
        maximum_bytes=MAX_SMALL_JSON_BYTES,
        label="recovery contract",
    )
    if contract.get("schema") != RECOVERY_CONTRACT_SCHEMA:
        raise FreshRecoveryTerminalAuditError(
            "recovery contract schema changed"
        )
    records = _verified_ledger(root / "fresh_experiment_ledger_v1.jsonl")
    launch_root = Path(launch_source_root).expanduser().resolve()
    proof = _validate_with_launch_protocol(
        launch_source_root=launch_root,
        verified_source=source,
        manifest=manifest,
        records=records,
        preregistration=preregistration,
        split_manifest=split_manifest,
        recovery_contract=contract,
        sealed_batch_result_path=(
            root / "fresh_recovery_discovery_batch_v1.json"
        ),
    )
    summary = _read_json(
        root / "fresh_run_summary_v1.json",
        maximum_bytes=MAX_SMALL_JSON_BYTES,
        label="recovery run summary",
    )
    holdout_opened, _ = _verify_summary(
        root,
        summary=summary,
        source=source,
        proof=proof,
    )
    return {
        "schema": AUDIT_SCHEMA,
        "status": "recovery_scientific_gate_verified",
        "scientificResultInterpreted": False,
        "finalStrategyConclusionAuthorized": False,
        "launchCommit": source.commit_sha,
        "launchSourceClosureSha256": source.closure_sha256,
        "recoveryImplementationManifestSha256": (
            source.implementation_manifest_sha256
        ),
        "recoveryProof": proof,
        "experimentLedgerSha256": _sha256_regular(
            root / "fresh_experiment_ledger_v1.jsonl",
            maximum_bytes=MAX_LEDGER_BYTES,
            label="recovery experiment ledger",
        ),
        "originalLedgerPrefixVerified": True,
        "duplicateTicksRemainVolume": True,
        "holdoutOpened": holdout_opened,
        "holdoutAuthorizationContainsRecoveryProof": holdout_opened,
        "nextRequiredGate": "sealed chronological terminal scientific audit",
    }


def main(arguments: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("terminal_output_directory")
    parser.add_argument("launch_source_root")
    parser.add_argument("expected_launch_commit_sha")
    parser.add_argument("recorded_remote_repository_root")
    selected = parser.parse_args(arguments)
    result = audit_recovery_scientific_gate(
        selected.terminal_output_directory,
        launch_source_root=selected.launch_source_root,
        expected_launch_commit_sha=selected.expected_launch_commit_sha,
        recorded_remote_repository_root=(
            selected.recorded_remote_repository_root
        ),
    )
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ALLOWED_OUTPUT_MEMBERS",
    "AUDIT_SCHEMA",
    "FreshRecoveryTerminalAuditError",
    "RECOVERY_IMPLEMENTATION_FILES",
    "VerifiedRecoveryLaunchSource",
    "audit_recovery_scientific_gate",
    "main",
    "verify_recovery_implementation_manifest_offline",
]
