"""Read-only audit of the immutable detached-v5 terminal adoption bundle.

This module deliberately has no database, network, browser, GitHub, or EC2
integration.  It accepts only the already-adopted local bundle, verifies its
receipt and archive identities, and reads the nested tar archive without
extracting it or making persistent writes.  Decompression uses a bounded
temporary spool.  A process failure is never converted into a scientific
``no-pass`` conclusion.
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tempfile
from typing import Any, Mapping, Sequence
import zlib

from datavis.research.fresh_candidate_grid import build_fresh_candidate_grid
from datavis.research.fresh_exit_grid import build_fresh_exit_grid
from datavis.research.fresh_pipeline import (
    BASELINE_BOOTSTRAP_REPLICATES,
    BASELINE_CLUSTER_CONFIDENCE,
    BASELINE_EVENTS_PER_SIDE_PER_SESSION,
    BASELINE_MINIMUM_UPLIFT,
    REFERENCE_SCENARIO_ID,
    SESSION_CLOSE_SAFETY_MS,
    _EntryEdgeSummary,
    _entry_rank_score,
    _parameter_neighbourhood_audit,
)
from datavis.research.fresh_preregistration import (
    replay_execution_configs_from_preregistration,
    required_fresh_v5_implementation_files,
    validate_fresh_implementation_manifest,
    validate_fresh_preregistration_v5,
)
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_restart_v5 import (
    FRESH_V5_STUDY_ID,
    RUN19_MEMBER_FILE_SHA256,
    RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256,
    RUN19_RUN_ID,
    RUN19_SCIENTIFIC_SPECIFICATION_SHA256,
    RUN19_STUDY_LINEAGE_SHA256,
    RUN19_V5_STUDY_LINEAGE_SHA256,
    canonical_fresh_v5_study_lineage,
)
from datavis.research.fresh_scoring import (
    EntryMetrics,
    EntryScoreReport,
    TradeMetrics,
    TradeScoreReport,
    build_candidate_scorecard,
    evaluate_entry_gate,
    scoring_config_from_preregistration,
)
from datavis.research.fresh_search import (
    EntryCandidateSpec,
    FrozenEntryCandidate,
    FrozenStrategyCandidate,
    StrategyCandidateSpec,
)
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


AUDIT_SCHEMA = "fresh-xauusd-v5-terminal-audit/v1"
RECEIPT_SCHEMA = "fresh-xauusd-detached-research-receipt/v1"
ADOPTION_SCHEMA = "fresh-xauusd-v5-terminal-adoption/v1"
RUN_SCHEMA = "fresh-xauusd-chronological-run/v1"
FROZEN_RUN_ID = 30067832187
FROZEN_RUN_ATTEMPT = 1
FROZEN_RUN_COMMIT = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
FROZEN_RUN_BRANCH = "codex/xauusd-fresh-walkforward"
FROZEN_CONTROLLER_SHA256 = (
    "da57bce0f90890a8712edbb8cb9830054bfc5b2b3d544c2363420836b8b9ce3f"
)
FROZEN_LAUNCH_ARTIFACT_ID = 8586881858
FROZEN_LAUNCH_ARTIFACT_SIZE = 801
FROZEN_LAUNCH_ARTIFACT_DIGEST = (
    "sha256:86f6a8b06e0fde6a5223099a1eda4a9ce6e2f6fdd6248dbef91bbc4395936e1e"
)
FROZEN_LAUNCH_RECEIPT_SIZE = 1146
FROZEN_LAUNCH_RECEIPT_SHA256 = (
    "c6e32cbbdaaa2b9d343eee2a2fc399804976a6493ad4f99776b5ad795c5c54a4"
)
FROZEN_V5_IMPLEMENTATION_MANIFEST_SHA256 = (
    "05adfc7f4904120088ae1529b8f7c91318b3a222e8f010d45f94467b31a1e963"
)
FROZEN_ADOPTION_EXECUTIONS = frozenset(
    (
        (
            30069405297,
            1,
            "0e03f568d6db68e0b31ebda126ab03b61d445154",
        ),
        (
            30070331273,
            1,
            "a6c76e4bd50750c4c1f468bdd232fd1fd7e3090d",
        ),
        (
            30101048443,
            1,
            "c730fd0a2c66426f995ac43f1d50035cf94265ff",
        ),
    )
)
FROZEN_STATE_ROOT = PurePosixPath(
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
)
MAX_RECEIPT_BYTES = 64 * 1024
MAX_MANIFEST_BYTES = 256 * 1024
MAX_ARCHIVE_BYTES = 512 * 1024**2
MAX_EXPANDED_BYTES = 256 * 1024**2
MAX_TAR_STREAM_BYTES = MAX_EXPANDED_BYTES + 8 * 1024**2
MAX_TAR_READ_BYTES = 1024**2
MAX_ARCHIVE_MEMBERS = 24
MAX_JSON_MEMBER_BYTES = 8 * 1024**2
MAX_LEDGER_BYTES = 64 * 1024**2
MAX_LOG_BYTES = 32 * 1024**2
MAX_IMPLEMENTATION_SOURCE_BYTES = 16 * 1024**2
MAX_LEDGER_RECORDS = 1_024
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}\Z")

_RECEIPT_KEYS = {
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
_PATH_KEYS = {
    "worktree",
    "output",
    "scratch",
    "restart",
    "transfer",
    "terminalArchive",
    "serverLog",
}
_STAGES = (
    "discovery",
    "walk_forward_1",
    "walk_forward_2",
    "exit_search",
    "walk_forward_3",
    "validation",
    "holdout",
)
_RESEARCH_ROLES = (
    "discovery",
    "walk_forward_1",
    "walk_forward_2",
    "walk_forward_3",
    "validation",
    "holdout",
)
_STAGE_CONTEXTS = {
    "discovery": (("discovery",), ("discovery",), "discovery", "entry"),
    "walk_forward_1": (
        ("discovery",),
        ("walk_forward_1",),
        "walk_forward_1",
        "entry",
    ),
    "walk_forward_2": (
        ("discovery", "walk_forward_1"),
        ("walk_forward_2",),
        "walk_forward_2",
        "entry",
    ),
    "exit_search": (
        ("discovery", "walk_forward_1", "walk_forward_2"),
        ("discovery", "walk_forward_1", "walk_forward_2"),
        "walk_forward_2",
        "strategy",
    ),
    "walk_forward_3": (
        ("discovery", "walk_forward_1", "walk_forward_2"),
        ("walk_forward_3",),
        "walk_forward_3",
        "strategy",
    ),
    "validation": (
        (
            "discovery",
            "walk_forward_1",
            "walk_forward_2",
            "walk_forward_3",
        ),
        ("validation",),
        "validation",
        "strategy",
    ),
    "holdout": (
        (
            "discovery",
            "walk_forward_1",
            "walk_forward_2",
            "walk_forward_3",
            "validation",
        ),
        ("holdout",),
        "holdout",
        "strategy",
    ),
}
_PROTOCOL_KINDS = {"stage-window-access", "batch-window-access"}
_BASE_INNER_MEMBERS = {
    "fresh_source_inventory_v1.json",
    "fresh_corpus_manifest_v1.json",
    "fresh_split_manifest_v2.json",
    "fresh_research_state_binding_v4.json",
    "predecessor_fresh_research_state_binding_v3.json",
    "predecessor_fresh_experiment_ledger_v1.jsonl",
    "predecessor_fresh_preregistration_v4.json",
    "predecessor_fresh_implementation_manifest_v1.json",
    "fresh_implementation_manifest_v1.json",
    "fresh_preregistration_v5.json",
    "fresh_quantile_bank_v1.json",
    "fresh_threshold_domain_preflight_v1.json",
    "fresh_entry_bank_v1.json",
}
_TERMINAL_MEMBERS = {
    "fresh_experiment_ledger_v1.jsonl",
    "fresh_run_summary_v1.json",
    "server-run.log",
    "remote-exit-status.txt",
}
_CONDITIONAL_MEMBERS = {
    "fresh_exit_bank_v1.json",
    "fresh_final_strategy_frozen_v1.json",
    "fresh_holdout_authorization_v1.json",
}
_ALLOWED_INNER_MEMBERS = (
    _BASE_INNER_MEMBERS | _TERMINAL_MEMBERS | _CONDITIONAL_MEMBERS
)

FROZEN_V5_IMPLEMENTATION_FILE_SHA256 = {
    ".github/research-launch.txt": (
        "e357976d5879b31664059b96017bb56f72c2d74456873d4939b91e8011ff4727"
    ),
    ".github/workflows/fresh-xauusd-research.yml": (
        "592b06c6fafc7272ae1ea5fbcd348924d316a8591caa85ea59b3a91f206b5a59"
    ),
    "datavis/db.py": (
        "e26524b82902441a2750311ad5ac5e6c31cb1e6140f2e9770470b058eebc3330"
    ),
    "datavis/research/__init__.py": (
        "0f729be715d82bf228511059f74fe074cbddab93f8e5d7794d2671a57b0c5fe3"
    ),
    "datavis/research/fresh_bootstrap.py": (
        "5048159a13fa30855570a0da2119be3db2a389d29ca1f7257503070b06669709"
    ),
    "datavis/research/fresh_candidate_grid.py": (
        "df7ac596e01c10dc1ffce2479459df76bf4b27ec92d439fa54936fd0cf376244"
    ),
    "datavis/research/fresh_data.py": (
        "e3aa81283e672faee932512310f746027f6eda653873fd606560124e252a5212"
    ),
    "datavis/research/fresh_db_source.py": (
        "f28e3c63f7f6a2e9d10fcde4b2860eadb1aca3bc51fcf585721f928dbc2c0acf"
    ),
    "datavis/research/fresh_decisions.py": (
        "f054fc7d7c24bf89bb7ea472025f836d69339ef0eeec2ab30d9e1733fd992795"
    ),
    "datavis/research/fresh_entry_diagnostics.py": (
        "4bd394e98e4770f7d63e699fb2f24aa5e255b943ffaf88a5ebf1fff81544bbb3"
    ),
    "datavis/research/fresh_event_filters.py": (
        "1ccab790e6864c280a21c2f6bf160ae34d9ad3298173b83aa3bc19e9e38da747"
    ),
    "datavis/research/fresh_exit_grid.py": (
        "18fc7fcba4f554d584465c4ba65756032f249cca4dd3e90fd27c041032dcc22b"
    ),
    "datavis/research/fresh_exits.py": (
        "5ce1a10a21d2fb19a5c8cb92d4ed8f445265c3edbe48443b2df2aae9d3328be0"
    ),
    "datavis/research/fresh_feature_bank.py": (
        "218fa40fbc8edfd1a22232612a6c6c57203270b671a45a89762e680d36c2a944"
    ),
    "datavis/research/fresh_features.py": (
        "9562cb71dc7e20c273b7ec9797144c217329bc2b3e7ea519d5459e851470c28c"
    ),
    "datavis/research/fresh_inventory.py": (
        "b84aa3fab3bf578066d275301de5096b79857b04d9eba5603c8015f160d5480c"
    ),
    "datavis/research/fresh_pipeline.py": (
        "7708e0cc74082b6b7e1a7db9ee1b3756aa8b62e7ba39056f63b9be5004d3f609"
    ),
    "datavis/research/fresh_pipeline_cli.py": (
        "2094381a25c58e1827d0b2656552b6932a3007e6a6c133aeb06fa0616ca00709"
    ),
    "datavis/research/fresh_preregistration.py": (
        "2d82a7f508957b9107fd860668958b4e2f6f0736d68f90187dd46ca0db28f029"
    ),
    "datavis/research/fresh_protocol.py": (
        "22b6642da2035fa7452b908b02d317ffeb99ad73dcedd99158fda746c7625009"
    ),
    "datavis/research/fresh_replay.py": (
        "35d931f4a69ac8cf139638a9416ef41d5e8f24fb8ba816d997a0e1c02e05ef0f"
    ),
    "datavis/research/fresh_restart.py": (
        "d5a3c605f8a6be9f524cedaf22eaf291b0f2b7813b292e9aeb1f972123b65237"
    ),
    "datavis/research/fresh_restart_v4.py": (
        "2d19382490fed141de26efde6e6f2ed45456bd897b6ecb7ae0bc0c09b624a291"
    ),
    "datavis/research/fresh_restart_v5.py": (
        "e35d0ee7f161048a66f4fd448fd4143940f855823f576fd094f99b0d6d2d2b2f"
    ),
    "datavis/research/fresh_scoring.py": (
        "976d8c8090ad673090d99f02392f8a7f6a88fb6d0a0bb10f8ab5f0625d3c5424"
    ),
    "datavis/research/fresh_search.py": (
        "23614fb3c6e751e7f59b70b5c450e531a4f1a373e470e102867e0c471787653b"
    ),
    "datavis/research/fresh_session_eval.py": (
        "e7c6fe53fc953b9ba0b1f2361e65292a74807ef2db4d53b53a7383550df3f62c"
    ),
    "datavis/research/fresh_sessions.py": (
        "9bd40f1afeedba0dcbdf88a22717b1347c831479a5a836e508a2ca007915b03d"
    ),
    "datavis/research/fresh_signals.py": (
        "83fbf412c5566c01c5e22e10a32983ecc35f8781f2b69e83547ef4b8b313d95a"
    ),
    "datavis/research/fresh_spool.py": (
        "d88fad486740d2ad8b9608d84b6817e8099d36f2b5a348e65a52aa4f5e41060b"
    ),
    "datavis/research/fresh_thresholds.py": (
        "3daac1d1f6e7ea4affd913172584c344bb32f97ffef76d9ad6ad4a51a10ed48c"
    ),
    "datavis/research/ticks.py": (
        "1abbe6959bda9031e4ee4e67553c476f2337f1eb6cbfda79af2ef8a5be913bb8"
    ),
}
FROZEN_LOCAL_RUNTIME_CLOSURE_SHA256 = {
    **FROZEN_V5_IMPLEMENTATION_FILE_SHA256,
    # The immutable implementation manifest was authored from a Windows
    # checkout. The detached production worktree was created by Git on Linux;
    # these two text blobs therefore differ only by CRLF-to-LF checkout
    # normalization. Pin the bytes that the remote Python process executed.
    "datavis/db.py": (
        "7f3c8dc45ceed968ec4c935752ba85b3b172fb538fb1ff63de4baa1fcec48999"
    ),
    "datavis/__init__.py": (
        "9bbe2d85ef0b6c651f607da9b43eda0c7ab9e9bb2e2383badc67e3cc154faf6f"
    ),
    "datavis/research/fresh_recovery.py": (
        "7e84e485157f671bf5df1b2514a3a98ce0b3242440756078a6d7a295863d757c"
    ),
}
FROZEN_LOCAL_RUNTIME_CLOSURE_MANIFEST_SHA256 = (
    "495bac1575f17f11c54be8c184f40434b0de4c9cf46025e6fd3dd2072b95f017"
)


class FreshTerminalAuditError(RuntimeError):
    """Raised when immutable identity or scientific evidence is inconsistent."""


def _pairs_without_duplicates(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FreshTerminalAuditError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> None:
    raise FreshTerminalAuditError(f"non-finite JSON value: {value}")


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs_without_duplicates,
            parse_constant=_reject_nonfinite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FreshTerminalAuditError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise FreshTerminalAuditError(f"{label} is not a JSON object")
    return value


def _canonical_compact_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _open_stable_regular(path: Path, *, maximum_bytes: int) -> tuple[int, os.stat_result]:
    if maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be positive")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size <= 0
            or metadata.st_size > maximum_bytes
        ):
            raise FreshTerminalAuditError(
                f"{path.name} is not a bounded non-empty regular file"
            )
        return descriptor, metadata
    except BaseException:
        os.close(descriptor)
        raise


def _stable_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_bounded_regular(
    path: Path,
    *,
    maximum_bytes: int,
    expected_size: int | None = None,
) -> bytes:
    descriptor, before = _open_stable_regular(
        path, maximum_bytes=maximum_bytes
    )
    try:
        if expected_size is not None and before.st_size != expected_size:
            raise FreshTerminalAuditError(f"{path.name} size changed")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, before.st_size - total + 1))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > before.st_size:
                raise FreshTerminalAuditError(f"{path.name} grew while reading")
        after = os.fstat(descriptor)
        if (
            total != before.st_size
            or _stable_identity(before) != _stable_identity(after)
        ):
            raise FreshTerminalAuditError(f"{path.name} changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _sha256_file(
    path: Path,
    *,
    maximum_bytes: int,
    expected_size: int | None = None,
) -> str:
    descriptor, before = _open_stable_regular(
        path, maximum_bytes=maximum_bytes
    )
    try:
        if expected_size is not None and before.st_size != expected_size:
            raise FreshTerminalAuditError(f"{path.name} size changed")
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
            if total > before.st_size:
                raise FreshTerminalAuditError(f"{path.name} grew while hashing")
        after = os.fstat(descriptor)
        if (
            total != before.st_size
            or _stable_identity(before) != _stable_identity(after)
        ):
            raise FreshTerminalAuditError(f"{path.name} changed while hashing")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _sha256_open_descriptor(
    descriptor: int,
    before: os.stat_result,
) -> str:
    digest = hashlib.sha256()
    total = 0
    os.lseek(descriptor, 0, os.SEEK_SET)
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        total += len(chunk)
        if total > before.st_size:
            raise FreshTerminalAuditError("open archive grew while hashing")
    after = os.fstat(descriptor)
    if (
        total != before.st_size
        or _stable_identity(before) != _stable_identity(after)
    ):
        raise FreshTerminalAuditError("open archive changed while hashing")
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest()


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FreshTerminalAuditError(f"{label} is not a positive integer")
    return value


def _regular_file(
    path: Path,
    label: str,
    *,
    maximum_bytes: int,
    expected_size: int | None = None,
) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_size <= 0
        or metadata.st_size > maximum_bytes
        or (expected_size is not None and metadata.st_size != expected_size)
    ):
        raise FreshTerminalAuditError(
            f"{label} is not a bounded non-empty regular file"
        )


def _verify_receipts(
    launch_raw: bytes,
    terminal_raw: bytes,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if (
        len(launch_raw) != FROZEN_LAUNCH_RECEIPT_SIZE
        or _sha256_bytes(launch_raw) != FROZEN_LAUNCH_RECEIPT_SHA256
    ):
        raise FreshTerminalAuditError("launch receipt exact identity changed")
    launch = _json_object(launch_raw, "launch receipt")
    terminal = _json_object(terminal_raw, "terminal receipt")
    if launch_raw != _canonical_compact_bytes(launch):
        raise FreshTerminalAuditError("launch receipt is not canonical")
    if terminal_raw != _canonical_compact_bytes(terminal):
        raise FreshTerminalAuditError("terminal receipt is not canonical")
    if set(launch) != _RECEIPT_KEYS or set(terminal) != _RECEIPT_KEYS:
        raise FreshTerminalAuditError("receipt field set changed")
    expected = {
        "schema": RECEIPT_SCHEMA,
        "kind": "launch_ready",
        "status": "running",
        "processExitStatus": None,
        "githubRunId": FROZEN_RUN_ID,
        "githubRunAttempt": FROZEN_RUN_ATTEMPT,
        "branch": FROZEN_RUN_BRANCH,
        "commitSha": FROZEN_RUN_COMMIT,
        "studyLineageSha256": RUN19_V5_STUDY_LINEAGE_SHA256,
        "controllerSha256": FROZEN_CONTROLLER_SHA256,
        "terminalArchive": None,
    }
    for key, value in expected.items():
        if launch.get(key) != value or type(launch.get(key)) is not type(value):
            raise FreshTerminalAuditError(f"launch receipt {key} changed")
    for key in (
        "run19ArtifactId",
        "controllerPid",
        "controllerStartTicks",
        "pipelinePid",
        "pipelin 1e-9
            or hhi < largest * largest - 1e-9
            or hhi > largest + 1e-9
        ):
            raise FreshTerminalAuditError(
                "positive-trade concentration metrics are impossible"
            )
    elif (
        values["largest_trade_share_of_gross_profit"] is not None
        or values["positive_trade_profit_hhi"] is not None
    ):
        raise FreshTerminalAuditError(
            "empty positive-trade set has concentration metrics"
        )
    expected_dd_ratio = (
        values["maximum_drawdown"] / values["gross_profit"]
        if values["gross_profit"] > 0.0
        else None
    )
    if not _same_number(
        values["maximum_drawdown_to_gross_profit"],
        expected_dd_ratio,
        tolerance=1e-9,
    ):
        raise FreshTerminalAuditError("trade drawdown ratio is inconsistent")
    raw_sessions = values["session_net_pnl"]
    if (
        not isinstance(raw_sessions, list)
        or len(raw_sessions) != session_count
    ):
        raise FreshTerminalAuditError("trade session P&L is incomplete")
    sessions: list[tuple[str, float]] = []
    for item in raw_sessions:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or isinstance(item[1], bool)
            or not isinstance(item[1], (int, float))
            or not math.isfinite(float(item[1]))
        ):
            raise FreshTerminalAuditError("trade session P&L is invalid")
        sessions.append((item[0], float(item[1])))
    if [item[0] for item in sessions] != list(expected_anchors):
        raise FreshTerminalAuditError("trade session labels changed")
    if not _same_number(
        math.fsum(item[1] for item in sessions),
        values["net_pnl"],
        tolerance=1e-8,
    ):
        raise FreshTerminalAuditError("session P&L does not sum to net P&L")
    positive_session_total = math.fsum(
        value for _, value in sessions if value > 0.0
    )
    negative_session_total = -math.fsum(
        value for _, value in sessions if value < 0.0
    )
    session_scale = max(
        values["gross_profit"],
        values["gross_loss"],
        positive_session_total,
        negative_session_total,
        abs(values["net_pnl"]),
        1.0,
    )
    session_slack = max(1e-12, 16.0 * math.ulp(session_scale))
    flat_allowance = values["flat_count"] * pnl_tolerance
    if (
        positive_session_total
        > values["gross_profit"] + flat_allowance + session_slack
        or negative_session_total
        > values["gross_loss"] + flat_allowance + session_slack
    ):
        raise FreshTerminalAuditError(
            "session P&L magnitudes exceed trade P&L magnitudes"
        )
    session_equity = 0.0
    session_peak = 0.0
    session_drawdown = 0.0
    for _, value in sessions:
        session_equity += value
        session_peak = max(session_peak, session_equity)
        session_drawdown = max(
            session_drawdown,
            session_peak - session_equity,
        )
    if values["maximum_drawdown"] + session_slack < session_drawdown:
        raise FreshTerminalAuditError(
            "trade drawdown is below chronological session drawdown"
        )
    if (
        sum(value > pnl_tolerance for _, value in sessions)
        != values["positive_session_count"]
    ):
        raise FreshTerminalAuditError("session activity counts are inconsistent")
    positive_session_pnl = [
        value for _, value in sessions if value > pnl_tolerance
    ]
    positive_session_shares = (
        [value / values["gross_profit"] for value in positive_session_pnl]
        if values["gross_profit"] > 0.0
        else []
    )
    expected_largest_session_share = (
        max(positive_session_shares) if positive_session_shares else None
    )
    expected_session_hhi = (
        math.fsum(value * value for value in positive_session_shares)
        if positive_session_shares
        else None
    )
    if (
        not _same_number(
            values["largest_session_share_of_gross_profit"],
            expected_largest_session_share,
            tolerance=1e-9,
        )
        or not _same_number(
            values["positive_session_profit_hhi"],
            expected_session_hhi,
            tolerance=1e-9,
        )
    ):
        raise FreshTerminalAuditError(
            "trade session-concentration metrics are inconsistent"
        )
    values["session_net_pnl"] = tuple(sessions)
    if not isinstance(values["profitability_valid"], bool):
        raise FreshTerminalAuditError("trade profitability-valid flag is invalid")
    if values["replay_censor_count"] > 0 and values["profitability_valid"]:
        raise FreshTerminalAuditError(
            "censored replay cannot be profitability-valid"
        )
    return TradeMetrics(**values)


def _evaluation_anchors(
    split: Mapping[str, Any],
    stage: str,
) -> tuple[str, ...]:
    windows = split["windows"]
    evaluation_roles = _STAGE_CONTEXTS[stage][1]
    return tuple(
        anchor
        for role in evaluation_roles
        for anchor in windows[role]["sessionAnchors"]
    )


def _entry_edge(payload: Any) -> _EntryEdgeSummary:
    if not isinstance(payload, Mapping) or set(payload) != set(
        _EntryEdgeSummary.__dataclass_fields__
    ):
        raise FreshTerminalAuditError("entry-edge metric schema changed")
    values = dict(payload)
    for name in (
        "coverage_10_cluster_interval",
        "coverage_30_cluster_interval",
        "uplift_10_cluster_interval",
        "uplift_30_cluster_interval",
    ):
        value = values[name]
        if value is not None:
            if (
                not isinstance(value, list)
                or len(value) != 2
                or any(
                    isinstance(item, bool)
                    or not isinstance(item, (int, float))
                    or not math.isfinite(float(item))
                    for item in value
                )
                or value[0] > value[1]
            ):
                raise FreshTerminalAuditError(
                    f"entry-edge interval {name} is invalid"
                )
            values[name] = tuple(float(item) for item in value)
    for name, value in values.items():
        if name.endswith("_interval") or name == "baseline_gate_passed":
            continue
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise FreshTerminalAuditError(f"entry-edge metric {name} is invalid")
    if not isinstance(values["baseline_gate_passed"], bool):
        raise FreshTerminalAuditError("entry-edge baseline gate is invalid")
    return _EntryEdgeSummary(**values)


def _canonical_equal(left: Any, right: Any) -> bool:
    try:
        return canonical_hash(left) == canonical_hash(right)
    except ValueError:
        return False


def _stage_context(
    split: Mapping[str, Any],
    stage: str,
) -> tuple[tuple[str, ...], tuple[str, ...], str, str, str]:
    try:
        training, evaluation, role, candidate_kind = _STAGE_CONTEXTS[stage]
        windows = split["windows"]
        ordered_roles: list[str] = []
        for window_role in (*training, *evaluation):
            if window_role not in ordered_roles:
                ordered_roles.append(window_role)
        window_sha = canonical_hash(
            [canonical_hash(windows[window_role]) for window_role in ordered_roles]
        )
    except (KeyError, TypeError, ValueError) as error:
        raise FreshTerminalAuditError(
            f"cannot reconstruct frozen window context for {stage}"
        ) from error
    return training, evaluation, role, candidate_kind, window_sha


def _exact_record_body(
    record: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    label: str,
) -> None:
    body = {
        key: value
        for key, value in record.items()
        if key not in {"recordNumber", "recordSha256"}
    }
    if not _canonical_equal(body, expected):
        raise FreshTerminalAuditError(f"{label} changed")


def _expected_stage_access_record(
    *,
    stage: str,
    training: Sequence[str],
    evaluation: Sequence[str],
    role: str,
    window_sha: str,
    preregistration_sha: str,
) -> dict[str, Any]:
    purpose = (
        "evaluate the immutable outcome-blind predecessor discovery "
        "threshold and entry bank in the new study"
    )
    payload = {
        "kind": "fresh-stage-window-access",
        "status": "window_access_started",
        "stage": stage,
        "role": role,
        "purpose": purpose,
        "trainingRoles": list(training),
        "evaluationRoles": list(evaluation),
    }
    return {
        "recordKind": "stage-window-access",
        "candidateId": f"protocol-stage-access::{stage}",
        "family": "protocol-window-access",
        "stage": stage,
        "trainingWindow": "+".join(training),
        "evaluationWindow": "+".join(evaluation),
        "parameters": payload,
        "entryVariant": "stage-window-access",
        "exitVariant": "stage-window-access",
        "metrics": {"purpose": purpose},
        "status": "window_access_started",
        "leakageChecks": {
            "durableBeforeCallback": True,
            "windowConsumedBeforeCallback": True,
        },
        "role": role,
        "outcomesRevealed": True,
        "gatePassed": False,
        "identitySha256": canonical_hash(payload),
        "windowSha256": window_sha,
        "preregistrationSha256": preregistration_sha,
    }


def _expected_batch_access_record(
    *,
    kind: str,
    status: str,
    stage: str,
    training: Sequence[str],
    evaluation: Sequence[str],
    role: str,
    candidate_ids: Sequence[str],
    candidate_sha256: Sequence[str],
    window_sha: str,
    preregistration_sha: str,
) -> dict[str, Any]:
    payload = {
        "kind": "fresh-batch-window-access",
        "batchKind": kind,
        "status": status,
        "stage": stage,
        "trainingRoles": list(training),
        "evaluationRoles": list(evaluation),
        "candidateIds": list(candidate_ids),
        "candidateSha256": list(candidate_sha256),
        "errorType": None,
    }
    return {
        "recordKind": "batch-window-access",
        "candidateId": f"protocol-batch-access::{kind}::{stage}",
        "family": "protocol-window-access",
        "stage": stage,
        "trainingWindow": "+".join(training),
        "evaluationWindow": "+".join(evaluation),
        "parameters": payload,
        "entryVariant": "batch-window-access",
        "exitVariant": "batch-window-access",
        "metrics": {
            "candidateCount": len(candidate_ids),
            "errorType": None,
        },
        "status": status,
        "leakageChecks": {
            "durableBeforeCallback": status == "batch_access_started",
            "callbackCompleted": status == "batch_access_completed",
            "callbackErrored": False,
        },
        "role": role,
        "outcomesRevealed": True,
        "gatePassed": False,
        "identitySha256": canonical_hash(payload),
        "windowSha256": window_sha,
        "preregistrationSha256": preregistration_sha,
    }


def _compact_entry_report(
    payload: Any,
    *,
    expected_anchors: Sequence[str],
) -> EntryScoreReport:
    expected_keys = {
        "overall",
        "byDay",
        "bySide",
        "byMarketSession",
        "byRegime",
    }
    if not isinstance(payload, Mapping) or set(payload) != expected_keys:
        raise FreshTerminalAuditError("compact entry-report schema changed")
    overall = _entry_metrics(
        payload["overall"],
        expected_sessions=len(expected_anchors),
    )
    expected_slice_keys = {
        "label",
        "filledCount",
        "coverage10",
        "coverage30",
        "coverage60",
        "barrierProfitFirstRate",
    }
    for dimension in ("byDay", "bySide", "byMarketSession", "byRegime"):
        rows = payload[dimension]
        if not isinstance(rows, list):
            raise FreshTerminalAuditError("entry slice collection is invalid")
        labels: list[str] = []
        filled_total = 0
        weighted_coverage = [0.0, 0.0, 0.0]
        weighted_barrier = 0.0
        active_days = 0
        for row in rows:
            if (
                not isinstance(row, Mapping)
                or set(row) != expected_slice_keys
                or not isinstance(row.get("label"), str)
                or not row["label"]
            ):
                raise FreshTerminalAuditError("entry slice row is invalid")
            labels.append(row["label"])
            filled = _nonnegative_count(
                row["filledCount"], "entry slice filled count"
            )
            filled_total += filled
            values = [
                _finite_probability(
                    row[name], f"entry slice {name}"
                )
                for name in (
                    "coverage10",
                    "coverage30",
                    "coverage60",
                    "barrierProfitFirstRate",
                )
            ]
            if filled and any(value is None for value in values):
                raise FreshTerminalAuditError(
                    "non-empty entry slice has missing metrics"
                )
            if not filled and any(value is not None for value in values):
                raise FreshTerminalAuditError(
                    "empty entry slice has outcome metrics"
                )
            if (
                values[0] is not None
                and values[1] is not None
                and values[2] is not None
                and not values[0] <= values[1] <= values[2]
            ):
                raise FreshTerminalAuditError(
                    "entry slice coverage is not monotonic"
                )
            if filled:
                active_days += int(dimension == "byDay")
                for index, value in enumerate(values[:3]):
                    assert value is not None
                    if not math.isclose(
                        filled * value,
                        round(filled * value),
                        rel_tol=0.0,
                        abs_tol=1e-8,
                    ):
                        raise FreshTerminalAuditError(
                            "entry slice coverage is not an integer outcome"
                        )
                    weighted_coverage[index] += filled * value
                assert values[3] is not None
                if not math.isclose(
                    filled * values[3],
                    round(filled * values[3]),
                    rel_tol=0.0,
                    abs_tol=1e-8,
                ):
                    raise FreshTerminalAuditError(
                        "entry slice barrier rate is not an integer outcome"
                    )
                weighted_barrier += filled * values[3]
        if labels != sorted(labels) or len(labels) != len(set(labels)):
            raise FreshTerminalAuditError("entry slice labels are not canonical")
        if dimension == "byDay" and labels != sorted(expected_anchors):
            raise FreshTerminalAuditError("entry day slices changed")
        if filled_total != overall.filled_count:
            raise FreshTerminalAuditError(
                f"entry {dimension} counts disagree with overall"
            )
        if overall.filled_count:
            expected_rates = (
                overall.coverage_probability(10),
                overall.coverage_probability(30),
                overall.coverage_probability(60),
                overall.barrier_profit_first_rate,
            )
            calculated_rates = (
                *(value / overall.filled_count for value in weighted_coverage),
                weighted_barrier / overall.filled_count,
            )
            if any(
                not _same_number(calculated, expected, tolerance=1e-9)
                for calculated, expected in zip(
                    calculated_rates, expected_rates
                )
            ):
                raise FreshTerminalAuditError(
                    f"entry {dimension} rates disagree with overall"
                )
        if (
            dimension == "byDay"
            and active_days != overall.active_session_count
        ):
            raise FreshTerminalAuditError(
                "entry active-session count disagrees with day slices"
            )
    return EntryScoreReport(overall, (), (), (), ())


def _compact_trade_report(
    payload: Any,
    *,
    expected_anchors: Sequence[str],
    pnl_tolerance: float,
) -> TradeScoreReport:
    expected_keys = {
        "overall",
        "byDay",
        "bySide",
        "byMarketSession",
        "byRegime",
    }
    if not isinstance(payload, Mapping) or set(payload) != expected_keys:
        raise FreshTerminalAuditError("compact trade-report schema changed")
    overall = _trade_metrics(
        payload["overall"],
        expected_anchors=expected_anchors,
        pnl_tolerance=pnl_tolerance,
    )
    expected_slice_keys = {
        "label",
        "tradeCount",
        "winRate",
        "netPnl",
        "expectancy",
        "profitFactor",
        "maximumDrawdown",
    }
    for dimension in ("byDay", "bySide", "byMarketSession", "byRegime"):
        rows = payload[dimension]
        if not isinstance(rows, list):
            raise FreshTerminalAuditError("trade slice collection is invalid")
        labels: list[str] = []
        trade_total = 0
        pnl_values: list[float] = []
        weighted_wins = 0.0
        active_days = 0
        day_pnl: dict[str, float] = {}
        for row in rows:
            if (
                not isinstance(row, Mapping)
                or set(row) != expected_slice_keys
                or not isinstance(row.get("label"), str)
                or not row["label"]
            ):
                raise FreshTerminalAuditError("trade slice row is invalid")
            labels.append(row["label"])
            count = _nonnegative_count(
                row["tradeCount"], "trade slice count"
            )
            trade_total += count
            win_rate = _finite_probability(
                row["winRate"], "trade slice win rate"
            )
            net_pnl = row["netPnl"]
            drawdown = row["maximumDrawdown"]
            if any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                for value in (net_pnl, drawdown)
            ) or float(drawdown) < 0.0:
                raise FreshTerminalAuditError("trade slice P&L is invalid")
            pnl_values.append(float(net_pnl))
            expectancy = row["expectancy"]
            if expectancy is not None and (
                isinstance(expectancy, bool)
                or not isinstance(expectancy, (int, float))
                or not math.isfinite(float(expectancy))
            ):
                raise FreshTerminalAuditError("trade slice expectancy is invalid")
            expected_expectancy = float(net_pnl) / count if count else None
            if not _same_number(
                expectancy, expected_expectancy, tolerance=1e-9
            ):
                raise FreshTerminalAuditError(
                    "trade slice expectancy is inconsistent"
                )
            profit_factor = row["profitFactor"]
            if (
                profit_factor not in (None, "Infinity")
                and (
                    isinstance(profit_factor, bool)
                    or not isinstance(profit_factor, (int, float))
                    or not math.isfinite(float(profit_factor))
                    or float(profit_factor) < 0.0
                )
            ):
                raise FreshTerminalAuditError(
                    "trade slice profit factor is invalid"
                )
            if count and win_rate is None:
                raise FreshTerminalAuditError(
                    "non-empty trade slice has no win rate"
                )
            if not count and any(
                value is not None
                for value in (win_rate, expectancy, profit_factor)
            ):
                raise FreshTerminalAuditError(
                    "empty trade slice has outcome ratios"
                )
            if not count and (
                not _same_number(net_pnl, 0.0)
                or not _same_number(drawdown, 0.0)
            ):
                raise FreshTerminalAuditError(
                    "empty trade slice has P&L"
                )
            if count:
                assert win_rate is not None
                if not math.isclose(
                    count * win_rate,
                    round(count * win_rate),
                    rel_tol=0.0,
                    abs_tol=1e-8,
                ):
                    raise FreshTerminalAuditError(
                        "trade slice win rate is not an integer outcome"
                    )
                weighted_wins += count * win_rate
                active_days += int(dimension == "byDay")
            if dimension == "byDay":
                day_pnl[row["label"]] = float(net_pnl)
        if labels != sorted(labels) or len(labels) != len(set(labels)):
            raise FreshTerminalAuditError("trade slice labels are not canonical")
        if dimension == "byDay" and labels != sorted(expected_anchors):
            raise FreshTerminalAuditError("trade day slices changed")
        if trade_total != overall.trade_count or not _same_number(
            math.fsum(pnl_values),
            overall.net_pnl,
            tolerance=1e-8,
        ):
            raise FreshTerminalAuditError(
                f"trade {dimension} slices disagree with overall"
            )
        if not _same_number(
            weighted_wins,
            overall.win_count,
            tolerance=1e-8,
        ):
            raise FreshTerminalAuditError(
                f"trade {dimension} wins disagree with overall"
            )
        if dimension == "byDay":
            if active_days != overall.active_session_count:
                raise FreshTerminalAuditError(
                    "trade active-session count disagrees with day slices"
                )
            if any(
                not _same_number(
                    day_pnl[label],
                    value,
                    tolerance=1e-8,
                )
                for label, value in overall.session_net_pnl
            ):
                raise FreshTerminalAuditError(
                    "trade session P&L disagrees with day slices"
                )
    return TradeScoreReport(overall, (), (), (), ())


def _validate_edge_against_entry(
    edge: _EntryEdgeSummary,
    entry: EntryMetrics,
) -> None:
    coverage_10 = entry.coverage_probability(10)
    coverage_30 = entry.coverage_probability(30)
    coverage_60 = entry.coverage_probability(60)
    fill_derived = (
        edge.expected_barrier_pnl_per_fill,
        edge.median_mae_before_coverage,
        edge.median_mfe_horizon,
        edge.p90_restricted_coverage_ms,
        edge.failure_to_cover_60s,
    )
    if entry.filled_count:
        if any(value is None for value in fill_derived):
            raise FreshTerminalAuditError(
                "non-empty entry edge has missing fill-derived metrics"
            )
    elif any(value is not None for value in fill_derived):
        raise FreshTerminalAuditError(
            "empty entry edge has fill-derived metrics"
        )
    if (
        edge.median_mae_before_coverage is not None
        and edge.median_mae_before_coverage > 0.0
    ):
        raise FreshTerminalAuditError(
            "entry adverse-excursion metric has an impossible sign"
        )
    for value, label in (
        (edge.baseline_coverage_10, "baseline coverage 10"),
        (edge.baseline_coverage_30, "baseline coverage 30"),
    ):
        _finite_probability(value, label)
    for interval, label, probability in (
        (edge.coverage_10_cluster_interval, "coverage 10 interval", True),
        (edge.coverage_30_cluster_interval, "coverage 30 interval", True),
        (edge.uplift_10_cluster_interval, "uplift 10 interval", False),
        (edge.uplift_30_cluster_interval, "uplift 30 interval", False),
    ):
        if interval is not None and any(
            (not -1.0 <= float(value) <= 1.0)
            if not probability
            else (not 0.0 <= float(value) <= 1.0)
            for value in interval
        ):
            raise FreshTerminalAuditError(f"{label} is out of range")
    for coverage, baseline, uplift, label in (
        (
            coverage_10,
            edge.baseline_coverage_10,
            edge.uplift_10,
            "10-second uplift",
        ),
        (
            coverage_30,
            edge.baseline_coverage_30,
            edge.uplift_30,
            "30-second uplift",
        ),
    ):
        expected = (
            coverage - baseline
            if coverage is not None and baseline is not None
            else None
        )
        if not _same_number(uplift, expected, tolerance=1e-9):
            raise FreshTerminalAuditError(f"{label} is inconsistent")
    expected_failure = 1.0 - coverage_60 if coverage_60 is not None else None
    if not _same_number(
        edge.failure_to_cover_60s,
        expected_failure,
        tolerance=1e-9,
    ):
        raise FreshTerminalAuditError(
            "60-second failure-to-cover rate is inconsistent"
        )
    if edge.p90_restricted_coverage_ms is not None and not (
        0.0 <= edge.p90_restricted_coverage_ms <= 60_000.0
    ):
        raise FreshTerminalAuditError("p90 restricted coverage is invalid")
    if (
        edge.p90_restricted_coverage_ms is not None
        and entry.restricted_median_coverage_milliseconds is not None
        and edge.p90_restricted_coverage_ms
        < entry.restricted_median_coverage_milliseconds - 1e-9
    ) or (
        entry.median_covered_time_milliseconds is not None
        and entry.restricted_median_coverage_milliseconds is not None
        and entry.median_covered_time_milliseconds
        > entry.restricted_median_coverage_milliseconds + 1e-9
    ):
        raise FreshTerminalAuditError(
            "entry coverage quantiles are inconsistent"
        )
    if entry.filled_count:
        censored_losses_upper = min(
            entry.censored_count,
            entry.barrier_loss_first_count,
        )
        barrier_lower = (
            entry.barrier_profit_first_count * entry.profit_barrier_net_per_unit
            - entry.barrier_loss_first_count * entry.loss_barrier_net_per_unit
        ) / entry.filled_count
        barrier_upper = barrier_lower + (
            censored_losses_upper
            * entry.loss_barrier_net_per_unit
            / entry.filled_count
        )
        if (
            edge.expected_barrier_pnl_per_fill is None
            or edge.expected_barrier_pnl_per_fill < barrier_lower - 1e-9
            or edge.expected_barrier_pnl_per_fill > barrier_upper + 1e-9
        ):
            raise FreshTerminalAuditError(
                "entry barrier expectancy is inconsistent with outcomes"
            )
    elif edge.expected_barrier_pnl_per_fill is not None:
        raise FreshTerminalAuditError(
            "empty entry result has a barrier expectancy"
        )
    expected_baseline_gate = bool(
        edge.uplift_10 is not None
        and edge.uplift_30 is not None
        and edge.uplift_10 >= BASELINE_MINIMUM_UPLIFT
        and edge.uplift_30 >= BASELINE_MINIMUM_UPLIFT
        and edge.uplift_10_cluster_interval is not None
        and edge.uplift_30_cluster_interval is not None
        and edge.uplift_10_cluster_interval[0] > 0.0
        and edge.uplift_30_cluster_interval[0] > 0.0
    )
    if edge.baseline_gate_passed is not expected_baseline_gate:
        raise FreshTerminalAuditError("baseline uplift gate was not recomputed")


def _entry_rank_offset(candidate_id: str) -> float:
    matches = {
        "-rank-minus-": -0.05,
        "-rank-base-": 0.0,
        "-rank-plus-": 0.05,
    }
    selected = [value for marker, value in matches.items() if marker in candidate_id]
    if len(selected) != 1:
        raise FreshTerminalAuditError(
            f"entry candidate rank is not frozen: {candidate_id}"
        )
    return selected[0]


def _rank_promoted(
    audited: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> list[str]:
    passed = [item for item in audited if item["passed"] is True]
    ordered = sorted(
        passed,
        key=lambda item: (
            item["score"] is None,
            -float(item["score"])
            if item["score"] is not None
            else float("inf"),
            str(item["candidateId"]),
        ),
    )
    return [str(item["candidateId"]) for item in ordered[:limit]]


_CANDIDATE_BODY_KEYS = {
    "candidateId",
    "family",
    "stage",
    "trainingWindow",
    "evaluationWindow",
    "parameters",
    "entryVariant",
    "exitVariant",
    "metrics",
    "status",
    "leakageChecks",
    "role",
    "outcomesRevealed",
    "gatePassed",
    "identitySha256",
    "frozenEntrySha256",
    "frozenStrategySha256",
    "windowSha256",
    "balancedScore",
    "preregistrationSha256",
}

_ENTRY_LEAKAGE_CHECKS = {
    "sessionCorpusFingerprintVerifiedBeforeFeatures": True,
    "featureCalculationPrefixCausal": True,
    "signalsUseCurrentOrEarlierRowsOnly": True,
    "strictlyLaterBidAskFill": True,
    "holdoutRolePresent": False,
}

_STRATEGY_LEAKAGE_CHECKS = {
    "entryDefinitionUnchangedDuringExitSearch": True,
    "sessionCorpusFingerprintVerifiedBeforeReplay": True,
    "causalFeatureRowsBoundToEveryReplayTick": True,
    "stopsTriggerOnObservedExecutableQuote": True,
    "allFillsUseStrictlyLaterObservedQuotes": True,
}


def _verify_candidate_body_schema(
    record: Mapping[str, Any],
    *,
    stage: str,
    training: Sequence[str],
    evaluation: Sequence[str],
    role: str,
    window_sha: str,
    preregistration_sha: str,
) -> None:
    body_keys = set(record) - {"recordNumber", "recordSha256"}
    if body_keys != _CANDIDATE_BODY_KEYS:
        raise FreshTerminalAuditError(
            f"candidate ledger schema changed during {stage}"
        )
    expected = {
        "stage": stage,
        "trainingWindow": "+".join(training),
        "evaluationWindow": "+".join(evaluation),
        "role": role,
        "outcomesRevealed": True,
        "windowSha256": window_sha,
        "preregistrationSha256": preregistration_sha,
    }
    if any(
        not _canonical_equal(record.get(key), value)
        for key, value in expected.items()
    ):
        raise FreshTerminalAuditError(
            f"candidate context changed during {stage}"
        )


def _audit_entry_candidate_record(
    record: Mapping[str, Any],
    *,
    candidate: FrozenEntryCandidate,
    bank_row: Mapping[str, Any],
    source_candidate: Any,
    stage: str,
    split: Mapping[str, Any],
    preregistration: Mapping[str, Any],
    preregistration_sha: str,
    scoring: Any,
) -> dict[str, Any]:
    training, evaluation, role, kind, window_sha = _stage_context(split, stage)
    if kind != "entry":
        raise FreshTerminalAuditError("entry appeared in a strategy stage")
    _verify_candidate_body_schema(
        record,
        stage=stage,
        training=training,
        evaluation=evaluation,
        role=role,
        window_sha=window_sha,
        preregistration_sha=preregistration_sha,
    )
    expected_parameters = {
        "entryConfig": candidate.config,
        "thresholdBankSha256": candidate.threshold_bank_sha256,
    }
    if (
        record.get("candidateId") != candidate.candidate_id
        or record.get("family") != candidate.family
        or not _canonical_equal(record.get("parameters"), expected_parameters)
        or record.get("entryVariant") != candidate.entry_variant
        or record.get("exitVariant") != "entry-edge-only"
        or record.get("identitySha256") != candidate.entry_sha256
        or record.get("frozenEntrySha256") != candidate.entry_sha256
        or record.get("frozenStrategySha256") is not None
        or not _canonical_equal(
            record.get("leakageChecks"), _ENTRY_LEAKAGE_CHECKS
        )
    ):
        raise FreshTerminalAuditError(
            f"frozen entry identity changed: {candidate.candidate_id}"
        )
    metrics = record.get("metrics")
    expected_metric_keys = {
        "entry",
        "registeredGate",
        "entryEdge",
        "parameterNeighbourhood",
    }
    if not isinstance(metrics, Mapping) or set(metrics) != expected_metric_keys:
        raise FreshTerminalAuditError("entry evaluation metric schema changed")
    anchors = _evaluation_anchors(split, stage)
    report = _compact_entry_report(
        metrics["entry"],
        expected_anchors=anchors,
    )
    edge = _entry_edge(metrics["entryEdge"])
    _validate_edge_against_entry(edge, report.overall)
    gate = evaluate_entry_gate(
        report.overall,
        minimum_sample=scoring.minimum_sample,
        thresholds=scoring.entry_gate,
    )
    if not _canonical_equal(metrics["registeredGate"], asdict(gate)):
        raise FreshTerminalAuditError(
            f"registered entry gate was not reproduced: {candidate.candidate_id}"
        )
    score = _entry_rank_score(report, edge)
    if not _same_number(record.get("balancedScore"), score):
        raise FreshTerminalAuditError(
            f"entry rank score changed: {candidate.candidate_id}"
        )
    rank_offset = float(source_candidate.rank_offset)
    if rank_offset != _entry_rank_offset(candidate.candidate_id):
        raise FreshTerminalAuditError(
            f"entry rank label changed: {candidate.candidate_id}"
        )
    return {
        "candidateId": candidate.candidate_id,
        "score": score,
        "basePassed": bool(gate.passed and edge.baseline_gate_passed),
        "entry": report.overall,
        "edge": edge,
        "group": str(bank_row["robustnessGroup"]),
        "rankOffset": rank_offset,
        "parameterSignature": canonical_hash(
            [
                (item.parameter, item.final_value)
                for item in source_candidate.threshold_provenance
            ]
        ),
        "storedNeighbourhood": metrics["parameterNeighbourhood"],
        "record": record,
    }


def _strategy_scenario_ids(
    preregistration: Mapping[str, Any],
    *,
    stage: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    execution = preregistration["execution"]
    all_ids = tuple(str(item["id"]) for item in execution["scenarios"])
    required = tuple(
        str(item) for item in execution["requiredStressScenarioIds"]
    )
    sensitivities = tuple(
        item
        for item in all_ids
        if item not in {REFERENCE_SCENARIO_ID, *required}
    )
    if stage == "exit_search":
        sensitivities = ()
    return required, sensitivities


def _audit_strategy_candidate_record(
    record: Mapping[str, Any],
    *,
    candidate: FrozenStrategyCandidate,
    exit_variant: Mapping[str, Any],
    stage: str,
    split: Mapping[str, Any],
    preregistration: Mapping[str, Any],
    preregistration_sha: str,
    scoring: Any,
) -> dict[str, Any]:
    training, evaluation, role, kind, window_sha = _stage_context(split, stage)
    if kind != "strategy":
        raise FreshTerminalAuditError("strategy appeared in an entry stage")
    _verify_candidate_body_schema(
        record,
        stage=stage,
        training=training,
        evaluation=evaluation,
        role=role,
        window_sha=window_sha,
        preregistration_sha=preregistration_sha,
    )
    expected_parameters = {
        "entryCandidateId": candidate.entry.candidate_id,
        "entryConfig": candidate.entry.config,
        "exitConfig": candidate.exit_config,
        "executionConfig": candidate.execution_config,
    }
    if (
        record.get("candidateId") != candidate.strategy_id
        or record.get("family") != candidate.entry.family
        or not _canonical_equal(record.get("parameters"), expected_parameters)
        or record.get("entryVariant") != candidate.entry.entry_variant
        or record.get("exitVariant") != candidate.exit_variant
        or record.get("identitySha256") != candidate.strategy_sha256
        or record.get("frozenEntrySha256") != candidate.entry.entry_sha256
        or record.get("frozenStrategySha256") != candidate.strategy_sha256
        or not _canonical_equal(
            record.get("leakageChecks"), _STRATEGY_LEAKAGE_CHECKS
        )
    ):
        raise FreshTerminalAuditError(
            f"frozen strategy identity changed: {candidate.strategy_id}"
        )
    metrics = record.get("metrics")
    expected_metric_keys = {
        "entry",
        "entryEdge",
        "reference",
        "stresses",
        "sensitivities",
        "registeredEntryGate",
        "registeredFullGate",
        "balancedScore",
        "exitParameterNeighbourhood",
    }
    if not isinstance(metrics, Mapping) or set(metrics) != expected_metric_keys:
        raise FreshTerminalAuditError("strategy evaluation metric schema changed")
    required_ids, sensitivity_ids = _strategy_scenario_ids(
        preregistration,
        stage=stage,
    )
    stresses = metrics["stresses"]
    sensitivities = metrics["sensitivities"]
    if (
        not isinstance(stresses, Mapping)
        or set(stresses) != set(required_ids)
        or not isinstance(sensitivities, Mapping)
        or set(sensitivities) != set(sensitivity_ids)
    ):
        raise FreshTerminalAuditError(
            f"execution scenario results changed during {stage}"
        )
    anchors = _evaluation_anchors(split, stage)
    entry_report = _compact_entry_report(
        metrics["entry"],
        expected_anchors=anchors,
    )
    edge = _entry_edge(metrics["entryEdge"])
    _validate_edge_against_entry(edge, entry_report.overall)
    reference = _compact_trade_report(
        metrics["reference"],
        expected_anchors=anchors,
        pnl_tolerance=scoring.trade_metrics.pnl_classification_tolerance,
    )
    stress_reports = {
        scenario_id: _compact_trade_report(
            stresses[scenario_id],
            expected_anchors=anchors,
            pnl_tolerance=scoring.trade_metrics.pnl_classification_tolerance,
        )
        for scenario_id in required_ids
    }
    for scenario_id in sensitivity_ids:
        _compact_trade_report(
            sensitivities[scenario_id],
            expected_anchors=anchors,
            pnl_tolerance=scoring.trade_metrics.pnl_classification_tolerance,
        )
    scorecard = build_candidate_scorecard(
        entry_report,
        reference,
        stress_reports,
        config=scoring,
    )
    if (
        not _canonical_equal(
            metrics["registeredEntryGate"], asdict(scorecard.entry_gate)
        )
        or not _canonical_equal(
            metrics["registeredFullGate"], asdict(scorecard.full_gate)
        )
        or not _canonical_equal(
            metrics["balancedScore"], asdict(scorecard.balanced_score)
        )
        or not _same_number(
            record.get("balancedScore"),
            scorecard.balanced_score.score,
        )
    ):
        raise FreshTerminalAuditError(
            f"strategy gates/score were not reproduced: {candidate.strategy_id}"
        )
    group = "::".join(
        (
            str(exit_variant["stop_structure_id"]),
            str(exit_variant["management_structure_id"]),
            str(exit_variant["invalidation_structure_id"]),
        )
    )
    signature = canonical_hash(
        {
            "policy": exit_variant["policy"],
            "weakening": exit_variant["weakening"],
        }
    )
    return {
        "candidateId": candidate.strategy_id,
        "score": scorecard.balanced_score.score,
        "basePassed": bool(
            scorecard.full_gate.passed and edge.baseline_gate_passed
        ),
        "entry": entry_report.overall,
        "edge": edge,
        "reference": reference.overall,
        "group": group,
        "rankOffset": float(exit_variant["rank_offset"]),
        "parameterSignature": signature,
        "storedNeighbourhood": metrics["exitParameterNeighbourhood"],
        "record": record,
    }


def _verify_strategy_stage_entry_invariance(
    audited: Sequence[Mapping[str, Any]],
    *,
    stage: str,
) -> None:
    if not audited:
        return
    first_metrics = audited[0]["record"]["metrics"]
    expected_entry = first_metrics["entry"]
    expected_edge = first_metrics["entryEdge"]
    for item in audited[1:]:
        metrics = item["record"]["metrics"]
        if (
            not _canonical_equal(metrics["entry"], expected_entry)
            or not _canonical_equal(metrics["entryEdge"], expected_edge)
        ):
            raise FreshTerminalAuditError(
                f"strategy entry evidence changed within {stage}"
            )


def _finalize_candidate_neighbourhoods(
    audited: Sequence[dict[str, Any]],
    *,
    stage: str,
    preregistration: Mapping[str, Any],
) -> list[dict[str, Any]]:
    parameter_spec = preregistration["robustnessAndGates"][
        "parameterNeighborhood"
    ]
    required = stage in {"discovery", "exit_search"}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in audited:
        grouped.setdefault(str(item["group"]), []).append(item)
    audits: dict[str, Mapping[str, Any]] = {}
    if required:
        for group, members in grouped.items():
            if len(members) != 3:
                raise FreshTerminalAuditError(
                    f"incomplete parameter neighbourhood: {group}"
                )
            audits[group] = _parameter_neighbourhood_audit(
                tuple(
                    (
                        str(item["candidateId"]),
                        float(item["rankOffset"]),
                        bool(item["basePassed"]),
                        (
                            item["edge"].expected_barrier_pnl_per_fill
                            if stage == "discovery"
                            else item["reference"].expectancy
                        ),
                        (
                            item["entry"].coverage_probability(30)
                            if stage == "discovery"
                            else None
                        ),
                        str(item["parameterSignature"]),
                    )
                    for item in members
                ),
                minimum_valid_neighbor_fraction=float(
                    parameter_spec["minimumValidNeighborFraction"]
                ),
                minimum_positive_expectancy_neighbor_fraction=float(
                    parameter_spec[
                        "minimumPositiveExpectancyNeighborFraction"
                    ]
                ),
                minimum_neighbor_expectancy_retention=float(
                    parameter_spec["minimumNeighborExpectancyRetention"]
                ),
                maximum_absolute_coverage_30_drop=(
                    float(
                        parameter_spec[
                            "maximumAbsoluteCoverage30SecondDrop"
                        ]
                    )
                    if stage == "discovery"
                    else None
                ),
            )
    finalized: list[dict[str, Any]] = []
    for item in audited:
        group = str(item["group"])
        neighbourhood = audits.get(
            group,
            {
                "centerCandidateId": item["candidateId"],
                "evaluatedCount": 1,
                "passed": True,
            },
        )
        is_center = float(item["rankOffset"]) == 0.0
        expected = {
            **neighbourhood,
            "group": group,
            "requiredDuringStage": required,
            "candidateIsCenter": is_center,
        }
        if not _canonical_equal(item["storedNeighbourhood"], expected):
            raise FreshTerminalAuditError(
                f"parameter neighbourhood changed: {item['candidateId']}"
            )
        passed = bool(
            item["basePassed"]
            and (not required or (is_center and bool(neighbourhood["passed"])))
        )
        record = item["record"]
        if (
            record.get("gatePassed") is not passed
            or record.get("status") != ("passed" if passed else "rejected")
        ):
            raise FreshTerminalAuditError(
                f"candidate pass status was not reproduced: {item['candidateId']}"
            )
        finalized.append({**item, "passed": passed})
    return finalized


def _strongest_record(
    records: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    order = {stage: index for index, stage in enumerate(_STAGES)}
    eligible = [
        record
        for record in records
        if record.get("stage") in order
        and isinstance(record.get("balancedScore"), (int, float))
        and not isinstance(record.get("balancedScore"), bool)
        and math.isfinite(float(record["balancedScore"]))
    ]
    if not eligible:
        return None
    furthest = max(order[str(record["stage"])] for record in eligible)
    selected = [
        record for record in eligible if order[str(record["stage"])] == furthest
    ]
    return min(
        selected,
        key=lambda record: (
            -float(record["balancedScore"]),
            str(record["candidateId"]),
        ),
    )


def _failed_checks(record: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(record, Mapping):
        return ["no candidate produced a finite ranked score"]
    metrics = record.get("metrics")
    if not isinstance(metrics, Mapping):
        return ["candidate metrics were unavailable"]
    names: list[str] = []
    for key in ("registeredGate", "registeredEntryGate", "registeredFullGate"):
        gate = metrics.get(key)
        checks = gate.get("checks") if isinstance(gate, Mapping) else None
        if isinstance(checks, list):
            for check in checks:
                if (
                    isinstance(check, Mapping)
                    and check.get("passed") is False
                    and isinstance(check.get("name"), str)
                ):
                    names.append(check["name"])
    edge = metrics.get("entryEdge")
    if isinstance(edge, Mapping) and edge.get("baseline_gate_passed") is False:
        names.append("baseline_uplift_gate")
    neighbourhood = metrics.get("parameterNeighbourhood")
    if (
        isinstance(neighbourhood, Mapping)
        and neighbourhood.get("requiredDuringStage") is True
        and neighbourhood.get("passed") is False
    ):
        names.append("entry_parameter_neighbourhood")
    exit_neighbourhood = metrics.get("exitParameterNeighbourhood")
    if (
        isinstance(exit_neighbourhood, Mapping)
        and exit_neighbourhood.get("requiredDuringStage") is True
        and exit_neighbourhood.get("passed") is False
    ):
        names.append("exit_parameter_neighbourhood")
    return sorted(set(names)) or ["the terminal stage promoted no eligible candidate"]


def _verify_stage_and_ledger(
    summary: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    preregistration_sha: str,
    *,
    split: Mapping[str, Any],
    preregistration: Mapping[str, Any],
    frozen: _FrozenAuditInputs,
) -> tuple[str, bool]:
    stages = summary.get("stageResults")
    if not isinstance(stages, list) or not stages:
        raise FreshTerminalAuditError("run summary has no completed stage")
    names = [item.get("stage") if isinstance(item, Mapping) else None for item in stages]
    if names != list(_STAGES[: len(names)]):
        raise FreshTerminalAuditError("completed stages are not a chronological prefix")

    budgets = preregistration.get("candidateSearch", {}).get("budgets")
    expected_budgets = {
        "discoveryDistinctCandidates": 240,
        "discoveryPerFamilyMaximum": 60,
        "walkForward1FrozenCandidates": 24,
        "walkForward2FrozenCandidates": 8,
        "exitSearchFrozenEntries": 1,
        "exitVariantsAfterEntryGate": 96,
        "walkForward3FullStrategies": 3,
        "validationFullStrategies": 1,
        "holdoutFullStrategies": 1,
    }
    if not _canonical_equal(budgets, expected_budgets):
        raise FreshTerminalAuditError("registered candidate budgets changed")
    if (
        len(frozen.entries) != expected_budgets["discoveryDistinctCandidates"]
        or tuple(frozen.entries_by_id) != tuple(
            candidate.candidate_id for candidate in frozen.entries
        )
    ):
        raise FreshTerminalAuditError("frozen discovery sequence changed")
    family_counts: dict[str, int] = {}
    for candidate in frozen.entries:
        family_counts[candidate.family] = family_counts.get(candidate.family, 0) + 1
    if any(
        count > expected_budgets["discoveryPerFamilyMaximum"]
        for count in family_counts.values()
    ):
        raise FreshTerminalAuditError("discovery family budget was exceeded")

    _candidate_records(records)
    stage_order = {stage: index for index, stage in enumerate(_STAGES)}
    previous_stage = -1
    records_by_stage: dict[str, list[Mapping[str, Any]]] = {
        stage: [] for stage in _STAGES
    }
    for record in records:
        stage = record.get("stage")
        if stage not in stage_order:
            raise FreshTerminalAuditError("ledger contains an unknown stage")
        ordinal = stage_order[str(stage)]
        if ordinal < previous_stage:
            raise FreshTerminalAuditError("ledger stage chronology is invalid")
        previous_stage = ordinal
        records_by_stage[str(stage)].append(record)

    scoring = scoring_config_from_preregistration(
        preregistration,
        verify_current_implementation_files=False,
    )
    promotion_limits = {
        "discovery": expected_budgets["walkForward1FrozenCandidates"],
        "walk_forward_1": expected_budgets["walkForward2FrozenCandidates"],
        "walk_forward_2": expected_budgets["exitSearchFrozenEntries"],
        "exit_search": expected_budgets["walkForward3FullStrategies"],
        "walk_forward_3": expected_budgets["validationFullStrategies"],
        "validation": expected_budgets["holdoutFullStrategies"],
        "holdout": expected_budgets["holdoutFullStrategies"],
    }
    flattened: list[int] = []
    prior_failed = False
    prior_promoted: list[str] = []
    for item in stages:
        if not isinstance(item, Mapping) or set(item) != {
            "stage",
            "evaluated_ids",
            "promoted_ids",
            "ledger_record_numbers",
            "study_failed",
        }:
            raise FreshTerminalAuditError("stage result schema changed")
        stage = str(item["stage"])
        evaluated = item["evaluated_ids"]
        promoted = item["promoted_ids"]
        numbers = item["ledger_record_numbers"]
        failed = item["study_failed"]
        if (
            not isinstance(evaluated, list)
            or not isinstance(promoted, list)
            or not isinstance(numbers, list)
            or not isinstance(failed, bool)
            or len(evaluated) != len(set(evaluated))
            or len(promoted) != len(set(promoted))
            or any(
                isinstance(number, bool) or not isinstance(number, int)
                for number in numbers
            )
        ):
            raise FreshTerminalAuditError(f"stage result is malformed: {stage}")
        if prior_failed:
            raise FreshTerminalAuditError("a stage ran after a failed stage")

        training, evaluation, role, kind, window_sha = _stage_context(
            split, stage
        )
        if stage == "discovery":
            expected_candidates: Sequence[
                FrozenEntryCandidate | FrozenStrategyCandidate
            ] = frozen.entries
        elif stage in {"walk_forward_1", "walk_forward_2"}:
            try:
                expected_candidates = tuple(
                    frozen.entries_by_id[candidate_id]
                    for candidate_id in prior_promoted
                )
            except KeyError as error:
                raise FreshTerminalAuditError(
                    "entry promotion continuity changed"
                ) from error
        elif stage == "exit_search":
            if (
                len(prior_promoted) != 1
                or not frozen.strategies
                or any(
                    candidate.entry.candidate_id != prior_promoted[0]
                    for candidate in frozen.strategies
                )
            ):
                raise FreshTerminalAuditError(
                    "exit search is not bound to the sole WF2 winner"
                )
            expected_candidates = frozen.strategies
        else:
            try:
                expected_candidates = tuple(
                    frozen.strategies_by_id[candidate_id]
                    for candidate_id in prior_promoted
                )
            except KeyError as error:
                raise FreshTerminalAuditError(
                    "strategy promotion continuity changed"
                ) from error
        expected_ids = [
            (
                candidate.candidate_id
                if isinstance(candidate, FrozenEntryCandidate)
                else candidate.strategy_id
            )
            for candidate in expected_candidates
        ]
        expected_sha = [
            (
                candidate.entry_sha256
                if isinstance(candidate, FrozenEntryCandidate)
                else candidate.strategy_sha256
            )
            for candidate in expected_candidates
        ]
        if evaluated != expected_ids:
            raise FreshTerminalAuditError(
                f"stage evaluated IDs changed: {stage}"
            )

        stage_records = records_by_stage[stage]
        expected_record_count = len(expected_candidates) + 2 + (
            1 if stage == "discovery" else 0
        )
        if len(stage_records) != expected_record_count:
            raise FreshTerminalAuditError(
                f"stage ledger block length changed: {stage}"
            )
        cursor = 0
        if stage == "discovery":
            _exact_record_body(
                stage_records[cursor],
                _expected_stage_access_record(
                    stage=stage,
                    training=training,
                    evaluation=evaluation,
                    role=role,
                    window_sha=window_sha,
                    preregistration_sha=preregistration_sha,
                ),
                label="discovery stage-access record",
            )
            cursor += 1
        for batch_status in (
            "batch_access_started",
            "batch_access_completed",
        ):
            _exact_record_body(
                stage_records[cursor],
                _expected_batch_access_record(
                    kind=kind,
                    status=batch_status,
                    stage=stage,
                    training=training,
                    evaluation=evaluation,
                    role=role,
                    candidate_ids=expected_ids,
                    candidate_sha256=expected_sha,
                    window_sha=window_sha,
                    preregistration_sha=preregistration_sha,
                ),
                label=f"{stage} {batch_status} record",
            )
            cursor += 1
        candidate_rows = stage_records[cursor:]
        if [record.get("candidateId") for record in candidate_rows] != expected_ids:
            raise FreshTerminalAuditError(
                f"candidate ledger order changed during {stage}"
            )

        provisional: list[dict[str, Any]] = []
        for candidate, record in zip(expected_candidates, candidate_rows):
            if isinstance(candidate, FrozenEntryCandidate):
                provisional.append(
                    _audit_entry_candidate_record(
                        record,
                        candidate=candidate,
                        bank_row=frozen.entry_bank_by_id[
                            candidate.candidate_id
                        ],
                        source_candidate=frozen.entry_source_by_id[
                            candidate.candidate_id
                        ],
                        stage=stage,
                        split=split,
                        preregistration=preregistration,
                        preregistration_sha=preregistration_sha,
                        scoring=scoring,
                    )
                )
            else:
                provisional.append(
                    _audit_strategy_candidate_record(
                        record,
                        candidate=candidate,
                        exit_variant=frozen.exit_variant_by_strategy_id[
                            candidate.strategy_id
                        ],
                        stage=stage,
                        split=split,
                        preregistration=preregistration,
                        preregistration_sha=preregistration_sha,
                        scoring=scoring,
                    )
                )
        if kind == "strategy":
            _verify_strategy_stage_entry_invariance(
                provisional,
                stage=stage,
            )
        audited = _finalize_candidate_neighbourhoods(
            provisional,
            stage=stage,
            preregistration=preregistration,
        )
        expected_promoted = _rank_promoted(
            audited,
            limit=promotion_limits[stage],
        )
        if stage == "holdout":
            expected_promoted = [
                str(candidate["candidateId"])
                for candidate in audited
                if candidate["passed"] is True
            ]
        if promoted != expected_promoted:
            raise FreshTerminalAuditError(
                f"stage promotion ranking changed: {stage}"
            )
        expected_failure = (
            len(expected_promoted) != 1
            if stage in {"walk_forward_3", "validation"}
            else not bool(expected_promoted)
        )
        if stage == "holdout":
            expected_failure = not bool(expected_promoted)
        if failed != expected_failure:
            raise FreshTerminalAuditError(f"stage failure flag is invalid: {stage}")
        actual_numbers = [
            int(record["recordNumber"]) for record in stage_records
        ]
        if numbers != actual_numbers:
            raise FreshTerminalAuditError(
                f"stage ledger-number binding changed: {stage}"
            )
        flattened.extend(actual_numbers)
        prior_failed = failed
        prior_promoted = list(expected_promoted)

    if flattened != list(range(1, len(records) + 1)):
        raise FreshTerminalAuditError("stage results do not cover the ledger exactly")
    terminal_stage = str(stages[-1]["stage"])
    holdout_opened = terminal_stage == "holdout"
    if not holdout_opened and stages[-1]["study_failed"] is not True:
        raise FreshTerminalAuditError(
            "successful process stopped before a scientific terminal gate"
        )
    if summary.get("holdoutOpened") is not holdout_opened:
        raise FreshTerminalAuditError("summary holdout-opened flag is invalid")
    expected_status = (
        "validated_holdout_pass"
        if holdout_opened and stages[-1]["study_failed"] is False
        else "no_robust_setup_survived_frozen_validation"
    )
    if summary.get("status") != expected_status:
        raise FreshTerminalAuditError("summary scientific status is invalid")
    if not _canonical_equal(
        summary.get("strongestRecord"),
        _strongest_record(records),
    ):
        raise FreshTerminalAuditError("summary strongest record is not reproducible")
    return terminal_stage, expected_status == "validated_holdout_pass"


def _verify_holdout_evidence(
    *,
    split: Mapping[str, Any],
    preregistration_sha: str,
    records: Sequence[Mapping[str, Any]],
    authorization: Mapping[str, Any],
    strategy: Mapping[str, Any],
    validated: bool,
    frozen: _FrozenAuditInputs,
) -> Mapping[str, Any]:
    expected_strategy_keys = {
        "schema",
        "strategyId",
        "strategySha256",
        "entryCandidateId",
        "entrySha256",
        "entryConfig",
        "exitConfig",
        "executionConfig",
        "noPostHoldoutTuning",
    }
    strategy_id = strategy.get("strategyId")
    reconstructed = (
        frozen.strategies_by_id.get(strategy_id)
        if isinstance(strategy_id, str)
        else None
    )
    expected_strategy = (
        {
            "schema": "fresh-xauusd-final-strategy/v1",
            "strategyId": reconstructed.strategy_id,
            "strategySha256": reconstructed.strategy_sha256,
            "entryCandidateId": reconstructed.entry.candidate_id,
            "entrySha256": reconstructed.entry.entry_sha256,
            "entryConfig": reconstructed.entry.config,
            "exitConfig": reconstructed.exit_config,
            "executionConfig": reconstructed.execution_config,
            "noPostHoldoutTuning": True,
        }
        if reconstructed is not None
        else None
    )
    if (
        set(strategy) != expected_strategy_keys
        or expected_strategy is None
        or not _canonical_equal(strategy, expected_strategy)
    ):
        raise FreshTerminalAuditError("frozen final strategy is invalid")
    strategy_sha = str(strategy["strategySha256"])
    candidate_records = _candidate_records(records)
    by_role = {
        role: [
            record
            for record in candidate_records
            if record.get("role") == role
            and record.get("frozenStrategySha256") == strategy_sha
        ]
        for role in ("walk_forward_3", "validation", "holdout")
    }
    if (
        len(
            [
                item
                for item in by_role["walk_forward_3"]
                if item.get("gatePassed") is True
            ]
        )
        != 1
        or len(by_role["validation"]) != 1
        or by_role["validation"][0].get("gatePassed") is not True
        or len(by_role["holdout"]) != 1
        or by_role["holdout"][0].get("gatePassed") is not validated
    ):
        raise FreshTerminalAuditError("holdout winner chronology is inconsistent")
    validation = by_role["validation"][0]
    holdout = by_role["holdout"][0]
    wf3 = next(
        item
        for item in by_role["walk_forward_3"]
        if item.get("gatePassed") is True
    )
    for record in (wf3, validation, holdout):
        if (
            record.get("candidateId") != strategy["strategyId"]
            or record.get("frozenEntrySha256") != strategy["entrySha256"]
            or record.get("identitySha256") != strategy_sha
        ):
            raise FreshTerminalAuditError("frozen strategy identity changed by stage")
        parameters = record.get("parameters")
        if not isinstance(parameters, Mapping) or any(
            not _canonical_equal(parameters.get(key), strategy[value])
            for key, value in (
                ("entryConfig", "entryConfig"),
                ("exitConfig", "exitConfig"),
                ("executionConfig", "executionConfig"),
            )
        ):
            raise FreshTerminalAuditError("strategy configuration changed by stage")
        leakage = record.get("leakageChecks")
        if (
            not isinstance(leakage, Mapping)
            or not leakage
            or any(value is not True for value in leakage.values())
        ):
            raise FreshTerminalAuditError("strategy causality evidence is incomplete")
    body = dict(authorization)
    claimed = body.pop("authorizationSha256", None)
    expected_authorization_keys = {
        "schemaVersion",
        "role",
        "window",
        "splitManifestSha256",
        "frozenStrategySha256",
        "outcomesRevealed",
        "preregistrationSha256",
        "walkForward3EvidenceSha256",
        "validationEvidenceSha256",
        "authorizationSha256",
    }
    if (
        set(authorization) != expected_authorization_keys
        or type(authorization.get("schemaVersion")) is not int
        or authorization.get("schemaVersion") != 2
        or canonical_hash(body) != claimed
    ):
        raise FreshTerminalAuditError("holdout authorization hash is invalid")
    expected_window = split.get("windows", {}).get("holdout")
    if (
        authorization.get("role") != "holdout"
        or authorization.get("outcomesRevealed") is not False
        or authorization.get("frozenStrategySha256") != strategy_sha
        or authorization.get("preregistrationSha256") != preregistration_sha
        or authorization.get("splitManifestSha256") != split.get("manifestSha256")
        or not _canonical_equal(authorization.get("window"), expected_window)
        or authorization.get("walkForward3EvidenceSha256") != canonical_hash(wf3)
        or authorization.get("validationEvidenceSha256")
        != canonical_hash(validation)
    ):
        raise FreshTerminalAuditError("holdout authorization is not bound to evidence")
    return holdout


def _headline_metrics(record: Mapping[str, Any]) -> dict[str, Any]:
    metrics = record.get("metrics")
    if not isinstance(metrics, Mapping):
        raise FreshTerminalAuditError("winner metrics are unavailable")
    entry = metrics.get("entry")
    reference = metrics.get("reference")
    if not isinstance(entry, Mapping) or not isinstance(reference, Mapping):
        raise FreshTerminalAuditError("winner entry/trade metrics are unavailable")
    entry_overall = entry.get("overall")
    trade_overall = reference.get("overall")
    if not isinstance(entry_overall, Mapping) or not isinstance(
        trade_overall, Mapping
    ):
        raise FreshTerminalAuditError("winner overall metrics are unavailable")
    required_trade = (
        "trade_count",
        "win_rate",
        "net_pnl",
        "expectancy",
        "profit_factor",
        "maximum_drawdown",
    )
    if any(key not in trade_overall for key in required_trade):
        raise FreshTerminalAuditError("winner headline trade metrics are incomplete")
    return {
        "tradeCount": trade_overall["trade_count"],
        "winRate": trade_overall["win_rate"],
        "netPnl": trade_overall["net_pnl"],
        "expectancy": trade_overall["expectancy"],
        "profitFactor": trade_overall["profit_factor"],
        "maximumDrawdown": trade_overall["maximum_drawdown"],
        "spreadCoverageProbabilities": entry_overall.get(
            "coverage_probabilities"
        ),
        "restrictedMedianCoverageMilliseconds": entry_overall.get(
            "restricted_median_coverage_milliseconds"
        ),
    }


def _winner_candidate_records(
    records: Sequence[Mapping[str, Any]],
    strategy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    entry_sha = strategy.get("entrySha256")
    strategy_sha = strategy.get("strategySha256")
    return [
        copy.deepcopy(dict(record))
        for record in _candidate_records(records)
        if (
            record.get("frozenStrategySha256") is None
            and record.get("frozenEntrySha256") == entry_sha
        )
        or record.get("frozenStrategySha256") == strategy_sha
    ]


def _infrastructure_failure_report(exit_status: int) -> dict[str, Any]:
    if (
        isinstance(exit_status, bool)
        or not isinstance(exit_status, int)
        or not 1 <= exit_status <= 255
    ):
        raise FreshTerminalAuditError("infrastructure exit status is invalid")
    return {
        "schema": AUDIT_SCHEMA,
        "status": "infrastructure_failure",
        "scientificConclusionAvailable": False,
        "processExitStatus": exit_status,
        "archiveIntegrityVerified": True,
        "userFacingLead": (
            "The detached study ended with an infrastructure/process "
            "failure; its archive is not evidence of a strategy no-pass."
        ),
    }


def _verify_frozen_launch_implementation(
    implementation: Mapping[str, Any],
    *,
    launch_worktree: str,
) -> None:
    expected_paths = tuple(sorted(FROZEN_V5_IMPLEMENTATION_FILE_SHA256))
    if tuple(required_fresh_v5_implementation_files()) != expected_paths:
        raise FreshTerminalAuditError(
            "local v5 implementation-file contract changed"
        )
    files = implementation.get("files")
    expected_files = [
        {
            "path": path,
            "sha256": FROZEN_V5_IMPLEMENTATION_FILE_SHA256[path],
        }
        for path in expected_paths
    ]
    if (
        implementation.get("schema")
        != "fresh-xauusd-implementation-manifest/v1"
        or implementation.get("repositoryRoot") != launch_worktree
        or not _canonical_equal(files, expected_files)
        or implementation.get("manifestSha256")
        != FROZEN_V5_IMPLEMENTATION_MANIFEST_SHA256
    ):
        raise FreshTerminalAuditError(
            "implementation manifest is not the exact frozen launch source"
        )


def _verify_local_frozen_scientific_runtime() -> None:
    """Bind imported production helpers to the exact launch implementation."""

    repository_root = Path(__file__).resolve().parents[2]
    closure = {
        "schema": "fresh-xauusd-v5-audit-runtime-closure/v1",
        "files": [
            {"path": path, "sha256": FROZEN_LOCAL_RUNTIME_CLOSURE_SHA256[path]}
            for path in sorted(FROZEN_LOCAL_RUNTIME_CLOSURE_SHA256)
        ],
    }
    if (
        canonical_hash(closure)
        != FROZEN_LOCAL_RUNTIME_CLOSURE_MANIFEST_SHA256
    ):
        raise FreshTerminalAuditError(
            "frozen local audit-runtime closure identity changed"
        )
    for relative, expected_sha in FROZEN_LOCAL_RUNTIME_CLOSURE_SHA256.items():
        path = repository_root / PurePosixPath(relative)
        try:
            _regular_file(
                path,
                f"local frozen implementation file {relative}",
                maximum_bytes=MAX_IMPLEMENTATION_SOURCE_BYTES,
            )
            actual_sha = _sha256_file(
                path,
                maximum_bytes=MAX_IMPLEMENTATION_SOURCE_BYTES,
            )
        except (OSError, ValueError) as error:
            raise FreshTerminalAuditError(
                f"local frozen implementation file is unavailable: {relative}"
            ) from error
        if actual_sha != expected_sha:
            raise FreshTerminalAuditError(
                f"local scientific runtime differs from launch source: {relative}"
            )


def _scientific_audit(
    *,
    bundle: _ArchiveBundle,
    members: Mapping[str, tarfile.TarInfo],
    launch: Mapping[str, Any],
) -> dict[str, Any]:
    _verify_local_frozen_scientific_runtime()
    missing = (_BASE_INNER_MEMBERS | _TERMINAL_MEMBERS) - set(members)
    if missing:
        raise FreshTerminalAuditError(
            "successful archive is missing: " + ", ".join(sorted(missing))
        )
    inherited_hashes = {
        **RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256,
        "predecessor_fresh_research_state_binding_v3.json": (
            RUN19_MEMBER_FILE_SHA256["fresh_research_state_binding_v3.json"]
        ),
        "predecessor_fresh_experiment_ledger_v1.jsonl": (
            RUN19_MEMBER_FILE_SHA256["fresh_experiment_ledger_v1.jsonl"]
        ),
        "predecessor_fresh_preregistration_v4.json": (
            RUN19_MEMBER_FILE_SHA256["fresh_preregistration_v4.json"]
        ),
        "predecessor_fresh_implementation_manifest_v1.json": (
            RUN19_MEMBER_FILE_SHA256["fresh_implementation_manifest_v1.json"]
        ),
    }
    for name, expected in inherited_hashes.items():
        if _sha256_bytes(_read_member(bundle, members, name)) != expected:
            raise FreshTerminalAuditError(f"frozen inherited input changed: {name}")

    split = _json_object(
        _read_member(bundle, members, "fresh_split_manifest_v2.json"),
        "split manifest",
    )
    split_body = dict(split)
    split_sha = split_body.pop("manifestSha256", None)
    if (
        canonical_hash(split_body) != split_sha
        or split_sha
        != canonical_fresh_v5_study_lineage()["splitManifestSha256"]
    ):
        raise FreshTerminalAuditError("split manifest identity is invalid")
    windows = split.get("windows")
    if not isinstance(windows, Mapping) or set(windows) != {
            "discovery",
            "walk_forward_1",
            "walk_forward_2",
            "walk_forward_3",
            "validation",
            "holdout",
    }:
        raise FreshTerminalAuditError("split windows are incomplete")
    expected_counts = {
        "discovery": 40,
        "walk_forward_1": 10,
        "walk_forward_2": 10,
        "walk_forward_3": 10,
        "validation": 15,
        "holdout": 30,
    }
    if any(
        not isinstance(windows[role], Mapping)
        or windows[role].get("sessionCount") != count
        for role, count in expected_counts.items()
    ):
        raise FreshTerminalAuditError("split window counts changed")
    corpus = _json_object(
        _read_member(bundle, members, "fresh_corpus_manifest_v1.json"),
        "corpus manifest",
    )
    corpus_body = dict(corpus)
    corpus_sha = corpus_body.pop("corpusManifestSha256", None)
    if canonical_hash(corpus_body) != corpus_sha:
        raise FreshTerminalAuditError("corpus manifest identity is invalid")

    implementation = _json_object(
        _read_member(bundle, members, "fresh_implementation_manifest_v1.json"),
        "implementation manifest",
    )
    implementation_sha = validate_fresh_implementation_manifest(
        implementation,
        verify_current_files=False,
    )
    launch_paths = launch.get("paths")
    if not isinstance(launch_paths, Mapping):
        raise FreshTerminalAuditError("launch receipt paths are unavailable")
    _verify_frozen_launch_implementation(
        implementation,
        launch_worktree=str(launch_paths["worktree"]),
    )
    preregistration = _json_object(
        _read_member(bundle, members, "fresh_preregistration_v5.json"),
        "v5 preregistration",
    )
    preregistration_sha = validate_fresh_preregistration_v5(
        preregistration,
        verify_current_implementation_files=False,
    )
    source = preregistration.get("sourceBindings")
    restart = preregistration.get("infrastructureRestart")
    if (
        not isinstance(source, Mapping)
        or not _canonical_equal(
            source.get("implementationManifest"), implementation
        )
        or source.get("implementationManifestSha256") != implementation_sha
        or source.get("splitManifestSha256") != split_sha
        or not isinstance(restart, Mapping)
        or restart.get("scientificSpecificationSha256")
        != RUN19_SCIENTIFIC_SPECIFICATION_SHA256
        or restart.get("studyLineageSha256")
        != RUN19_V5_STUDY_LINEAGE_SHA256
    ):
        raise FreshTerminalAuditError("preregistration source binding changed")

    quantile_bank = _json_object(
        _read_member(bundle, members, "fresh_quantile_bank_v1.json"),
        "frozen quantile bank",
    )
    entry_bank = _json_object(
        _read_member(bundle, members, "fresh_entry_bank_v1.json"),
        "frozen entry bank",
    )
    (
        entries,
        entries_by_id,
        entry_bank_by_id,
        entry_source_by_id,
    ) = _reconstruct_entries(
        entry_bank,
        quantile_bank,
        preregistration,
    )
    exit_bank = (
        _json_object(
            _read_member(bundle, members, "fresh_exit_bank_v1.json"),
            "frozen exit bank",
        )
        if "fresh_exit_bank_v1.json" in members
        else None
    )
    selected_entry: FrozenEntryCandidate | None = None
    if exit_bank is not None:
        selected_id = exit_bank.get("selectedEntryCandidateId")
        if not isinstance(selected_id, str) or selected_id not in entries_by_id:
            raise FreshTerminalAuditError(
                "exit bank selected an unknown entry candidate"
            )
        selected_entry = entries_by_id[selected_id]
    (
        strategies,
        strategies_by_id,
        exit_variant_by_strategy_id,
    ) = _reconstruct_strategies(
        exit_bank=exit_bank,
        selected_entry=selected_entry,
        quantile_bank=quantile_bank,
        preregistration=preregistration,
    )
    frozen_inputs = _FrozenAuditInputs(
        entries=entries,
        entries_by_id=entries_by_id,
        entry_bank_by_id=entry_bank_by_id,
        entry_source_by_id=entry_source_by_id,
        strategies=strategies,
        strategies_by_id=strategies_by_id,
        exit_variant_by_strategy_id=exit_variant_by_strategy_id,
    )

    state = _json_object(
        _read_member(bundle, members, "fresh_research_state_binding_v4.json"),
        "v5 state binding",
    )
    lineage = canonical_fresh_v5_study_lineage()
    window_set_sha = canonical_hash(
        [
            canonical_hash(windows[role])
            for role in (
                "discovery",
                "walk_forward_1",
                "walk_forward_2",
                "walk_forward_3",
                "validation",
                "holdout",
            )
        ]
    )
    holdout_window_sha = canonical_hash(windows["holdout"])
    lineage_root = (
        FROZEN_STATE_ROOT / "studies" / window_set_sha / "lineages"
    )
    expected_state = {
        "schema": "fresh-xauusd-durable-research-state/v4",
        **{key: value for key, value in lineage.items() if key != "schema"},
        "studyLineage": lineage,
        "studyLineageSha256": RUN19_V5_STUDY_LINEAGE_SHA256,
        "holdoutWindowSha256": holdout_window_sha,
        "stateDirectory": str(FROZEN_STATE_ROOT),
        "predecessorExperimentLedgerPath": str(
            lineage_root
            / RUN19_STUDY_LINEAGE_SHA256
            / "fresh_experiment_ledger_v1.jsonl"
        ),
        "experimentLedgerPath": str(
            lineage_root
            / RUN19_V5_STUDY_LINEAGE_SHA256
            / "fresh_experiment_ledger_v1.jsonl"
        ),
        "holdoutAuthorizationRegistryPath": str(
            FROZEN_STATE_ROOT
            / "holdouts"
            / holdout_window_sha
            / "fresh_holdout_authorization_v1.json"
        ),
    }
    if not _canonical_equal(state, expected_state) or any(
        not _canonical_equal(source.get(key), state[key])
        for key in (
            "experimentLedgerPath",
            "holdoutAuthorizationRegistryPath",
        )
    ):
        raise FreshTerminalAuditError("durable v5 state binding changed")

    ledger_raw = _read_member(bundle, members, "fresh_experiment_ledger_v1.jsonl")
    records = _verified_ledger(ledger_raw)
    summary = _json_object(
        _read_member(bundle, members, "fresh_run_summary_v1.json"),
        "run summary",
    )
    expected_summary_keys = {
        "schema",
        "status",
        "preregistrationSha256",
        "implementationManifestSha256",
        "recoveryUsed",
        "recoveryOriginalRunId",
        "recoveryImplementationManifestSha256",
        "infrastructureRestartUsed",
        "infrastructureRestartVersion",
        "predecessorRunId",
        "studyId",
        "studyLineageSha256",
        "splitManifestSha256",
        "corpusManifestSha256",
        "holdoutOpened",
        "stageResults",
        "strongestRecord",
        "artifactFiles",
    }
    if (
        set(summary) != expected_summary_keys
        or summary.get("schema") != RUN_SCHEMA
        or summary.get("preregistrationSha256") != preregistration_sha
        or summary.get("implementationManifestSha256") != implementation_sha
        or summary.get("recoveryUsed") is not False
        or summary.get("recoveryOriginalRunId") is not None
        or summary.get("recoveryImplementationManifestSha256") is not None
        or summary.get("studyId") != FRESH_V5_STUDY_ID
        or summary.get("studyLineageSha256") != RUN19_V5_STUDY_LINEAGE_SHA256
        or summary.get("splitManifestSha256") != split_sha
        or summary.get("corpusManifestSha256") != corpus_sha
        or type(summary.get("predecessorRunId")) is not int
        or summary.get("predecessorRunId") != RUN19_RUN_ID
        or type(summary.get("infrastructureRestartVersion")) is not int
        or summary.get("infrastructureRestartVersion") != 5
        or summary.get("infrastructureRestartUsed") is not True
    ):
        raise FreshTerminalAuditError("run summary identity changed")
    artifact_files = sorted(
        set(members) - {"fresh_run_summary_v1.json", "server-run.log", "remote-exit-status.txt"}
    )
    if not _canonical_equal(summary.get("artifactFiles"), artifact_files):
        raise FreshTerminalAuditError("run summary artifact inventory changed")
    terminal_stage, validated = _verify_stage_and_ledger(
        summary,
        records,
        preregistration_sha,
        split=split,
        preregistration=preregistration,
        frozen=frozen_inputs,
    )
    exit_search_reached = _STAGES.index(terminal_stage) >= _STAGES.index(
        "exit_search"
    )
    if exit_search_reached != ("fresh_exit_bank_v1.json" in members):
        raise FreshTerminalAuditError(
            "exit-bank presence disagrees with completed stages"
        )
    holdout_opened = terminal_stage == "holdout"
    holdout_files = {
        "fresh_final_strategy_frozen_v1.json",
        "fresh_holdout_authorization_v1.json",
    }
    if holdout_opened != holdout_files.issubset(members):
        raise FreshTerminalAuditError("holdout files disagree with holdout access")
    if not holdout_opened and holdout_files & set(members):
        raise FreshTerminalAuditError("holdout state exists without authorization")

    ledger_sha = _sha256_bytes(ledger_raw)
    common = {
        "schema": AUDIT_SCHEMA,
        "archiveIntegrityVerified": True,
        "scientificIntegrityVerified": True,
        "launchCommit": FROZEN_RUN_COMMIT,
        "studyLineageSha256": RUN19_V5_STUDY_LINEAGE_SHA256,
        "preregistrationSha256": preregistration_sha,
        "implementationManifestSha256": implementation_sha,
        "auditRuntimeClosureSha256": (
            FROZEN_LOCAL_RUNTIME_CLOSURE_MANIFEST_SHA256
        ),
        "experimentLedgerSha256": ledger_sha,
        "terminalStage": terminal_stage,
        "holdoutOpened": holdout_opened,
    }
    if not holdout_opened:
        strongest = summary.get("strongestRecord")
        failed = _failed_checks(strongest)
        return {
            **common,
            "status": "no_robust_setup_survived_frozen_validation",
            "userFacingLead": (
                "No robust profitable setup has yet survived unseen validation."
            ),
            "strongestCurrentCandidate": strongest,
            "validationGateReached": terminal_stage,
            "singleMostImportantUnresolvedWeakness": failed[0],
            "nextExperiment": (
                "Use only consumed research windows to diagnose this gate, freeze "
                "one revised causal candidate, and test it chronologically on newly "
                "collected validation data while preserving a later untouched holdout."
            ),
        }

    authorization = _json_object(
        _read_member(bundle, members, "fresh_holdout_authorization_v1.json"),
        "holdout authorization",
    )
    strategy = _json_object(
        _read_member(bundle, members, "fresh_final_strategy_frozen_v1.json"),
        "frozen final strategy",
    )
    holdout = _verify_holdout_evidence(
        split=split,
        preregistration_sha=preregistration_sha,
        records=records,
        authorization=authorization,
        strategy=strategy,
        validated=validated,
        frozen=frozen_inputs,
    )
    if not validated:
        failed = _failed_checks(holdout)
        return {
            **common,
            "status": "no_robust_setup_survived_frozen_validation",
            "userFacingLead": (
                "No robust profitable setup has yet survived unseen validation."
            ),
            "strongestCurrentCandidate": summary.get("strongestRecord"),
            "validationGateReached": "holdout",
            "singleMostImportantUnresolvedWeakness": failed[0],
            "nextExperiment": (
                "The original holdout is consumed. Diagnose with research data only, "
                "freeze one revised causal candidate, and wait for wholly new "
                "chronological validation and holdout windows before another claim."
            ),
        }

    winner_records = _winner_candidate_records(records, strategy)
    for record in winner_records:
        expected_leakage = (
            _ENTRY_LEAKAGE_CHECKS
            if record.get("frozenStrategySha256") is None
            else _STRATEGY_LEAKAGE_CHECKS
        )
        if not _canonical_equal(record.get("leakageChecks"), expected_leakage):
            raise FreshTerminalAuditError(
                "winner causality evidence does not match the frozen schema"
            )
    holdout_metrics = holdout["metrics"]
    return {
        **common,
        "status": "validated_holdout_pass",
        "strategy": strategy,
        "headlineHoldoutMetrics": _headline_metrics(holdout),
        "chronologicalResults": {
            stage: [
                record for record in winner_records if record.get("stage") == stage
            ]
            for stage in _STAGES
        },
        "holdoutBreakdowns": {
            "entry": holdout_metrics.get("entry"),
            "reference": holdout_metrics.get("reference"),
            "requiredStresses": holdout_metrics.get("stresses"),
            "sensitivities": holdout_metrics.get("sensitivities"),
        },
        "parameterSensitivity": {
            "exitSearchNeighbourhood": next(
                (
                    record.get("metrics", {}).get("exitParameterNeighbourhood")
                    for record in winner_records
                    if record.get("stage") == "exit_search"
                ),
                None,
            ),
            "holdoutExecutionSensitivities": holdout_metrics.get("sensitivities"),
        },
        "implementationInputs": {
            "implementationManifest": implementation,
            "features": preregistration.get("features"),
            "entryDiagnostics": preregistration.get("entryDiagnostics"),
            "candidateSearch": preregistration.get("candidateSearch"),
            "execution": preregistration.get("execution"),
            "exitResearch": preregistration.get("exitResearch"),
            "sessionAndData": preregistration.get("sessionAndData"),
            "robustnessAndGates": preregistration.get("robustnessAndGates"),
            "holdout": preregistration.get("holdout"),
            "splitWindows": windows,
        },
        "causalityEvidence": {
            "winnerLeakageChecksMatchFrozenSchemas": True,
            "allLedgerRecordsCanonicallyHashed": True,
            "chronologicalStagePrefixVerified": True,
            "singleHoldoutAuthorizationVerified": True,
            "identicalStrategyAcrossWf3ValidationHoldout": True,
            "implementationAndPreregistrationBoundToLaunchCommit": True,
            "localScientificRuntimeMatchesFrozenClosure": True,
        },
        "remainingLimitations": preregistration.get("execution", {}).get(
            "calibrationLimitation"
        ),
    }


def audit_v5_terminal_adoption(
    bundle_directory: str | Path,
) -> dict[str, Any]:
    """Audit without mutating bundle/state; decompression uses a bounded temp spool."""

    selected = Path(bundle_directory).expanduser()
    if selected.is_symlink():
        raise FreshTerminalAuditError("adoption bundle cannot be a symlink")
    root = selected.resolve()
    if not root.is_dir():
        raise FreshTerminalAuditError("adoption bundle is not a directory")
    children = tuple(root.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in children):
        raise FreshTerminalAuditError("adoption bundle may contain only regular files")
    archive_name = f"fresh-xauusd-{FROZEN_RUN_ID}-{FROZEN_RUN_ATTEMPT}.tgz"
    expected_names = {
        "fresh-xauusd-v5-launch-receipt.json",
        "fresh-xauusd-v5-terminal-receipt.json",
        "fresh-xauusd-v5-adoption-manifest.json",
        archive_name,
    }
    paths = {path.name: path for path in children}
    if set(paths) != expected_names:
        raise FreshTerminalAuditError("adoption bundle member set changed")
    _regular_file(
        paths["fresh-xauusd-v5-launch-receipt.json"],
        "launch receipt",
        maximum_bytes=MAX_RECEIPT_BYTES,
        expected_size=FROZEN_LAUNCH_RECEIPT_SIZE,
    )
    _regular_file(
        paths["fresh-xauusd-v5-terminal-receipt.json"],
        "terminal receipt",
        maximum_bytes=MAX_RECEIPT_BYTES,
    )
    _regular_file(
        paths["fresh-xauusd-v5-adoption-manifest.json"],
        "adoption manifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
    )
    _regular_file(
        paths[archive_name],
        "terminal archive",
        maximum_bytes=MAX_ARCHIVE_BYTES,
    )

    launch_raw = _read_bounded_regular(
        paths["fresh-xauusd-v5-launch-receipt.json"],
        maximum_bytes=MAX_RECEIPT_BYTES,
        expected_size=FROZEN_LAUNCH_RECEIPT_SIZE,
    )
    terminal_raw = _read_bounded_regular(
        paths["fresh-xauusd-v5-terminal-receipt.json"],
        maximum_bytes=MAX_RECEIPT_BYTES,
    )
    launch, terminal, archive_identity = _verify_receipts(
        launch_raw, terminal_raw
    )
    archive_path = paths[archive_name]
    archive_descriptor, archive_before = _open_stable_regular(
        archive_path,
        maximum_bytes=MAX_ARCHIVE_BYTES,
    )
    archive_file: Any | None = None
    bundle: _ArchiveBundle | None = None
    try:
        if archive_before.st_size != archive_identity["size"]:
            raise FreshTerminalAuditError(
                "terminal archive differs from receipt"
            )
        archive_sha = _sha256_open_descriptor(
            archive_descriptor,
            archive_before,
        )
        if archive_sha != archive_identity["sha256"]:
            raise FreshTerminalAuditError(
                "terminal archive differs from receipt"
            )
        manifest_raw = _read_bounded_regular(
            paths["fresh-xauusd-v5-adoption-manifest.json"],
            maximum_bytes=MAX_MANIFEST_BYTES,
        )
        manifest = _json_object(manifest_raw, "adoption manifest")
    except BaseException:
        os.close(archive_descriptor)
        raise
    try:
        _verify_adoption_manifest(
            manifest_raw,
            manifest,
            {
                "fresh-xauusd-v5-launch-receipt.json": (
                    len(launch_raw),
                    _sha256_bytes(launch_raw),
                ),
                "fresh-xauusd-v5-terminal-receipt.json": (
                    len(terminal_raw),
                    _sha256_bytes(terminal_raw),
                ),
                archive_name: (archive_before.st_size, archive_sha),
            },
            launch,
        )
        archive_file = os.fdopen(
            archive_descriptor,
            "rb",
            closefd=False,
        )
        bundle, members = _archive_members(archive_file)
        remote_status = _read_member(
            bundle, members, "remote-exit-status.txt"
        )
        if remote_status != f"{terminal['processExitStatus']}\n".encode("ascii"):
            raise FreshTerminalAuditError(
                "remote exit status differs from terminal receipt"
            )
        if terminal["processExitStatus"] != 0:
            return _infrastructure_failure_report(
                terminal["processExitStatus"]
            )
        return _scientific_audit(
            bundle=bundle,
            members=members,
            launch=launch,
        )
    finally:
        if bundle is not None:
            bundle.close()
        if archive_file is not None:
            archive_file.close()
        try:
            archive_after = os.fstat(archive_descriptor)
            if _stable_identity(archive_before) != _stable_identity(
                archive_after
            ):
                raise FreshTerminalAuditError(
                    "terminal archive changed during audit"
                )
        finally:
            os.close(archive_descriptor)


def main(arguments: Sequence[str] | None = None) -> int:
    """Print one deterministic JSON audit to stdout."""

    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle_directory")
    selected = parser.parse_args(arguments)
    result = audit_v5_terminal_adoption(selected.bundle_directory)
    print(json.dumps(result, allow_nan=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUDIT_SCHEMA",
    "FreshTerminalAuditError",
    "audit_v5_terminal_adoption",
    "main",
]
