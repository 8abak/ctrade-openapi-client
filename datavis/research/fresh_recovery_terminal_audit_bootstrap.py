"""Verified loader for the pre-outcome V5 recovery scientific gate.

Run this file directly with ``python -I -B``.  It reads the separately sealed
auditor once, verifies its exact bytes, executes only that cached source, and
refuses a process in which any ``datavis`` module was imported beforehand.
The auditor subsequently loads the recovery protocol from Git blobs belonging
to the exact launch commit rather than from this audit checkout.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys
import types
from typing import Any, Sequence


BOOTSTRAP_SCHEMA = "fresh-xauusd-v5-recovery-audit-bootstrap/v1"
AUDITOR_MODULE = "fresh_xauusd_v5_recovery_terminal_auditor_sealed"
AUDITOR_RELATIVE_PATH = "fresh_recovery_terminal_audit.py"
FROZEN_AUDITOR_SHA256 = (
    "97c2ccbe1308bdac47b02487c3e12df7dedf64da04a976d46e660a247fcc387f"
)
MAX_AUDITOR_BYTES = 4 * 1024**2
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class FreshRecoveryAuditBootstrapError(RuntimeError):
    """Raised when the recovery auditor cannot be loaded from sealed bytes."""


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


def _stable_read(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise FreshRecoveryAuditBootstrapError(
            "sealed recovery auditor is unavailable"
        ) from error
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_nlink != 1
        or before.st_size <= 0
        or before.st_size > MAX_AUDITOR_BYTES
    ):
        os.close(descriptor)
        raise FreshRecoveryAuditBootstrapError(
            "sealed recovery auditor is not a bounded regular file"
        )
    chunks: list[bytes] = []
    total = 0
    try:
        while True:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, MAX_AUDITOR_BYTES + 1 - total),
            )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_AUDITOR_BYTES:
                raise FreshRecoveryAuditBootstrapError(
                    "sealed recovery auditor exceeds its byte bound"
                )
        after = os.fstat(descriptor)
        if (
            _stable_identity(before) != _stable_identity(after)
            or total != before.st_size
        ):
            raise FreshRecoveryAuditBootstrapError(
                "sealed recovery auditor changed while being read"
            )
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _runtime_failures() -> tuple[str, ...]:
    failures: list[str] = []
    if platform.python_implementation() != "CPython":
        failures.append("Python implementation is not CPython")
    if sys.version_info[:2] != (3, 11):
        failures.append("Python series is not 3.11")
    required_flags = {
        "isolated": 1,
        "ignore_environment": 1,
        "dont_write_bytecode": 1,
        "safe_path": True,
    }
    for name, expected in required_flags.items():
        if getattr(sys.flags, name, None) != expected:
            failures.append(f"runtime flag changed: {name}")
    return tuple(failures)


def _reject_preloaded_datavis() -> None:
    names = sorted(
        name
        for name in sys.modules
        if name == "datavis" or name.startswith("datavis.")
    )
    if names:
        raise FreshRecoveryAuditBootstrapError(
            "datavis modules were imported before auditor verification"
        )


def _sealed_auditor_sha256(value: str) -> str:
    if (
        not isinstance(value, str)
        or SHA256_PATTERN.fullmatch(value) is None
        or value != FROZEN_AUDITOR_SHA256
    ):
        raise FreshRecoveryAuditBootstrapError(
            "requested auditor digest differs from the frozen seal"
        )
    return value


def _load_verified_auditor(
    *,
    auditor_path: Path,
    expected_sha256: str,
) -> Any:
    _reject_preloaded_datavis()
    expected = _sealed_auditor_sha256(expected_sha256)
    raw = _stable_read(auditor_path)
    actual = hashlib.sha256(raw).hexdigest()
    if actual != expected:
        raise FreshRecoveryAuditBootstrapError(
            "sealed recovery auditor bytes changed"
        )
    try:
        code = compile(raw, str(auditor_path), "exec", dont_inherit=True)
    except (SyntaxError, ValueError) as error:
        raise FreshRecoveryAuditBootstrapError(
            "sealed recovery auditor does not compile"
        ) from error
    module = types.ModuleType(AUDITOR_MODULE)
    module.__file__ = str(auditor_path)
    module.__package__ = ""
    prior = sys.modules.get(AUDITOR_MODULE)
    sys.modules[AUDITOR_MODULE] = module
    try:
        exec(code, module.__dict__)
    except BaseException:
        if prior is None:
            sys.modules.pop(AUDITOR_MODULE, None)
        else:
            sys.modules[AUDITOR_MODULE] = prior
        raise
    return module


def run_verified_auditor(
    *,
    terminal_output_directory: str | Path,
    launch_source_root: str | Path,
    expected_launch_commit_sha: str,
    recorded_remote_repository_root: str,
    auditor_sha256: str,
) -> dict[str, Any]:
    failures = _runtime_failures()
    if failures:
        raise FreshRecoveryAuditBootstrapError("; ".join(failures))
    source = Path(__file__).resolve().with_name(AUDITOR_RELATIVE_PATH)
    auditor = _load_verified_auditor(
        auditor_path=source,
        expected_sha256=auditor_sha256,
    )
    entrypoint = getattr(auditor, "audit_recovery_scientific_gate", None)
    if not callable(entrypoint):
        raise FreshRecoveryAuditBootstrapError(
            "sealed recovery auditor has no gate entrypoint"
        )
    result = entrypoint(
        terminal_output_directory,
        launch_source_root=launch_source_root,
        expected_launch_commit_sha=expected_launch_commit_sha,
        recorded_remote_repository_root=recorded_remote_repository_root,
    )
    if (
        not isinstance(result, dict)
        or result.get("schema")
        != "fresh-xauusd-v5-recovery-scientific-gate-audit/v1"
        or result.get("scientificResultInterpreted") is not False
        or result.get("finalStrategyConclusionAuthorized") is not False
    ):
        raise FreshRecoveryAuditBootstrapError(
            "sealed recovery auditor returned an invalid gate receipt"
        )
    return {
        "schema": BOOTSTRAP_SCHEMA,
        "auditorSha256": auditor_sha256,
        "audit": result,
    }


def main(arguments: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("terminal_output_directory")
    parser.add_argument("launch_source_root")
    parser.add_argument("expected_launch_commit_sha")
    parser.add_argument("recorded_remote_repository_root")
    parser.add_argument(
        "--auditor-sha256",
        required=True,
    )
    selected = parser.parse_args(arguments)
    result = run_verified_auditor(
        terminal_output_directory=selected.terminal_output_directory,
        launch_source_root=selected.launch_source_root,
        expected_launch_commit_sha=selected.expected_launch_commit_sha,
        recorded_remote_repository_root=(
            selected.recorded_remote_repository_root
        ),
        auditor_sha256=selected.auditor_sha256,
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
    "BOOTSTRAP_SCHEMA",
    "FROZEN_AUDITOR_SHA256",
    "FreshRecoveryAuditBootstrapError",
    "main",
    "run_verified_auditor",
]
