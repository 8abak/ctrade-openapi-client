"""Strategy-neutral bootstrap for the fresh chronological XAUUSD study.

The bootstrap is deliberately limited to source integrity.  It streams each
scheduled broker session through the read-only database adapter, fingerprints
the normalized quotes, freezes QC exclusions, and builds the chronological
split.  It never computes a feature, signal, future return, barrier, or trade.

One connection is opened for one session and closed immediately afterwards.
This keeps PostgreSQL named-cursor transactions short and makes an interrupted
inventory safely restartable from a new, empty artifact directory.
"""

from __future__ import annotations

import json
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from datavis.research.fresh_data import FreshDataConfig
from datavis.research.fresh_inventory import (
    FreshScannedSession,
    build_fresh_inventory_manifests,
    scan_and_fingerprint_db_session,
    weekday_anchors,
)
from datavis.research.fresh_protocol import FreshWindowPolicy, build_fresh_split_manifest
from datavis.research.fresh_sessions import SessionAuditConfig


FRESH_SOURCE_FIRST_ANCHOR = date(2025, 12, 31)
FRESH_SOURCE_LAST_FROZEN_ANCHOR = date(2026, 7, 17)
FRESH_EXPECTED_ELIGIBLE_SESSIONS = 139
FRESH_WINDOW_POLICY = FreshWindowPolicy(
    discovery_sessions=46,
    walk_forward_sessions=(12, 12, 12),
    validation_sessions=21,
    holdout_sessions=36,
)


class ClosableConnection(Protocol):
    autocommit: bool

    def close(self) -> Any: ...


ConnectionContextFactory = Callable[[], AbstractContextManager[ClosableConnection]]
ProgressCallback = Callable[[Mapping[str, Any]], None]


@dataclass(frozen=True, slots=True)
class FreshBootstrapConfig:
    first_anchor: date
    last_anchor: date
    data_config: FreshDataConfig
    window_policy: FreshWindowPolicy
    expected_eligible_sessions: int

    def __post_init__(self) -> None:
        if not isinstance(self.first_anchor, date) or not isinstance(
            self.last_anchor, date
        ):
            raise TypeError("bootstrap anchors must be date values")
        if self.last_anchor < self.first_anchor:
            raise ValueError("last_anchor must not precede first_anchor")
        if not isinstance(self.data_config, FreshDataConfig):
            raise TypeError("data_config must be FreshDataConfig")
        if not isinstance(self.window_policy, FreshWindowPolicy):
            raise TypeError("window_policy must be FreshWindowPolicy")
        if (
            not isinstance(self.expected_eligible_sessions, int)
            or isinstance(self.expected_eligible_sessions, bool)
            or self.expected_eligible_sessions <= 0
        ):
            raise ValueError("expected_eligible_sessions must be a positive integer")
        if self.window_policy.required_sessions != self.expected_eligible_sessions:
            raise ValueError(
                "window policy and expected eligible-session count must agree"
            )


def registered_fresh_bootstrap_config() -> FreshBootstrapConfig:
    """Return every outcome-blind source-QC choice explicitly."""

    return FreshBootstrapConfig(
        first_anchor=FRESH_SOURCE_FIRST_ANCHOR,
        last_anchor=FRESH_SOURCE_LAST_FROZEN_ANCHOR,
        data_config=FreshDataConfig(
            session_audit=SessionAuditConfig(
                open_tolerance_seconds=120.0,
                close_tolerance_seconds=120.0,
                friday_close_tolerance_seconds=600.0,
                unexpected_gap_seconds=300.0,
            ),
            expected_symbol="XAUUSD",
            chunk_rows=50_000,
            expected_anchors=(),
            maximum_issue_samples=20,
        ),
        window_policy=FRESH_WINDOW_POLICY,
        expected_eligible_sessions=FRESH_EXPECTED_ELIGIBLE_SESSIONS,
    )


def build_fresh_source_bootstrap(
    connection_context_factory: ConnectionContextFactory,
    *,
    config: FreshBootstrapConfig,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Scan the frozen source range and return inventory/corpus/split manifests.

    The progress callback receives counts and QC status only.  No quote value,
    derived price statistic, signal, or outcome is exposed through it.
    """

    if not callable(connection_context_factory):
        raise TypeError("connection_context_factory must be callable")
    if not isinstance(config, FreshBootstrapConfig):
        raise TypeError("config must be FreshBootstrapConfig")
    anchors = weekday_anchors(config.first_anchor, config.last_anchor)
    scanned: list[FreshScannedSession] = []
    for ordinal, anchor in enumerate(anchors, start=1):
        with connection_context_factory() as connection:
            item = scan_and_fingerprint_db_session(
                connection,
                anchor,
                config=config.data_config,
                cursor_name=f"fresh_inventory_{anchor:%Y%m%d}",
            )
        scanned.append(item)
        if on_progress is not None:
            on_progress(
                {
                    "stage": "source_inventory",
                    "sessionOrdinal": ordinal,
                    "scheduledSessionCount": len(anchors),
                    "sessionAnchor": anchor.isoformat(),
                    "normalizedQuoteCount": item.inventory.normalized_quote_count,
                    "coverageStatus": item.inventory.coverage_status,
                    "isComplete": item.inventory.is_complete,
                }
            )

    inventory, corpus, eligible, exclusions = build_fresh_inventory_manifests(
        scanned,
        config=config.data_config,
    )
    if len(eligible) != config.expected_eligible_sessions:
        raise RuntimeError(
            "source QC produced "
            f"{len(eligible)} eligible sessions; the frozen protocol requires "
            f"{config.expected_eligible_sessions}. Outcome analysis is forbidden."
        )
    split = build_fresh_split_manifest(
        eligible,
        inventory_sha256=str(inventory["inventorySha256"]),
        excluded_sessions=exclusions,
        policy=config.window_policy,
    )
    return {
        "inventory": inventory,
        "corpus": corpus,
        "split": split,
    }


def _write_new_json(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to overwrite immutable artifact: {path}")
    encoded = (
        json.dumps(
            dict(payload),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb", closefd=True) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def write_fresh_source_bootstrap(
    output_directory: str | Path,
    artifacts: Mapping[str, Any],
) -> dict[str, str]:
    """Atomically publish the three strategy-neutral bootstrap artifacts."""

    output = Path(output_directory).expanduser().resolve()
    if output.is_symlink():
        raise ValueError("output_directory may not be a symbolic link")
    output.mkdir(parents=True, exist_ok=True)
    if not output.is_dir():
        raise ValueError("output_directory must be a directory")
    required = ("inventory", "corpus", "split")
    if set(artifacts) != set(required):
        raise ValueError("artifacts must contain exactly inventory, corpus, and split")
    targets = {
        "inventory": output / "fresh_source_inventory_v1.json",
        "corpus": output / "fresh_corpus_manifest_v1.json",
        "split": output / "fresh_split_manifest_v2.json",
    }
    collisions = [path for path in targets.values() if path.exists() or path.is_symlink()]
    if collisions:
        raise FileExistsError(
            f"refusing to overwrite immutable artifact: {collisions[0]}"
        )
    written: list[Path] = []
    try:
        for name in required:
            target = targets[name]
            _write_new_json(target, artifacts[name])
            written.append(target)
    except Exception:
        for target in written:
            target.unlink(missing_ok=True)
        raise
    return {name: str(path) for name, path in targets.items()}


__all__ = [
    "FRESH_EXPECTED_ELIGIBLE_SESSIONS",
    "FRESH_SOURCE_FIRST_ANCHOR",
    "FRESH_SOURCE_LAST_FROZEN_ANCHOR",
    "FRESH_WINDOW_POLICY",
    "FreshBootstrapConfig",
    "build_fresh_source_bootstrap",
    "registered_fresh_bootstrap_config",
    "write_fresh_source_bootstrap",
]
