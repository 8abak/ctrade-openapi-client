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
from datavis.research.fresh_protocol import (
    REGISTERED_FRESH_WINDOW_POLICY,
    FreshWindowPolicy,
    build_fresh_split_manifest,
)
from datavis.research.fresh_sessions import SessionAuditConfig


FRESH_SOURCE_FIRST_ANCHOR = date(2025, 12, 31)
FRESH_SOURCE_LAST_FROZEN_ANCHOR = date(2026, 7, 17)
# Frozen from the outcome-blind source audit in GitHub Actions run 29869472203
# at commit 3f734dad493ffadef692e3f382c7b8236e546118.  No feature, signal,
# barrier, trade, or return was computed before this list was registered.
FRESH_REGISTERED_ELIGIBLE_ANCHORS = (
    "2026-01-02",
    "2026-01-05",
    "2026-01-06",
    "2026-01-07",
    "2026-01-08",
    "2026-01-09",
    "2026-01-12",
    "2026-01-14",
    "2026-01-15",
    "2026-01-16",
    "2026-01-21",
    "2026-01-22",
    "2026-01-23",
    "2026-01-26",
    "2026-01-27",
    "2026-01-28",
    "2026-01-29",
    "2026-01-30",
    "2026-02-02",
    "2026-02-03",
    "2026-02-04",
    "2026-02-05",
    "2026-02-06",
    "2026-02-09",
    "2026-02-10",
    "2026-02-11",
    "2026-02-12",
    "2026-02-13",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-23",
    "2026-02-25",
    "2026-02-27",
    "2026-03-03",
    "2026-03-04",
    "2026-03-05",
    "2026-03-06",
    "2026-03-11",
    "2026-03-12",
    "2026-03-13",
    "2026-03-16",
    "2026-03-17",
    "2026-03-18",
    "2026-03-19",
    "2026-03-20",
    "2026-03-25",
    "2026-03-27",
    "2026-03-30",
    "2026-03-31",
    "2026-04-01",
    "2026-04-07",
    "2026-04-08",
    "2026-04-09",
    "2026-04-13",
    "2026-04-16",
    "2026-04-17",
    "2026-04-20",
    "2026-04-21",
    "2026-04-22",
    "2026-04-23",
    "2026-04-24",
    "2026-04-27",
    "2026-04-28",
    "2026-04-29",
    "2026-04-30",
    "2026-05-01",
    "2026-05-05",
    "2026-05-06",
    "2026-05-07",
    "2026-05-08",
    "2026-05-11",
    "2026-05-12",
    "2026-05-13",
    "2026-05-14",
    "2026-05-15",
    "2026-05-18",
    "2026-05-19",
    "2026-05-20",
    "2026-05-21",
    "2026-05-26",
    "2026-05-27",
    "2026-05-29",
    "2026-06-01",
    "2026-06-02",
    "2026-06-03",
    "2026-06-04",
    "2026-06-05",
    "2026-06-08",
    "2026-06-09",
    "2026-06-10",
    "2026-06-11",
    "2026-06-16",
    "2026-06-17",
    "2026-06-18",
    "2026-06-22",
    "2026-06-23",
    "2026-06-24",
    "2026-06-25",
    "2026-06-26",
    "2026-06-29",
    "2026-06-30",
    "2026-07-01",
    "2026-07-02",
    "2026-07-06",
    "2026-07-07",
    "2026-07-08",
    "2026-07-09",
    "2026-07-10",
    "2026-07-13",
    "2026-07-14",
    "2026-07-15",
    "2026-07-16",
    "2026-07-17",
)
FRESH_WINDOW_POLICY = REGISTERED_FRESH_WINDOW_POLICY
FRESH_EXPECTED_ELIGIBLE_SESSIONS = FRESH_WINDOW_POLICY.required_sessions


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
    expected_eligible_anchors: tuple[str, ...] = ()

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
        if not isinstance(self.expected_eligible_anchors, tuple):
            raise TypeError("expected_eligible_anchors must be a tuple")
        if self.expected_eligible_anchors:
            if len(self.expected_eligible_anchors) != self.expected_eligible_sessions:
                raise ValueError(
                    "registered eligible anchors and expected count must agree"
                )
            previous: date | None = None
            for raw_anchor in self.expected_eligible_anchors:
                if not isinstance(raw_anchor, str):
                    raise ValueError("registered eligible anchors must be ISO dates")
                try:
                    parsed = date.fromisoformat(raw_anchor)
                except ValueError as exc:
                    raise ValueError(
                        "registered eligible anchors must be ISO dates"
                    ) from exc
                if parsed.weekday() >= 5 or (previous is not None and parsed <= previous):
                    raise ValueError(
                        "registered eligible anchors must be chronological weekdays"
                    )
                previous = parsed


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
        expected_eligible_anchors=FRESH_REGISTERED_ELIGIBLE_ANCHORS,
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
    if config.expected_eligible_anchors and tuple(eligible) != (
        config.expected_eligible_anchors
    ):
        raise RuntimeError(
            "source QC eligible anchors differ from the registered outcome-blind "
            "audit. Outcome analysis is forbidden."
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
    "FRESH_REGISTERED_ELIGIBLE_ANCHORS",
    "FRESH_SOURCE_FIRST_ANCHOR",
    "FRESH_SOURCE_LAST_FROZEN_ANCHOR",
    "FRESH_WINDOW_POLICY",
    "FreshBootstrapConfig",
    "build_fresh_source_bootstrap",
    "registered_fresh_bootstrap_config",
    "write_fresh_source_bootstrap",
]
