"""Immutable protocol and leakage guards for the fresh acceleration study.

This module contains no trading rule or selected threshold.  It assigns raw
broker partitions to chronological research roles, fingerprints every frozen
input, and protects the final holdout from accidental repeated evaluation.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = 2
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ROLES = (
    "discovery",
    "walk_forward_1",
    "walk_forward_2",
    "walk_forward_3",
    "validation",
    "holdout",
)


@dataclass(frozen=True, slots=True)
class FreshWindowPolicy:
    discovery_sessions: int
    walk_forward_sessions: tuple[int, int, int]
    validation_sessions: int
    holdout_sessions: int

    def __post_init__(self) -> None:
        values = (
            self.discovery_sessions,
            *self.walk_forward_sessions,
            self.validation_sessions,
            self.holdout_sessions,
        )
        if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in values):
            raise ValueError("every window size must be a positive integer")
        if len(self.walk_forward_sessions) != 3:
            raise ValueError("exactly three walk-forward window sizes are required")

    @property
    def required_sessions(self) -> int:
        return (
            self.discovery_sessions
            + sum(self.walk_forward_sessions)
            + self.validation_sessions
            + self.holdout_sessions
        )


def canonical_hash(payload: Any) -> str:
    """Return a stable SHA-256 for a JSON-compatible payload."""

    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("payload must be finite and JSON-compatible") from exc
    return hashlib.sha256(encoded).hexdigest()


def _sha256_text(value: str, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value.lower()) is None:
        raise ValueError(f"{name} must be a SHA-256 hex digest")
    return value.lower()


def build_fresh_split_manifest(
    eligible_session_anchors: Sequence[str],
    *,
    inventory_sha256: str,
    excluded_sessions: Sequence[Mapping[str, Any]],
    policy: FreshWindowPolicy,
) -> dict[str, Any]:
    """Freeze exact DST-aware session assignments without price outcomes.

    The caller supplies only anchors that passed the separately frozen data-QC
    policy.  This module never infers sessions from a fixed civil-time
    partition, and it never attaches weekend fragments retrospectively.
    """

    if not isinstance(policy, FreshWindowPolicy):
        raise TypeError("policy must be a FreshWindowPolicy")
    settings = policy
    inventory_hash = _sha256_text(inventory_sha256, "inventory_sha256")
    anchors: list[str] = []
    previous: date | None = None
    for raw_anchor in eligible_session_anchors:
        if not isinstance(raw_anchor, str):
            raise ValueError("eligible session anchors must be ISO date strings")
        try:
            parsed = date.fromisoformat(raw_anchor)
        except ValueError as exc:
            raise ValueError("eligible session anchors must be ISO date strings") from exc
        if parsed.weekday() >= 5:
            raise ValueError("eligible session anchors must be weekdays")
        if previous is not None and parsed <= previous:
            raise ValueError("eligible session anchors must be strictly chronological")
        anchors.append(parsed.isoformat())
        previous = parsed
    if len(anchors) != settings.required_sessions:
        raise ValueError(
            f"window policy requires {settings.required_sessions} session anchors; "
            f"received {len(anchors)}"
        )
    if not isinstance(excluded_sessions, Sequence) or isinstance(excluded_sessions, (str, bytes)):
        raise ValueError("excluded_sessions must be a sequence of QC records")
    exclusions = [dict(record) for record in excluded_sessions]
    if not exclusions or any(not record.get("reason") for record in exclusions):
        raise ValueError("excluded_sessions must contain explicit QC reasons")

    sizes = (
        settings.discovery_sessions,
        *settings.walk_forward_sessions,
        settings.validation_sessions,
        settings.holdout_sessions,
    )
    cursor = 0
    windows: dict[str, dict[str, Any]] = {}
    assignments: list[dict[str, Any]] = []
    for role, size in zip(_ROLES, sizes):
        selected = anchors[cursor : cursor + size]
        cursor += size
        window = {
            "role": role,
            "sessionCount": len(selected),
            "firstSessionAnchor": selected[0],
            "lastSessionAnchor": selected[-1],
            "sessionAnchors": list(selected),
        }
        windows[role] = window
        assignments.extend(
            {
                "sessionAnchor": anchor,
                "role": role,
                "classification": "new_york_maintenance_schedule",
            }
            for anchor in selected
        )

    body = {
        "schemaVersion": SCHEMA_VERSION,
        "policy": asdict(settings),
        "inventorySha256": inventory_hash,
        "sessionCount": len(anchors),
        "excludedSessionsBeforeOutcomeInspection": exclusions,
        "windows": windows,
        "assignments": assignments,
        "sessionSchedule": {
            "timezone": "America/New_York",
            "start": "previous-calendar-day 18:00",
            "end": "anchor-calendar-day 17:00",
            "interval": "half-open",
            "sydneyConversion": "IANA Australia/Sydney",
        },
        "holdoutPolicy": {
            "maximumFrozenCandidates": 1,
            "explicitAuthorizationRequired": True,
            "repeatEvaluationForbidden": True,
        },
    }
    return {**body, "manifestSha256": canonical_hash(body)}


def frozen_research_fingerprint(
    *,
    split_manifest: Mapping[str, Any],
    data_manifest: Mapping[str, Any],
    feature_specification: Mapping[str, Any],
    execution_config: Mapping[str, Any],
    candidate_grid: Mapping[str, Any],
    code_identifier: str,
) -> dict[str, str]:
    """Bind every input that can change signals, fills, or selection."""

    if not isinstance(code_identifier, str) or not code_identifier.strip():
        raise ValueError("code_identifier must be non-empty")
    components = {
        "splitSha256": canonical_hash(split_manifest),
        "dataSha256": canonical_hash(data_manifest),
        "featureSha256": canonical_hash(feature_specification),
        "executionSha256": canonical_hash(execution_config),
        "candidateGridSha256": canonical_hash(candidate_grid),
        "codeSha256": hashlib.sha256(code_identifier.encode("utf-8")).hexdigest(),
    }
    return {**components, "researchSha256": canonical_hash(components)}


def authorize_evaluation(
    role: str,
    *,
    split_manifest: Mapping[str, Any],
    access_records: Sequence[Mapping[str, Any]] = (),
    frozen_strategy_sha256: str | None = None,
    explicit_holdout_authorization: bool = False,
) -> dict[str, Any]:
    """Authorize one role and make repeated holdout access impossible by contract."""

    if role not in _ROLES:
        raise ValueError(f"unknown evaluation role: {role!r}")
    windows = split_manifest.get("windows")
    if not isinstance(windows, Mapping) or role not in windows:
        raise ValueError("split manifest does not contain the requested role")
    consumed_roles = {
        str(record.get("role"))
        for record in access_records
        if record.get("outcomesRevealed") is True
    }
    if role in consumed_roles:
        raise PermissionError(f"{role} outcomes have already been consumed")

    strategy_hash: str | None = None
    if role == "holdout":
        if explicit_holdout_authorization is not True:
            raise PermissionError("holdout evaluation requires explicit authorization")
        strategy_hash = _sha256_text(
            frozen_strategy_sha256 or "", "frozen_strategy_sha256"
        )
        prior_holdout = [record for record in access_records if record.get("role") == "holdout"]
        if prior_holdout:
            raise PermissionError("holdout access has already been attempted")

    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "role": role,
        "window": dict(windows[role]),
        "splitManifestSha256": str(split_manifest.get("manifestSha256") or canonical_hash(split_manifest)),
        "frozenStrategySha256": strategy_hash,
        "outcomesRevealed": False,
    }
    return {**payload, "authorizationSha256": canonical_hash(payload)}


_TRIAL_REQUIRED = {
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
}


def append_fresh_record(path: str | Path, record: Mapping[str, Any]) -> dict[str, Any]:
    """Append one validated, numbered, JSON-safe experiment or access record."""

    if not isinstance(record, Mapping):
        raise ValueError("record must be a mapping")
    missing = _TRIAL_REQUIRED.difference(record)
    if missing:
        raise ValueError(f"record is missing required fields: {sorted(missing)}")
    if "recordNumber" in record or "recordSha256" in record:
        raise ValueError("recordNumber and recordSha256 are reserved")
    raw = dict(record)
    digest = canonical_hash(raw)
    destination = Path(path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError("experiment ledger may not be a symbolic link")
    lock = destination.with_name(destination.name + ".lock")
    try:
        lock_fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise RuntimeError("experiment ledger is already being appended") from exc
    try:
        os.close(lock_fd)
        number = 1
        if destination.exists():
            with destination.open("r", encoding="utf-8") as handle:
                number += sum(1 for line in handle if line.strip())
        enriched = {"recordNumber": number, "recordSha256": digest, **raw}
        encoded = (
            json.dumps(
                enriched,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
        descriptor = os.open(destination, flags, 0o600)
        with os.fdopen(descriptor, "ab", closefd=True) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        return enriched
    finally:
        lock.unlink(missing_ok=True)


def finite_metrics(payload: Mapping[str, Any]) -> bool:
    """Return false when a nested metric contains NaN or infinity."""

    def valid(value: Any) -> bool:
        if isinstance(value, float):
            return math.isfinite(value)
        if isinstance(value, Mapping):
            return all(valid(item) for item in value.values())
        if isinstance(value, (list, tuple)):
            return all(valid(item) for item in value)
        return True

    return valid(payload)


__all__ = [
    "FreshWindowPolicy",
    "append_fresh_record",
    "authorize_evaluation",
    "build_fresh_split_manifest",
    "canonical_hash",
    "finite_metrics",
    "frozen_research_fingerprint",
]
