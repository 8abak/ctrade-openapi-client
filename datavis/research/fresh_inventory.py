"""Outcome-blind source inventory and normalized-corpus fingerprints.

Inventory records contain data-integrity facts only: scheduled bounds, row and
repeated-quote counts, quote validity, boundary coverage, and unexpected feed
gaps.  Repeated quote values with distinct ids remain separate tick-volume
events.  The records intentionally contain no return, movement, barrier,
signal, or P&L measure.  A normalized tick digest is updated in streaming order
so later research can prove it used the same executable quote corpus without
exposing the quotes in the manifest.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Mapping

from datavis.research.fresh_data import FreshDataConfig
from datavis.research.fresh_db_source import (
    BROKER_SESSION_QUERY,
    FreshDbSessionInventory,
    NamedCursorConnection,
    scan_fresh_db_session,
)
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_sessions import AssignedBrokerTick, broker_session_bounds


INVENTORY_SCHEMA = "fresh-xauusd-source-inventory/v3"
CORPUS_SCHEMA = "fresh-xauusd-normalized-corpus/v2"
_UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class FreshScannedSession:
    inventory: FreshDbSessionInventory
    normalized_sha256: str


def _canonical_tick_bytes(assigned: AssignedBrokerTick) -> bytes:
    tick = assigned.tick
    timestamp = tick.timestamp.astimezone(_UTC).isoformat(timespec="microseconds")
    fields = (
        str(tick.id),
        tick.symbol,
        timestamp,
        float(tick.bid).hex(),
        float(tick.ask).hex(),
    )
    return ("\x1f".join(fields) + "\n").encode("ascii")


def scan_and_fingerprint_db_session(
    connection: NamedCursorConnection,
    anchor: date | str,
    *,
    config: FreshDataConfig,
    cursor_name: str,
    on_tick: Callable[[AssignedBrokerTick], None] | None = None,
) -> FreshScannedSession:
    """Scan once, hash every valid unique-id quote, and optionally forward it."""

    digest = hashlib.sha256()

    def observe(assigned: AssignedBrokerTick) -> None:
        digest.update(_canonical_tick_bytes(assigned))
        if on_tick is not None:
            on_tick(assigned)

    inventory = scan_fresh_db_session(
        connection,
        anchor,
        config=config,
        on_tick=observe,
        cursor_name=cursor_name,
    )
    return FreshScannedSession(inventory=inventory, normalized_sha256=digest.hexdigest())


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(_UTC).isoformat() if value is not None else None


def session_inventory_record(scanned: FreshScannedSession) -> dict[str, Any]:
    item = scanned.inventory
    audit = item.audit
    return {
        "sessionAnchor": item.anchor.isoformat(),
        "symbol": item.symbol,
        "sessionStartUtc": item.bounds.start_utc.isoformat(),
        "sessionEndUtc": item.bounds.end_utc.isoformat(),
        "sessionStartSydney": item.bounds.start_sydney.isoformat(),
        "sessionEndSydney": item.bounds.end_sydney.isoformat(),
        "rawRowCount": item.raw_row_count,
        "validQuoteCount": item.valid_quote_count,
        "normalizedQuoteCount": item.normalized_quote_count,
        "duplicateQuoteCount": item.duplicate_quote_count,
        "duplicateGroupCount": item.duplicate_group_count,
        "invalidQuoteCount": item.invalid_quote_count,
        "invalidQuoteSamples": [
            {
                "source": sample.source,
                "rowNumber": sample.row_number,
                "tickId": sample.tick_id,
                "symbol": sample.symbol,
                "timestampUtc": sample.timestamp_utc.isoformat(),
                "reason": sample.reason,
            }
            for sample in item.invalid_quote_samples
        ],
        "lockedQuoteCount": item.locked_quote_count,
        "firstTimestampUtc": _iso(item.first_timestamp_utc),
        "lastTimestampUtc": _iso(item.last_timestamp_utc),
        "openDelaySeconds": item.open_delay_seconds,
        "closeLeadSeconds": item.close_lead_seconds,
        "openBoundaryCovered": item.open_boundary_covered,
        "closeBoundaryCovered": item.close_boundary_covered,
        "boundaryComplete": item.boundary_complete,
        "unexpectedGapCount": item.unexpected_gap_count,
        "longestUnexpectedGapSeconds": item.longest_unexpected_gap_seconds,
        "totalUnexpectedGapSeconds": item.total_unexpected_gap_seconds,
        "unexpectedGapSamples": [
            {
                "leftTickId": gap.left_tick_id,
                "rightTickId": gap.right_tick_id,
                "leftTimestampUtc": gap.left_timestamp_utc.isoformat(),
                "rightTimestampUtc": gap.right_timestamp_utc.isoformat(),
                "durationSeconds": gap.duration_seconds,
            }
            for gap in audit.unexpected_gaps
        ],
        "coverageStatus": item.coverage_status,
        "isComplete": item.is_complete,
        "normalizedSha256": scanned.normalized_sha256,
    }


def _exclusion_reason(record: Mapping[str, Any]) -> str:
    status = record.get("coverageStatus")
    if status == "empty":
        return "no valid XAUUSD quotes in the scheduled broker session"
    reasons: list[str] = []
    if not record.get("openBoundaryCovered"):
        reasons.append("opening boundary not covered within the frozen tolerance")
    if not record.get("closeBoundaryCovered"):
        reasons.append("closing boundary not covered within the frozen tolerance")
    if int(record.get("unexpectedGapCount") or 0) > 0:
        reasons.append("unexpected in-session quote gap exceeded the frozen tolerance")
    if int(record.get("invalidQuoteCount") or 0) > 0:
        reasons.append("invalid executable quote was observed")
    return "; ".join(reasons) or f"session failed data QC with status {status!r}"


def build_fresh_inventory_manifests(
    scanned_sessions: Iterable[FreshScannedSession],
    *,
    config: FreshDataConfig,
) -> tuple[dict[str, Any], dict[str, Any], list[str], list[dict[str, str]]]:
    """Build deterministic inventory/corpus manifests and QC classifications."""

    if not isinstance(config, FreshDataConfig):
        raise TypeError("config must be a FreshDataConfig")
    sessions = tuple(scanned_sessions)
    anchors = [item.inventory.anchor for item in sessions]
    if anchors != sorted(anchors) or len(anchors) != len(set(anchors)):
        raise ValueError("scanned sessions must have unique chronological anchors")
    records = [session_inventory_record(item) for item in sessions]
    eligible = [
        str(record["sessionAnchor"])
        for record in records
        if record["isComplete"] is True
    ]
    exclusions = [
        {
            "sessionAnchor": str(record["sessionAnchor"]),
            "reason": _exclusion_reason(record),
        }
        for record in records
        if record["isComplete"] is not True
    ]
    query_sha = hashlib.sha256(BROKER_SESSION_QUERY.encode("utf-8")).hexdigest()
    inventory_body = {
        "schema": INVENTORY_SCHEMA,
        "symbol": config.expected_symbol,
        "sessionSchedule": {
            "timezone": "America/New_York",
            "start": "previous-calendar-day 18:00",
            "end": "anchor-calendar-day 17:00",
            "interval": "half-open",
        },
        "sortKey": ["timestamp UTC", "id"],
        "repeatedQuoteDiagnosticKey": ["symbol", "timestamp UTC", "bid", "ask"],
        "repeatedQuotePolicy": (
            "retain every record with a unique id as a separate tick-volume unit; "
            "counts are informational and never an exclusion reason"
        ),
        "querySha256": query_sha,
        "dataConfig": {
            "sessionAudit": asdict(config.session_audit),
            "expectedSymbol": config.expected_symbol,
            "chunkRows": config.chunk_rows,
            "expectedAnchors": [
                (value.isoformat() if isinstance(value, date) else str(value))
                for value in config.expected_anchors
            ],
            "maximumIssueSamples": config.maximum_issue_samples,
        },
        "scannedSessionCount": len(records),
        "eligibleSessionCount": len(eligible),
        "excludedSessionCount": len(exclusions),
        "sessions": records,
    }
    inventory = {
        **inventory_body,
        "inventorySha256": canonical_hash(inventory_body),
    }
    corpus_body = {
        "schema": CORPUS_SCHEMA,
        "symbol": config.expected_symbol,
        "inventorySha256": inventory["inventorySha256"],
        "normalization": {
            "timestamp": "UTC ISO-8601 microseconds",
            "price": "IEEE-754 float.hex",
            "separator": "ASCII unit-separator with newline records",
            "order": ["timestamp UTC", "id"],
            "repeatedQuoteHandling": (
                "retain every record with a unique id as a separate event and "
                "tick-volume unit"
            ),
        },
        "sessions": [
            {
                "sessionAnchor": record["sessionAnchor"],
                "normalizedQuoteCount": record["normalizedQuoteCount"],
                "normalizedSha256": record["normalizedSha256"],
                "eligible": record["isComplete"],
            }
            for record in records
        ],
    }
    corpus = {**corpus_body, "corpusManifestSha256": canonical_hash(corpus_body)}
    return inventory, corpus, eligible, exclusions


def weekday_anchors(start: date, end: date) -> tuple[date, ...]:
    """Return every scheduled Monday-Friday anchor in an inclusive range."""

    if not isinstance(start, date) or isinstance(start, datetime):
        raise TypeError("start must be a date")
    if not isinstance(end, date) or isinstance(end, datetime):
        raise TypeError("end must be a date")
    if end < start:
        raise ValueError("end must not precede start")
    anchors: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            broker_session_bounds(cursor)
            anchors.append(cursor)
        cursor = date.fromordinal(cursor.toordinal() + 1)
    return tuple(anchors)


__all__ = [
    "CORPUS_SCHEMA",
    "FreshScannedSession",
    "INVENTORY_SCHEMA",
    "build_fresh_inventory_manifests",
    "scan_and_fingerprint_db_session",
    "session_inventory_record",
    "weekday_anchors",
]
