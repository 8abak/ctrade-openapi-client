from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional


@dataclass(frozen=True, slots=True)
class Tick:
    """One executable quote observed by the collector."""

    id: int
    timestamp: datetime
    bid: float
    ask: float
    id_is_synthetic: bool = False
    source_timestamp: Optional[datetime] = None
    received_timestamp: Optional[datetime] = None
    bid_source_timestamp: Optional[datetime] = None
    ask_source_timestamp: Optional[datetime] = None
    collector_session_id: Optional[str] = None
    collector_sequence: Optional[int] = None
    collector_version: Optional[str] = None

    def __post_init__(self) -> None:
        if self.id < 0:
            raise ValueError("tick id must be non-negative")
        if self.timestamp.tzinfo is None:
            raise ValueError("tick timestamp must include a timezone")
        for name in (
            "source_timestamp",
            "received_timestamp",
            "bid_source_timestamp",
            "ask_source_timestamp",
        ):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                raise ValueError(f"tick {name} must include a timezone")
        if self.collector_sequence is not None and self.collector_sequence < 0:
            raise ValueError("collector_sequence must be non-negative")
        if not (math.isfinite(self.bid) and math.isfinite(self.ask)):
            raise ValueError("tick bid and ask must be finite")
        if self.bid <= 0 or self.ask <= 0:
            raise ValueError("tick bid and ask must be positive")
        if self.ask < self.bid:
            raise ValueError("crossed quote: ask is below bid")

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def timestamp_ms(self) -> int:
        return int(self.timestamp.timestamp() * 1000)

    @property
    def utc_date(self):
        return self.timestamp.astimezone(timezone.utc).date()


def _parse_timestamp(row: dict[str, str]) -> datetime:
    raw_iso = (row.get("timestamp") or row.get("time") or "").strip()
    if raw_iso:
        parsed = datetime.fromisoformat(raw_iso.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError(f"timestamp has no timezone: {raw_iso!r}")
        return parsed

    raw_ms = (row.get("timestampMs") or row.get("timestamp_ms") or "").strip()
    if raw_ms:
        return datetime.fromtimestamp(int(raw_ms) / 1000.0, tz=timezone.utc)
    raise ValueError("tick row has no timestamp or timestampMs")


def _optional_timestamp(row: dict[str, str], *names: str) -> Optional[datetime]:
    for name in names:
        raw = (row.get(name) or "").strip()
        if not raw:
            continue
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError(f"{name} has no timezone: {raw!r}")
        return parsed
    return None


def iter_csv_ticks(
    path: str | Path,
    *,
    symbol: Optional[str] = "XAUUSD",
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> Iterator[Tick]:
    """Stream validated quotes from a Datavis CSV export."""

    source = Path(path)
    previous_timestamp: Optional[datetime] = None
    emitted = 0
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = {"bid", "ask"}.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV is missing required columns: {sorted(missing)}")
        for row_number, row in enumerate(reader, start=2):
            row_symbol = (row.get("symbol") or symbol or "").strip().upper()
            if symbol and row_symbol and row_symbol != symbol.upper():
                continue
            try:
                source_timestamp = _parse_timestamp(row)
                received_timestamp = _optional_timestamp(
                    row, "received_timestamp", "receive_timestamp"
                )
                timestamp = received_timestamp or source_timestamp
                raw_id = (row.get("id") or "").strip()
                raw_sequence = (row.get("collector_sequence") or "").strip()
                tick = Tick(
                    id=int(raw_id) if raw_id else row_number - 1,
                    timestamp=timestamp,
                    bid=float((row.get("bid") or "").strip()),
                    ask=float((row.get("ask") or "").strip()),
                    id_is_synthetic=not bool(raw_id),
                    source_timestamp=source_timestamp,
                    received_timestamp=received_timestamp,
                    bid_source_timestamp=_optional_timestamp(
                        row, "bid_source_timestamp", "bid_timestamp"
                    ),
                    ask_source_timestamp=_optional_timestamp(
                        row, "ask_source_timestamp", "ask_timestamp"
                    ),
                    collector_session_id=(
                        (row.get("collector_session_id") or "").strip() or None
                    ),
                    collector_sequence=int(raw_sequence) if raw_sequence else None,
                    collector_version=(
                        (row.get("collector_version") or "").strip() or None
                    ),
                )
            except Exception as exc:
                raise ValueError(f"invalid tick at CSV row {row_number}: {exc}") from exc
            if previous_timestamp is not None and tick.timestamp <= previous_timestamp:
                raise ValueError(f"non-increasing timestamp at CSV row {row_number}")
            previous_timestamp = tick.timestamp
            if start is not None and tick.timestamp < start:
                continue
            if end is not None and tick.timestamp > end:
                break
            yield tick
            emitted += 1
            if limit is not None and emitted >= limit:
                break
