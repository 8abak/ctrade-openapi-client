from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .models import Tick


NEW_YORK = ZoneInfo("America/New_York")
ACD_START = time(9, 30)
ACD_OPENING_MINUTES = 15


def _next_weekday(value: date) -> date:
    result = value + timedelta(days=1)
    while result.weekday() >= 5:
        result += timedelta(days=1)
    return result


def _previous_weekday(value: date) -> date:
    result = value - timedelta(days=1)
    while result.weekday() >= 5:
        result -= timedelta(days=1)
    return result


@dataclass(frozen=True)
class AcdSession:
    session_date: date
    start: datetime
    opening_end: datetime
    end: datetime

    @property
    def weekend_carry(self) -> bool:
        return (self.end.date() - self.start.date()).days > 1


def acd_session_for_date(session_date: date) -> AcdSession:
    if session_date.weekday() >= 5:
        raise ValueError("ACD session_date must be a New York weekday")
    start = datetime.combine(session_date, ACD_START, tzinfo=NEW_YORK)
    opening_end = start + timedelta(minutes=ACD_OPENING_MINUTES)
    end = datetime.combine(_next_weekday(session_date), ACD_START, tzinfo=NEW_YORK)
    return AcdSession(
        session_date=session_date,
        start=start.astimezone(timezone.utc),
        opening_end=opening_end.astimezone(timezone.utc),
        end=end.astimezone(timezone.utc),
    )


def acd_session_for_timestamp(value: datetime) -> AcdSession:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    local = value.astimezone(NEW_YORK)
    session_date = local.date()
    if session_date.weekday() >= 5 or local.timetz().replace(tzinfo=None) < ACD_START:
        session_date = _previous_weekday(session_date)
    return acd_session_for_date(session_date)


class CausalAcdEngine:
    """New York opening-band ACD clock with exact, non-backdated events."""

    def __init__(
        self,
        session: AcdSession,
        *,
        a_offset: float = 0.75,
        c_offset: float = 1.0,
        acceptance_minutes: float = 7.5,
        maximum_quote_gap_seconds: float = 60.0,
    ) -> None:
        if a_offset < 0 or c_offset < 0 or acceptance_minutes <= 0 or maximum_quote_gap_seconds <= 0:
            raise ValueError("invalid ACD parameters")
        self.session = session
        self.a_offset = float(a_offset)
        self.c_offset = float(c_offset)
        self.acceptance = timedelta(minutes=float(acceptance_minutes))
        self.maximum_quote_gap = timedelta(seconds=float(maximum_quote_gap_seconds))
        self.opening_high: float | None = None
        self.opening_low: float | None = None
        self.levels: dict[str, float] | None = None
        self.locked_at_tick: int | None = None
        self.direction: str | None = None
        self.confirmed_at_tick: int | None = None
        self._up_since: datetime | None = None
        self._down_since: datetime | None = None
        self._previous_timestamp: datetime | None = None

    def process_tick(self, tick: Tick) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        if tick.timestamp < self.session.start or tick.timestamp >= self.session.end:
            return events
        if tick.timestamp < self.session.opening_end:
            self.opening_high = tick.mid if self.opening_high is None else max(self.opening_high, tick.mid)
            self.opening_low = tick.mid if self.opening_low is None else min(self.opening_low, tick.mid)
            self._previous_timestamp = tick.timestamp
            return events
        if self.levels is None:
            if self.opening_high is None or self.opening_low is None:
                return events
            self.levels = {
                "openingHigh": self.opening_high,
                "openingLow": self.opening_low,
                "aUp": self.opening_high + self.a_offset,
                "aDown": self.opening_low - self.a_offset,
                "cUp": self.opening_high + self.c_offset,
                "cDown": self.opening_low - self.c_offset,
            }
            self.locked_at_tick = tick.tick_id
            events.append(self._event("OR_LOCK", tick))

        if self.direction is not None:
            self._previous_timestamp = tick.timestamp
            return events
        if self._previous_timestamp is not None and tick.timestamp - self._previous_timestamp > self.maximum_quote_gap:
            self._up_since = None
            self._down_since = None
        self._previous_timestamp = tick.timestamp
        assert self.levels is not None
        self._up_since = tick.timestamp if self._up_since is None and tick.bid >= self.levels["aUp"] else (
            self._up_since if tick.bid >= self.levels["aUp"] else None
        )
        self._down_since = tick.timestamp if self._down_since is None and tick.ask <= self.levels["aDown"] else (
            self._down_since if tick.ask <= self.levels["aDown"] else None
        )
        if self._up_since is not None and tick.timestamp - self._up_since >= self.acceptance:
            self.direction = "up"
            self.confirmed_at_tick = tick.tick_id
            events.append(self._event("A_UP", tick))
        elif self._down_since is not None and tick.timestamp - self._down_since >= self.acceptance:
            self.direction = "down"
            self.confirmed_at_tick = tick.tick_id
            events.append(self._event("A_DOWN", tick))
        return events

    def _event(self, kind: str, tick: Tick) -> dict[str, object]:
        return {
            "kind": kind,
            "tickId": tick.tick_id,
            "timestamp": tick.timestamp.isoformat(),
            "bid": tick.bid,
            "ask": tick.ask,
            "mid": tick.mid,
            "levels": dict(self.levels or {}),
        }

    def snapshot(self) -> dict[str, object]:
        return {
            "sessionDate": self.session.session_date.isoformat(),
            "start": self.session.start.isoformat(),
            "openingEnd": self.session.opening_end.isoformat(),
            "end": self.session.end.isoformat(),
            "weekendCarry": self.session.weekend_carry,
            "levels": dict(self.levels or {}),
            "lockedAtTick": self.locked_at_tick,
            "direction": self.direction,
            "confirmedAtTick": self.confirmed_at_tick,
            "acceptanceMinutes": self.acceptance.total_seconds() / 60.0,
        }
