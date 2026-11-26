from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class Direction(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class StructureType(str, Enum):
    H = "H"
    L = "L"
    HH = "HH"
    HL = "HL"
    LH = "LH"
    LL = "LL"

    @property
    def is_high(self) -> bool:
        return self in {self.H, self.HH, self.LH}

    @property
    def is_low(self) -> bool:
        return self in {self.L, self.HL, self.LL}


class RTState(str, Enum):
    WAITING_FOR_FIRST_RT = "WAITING_FOR_FIRST_RT"
    BEARISH_ACTIVE = "BEARISH_ACTIVE"
    BEARISH_OUTSIDE_H = "BEARISH_OUTSIDE_H"
    BEARISH_REVERSAL_ARMED = "BEARISH_REVERSAL_ARMED"
    BULLISH_ACTIVE = "BULLISH_ACTIVE"
    BULLISH_OUTSIDE_L = "BULLISH_OUTSIDE_L"
    BULLISH_REVERSAL_ARMED = "BULLISH_REVERSAL_ARMED"
    RT_REVISION = "RT_REVISION"
    RT_REVERSAL_CONFIRMED = "RT_REVERSAL_CONFIRMED"


class EventType(str, Enum):
    FIRST_RT_CREATED = "FIRST_RT_CREATED"
    OUTSIDE_H_DETECTED = "OUTSIDE_H_DETECTED"
    OUTSIDE_L_DETECTED = "OUTSIDE_L_DETECTED"
    REVERSAL_ARMED = "REVERSAL_ARMED"
    CANDIDATE_H_BROKEN = "CANDIDATE_H_BROKEN"
    CANDIDATE_L_BROKEN = "CANDIDATE_L_BROKEN"
    DEGENERATE_REVERSAL_IGNORED = "DEGENERATE_REVERSAL_IGNORED"
    RT_REVISED = "RT_REVISED"
    RT_TERMINATED = "RT_TERMINATED"
    BULLISH_RT_CREATED = "BULLISH_RT_CREATED"
    BEARISH_RT_CREATED = "BEARISH_RT_CREATED"
    ACD_EVENT = "ACD_EVENT"


@dataclass(frozen=True)
class Tick:
    tick_id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "tick_id", int(self.tick_id))
        object.__setattr__(self, "timestamp", _aware(self.timestamp))
        object.__setattr__(self, "bid", float(self.bid))
        object.__setattr__(self, "ask", float(self.ask))
        object.__setattr__(self, "mid", float(self.mid))
        if self.tick_id <= 0:
            raise ValueError("tick_id must be positive")
        if self.ask < self.bid:
            raise ValueError("ask must not be below bid")

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat()
        return result


@dataclass(frozen=True)
class StructuralPoint:
    tick_id: int
    timestamp: datetime
    bid: float
    ask: float
    mid: float
    classification: StructureType
    confirmation_tick_id: int
    confirmation_timestamp: datetime
    previous_structural_tick_id: Optional[int] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tick_id", int(self.tick_id))
        object.__setattr__(self, "confirmation_tick_id", int(self.confirmation_tick_id))
        object.__setattr__(self, "timestamp", _aware(self.timestamp))
        object.__setattr__(self, "confirmation_timestamp", _aware(self.confirmation_timestamp))
        object.__setattr__(self, "bid", float(self.bid))
        object.__setattr__(self, "ask", float(self.ask))
        object.__setattr__(self, "mid", float(self.mid))
        if not isinstance(self.classification, StructureType):
            object.__setattr__(self, "classification", StructureType(self.classification))
        if self.tick_id <= 0 or self.confirmation_tick_id <= 0:
            raise ValueError("structural tick IDs must be positive")
        if self.confirmation_tick_id < self.tick_id:
            raise ValueError("a structural point cannot be confirmed before its pivot tick")
        if self.confirmation_timestamp < self.timestamp:
            raise ValueError("a structural point cannot be confirmed before its pivot time")
        if self.ask < self.bid:
            raise ValueError("ask must not be below bid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "tickId": self.tick_id,
            "timestamp": self.timestamp.isoformat(),
            "bid": self.bid,
            "ask": self.ask,
            "mid": self.mid,
            "classification": self.classification.value,
            "confirmationTickId": self.confirmation_tick_id,
            "confirmationTimestamp": self.confirmation_timestamp.isoformat(),
            "previousStructuralTickId": self.previous_structural_tick_id,
        }


@dataclass(frozen=True)
class RTRevision:
    revision_number: int
    first_anchor_tick_id: int
    second_anchor_tick_id: int
    fit_start_timestamp: datetime
    fit_end_timestamp: datetime
    fit_tick_count: int
    slope: float
    intercept: float
    std_deviation: float
    upper_offset: float
    lower_offset: float
    middle_offset: float
    knowable_at_tick: int
    knowable_at_timestamp: datetime
    reason: str
    triggering_candidate_tick_id: Optional[int] = None
    confirming_structure_tick_id: Optional[int] = None


@dataclass
class RegressionTrain:
    rt_id: str
    direction: Direction
    first_anchor_tick_id: int
    second_anchor_tick_id: int
    fit_start_timestamp: datetime
    fit_end_timestamp: datetime
    fit_tick_count: int
    slope: float
    intercept: float
    std_deviation: float
    upper_band_parameters: dict[str, float]
    lower_band_parameters: dict[str, float]
    middle_line_parameters: dict[str, float]
    revision_number: int
    created_at_tick: int
    confirmed_at_tick: int
    terminated_at_tick: Optional[int] = None
    termination_reason: Optional[str] = None
    previous_rt_id: Optional[str] = None
    revisions: list[RTRevision] = field(default_factory=list)


@dataclass(frozen=True)
class StateTransition:
    tick_id: int
    timestamp: datetime
    previous_state: RTState
    new_state: RTState
    cause: str
    structural_object: Optional[dict[str, Any]]
    rt_id: Optional[str]
    revision_number: int


@dataclass(frozen=True)
class LedgerEvent:
    sequence: int
    event_type: EventType
    tick_id: int
    timestamp: datetime
    rt_id: Optional[str]
    revision_number: int
    payload: dict[str, Any]
