from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, Generator, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

from datavis.db import db_connect as shared_db_connect


BROKER_TIMEZONE = ZoneInfo("Australia/Sydney")
BROKER_DAY_START_HOUR = 8
SEPARATION_VERSION = 2
LEVELS = ("micro", "median", "macro")


@dataclass(frozen=True)
class BehaviorConfig:
    flatmoveunits: float = 1.20
    impulseefficiency: float = 0.78
    impulsenetratio: float = 0.62
    impulsedepth: float = 1.85
    driftefficiency: float = 0.52
    driftnetratio: float = 0.34
    driftdepth: float = 3.10
    balanceefficiency: float = 0.26
    balancerangeunits: float = 2.40
    churnefficiency: float = 0.38
    churndepth: float = 4.80
    transitionoverlap: float = 0.60


@dataclass(frozen=True)
class MicroConfig:
    level: str
    unitalpha: float
    minunit: float
    maxunit: float
    flatthreshold: float
    minticks: int
    mindurationseconds: int
    minrangeunits: float
    minpathunits: float
    directionmoveunits: float
    reversalunits: float
    reversalpoints: int
    reversalepsilonunits: float
    flatticks: int
    flatdurationseconds: int
    flatrangeunits: float
    flatefficiency: float
    maxpoints: int
    maxdurationseconds: int


@dataclass(frozen=True)
class ContainerConfig:
    level: str
    minunit: float
    maxunit: float
    flatthreshold: float
    minchildcount: int
    compatibilitythreshold: float
    incompatibilitythreshold: float
    confirmationcount: int
    changerangeunits: float
    changemoveunits: float
    hysteresis: float
    maxchildcount: int
    maxdurationseconds: int


SEPARATION_BEHAVIOR_CONFIG = BehaviorConfig()
SEPARATION_MICRO_CONFIG = MicroConfig(
    level="micro",
    unitalpha=0.08,
    minunit=0.03,
    maxunit=3.50,
    flatthreshold=1.80,
    minticks=24,
    mindurationseconds=45,
    minrangeunits=6.00,
    minpathunits=8.00,
    directionmoveunits=7.20,
    reversalunits=3.40,
    reversalpoints=4,
    reversalepsilonunits=0.35,
    flatticks=52,
    flatdurationseconds=180,
    flatrangeunits=3.20,
    flatefficiency=0.25,
    maxpoints=420,
    maxdurationseconds=3600,
)
SEPARATION_CONTAINER_CONFIG: Dict[str, ContainerConfig] = {
    "median": ContainerConfig(
        level="median",
        minunit=0.12,
        maxunit=25.00,
        flatthreshold=0.90,
        minchildcount=3,
        compatibilitythreshold=0.35,
        incompatibilitythreshold=-0.30,
        confirmationcount=2,
        changerangeunits=2.60,
        changemoveunits=1.60,
        hysteresis=0.18,
        maxchildcount=18,
        maxdurationseconds=12 * 3600,
    ),
    "macro": ContainerConfig(
        level="macro",
        minunit=0.25,
        maxunit=60.00,
        flatthreshold=0.82,
        minchildcount=3,
        compatibilitythreshold=0.48,
        incompatibilitythreshold=-0.36,
        confirmationcount=2,
        changerangeunits=1.90,
        changemoveunits=1.25,
        hysteresis=0.20,
        maxchildcount=12,
        maxdurationseconds=24 * 3600,
    ),
}


def database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if value.startswith("postgresql+psycopg2://"):
        value = value.replace("postgresql+psycopg2://", "postgresql://", 1)
    return value


def db_connect(*, readonly: bool = False, autocommit: bool = False) -> Any:
    url = database_url()
    if url:
        conn = psycopg2.connect(url)
        conn.autocommit = autocommit
        if readonly:
            conn.set_session(readonly=True, autocommit=autocommit)
        return conn
    return shared_db_connect(readonly=readonly, autocommit=autocommit)


@contextmanager
def db_connection(*, readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
    conn = db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def brokerday_for_timestamp(value: datetime) -> date:
    localized = value.astimezone(BROKER_TIMEZONE)
    if localized.timetz().replace(tzinfo=None) < dt_time(hour=BROKER_DAY_START_HOUR):
        localized = localized - timedelta(days=1)
    return localized.date()


def brokerday_bounds(day_value: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(day_value, dt_time(hour=BROKER_DAY_START_HOUR), tzinfo=BROKER_TIMEZONE)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def iter_brokerdays(start_day: date, end_day: date) -> Iterable[date]:
    cursor = start_day
    while cursor <= end_day:
        yield cursor
        cursor += timedelta(days=1)


def tick_mid(row: Dict[str, Any]) -> Optional[float]:
    mid = row.get("mid")
    if mid is not None:
        return float(mid)
    bid = row.get("bid")
    ask = row.get("ask")
    if bid is None and ask is None:
        return None
    if bid is None:
        return float(ask)
    if ask is None:
        return float(bid)
    return (float(bid) + float(ask)) / 2.0


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_state_time(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        try:
            return _as_utc(datetime.fromisoformat(value))
        except ValueError:
            return None
    return None


def _coerce_state_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _duration_ms(starttime: datetime, endtime: datetime) -> int:
    return max(0, int((endtime - starttime).total_seconds() * 1000.0))


def _range_overlap_ratio(low_one: float, high_one: float, low_two: float, high_two: float, fallback: float) -> float:
    overlap = max(0.0, min(high_one, high_two) - max(low_one, low_two))
    base = max(fallback, min(max(0.0, high_one - low_one), max(0.0, high_two - low_two)))
    return overlap / base if base > 0 else 0.0


def _regime_family(regime: str) -> str:
    return regime.split("_", 1)[0] if regime else "transition"


def _behavior_profile(
    *,
    startprice: float,
    endprice: float,
    highprice: float,
    lowprice: float,
    pathlength: float,
    unitprice: float,
    flatthreshold: float,
    starttime: datetime,
    endtime: datetime,
    previous: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    config = SEPARATION_BEHAVIOR_CONFIG
    netmove = endprice - startprice
    rangeprice = max(0.0, highprice - lowprice)
    efficiency = 0.0
    if pathlength > 0:
        efficiency = abs(netmove) / pathlength
    elif abs(netmove) >= max(unitprice, 1e-9):
        efficiency = 1.0
    thickness = rangeprice / max(abs(netmove), unitprice, 1e-9)
    direction = "flat"
    if abs(netmove) >= max(unitprice * flatthreshold, 1e-9):
        direction = "up" if netmove > 0 else "down"
    overlap = 0.0
    expansion = 1.0
    if previous:
        overlap = _range_overlap_ratio(
            lowprice,
            highprice,
            float(previous.get("lowprice") or lowprice),
            float(previous.get("highprice") or highprice),
            max(unitprice, float(previous.get("unitprice") or unitprice), 1e-9),
        )
        previous_range = max(float(previous.get("rangeprice") or 0.0), float(previous.get("unitprice") or unitprice), 1e-9)
        expansion = rangeprice / previous_range if previous_range > 0 else 1.0
    regime = "transition"
    if direction == "flat":
        if efficiency <= config.balanceefficiency and rangeprice <= unitprice * config.balancerangeunits:
            regime = "balance"
        elif efficiency <= config.churnefficiency or thickness >= config.churndepth:
            regime = "churn"
        else:
            regime = "transition"
    else:
        if (
            efficiency >= config.impulseefficiency
            and thickness <= config.impulsedepth
            and abs(netmove) >= max(unitprice, rangeprice * config.impulsenetratio)
        ):
            regime = f"impulse_{direction}"
        elif (
            efficiency >= config.driftefficiency
            and thickness <= config.driftdepth
            and abs(netmove) >= max(unitprice, rangeprice * config.driftnetratio)
        ):
            regime = f"drift_{direction}"
        elif overlap >= config.transitionoverlap and expansion <= 1.15:
            regime = "transition"
        elif efficiency <= config.balanceefficiency and thickness >= config.churndepth:
            regime = "transition"
        else:
            regime = f"drift_{direction}"
    duration_ms = _duration_ms(starttime, endtime)
    slope = netmove / max(duration_ms / 1000.0, 1e-9) if duration_ms > 0 else 0.0
    return {
        "direction": direction,
        "efficiency": efficiency,
        "thickness": thickness,
        "rangeprice": rangeprice,
        "durationms": duration_ms,
        "slope": slope,
        "overlap": overlap,
        "expansion": expansion,
        "regime": regime,
        "family": _regime_family(regime),
    }


def point_from_tick(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    reference = tick_mid(row)
    if reference is None:
        return None
    timestamp = row.get("timestamp")
    if not isinstance(timestamp, datetime):
        return None
    tick_id = int(row.get("id") or 0)
    if tick_id <= 0:
        return None
    return {
        "time": _as_utc(timestamp),
        "refprice": float(reference),
        "highprice": float(reference),
        "lowprice": float(reference),
        "starttickid": tick_id,
        "endtickid": tick_id,
        "sourceid": tick_id,
        "pointcount": 1,
    }


def _summary_from_segment(segment: Dict[str, Any]) -> Dict[str, Any]:
    profile = dict(segment.get("_profile") or {})
    if not profile:
        level = str(segment.get("level") or "micro")
        flatthreshold = (
            SEPARATION_MICRO_CONFIG.flatthreshold
            if level == "micro"
            else SEPARATION_CONTAINER_CONFIG[level].flatthreshold
        )
        profile = _behavior_profile(
            startprice=float(segment["startprice"]),
            endprice=float(segment["endprice"]),
            highprice=float(segment["highprice"]),
            lowprice=float(segment["lowprice"]),
            pathlength=float(segment.get("pathlength") or 0.0),
            unitprice=max(float(segment.get("unitprice") or 0.0), 1e-9),
            flatthreshold=float(flatthreshold),
            starttime=_as_utc(segment["starttime"]),
            endtime=_as_utc(segment["endtime"]),
        )
    return {
        "level": str(segment.get("level") or ""),
        "starttickid": int(segment.get("starttickid") or 0),
        "endtickid": int(segment.get("endtickid") or 0),
        "starttime": _as_utc(segment["starttime"]),
        "endtime": _as_utc(segment["endtime"]),
        "startprice": float(segment["startprice"]),
        "endprice": float(segment["endprice"]),
        "highprice": float(segment["highprice"]),
        "lowprice": float(segment["lowprice"]),
        "tickcount": int(segment.get("tickcount") or 0),
        "netmove": float(segment.get("netmove") or 0.0),
        "rangeprice": float(segment.get("rangeprice") or 0.0),
        "pathlength": float(segment.get("pathlength") or 0.0),
        "efficiency": float(segment.get("efficiency") or 0.0),
        "thickness": float(segment.get("thickness") or 0.0),
        "direction": str(segment.get("direction") or "flat"),
        "shapetype": str(segment.get("shapetype") or "transition"),
        "angle": float(segment.get("angle") or 0.0),
        "unitprice": float(segment.get("unitprice") or 0.0),
        "profile": profile,
    }


def _serialize_summary(summary: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not summary:
        return None
    payload = dict(summary)
    payload["starttime"] = summary["starttime"].isoformat()
    payload["endtime"] = summary["endtime"].isoformat()
    return payload


def _deserialize_summary(summary: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(summary, dict):
        return None
    starttime = _parse_state_time(summary.get("starttime"))
    endtime = _parse_state_time(summary.get("endtime"))
    if starttime is None or endtime is None:
        return None
    payload = dict(summary)
    payload["starttime"] = starttime
    payload["endtime"] = endtime
    payload["starttickid"] = int(payload.get("starttickid") or 0)
    payload["endtickid"] = int(payload.get("endtickid") or 0)
    payload["tickcount"] = int(payload.get("tickcount") or 0)
    for key in (
        "startprice",
        "endprice",
        "highprice",
        "lowprice",
        "netmove",
        "rangeprice",
        "pathlength",
        "efficiency",
        "thickness",
        "angle",
        "unitprice",
    ):
        payload[key] = float(payload.get(key) or 0.0)
    payload["profile"] = dict(payload.get("profile") or {})
    return payload


def _aggregate_child_summaries(children: Sequence[Dict[str, Any]], config: ContainerConfig) -> Optional[Dict[str, Any]]:
    if not children:
        return None
    first = children[0]
    last = children[-1]
    unit_components = [max(float(child.get("rangeprice") or 0.0), float(child.get("unitprice") or 0.0), config.minunit) for child in children]
    return {
        "starttime": first["starttime"],
        "endtime": last["endtime"],
        "startprice": float(first["startprice"]),
        "endprice": float(last["endprice"]),
        "highprice": max(float(child["highprice"]) for child in children),
        "lowprice": min(float(child["lowprice"]) for child in children),
        "tickcount": sum(max(0, int(child.get("tickcount") or 0)) for child in children),
        "pathlength": sum(max(0.0, float(child.get("pathlength") or 0.0)) for child in children),
        "unitprice": clamp(sum(unit_components) / float(len(unit_components)), config.minunit, config.maxunit),
        "starttickid": int(first["starttickid"]),
        "endtickid": int(last["endtickid"]),
        "lastsourceid": int(last["endtickid"]),
        "childcount": len(children),
    }


def _finalize_packet(
    *,
    level: str,
    flatthreshold: float,
    packet: Dict[str, Any],
    symbol: str,
    brokerday: date,
    sourcemode: str,
    status: str,
    previous_closed: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    starttime = _as_utc(packet["starttime"])
    endtime = _as_utc(packet["endtime"])
    startprice = float(packet["startprice"])
    endprice = float(packet["endprice"])
    highprice = float(packet["highprice"])
    lowprice = float(packet["lowprice"])
    pathlength = max(0.0, float(packet.get("pathlength") or 0.0))
    unitprice = max(float(packet.get("unitprice") or 0.0), 1e-9)
    netmove = endprice - startprice
    rangeprice = max(0.0, highprice - lowprice)
    profile = _behavior_profile(
        startprice=startprice,
        endprice=endprice,
        highprice=highprice,
        lowprice=lowprice,
        pathlength=pathlength,
        unitprice=unitprice,
        flatthreshold=flatthreshold,
        starttime=starttime,
        endtime=endtime,
        previous=previous_closed,
    )
    return {
        "symbol": symbol,
        "brokerday": brokerday,
        "level": level,
        "status": status,
        "sourcemode": sourcemode,
        "starttickid": int(packet["starttickid"]),
        "endtickid": int(packet["endtickid"]),
        "starttime": starttime,
        "endtime": endtime,
        "startprice": startprice,
        "endprice": endprice,
        "highprice": highprice,
        "lowprice": lowprice,
        "tickcount": int(packet.get("tickcount") or 0),
        "netmove": netmove,
        "rangeprice": rangeprice,
        "pathlength": pathlength,
        "efficiency": float(profile["efficiency"]),
        "thickness": float(profile["thickness"]),
        "direction": str(profile["direction"]),
        "shapetype": str(profile["regime"]),
        "angle": float(profile["slope"]),
        "unitprice": unitprice,
        "version": SEPARATION_VERSION,
        "createdat": utc_now(),
        "updatedat": utc_now(),
        "_profile": profile,
        "childcount": int(packet.get("childcount") or 0),
    }


def _compatibility_score(left: Dict[str, Any], right: Dict[str, Any]) -> float:
    left_profile = dict(left.get("profile") or {})
    right_profile = dict(right.get("profile") or {})
    left_direction = str(left_profile.get("direction") or left.get("direction") or "flat")
    right_direction = str(right_profile.get("direction") or right.get("direction") or "flat")
    left_family = str(left_profile.get("family") or _regime_family(str(left_profile.get("regime") or left.get("shapetype") or "")))
    right_family = str(right_profile.get("family") or _regime_family(str(right_profile.get("regime") or right.get("shapetype") or "")))
    score = 0.0
    if left_direction == right_direction:
        score += 0.70 if left_direction != "flat" else 0.25
    elif "flat" in {left_direction, right_direction}:
        score -= 0.05
    else:
        score -= 0.90
    if left_family == right_family:
        score += 0.85
    elif left_direction == right_direction and {left_family, right_family}.issubset({"impulse", "drift", "transition"}):
        score += 0.30
    elif "balance" in {left_family, right_family} or "churn" in {left_family, right_family}:
        score -= 0.10 if left_direction == right_direction else 0.45
    else:
        score -= 0.55
    left_range = max(float(left.get("rangeprice") or 0.0), float(left.get("unitprice") or 0.0), 1e-9)
    right_range = max(float(right.get("rangeprice") or 0.0), float(right.get("unitprice") or 0.0), 1e-9)
    range_ratio = max(left_range, right_range) / min(left_range, right_range)
    score += 0.18 if range_ratio <= 2.40 else -0.16
    efficiency_gap = abs(float(left_profile.get("efficiency") or left.get("efficiency") or 0.0) - float(right_profile.get("efficiency") or right.get("efficiency") or 0.0))
    score += 0.20 if efficiency_gap <= 0.22 else -0.16
    overlap = _range_overlap_ratio(
        float(left.get("lowprice") or 0.0),
        float(left.get("highprice") or 0.0),
        float(right.get("lowprice") or 0.0),
        float(right.get("highprice") or 0.0),
        max(float(left.get("unitprice") or 0.0), float(right.get("unitprice") or 0.0), 1e-9),
    )
    if overlap >= 0.35:
        score += 0.15
    elif overlap <= 0.08 and left_direction != right_direction and "flat" not in {left_direction, right_direction}:
        score -= 0.20
    return score


class MicroSeparationEngine:
    def __init__(self) -> None:
        self.config = SEPARATION_MICRO_CONFIG
        self.reset()

    def reset(self) -> None:
        self.unitprice = float(self.config.minunit)
        self.lastpoint: Optional[Dict[str, Any]] = None
        self.current: Optional[Dict[str, Any]] = None
        self.previous_closed: Optional[Dict[str, Any]] = None

    def restore(self, row: Dict[str, Any]) -> None:
        self.reset()
        state = _coerce_state_json(row.get("statejson"))
        if int(state.get("engineVersion") or 0) != SEPARATION_VERSION:
            return
        self.unitprice = clamp(float(state.get("unitprice") or self.config.minunit), self.config.minunit, self.config.maxunit)
        self.previous_closed = _deserialize_summary(state.get("previousClosed"))
        lastpoint = state.get("lastPoint")
        if isinstance(lastpoint, dict):
            point_time = _parse_state_time(lastpoint.get("time"))
            if point_time is not None:
                self.lastpoint = {
                    "time": point_time,
                    "refprice": float(lastpoint.get("refprice") or 0.0),
                    "highprice": float(lastpoint.get("highprice") or 0.0),
                    "lowprice": float(lastpoint.get("lowprice") or 0.0),
                    "starttickid": int(lastpoint.get("starttickid") or 0),
                    "endtickid": int(lastpoint.get("endtickid") or 0),
                    "sourceid": int(lastpoint.get("sourceid") or 0),
                    "pointcount": max(1, int(lastpoint.get("pointcount") or 1)),
                }
        packet = state.get("current")
        if isinstance(packet, dict):
            starttime = _parse_state_time(packet.get("starttime"))
            endtime = _parse_state_time(packet.get("endtime"))
            if starttime is not None and endtime is not None:
                self.current = {
                    "starttime": starttime,
                    "endtime": endtime,
                    "startprice": float(packet.get("startprice") or 0.0),
                    "endprice": float(packet.get("endprice") or 0.0),
                    "highprice": float(packet.get("highprice") or 0.0),
                    "lowprice": float(packet.get("lowprice") or 0.0),
                    "tickcount": max(1, int(packet.get("tickcount") or 1)),
                    "pathlength": float(packet.get("pathlength") or 0.0),
                    "directioncandidate": str(packet.get("directioncandidate") or "flat"),
                    "dominantdirection": str(packet.get("dominantdirection") or "flat"),
                    "dominantprice": float(packet.get("dominantprice") or packet.get("startprice") or 0.0),
                    "reversalpoints": max(0, int(packet.get("reversalpoints") or 0)),
                    "unitprice": clamp(float(packet.get("unitprice") or self.unitprice), self.config.minunit, self.config.maxunit),
                    "starttickid": int(packet.get("starttickid") or 0),
                    "endtickid": int(packet.get("endtickid") or 0),
                    "lastsourceid": int(packet.get("lastsourceid") or packet.get("endtickid") or 0),
                }

    def serialize_state(self) -> Dict[str, Any]:
        return {
            "engineVersion": SEPARATION_VERSION,
            "unitprice": self.unitprice,
            "previousClosed": _serialize_summary(self.previous_closed),
            "lastPoint": None
            if self.lastpoint is None
            else {
                "time": self.lastpoint["time"].isoformat(),
                "refprice": self.lastpoint["refprice"],
                "highprice": self.lastpoint["highprice"],
                "lowprice": self.lastpoint["lowprice"],
                "starttickid": self.lastpoint["starttickid"],
                "endtickid": self.lastpoint["endtickid"],
                "sourceid": self.lastpoint["sourceid"],
                "pointcount": self.lastpoint["pointcount"],
            },
            "current": None
            if self.current is None
            else {
                "starttime": self.current["starttime"].isoformat(),
                "endtime": self.current["endtime"].isoformat(),
                "startprice": self.current["startprice"],
                "endprice": self.current["endprice"],
                "highprice": self.current["highprice"],
                "lowprice": self.current["lowprice"],
                "tickcount": self.current["tickcount"],
                "pathlength": self.current["pathlength"],
                "directioncandidate": self.current["directioncandidate"],
                "dominantdirection": self.current["dominantdirection"],
                "dominantprice": self.current["dominantprice"],
                "reversalpoints": self.current["reversalpoints"],
                "unitprice": self.current["unitprice"],
                "starttickid": self.current["starttickid"],
                "endtickid": self.current["endtickid"],
                "lastsourceid": self.current["lastsourceid"],
            },
        }

    def current_state_row(self, *, symbol: str, brokerday: date) -> Optional[Dict[str, Any]]:
        if self.current is None:
            return None
        packet = self.current
        return {
            "symbol": symbol,
            "brokerday": brokerday,
            "level": "micro",
            "lastsourceid": int(packet["lastsourceid"]),
            "opentickid": int(packet["starttickid"]),
            "starttime": packet["starttime"],
            "startprice": packet["startprice"],
            "lasttime": packet["endtime"],
            "lastprice": packet["endprice"],
            "highprice": packet["highprice"],
            "lowprice": packet["lowprice"],
            "tickcount": int(packet["tickcount"]),
            "pathlength": float(packet["pathlength"]),
            "directioncandidate": str(packet["directioncandidate"]),
            "unitprice": float(self.unitprice),
            "status": "open",
            "statejson": self.serialize_state(),
            "updatedat": utc_now(),
        }

    def current_segment(self, *, symbol: str, brokerday: date, sourcemode: str) -> Optional[Dict[str, Any]]:
        if self.current is None:
            return None
        return _finalize_packet(
            level="micro",
            flatthreshold=self.config.flatthreshold,
            packet=self.current,
            symbol=symbol,
            brokerday=brokerday,
            sourcemode=sourcemode,
            status="open",
            previous_closed=self.previous_closed,
        )

    def _open_packet(self, point: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "starttime": point["time"],
            "endtime": point["time"],
            "startprice": point["refprice"],
            "endprice": point["refprice"],
            "highprice": point["highprice"],
            "lowprice": point["lowprice"],
            "tickcount": point["pointcount"],
            "pathlength": 0.0,
            "directioncandidate": "flat",
            "dominantdirection": "flat",
            "dominantprice": point["refprice"],
            "reversalpoints": 0,
            "unitprice": float(self.unitprice),
            "starttickid": int(point["starttickid"]),
            "endtickid": int(point["endtickid"]),
            "lastsourceid": int(point["sourceid"]),
        }

    def _refresh_direction_state(self, packet: Dict[str, Any], point: Dict[str, Any]) -> None:
        upmove = float(packet["highprice"]) - float(packet["startprice"])
        downmove = float(packet["startprice"]) - float(packet["lowprice"])
        dominant = "flat"
        dominant_price = float(packet["startprice"])
        if upmove >= downmove and upmove >= float(self.unitprice) * (self.config.directionmoveunits * 0.60):
            dominant = "up"
            dominant_price = float(packet["highprice"])
        elif downmove > upmove and downmove >= float(self.unitprice) * (self.config.directionmoveunits * 0.60):
            dominant = "down"
            dominant_price = float(packet["lowprice"])
        packet["dominantdirection"] = dominant
        packet["dominantprice"] = dominant_price
        netmove = float(packet["endprice"]) - float(packet["startprice"])
        flat_move = float(self.unitprice) * float(self.config.flatthreshold)
        if netmove >= flat_move:
            packet["directioncandidate"] = "up"
        elif netmove <= -flat_move:
            packet["directioncandidate"] = "down"
        else:
            packet["directioncandidate"] = "flat"
        step_delta = 0.0
        if self.lastpoint is not None:
            step_delta = float(point["refprice"]) - float(self.lastpoint["refprice"])
        if dominant == "up":
            retreat = dominant_price - float(point["refprice"])
            if retreat >= float(self.unitprice) * self.config.reversalepsilonunits and step_delta < 0:
                packet["reversalpoints"] = int(packet.get("reversalpoints") or 0) + int(point["pointcount"])
            elif float(point["refprice"]) >= dominant_price - (float(self.unitprice) * self.config.reversalepsilonunits * 0.40):
                packet["reversalpoints"] = 0
        elif dominant == "down":
            retreat = float(point["refprice"]) - dominant_price
            if retreat >= float(self.unitprice) * self.config.reversalepsilonunits and step_delta > 0:
                packet["reversalpoints"] = int(packet.get("reversalpoints") or 0) + int(point["pointcount"])
            elif float(point["refprice"]) <= dominant_price + (float(self.unitprice) * self.config.reversalepsilonunits * 0.40):
                packet["reversalpoints"] = 0
        else:
            packet["reversalpoints"] = 0


