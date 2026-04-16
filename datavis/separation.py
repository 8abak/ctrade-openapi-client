from __future__ import annotations

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
SEPARATION_VERSION = 1
LEVELS = ("micro", "median", "macro")


@dataclass(frozen=True)
class LevelConfig:
    level: str
    unitalpha: float
    minunit: float
    maxunit: float
    movethreshold: float
    reversalthreshold: float
    flatthreshold: float
    flatpoints: int
    maxpoints: int
    maxdurationseconds: int


@dataclass(frozen=True)
class ShapeConfig:
    spikeefficiency: float = 0.82
    spikethickness: float = 1.55
    driftefficiency: float = 0.58
    driftthickness: float = 2.60
    ovalefficiency: float = 0.46
    ovalthickness: float = 3.10
    balanceefficiency: float = 0.28
    balancenetunits: float = 1.10


SEPARATION_LEVEL_CONFIG: Dict[str, LevelConfig] = {
    "micro": LevelConfig(
        level="micro",
        unitalpha=0.24,
        minunit=0.02,
        maxunit=2.50,
        movethreshold=4.20,
        reversalthreshold=2.00,
        flatthreshold=1.30,
        flatpoints=18,
        maxpoints=140,
        maxdurationseconds=1800,
    ),
    "median": LevelConfig(
        level="median",
        unitalpha=0.18,
        minunit=0.05,
        maxunit=6.00,
        movethreshold=1.90,
        reversalthreshold=1.25,
        flatthreshold=1.08,
        flatpoints=6,
        maxpoints=42,
        maxdurationseconds=6 * 3600,
    ),
    "macro": LevelConfig(
        level="macro",
        unitalpha=0.14,
        minunit=0.10,
        maxunit=12.00,
        movethreshold=1.45,
        reversalthreshold=1.00,
        flatthreshold=0.98,
        flatpoints=4,
        maxpoints=24,
        maxdurationseconds=24 * 3600,
    ),
}
SEPARATION_SHAPE_CONFIG = ShapeConfig()


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
        "time": timestamp.astimezone(timezone.utc) if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc),
        "refprice": float(reference),
        "highprice": float(reference),
        "lowprice": float(reference),
        "starttickid": tick_id,
        "endtickid": tick_id,
        "sourceid": tick_id,
        "pointcount": 1,
    }


def point_from_segment(segment: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "time": segment["endtime"],
        "refprice": float(segment["endprice"]),
        "highprice": float(segment["highprice"]),
        "lowprice": float(segment["lowprice"]),
        "starttickid": int(segment["starttickid"]),
        "endtickid": int(segment["endtickid"]),
        "sourceid": int(segment["endtickid"]),
        "pointcount": int(segment.get("tickcount") or 1),
    }


def _shape_type(direction: str, efficiency: float, thickness: float, netmove: float, unitprice: float) -> str:
    config = SEPARATION_SHAPE_CONFIG
    if direction == "flat":
        if efficiency <= config.balanceefficiency and abs(netmove) <= unitprice * config.balancenetunits:
            return "balance"
        if thickness >= config.ovalthickness:
            return "oval"
        return "transition"
    if efficiency >= config.spikeefficiency and thickness <= config.spikethickness:
        return "spike"
    if efficiency >= config.driftefficiency and thickness <= config.driftthickness:
        return "drift"
    if efficiency <= config.ovalefficiency and thickness >= config.ovalthickness:
        return "oval"
    if efficiency <= config.balanceefficiency and abs(netmove) <= unitprice * config.balancenetunits:
        return "balance"
    return "transition"


class SeparationEngine:
    def __init__(self, level: str):
        if level not in SEPARATION_LEVEL_CONFIG:
            raise ValueError(f"Unsupported separation level: {level}")
        self.level = level
        self.config = SEPARATION_LEVEL_CONFIG[level]
        self.reset()

    def reset(self) -> None:
        self.unitprice = float(self.config.minunit)
        self.lastpoint: Optional[Dict[str, Any]] = None
        self.current: Optional[Dict[str, Any]] = None

    def restore(self, row: Dict[str, Any]) -> None:
        self.reset()
        if not row:
            return
        starttime = row.get("starttime")
        lasttime = row.get("lasttime")
        if not isinstance(starttime, datetime) or not isinstance(lasttime, datetime):
            return
        startprice = float(row.get("startprice") or 0.0)
        lastprice = float(row.get("lastprice") or 0.0)
        highprice = float(row.get("highprice") or lastprice or startprice)
        lowprice = float(row.get("lowprice") or lastprice or startprice)
        opentickid = int(row.get("opentickid") or row.get("lastsourceid") or 0)
        lastsourceid = int(row.get("lastsourceid") or opentickid)
        tickcount = max(1, int(row.get("tickcount") or 1))
        self.unitprice = clamp(
            float(row.get("unitprice") or self.config.minunit),
            self.config.minunit,
            self.config.maxunit,
        )
        self.current = {
            "starttime": starttime.astimezone(timezone.utc) if starttime.tzinfo else starttime.replace(tzinfo=timezone.utc),
            "endtime": lasttime.astimezone(timezone.utc) if lasttime.tzinfo else lasttime.replace(tzinfo=timezone.utc),
            "startprice": startprice,
            "endprice": lastprice,
            "highprice": highprice,
            "lowprice": lowprice,
            "tickcount": tickcount,
            "pathlength": float(row.get("pathlength") or 0.0),
            "directioncandidate": str(row.get("directioncandidate") or "flat"),
            "unitprice": self.unitprice,
            "starttickid": opentickid,
            "endtickid": lastsourceid,
            "lastsourceid": lastsourceid,
        }
        self.lastpoint = {
            "time": self.current["endtime"],
            "refprice": lastprice,
            "highprice": highprice,
            "lowprice": lowprice,
            "starttickid": lastsourceid,
            "endtickid": lastsourceid,
            "sourceid": lastsourceid,
            "pointcount": 1,
        }

    def current_state_row(self, *, symbol: str, brokerday: date) -> Optional[Dict[str, Any]]:
        if not self.current:
            return None
        packet = self.current
        return {
            "symbol": symbol,
            "brokerday": brokerday,
            "level": self.level,
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
            "updatedat": utc_now(),
        }

    def current_segment(self, *, symbol: str, brokerday: date, sourcemode: str) -> Optional[Dict[str, Any]]:
        if not self.current:
            return None
        return self._finalize(
            packet=self.current,
            symbol=symbol,
            brokerday=brokerday,
            sourcemode=sourcemode,
            status="open",
        )

    def process_point(
        self,
        point: Dict[str, Any],
        *,
        symbol: str,
        brokerday: date,
        sourcemode: str,
    ) -> List[Dict[str, Any]]:
        closed: List[Dict[str, Any]] = []
        point = dict(point)
        point["refprice"] = float(point["refprice"])
        point["highprice"] = float(point["highprice"])
        point["lowprice"] = float(point["lowprice"])
        point["starttickid"] = int(point["starttickid"])
        point["endtickid"] = int(point["endtickid"])
        point["sourceid"] = int(point.get("sourceid") or point["endtickid"])
        point["pointcount"] = max(1, int(point.get("pointcount") or 1))
        point_time = point["time"]
        if point_time.tzinfo is None:
            point_time = point_time.replace(tzinfo=timezone.utc)
        point["time"] = point_time.astimezone(timezone.utc)

        if self.lastpoint is not None:
            absdelta = abs(point["refprice"] - float(self.lastpoint["refprice"]))
            self.unitprice = clamp(
                (self.config.unitalpha * absdelta) + ((1.0 - self.config.unitalpha) * float(self.unitprice)),
                self.config.minunit,
                self.config.maxunit,
            )

        if self.current is None:
            self.current = self._open_packet(point)
            self.lastpoint = point
            return closed

        packet = self.current
        packet["endtime"] = point["time"]
        packet["endprice"] = point["refprice"]
        packet["highprice"] = max(float(packet["highprice"]), point["highprice"])
        packet["lowprice"] = min(float(packet["lowprice"]), point["lowprice"])
        packet["tickcount"] = int(packet["tickcount"]) + point["pointcount"]
        packet["endtickid"] = max(int(packet["endtickid"]), int(point["endtickid"]))
        packet["lastsourceid"] = int(point["sourceid"])
        if self.lastpoint is not None:
            packet["pathlength"] = float(packet["pathlength"]) + abs(point["refprice"] - float(self.lastpoint["refprice"]))
        packet["unitprice"] = float(self.unitprice)
        packet["directioncandidate"] = self._direction_candidate(packet)

        close_reason = self._close_reason(packet, point)
        self.lastpoint = point
        if close_reason:
            closed.append(
                self._finalize(
                    packet=packet,
                    symbol=symbol,
                    brokerday=brokerday,
                    sourcemode=sourcemode,
                    status="closed",
                )
            )
            self.current = self._open_packet(point)
        return closed

    def force_close(
        self,
        *,
        symbol: str,
        brokerday: date,
        sourcemode: str,
    ) -> List[Dict[str, Any]]:
        if not self.current:
            return []
        closed = [
            self._finalize(
                packet=self.current,
                symbol=symbol,
                brokerday=brokerday,
                sourcemode=sourcemode,
                status="closed",
            )
        ]
        self.current = None
        return closed

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
            "unitprice": float(self.unitprice),
            "starttickid": int(point["starttickid"]),
            "endtickid": int(point["endtickid"]),
            "lastsourceid": int(point["sourceid"]),
        }

    def _direction_candidate(self, packet: Dict[str, Any]) -> str:
        move = float(packet["endprice"]) - float(packet["startprice"])
        threshold = float(self.unitprice) * float(self.config.flatthreshold)
        if move >= threshold:
            return "up"
        if move <= -threshold:
            return "down"
        return "flat"

    def _close_reason(self, packet: Dict[str, Any], point: Dict[str, Any]) -> Optional[str]:
        move = float(packet["endprice"]) - float(packet["startprice"])
        duration_seconds = max(0.0, (packet["endtime"] - packet["starttime"]).total_seconds())
        directional_move = float(self.unitprice) * float(self.config.movethreshold)
        reversal_move = float(self.unitprice) * float(self.config.reversalthreshold)
        flat_move = float(self.unitprice) * float(self.config.flatthreshold)
        candidate = str(packet.get("directioncandidate") or "flat")

        if candidate == "up":
            advance = float(packet["highprice"]) - float(packet["startprice"])
            pullback = float(packet["highprice"]) - float(point["lowprice"])
            if advance >= directional_move and pullback >= reversal_move:
                return "reversal"
        elif candidate == "down":
            advance = float(packet["startprice"]) - float(packet["lowprice"])
            pullback = float(point["highprice"]) - float(packet["lowprice"])
            if advance >= directional_move and pullback >= reversal_move:
                return "reversal"
        else:
            efficiency = self._efficiency(packet, unit_override=max(float(self.unitprice), 0.000001))
            if (
                int(packet["tickcount"]) >= int(self.config.flatpoints)
                and abs(move) <= flat_move
                and efficiency <= SEPARATION_SHAPE_CONFIG.balanceefficiency + 0.05
            ):
                return "balance"

        if int(packet["tickcount"]) >= int(self.config.maxpoints):
            return "pointcap"
        if duration_seconds >= float(self.config.maxdurationseconds):
            return "timecap"
        return None

    def _efficiency(self, packet: Dict[str, Any], *, unit_override: Optional[float] = None) -> float:
        pathlength = float(packet.get("pathlength") or 0.0)
        netmove = abs(float(packet["endprice"]) - float(packet["startprice"]))
        if pathlength <= 0:
            fallback = float(unit_override if unit_override is not None else self.unitprice)
            return 1.0 if netmove >= fallback else 0.0
        return netmove / pathlength

    def _finalize(
        self,
        *,
        packet: Dict[str, Any],
        symbol: str,
        brokerday: date,
        sourcemode: str,
        status: str,
    ) -> Dict[str, Any]:
        startprice = float(packet["startprice"])
        endprice = float(packet["endprice"])
        highprice = float(packet["highprice"])
        lowprice = float(packet["lowprice"])
        unitprice = clamp(float(packet.get("unitprice") or self.unitprice), self.config.minunit, self.config.maxunit)
        netmove = endprice - startprice
        rangeprice = max(0.0, highprice - lowprice)
        pathlength = max(0.0, float(packet.get("pathlength") or 0.0))
        efficiency = self._efficiency(packet, unit_override=unitprice)
        thickness = rangeprice / max(abs(netmove), unitprice)
        duration_ms = max(0, int((packet["endtime"] - packet["starttime"]).total_seconds() * 1000.0))
        direction = "flat"
        if abs(netmove) >= unitprice * self.config.flatthreshold:
            direction = "up" if netmove > 0 else "down"
        shapetype = _shape_type(direction, efficiency, thickness, netmove, unitprice)
        angle = 0.0
        if duration_ms > 0:
            angle = netmove / (duration_ms / 1000.0)
        return {
            "symbol": symbol,
            "brokerday": brokerday,
            "level": self.level,
            "status": status,
            "sourcemode": sourcemode,
            "starttickid": int(packet["starttickid"]),
            "endtickid": int(packet["endtickid"]),
            "starttime": packet["starttime"],
            "endtime": packet["endtime"],
            "startprice": startprice,
            "endprice": endprice,
            "highprice": highprice,
            "lowprice": lowprice,
            "tickcount": int(packet["tickcount"]),
            "netmove": netmove,
            "rangeprice": rangeprice,
            "pathlength": pathlength,
            "efficiency": efficiency,
            "thickness": thickness,
            "direction": direction,
            "shapetype": shapetype,
            "angle": angle,
            "unitprice": unitprice,
            "version": SEPARATION_VERSION,
            "createdat": utc_now(),
            "updatedat": utc_now(),
        }


class SeparationCascade:
    def __init__(self, *, symbol: str, sourcemode: str):
        self.symbol = symbol
        self.sourcemode = sourcemode
        self.engines = {level: SeparationEngine(level) for level in LEVELS}
        self.brokerday: Optional[date] = None

    def reset(self) -> None:
        for engine in self.engines.values():
            engine.reset()
        self.brokerday = None

    def restore(self, *, brokerday: date, state_rows: Sequence[Dict[str, Any]]) -> None:
        self.reset()
        self.brokerday = brokerday
        by_level = {str(row.get("level")): row for row in state_rows}
        for level, engine in self.engines.items():
            engine.restore(by_level.get(level) or {})

    def process_tick(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        point = point_from_tick(row)
        if point is None:
            return []
        point_brokerday = brokerday_for_timestamp(point["time"])
        closed: List[Dict[str, Any]] = []
        if self.brokerday is None:
            self.brokerday = point_brokerday
        elif point_brokerday != self.brokerday:
            closed.extend(self.force_close_all())
            for engine in self.engines.values():
                engine.reset()
            self.brokerday = point_brokerday

        micro_closed = self.engines["micro"].process_point(
            point,
            symbol=self.symbol,
            brokerday=self.brokerday,
            sourcemode=self.sourcemode,
        )
        closed.extend(micro_closed)
        for micro_segment in micro_closed:
            median_closed = self.engines["median"].process_point(
                point_from_segment(micro_segment),
                symbol=self.symbol,
                brokerday=self.brokerday,
                sourcemode=self.sourcemode,
            )
            closed.extend(median_closed)
            for median_segment in median_closed:
                closed.extend(
                    self.engines["macro"].process_point(
                        point_from_segment(median_segment),
                        symbol=self.symbol,
                        brokerday=self.brokerday,
                        sourcemode=self.sourcemode,
                    )
                )
        return closed

    def force_close_all(self) -> List[Dict[str, Any]]:
        if self.brokerday is None:
            return []
        closed: List[Dict[str, Any]] = []
        micro_closed = self.engines["micro"].force_close(
            symbol=self.symbol,
            brokerday=self.brokerday,
            sourcemode=self.sourcemode,
        )
        closed.extend(micro_closed)
        for micro_segment in micro_closed:
            median_closed = self.engines["median"].process_point(
                point_from_segment(micro_segment),
                symbol=self.symbol,
                brokerday=self.brokerday,
                sourcemode=self.sourcemode,
            )
            closed.extend(median_closed)
            for median_segment in median_closed:
                closed.extend(
                    self.engines["macro"].process_point(
                        point_from_segment(median_segment),
                        symbol=self.symbol,
                        brokerday=self.brokerday,
                        sourcemode=self.sourcemode,
                    )
                )
        closed.extend(self.engines["median"].force_close(symbol=self.symbol, brokerday=self.brokerday, sourcemode=self.sourcemode))
        closed.extend(self.engines["macro"].force_close(symbol=self.symbol, brokerday=self.brokerday, sourcemode=self.sourcemode))
        return _dedupe_segments(closed)

    def current_open_segments(self) -> List[Dict[str, Any]]:
        if self.brokerday is None:
            return []
        open_segments = []
        for level in LEVELS:
            segment = self.engines[level].current_segment(
                symbol=self.symbol,
                brokerday=self.brokerday,
                sourcemode=self.sourcemode,
            )
            if segment is not None:
                open_segments.append(segment)
        return open_segments

    def current_state_rows(self) -> List[Dict[str, Any]]:
        if self.brokerday is None:
            return []
        rows = []
        for level in LEVELS:
            row = self.engines[level].current_state_row(symbol=self.symbol, brokerday=self.brokerday)
            if row is not None:
                rows.append(row)
        return rows


def _dedupe_segments(items: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    output: List[Dict[str, Any]] = []
    for item in items:
        key = (
            item.get("brokerday"),
            item.get("level"),
            item.get("status"),
            item.get("starttickid"),
            item.get("endtickid"),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def fetch_latest_tick(conn: Any, *, symbol: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def fetch_ticks_after(conn: Any, *, symbol: str, after_id: int, limit: int) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, max(0, int(after_id or 0)), max(1, int(limit or 1))),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_ticks_for_brokerday(conn: Any, *, symbol: str, brokerday: date) -> List[Dict[str, Any]]:
    start_ts, end_ts = brokerday_bounds(brokerday)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY id ASC
            """,
            (symbol, start_ts, end_ts),
        )
        return [dict(row) for row in cur.fetchall()]


def delete_brokerday(conn: Any, *, symbol: str, brokerday: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.separationstate
            WHERE symbol = %s
              AND brokerday = %s
            """,
            (symbol, brokerday),
        )
        cur.execute(
            """
            DELETE FROM public.separationsegments
            WHERE symbol = %s
              AND brokerday = %s
            """,
            (symbol, brokerday),
        )


def _update_or_insert_closed_segment(conn: Any, segment: Dict[str, Any]) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            UPDATE public.separationsegments
            SET
                status = 'closed',
                sourcemode = %(sourcemode)s,
                endtickid = %(endtickid)s,
                endtime = %(endtime)s,
                endprice = %(endprice)s,
                highprice = %(highprice)s,
                lowprice = %(lowprice)s,
                tickcount = %(tickcount)s,
                netmove = %(netmove)s,
                rangeprice = %(rangeprice)s,
                pathlength = %(pathlength)s,
                efficiency = %(efficiency)s,
                thickness = %(thickness)s,
                direction = %(direction)s,
                shapetype = %(shapetype)s,
                angle = %(angle)s,
                unitprice = %(unitprice)s,
                version = %(version)s,
                updatedat = %(updatedat)s
            WHERE symbol = %(symbol)s
              AND brokerday = %(brokerday)s
              AND level = %(level)s
              AND status = 'open'
              AND starttickid = %(starttickid)s
            RETURNING id
            """,
            segment,
        )
        row = cur.fetchone()
        if row:
            return
        cur.execute(
            """
            INSERT INTO public.separationsegments (
                symbol, brokerday, level, status, sourcemode,
                starttickid, endtickid, starttime, endtime,
                startprice, endprice, highprice, lowprice, tickcount,
                netmove, rangeprice, pathlength, efficiency, thickness,
                direction, shapetype, angle, unitprice, version,
                createdat, updatedat
            ) VALUES (
                %(symbol)s, %(brokerday)s, %(level)s, %(status)s, %(sourcemode)s,
                %(starttickid)s, %(endtickid)s, %(starttime)s, %(endtime)s,
                %(startprice)s, %(endprice)s, %(highprice)s, %(lowprice)s, %(tickcount)s,
                %(netmove)s, %(rangeprice)s, %(pathlength)s, %(efficiency)s, %(thickness)s,
                %(direction)s, %(shapetype)s, %(angle)s, %(unitprice)s, %(version)s,
                %(createdat)s, %(updatedat)s
            )
            """,
            segment,
        )


def sync_open_segments(conn: Any, *, symbol: str, brokerday: Optional[date], open_segments: Sequence[Dict[str, Any]]) -> None:
    wanted_by_level = {str(item["level"]): item for item in open_segments}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if brokerday is None:
            cur.execute(
                """
                DELETE FROM public.separationsegments
                WHERE symbol = %s
                  AND status = 'open'
                """,
                (symbol,),
            )
            return
        cur.execute(
            """
            SELECT id, level
            FROM public.separationsegments
            WHERE symbol = %s
              AND brokerday = %s
              AND status = 'open'
            FOR UPDATE
            """,
            (symbol, brokerday),
        )
        existing_rows = [dict(row) for row in cur.fetchall()]
        existing_by_level = {str(row["level"]): int(row["id"]) for row in existing_rows}

        for level in LEVELS:
            item = wanted_by_level.get(level)
            existing_id = existing_by_level.get(level)
            if item is None:
                if existing_id is not None:
                    cur.execute("DELETE FROM public.separationsegments WHERE id = %s", (existing_id,))
                continue
            if existing_id is not None:
                payload = dict(item)
                payload["id"] = existing_id
                cur.execute(
                    """
                    UPDATE public.separationsegments
                    SET
                        sourcemode = %(sourcemode)s,
                        starttickid = %(starttickid)s,
                        endtickid = %(endtickid)s,
                        starttime = %(starttime)s,
                        endtime = %(endtime)s,
                        startprice = %(startprice)s,
                        endprice = %(endprice)s,
                        highprice = %(highprice)s,
                        lowprice = %(lowprice)s,
                        tickcount = %(tickcount)s,
                        netmove = %(netmove)s,
                        rangeprice = %(rangeprice)s,
                        pathlength = %(pathlength)s,
                        efficiency = %(efficiency)s,
                        thickness = %(thickness)s,
                        direction = %(direction)s,
                        shapetype = %(shapetype)s,
                        angle = %(angle)s,
                        unitprice = %(unitprice)s,
                        version = %(version)s,
                        updatedat = %(updatedat)s
                    WHERE id = %(id)s
                    """,
                    payload,
                )
            else:
                cur.execute(
                    """
                    INSERT INTO public.separationsegments (
                        symbol, brokerday, level, status, sourcemode,
                        starttickid, endtickid, starttime, endtime,
                        startprice, endprice, highprice, lowprice, tickcount,
                        netmove, rangeprice, pathlength, efficiency, thickness,
                        direction, shapetype, angle, unitprice, version,
                        createdat, updatedat
                    ) VALUES (
                        %(symbol)s, %(brokerday)s, %(level)s, %(status)s, %(sourcemode)s,
                        %(starttickid)s, %(endtickid)s, %(starttime)s, %(endtime)s,
                        %(startprice)s, %(endprice)s, %(highprice)s, %(lowprice)s, %(tickcount)s,
                        %(netmove)s, %(rangeprice)s, %(pathlength)s, %(efficiency)s, %(thickness)s,
                        %(direction)s, %(shapetype)s, %(angle)s, %(unitprice)s, %(version)s,
                        %(createdat)s, %(updatedat)s
                    )
                    """,
                    item,
                )


def sync_state_rows(conn: Any, *, symbol: str, brokerday: Optional[date], rows: Sequence[Dict[str, Any]]) -> None:
    wanted_by_level = {str(item["level"]): item for item in rows}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if brokerday is None:
            cur.execute("DELETE FROM public.separationstate WHERE symbol = %s", (symbol,))
            return
        cur.execute(
            """
            SELECT id, level
            FROM public.separationstate
            WHERE symbol = %s
              AND brokerday = %s
            FOR UPDATE
            """,
            (symbol, brokerday),
        )
        existing_rows = [dict(row) for row in cur.fetchall()]
        existing_by_level = {str(row["level"]): int(row["id"]) for row in existing_rows}

        for level in LEVELS:
            item = wanted_by_level.get(level)
            existing_id = existing_by_level.get(level)
            if item is None:
                if existing_id is not None:
                    cur.execute("DELETE FROM public.separationstate WHERE id = %s", (existing_id,))
                continue
            if existing_id is not None:
                payload = dict(item)
                payload["id"] = existing_id
                cur.execute(
                    """
                    UPDATE public.separationstate
                    SET
                        lastsourceid = %(lastsourceid)s,
                        opentickid = %(opentickid)s,
                        starttime = %(starttime)s,
                        startprice = %(startprice)s,
                        lasttime = %(lasttime)s,
                        lastprice = %(lastprice)s,
                        highprice = %(highprice)s,
                        lowprice = %(lowprice)s,
                        tickcount = %(tickcount)s,
                        pathlength = %(pathlength)s,
                        directioncandidate = %(directioncandidate)s,
                        unitprice = %(unitprice)s,
                        status = %(status)s,
                        updatedat = %(updatedat)s
                    WHERE id = %(id)s
                    """,
                    payload,
                )
            else:
                cur.execute(
                    """
                    INSERT INTO public.separationstate (
                        symbol, brokerday, level, lastsourceid, opentickid,
                        starttime, startprice, lasttime, lastprice,
                        highprice, lowprice, tickcount, pathlength,
                        directioncandidate, unitprice, status, updatedat
                    ) VALUES (
                        %(symbol)s, %(brokerday)s, %(level)s, %(lastsourceid)s, %(opentickid)s,
                        %(starttime)s, %(startprice)s, %(lasttime)s, %(lastprice)s,
                        %(highprice)s, %(lowprice)s, %(tickcount)s, %(pathlength)s,
                        %(directioncandidate)s, %(unitprice)s, %(status)s, %(updatedat)s
                    )
                    """,
                    item,
                )


def load_state_rows(conn: Any, *, symbol: str, brokerday: date) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.separationstate
            WHERE symbol = %s
              AND brokerday = %s
            ORDER BY level
            """,
            (symbol, brokerday),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_run(
    conn: Any,
    *,
    symbol: str,
    brokerday: Optional[date],
    mode: str,
    starttickid: Optional[int],
    endtickid: Optional[int],
    tickcount: int,
    microcount: int,
    mediancount: int,
    macrocount: int,
    status: str,
    message: str,
    startedat: datetime,
    finishedat: Optional[datetime],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.separationruns (
                symbol, brokerday, mode, starttickid, endtickid, tickcount,
                microcount, mediancount, macrocount, status, message,
                startedat, finishedat
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            """,
            (
                symbol,
                brokerday,
                mode,
                starttickid,
                endtickid,
                tickcount,
                microcount,
                mediancount,
                macrocount,
                status,
                message,
                startedat,
                finishedat,
            ),
        )


def persist_batch(
    conn: Any,
    *,
    symbol: str,
    brokerday: Optional[date],
    closed_segments: Sequence[Dict[str, Any]],
    open_segments: Sequence[Dict[str, Any]],
    state_rows: Sequence[Dict[str, Any]],
) -> None:
    for segment in closed_segments:
        _update_or_insert_closed_segment(conn, segment)
    sync_open_segments(conn, symbol=symbol, brokerday=brokerday, open_segments=open_segments)
    sync_state_rows(conn, symbol=symbol, brokerday=brokerday, rows=state_rows)


def counts_by_level(segments: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    counts = {level: 0 for level in LEVELS}
    for item in segments:
        level = str(item.get("level") or "")
        if level in counts and str(item.get("status") or "") == "closed":
            counts[level] += 1
    return counts


def latest_processed_tick_id(state_rows: Sequence[Dict[str, Any]]) -> int:
    for row in state_rows:
        if str(row.get("level")) == "micro":
            return int(row.get("lastsourceid") or 0)
    return 0


def run_backfill_for_brokerday(
    conn: Any,
    *,
    symbol: str,
    brokerday: date,
    replace: bool,
) -> Dict[str, Any]:
    startedat = utc_now()
    if replace:
        delete_brokerday(conn, symbol=symbol, brokerday=brokerday)
    rows = fetch_ticks_for_brokerday(conn, symbol=symbol, brokerday=brokerday)
    cascade = SeparationCascade(symbol=symbol, sourcemode="backfill")
    closed_segments: List[Dict[str, Any]] = []
    for row in rows:
        closed_segments.extend(cascade.process_tick(row))
    closed_segments.extend(cascade.force_close_all())
    persist_batch(
        conn,
        symbol=symbol,
        brokerday=brokerday,
        closed_segments=_dedupe_segments(closed_segments),
        open_segments=[],
        state_rows=[],
    )
    counts = counts_by_level(closed_segments)
    insert_run(
        conn,
        symbol=symbol,
        brokerday=brokerday,
        mode="backfill",
        starttickid=int(rows[0]["id"]) if rows else None,
        endtickid=int(rows[-1]["id"]) if rows else None,
        tickcount=len(rows),
        microcount=counts["micro"],
        mediancount=counts["median"],
        macrocount=counts["macro"],
        status="done",
        message="Backfill completed.",
        startedat=startedat,
        finishedat=utc_now(),
    )
    return {
        "symbol": symbol,
        "brokerday": brokerday,
        "tickcount": len(rows),
        "counts": counts,
    }


class SeparationLiveRuntime:
    def __init__(self, *, symbol: str, batch_size: int = 400):
        self.symbol = symbol
        self.batch_size = max(1, int(batch_size))
        self.cascade = SeparationCascade(symbol=symbol, sourcemode="live")

    def bootstrap(self, conn: Any, *, brokerday: Optional[date] = None) -> Dict[str, Any]:
        latest_tick = fetch_latest_tick(conn, symbol=self.symbol)
        target_brokerday = brokerday
        if target_brokerday is None and latest_tick and isinstance(latest_tick.get("timestamp"), datetime):
            target_brokerday = brokerday_for_timestamp(latest_tick["timestamp"])
        if target_brokerday is None:
            return {"brokerday": None, "tickcount": 0, "counts": counts_by_level([])}

        state_rows = load_state_rows(conn, symbol=self.symbol, brokerday=target_brokerday)
        if state_rows:
            self.cascade.restore(brokerday=target_brokerday, state_rows=state_rows)
            return {"brokerday": target_brokerday, "tickcount": 0, "counts": counts_by_level([])}

        delete_brokerday(conn, symbol=self.symbol, brokerday=target_brokerday)
        rows = fetch_ticks_for_brokerday(conn, symbol=self.symbol, brokerday=target_brokerday)
        closed_segments: List[Dict[str, Any]] = []
        for row in rows:
            closed_segments.extend(self.cascade.process_tick(row))
        persist_batch(
            conn,
            symbol=self.symbol,
            brokerday=self.cascade.brokerday,
            closed_segments=_dedupe_segments(closed_segments),
            open_segments=self.cascade.current_open_segments(),
            state_rows=self.cascade.current_state_rows(),
        )
        counts = counts_by_level(closed_segments)
        insert_run(
            conn,
            symbol=self.symbol,
            brokerday=target_brokerday,
            mode="live-bootstrap",
            starttickid=int(rows[0]["id"]) if rows else None,
            endtickid=int(rows[-1]["id"]) if rows else None,
            tickcount=len(rows),
            microcount=counts["micro"],
            mediancount=counts["median"],
            macrocount=counts["macro"],
            status="done",
            message="Live bootstrap completed.",
            startedat=utc_now(),
            finishedat=utc_now(),
        )
        return {"brokerday": target_brokerday, "tickcount": len(rows), "counts": counts}

    def process_once(self, conn: Any) -> Dict[str, Any]:
        brokerday = self.cascade.brokerday
        if brokerday is None:
            bootstrap_result = self.bootstrap(conn)
            brokerday = bootstrap_result.get("brokerday")
            if brokerday is None:
                return {"brokerday": None, "tickcount": 0, "counts": counts_by_level([])}

        state_rows = load_state_rows(conn, symbol=self.symbol, brokerday=brokerday)
        after_id = latest_processed_tick_id(state_rows)
        startedat = utc_now()
        rows = fetch_ticks_after(conn, symbol=self.symbol, after_id=after_id, limit=self.batch_size)
        closed_segments: List[Dict[str, Any]] = []
        for row in rows:
            closed_segments.extend(self.cascade.process_tick(row))
        persist_batch(
            conn,
            symbol=self.symbol,
            brokerday=self.cascade.brokerday,
            closed_segments=_dedupe_segments(closed_segments),
            open_segments=self.cascade.current_open_segments(),
            state_rows=self.cascade.current_state_rows(),
        )
        counts = counts_by_level(closed_segments)
        if rows:
            insert_run(
                conn,
                symbol=self.symbol,
                brokerday=self.cascade.brokerday,
                mode="live-incremental",
                starttickid=int(rows[0]["id"]),
                endtickid=int(rows[-1]["id"]),
                tickcount=len(rows),
                microcount=counts["micro"],
                mediancount=counts["median"],
                macrocount=counts["macro"],
                status="done",
                message="Live incremental batch completed.",
                startedat=startedat,
                finishedat=utc_now(),
            )
        return {"brokerday": self.cascade.brokerday, "tickcount": len(rows), "counts": counts}
