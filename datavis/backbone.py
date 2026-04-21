from __future__ import annotations

import argparse
import json
import math
import os
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from datavis.brokerday import brokerday_bounds, brokerday_for_timestamp, tick_mid
from datavis.db import db_connect as shared_db_connect


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

BACKBONE_VERSION = 2
BACKBONE_SOURCE = "adaptivehysteresis"
BIGBONES_SOURCE = "adaptivehysteresis.bigbones"
DEFAULT_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD").strip().upper() or "XAUUSD"

SPREAD_SPAN = 100
NOISE_WINDOW = 200
THRESHOLD_SMOOTH_SPAN = 50
SPREAD_WEIGHT = 0.50
DELTA_Q80_WEIGHT = 3.00
DELTA_STD_WEIGHT = 3.00
THRESHOLD_FLOOR = 0.25
ABS_DELTA_MIN_PERIODS = 20

_DAYS_TABLE_DESCRIPTOR: Any = ...


@dataclass(frozen=True)
class DayRef:
    dayid: int
    brokerday: date
    starttime: datetime
    endtime: datetime


@dataclass(frozen=True)
class DaysTableDescriptor:
    datecol: Optional[str]
    endcol: Optional[str]
    startcol: Optional[str]
    symbolcol: Optional[str]


@dataclass
class BackbonePoint:
    index: int
    price: float
    tickid: int
    ticktime: datetime


def database_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    return ""


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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _derive_spread(row: Dict[str, Any]) -> Optional[float]:
    spread = _safe_float(row.get("spread"))
    if spread is not None:
        return spread
    bid = _safe_float(row.get("bid"))
    ask = _safe_float(row.get("ask"))
    if bid is None or ask is None:
        return None
    return float(ask - bid)


def _synthetic_dayid(brokerday: date) -> int:
    return int(brokerday.strftime("%Y%m%d"))


def _first_matching(columns: Sequence[str], names: Sequence[str]) -> Optional[str]:
    column_map = {str(name).lower(): str(name) for name in columns}
    for candidate in names:
        if candidate.lower() in column_map:
            return column_map[candidate.lower()]
    return None


def describe_days_table(conn: Any) -> Optional[DaysTableDescriptor]:
    global _DAYS_TABLE_DESCRIPTOR
    if _DAYS_TABLE_DESCRIPTOR is not ...:
        return _DAYS_TABLE_DESCRIPTOR

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'days'
            ORDER BY ordinal_position
            """
        )
        columns = [str(row[0]) for row in cur.fetchall()]

    if not columns or "id" not in {column.lower() for column in columns}:
        _DAYS_TABLE_DESCRIPTOR = None
        return None

    descriptor = DaysTableDescriptor(
        datecol=_first_matching(columns, ("brokerday", "daydate", "tradingday", "day", "sessionday")),
        startcol=_first_matching(columns, ("starttime", "startat", "startts", "daystart", "sessionstart", "opentime")),
        endcol=_first_matching(columns, ("endtime", "endat", "endts", "dayend", "sessionend", "closetime")),
        symbolcol=_first_matching(columns, ("symbol",)),
    )
    if descriptor.datecol is None and (descriptor.startcol is None or descriptor.endcol is None):
        _DAYS_TABLE_DESCRIPTOR = None
        return None
    _DAYS_TABLE_DESCRIPTOR = descriptor
    return descriptor


def resolve_day_ref_for_timestamp(conn: Any, *, symbol: str, timestamp: datetime) -> DayRef:
    brokerday = brokerday_for_timestamp(timestamp)
    fallback_start, fallback_end = brokerday_bounds(brokerday)
    descriptor = describe_days_table(conn)
    if descriptor is None:
        return DayRef(dayid=_synthetic_dayid(brokerday), brokerday=brokerday, starttime=fallback_start, endtime=fallback_end)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if descriptor.startcol and descriptor.endcol:
            where = ["{0} <= %s".format(descriptor.startcol), "{0} > %s".format(descriptor.endcol)]
            params: List[Any] = [timestamp, timestamp]
            if descriptor.symbolcol:
                where.append("{0} = %s".format(descriptor.symbolcol))
                params.append(symbol)
            cur.execute(
                """
                SELECT id, {datecol}, {startcol}, {endcol}
                FROM public.days
                WHERE {where_sql}
                ORDER BY {startcol} DESC, id DESC
                LIMIT 1
                """.format(
                    datecol=descriptor.datecol or "NULL AS brokerday",
                    startcol=descriptor.startcol,
                    endcol=descriptor.endcol,
                    where_sql=" AND ".join(where),
                ),
                tuple(params),
            )
            row = dict(cur.fetchone() or {})
            if row:
                resolved_day = row.get(descriptor.datecol) if descriptor.datecol else brokerday
                return DayRef(
                    dayid=int(row["id"]),
                    brokerday=resolved_day or brokerday,
                    starttime=_as_utc(row.get(descriptor.startcol) or fallback_start),
                    endtime=_as_utc(row.get(descriptor.endcol) or fallback_end),
                )

        if descriptor.datecol:
            where = ["{0} = %s".format(descriptor.datecol)]
            params = [brokerday]
            if descriptor.symbolcol:
                where.append("{0} = %s".format(descriptor.symbolcol))
                params.append(symbol)
            cur.execute(
                """
                SELECT id, {datecol}
                FROM public.days
                WHERE {where_sql}
                ORDER BY id DESC
                LIMIT 1
                """.format(datecol=descriptor.datecol, where_sql=" AND ".join(where)),
                tuple(params),
            )
            row = dict(cur.fetchone() or {})
            if row:
                resolved_day = row.get(descriptor.datecol) or brokerday
                return DayRef(
                    dayid=int(row["id"]),
                    brokerday=resolved_day,
                    starttime=fallback_start,
                    endtime=fallback_end,
                )

    return DayRef(dayid=_synthetic_dayid(brokerday), brokerday=brokerday, starttime=fallback_start, endtime=fallback_end)


def resolve_day_ref_for_brokerday(conn: Any, *, symbol: str, brokerday: date) -> DayRef:
    starttime, _ = brokerday_bounds(brokerday)
    return resolve_day_ref_for_timestamp(conn, symbol=symbol, timestamp=starttime)


def fetch_latest_tick(conn: Any, *, symbol: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp
            FROM public.ticks
            WHERE symbol = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def resolve_current_day_ref(conn: Any, *, symbol: str) -> Optional[DayRef]:
    latest_tick = fetch_latest_tick(conn, symbol=symbol)
    latest_timestamp = latest_tick.get("timestamp") if latest_tick else None
    if latest_timestamp is None:
        return None
    return resolve_day_ref_for_timestamp(conn, symbol=symbol, timestamp=latest_timestamp)


def fetch_day_latest_tick(conn: Any, *, symbol: str, dayref: DayRef) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol, dayref.starttime, dayref.endtime),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def iter_ticks_for_day(conn: Any, *, symbol: str, dayref: DayRef, batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    with conn.cursor(name="backbone_day_ticks", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = max(1, int(batch_size))
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, dayref.starttime, dayref.endtime),
        )
        while True:
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                return
            yield [dict(row) for row in rows]


def fetch_ticks_after_for_day(conn: Any, *, symbol: str, dayref: DayRef, after_id: int, limit: int) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
              AND id > %s
            ORDER BY timestamp ASC, id ASC
            LIMIT %s
            """,
            (symbol, dayref.starttime, dayref.endtime, after_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_day_latest_move(conn: Any, *, dayref: DayRef, source: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT endtickid, endtime
            FROM public.backbonemoves
            WHERE dayid = %s
              AND source = %s
            ORDER BY endtickid DESC, id DESC
            LIMIT 1
            """,
            (dayref.dayid, source),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def load_state_row(conn: Any, *, symbol: str, dayid: int, source: str = BACKBONE_SOURCE) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.backbonestate
            WHERE symbol = %s
              AND dayid = %s
              AND source = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol, dayid, source),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def delete_day(conn: Any, *, dayid: int, symbol: str, source: Optional[str] = None) -> None:
    with conn.cursor() as cur:
        if source:
            cur.execute(
                "DELETE FROM public.backbonestate WHERE symbol = %s AND dayid = %s AND source = %s",
                (symbol, dayid, source),
            )
            cur.execute("DELETE FROM public.backbonemoves WHERE dayid = %s AND source = %s", (dayid, source))
            cur.execute("DELETE FROM public.backbonepivots WHERE dayid = %s AND source = %s", (dayid, source))
            return
        cur.execute("DELETE FROM public.backbonestate WHERE symbol = %s AND dayid = %s", (symbol, dayid))
        cur.execute("DELETE FROM public.backbonemoves WHERE dayid = %s", (dayid,))
        cur.execute("DELETE FROM public.backbonepivots WHERE dayid = %s", (dayid,))


class RollingPercentileWindow:
    def __init__(self, *, window: int, quantile: float, min_periods: int) -> None:
        self.window = max(1, int(window))
        self.quantile = max(0.0, min(1.0, float(quantile)))
        self.min_periods = max(1, int(min_periods))
        self.values: deque[float] = deque()
        self.sorted_values: List[float] = []

    def restore(self, values: Sequence[float]) -> None:
        self.values = deque()
        self.sorted_values = []
        for value in values[-self.window :]:
            self.add(float(value))

    def add(self, value: Optional[float]) -> Optional[float]:
        if value is None or not math.isfinite(float(value)):
            return self.current()
        numeric = float(value)
        if len(self.values) >= self.window:
            removed = self.values.popleft()
            removed_index = self._bisect_left(self.sorted_values, removed)
            if removed_index < len(self.sorted_values):
                self.sorted_values.pop(removed_index)
        insert_at = self._bisect_left(self.sorted_values, numeric)
        self.sorted_values.insert(insert_at, numeric)
        self.values.append(numeric)
        return self.current()

    def current(self) -> Optional[float]:
        count = len(self.sorted_values)
        if count < self.min_periods:
            return None
        if count == 1:
            return float(self.sorted_values[0])
        position = (count - 1) * self.quantile
        lower_index = int(math.floor(position))
        upper_index = int(math.ceil(position))
        lower_value = self.sorted_values[lower_index]
        upper_value = self.sorted_values[upper_index]
        if lower_index == upper_index:
            return float(lower_value)
        ratio = position - float(lower_index)
        return float(lower_value + ((upper_value - lower_value) * ratio))

    @staticmethod
    def _bisect_left(values: Sequence[float], needle: float) -> int:
        low = 0
        high = len(values)
        while low < high:
            middle = (low + high) // 2
            if values[middle] < needle:
                low = middle + 1
            else:
                high = middle
        return low


class EmaTracker:
    def __init__(self, *, span: int) -> None:
        self.alpha = 2.0 / (max(1, int(span)) + 1.0)
        self.value: Optional[float] = None

    def restore(self, value: Any) -> None:
        restored = _safe_float(value)
        self.value = restored if restored is not None and math.isfinite(restored) else None

    def update(self, sample: Optional[float]) -> Optional[float]:
        numeric = _safe_float(sample)
        if numeric is None or not math.isfinite(numeric):
            return self.value
        if self.value is None:
            self.value = numeric
        else:
            self.value = self.value + (self.alpha * (numeric - self.value))
        return self.value


class EwmStdTracker:
    def __init__(self, *, span: int) -> None:
        self.alpha = 2.0 / (max(1, int(span)) + 1.0)
        self.mean: Optional[float] = None
        self.variance: float = 0.0

    def restore(self, *, mean: Any, variance: Any) -> None:
        self.mean = _safe_float(mean)
        restored_variance = _safe_float(variance)
        self.variance = max(0.0, restored_variance or 0.0)

    def update(self, sample: Optional[float]) -> float:
        numeric = _safe_float(sample)
        if numeric is None or not math.isfinite(numeric):
            return self.current()
        if self.mean is None:
            self.mean = numeric
            self.variance = 0.0
            return 0.0
        previous_mean = self.mean
        self.mean = previous_mean + (self.alpha * (numeric - previous_mean))
        self.variance = (1.0 - self.alpha) * (self.variance + (self.alpha * ((numeric - previous_mean) ** 2)))
        return self.current()

    def current(self) -> float:
        return math.sqrt(max(0.0, float(self.variance)))


def normalize_input_batch(
    rows: Sequence[Dict[str, Any]],
    *,
    previous_spread: Optional[float],
    input_kind: str,
) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    raw_spreads: List[Optional[float]] = []
    last_valid = _safe_float(previous_spread)
    for row in rows:
        item = dict(row)
        if input_kind == "backbone_moves":
            item["_pointid"] = int(item.get("endtickid") or 0) or None
            item["_pointtime"] = item.get("endtime")
            item["_midvalue"] = _safe_float(item.get("endprice"))
            spread = _safe_float(item.get("thresholdatconfirm"))
            if spread is None:
                spread = abs(_safe_float(item.get("pricedelta")) or 0.0) or None
        else:
            item["_pointid"] = int(item.get("id") or 0) or None
            item["_pointtime"] = item.get("timestamp")
            item["_midvalue"] = _safe_float(item.get("mid"))
            if item["_midvalue"] is None:
                item["_midvalue"] = _safe_float(tick_mid(item))
            spread = _derive_spread(item)
        raw_spreads.append(spread)
        prepared.append(item)

    first_future_spread: Optional[float] = None
    for spread in raw_spreads:
        numeric = _safe_float(spread)
        if numeric is not None and math.isfinite(numeric):
            first_future_spread = numeric
            break

    fallback_spread = first_future_spread if first_future_spread is not None else (last_valid if last_valid is not None else 0.0)
    fill_value = last_valid if last_valid is not None else fallback_spread

    for index, item in enumerate(prepared):
        numeric = _safe_float(raw_spreads[index])
        if numeric is None or not math.isfinite(numeric):
            numeric = fill_value
        else:
            fill_value = numeric
        item["_spreadvalue"] = float(numeric)

    return prepared


class BackboneEngine:
    def __init__(self, *, symbol: str, source: str, input_kind: str) -> None:
        self.symbol = symbol
        self.source = source
        self.input_kind = input_kind
        self.abs_window = RollingPercentileWindow(window=NOISE_WINDOW, quantile=0.80, min_periods=ABS_DELTA_MIN_PERIODS)
        self.spread_ema = EmaTracker(span=SPREAD_SPAN)
        self.threshold_ema = EmaTracker(span=THRESHOLD_SMOOTH_SPAN)
        self.delta_std = EwmStdTracker(span=NOISE_WINDOW)
        self.reset()

    def reset(self, dayref: Optional[DayRef] = None) -> None:
        self.dayref = dayref
        self.lastprocessedtickid: Optional[int] = None
        self.lastprocessedtime: Optional[datetime] = None
        self.processedtickcount = 0
        self.prevmid: Optional[float] = None
        self.lastvalidspread: Optional[float] = None
        self.currentthreshold: float = THRESHOLD_FLOOR
        self.direction: Optional[str] = None
        self.confirmedpivot: Optional[BackbonePoint] = None
        self.candidateextreme: Optional[BackbonePoint] = None
        self.spread_ema.restore(None)
        self.threshold_ema.restore(None)
        self.delta_std.restore(mean=None, variance=None)
        self.abs_window.restore([])

    def restore(self, *, dayref: DayRef, state_row: Dict[str, Any]) -> None:
        self.reset(dayref)
        statejson = state_row.get("statejson")
        if isinstance(statejson, str):
            try:
                statejson = json.loads(statejson)
            except json.JSONDecodeError:
                statejson = {}
        statejson = statejson if isinstance(statejson, dict) else {}

        self.lastprocessedtickid = int(state_row.get("lastprocessedtickid") or 0) or None
        self.lastprocessedtime = _as_utc(state_row["updatedat"]) if state_row.get("updatedat") else None
        self.prevmid = _safe_float(statejson.get("prevmid"))
        self.lastvalidspread = _safe_float(statejson.get("lastvalidspread"))
        self.processedtickcount = int(statejson.get("processedtickcount") or 0)
        self.currentthreshold = max(THRESHOLD_FLOOR, _safe_float(state_row.get("currentthreshold")) or THRESHOLD_FLOOR)
        self.direction = str(state_row.get("direction") or "").strip() or None
        self.spread_ema.restore(statejson.get("spreadema"))
        self.threshold_ema.restore(statejson.get("thresholdema"))
        self.delta_std.restore(mean=statejson.get("deltaewmmean"), variance=statejson.get("deltaewmvariance"))
        self.abs_window.restore([float(value) for value in statejson.get("absdeltas") or [] if _safe_float(value) is not None])

        confirmed_tickid = int(state_row.get("confirmedpivottickid") or 0)
        confirmed_time = state_row.get("confirmedpivottime")
        confirmed_price = _safe_float(state_row.get("confirmedpivotprice"))
        confirmed_index = int(statejson.get("confirmedpivotindex") or 0)
        if confirmed_tickid and confirmed_time is not None and confirmed_price is not None:
            self.confirmedpivot = BackbonePoint(
                tickid=confirmed_tickid,
                ticktime=_as_utc(confirmed_time),
                price=confirmed_price,
                index=confirmed_index,
            )

        candidate_tickid = int(state_row.get("candidateextremetickid") or 0)
        candidate_time = state_row.get("candidateextremetime")
        candidate_price = _safe_float(state_row.get("candidateextremeprice"))
        candidate_index = int(statejson.get("candidateextremeindex") or 0)
        if candidate_tickid and candidate_time is not None and candidate_price is not None:
            self.candidateextreme = BackbonePoint(
                tickid=candidate_tickid,
                ticktime=_as_utc(candidate_time),
                price=candidate_price,
                index=candidate_index,
            )

    def state_matches_version(self, state_row: Optional[Dict[str, Any]]) -> bool:
        if not state_row:
            return False
        statejson = state_row.get("statejson")
        if isinstance(statejson, str):
            try:
                statejson = json.loads(statejson)
            except json.JSONDecodeError:
                return False
        if not isinstance(statejson, dict):
            return False
        return int(statejson.get("engineVersion") or 0) == BACKBONE_VERSION

    def current_state_row(self) -> Optional[Dict[str, Any]]:
        if self.dayref is None:
            return None
        payload = {
            "dayid": self.dayref.dayid,
            "symbol": self.symbol,
            "source": self.source,
            "lastprocessedtickid": self.lastprocessedtickid,
            "confirmedpivottickid": self.confirmedpivot.tickid if self.confirmedpivot else None,
            "confirmedpivottime": self.confirmedpivot.ticktime if self.confirmedpivot else None,
            "confirmedpivotprice": self.confirmedpivot.price if self.confirmedpivot else None,
            "direction": self.direction,
            "candidateextremetickid": self.candidateextreme.tickid if self.candidateextreme else None,
            "candidateextremetime": self.candidateextreme.ticktime if self.candidateextreme else None,
            "candidateextremeprice": self.candidateextreme.price if self.candidateextreme else None,
            "currentthreshold": self.currentthreshold,
            "updatedat": utc_now(),
            "statejson": psycopg2.extras.Json(
                {
                    "engineVersion": BACKBONE_VERSION,
                    "brokerday": self.dayref.brokerday.isoformat(),
                    "inputKind": self.input_kind,
                    "processedtickcount": self.processedtickcount,
                    "prevmid": self.prevmid,
                    "lastvalidspread": self.lastvalidspread,
                    "spreadema": self.spread_ema.value,
                    "thresholdema": self.threshold_ema.value,
                    "deltaewmmean": self.delta_std.mean,
                    "deltaewmvariance": self.delta_std.variance,
                    "absdeltas": list(self.abs_window.values),
                    "confirmedpivotindex": self.confirmedpivot.index if self.confirmedpivot else None,
                    "candidateextremeindex": self.candidateextreme.index if self.candidateextreme else None,
                }
            ),
        }
        return payload

    def process_rows(self, rows: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        pivots: List[Dict[str, Any]] = []
        moves: List[Dict[str, Any]] = []
        for row in normalize_input_batch(rows, previous_spread=self.lastvalidspread, input_kind=self.input_kind):
            row_pivots, row_moves = self.process_tick(row)
            pivots.extend(row_pivots)
            moves.extend(row_moves)
        return pivots, moves

    def process_tick(self, row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if self.dayref is None:
            timestamp = row.get("timestamp")
            if not isinstance(timestamp, datetime):
                return [], []
            self.dayref = DayRef(
                dayid=_synthetic_dayid(brokerday_for_timestamp(timestamp)),
                brokerday=brokerday_for_timestamp(timestamp),
                starttime=brokerday_bounds(brokerday_for_timestamp(timestamp))[0],
                endtime=brokerday_bounds(brokerday_for_timestamp(timestamp))[1],
            )

        tickid = int(row.get("_pointid") or 0)
        ticktime = row.get("_pointtime")
        if tickid <= 0 or not isinstance(ticktime, datetime):
            return [], []
        ticktime = _as_utc(ticktime)

        self.processedtickcount += 1
        self.lastprocessedtickid = tickid
        self.lastprocessedtime = ticktime

        spread = _safe_float(row.get("_spreadvalue"))
        if spread is not None and math.isfinite(spread):
            self.lastvalidspread = spread
        spread_ema = self.spread_ema.update(self.lastvalidspread if self.lastvalidspread is not None else THRESHOLD_FLOOR)

        mid = _safe_float(row.get("_midvalue"))
        delta: Optional[float] = None
        if mid is not None and self.prevmid is not None:
            delta = float(mid - self.prevmid)
            self.abs_window.add(abs(delta))
        abs_delta_q80 = self.abs_window.current() or 0.0
        delta_std = self.delta_std.update(delta)
        threshold_raw = max(
            float(spread_ema or 0.0) * SPREAD_WEIGHT,
            float(abs_delta_q80) * DELTA_Q80_WEIGHT,
            float(delta_std) * DELTA_STD_WEIGHT,
            THRESHOLD_FLOOR,
        )
        adaptive_threshold = self.threshold_ema.update(threshold_raw)
        self.currentthreshold = float(adaptive_threshold or threshold_raw or THRESHOLD_FLOOR)

        if mid is None or not math.isfinite(mid):
            return [], []

        point = BackbonePoint(index=self.processedtickcount, price=float(mid), tickid=tickid, ticktime=ticktime)
        emitted_pivots: List[Dict[str, Any]] = []
        emitted_moves: List[Dict[str, Any]] = []

        if self.confirmedpivot is None:
            self.confirmedpivot = point
            emitted_pivots.append(self._pivot_row(point, pivottype="Start", threshold=self.currentthreshold))
            self.prevmid = point.price
            return emitted_pivots, emitted_moves

        if self.direction is None:
            if point.price >= self.confirmedpivot.price + self.currentthreshold:
                self.direction = "Up"
                self.candidateextreme = point
            elif point.price <= self.confirmedpivot.price - self.currentthreshold:
                self.direction = "Down"
                self.candidateextreme = point
        elif self.direction == "Up":
            if self.candidateextreme is None or point.price >= self.candidateextreme.price:
                self.candidateextreme = point
            elif self.candidateextreme.price - point.price >= self.currentthreshold:
                confirmed = self.candidateextreme
                emitted_pivots.append(self._pivot_row(confirmed, pivottype="High", threshold=self.currentthreshold))
                emitted_moves.append(self._move_row(self.confirmedpivot, confirmed, direction="Up", threshold=self.currentthreshold))
                self.confirmedpivot = confirmed
                self.direction = "Down"
                self.candidateextreme = point
        else:
            if self.candidateextreme is None or point.price <= self.candidateextreme.price:
                self.candidateextreme = point
            elif point.price - self.candidateextreme.price >= self.currentthreshold:
                confirmed = self.candidateextreme
                emitted_pivots.append(self._pivot_row(confirmed, pivottype="Low", threshold=self.currentthreshold))
                emitted_moves.append(self._move_row(self.confirmedpivot, confirmed, direction="Down", threshold=self.currentthreshold))
                self.confirmedpivot = confirmed
                self.direction = "Up"
                self.candidateextreme = point

        self.prevmid = point.price
        return emitted_pivots, emitted_moves

    def _pivot_row(self, point: BackbonePoint, *, pivottype: str, threshold: float) -> Dict[str, Any]:
        return {
            "dayid": self.dayref.dayid if self.dayref else None,
            "tickid": point.tickid,
            "ticktime": point.ticktime,
            "price": point.price,
            "pivottype": pivottype,
            "threshold": float(threshold),
            "source": self.source,
        }

    def _move_row(self, start: BackbonePoint, end: BackbonePoint, *, direction: str, threshold: float) -> Dict[str, Any]:
        return {
            "dayid": self.dayref.dayid if self.dayref else None,
            "starttickid": start.tickid,
            "endtickid": end.tickid,
            "starttime": start.ticktime,
            "endtime": end.ticktime,
            "startprice": start.price,
            "endprice": end.price,
            "direction": direction,
            "pricedelta": float(end.price - start.price),
            "tickcount": max(1, int(end.index - start.index + 1)),
            "thresholdatconfirm": float(threshold),
            "source": self.source,
        }


def insert_pivots(conn: Any, pivots: Sequence[Dict[str, Any]]) -> None:
    if not pivots:
        return
    with conn.cursor() as cur:
        for pivot in pivots:
            cur.execute(
                """
                INSERT INTO public.backbonepivots (
                    dayid, tickid, ticktime, price, pivottype, threshold, source
                ) VALUES (
                    %(dayid)s, %(tickid)s, %(ticktime)s, %(price)s, %(pivottype)s, %(threshold)s, %(source)s
                )
                ON CONFLICT (dayid, tickid, pivottype, source)
                DO UPDATE SET
                    ticktime = EXCLUDED.ticktime,
                    price = EXCLUDED.price,
                    threshold = EXCLUDED.threshold
                """,
                pivot,
            )


def insert_moves(conn: Any, moves: Sequence[Dict[str, Any]]) -> None:
    if not moves:
        return
    with conn.cursor() as cur:
        for move in moves:
            cur.execute(
                """
                INSERT INTO public.backbonemoves (
                    dayid, starttickid, endtickid, starttime, endtime,
                    startprice, endprice, direction, pricedelta, tickcount,
                    thresholdatconfirm, source
                ) VALUES (
                    %(dayid)s, %(starttickid)s, %(endtickid)s, %(starttime)s, %(endtime)s,
                    %(startprice)s, %(endprice)s, %(direction)s, %(pricedelta)s, %(tickcount)s,
                    %(thresholdatconfirm)s, %(source)s
                )
                ON CONFLICT (dayid, starttickid, endtickid, direction, source)
                DO UPDATE SET
                    starttime = EXCLUDED.starttime,
                    endtime = EXCLUDED.endtime,
                    startprice = EXCLUDED.startprice,
                    endprice = EXCLUDED.endprice,
                    pricedelta = EXCLUDED.pricedelta,
                    tickcount = EXCLUDED.tickcount,
                    thresholdatconfirm = EXCLUDED.thresholdatconfirm
                """,
                move,
            )


def upsert_state(conn: Any, state_row: Optional[Dict[str, Any]]) -> None:
    if state_row is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
                INSERT INTO public.backbonestate (
                dayid, symbol, source, lastprocessedtickid,
                confirmedpivottickid, confirmedpivottime, confirmedpivotprice,
                direction, candidateextremetickid, candidateextremetime,
                candidateextremeprice, currentthreshold, statejson, updatedat
            ) VALUES (
                %(dayid)s, %(symbol)s, %(source)s, %(lastprocessedtickid)s,
                %(confirmedpivottickid)s, %(confirmedpivottime)s, %(confirmedpivotprice)s,
                %(direction)s, %(candidateextremetickid)s, %(candidateextremetime)s,
                %(candidateextremeprice)s, %(currentthreshold)s, %(statejson)s, %(updatedat)s
            )
            ON CONFLICT (dayid, symbol, source)
            DO UPDATE SET
                lastprocessedtickid = EXCLUDED.lastprocessedtickid,
                confirmedpivottickid = EXCLUDED.confirmedpivottickid,
                confirmedpivottime = EXCLUDED.confirmedpivottime,
                confirmedpivotprice = EXCLUDED.confirmedpivotprice,
                direction = EXCLUDED.direction,
                candidateextremetickid = EXCLUDED.candidateextremetickid,
                candidateextremetime = EXCLUDED.candidateextremetime,
                candidateextremeprice = EXCLUDED.candidateextremeprice,
                currentthreshold = EXCLUDED.currentthreshold,
                statejson = EXCLUDED.statejson,
                updatedat = EXCLUDED.updatedat
            """,
            state_row,
        )


class BackboneLiveRuntime:
    def __init__(self, *, symbol: str, batch_size: int = 400) -> None:
        self.symbol = symbol
        self.batch_size = max(1, int(batch_size))
        self.backbone_engine = BackboneEngine(symbol=symbol, source=BACKBONE_SOURCE, input_kind="ticks")
        self.bigbones_engine = BackboneEngine(symbol=symbol, source=BIGBONES_SOURCE, input_kind="backbone_moves")

    def bootstrap(self, conn: Any) -> Dict[str, Any]:
        dayref = resolve_current_day_ref(conn, symbol=self.symbol)
        if dayref is None:
            self.backbone_engine.reset()
            self.bigbones_engine.reset()
            return {
                "dayid": None,
                "brokerday": None,
                "tickcount": 0,
                "pivotcount": 0,
                "movecount": 0,
                "bigbonepivotcount": 0,
                "bigbonemovecount": 0,
            }

        latest_tick = fetch_day_latest_tick(conn, symbol=self.symbol, dayref=dayref)
        latest_backbone_move = fetch_day_latest_move(conn, dayref=dayref, source=BACKBONE_SOURCE)
        state_row = load_state_row(conn, symbol=self.symbol, dayid=dayref.dayid, source=BACKBONE_SOURCE)
        bigbones_state_row = load_state_row(conn, symbol=self.symbol, dayid=dayref.dayid, source=BIGBONES_SOURCE)
        backbone_current = bool(
            latest_tick
            and state_row
            and self.backbone_engine.state_matches_version(state_row)
            and int(state_row.get("lastprocessedtickid") or 0) == int(latest_tick.get("id") or 0)
        )
        bigbones_current = False
        if latest_backbone_move is None:
            bigbones_current = bigbones_state_row is None or (
                self.bigbones_engine.state_matches_version(bigbones_state_row)
                and int(bigbones_state_row.get("lastprocessedtickid") or 0) <= 0
            )
        else:
            bigbones_current = bool(
                bigbones_state_row
                and self.bigbones_engine.state_matches_version(bigbones_state_row)
                and int(bigbones_state_row.get("lastprocessedtickid") or 0) == int(latest_backbone_move.get("endtickid") or 0)
            )
        if (
            backbone_current
            and bigbones_current
        ):
            self.backbone_engine.restore(dayref=dayref, state_row=state_row)
            if bigbones_state_row:
                self.bigbones_engine.restore(dayref=dayref, state_row=bigbones_state_row)
            else:
                self.bigbones_engine.reset(dayref)
            return {
                "dayid": dayref.dayid,
                "brokerday": dayref.brokerday,
                "tickcount": 0,
                "pivotcount": 0,
                "movecount": 0,
                "bigbonepivotcount": 0,
                "bigbonemovecount": 0,
            }

        delete_day(conn, dayid=dayref.dayid, symbol=self.symbol, source=BACKBONE_SOURCE)
        delete_day(conn, dayid=dayref.dayid, symbol=self.symbol, source=BIGBONES_SOURCE)
        self.backbone_engine.reset(dayref)
        self.bigbones_engine.reset(dayref)

        tickcount = 0
        pivotcount = 0
        movecount = 0
        bigbonepivotcount = 0
        bigbonemovecount = 0
        for batch in iter_ticks_for_day(conn, symbol=self.symbol, dayref=dayref, batch_size=self.batch_size):
            tickcount += len(batch)
            pivots, moves = self.backbone_engine.process_rows(batch)
            bigbone_pivots, bigbone_moves = self.bigbones_engine.process_rows(moves)
            pivotcount += len(pivots)
            movecount += len(moves)
            bigbonepivotcount += len(bigbone_pivots)
            bigbonemovecount += len(bigbone_moves)
            insert_pivots(conn, pivots)
            insert_moves(conn, moves)
            insert_pivots(conn, bigbone_pivots)
            insert_moves(conn, bigbone_moves)
            upsert_state(conn, self.backbone_engine.current_state_row())
            upsert_state(conn, self.bigbones_engine.current_state_row())

        if tickcount <= 0:
            upsert_state(conn, self.backbone_engine.current_state_row())
            upsert_state(conn, self.bigbones_engine.current_state_row())

        return {
            "dayid": dayref.dayid,
            "brokerday": dayref.brokerday,
            "tickcount": tickcount,
            "pivotcount": pivotcount,
            "movecount": movecount,
            "bigbonepivotcount": bigbonepivotcount,
            "bigbonemovecount": bigbonemovecount,
        }

    def process_once(self, conn: Any) -> Dict[str, Any]:
        dayref = resolve_current_day_ref(conn, symbol=self.symbol)
        if dayref is None:
            return {
                "dayid": None,
                "brokerday": None,
                "tickcount": 0,
                "pivotcount": 0,
                "movecount": 0,
                "bigbonepivotcount": 0,
                "bigbonemovecount": 0,
            }

        if (
            self.backbone_engine.dayref is None
            or self.bigbones_engine.dayref is None
            or self.backbone_engine.dayref.dayid != dayref.dayid
            or self.bigbones_engine.dayref.dayid != dayref.dayid
        ):
            return self.bootstrap(conn)

        after_id = int(self.backbone_engine.lastprocessedtickid or 0)
        rows = fetch_ticks_after_for_day(conn, symbol=self.symbol, dayref=dayref, after_id=after_id, limit=self.batch_size)
        if not rows:
            upsert_state(conn, self.backbone_engine.current_state_row())
            upsert_state(conn, self.bigbones_engine.current_state_row())
            return {
                "dayid": dayref.dayid,
                "brokerday": dayref.brokerday,
                "tickcount": 0,
                "pivotcount": 0,
                "movecount": 0,
                "bigbonepivotcount": 0,
                "bigbonemovecount": 0,
            }

        pivots, moves = self.backbone_engine.process_rows(rows)
        bigbone_pivots, bigbone_moves = self.bigbones_engine.process_rows(moves)
        insert_pivots(conn, pivots)
        insert_moves(conn, moves)
        insert_pivots(conn, bigbone_pivots)
        insert_moves(conn, bigbone_moves)
        upsert_state(conn, self.backbone_engine.current_state_row())
        upsert_state(conn, self.bigbones_engine.current_state_row())
        return {
            "dayid": dayref.dayid,
            "brokerday": dayref.brokerday,
            "tickcount": len(rows),
            "pivotcount": len(pivots),
            "movecount": len(moves),
            "bigbonepivotcount": len(bigbone_pivots),
            "bigbonemovecount": len(bigbone_moves),
        }


def rebuild_current_day(conn: Any, *, symbol: str, batch_size: int = 400) -> Dict[str, Any]:
    runtime = BackboneLiveRuntime(symbol=symbol, batch_size=batch_size)
    return runtime.bootstrap(conn)


def reset_current_day(conn: Any, *, symbol: str) -> Dict[str, Any]:
    dayref = resolve_current_day_ref(conn, symbol=symbol)
    if dayref is None:
        return {"dayid": None, "brokerday": None, "deleted": False}
    delete_day(conn, dayid=dayref.dayid, symbol=symbol)
    return {"dayid": dayref.dayid, "brokerday": dayref.brokerday, "deleted": True}


def _print(message: str) -> None:
    print(message, flush=True)


def build_jobs_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backbone operational jobs.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Symbol to process.")
    parser.add_argument("--batch-size", type=int, default=400, help="Tick batch size for rebuilds.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("rebuild-current-day", help="Rebuild the current broker day backbone rows.")
    subparsers.add_parser("reset-current-day", help="Delete the current broker day backbone rows and state.")
    return parser


def jobs_main() -> int:
    args = build_jobs_parser().parse_args()
    symbol = str(args.symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL
    with db_connection(readonly=False, autocommit=False) as conn:
        if args.command == "rebuild-current-day":
            result = rebuild_current_day(conn, symbol=symbol, batch_size=max(1, int(args.batch_size)))
            conn.commit()
            _print(
                "brokerday={0} dayid={1} ticks={2} pivots={3} moves={4} bigbone_pivots={5} bigbone_moves={6}".format(
                    result["brokerday"].isoformat() if result.get("brokerday") else "-",
                    result.get("dayid"),
                    result.get("tickcount"),
                    result.get("pivotcount"),
                    result.get("movecount"),
                    result.get("bigbonepivotcount"),
                    result.get("bigbonemovecount"),
                )
            )
            return 0
        if args.command == "reset-current-day":
            result = reset_current_day(conn, symbol=symbol)
            conn.commit()
            _print(
                "brokerday={0} dayid={1} deleted={2}".format(
                    result["brokerday"].isoformat() if result.get("brokerday") else "-",
                    result.get("dayid"),
                    result.get("deleted"),
                )
            )
            return 0
    return 1
