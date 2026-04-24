from __future__ import annotations

import argparse
import csv
import math
import os
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Generator, Iterable, List, Optional, Sequence

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from datavis.backbone import describe_days_table
from datavis.brokerday import BROKER_TIMEZONE, brokerday_bounds, brokerday_for_timestamp, tick_mid
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DEFAULT_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD").strip().upper() or "XAUUSD"
DEFAULT_BATCH_SIZE = 1000
EXPORT_BATCH_SIZE = 5000
MOTION_WINDOWS = (3, 10, 30)
DEFAULT_SIGNAL_RULE = "motion_v1_basic_acceleration"
MICRO_BURST_SIGNAL_RULE = "motion_v2_micro_burst"
LOOKAHEAD_SECONDS = 300
RISKFREE_DISTANCE = 0.30
TARGET_DISTANCE = 1.00
STOP_DISTANCE = 1.00
SIGNAL_ACCEL_EPSILON = 0.01
MAX_REASONABLE_SPREAD = 0.50
MAX_WINDOW_SECONDS = max(MOTION_WINDOWS)
SIGNAL_RULE_COOLDOWN_SECONDS = {
    DEFAULT_SIGNAL_RULE: 10,
    MICRO_BURST_SIGNAL_RULE: 20,
}


@dataclass(frozen=True)
class TickSample:
    tickid: int
    timestamp: datetime
    bid: Optional[float]
    ask: Optional[float]
    mid: Optional[float]
    spread: Optional[float]
    cumulative_abs_move: float


@dataclass(frozen=True)
class MotionSeed:
    timestamp: datetime
    velocity: Optional[float]
    acceleration: Optional[float]


@dataclass(frozen=True)
class DayRef:
    dayid: int
    brokerday: date
    starttime: datetime
    endtime: datetime


@dataclass
class BackfillRange:
    start: datetime
    end: datetime
    brokerdays: List[date]


@dataclass
class PendingSignal:
    tickid: int
    timestamp: datetime
    side: str
    mid: float
    bid: Optional[float]
    ask: Optional[float]
    spread: Optional[float]
    velocity3: Optional[float]
    acceleration3: Optional[float]
    efficiency3: Optional[float]
    spreadmultiple3: Optional[float]
    state3: Optional[str]
    velocity10: Optional[float]
    acceleration10: Optional[float]
    efficiency10: Optional[float]
    spreadmultiple10: Optional[float]
    state10: Optional[str]
    velocity30: Optional[float]
    acceleration30: Optional[float]
    efficiency30: Optional[float]
    spreadmultiple30: Optional[float]
    state30: Optional[str]
    riskfreeprice: float
    stopprice: float
    targetprice: float
    lookaheadsec: int
    signalrule: str
    maxfavourable: float = 0.0
    maxadverse: float = 0.0
    seconds_to_riskfree: Optional[float] = None
    seconds_to_target: Optional[float] = None
    seconds_to_stop: Optional[float] = None
    expire_at: datetime = field(init=False)

    def __post_init__(self) -> None:
        self.expire_at = self.timestamp + timedelta(seconds=self.lookaheadsec)

    def update(self, row: Dict[str, Any]) -> None:
        ticktime = _as_utc(row.get("timestamp"))
        if ticktime is None or ticktime < self.timestamp or ticktime > self.expire_at:
            return

        if self.side == "buy":
            close_price = _safe_float(row.get("bid"))
            if close_price is None:
                return
            favourable = max(0.0, close_price - self.ask_price)
            adverse = max(0.0, self.ask_price - close_price)
            elapsed = (ticktime - self.timestamp).total_seconds()
            self.maxfavourable = max(self.maxfavourable, favourable)
            self.maxadverse = max(self.maxadverse, adverse)
            if self.seconds_to_riskfree is None and close_price >= self.riskfreeprice:
                self.seconds_to_riskfree = elapsed
            if self.seconds_to_target is None and close_price >= self.targetprice:
                self.seconds_to_target = elapsed
            if self.seconds_to_stop is None and close_price <= self.stopprice:
                self.seconds_to_stop = elapsed
            return

        close_price = _safe_float(row.get("ask"))
        if close_price is None:
            return
        favourable = max(0.0, self.bid_price - close_price)
        adverse = max(0.0, close_price - self.bid_price)
        elapsed = (ticktime - self.timestamp).total_seconds()
        self.maxfavourable = max(self.maxfavourable, favourable)
        self.maxadverse = max(self.maxadverse, adverse)
        if self.seconds_to_riskfree is None and close_price <= self.riskfreeprice:
            self.seconds_to_riskfree = elapsed
        if self.seconds_to_target is None and close_price <= self.targetprice:
            self.seconds_to_target = elapsed
        if self.seconds_to_stop is None and close_price >= self.stopprice:
            self.seconds_to_stop = elapsed

    @property
    def ask_price(self) -> float:
        return float(self.ask or 0.0)

    @property
    def bid_price(self) -> float:
        return float(self.bid or 0.0)

    def finalize_row(self) -> Dict[str, Any]:
        outcome = classify_signal_outcome(
            seconds_to_riskfree=self.seconds_to_riskfree,
            seconds_to_target=self.seconds_to_target,
            seconds_to_stop=self.seconds_to_stop,
        )
        score = score_signal(
            outcome=outcome,
            seconds_to_riskfree=self.seconds_to_riskfree,
            maxadverse=self.maxadverse,
        )
        return {
            "tickid": self.tickid,
            "timestamp": self.timestamp,
            "side": self.side,
            "mid": self.mid,
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.spread,
            "velocity3": self.velocity3,
            "acceleration3": self.acceleration3,
            "efficiency3": self.efficiency3,
            "spreadmultiple3": self.spreadmultiple3,
            "state3": self.state3,
            "velocity10": self.velocity10,
            "acceleration10": self.acceleration10,
            "efficiency10": self.efficiency10,
            "spreadmultiple10": self.spreadmultiple10,
            "state10": self.state10,
            "velocity30": self.velocity30,
            "acceleration30": self.acceleration30,
            "efficiency30": self.efficiency30,
            "spreadmultiple30": self.spreadmultiple30,
            "state30": self.state30,
            "riskfreeprice": self.riskfreeprice,
            "stopprice": self.stopprice,
            "targetprice": self.targetprice,
            "lookaheadsec": self.lookaheadsec,
            "maxfavourable": self.maxfavourable,
            "maxadverse": self.maxadverse,
            "seconds_to_riskfree": self.seconds_to_riskfree,
            "seconds_to_target": self.seconds_to_target,
            "seconds_to_stop": self.seconds_to_stop,
            "outcome": outcome,
            "score": score,
            "signalrule": self.signalrule,
        }


class TickHistory:
    def __init__(self, *, windows: Sequence[int]) -> None:
        self.windows = tuple(sorted({max(1, int(window)) for window in windows}))
        self.rows: Deque[TickSample] = deque()
        self.offsets: Dict[int, int] = {window: 0 for window in self.windows}
        self.cumulative_abs_move = 0.0
        self.last_mid: Optional[float] = None

    def append(self, *, tickid: int, timestamp: datetime, bid: Optional[float], ask: Optional[float], mid: float, spread: Optional[float]) -> TickSample:
        if self.last_mid is not None:
            self.cumulative_abs_move += abs(float(mid) - float(self.last_mid))
        self.last_mid = float(mid)
        sample = TickSample(
            tickid=int(tickid),
            timestamp=timestamp,
            bid=bid,
            ask=ask,
            mid=float(mid),
            spread=spread,
            cumulative_abs_move=self.cumulative_abs_move,
        )
        self.rows.append(sample)
        return sample

    def past_for(self, *, windowsec: int, current_time: datetime) -> Optional[TickSample]:
        if not self.rows:
            return None
        target_time = current_time - timedelta(seconds=max(1, int(windowsec)))
        offset = int(self.offsets.get(windowsec, 0))
        while offset + 1 < len(self.rows) and self.rows[offset + 1].timestamp <= target_time:
            offset += 1
        self.offsets[windowsec] = offset
        candidate = self.rows[offset]
        if candidate.timestamp <= target_time:
            return candidate
        return None

    def trim(self) -> None:
        if not self.rows:
            return
        removable = min(self.offsets.values(), default=0)
        while removable > 0:
            self.rows.popleft()
            for window in self.offsets:
                self.offsets[window] = max(0, self.offsets[window] - 1)
            removable -= 1


def database_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    raise RuntimeError(
        "No DATABASE_URL or DATAVIS_DB_URL was available. Set DATABASE_URL or DATAVIS_DB_URL before running datavis.motion_trade_spots."
    )


def db_connect(*, readonly: bool = False, autocommit: bool = False) -> Any:
    url = database_url()
    conn = psycopg2.connect(url)
    conn.autocommit = autocommit
    if readonly:
        conn.set_session(readonly=True, autocommit=autocommit)
    return conn


@contextmanager
def db_connection(*, readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
    conn = db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def _print(message: str) -> None:
    print(message, flush=True)


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _derive_spread(row: Dict[str, Any]) -> Optional[float]:
    spread = _safe_float(row.get("spread"))
    if spread is not None:
        return spread
    bid = _safe_float(row.get("bid"))
    ask = _safe_float(row.get("ask"))
    if bid is None or ask is None:
        return None
    return float(ask - bid)


def _normalize_tick_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = _as_utc(row.get("timestamp"))
    bid = _safe_float(row.get("bid"))
    ask = _safe_float(row.get("ask"))
    mid = _safe_float(tick_mid(row))
    spread = _derive_spread(row)
    return {
        "id": int(row.get("id") or 0),
        "timestamp": timestamp,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
    }


def _csv_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def latest_tick(conn: Any, *, symbol: str) -> Optional[Dict[str, Any]]:
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


def iter_ticks_between(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
) -> Iterable[List[Dict[str, Any]]]:
    with conn.cursor(name="motion_ticks_between", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = max(1, int(batch_size))
        cur.execute(
            """
            SELECT id, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, start_ts, end_ts),
        )
        while True:
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                return
            yield [dict(row) for row in rows]


def iter_ticks_with_motionpoints_between(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
) -> Iterable[List[Dict[str, Any]]]:
    with conn.cursor(name="motion_ticks_points_between", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = max(1, int(batch_size))
        cur.execute(
            """
            SELECT
                t.id,
                t.timestamp,
                t.bid,
                t.ask,
                t.mid,
                t.spread,
                mp3.velocity AS velocity3,
                mp3.acceleration AS acceleration3,
                mp3.efficiency AS efficiency3,
                mp3.spreadmultiple AS spreadmultiple3,
                mp3.motionstate AS state3,
                mp10.velocity AS velocity10,
                mp10.acceleration AS acceleration10,
                mp10.efficiency AS efficiency10,
                mp10.spreadmultiple AS spreadmultiple10,
                mp10.motionstate AS state10,
                mp30.velocity AS velocity30,
                mp30.acceleration AS acceleration30,
                mp30.efficiency AS efficiency30,
                mp30.spreadmultiple AS spreadmultiple30,
                mp30.motionstate AS state30
            FROM public.ticks t
            LEFT JOIN public.motionpoint mp3
                ON mp3.tickid = t.id
               AND mp3.windowsec = 3
            LEFT JOIN public.motionpoint mp10
                ON mp10.tickid = t.id
               AND mp10.windowsec = 10
            LEFT JOIN public.motionpoint mp30
                ON mp30.tickid = t.id
               AND mp30.windowsec = 30
            WHERE t.symbol = %s
              AND t.timestamp >= %s
              AND t.timestamp <= %s
            ORDER BY t.timestamp ASC, t.id ASC
            """,
            (symbol, start_ts, end_ts),
        )
        while True:
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                return
            yield [dict(row) for row in rows]


def load_seed_tick(conn: Any, *, symbol: str, before_ts: datetime) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp, bid, ask, mid, spread
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp < %s
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """,
            (symbol, before_ts),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def load_seed_motion_states(conn: Any, *, before_ts: datetime) -> Dict[int, MotionSeed]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (windowsec) windowsec, timestamp, velocity, acceleration
            FROM public.motionpoint
            WHERE windowsec = ANY(%s)
              AND timestamp < %s
            ORDER BY windowsec ASC, timestamp DESC, id DESC
            """,
            (list(MOTION_WINDOWS), before_ts),
        )
        rows = [dict(row) for row in cur.fetchall()]
    result: Dict[int, MotionSeed] = {}
    for row in rows:
        timestamp = _as_utc(row.get("timestamp"))
        if timestamp is None:
            continue
        result[int(row["windowsec"])] = MotionSeed(
            timestamp=timestamp,
            velocity=_safe_float(row.get("velocity")),
            acceleration=_safe_float(row.get("acceleration")),
        )
    return result


def load_signal_cooldowns(conn: Any, *, before_ts: datetime, signalrule: str) -> Dict[str, datetime]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (side) side, timestamp
            FROM public.motionsignal
            WHERE signalrule = %s
              AND timestamp < %s
            ORDER BY side ASC, timestamp DESC, id DESC
            """,
            (signalrule, before_ts),
        )
        rows = [dict(row) for row in cur.fetchall()]
    result: Dict[str, datetime] = {}
    for row in rows:
        timestamp = _as_utc(row.get("timestamp"))
        if timestamp is not None:
            result[str(row.get("side") or "").strip().lower()] = timestamp
    return result


def delete_signal_range(conn: Any, *, start_ts: datetime, end_ts: datetime, signalrule: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.motionsignal
            WHERE timestamp >= %s
              AND timestamp <= %s
              AND signalrule = %s
            """,
            (start_ts, end_ts, signalrule),
        )


def delete_motionpoint_range(conn: Any, *, start_ts: datetime, end_ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.motionpoint
            WHERE timestamp >= %s
              AND timestamp <= %s
              AND windowsec = ANY(%s)
            """,
            (start_ts, end_ts, list(MOTION_WINDOWS)),
        )


def delete_backfill_range(conn: Any, *, start_ts: datetime, end_ts: datetime, signalrule: str) -> None:
    delete_signal_range(conn, start_ts=start_ts, end_ts=end_ts, signalrule=signalrule)
    delete_motionpoint_range(conn, start_ts=start_ts, end_ts=end_ts)


def insert_motionpoints(conn: Any, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.motionpoint (
                tickid,
                timestamp,
                windowsec,
                mid,
                bid,
                ask,
                spread,
                pasttickid,
                pasttimestamp,
                pastmid,
                elapsedsec,
                pricechange,
                velocity,
                prevvelocity,
                acceleration,
                prevacceleration,
                jerk,
                totalmove,
                efficiency,
                spreadmultiple,
                direction,
                motionstate
            ) VALUES %s
            ON CONFLICT (tickid, windowsec)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                mid = EXCLUDED.mid,
                bid = EXCLUDED.bid,
                ask = EXCLUDED.ask,
                spread = EXCLUDED.spread,
                pasttickid = EXCLUDED.pasttickid,
                pasttimestamp = EXCLUDED.pasttimestamp,
                pastmid = EXCLUDED.pastmid,
                elapsedsec = EXCLUDED.elapsedsec,
                pricechange = EXCLUDED.pricechange,
                velocity = EXCLUDED.velocity,
                prevvelocity = EXCLUDED.prevvelocity,
                acceleration = EXCLUDED.acceleration,
                prevacceleration = EXCLUDED.prevacceleration,
                jerk = EXCLUDED.jerk,
                totalmove = EXCLUDED.totalmove,
                efficiency = EXCLUDED.efficiency,
                spreadmultiple = EXCLUDED.spreadmultiple,
                direction = EXCLUDED.direction,
                motionstate = EXCLUDED.motionstate
            """,
            [(
                row["tickid"],
                row["timestamp"],
                row["windowsec"],
                row["mid"],
                row["bid"],
                row["ask"],
                row["spread"],
                row["pasttickid"],
                row["pasttimestamp"],
                row["pastmid"],
                row["elapsedsec"],
                row["pricechange"],
                row["velocity"],
                row["prevvelocity"],
                row["acceleration"],
                row["prevacceleration"],
                row["jerk"],
                row["totalmove"],
                row["efficiency"],
                row["spreadmultiple"],
                row["direction"],
                row["motionstate"],
            ) for row in rows],
            page_size=min(max(1, len(rows)), 1000),
        )


def insert_signals(conn: Any, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.motionsignal (
                tickid,
                timestamp,
                side,
                mid,
                bid,
                ask,
                spread,
                velocity3,
                acceleration3,
                efficiency3,
                spreadmultiple3,
                state3,
                velocity10,
                acceleration10,
                efficiency10,
                spreadmultiple10,
                state10,
                velocity30,
                acceleration30,
                efficiency30,
                spreadmultiple30,
                state30,
                riskfreeprice,
                stopprice,
                targetprice,
                lookaheadsec,
                maxfavourable,
                maxadverse,
                seconds_to_riskfree,
                seconds_to_target,
                seconds_to_stop,
                outcome,
                score,
                signalrule
            ) VALUES %s
            ON CONFLICT (tickid, side, signalrule)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                mid = EXCLUDED.mid,
                bid = EXCLUDED.bid,
                ask = EXCLUDED.ask,
                spread = EXCLUDED.spread,
                velocity3 = EXCLUDED.velocity3,
                acceleration3 = EXCLUDED.acceleration3,
                efficiency3 = EXCLUDED.efficiency3,
                spreadmultiple3 = EXCLUDED.spreadmultiple3,
                state3 = EXCLUDED.state3,
                velocity10 = EXCLUDED.velocity10,
                acceleration10 = EXCLUDED.acceleration10,
                efficiency10 = EXCLUDED.efficiency10,
                spreadmultiple10 = EXCLUDED.spreadmultiple10,
                state10 = EXCLUDED.state10,
                velocity30 = EXCLUDED.velocity30,
                acceleration30 = EXCLUDED.acceleration30,
                efficiency30 = EXCLUDED.efficiency30,
                spreadmultiple30 = EXCLUDED.spreadmultiple30,
                state30 = EXCLUDED.state30,
                riskfreeprice = EXCLUDED.riskfreeprice,
                stopprice = EXCLUDED.stopprice,
                targetprice = EXCLUDED.targetprice,
                lookaheadsec = EXCLUDED.lookaheadsec,
                maxfavourable = EXCLUDED.maxfavourable,
                maxadverse = EXCLUDED.maxadverse,
                seconds_to_riskfree = EXCLUDED.seconds_to_riskfree,
                seconds_to_target = EXCLUDED.seconds_to_target,
                seconds_to_stop = EXCLUDED.seconds_to_stop,
                outcome = EXCLUDED.outcome,
                score = EXCLUDED.score
            """,
            [(
                row["tickid"],
                row["timestamp"],
                row["side"],
                row["mid"],
                row["bid"],
                row["ask"],
                row["spread"],
                row["velocity3"],
                row["acceleration3"],
                row["efficiency3"],
                row["spreadmultiple3"],
                row["state3"],
                row["velocity10"],
                row["acceleration10"],
                row["efficiency10"],
                row["spreadmultiple10"],
                row["state10"],
                row["velocity30"],
                row["acceleration30"],
                row["efficiency30"],
                row["spreadmultiple30"],
                row["state30"],
                row["riskfreeprice"],
                row["stopprice"],
                row["targetprice"],
                row["lookaheadsec"],
                row["maxfavourable"],
                row["maxadverse"],
                row["seconds_to_riskfree"],
                row["seconds_to_target"],
                row["seconds_to_stop"],
                row["outcome"],
                row["score"],
                row["signalrule"],
            ) for row in rows],
            page_size=min(max(1, len(rows)), 500),
        )


def update_motionstate(conn: Any, *, lasttickid: Optional[int]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.motionstate (id, lasttickid, updatedat)
            VALUES (1, %s, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                lasttickid = EXCLUDED.lasttickid,
                updatedat = EXCLUDED.updatedat
            """,
            (lasttickid,),
        )


def _synthetic_dayid(brokerday: date) -> int:
    return int(brokerday.strftime("%Y%m%d"))


def _dayref_from_row(row: Dict[str, Any], *, descriptor: Any) -> Optional[DayRef]:
    day_value: Optional[date] = None
    start_value: Optional[datetime] = None
    end_value: Optional[datetime] = None
    if getattr(descriptor, "datecol", None):
        day_value = row.get(descriptor.datecol)
    if getattr(descriptor, "startcol", None):
        start_value = _as_utc(row.get(descriptor.startcol))
    if getattr(descriptor, "endcol", None):
        end_value = _as_utc(row.get(descriptor.endcol))
    if day_value is None:
        source_time = start_value or end_value
        if source_time is not None:
            day_value = brokerday_for_timestamp(source_time)
    if day_value is None:
        return None
    if start_value is None or end_value is None:
        start_value, end_value = brokerday_bounds(day_value)
    return DayRef(
        dayid=int(row.get("id") or _synthetic_dayid(day_value)),
        brokerday=day_value,
        starttime=start_value,
        endtime=end_value,
    )


def recent_day_refs(conn: Any, *, symbol: str, last_broker_days: int, anchor_ts: datetime) -> List[DayRef]:
    count = max(1, int(last_broker_days))
    descriptor = describe_days_table(conn)
    if descriptor is not None:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            rows: List[Dict[str, Any]] = []
            if getattr(descriptor, "startcol", None) and getattr(descriptor, "endcol", None):
                where = ["{0} <= %s".format(descriptor.startcol)]
                params: List[Any] = [anchor_ts]
                if getattr(descriptor, "symbolcol", None):
                    where.append("{0} = %s".format(descriptor.symbolcol))
                    params.append(symbol)
                cur.execute(
                    """
                    SELECT id, {datecol}, {startcol}, {endcol}
                    FROM public.days
                    WHERE {where_sql}
                    ORDER BY {startcol} DESC, id DESC
                    LIMIT %s
                    """.format(
                        datecol=getattr(descriptor, "datecol", None) or "NULL AS brokerday",
                        startcol=descriptor.startcol,
                        endcol=descriptor.endcol,
                        where_sql=" AND ".join(where),
                    ),
                    tuple(params + [count]),
                )
                rows = [dict(row) for row in cur.fetchall()]
            elif getattr(descriptor, "datecol", None):
                where = ["{0} <= %s".format(descriptor.datecol)]
                params = [brokerday_for_timestamp(anchor_ts)]
                if getattr(descriptor, "symbolcol", None):
                    where.append("{0} = %s".format(descriptor.symbolcol))
                    params.append(symbol)
                cur.execute(
                    """
                    SELECT id, {datecol}
                    FROM public.days
                    WHERE {where_sql}
                    ORDER BY {datecol} DESC, id DESC
                    LIMIT %s
                    """.format(
                        datecol=descriptor.datecol,
                        where_sql=" AND ".join(where),
                    ),
                    tuple(params + [count]),
                )
                rows = [dict(row) for row in cur.fetchall()]
            refs = [_dayref_from_row(row, descriptor=descriptor) for row in rows]
            materialized = [ref for ref in refs if ref is not None]
            if materialized:
                materialized.sort(key=lambda item: (item.starttime, item.dayid))
                return materialized[-count:]

    latest_day = brokerday_for_timestamp(anchor_ts)
    refs: List[DayRef] = []
    for offset in range(count - 1, -1, -1):
        brokerday = latest_day - timedelta(days=offset)
        starttime, endtime = brokerday_bounds(brokerday)
        refs.append(
            DayRef(
                dayid=_synthetic_dayid(brokerday),
                brokerday=brokerday,
                starttime=starttime,
                endtime=endtime,
            )
        )
    return refs


def resolve_backfill_range(
    conn: Any,
    *,
    symbol: str,
    last_broker_days: Optional[int],
    from_ts: Optional[datetime],
    to_ts: Optional[datetime],
) -> BackfillRange:
    if from_ts is not None or to_ts is not None:
        if from_ts is None or to_ts is None:
            raise ValueError("--from and --to must be provided together")
        start = _as_utc(from_ts)
        end = _as_utc(to_ts)
        if start is None or end is None or end <= start:
            raise ValueError("invalid time range")
        brokerdays: List[date] = []
        cursor = brokerday_for_timestamp(start)
        terminal = brokerday_for_timestamp(end)
        while cursor <= terminal:
            brokerdays.append(cursor)
            cursor += timedelta(days=1)
        return BackfillRange(start=start, end=end, brokerdays=brokerdays)

    latest = latest_tick(conn, symbol=symbol)
    anchor_ts = _as_utc(latest.get("timestamp") if latest else None)
    if anchor_ts is None:
        raise ValueError("no ticks found for symbol {0}".format(symbol))
    refs = recent_day_refs(conn, symbol=symbol, last_broker_days=max(1, int(last_broker_days or 2)), anchor_ts=anchor_ts)
    return BackfillRange(
        start=refs[0].starttime,
        end=refs[-1].endtime,
        brokerdays=[ref.brokerday for ref in refs],
    )


def direction_from_pricechange(pricechange: Optional[float]) -> str:
    if pricechange is None:
        return "flat"
    if pricechange > 0:
        return "up"
    if pricechange < 0:
        return "down"
    return "flat"


def classify_motionstate(
    *,
    pricechange: Optional[float],
    velocity: Optional[float],
    acceleration: Optional[float],
    efficiency: Optional[float],
    spreadmultiple: Optional[float],
) -> str:
    # Order matters. Exhausted states are checked before generic decelerating states so they stay reachable.
    if efficiency is not None and efficiency < 0.25:
        return "choppy"
    if spreadmultiple is not None and spreadmultiple < 1.2:
        return "quiet"
    if pricechange is not None and pricechange > 0 and velocity is not None and velocity > 0:
        if acceleration is not None and acceleration < 0 and efficiency is not None and efficiency < 0.35:
            return "exhausted_up"
        if (
            acceleration is not None
            and acceleration > 0
            and spreadmultiple is not None
            and spreadmultiple >= 3.0
            and efficiency is not None
            and efficiency >= 0.45
        ):
            return "fast_up"
        if acceleration is not None and acceleration < 0:
            return "decelerating_up"
        if acceleration is not None and acceleration > 0:
            return "building_up"
    if pricechange is not None and pricechange < 0 and velocity is not None and velocity < 0:
        if acceleration is not None and acceleration > 0 and efficiency is not None and efficiency < 0.35:
            return "exhausted_down"
        if (
            acceleration is not None
            and acceleration < 0
            and spreadmultiple is not None
            and spreadmultiple >= 3.0
            and efficiency is not None
            and efficiency >= 0.45
        ):
            return "fast_down"
        if acceleration is not None and acceleration > 0:
            return "decelerating_down"
        if acceleration is not None and acceleration < 0:
            return "building_down"
    return "quiet"


def build_motionpoint_row(
    *,
    sample: TickSample,
    windowsec: int,
    past_sample: Optional[TickSample],
    prev_state: Optional[MotionSeed],
) -> Dict[str, Any]:
    elapsedsec: Optional[float] = None
    pricechange: Optional[float] = None
    velocity: Optional[float] = None
    totalmove: Optional[float] = None
    efficiency: Optional[float] = None
    spreadmultiple: Optional[float] = None
    prevvelocity = prev_state.velocity if prev_state else None
    prevacceleration = prev_state.acceleration if prev_state else None
    acceleration: Optional[float] = None
    jerk: Optional[float] = None

    if past_sample is not None and sample.mid is not None and past_sample.mid is not None:
        elapsedsec = (sample.timestamp - past_sample.timestamp).total_seconds()
        if elapsedsec > 0:
            pricechange = float(sample.mid - past_sample.mid)
            velocity = float(pricechange / elapsedsec)
            totalmove = max(0.0, float(sample.cumulative_abs_move - past_sample.cumulative_abs_move))
            if totalmove > 0:
                efficiency = abs(pricechange) / totalmove
            if sample.spread is not None and sample.spread > 0:
                spreadmultiple = abs(pricechange) / sample.spread
    if prev_state is not None:
        state_elapsed = (sample.timestamp - prev_state.timestamp).total_seconds()
        if state_elapsed > 0 and velocity is not None and prevvelocity is not None:
            acceleration = float((velocity - prevvelocity) / state_elapsed)
        if state_elapsed > 0 and acceleration is not None and prevacceleration is not None:
            jerk = float((acceleration - prevacceleration) / state_elapsed)

    direction = direction_from_pricechange(pricechange)
    motionstate = classify_motionstate(
        pricechange=pricechange,
        velocity=velocity,
        acceleration=acceleration,
        efficiency=efficiency,
        spreadmultiple=spreadmultiple,
    )
    return {
        "tickid": sample.tickid,
        "timestamp": sample.timestamp,
        "windowsec": int(windowsec),
        "mid": float(sample.mid or 0.0),
        "bid": sample.bid,
        "ask": sample.ask,
        "spread": sample.spread,
        "pasttickid": past_sample.tickid if past_sample is not None else None,
        "pasttimestamp": past_sample.timestamp if past_sample is not None else None,
        "pastmid": past_sample.mid if past_sample is not None else None,
        "elapsedsec": elapsedsec,
        "pricechange": pricechange,
        "velocity": velocity,
        "prevvelocity": prevvelocity,
        "acceleration": acceleration,
        "prevacceleration": prevacceleration,
        "jerk": jerk,
        "totalmove": totalmove,
        "efficiency": efficiency,
        "spreadmultiple": spreadmultiple,
        "direction": direction,
        "motionstate": motionstate,
    }


def build_signal_candidate(
    *,
    tick_row: Dict[str, Any],
    points: Dict[int, Dict[str, Any]],
    last_signal_at: Dict[str, datetime],
    signalrule: str,
) -> Optional[PendingSignal]:
    state3 = str(points.get(3, {}).get("motionstate") or "")
    state10 = str(points.get(10, {}).get("motionstate") or "")
    state30 = str(points.get(30, {}).get("motionstate") or "")
    velocity3 = _safe_float(points.get(3, {}).get("velocity"))
    acceleration3 = _safe_float(points.get(3, {}).get("acceleration"))
    velocity10 = _safe_float(points.get(10, {}).get("velocity"))
    acceleration10 = _safe_float(points.get(10, {}).get("acceleration"))
    efficiency3 = _safe_float(points.get(3, {}).get("efficiency"))
    spreadmultiple3 = _safe_float(points.get(3, {}).get("spreadmultiple"))
    spread = _safe_float(tick_row.get("spread"))
    ticktime = _as_utc(tick_row.get("timestamp"))
    bid = _safe_float(tick_row.get("bid"))
    ask = _safe_float(tick_row.get("ask"))
    mid = _safe_float(tick_row.get("mid"))
    if ticktime is None or bid is None or ask is None or mid is None or spread is None:
        return None
    if spread <= 0 or spread > MAX_REASONABLE_SPREAD:
        return None

    side: Optional[str] = None
    if signalrule == DEFAULT_SIGNAL_RULE:
        if efficiency3 is None or spreadmultiple3 is None or efficiency3 < 0.45 or spreadmultiple3 < 2.5:
            return None
        if state3 in {"fast_up", "building_up"} and velocity10 is not None and velocity10 > 0 and acceleration10 is not None and acceleration10 >= -SIGNAL_ACCEL_EPSILON:
            side = "buy"
        elif state3 in {"fast_down", "building_down"} and velocity10 is not None and velocity10 < 0 and acceleration10 is not None and acceleration10 <= SIGNAL_ACCEL_EPSILON:
            side = "sell"
    elif signalrule == MICRO_BURST_SIGNAL_RULE:
        if state10 != "choppy" or state30 != "choppy":
            return None
        if efficiency3 is None or efficiency3 < 0.6:
            return None
        if velocity3 is None or acceleration3 is None or velocity10 is None:
            return None
        if state3 in {"fast_up", "building_up"} and velocity3 > 0 and acceleration3 > 0 and abs(velocity10) < abs(velocity3) * 0.6:
            side = "buy"
        elif state3 in {"fast_down", "building_down"} and velocity3 < 0 and acceleration3 < 0 and abs(velocity10) < abs(velocity3) * 0.6:
            side = "sell"
    else:
        raise ValueError("unsupported signal rule: {0}".format(signalrule))
    if side is None:
        return None

    previous_signal_at = last_signal_at.get(side)
    cooldown_seconds = int(SIGNAL_RULE_COOLDOWN_SECONDS[signalrule])
    if previous_signal_at is not None and (ticktime - previous_signal_at).total_seconds() < cooldown_seconds:
        return None

    if side == "buy":
        candidate = PendingSignal(
            tickid=int(tick_row["id"]),
            timestamp=ticktime,
            side=side,
            mid=mid,
            bid=bid,
            ask=ask,
            spread=spread,
            velocity3=velocity3,
            acceleration3=acceleration3,
            efficiency3=efficiency3,
            spreadmultiple3=spreadmultiple3,
            state3=points.get(3, {}).get("motionstate"),
            velocity10=velocity10,
            acceleration10=acceleration10,
            efficiency10=_safe_float(points.get(10, {}).get("efficiency")),
            spreadmultiple10=_safe_float(points.get(10, {}).get("spreadmultiple")),
            state10=points.get(10, {}).get("motionstate"),
            velocity30=_safe_float(points.get(30, {}).get("velocity")),
            acceleration30=_safe_float(points.get(30, {}).get("acceleration")),
            efficiency30=_safe_float(points.get(30, {}).get("efficiency")),
            spreadmultiple30=_safe_float(points.get(30, {}).get("spreadmultiple")),
            state30=points.get(30, {}).get("motionstate"),
            riskfreeprice=ask + RISKFREE_DISTANCE,
            stopprice=ask - STOP_DISTANCE,
            targetprice=ask + TARGET_DISTANCE,
            lookaheadsec=LOOKAHEAD_SECONDS,
            signalrule=signalrule,
        )
    else:
        candidate = PendingSignal(
            tickid=int(tick_row["id"]),
            timestamp=ticktime,
            side=side,
            mid=mid,
            bid=bid,
            ask=ask,
            spread=spread,
            velocity3=velocity3,
            acceleration3=acceleration3,
            efficiency3=efficiency3,
            spreadmultiple3=spreadmultiple3,
            state3=points.get(3, {}).get("motionstate"),
            velocity10=velocity10,
            acceleration10=acceleration10,
            efficiency10=_safe_float(points.get(10, {}).get("efficiency")),
            spreadmultiple10=_safe_float(points.get(10, {}).get("spreadmultiple")),
            state10=points.get(10, {}).get("motionstate"),
            velocity30=_safe_float(points.get(30, {}).get("velocity")),
            acceleration30=_safe_float(points.get(30, {}).get("acceleration")),
            efficiency30=_safe_float(points.get(30, {}).get("efficiency")),
            spreadmultiple30=_safe_float(points.get(30, {}).get("spreadmultiple")),
            state30=points.get(30, {}).get("motionstate"),
            riskfreeprice=bid - RISKFREE_DISTANCE,
            stopprice=bid + STOP_DISTANCE,
            targetprice=bid - TARGET_DISTANCE,
            lookaheadsec=LOOKAHEAD_SECONDS,
            signalrule=signalrule,
        )
    last_signal_at[side] = ticktime
    return candidate


def classify_signal_outcome(
    *,
    seconds_to_riskfree: Optional[float],
    seconds_to_target: Optional[float],
    seconds_to_stop: Optional[float],
) -> str:
    if seconds_to_target is not None and (seconds_to_stop is None or seconds_to_target <= seconds_to_stop):
        return "target_before_stop"
    if seconds_to_riskfree is not None and (seconds_to_stop is None or seconds_to_riskfree <= seconds_to_stop):
        return "riskfree_before_stop"
    if seconds_to_stop is not None and (seconds_to_riskfree is None or seconds_to_stop < seconds_to_riskfree):
        return "stop_before_riskfree"
    return "no_decision"


def score_signal(*, outcome: str, seconds_to_riskfree: Optional[float], maxadverse: Optional[float]) -> float:
    score = 0.0
    if outcome == "target_before_stop":
        score += 100.0
    elif outcome == "riskfree_before_stop":
        score += 50.0
    elif outcome == "stop_before_riskfree":
        score -= 100.0
    if seconds_to_riskfree is not None:
        score -= float(seconds_to_riskfree)
    if maxadverse is not None:
        score -= float(maxadverse) * 10.0
    return float(score)


def finalize_expired_signals(
    pending_signals: Deque[PendingSignal],
    *,
    current_time: datetime,
    finalized_rows: List[Dict[str, Any]],
) -> int:
    finalized = 0
    while pending_signals and pending_signals[0].expire_at < current_time:
        finalized_rows.append(pending_signals.popleft().finalize_row())
        finalized += 1
    return finalized


def backfill_motion_trade_spots(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
    signalrule: str = DEFAULT_SIGNAL_RULE,
) -> Dict[str, Any]:
    start_ts = _as_utc(start_ts)
    end_ts = _as_utc(end_ts)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        raise ValueError("invalid backfill time range")
    if signalrule not in SIGNAL_RULE_COOLDOWN_SECONDS:
        raise ValueError("unsupported signal rule: {0}".format(signalrule))

    effective_start = start_ts - timedelta(seconds=MAX_WINDOW_SECONDS)
    evaluation_end = end_ts + timedelta(seconds=LOOKAHEAD_SECONDS)

    delete_backfill_range(conn, start_ts=start_ts, end_ts=end_ts, signalrule=signalrule)

    history = TickHistory(windows=MOTION_WINDOWS)
    seed_tick = load_seed_tick(conn, symbol=symbol, before_ts=effective_start)
    normalized_seed = _normalize_tick_row(seed_tick or {})
    if normalized_seed.get("timestamp") is not None and normalized_seed.get("mid") is not None:
        history.append(
            tickid=int(normalized_seed["id"]),
            timestamp=normalized_seed["timestamp"],
            bid=normalized_seed.get("bid"),
            ask=normalized_seed.get("ask"),
            mid=float(normalized_seed["mid"]),
            spread=normalized_seed.get("spread"),
        )

    previous_states = load_seed_motion_states(conn, before_ts=effective_start)
    last_signal_at = load_signal_cooldowns(conn, before_ts=start_ts, signalrule=signalrule)
    pending_signals: Deque[PendingSignal] = deque()
    pending_motion_rows: List[Dict[str, Any]] = []
    pending_signal_rows: List[Dict[str, Any]] = []

    tickcount = 0
    motionpoint_count = 0
    signal_count = 0
    lasttickid: Optional[int] = None

    for batch in iter_ticks_between(conn, symbol=symbol, start_ts=effective_start, end_ts=evaluation_end, batch_size=batch_size):
        for raw_row in batch:
            tickcount += 1
            tick_row = _normalize_tick_row(raw_row)
            ticktime = tick_row.get("timestamp")
            if ticktime is None:
                continue

            current_points: Dict[int, Dict[str, Any]] = {}
            if tick_row.get("mid") is not None:
                sample = history.append(
                    tickid=int(tick_row["id"]),
                    timestamp=ticktime,
                    bid=tick_row.get("bid"),
                    ask=tick_row.get("ask"),
                    mid=float(tick_row["mid"]),
                    spread=tick_row.get("spread"),
                )
                for windowsec in MOTION_WINDOWS:
                    point_row = build_motionpoint_row(
                        sample=sample,
                        windowsec=windowsec,
                        past_sample=history.past_for(windowsec=windowsec, current_time=ticktime),
                        prev_state=previous_states.get(windowsec),
                    )
                    current_points[windowsec] = point_row
                    previous_states[windowsec] = MotionSeed(
                        timestamp=ticktime,
                        velocity=_safe_float(point_row.get("velocity")),
                        acceleration=_safe_float(point_row.get("acceleration")),
                    )
                    if start_ts <= ticktime <= end_ts:
                        pending_motion_rows.append(point_row)
                        motionpoint_count += 1
                        lasttickid = int(tick_row["id"])
                history.trim()

            if start_ts <= ticktime <= end_ts and current_points:
                candidate = build_signal_candidate(
                    tick_row=tick_row,
                    points=current_points,
                    last_signal_at=last_signal_at,
                    signalrule=signalrule,
                )
                if candidate is not None:
                    pending_signals.append(candidate)

            if pending_signals:
                for signal in pending_signals:
                    signal.update(tick_row)
                signal_count += finalize_expired_signals(
                    pending_signals,
                    current_time=ticktime,
                    finalized_rows=pending_signal_rows,
                )

            if len(pending_motion_rows) >= max(300, batch_size * len(MOTION_WINDOWS)):
                insert_motionpoints(conn, pending_motion_rows)
                pending_motion_rows = []

            if len(pending_signal_rows) >= 200:
                insert_signals(conn, pending_signal_rows)
                pending_signal_rows = []

    while pending_signals:
        pending_signal_rows.append(pending_signals.popleft().finalize_row())
        signal_count += 1

    if pending_motion_rows:
        insert_motionpoints(conn, pending_motion_rows)
    if pending_signal_rows:
        insert_signals(conn, pending_signal_rows)
    update_motionstate(conn, lasttickid=lasttickid)

    return {
        "symbol": symbol,
        "start": start_ts,
        "end": end_ts,
        "tickcount": tickcount,
        "motionpoint_count": motionpoint_count,
        "motionsignal_count": signal_count,
        "lasttickid": lasttickid,
    }


def motionpoints_from_signal_row(row: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    return {
        3: {
            "velocity": row.get("velocity3"),
            "acceleration": row.get("acceleration3"),
            "efficiency": row.get("efficiency3"),
            "spreadmultiple": row.get("spreadmultiple3"),
            "motionstate": row.get("state3"),
        },
        10: {
            "velocity": row.get("velocity10"),
            "acceleration": row.get("acceleration10"),
            "efficiency": row.get("efficiency10"),
            "spreadmultiple": row.get("spreadmultiple10"),
            "motionstate": row.get("state10"),
        },
        30: {
            "velocity": row.get("velocity30"),
            "acceleration": row.get("acceleration30"),
            "efficiency": row.get("efficiency30"),
            "spreadmultiple": row.get("spreadmultiple30"),
            "motionstate": row.get("state30"),
        },
    }


def recreate_signals_from_motionpoints(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
    signalrule: str,
) -> Dict[str, Any]:
    start_ts = _as_utc(start_ts)
    end_ts = _as_utc(end_ts)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        raise ValueError("invalid recreate-signals time range")
    if signalrule not in SIGNAL_RULE_COOLDOWN_SECONDS:
        raise ValueError("unsupported signal rule: {0}".format(signalrule))

    evaluation_end = end_ts + timedelta(seconds=LOOKAHEAD_SECONDS)
    delete_signal_range(conn, start_ts=start_ts, end_ts=end_ts, signalrule=signalrule)

    last_signal_at = load_signal_cooldowns(conn, before_ts=start_ts, signalrule=signalrule)
    pending_signals: Deque[PendingSignal] = deque()
    pending_signal_rows: List[Dict[str, Any]] = []
    tickcount = 0
    signal_count = 0

    for batch in iter_ticks_with_motionpoints_between(
        conn,
        symbol=symbol,
        start_ts=start_ts,
        end_ts=evaluation_end,
        batch_size=batch_size,
    ):
        for raw_row in batch:
            tickcount += 1
            tick_row = _normalize_tick_row(raw_row)
            ticktime = tick_row.get("timestamp")
            if ticktime is None:
                continue

            if start_ts <= ticktime <= end_ts:
                candidate = build_signal_candidate(
                    tick_row=tick_row,
                    points=motionpoints_from_signal_row(raw_row),
                    last_signal_at=last_signal_at,
                    signalrule=signalrule,
                )
                if candidate is not None:
                    pending_signals.append(candidate)

            if pending_signals:
                for signal in pending_signals:
                    signal.update(tick_row)
                signal_count += finalize_expired_signals(
                    pending_signals,
                    current_time=ticktime,
                    finalized_rows=pending_signal_rows,
                )

            if len(pending_signal_rows) >= 200:
                insert_signals(conn, pending_signal_rows)
                pending_signal_rows = []

    while pending_signals:
        pending_signal_rows.append(pending_signals.popleft().finalize_row())
        signal_count += 1

    if pending_signal_rows:
        insert_signals(conn, pending_signal_rows)

    return {
        "symbol": symbol,
        "signalrule": signalrule,
        "start": start_ts,
        "end": end_ts,
        "tickcount": tickcount,
        "motionsignal_count": signal_count,
    }


def export_query_to_csv(
    conn: Any,
    *,
    query: str,
    params: Sequence[Any],
    output_path: Path,
    cursor_name: str,
) -> int:
    row_count = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = EXPORT_BATCH_SIZE
            cur.execute(query, tuple(params))
            writer.writerow([str(description[0]) for description in cur.description])
            while True:
                rows = cur.fetchmany(cur.itersize)
                if not rows:
                    break
                for row in rows:
                    writer.writerow([_csv_value(value) for value in row])
                    row_count += 1
    return row_count


def export_motion_tables(
    conn: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    output_dir: Path,
) -> Dict[str, Any]:
    motionpoint_path = output_dir / "motionpoint_last2days.csv"
    motionsignal_path = output_dir / "motionsignal_last2days.csv"

    motionpoint_rows = export_query_to_csv(
        conn,
        query="""
            SELECT *
            FROM public.motionpoint
            WHERE timestamp >= %s
              AND timestamp <= %s
              AND windowsec = ANY(%s)
            ORDER BY timestamp ASC, tickid ASC, windowsec ASC
        """,
        params=(start_ts, end_ts, list(MOTION_WINDOWS)),
        output_path=motionpoint_path,
        cursor_name="motionpoint_export",
    )
    motionsignal_rows = export_query_to_csv(
        conn,
        query="""
            SELECT *
            FROM public.motionsignal
            WHERE timestamp >= %s
              AND timestamp <= %s
              AND signalrule = %s
            ORDER BY timestamp ASC, id ASC
        """,
        params=(start_ts, end_ts, DEFAULT_SIGNAL_RULE),
        output_path=motionsignal_path,
        cursor_name="motionsignal_export",
    )
    return {
        "motionpoint_rows": motionpoint_rows,
        "motionsignal_rows": motionsignal_rows,
        "motionpoint_path": str(motionpoint_path.resolve()),
        "motionsignal_path": str(motionsignal_path.resolve()),
    }


def parse_timestamp_arg(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).strip())
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BROKER_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def add_range_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--last-broker-days", type=int, help="Backfill/export the most recent broker sessions.")
    parser.add_argument("--from", dest="from_ts", help="Inclusive range start timestamp.")
    parser.add_argument("--to", dest="to_ts", help="Inclusive range end timestamp.")


def resolve_args_range(conn: Any, *, symbol: str, args: argparse.Namespace) -> BackfillRange:
    last_broker_days = getattr(args, "last_broker_days", None)
    from_raw = getattr(args, "from_ts", None)
    to_raw = getattr(args, "to_ts", None)
    if from_raw or to_raw:
        if not from_raw or not to_raw:
            raise ValueError("--from and --to must be provided together")
        return resolve_backfill_range(
            conn,
            symbol=symbol,
            last_broker_days=None,
            from_ts=parse_timestamp_arg(str(from_raw)),
            to_ts=parse_timestamp_arg(str(to_raw)),
        )
    return resolve_backfill_range(
        conn,
        symbol=symbol,
        last_broker_days=max(1, int(last_broker_days or 2)),
        from_ts=None,
        to_ts=None,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill and export motion research layers for raw XAUUSD ticks.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Tick symbol to process.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Tick batch size.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill", help="Backfill motionpoint and motionsignal for a time range.")
    add_range_arguments(backfill)

    export = subparsers.add_parser("export", help="Export motionpoint and motionsignal CSV files for a time range.")
    add_range_arguments(export)
    export.add_argument("--output-dir", default=str(BASE_DIR / "logs"), help="Directory to write CSV exports into.")

    recreate_signals = subparsers.add_parser("recreate-signals", help="Rebuild motionsignal rows from existing motionpoint rows for a time range.")
    add_range_arguments(recreate_signals)
    recreate_signals.add_argument("--rule", required=True, choices=sorted(SIGNAL_RULE_COOLDOWN_SECONDS), help="Signal rule to recreate.")
    return parser


def jobs_main() -> int:
    args = build_parser().parse_args()
    symbol = str(args.symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL
    batch_size = max(100, int(args.batch_size))
    readonly = args.command == "export"
    with db_connection(readonly=readonly, autocommit=False) as conn:
        target_range = resolve_args_range(conn, symbol=symbol, args=args)
        brokerdays_text = ",".join(day.isoformat() for day in target_range.brokerdays) if target_range.brokerdays else "-"
        if args.command == "backfill":
            result = backfill_motion_trade_spots(
                conn,
                symbol=symbol,
                start_ts=target_range.start,
                end_ts=target_range.end,
                batch_size=batch_size,
                signalrule=DEFAULT_SIGNAL_RULE,
            )
            conn.commit()
            _print(
                "symbol={0} brokerdays={1} start={2} end={3} ticks={4} motionpoints={5} motionsignals={6} lasttickid={7}".format(
                    result["symbol"],
                    brokerdays_text,
                    result["start"].isoformat(),
                    result["end"].isoformat(),
                    result["tickcount"],
                    result["motionpoint_count"],
                    result["motionsignal_count"],
                    result["lasttickid"],
                )
            )
            return 0
        if args.command == "recreate-signals":
            result = recreate_signals_from_motionpoints(
                conn,
                symbol=symbol,
                start_ts=target_range.start,
                end_ts=target_range.end,
                batch_size=batch_size,
                signalrule=str(args.rule),
            )
            conn.commit()
            _print(
                "symbol={0} rule={1} brokerdays={2} start={3} end={4} ticks={5} motionsignals={6}".format(
                    result["symbol"],
                    result["signalrule"],
                    brokerdays_text,
                    result["start"].isoformat(),
                    result["end"].isoformat(),
                    result["tickcount"],
                    result["motionsignal_count"],
                )
            )
            return 0

        export_result = export_motion_tables(
            conn,
            start_ts=target_range.start,
            end_ts=target_range.end,
            output_dir=Path(str(args.output_dir)).expanduser(),
        )
        conn.rollback()
        _print(
            "brokerdays={0} start={1} end={2}".format(
                brokerdays_text,
                target_range.start.isoformat(),
                target_range.end.isoformat(),
            )
        )
        _print(
            "motionpoint_rows={0} output={1}".format(
                export_result["motionpoint_rows"],
                export_result["motionpoint_path"],
            )
        )
        _print(
            "motionsignal_rows={0} output={1}".format(
                export_result["motionsignal_rows"],
                export_result["motionsignal_path"],
            )
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(jobs_main())
