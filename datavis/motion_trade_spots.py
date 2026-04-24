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
from typing import Any, Deque, Dict, FrozenSet, Generator, Iterable, List, Optional, Sequence, Set

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
BEST_FINGERPRINT_SIGNAL_RULE = "motion_v3_best_fingerprints"
LOOKAHEAD_SECONDS = 300
RISKFREE_DISTANCE = 0.30
TARGET_DISTANCE = 1.00
STOP_DISTANCE = 1.00
SIGNAL_ACCEL_EPSILON = 0.01
MAX_REASONABLE_SPREAD = 0.50
MAX_WINDOW_SECONDS = max(MOTION_WINDOWS)
MIN_FINGERPRINT_SIGNALS = 20
SPREADMULTIPLE3_BUCKET_STEP = 1.0
EFFICIENCY3_BUCKET_STEP = 0.1
VELOCITY3_BUCKET_STEP = 0.05
ACCELERATION3_BUCKET_STEP = 0.01
VELOCITY10_BUCKET_STEP = 0.02
ACCELERATION10_BUCKET_STEP = 0.005
SCENARIO_MIN_SIGNALS = 50
SCENARIO_MIN_USEFUL_PCT = 60.0
SCENARIO_MAX_STOP_PCT = 35.0
SCENARIO_MAX_SECONDS_TO_RISKFREE = 20.0
SCENARIO_MAX_MAX_ADVERSE = 3.0
DEFAULT_DIRECTIONAL_STATES: FrozenSet[str] = frozenset({"fast_up", "building_up", "fast_down", "building_down"})
MICRO_BURST_SCENARIO_FAMILIES: FrozenSet[str] = frozenset(
    {
        "micro_burst_choppy",
        "micro_burst_short_confirm",
        "strict_micro_burst",
    }
)
CONTINUATION_SCENARIO_FAMILIES: FrozenSet[str] = frozenset({"continuation"})
SIGNAL_RULE_COOLDOWN_SECONDS = {
    DEFAULT_SIGNAL_RULE: 10,
    MICRO_BURST_SIGNAL_RULE: 20,
    BEST_FINGERPRINT_SIGNAL_RULE: 10,
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


@dataclass(frozen=True)
class SignalGenerationConfig:
    signalrule: str
    strategy: str
    family: Optional[str]
    min_efficiency3: Optional[float]
    min_spreadmultiple3: Optional[float]
    max_spreadmultiple3: Optional[float]
    require_state10: Optional[str]
    require_state30: Optional[str]
    allow_state3: FrozenSet[str]
    velocity10_ratio_max: Optional[float]
    cooldownsec: int
    riskfreeusd: float
    targetusd: float
    stopusd: float
    lookaheadsec: int


@dataclass(frozen=True)
class MotionModelScenario:
    id: int
    scenarioname: str
    signalrule: str
    family: Optional[str]
    min_efficiency3: Optional[float]
    min_spreadmultiple3: Optional[float]
    max_spreadmultiple3: Optional[float]
    require_state10: Optional[str]
    require_state30: Optional[str]
    allow_state3: FrozenSet[str]
    velocity10_ratio_max: Optional[float]
    cooldownsec: int
    riskfreeusd: float
    targetusd: float
    stopusd: float
    lookaheadsec: int
    isactive: bool
    createdat: Optional[datetime]

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "MotionModelScenario":
        riskfreeusd = _safe_float(row.get("riskfreeusd"))
        targetusd = _safe_float(row.get("targetusd"))
        stopusd = _safe_float(row.get("stopusd"))
        return cls(
            id=int(row.get("id") or 0),
            scenarioname=str(row.get("scenarioname") or "").strip(),
            signalrule=str(row.get("signalrule") or "").strip(),
            family=_normalize_text(row.get("family")),
            min_efficiency3=_safe_float(row.get("min_efficiency3")),
            min_spreadmultiple3=_safe_float(row.get("min_spreadmultiple3")),
            max_spreadmultiple3=_safe_float(row.get("max_spreadmultiple3")),
            require_state10=_normalize_text(row.get("require_state10")),
            require_state30=_normalize_text(row.get("require_state30")),
            allow_state3=_normalize_text_set(row.get("allow_state3")),
            velocity10_ratio_max=_safe_float(row.get("velocity10_ratio_max")),
            cooldownsec=max(1, _safe_int(row.get("cooldownsec"), default=SIGNAL_RULE_COOLDOWN_SECONDS[DEFAULT_SIGNAL_RULE])),
            riskfreeusd=max(0.0, riskfreeusd if riskfreeusd is not None else RISKFREE_DISTANCE),
            targetusd=max(0.0, targetusd if targetusd is not None else TARGET_DISTANCE),
            stopusd=max(0.0, stopusd if stopusd is not None else STOP_DISTANCE),
            lookaheadsec=max(1, _safe_int(row.get("lookaheadsec"), default=LOOKAHEAD_SECONDS)),
            isactive=bool(row.get("isactive", True)),
            createdat=_as_utc(row.get("createdat")),
        )

    def signal_config(self) -> SignalGenerationConfig:
        return SignalGenerationConfig(
            signalrule=self.signalrule,
            strategy=scenario_strategy_from_family(self.family),
            family=self.family,
            min_efficiency3=self.min_efficiency3,
            min_spreadmultiple3=self.min_spreadmultiple3,
            max_spreadmultiple3=self.max_spreadmultiple3,
            require_state10=self.require_state10,
            require_state30=self.require_state30,
            allow_state3=self.allow_state3,
            velocity10_ratio_max=self.velocity10_ratio_max,
            cooldownsec=self.cooldownsec,
            riskfreeusd=self.riskfreeusd,
            targetusd=self.targetusd,
            stopusd=self.stopusd,
            lookaheadsec=self.lookaheadsec,
        )


@dataclass(frozen=True)
class FingerprintKey:
    side: str
    state3: Optional[str]
    state10: Optional[str]
    state30: Optional[str]
    sm3bucket: Optional[int]
    eff3bucket: Optional[int]
    v3bucket: Optional[int]
    a3bucket: Optional[int]
    v10bucket: Optional[int]
    a10bucket: Optional[int]


@dataclass
class FingerprintAggregate:
    total: int = 0
    targets: int = 0
    riskfree: int = 0
    stops: int = 0
    seconds_to_riskfree_sum: float = 0.0
    seconds_to_riskfree_count: int = 0
    maxadverse_sum: float = 0.0
    maxadverse_count: int = 0
    score_sum: float = 0.0
    score_count: int = 0

    def observe(self, row: Dict[str, Any]) -> None:
        self.total += 1
        outcome = str(row.get("outcome") or "").strip().lower()
        if outcome == "target_before_stop":
            self.targets += 1
        elif outcome == "riskfree_before_stop":
            self.riskfree += 1
        elif outcome == "stop_before_riskfree":
            self.stops += 1

        seconds_to_riskfree = _safe_float(row.get("seconds_to_riskfree"))
        if seconds_to_riskfree is not None and outcome in {"target_before_stop", "riskfree_before_stop"}:
            self.seconds_to_riskfree_sum += seconds_to_riskfree
            self.seconds_to_riskfree_count += 1

        maxadverse = _safe_float(row.get("maxadverse"))
        if maxadverse is not None:
            self.maxadverse_sum += maxadverse
            self.maxadverse_count += 1

        score = _safe_float(row.get("score"))
        if score is not None:
            self.score_sum += score
            self.score_count += 1

    @property
    def useful(self) -> int:
        return self.targets + self.riskfree

    def _average(self, total: float, count: int) -> Optional[float]:
        if count <= 0:
            return None
        return float(total / count)

    def as_row(self, *, signalrule: str, key: FingerprintKey, baseline_useful_pct: Optional[float]) -> Dict[str, Any]:
        total = max(1, int(self.total))
        target_pct = float(self.targets * 100.0 / total)
        useful_pct = float(self.useful * 100.0 / total)
        stop_pct = float(self.stops * 100.0 / total)
        lift: Optional[float] = None
        if baseline_useful_pct is not None and baseline_useful_pct > 0:
            lift = float(useful_pct / baseline_useful_pct)
        return {
            "signalrule": signalrule,
            "side": key.side,
            "state3": key.state3,
            "state10": key.state10,
            "state30": key.state30,
            "sm3bucket": key.sm3bucket,
            "eff3bucket": key.eff3bucket,
            "v3bucket": key.v3bucket,
            "a3bucket": key.a3bucket,
            "v10bucket": key.v10bucket,
            "a10bucket": key.a10bucket,
            "total": self.total,
            "targets": self.targets,
            "riskfree": self.riskfree,
            "stops": self.stops,
            "targetpct": target_pct,
            "usefulpct": useful_pct,
            "stoppct": stop_pct,
            "avgsectoriskfree": self._average(self.seconds_to_riskfree_sum, self.seconds_to_riskfree_count),
            "avgmaxadverse": self._average(self.maxadverse_sum, self.maxadverse_count),
            "avgscore": self._average(self.score_sum, self.score_count),
            "lift": lift,
        }


@dataclass
class SignalSummaryAggregate:
    signals: int = 0
    targets: int = 0
    riskfree: int = 0
    stops: int = 0
    nodecision: int = 0
    seconds_to_riskfree_sum: float = 0.0
    seconds_to_riskfree_count: int = 0
    maxadverse_sum: float = 0.0
    maxadverse_count: int = 0
    score_sum: float = 0.0
    score_count: int = 0

    def observe(self, row: Dict[str, Any]) -> None:
        self.signals += 1
        outcome = str(row.get("outcome") or "").strip().lower()
        if outcome == "target_before_stop":
            self.targets += 1
        elif outcome == "riskfree_before_stop":
            self.riskfree += 1
        elif outcome == "stop_before_riskfree":
            self.stops += 1
        else:
            self.nodecision += 1

        seconds_to_riskfree = _safe_float(row.get("seconds_to_riskfree"))
        if seconds_to_riskfree is not None and outcome in {"target_before_stop", "riskfree_before_stop"}:
            self.seconds_to_riskfree_sum += seconds_to_riskfree
            self.seconds_to_riskfree_count += 1

        maxadverse = _safe_float(row.get("maxadverse"))
        if maxadverse is not None:
            self.maxadverse_sum += maxadverse
            self.maxadverse_count += 1

        score = _safe_float(row.get("score"))
        if score is not None:
            self.score_sum += score
            self.score_count += 1

    @property
    def useful(self) -> int:
        return self.targets + self.riskfree

    def _average(self, total: float, count: int) -> Optional[float]:
        if count <= 0:
            return None
        return float(total / count)

    def as_result_row(
        self,
        *,
        scenarioid: int,
        signalrule: str,
        fromts: datetime,
        tots: datetime,
        riskfreeusd: float,
        targetusd: float,
        stopusd: float,
    ) -> Dict[str, Any]:
        total = max(1, int(self.signals))
        targetpct = float(self.targets * 100.0 / total)
        usefulpct = float(self.useful * 100.0 / total)
        stoppct = float(self.stops * 100.0 / total)
        avg_seconds_to_riskfree = self._average(self.seconds_to_riskfree_sum, self.seconds_to_riskfree_count)
        avg_maxadverse = self._average(self.maxadverse_sum, self.maxadverse_count)
        avg_score = self._average(self.score_sum, self.score_count)
        passed_constraints = (
            self.signals >= SCENARIO_MIN_SIGNALS
            and usefulpct >= SCENARIO_MIN_USEFUL_PCT
            and stoppct <= SCENARIO_MAX_STOP_PCT
            and avg_seconds_to_riskfree is not None
            and avg_seconds_to_riskfree <= SCENARIO_MAX_SECONDS_TO_RISKFREE
            and avg_maxadverse is not None
            and avg_maxadverse <= SCENARIO_MAX_MAX_ADVERSE
        )
        profitproxy = float(self.targets * targetusd + self.riskfree * riskfreeusd - self.stops * stopusd)
        return {
            "scenarioid": scenarioid,
            "signalrule": signalrule,
            "fromts": fromts,
            "tots": tots,
            "signals": self.signals,
            "targets": self.targets,
            "riskfree": self.riskfree,
            "stops": self.stops,
            "nodecision": self.nodecision,
            "targetpct": targetpct,
            "usefulpct": usefulpct,
            "stoppct": stoppct,
            "avgsecondstoriskfree": avg_seconds_to_riskfree,
            "avgmaxadverse": avg_maxadverse,
            "avgscore": avg_score,
            "profitproxy": profitproxy,
            "passedconstraints": passed_constraints,
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


def _safe_int(value: Any, *, default: int = 0) -> int:
    if value is None:
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _normalize_text(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    return text or None


def _normalize_text_set(values: Any) -> FrozenSet[str]:
    if values is None:
        return frozenset()
    if isinstance(values, str):
        iterable: Iterable[Any] = [values]
    else:
        try:
            iterable = list(values)
        except TypeError:
            iterable = [values]
    normalized = {_normalize_text(value) for value in iterable}
    return frozenset(value for value in normalized if value)


def _bucket_floor(value: Any, *, step: float) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None or step <= 0:
        return None
    return int(math.floor(numeric / step))


def _bucket_round(value: Any, *, step: float) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None or step <= 0:
        return None
    scaled = numeric / step
    if scaled >= 0:
        return int(math.floor(scaled + 0.5))
    return int(math.ceil(scaled - 0.5))


def build_fingerprint_key(
    *,
    side: Any,
    state3: Any,
    state10: Any,
    state30: Any,
    spreadmultiple3: Any,
    efficiency3: Any,
    velocity3: Any,
    acceleration3: Any,
    velocity10: Any,
    acceleration10: Any,
) -> Optional[FingerprintKey]:
    normalized_side = _normalize_text(side)
    if normalized_side not in {"buy", "sell"}:
        return None
    return FingerprintKey(
        side=normalized_side,
        state3=_normalize_text(state3),
        state10=_normalize_text(state10),
        state30=_normalize_text(state30),
        sm3bucket=_bucket_floor(spreadmultiple3, step=SPREADMULTIPLE3_BUCKET_STEP),
        eff3bucket=_bucket_floor(efficiency3, step=EFFICIENCY3_BUCKET_STEP),
        v3bucket=_bucket_round(velocity3, step=VELOCITY3_BUCKET_STEP),
        a3bucket=_bucket_round(acceleration3, step=ACCELERATION3_BUCKET_STEP),
        v10bucket=_bucket_round(velocity10, step=VELOCITY10_BUCKET_STEP),
        a10bucket=_bucket_round(acceleration10, step=ACCELERATION10_BUCKET_STEP),
    )


def _format_metric(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return "{0:.2f}".format(value)


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


def scenario_strategy_from_family(family: Optional[str]) -> str:
    normalized_family = _normalize_text(family)
    if normalized_family in MICRO_BURST_SCENARIO_FAMILIES:
        return "micro_burst"
    if normalized_family in CONTINUATION_SCENARIO_FAMILIES or normalized_family is None:
        return "continuation"
    raise ValueError("unsupported scenario family: {0}".format(family))


def build_named_signal_config(signalrule: str) -> SignalGenerationConfig:
    if signalrule == DEFAULT_SIGNAL_RULE:
        return SignalGenerationConfig(
            signalrule=signalrule,
            strategy="continuation",
            family="continuation",
            min_efficiency3=0.45,
            min_spreadmultiple3=2.5,
            max_spreadmultiple3=None,
            require_state10=None,
            require_state30=None,
            allow_state3=DEFAULT_DIRECTIONAL_STATES,
            velocity10_ratio_max=None,
            cooldownsec=SIGNAL_RULE_COOLDOWN_SECONDS[signalrule],
            riskfreeusd=RISKFREE_DISTANCE,
            targetusd=TARGET_DISTANCE,
            stopusd=STOP_DISTANCE,
            lookaheadsec=LOOKAHEAD_SECONDS,
        )
    if signalrule == MICRO_BURST_SIGNAL_RULE:
        return SignalGenerationConfig(
            signalrule=signalrule,
            strategy="micro_burst",
            family="micro_burst_choppy",
            min_efficiency3=0.60,
            min_spreadmultiple3=None,
            max_spreadmultiple3=None,
            require_state10="choppy",
            require_state30="choppy",
            allow_state3=DEFAULT_DIRECTIONAL_STATES,
            velocity10_ratio_max=0.60,
            cooldownsec=SIGNAL_RULE_COOLDOWN_SECONDS[signalrule],
            riskfreeusd=RISKFREE_DISTANCE,
            targetusd=TARGET_DISTANCE,
            stopusd=STOP_DISTANCE,
            lookaheadsec=LOOKAHEAD_SECONDS,
        )
    if signalrule == BEST_FINGERPRINT_SIGNAL_RULE:
        return SignalGenerationConfig(
            signalrule=signalrule,
            strategy="fingerprint",
            family="best_fingerprint",
            min_efficiency3=None,
            min_spreadmultiple3=None,
            max_spreadmultiple3=None,
            require_state10=None,
            require_state30=None,
            allow_state3=frozenset(),
            velocity10_ratio_max=None,
            cooldownsec=SIGNAL_RULE_COOLDOWN_SECONDS[signalrule],
            riskfreeusd=RISKFREE_DISTANCE,
            targetusd=TARGET_DISTANCE,
            stopusd=STOP_DISTANCE,
            lookaheadsec=LOOKAHEAD_SECONDS,
        )
    raise ValueError("unsupported signal rule: {0}".format(signalrule))


def _allowed_state3_values(config: SignalGenerationConfig) -> FrozenSet[str]:
    return config.allow_state3 or DEFAULT_DIRECTIONAL_STATES


def _side_from_state3(state3: Optional[str]) -> Optional[str]:
    if state3 in {"fast_up", "building_up"}:
        return "buy"
    if state3 in {"fast_down", "building_down"}:
        return "sell"
    return None


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


def iter_signals_between(
    conn: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    signalrule: str,
    batch_size: int,
) -> Iterable[List[Dict[str, Any]]]:
    with conn.cursor(name="motion_signals_between", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = max(1, int(batch_size))
        cur.execute(
            """
            SELECT
                id,
                timestamp,
                side,
                state3,
                state10,
                state30,
                spreadmultiple3,
                efficiency3,
                velocity3,
                acceleration3,
                velocity10,
                acceleration10,
                outcome,
                seconds_to_riskfree,
                maxadverse,
                score
            FROM public.motionsignal
            WHERE timestamp >= %s
              AND timestamp <= %s
              AND signalrule = %s
            ORDER BY timestamp ASC, id ASC
            """,
            (start_ts, end_ts, signalrule),
        )
        while True:
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                return
            yield [dict(row) for row in rows]


def ensure_motionfingerprint_table(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.motionfingerprint (
                id BIGSERIAL PRIMARY KEY,
                signalrule TEXT,
                side TEXT,
                state3 TEXT,
                state10 TEXT,
                state30 TEXT,
                sm3bucket INTEGER,
                eff3bucket INTEGER,
                v3bucket INTEGER,
                a3bucket INTEGER,
                v10bucket INTEGER,
                a10bucket INTEGER,
                total INTEGER,
                targets INTEGER,
                riskfree INTEGER,
                stops INTEGER,
                targetpct DOUBLE PRECISION,
                usefulpct DOUBLE PRECISION,
                stoppct DOUBLE PRECISION,
                avgsectoriskfree DOUBLE PRECISION,
                avgmaxadverse DOUBLE PRECISION,
                avgscore DOUBLE PRECISION,
                lift DOUBLE PRECISION,
                createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS motionfingerprint_signalrule_createdat_idx
                ON public.motionfingerprint (signalrule, createdat DESC);

            CREATE INDEX IF NOT EXISTS motionfingerprint_signalrule_lift_desc_idx
                ON public.motionfingerprint (signalrule, lift DESC, usefulpct DESC, total DESC);
            """
        )


def ensure_motionmodel_tables(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.motionmodelscenario (
                id BIGSERIAL PRIMARY KEY,
                scenarioname TEXT NOT NULL,
                signalrule TEXT NOT NULL,
                family TEXT,
                min_efficiency3 DOUBLE PRECISION,
                min_spreadmultiple3 DOUBLE PRECISION,
                max_spreadmultiple3 DOUBLE PRECISION,
                require_state10 TEXT,
                require_state30 TEXT,
                allow_state3 TEXT[],
                velocity10_ratio_max DOUBLE PRECISION,
                cooldownsec INTEGER,
                riskfreeusd DOUBLE PRECISION,
                targetusd DOUBLE PRECISION,
                stopusd DOUBLE PRECISION,
                lookaheadsec INTEGER,
                isactive BOOLEAN NOT NULL DEFAULT TRUE,
                createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE UNIQUE INDEX IF NOT EXISTS motionmodelscenario_signalrule_uidx
                ON public.motionmodelscenario (signalrule);

            CREATE INDEX IF NOT EXISTS motionmodelscenario_isactive_family_idx
                ON public.motionmodelscenario (isactive, family, scenarioname);

            CREATE TABLE IF NOT EXISTS public.motionmodelresult (
                id BIGSERIAL PRIMARY KEY,
                scenarioid BIGINT REFERENCES public.motionmodelscenario (id),
                signalrule TEXT,
                fromts TIMESTAMPTZ,
                tots TIMESTAMPTZ,
                signals INTEGER,
                targets INTEGER,
                riskfree INTEGER,
                stops INTEGER,
                nodecision INTEGER,
                targetpct DOUBLE PRECISION,
                usefulpct DOUBLE PRECISION,
                stoppct DOUBLE PRECISION,
                avgsecondstoriskfree DOUBLE PRECISION,
                avgmaxadverse DOUBLE PRECISION,
                avgscore DOUBLE PRECISION,
                profitproxy DOUBLE PRECISION,
                passedconstraints BOOLEAN,
                createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS motionmodelresult_scenarioid_createdat_idx
                ON public.motionmodelresult (scenarioid, createdat DESC);

            CREATE INDEX IF NOT EXISTS motionmodelresult_signalrule_fromts_tots_idx
                ON public.motionmodelresult (signalrule, fromts, tots);
            """
        )


def load_active_motion_model_scenarios(conn: Any) -> List[MotionModelScenario]:
    ensure_motionmodel_tables(conn)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                scenarioname,
                signalrule,
                family,
                min_efficiency3,
                min_spreadmultiple3,
                max_spreadmultiple3,
                require_state10,
                require_state30,
                allow_state3,
                velocity10_ratio_max,
                cooldownsec,
                riskfreeusd,
                targetusd,
                stopusd,
                lookaheadsec,
                isactive,
                createdat
            FROM public.motionmodelscenario
            WHERE isactive = TRUE
            ORDER BY family ASC NULLS LAST, scenarioname ASC, id ASC
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
    return [MotionModelScenario.from_row(row) for row in rows]


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


def delete_motionfingerprints(conn: Any, *, signalrule: str) -> None:
    ensure_motionfingerprint_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.motionfingerprint
            WHERE signalrule = %s
            """,
            (signalrule,),
        )


def delete_motionmodel_results(conn: Any, *, scenarioid: int, fromts: datetime, tots: datetime) -> None:
    ensure_motionmodel_tables(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.motionmodelresult
            WHERE scenarioid = %s
              AND fromts = %s
              AND tots = %s
            """,
            (scenarioid, fromts, tots),
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


def insert_motionfingerprints(conn: Any, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_motionfingerprint_table(conn)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.motionfingerprint (
                signalrule,
                side,
                state3,
                state10,
                state30,
                sm3bucket,
                eff3bucket,
                v3bucket,
                a3bucket,
                v10bucket,
                a10bucket,
                total,
                targets,
                riskfree,
                stops,
                targetpct,
                usefulpct,
                stoppct,
                avgsectoriskfree,
                avgmaxadverse,
                avgscore,
                lift
            ) VALUES %s
            """,
            [(
                row["signalrule"],
                row["side"],
                row["state3"],
                row["state10"],
                row["state30"],
                row["sm3bucket"],
                row["eff3bucket"],
                row["v3bucket"],
                row["a3bucket"],
                row["v10bucket"],
                row["a10bucket"],
                row["total"],
                row["targets"],
                row["riskfree"],
                row["stops"],
                row["targetpct"],
                row["usefulpct"],
                row["stoppct"],
                row["avgsectoriskfree"],
                row["avgmaxadverse"],
                row["avgscore"],
                row["lift"],
            ) for row in rows],
            page_size=min(max(1, len(rows)), 500),
        )


def insert_motionmodel_results(conn: Any, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_motionmodel_tables(conn)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.motionmodelresult (
                scenarioid,
                signalrule,
                fromts,
                tots,
                signals,
                targets,
                riskfree,
                stops,
                nodecision,
                targetpct,
                usefulpct,
                stoppct,
                avgsecondstoriskfree,
                avgmaxadverse,
                avgscore,
                profitproxy,
                passedconstraints
            ) VALUES %s
            """,
            [(
                row["scenarioid"],
                row["signalrule"],
                row["fromts"],
                row["tots"],
                row["signals"],
                row["targets"],
                row["riskfree"],
                row["stops"],
                row["nodecision"],
                row["targetpct"],
                row["usefulpct"],
                row["stoppct"],
                row["avgsecondstoriskfree"],
                row["avgmaxadverse"],
                row["avgscore"],
                row["profitproxy"],
                row["passedconstraints"],
            ) for row in rows],
            page_size=min(max(1, len(rows)), 500),
        )


def load_top_motionfingerprints(
    conn: Any,
    *,
    signalrule: str,
    usefulpct_at_least: float = 65.0,
    stoppct_at_most: float = 35.0,
    total_at_least: int = MIN_FINGERPRINT_SIGNALS,
    avgsectoriskfree_at_most: float = 20.0,
) -> List[Dict[str, Any]]:
    ensure_motionfingerprint_table(conn)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                signalrule,
                side,
                state3,
                state10,
                state30,
                sm3bucket,
                eff3bucket,
                v3bucket,
                a3bucket,
                v10bucket,
                a10bucket,
                total,
                targets,
                riskfree,
                stops,
                targetpct,
                usefulpct,
                stoppct,
                avgsectoriskfree,
                avgmaxadverse,
                avgscore,
                lift,
                createdat
            FROM public.motionfingerprint
            WHERE signalrule = %s
              AND usefulpct >= %s
              AND stoppct <= %s
              AND total >= %s
              AND avgsectoriskfree <= %s
            ORDER BY lift DESC NULLS LAST, usefulpct DESC, avgscore DESC NULLS LAST, total DESC, id ASC
            """,
            (
                signalrule,
                float(usefulpct_at_least),
                float(stoppct_at_most),
                int(total_at_least),
                float(avgsectoriskfree_at_most),
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def load_signal_outcome_comparison(
    conn: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT signalrule, side, outcome, count(*) AS total
            FROM public.motionsignal
            WHERE timestamp >= %s
              AND timestamp <= %s
            GROUP BY signalrule, side, outcome
            ORDER BY signalrule ASC, side ASC, outcome ASC
            """,
            (start_ts, end_ts),
        )
        return [dict(row) for row in cur.fetchall()]


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
    config: SignalGenerationConfig,
    allowed_fingerprints: Optional[Set[FingerprintKey]] = None,
) -> Optional[PendingSignal]:
    state3_raw = points.get(3, {}).get("motionstate")
    state10_raw = points.get(10, {}).get("motionstate")
    state30_raw = points.get(30, {}).get("motionstate")
    state3 = _normalize_text(state3_raw)
    state10 = _normalize_text(state10_raw)
    state30 = _normalize_text(state30_raw)
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
    if config.strategy == "fingerprint":
        if not allowed_fingerprints:
            return None
        buy_key = build_fingerprint_key(
            side="buy",
            state3=state3,
            state10=state10,
            state30=state30,
            spreadmultiple3=spreadmultiple3,
            efficiency3=efficiency3,
            velocity3=velocity3,
            acceleration3=acceleration3,
            velocity10=velocity10,
            acceleration10=acceleration10,
        )
        sell_key = build_fingerprint_key(
            side="sell",
            state3=state3,
            state10=state10,
            state30=state30,
            spreadmultiple3=spreadmultiple3,
            efficiency3=efficiency3,
            velocity3=velocity3,
            acceleration3=acceleration3,
            velocity10=velocity10,
            acceleration10=acceleration10,
        )
        buy_match = buy_key in allowed_fingerprints if buy_key is not None else False
        sell_match = sell_key in allowed_fingerprints if sell_key is not None else False
        if buy_match == sell_match:
            return None
        side = "buy" if buy_match else "sell"
    else:
        if config.min_efficiency3 is not None and (efficiency3 is None or efficiency3 < config.min_efficiency3):
            return None
        if config.min_spreadmultiple3 is not None and (spreadmultiple3 is None or spreadmultiple3 < config.min_spreadmultiple3):
            return None
        if config.max_spreadmultiple3 is not None and (spreadmultiple3 is None or spreadmultiple3 > config.max_spreadmultiple3):
            return None
        if config.require_state10 is not None and state10 != config.require_state10:
            return None
        if config.require_state30 is not None and state30 != config.require_state30:
            return None
        allowed_state3 = _allowed_state3_values(config)
        if state3 is None or state3 not in allowed_state3:
            return None
        side = _side_from_state3(state3)
        if side is None:
            return None
        if config.strategy == "continuation":
            if velocity10 is None or acceleration10 is None:
                return None
            if side == "buy" and (velocity10 <= 0 or acceleration10 < -SIGNAL_ACCEL_EPSILON):
                return None
            if side == "sell" and (velocity10 >= 0 or acceleration10 > SIGNAL_ACCEL_EPSILON):
                return None
        elif config.strategy == "micro_burst":
            if velocity3 is None or acceleration3 is None or velocity10 is None:
                return None
            if side == "buy" and (velocity3 <= 0 or acceleration3 <= 0):
                return None
            if side == "sell" and (velocity3 >= 0 or acceleration3 >= 0):
                return None
            if config.velocity10_ratio_max is not None:
                if abs(velocity3) <= 0:
                    return None
                if abs(velocity10) > abs(velocity3) * float(config.velocity10_ratio_max):
                    return None
        else:
            raise ValueError("unsupported signal strategy: {0}".format(config.strategy))

    previous_signal_at = last_signal_at.get(side)
    cooldown_seconds = int(config.cooldownsec)
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
            state3=state3_raw,
            velocity10=velocity10,
            acceleration10=acceleration10,
            efficiency10=_safe_float(points.get(10, {}).get("efficiency")),
            spreadmultiple10=_safe_float(points.get(10, {}).get("spreadmultiple")),
            state10=state10_raw,
            velocity30=_safe_float(points.get(30, {}).get("velocity")),
            acceleration30=_safe_float(points.get(30, {}).get("acceleration")),
            efficiency30=_safe_float(points.get(30, {}).get("efficiency")),
            spreadmultiple30=_safe_float(points.get(30, {}).get("spreadmultiple")),
            state30=state30_raw,
            riskfreeprice=ask + config.riskfreeusd,
            stopprice=ask - config.stopusd,
            targetprice=ask + config.targetusd,
            lookaheadsec=config.lookaheadsec,
            signalrule=config.signalrule,
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
            state3=state3_raw,
            velocity10=velocity10,
            acceleration10=acceleration10,
            efficiency10=_safe_float(points.get(10, {}).get("efficiency")),
            spreadmultiple10=_safe_float(points.get(10, {}).get("spreadmultiple")),
            state10=state10_raw,
            velocity30=_safe_float(points.get(30, {}).get("velocity")),
            acceleration30=_safe_float(points.get(30, {}).get("acceleration")),
            efficiency30=_safe_float(points.get(30, {}).get("efficiency")),
            spreadmultiple30=_safe_float(points.get(30, {}).get("spreadmultiple")),
            state30=state30_raw,
            riskfreeprice=bid - config.riskfreeusd,
            stopprice=bid + config.stopusd,
            targetprice=bid - config.targetusd,
            lookaheadsec=config.lookaheadsec,
            signalrule=config.signalrule,
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
    config = build_named_signal_config(signalrule)

    effective_start = start_ts - timedelta(seconds=MAX_WINDOW_SECONDS)
    evaluation_end = end_ts + timedelta(seconds=config.lookaheadsec)

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
                    config=config,
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


def flush_signal_rows(
    conn: Any,
    *,
    rows: List[Dict[str, Any]],
    summary: Optional[SignalSummaryAggregate] = None,
) -> None:
    if not rows:
        return
    if summary is not None:
        for row in rows:
            summary.observe(row)
    insert_signals(conn, rows)
    rows.clear()


def recreate_signals_for_config(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
    config: SignalGenerationConfig,
    allowed_fingerprints: Optional[Set[FingerprintKey]] = None,
) -> Dict[str, Any]:
    start_ts = _as_utc(start_ts)
    end_ts = _as_utc(end_ts)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        raise ValueError("invalid recreate-signals time range")

    evaluation_end = end_ts + timedelta(seconds=config.lookaheadsec)
    delete_signal_range(conn, start_ts=start_ts, end_ts=end_ts, signalrule=config.signalrule)

    last_signal_at = load_signal_cooldowns(conn, before_ts=start_ts, signalrule=config.signalrule)
    pending_signals: Deque[PendingSignal] = deque()
    pending_signal_rows: List[Dict[str, Any]] = []
    summary = SignalSummaryAggregate()
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
                    config=config,
                    allowed_fingerprints=allowed_fingerprints,
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
                flush_signal_rows(conn, rows=pending_signal_rows, summary=summary)

    while pending_signals:
        pending_signal_rows.append(pending_signals.popleft().finalize_row())
        signal_count += 1

    flush_signal_rows(conn, rows=pending_signal_rows, summary=summary)
    return {
        "symbol": symbol,
        "signalrule": config.signalrule,
        "start": start_ts,
        "end": end_ts,
        "tickcount": tickcount,
        "motionsignal_count": signal_count,
        "summary": summary,
    }


def analyze_winning_fingerprints(
    conn: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
    source_signalrule: str,
    fingerprint_signalrule: str,
) -> Dict[str, Any]:
    start_ts = _as_utc(start_ts)
    end_ts = _as_utc(end_ts)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        raise ValueError("invalid analyze-winners time range")

    aggregates: Dict[FingerprintKey, FingerprintAggregate] = {}
    total_signals = 0
    useful_signals = 0

    for batch in iter_signals_between(
        conn,
        start_ts=start_ts,
        end_ts=end_ts,
        signalrule=source_signalrule,
        batch_size=batch_size,
    ):
        for row in batch:
            key = build_fingerprint_key(
                side=row.get("side"),
                state3=row.get("state3"),
                state10=row.get("state10"),
                state30=row.get("state30"),
                spreadmultiple3=row.get("spreadmultiple3"),
                efficiency3=row.get("efficiency3"),
                velocity3=row.get("velocity3"),
                acceleration3=row.get("acceleration3"),
                velocity10=row.get("velocity10"),
                acceleration10=row.get("acceleration10"),
            )
            if key is None:
                continue
            total_signals += 1
            outcome = str(row.get("outcome") or "").strip().lower()
            if outcome in {"target_before_stop", "riskfree_before_stop"}:
                useful_signals += 1
            aggregates.setdefault(key, FingerprintAggregate()).observe(row)

    baseline_useful_pct: Optional[float] = None
    if total_signals > 0:
        baseline_useful_pct = float(useful_signals * 100.0 / total_signals)

    all_rows = [
        aggregate.as_row(
            signalrule=fingerprint_signalrule,
            key=key,
            baseline_useful_pct=baseline_useful_pct,
        )
        for key, aggregate in aggregates.items()
    ]
    all_rows.sort(
        key=lambda row: (
            float(row.get("lift") or -1e9),
            float(row.get("usefulpct") or -1e9),
            float(row.get("avgscore") or -1e9),
            int(row.get("total") or 0),
            int(row.get("targets") or 0),
        ),
        reverse=True,
    )

    delete_motionfingerprints(conn, signalrule=fingerprint_signalrule)
    insert_motionfingerprints(conn, all_rows)

    ranked_rows = [row for row in all_rows if int(row.get("total") or 0) >= MIN_FINGERPRINT_SIGNALS]
    return {
        "source_signalrule": source_signalrule,
        "fingerprint_signalrule": fingerprint_signalrule,
        "start": start_ts,
        "end": end_ts,
        "baseline_usefulpct": baseline_useful_pct,
        "signals": total_signals,
        "fingerprint_rows": len(all_rows),
        "ranked_rows": ranked_rows,
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
    selected_fingerprint_rows: List[Dict[str, Any]] = []
    allowed_fingerprints: Optional[Set[FingerprintKey]] = None
    config = build_named_signal_config(signalrule)
    if signalrule == BEST_FINGERPRINT_SIGNAL_RULE:
        selected_fingerprint_rows = load_top_motionfingerprints(conn, signalrule=signalrule)
        allowed_fingerprints = {
            FingerprintKey(
                side=str(row.get("side") or "").strip().lower(),
                state3=_normalize_text(row.get("state3")),
                state10=_normalize_text(row.get("state10")),
                state30=_normalize_text(row.get("state30")),
                sm3bucket=row.get("sm3bucket"),
                eff3bucket=row.get("eff3bucket"),
                v3bucket=row.get("v3bucket"),
                a3bucket=row.get("a3bucket"),
                v10bucket=row.get("v10bucket"),
                a10bucket=row.get("a10bucket"),
            )
            for row in selected_fingerprint_rows
            if str(row.get("side") or "").strip().lower() in {"buy", "sell"}
        }
    result = recreate_signals_for_config(
        conn,
        symbol=symbol,
        start_ts=start_ts,
        end_ts=end_ts,
        batch_size=batch_size,
        config=config,
        allowed_fingerprints=allowed_fingerprints,
    )
    return {
        "symbol": result["symbol"],
        "signalrule": result["signalrule"],
        "start": result["start"],
        "end": result["end"],
        "tickcount": result["tickcount"],
        "motionsignal_count": result["motionsignal_count"],
        "selected_fingerprint_count": len(allowed_fingerprints or set()),
        "comparison_rows": load_signal_outcome_comparison(conn, start_ts=result["start"], end_ts=result["end"]),
    }


def _rank_motion_model_row(row: Dict[str, Any]) -> Any:
    usefulpct = _safe_float(row.get("usefulpct"))
    avg_seconds = _safe_float(row.get("avgsecondstoriskfree"))
    avg_maxadverse = _safe_float(row.get("avgmaxadverse"))
    signals = _safe_int(row.get("signals"))
    return (
        0 if bool(row.get("passedconstraints")) else 1,
        -(usefulpct if usefulpct is not None else -1e9),
        avg_seconds if avg_seconds is not None else float("inf"),
        avg_maxadverse if avg_maxadverse is not None else float("inf"),
        -signals,
        str(row.get("signalrule") or ""),
    )


def run_motion_model_scenarios(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
) -> Dict[str, Any]:
    start_ts = _as_utc(start_ts)
    end_ts = _as_utc(end_ts)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        raise ValueError("invalid run-scenarios time range")

    scenarios = load_active_motion_model_scenarios(conn)
    ranked_rows: List[Dict[str, Any]] = []
    if not scenarios:
        return {
            "symbol": symbol,
            "start": start_ts,
            "end": end_ts,
            "scenario_count": 0,
            "ranked_rows": ranked_rows,
        }

    for index, scenario in enumerate(scenarios, start=1):
        result = recreate_signals_for_config(
            conn,
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            batch_size=batch_size,
            config=scenario.signal_config(),
        )
        delete_motionmodel_results(conn, scenarioid=scenario.id, fromts=start_ts, tots=end_ts)
        result_row = result["summary"].as_result_row(
            scenarioid=scenario.id,
            signalrule=scenario.signalrule,
            fromts=start_ts,
            tots=end_ts,
            riskfreeusd=scenario.riskfreeusd,
            targetusd=scenario.targetusd,
            stopusd=scenario.stopusd,
        )
        insert_motionmodel_results(conn, [result_row])
        conn.commit()
        ranked_rows.append(
            {
                "scenarioid": scenario.id,
                "scenarioname": scenario.scenarioname,
                "family": scenario.family,
                **result_row,
            }
        )
        if index == 1 or index % 25 == 0 or index == len(scenarios):
            _print(
                "scenario_progress={0}/{1} signalrule={2} signals={3} usefulpct={4} passed={5}".format(
                    index,
                    len(scenarios),
                    scenario.signalrule,
                    result_row["signals"],
                    _format_metric(_safe_float(result_row.get("usefulpct"))),
                    "yes" if result_row["passedconstraints"] else "no",
                )
            )

    ranked_rows.sort(key=_rank_motion_model_row)
    return {
        "symbol": symbol,
        "start": start_ts,
        "end": end_ts,
        "scenario_count": len(scenarios),
        "ranked_rows": ranked_rows,
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


def print_ranked_fingerprints(result: Dict[str, Any]) -> None:
    _print(
        "source_rule={0} fingerprint_rule={1} start={2} end={3} signals={4} baseline_usefulpct={5} fingerprints={6}".format(
            result["source_signalrule"],
            result["fingerprint_signalrule"],
            result["start"].isoformat(),
            result["end"].isoformat(),
            result["signals"],
            _format_metric(result.get("baseline_usefulpct")),
            result["fingerprint_rows"],
        )
    )
    rows = list(result.get("ranked_rows") or [])
    if not rows:
        _print("No fingerprints met the minimum signal threshold.")
        return
    _print(
        "\t".join(
            [
                "rank",
                "side",
                "state3",
                "state10",
                "state30",
                "sm3",
                "eff3",
                "v3",
                "a3",
                "v10",
                "a10",
                "total",
                "targets",
                "riskfree",
                "stops",
                "target_pct",
                "useful_pct",
                "stop_pct",
                "avg_seconds_to_riskfree",
                "avg_maxadverse",
                "avg_score",
                "lift",
            ]
        )
    )
    for index, row in enumerate(rows, start=1):
        _print(
            "\t".join(
                [
                    str(index),
                    str(row.get("side") or "-"),
                    str(row.get("state3") or "-"),
                    str(row.get("state10") or "-"),
                    str(row.get("state30") or "-"),
                    str(row.get("sm3bucket") if row.get("sm3bucket") is not None else "-"),
                    str(row.get("eff3bucket") if row.get("eff3bucket") is not None else "-"),
                    str(row.get("v3bucket") if row.get("v3bucket") is not None else "-"),
                    str(row.get("a3bucket") if row.get("a3bucket") is not None else "-"),
                    str(row.get("v10bucket") if row.get("v10bucket") is not None else "-"),
                    str(row.get("a10bucket") if row.get("a10bucket") is not None else "-"),
                    str(row.get("total") or 0),
                    str(row.get("targets") or 0),
                    str(row.get("riskfree") or 0),
                    str(row.get("stops") or 0),
                    _format_metric(_safe_float(row.get("targetpct"))),
                    _format_metric(_safe_float(row.get("usefulpct"))),
                    _format_metric(_safe_float(row.get("stoppct"))),
                    _format_metric(_safe_float(row.get("avgsectoriskfree"))),
                    _format_metric(_safe_float(row.get("avgmaxadverse"))),
                    _format_metric(_safe_float(row.get("avgscore"))),
                    _format_metric(_safe_float(row.get("lift"))),
                ]
            )
        )


def print_signal_outcome_comparison(rows: Sequence[Dict[str, Any]]) -> None:
    _print("signalrule\tside\toutcome\ttotal")
    for row in rows:
        _print(
            "\t".join(
                [
                    str(row.get("signalrule") or "-"),
                    str(row.get("side") or "-"),
                    str(row.get("outcome") or "-"),
                    str(row.get("total") or 0),
                ]
            )
        )


def print_ranked_motion_model_results(result: Dict[str, Any]) -> None:
    rows = list(result.get("ranked_rows") or [])
    _print(
        "symbol={0} start={1} end={2} scenarios={3}".format(
            result.get("symbol") or DEFAULT_SYMBOL,
            result["start"].isoformat(),
            result["end"].isoformat(),
            result.get("scenario_count") or 0,
        )
    )
    if not rows:
        _print("No active scenarios were available.")
        return
    _print(
        "\t".join(
            [
                "rank",
                "pass",
                "scenario",
                "family",
                "signalrule",
                "signals",
                "targets",
                "riskfree",
                "stops",
                "nodecision",
                "target_pct",
                "useful_pct",
                "stop_pct",
                "avg_seconds_to_riskfree",
                "avg_maxadverse",
                "avg_score",
                "profit_proxy",
            ]
        )
    )
    for index, row in enumerate(rows, start=1):
        _print(
            "\t".join(
                [
                    str(index),
                    "yes" if bool(row.get("passedconstraints")) else "no",
                    str(row.get("scenarioname") or "-"),
                    str(row.get("family") or "-"),
                    str(row.get("signalrule") or "-"),
                    str(row.get("signals") or 0),
                    str(row.get("targets") or 0),
                    str(row.get("riskfree") or 0),
                    str(row.get("stops") or 0),
                    str(row.get("nodecision") or 0),
                    _format_metric(_safe_float(row.get("targetpct"))),
                    _format_metric(_safe_float(row.get("usefulpct"))),
                    _format_metric(_safe_float(row.get("stoppct"))),
                    _format_metric(_safe_float(row.get("avgsecondstoriskfree"))),
                    _format_metric(_safe_float(row.get("avgmaxadverse"))),
                    _format_metric(_safe_float(row.get("avgscore"))),
                    _format_metric(_safe_float(row.get("profitproxy"))),
                ]
            )
        )


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

    analyze = subparsers.add_parser("analyze-winners", help="Rank high-value fingerprints from recent motionsignal rows.")
    add_range_arguments(analyze)
    analyze.add_argument(
        "--source-rule",
        default=DEFAULT_SIGNAL_RULE,
        choices=sorted({DEFAULT_SIGNAL_RULE, MICRO_BURST_SIGNAL_RULE, BEST_FINGERPRINT_SIGNAL_RULE}),
        help="Existing motionsignal rule to analyze as the source population.",
    )
    analyze.add_argument(
        "--fingerprint-rule",
        default=BEST_FINGERPRINT_SIGNAL_RULE,
        help="Fingerprint rule name to write into public.motionfingerprint.",
    )

    recreate_signals = subparsers.add_parser("recreate-signals", help="Rebuild motionsignal rows from existing motionpoint rows for a time range.")
    add_range_arguments(recreate_signals)
    recreate_signals.add_argument("--rule", required=True, choices=sorted(SIGNAL_RULE_COOLDOWN_SECONDS), help="Signal rule to recreate.")

    run_scenarios = subparsers.add_parser("run-scenarios", help="Run active PostgreSQL motion-model research scenarios over a time range.")
    add_range_arguments(run_scenarios)
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
        if args.command == "analyze-winners":
            result = analyze_winning_fingerprints(
                conn,
                start_ts=target_range.start,
                end_ts=target_range.end,
                batch_size=batch_size,
                source_signalrule=str(args.source_rule),
                fingerprint_signalrule=str(args.fingerprint_rule),
            )
            conn.commit()
            print_ranked_fingerprints(result)
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
            if result["signalrule"] == BEST_FINGERPRINT_SIGNAL_RULE:
                _print("selected_fingerprints={0}".format(result["selected_fingerprint_count"]))
            print_signal_outcome_comparison(result.get("comparison_rows") or [])
            return 0
        if args.command == "run-scenarios":
            result = run_motion_model_scenarios(
                conn,
                symbol=symbol,
                start_ts=target_range.start,
                end_ts=target_range.end,
                batch_size=batch_size,
            )
            print_ranked_motion_model_results(result)
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
