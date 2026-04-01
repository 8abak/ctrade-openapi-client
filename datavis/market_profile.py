from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo


PROFILE_SOURCES = ("ask", "bid", "mid")
DEFAULT_PROFILE_SOURCE = "mid"
DEFAULT_PROFILE_BIN_SIZE = 0.10
DEFAULT_PROFILE_MAX_GAP_MS = 1500
DEFAULT_PROFILE_VALUE_AREA = 0.70
DEFAULT_PROFILE_NODE_LIMIT = 3
DEFAULT_PROFILE_TIMEZONE = "Australia/Sydney"
DEFAULT_PROFILE_SESSION_HOUR = 8


def safe_float_token(value: float) -> str:
    return ("{0:.8f}".format(float(value))).rstrip("0").rstrip(".") or "0"


@dataclass(frozen=True)
class MarketProfileConfig:
    source: str = DEFAULT_PROFILE_SOURCE
    binsize: float = DEFAULT_PROFILE_BIN_SIZE
    maxgapms: int = DEFAULT_PROFILE_MAX_GAP_MS
    timezone_name: str = DEFAULT_PROFILE_TIMEZONE
    sessionstarthour: int = DEFAULT_PROFILE_SESSION_HOUR
    valueareapercent: float = DEFAULT_PROFILE_VALUE_AREA
    nodelimit: int = DEFAULT_PROFILE_NODE_LIMIT

    def normalized(self) -> "MarketProfileConfig":
        source = (self.source or DEFAULT_PROFILE_SOURCE).lower()
        binsize = float(self.binsize)
        maxgapms = max(1, int(self.maxgapms))
        timezone_name = str(self.timezone_name or DEFAULT_PROFILE_TIMEZONE)
        sessionstarthour = max(0, min(23, int(self.sessionstarthour)))
        valueareapercent = float(self.valueareapercent)
        nodelimit = max(1, int(self.nodelimit))
        if source not in PROFILE_SOURCES:
            raise ValueError("Unsupported market profile source: {0}".format(source))
        if binsize <= 0:
            raise ValueError("Market profile bin size must be greater than zero.")
        if not 0 < valueareapercent <= 1:
            raise ValueError("Market profile value area percent must be between 0 and 1.")
        return MarketProfileConfig(
            source=source,
            binsize=binsize,
            maxgapms=maxgapms,
            timezone_name=timezone_name,
            sessionstarthour=sessionstarthour,
            valueareapercent=valueareapercent,
            nodelimit=nodelimit,
        )

    def key(self) -> str:
        return "{0}:{1}:{2}".format(
            self.source,
            safe_float_token(self.binsize),
            self.maxgapms,
        )

    def worker_job_name(self, symbol: str) -> str:
        return "marketprofile:{0}:{1}:worker".format(symbol, self.key())

    def backfill_job_name(self, symbol: str, range_token: str) -> str:
        return "marketprofile:{0}:{1}:backfill:{2}".format(symbol, self.key(), range_token)


def select_profile_price(row: Dict[str, Any], source: str) -> float:
    source = source.lower()
    if source == "ask":
        value = row.get("ask")
    elif source == "bid":
        value = row.get("bid")
    else:
        value = row.get("mid")
        if value is None:
            bid = row.get("bid")
            ask = row.get("ask")
            value = ((float(bid) + float(ask)) / 2.0) if bid is not None and ask is not None else row.get("price")
    if value is None:
        raise ValueError("Missing market profile price for tick {0}".format(row.get("id")))
    return float(value)


def price_bin(price: float, binsize: float) -> float:
    size = Decimal(str(binsize))
    scaled = Decimal(str(price)) / size
    return float(scaled.to_integral_value(rounding=ROUND_HALF_UP) * size)


def session_bounds(timestamp_value: datetime, config: MarketProfileConfig) -> Tuple[datetime, datetime]:
    config = config.normalized()
    zone = ZoneInfo(config.timezone_name)
    current = timestamp_value if timestamp_value.tzinfo is not None else timestamp_value.replace(tzinfo=timezone.utc)
    local = current.astimezone(zone)
    start_date = local.date()
    if (local.hour, local.minute, local.second, local.microsecond) < (config.sessionstarthour, 0, 0, 0):
        start_date = start_date - timedelta(days=1)
    start_local = datetime.combine(start_date, dt_time(hour=config.sessionstarthour), tzinfo=zone)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def session_label(session_start: datetime, config: MarketProfileConfig) -> str:
    zone = ZoneInfo(config.timezone_name)
    return session_start.astimezone(zone).strftime("%Y-%m-%d")


def pending_tick_to_state(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    timestamp_value = row.get("timestamp")
    return {
        "id": int(row["id"]),
        "symbol": row["symbol"],
        "timestamp": timestamp_value.isoformat() if isinstance(timestamp_value, datetime) else str(timestamp_value),
        "bid": row.get("bid"),
        "ask": row.get("ask"),
        "mid": row.get("mid"),
        "price": row.get("price"),
    }


def pending_tick_from_state(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    return {
        "id": int(payload["id"]),
        "symbol": payload["symbol"],
        "timestamp": datetime.fromisoformat(str(payload["timestamp"])),
        "bid": payload.get("bid"),
        "ask": payload.get("ask"),
        "mid": payload.get("mid"),
        "price": payload.get("price"),
    }


class MarketProfileProcessor:
    def __init__(self, config: MarketProfileConfig, state: Optional[Dict[str, Any]] = None):
        self.config = config.normalized()
        self.pending_tick: Optional[Dict[str, Any]] = None
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.pending_tick = pending_tick_from_state(state.get("pendingtick"))

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "pendingtick": pending_tick_to_state(self.pending_tick),
        }

    def current_pending_session(self) -> Optional[Dict[str, Any]]:
        if not self.pending_tick:
            return None
        session_start, session_end = session_bounds(self.pending_tick["timestamp"], self.config)
        return {
            "sessionstart": session_start,
            "sessionend": session_end,
            "sessionlabel": session_label(session_start, self.config),
            "tickid": int(self.pending_tick["id"]),
            "timestamp": self.pending_tick["timestamp"],
        }

    def process_tick(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.pending_tick is None:
            self.pending_tick = dict(row)
            return None

        current_tick = dict(row)
        closed_tick = self.pending_tick
        self.pending_tick = current_tick

        session_start, session_end = session_bounds(closed_tick["timestamp"], self.config)
        dwell_end = min(current_tick["timestamp"], session_end)
        delta_ms = max(0, int((dwell_end - closed_tick["timestamp"]).total_seconds() * 1000.0))
        weight_ms = min(delta_ms, self.config.maxgapms)
        price = select_profile_price(closed_tick, self.config.source)
        session_changed = current_tick["timestamp"] >= session_end

        return {
            "symbol": closed_tick["symbol"],
            "sessionstart": session_start,
            "sessionend": session_end,
            "sessionlabel": session_label(session_start, self.config),
            "tickid": int(closed_tick["id"]),
            "nexttickid": int(current_tick["id"]),
            "timestamp": closed_tick["timestamp"],
            "price": price,
            "pricebin": price_bin(price, self.config.binsize),
            "tickcount": 1,
            "weightms": weight_ms,
            "sessionchanged": session_changed,
        }


def compute_profile_metrics(
    bins: Sequence[Dict[str, Any]],
    *,
    binsize: float,
    valueareapercent: float,
    nodelimit: int,
) -> Dict[str, Any]:
    ordered = sorted(
        [
            {
                "pricebin": float(row["pricebin"]),
                "weightms": float(row.get("weightms") or 0),
                "tickcount": int(row.get("tickcount") or 0),
            }
            for row in bins
        ],
        key=lambda row: row["pricebin"],
    )
    total_weight = sum(row["weightms"] for row in ordered)
    total_ticks = sum(row["tickcount"] for row in ordered)
    if not ordered or total_weight <= 0:
        return {
            "totalweightms": total_weight,
            "totalticks": total_ticks,
            "poc": None,
            "vah": None,
            "val": None,
            "hvns": [],
            "lvns": [],
            "flags": {},
        }

    poc_index = max(
        range(len(ordered)),
        key=lambda index: (ordered[index]["weightms"], ordered[index]["tickcount"], -ordered[index]["pricebin"]),
    )
    included = {poc_index}
    left = poc_index
    right = poc_index
    covered = ordered[poc_index]["weightms"]
    target = total_weight * float(valueareapercent)

    while covered < target and (left > 0 or right < len(ordered) - 1):
        down = ordered[left - 1]["weightms"] if left > 0 else None
        up = ordered[right + 1]["weightms"] if right < len(ordered) - 1 else None
        if down is None:
            right += 1
            included.add(right)
            covered += ordered[right]["weightms"]
            continue
        if up is None:
            left -= 1
            included.add(left)
            covered += ordered[left]["weightms"]
            continue
        if up >= down:
            right += 1
            included.add(right)
            covered += ordered[right]["weightms"]
        else:
            left -= 1
            included.add(left)
            covered += ordered[left]["weightms"]

    poc_price = ordered[poc_index]["pricebin"]
    vah_price = ordered[max(included)]["pricebin"]
    val_price = ordered[min(included)]["pricebin"]
    average_weight = total_weight / float(len(ordered))

    hvn_candidates: List[Dict[str, Any]] = []
    lvn_candidates: List[Dict[str, Any]] = []
    for index, row in enumerate(ordered):
        prev_weight = ordered[index - 1]["weightms"] if index > 0 else row["weightms"]
        next_weight = ordered[index + 1]["weightms"] if index < len(ordered) - 1 else row["weightms"]
        is_peak = (row["weightms"] > prev_weight and row["weightms"] >= next_weight) or (
            row["weightms"] >= prev_weight and row["weightms"] > next_weight
        )
        is_valley = (row["weightms"] < prev_weight and row["weightms"] <= next_weight) or (
            row["weightms"] <= prev_weight and row["weightms"] < next_weight
        )
        zone = {
            "price": row["pricebin"],
            "low": row["pricebin"] - (binsize / 2.0),
            "high": row["pricebin"] + (binsize / 2.0),
            "weightms": row["weightms"],
            "tickcount": row["tickcount"],
        }
        if is_peak and row["weightms"] >= average_weight and row["pricebin"] != poc_price:
            hvn_candidates.append(zone)
        if is_valley and row["weightms"] <= average_weight:
            lvn_candidates.append(zone)

    hvns = sorted(hvn_candidates, key=lambda row: (-row["weightms"], abs(row["price"] - poc_price), row["price"]))[:nodelimit]
    lvns = sorted(lvn_candidates, key=lambda row: (row["weightms"], abs(row["price"] - poc_price), row["price"]))[:nodelimit]

    flags: Dict[float, Dict[str, Any]] = {}
    for row in ordered:
        price_value = row["pricebin"]
        flags[price_value] = {
            "ispoc": price_value == poc_price,
            "isvah": price_value == vah_price,
            "isval": price_value == val_price,
            "ishvn": any(node["price"] == price_value for node in hvns),
            "islvn": any(node["price"] == price_value for node in lvns),
        }

    return {
        "totalweightms": total_weight,
        "totalticks": total_ticks,
        "poc": poc_price,
        "vah": vah_price,
        "val": val_price,
        "hvns": hvns,
        "lvns": lvns,
        "flags": flags,
    }
