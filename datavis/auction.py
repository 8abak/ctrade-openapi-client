from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras


PRICE_BIN_SIZE = 0.10
CONTEXT_SECONDS = 48 * 60 * 60
DWELL_CAP_MS = 4000
SNAPSHOT_VALUE_PERCENT = 0.70
PERSIST_INTERVAL_SECONDS = 15.0

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
LONDON_TZ = ZoneInfo("Europe/London")
NEW_YORK_TZ = ZoneInfo("America/New_York")

ACTIVE_SESSION_KINDS = {"brokerday", "london", "newyork"}
HISTORY_SESSION_KINDS = ("brokerday", "london", "newyork")
WINDOW_ORDER = ["rolling15m", "rolling60m", "rolling240m", "session", "rolling24h"]
WINDOW_LABELS = {
    "rolling15m": "15m",
    "rolling60m": "60m",
    "rolling240m": "240m",
    "rolling24h": "24h",
    "brokerday": "Broker Day",
    "london": "London Session",
    "newyork": "New York Session",
    "session": "Session",
}
ROLLING_WINDOWS = {
    "rolling15m": 15 * 60,
    "rolling60m": 60 * 60,
    "rolling240m": 240 * 60,
    "rolling24h": 24 * 60 * 60,
}
HISTORY_STATE_RETENTION_DAYS = 14


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def ms_to_dt(value: Optional[int]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))


def round_price(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 2)


def bin_index(price: float) -> int:
    return int(round(float(price) / PRICE_BIN_SIZE))


def bin_price(index: int) -> float:
    return round(float(index) * PRICE_BIN_SIZE, 2)


def normalize_tick_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        tick_id = int(row["id"])
        timestamp = row["timestamp"]
    except Exception:
        return None
    if not isinstance(timestamp, datetime):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    bid = float(row.get("bid")) if row.get("bid") is not None else None
    ask = float(row.get("ask")) if row.get("ask") is not None else None
    mid = float(row.get("mid")) if row.get("mid") is not None else None
    if mid is None and bid is not None and ask is not None:
        mid = (bid + ask) / 2.0
    if mid is None:
        return None
    spread = float(row.get("spread")) if row.get("spread") is not None else None
    if spread is None and bid is not None and ask is not None:
        spread = max(0.0, ask - bid)
    spread = max(0.0, float(spread or 0.0))
    return {
        "id": tick_id,
        "timestamp": timestamp,
        "timestampMs": dt_to_ms(timestamp),
        "bid": bid,
        "ask": ask,
        "mid": float(mid),
        "spread": spread,
        "binIndex": bin_index(mid),
    }


@dataclass
class BucketBin:
    tick_count: int = 0
    time_ms: int = 0
    spread_sum: float = 0.0
    delta_score: float = 0.0
    revisit_count: int = 0

    def merge(self, other: "BucketBin") -> None:
        self.tick_count += int(other.tick_count)
        self.time_ms += int(other.time_ms)
        self.spread_sum += float(other.spread_sum)
        self.delta_score += float(other.delta_score)
        self.revisit_count += int(other.revisit_count)

    def serialize(self, index: int) -> Dict[str, Any]:
        activity_score = self.tick_count + (self.time_ms / 500.0) + (self.revisit_count * 0.75) + (abs(self.delta_score) * 0.2)
        return {
            "priceBin": bin_price(index),
            "tickCount": int(self.tick_count),
            "timeMs": int(self.time_ms),
            "bidHitCount": 0,
            "askLiftCount": 0,
            "spreadSum": round(self.spread_sum, 4),
            "l2BidVol": 0,
            "l2AskVol": 0,
            "activityScore": round(activity_score, 4),
            "dwellScore": round(self.time_ms / 1000.0, 4),
            "deltaScore": round(self.delta_score, 4),
            "revisitCount": int(self.revisit_count),
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "BucketBin":
        return cls(
            tick_count=int(payload.get("tickCount") or 0),
            time_ms=int(payload.get("timeMs") or 0),
            spread_sum=float(payload.get("spreadSum") or 0.0),
            delta_score=float(payload.get("deltaScore") or 0.0),
            revisit_count=int(payload.get("revisitCount") or 0),
        )


@dataclass
class SecondBucket:
    timestamp_ms: int
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    tick_count: int = 0
    spread_sum: float = 0.0
    bins: Dict[int, BucketBin] = field(default_factory=dict)
    up_moves: int = 0
    down_moves: int = 0
    transition_count: int = 0
    last_bin_index: Optional[int] = None
    last_price: Optional[float] = None
    _seen_bins: set[int] = field(default_factory=set, repr=False)

    def register_tick(self, *, price: float, spread: float, delta: float, bucket_bin_index: int) -> None:
        if self.tick_count == 0:
            self.open_price = price
            self.high_price = price
            self.low_price = price
            self.close_price = price
            self.last_price = price
        else:
            self.high_price = max(self.high_price, price)
            self.low_price = min(self.low_price, price)
            if delta > 0:
                self.up_moves += 1
            elif delta < 0:
                self.down_moves += 1
            if self.last_bin_index is not None and self.last_bin_index != bucket_bin_index:
                self.transition_count += 1
        entry = self.bins.setdefault(bucket_bin_index, BucketBin())
        entry.tick_count += 1
        entry.spread_sum += float(spread)
        entry.delta_score += float(delta)
        if self.last_bin_index is not None and self.last_bin_index != bucket_bin_index and bucket_bin_index in self._seen_bins:
            entry.revisit_count += 1
        self._seen_bins.add(bucket_bin_index)
        self.tick_count += 1
        self.spread_sum += float(spread)
        self.close_price = price
        self.last_bin_index = bucket_bin_index
        self.last_price = price

    def add_dwell(self, bucket_bin_index: int, dwell_ms: int) -> None:
        entry = self.bins.setdefault(bucket_bin_index, BucketBin())
        entry.time_ms += max(0, int(dwell_ms))

    def serialize(self) -> Dict[str, Any]:
        return {
            "timestampMs": int(self.timestamp_ms),
            "timestamp": self.timestamp.isoformat(),
            "openPrice": round_price(self.open_price),
            "highPrice": round_price(self.high_price),
            "lowPrice": round_price(self.low_price),
            "closePrice": round_price(self.close_price),
            "tickCount": int(self.tick_count),
            "spreadSum": round(self.spread_sum, 4),
            "upMoves": int(self.up_moves),
            "downMoves": int(self.down_moves),
            "transitionCount": int(self.transition_count),
            "lastBinIndex": self.last_bin_index,
            "bins": [
                {"index": int(index), **entry.serialize(index)}
                for index, entry in sorted(self.bins.items(), key=lambda item: item[0])
            ],
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> Optional["SecondBucket"]:
        try:
            timestamp_ms = int(payload["timestampMs"])
        except Exception:
            return None
        timestamp = ms_to_dt(timestamp_ms)
        if timestamp is None:
            return None
        bucket = cls(
            timestamp_ms=timestamp_ms,
            timestamp=timestamp,
            open_price=float(payload.get("openPrice") or 0.0),
            high_price=float(payload.get("highPrice") or 0.0),
            low_price=float(payload.get("lowPrice") or 0.0),
            close_price=float(payload.get("closePrice") or 0.0),
            tick_count=int(payload.get("tickCount") or 0),
            spread_sum=float(payload.get("spreadSum") or 0.0),
            up_moves=int(payload.get("upMoves") or 0),
            down_moves=int(payload.get("downMoves") or 0),
            transition_count=int(payload.get("transitionCount") or 0),
            last_bin_index=payload.get("lastBinIndex"),
        )
        for item in payload.get("bins") or []:
            try:
                index = int(item["index"])
            except Exception:
                continue
            bucket.bins[index] = BucketBin.from_payload(item)
        return bucket


def activity_score(entry: BucketBin) -> float:
    return entry.tick_count + (entry.time_ms / 500.0) + (entry.revisit_count * 0.75) + (abs(entry.delta_score) * 0.2)


def aggregate_profile(buckets: List[SecondBucket]) -> Dict[int, BucketBin]:
    combined: Dict[int, BucketBin] = {}
    for bucket in buckets:
        for index, entry in bucket.bins.items():
            target = combined.setdefault(index, BucketBin())
            target.merge(entry)
    return combined


def select_window_buckets(
    buckets: List[SecondBucket],
    *,
    start_ms: int,
    end_ms: int,
) -> List[SecondBucket]:
    return [bucket for bucket in buckets if start_ms <= bucket.timestamp_ms <= end_ms]


def profile_summary(profile_bins: Dict[int, BucketBin]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not profile_bins:
        return [], {
            "pocIndex": None,
            "pocPrice": None,
            "vahIndex": None,
            "vahPrice": None,
            "valIndex": None,
            "valPrice": None,
            "valueCenter": None,
            "valueWidth": None,
            "totalActivity": 0.0,
        }

    ordered = sorted(profile_bins.items(), key=lambda item: item[0])
    activity_map = {index: activity_score(entry) for index, entry in ordered}
    total_activity = sum(activity_map.values())
    poc_index = max(activity_map.items(), key=lambda item: (item[1], item[0]))[0]
    poc_position = next((position for position, item in enumerate(ordered) if item[0] == poc_index), 0)
    included_positions = {poc_position}
    covered_activity = activity_map.get(poc_index, 0.0)
    left = poc_position - 1
    right = poc_position + 1
    target_activity = total_activity * SNAPSHOT_VALUE_PERCENT
    while covered_activity < target_activity and (left >= 0 or right < len(ordered)):
        left_score = activity_map.get(ordered[left][0], -1.0) if left >= 0 else -1.0
        right_score = activity_map.get(ordered[right][0], -1.0) if right < len(ordered) else -1.0
        if right_score > left_score:
            included_positions.add(right)
            covered_activity += right_score
            right += 1
        else:
            if left >= 0:
                included_positions.add(left)
                covered_activity += left_score
                left -= 1
            elif right < len(ordered):
                included_positions.add(right)
                covered_activity += right_score
                right += 1
            else:
                break

    included_indexes = [ordered[position][0] for position in sorted(included_positions)]
    vah_index = max(included_indexes)
    val_index = min(included_indexes)
    weighted_sum = 0.0
    for index, entry in ordered:
        weighted_sum += bin_price(index) * activity_map[index]
    value_center = (weighted_sum / total_activity) if total_activity > 0 else bin_price(poc_index)
    bins_payload = []
    for index, entry in ordered:
        item = entry.serialize(index)
        item["isPoc"] = bool(index == poc_index)
        item["inValue"] = bool(val_index <= index <= vah_index)
        bins_payload.append(item)
    return bins_payload, {
        "pocIndex": poc_index,
        "pocPrice": bin_price(poc_index),
        "vahIndex": vah_index,
        "vahPrice": bin_price(vah_index),
        "valIndex": val_index,
        "valPrice": bin_price(val_index),
        "valueCenter": round_price(value_center),
        "valueWidth": round_price(bin_price(vah_index) - bin_price(val_index)),
        "totalActivity": round(total_activity, 4),
    }


def bucket_direction_changes(buckets: List[SecondBucket]) -> Tuple[int, float, float]:
    if not buckets:
        return 0, 0.0, 0.0
    direction_changes = 0
    total_abs_move = 0.0
    net_move = round(float(buckets[-1].close_price) - float(buckets[0].open_price), 4)
    previous_delta = None
    previous_close = float(buckets[0].open_price)
    for bucket in buckets:
        current_close = float(bucket.close_price)
        delta = current_close - previous_close
        total_abs_move += abs(delta)
        current_direction = 1 if delta > 0 else (-1 if delta < 0 else 0)
        if previous_delta is not None and current_direction != 0 and previous_delta != 0 and current_direction != previous_delta:
            direction_changes += 1
        if current_direction != 0:
            previous_delta = current_direction
        previous_close = current_close
    return direction_changes, total_abs_move, net_move


def price_location_kind(current_price: float, poc_price: Optional[float], vah_price: Optional[float], val_price: Optional[float]) -> str:
    if vah_price is not None and current_price > vah_price:
        return "OutsideUpper"
    if val_price is not None and current_price < val_price:
        return "OutsideLower"
    if poc_price is None or vah_price is None or val_price is None:
        return "InValue"
    upper_mid = poc_price + max(PRICE_BIN_SIZE, (vah_price - poc_price) * 0.45)
    lower_mid = poc_price - max(PRICE_BIN_SIZE, (poc_price - val_price) * 0.45)
    if lower_mid <= current_price <= upper_mid:
        return "InValue"
    if current_price >= poc_price:
        return "AboveValue"
    return "BelowValue"


def rejection_flags(
    *,
    buckets: List[SecondBucket],
    vah_price: Optional[float],
    val_price: Optional[float],
    current_price: float,
    current_state: str,
    value_drift: float,
) -> Tuple[str, List[Dict[str, Any]]]:
    events: List[Dict[str, Any]] = []
    recent = buckets[-6:] if len(buckets) > 6 else buckets
    if not recent or vah_price is None or val_price is None:
        return "Neutral", events

    recent_high = max(bucket.high_price for bucket in recent)
    recent_low = min(bucket.low_price for bucket in recent)
    closes_above = sum(1 for bucket in recent if bucket.close_price > vah_price)
    closes_below = sum(1 for bucket in recent if bucket.close_price < val_price)

    if current_price > vah_price:
        if closes_above >= max(2, len(recent) // 2) and value_drift > 0:
            events.append({"eventKind": "AcceptedBreakoutUp", "direction": "Up", "strength": 0.72, "confirmed": True, "price1": round_price(current_price)})
            return "Accepted", events
        return "Neutral", events

    if current_price < val_price:
        if closes_below >= max(2, len(recent) // 2) and value_drift < 0:
            events.append({"eventKind": "AcceptedBreakoutDown", "direction": "Down", "strength": 0.72, "confirmed": True, "price1": round_price(current_price)})
            return "Accepted", events
        return "Neutral", events

    if recent_high > vah_price + PRICE_BIN_SIZE and current_price <= vah_price:
        events.append({"eventKind": "RejectedProbeUp", "direction": "Down", "strength": 0.68, "confirmed": True, "price1": round_price(recent_high), "price2": round_price(current_price)})
        return "Rejected", events
    if recent_low < val_price - PRICE_BIN_SIZE and current_price >= val_price:
        events.append({"eventKind": "RejectedProbeDown", "direction": "Up", "strength": 0.68, "confirmed": True, "price1": round_price(recent_low), "price2": round_price(current_price)})
        return "Rejected", events
    if current_state == "Balance":
        return "Accepted", events
    return "Neutral", events


def compute_open_type(
    *,
    kind: str,
    buckets: List[SecondBucket],
    previous_summary: Optional[Dict[str, Any]],
) -> str:
    if kind not in ACTIVE_SESSION_KINDS or len(buckets) < 3:
        return "Unknown"
    start_ms = buckets[0].timestamp_ms
    early = [bucket for bucket in buckets if bucket.timestamp_ms <= start_ms + (30 * 60 * 1000)]
    if len(early) < 3:
        return "Unknown"
    direction_changes, total_abs_move, net_move = bucket_direction_changes(early)
    price_range = max((max(bucket.high_price for bucket in early) - min(bucket.low_price for bucket in early)), PRICE_BIN_SIZE)
    if abs(net_move) >= max(price_range * 0.65, PRICE_BIN_SIZE * 3) and direction_changes <= 1:
        return "OpenDrive"
    first_third = early[: max(2, len(early) // 3)]
    last_two_thirds = early[len(first_third):]
    if first_third and last_two_thirds:
        first_move = float(first_third[-1].close_price) - float(first_third[0].open_price)
        later_move = float(last_two_thirds[-1].close_price) - float(last_two_thirds[0].open_price)
        if first_move != 0 and later_move != 0 and math.copysign(1.0, first_move) != math.copysign(1.0, later_move) and abs(later_move) > abs(first_move) * 1.35:
            return "OpenTestDrive"
    if previous_summary and previous_summary.get("vahPrice") is not None and previous_summary.get("valPrice") is not None:
        previous_vah = float(previous_summary["vahPrice"])
        previous_val = float(previous_summary["valPrice"])
        session_open = float(early[0].open_price)
        session_close = float(early[-1].close_price)
        if session_open > previous_vah and session_close < previous_vah:
            return "OpenRejectionReverse"
        if session_open < previous_val and session_close > previous_val:
            return "OpenRejectionReverse"
    if total_abs_move > 0:
        return "OpenAuction"
    return "Unknown"


def detect_excess_and_poor(
    *,
    bins_payload: List[Dict[str, Any]],
    profile_meta: Dict[str, Any],
    current_price: float,
    high_price: float,
    low_price: float,
) -> Dict[str, Any]:
    result = {
        "excessHigh": None,
        "excessLow": None,
        "poorHigh": None,
        "poorLow": None,
    }
    if not bins_payload:
        return result
    total_activity = float(profile_meta.get("totalActivity") or 0.0)
    if total_activity <= 0:
        return result
    top_bins = bins_payload[-2:]
    bottom_bins = bins_payload[:2]
    top_activity = sum(float(item.get("activityScore") or 0.0) for item in top_bins)
    bottom_activity = sum(float(item.get("activityScore") or 0.0) for item in bottom_bins)
    top_bin = bins_payload[-1]
    bottom_bin = bins_payload[0]
    if top_activity / total_activity <= 0.14 and current_price < high_price - max(PRICE_BIN_SIZE * 3, (high_price - low_price) * 0.15):
        result["excessHigh"] = round_price(high_price)
    elif top_bin.get("tickCount", 0) >= 2 and top_activity / total_activity >= 0.18:
        result["poorHigh"] = round_price(high_price)
    if bottom_activity / total_activity <= 0.14 and current_price > low_price + max(PRICE_BIN_SIZE * 3, (high_price - low_price) * 0.15):
        result["excessLow"] = round_price(low_price)
    elif bottom_bin.get("tickCount", 0) >= 2 and bottom_activity / total_activity >= 0.18:
        result["poorLow"] = round_price(low_price)
    return result


def balance_scores(
    *,
    buckets: List[SecondBucket],
    early_buckets: List[SecondBucket],
    late_buckets: List[SecondBucket],
    profile_meta: Dict[str, Any],
    current_price: float,
) -> Dict[str, Any]:
    if not buckets:
        return {
            "balanceScore": 0.0,
            "trendScore": 0.0,
            "transitionScore": 0.0,
            "stateKind": "Transition",
            "valueDrift": 0.0,
            "directionChanges": 0,
            "netMove": 0.0,
            "totalAbsMove": 0.0,
        }

    direction_changes, total_abs_move, net_move = bucket_direction_changes(buckets)
    high_price = max(bucket.high_price for bucket in buckets)
    low_price = min(bucket.low_price for bucket in buckets)
    price_range = max(high_price - low_price, PRICE_BIN_SIZE)
    value_width = max(float(profile_meta.get("valueWidth") or PRICE_BIN_SIZE), PRICE_BIN_SIZE)
    current_center = float(profile_meta.get("valueCenter") or current_price)
    early_profile = profile_summary(aggregate_profile(early_buckets))[1] if early_buckets else {}
    late_profile = profile_summary(aggregate_profile(late_buckets))[1] if late_buckets else {}
    early_center = float(early_profile.get("valueCenter") or current_center)
    late_center = float(late_profile.get("valueCenter") or current_center)
    value_drift = round(late_center - early_center, 4)
    efficiency = abs(net_move) / max(total_abs_move, PRICE_BIN_SIZE)
    rotation_score = clamp(direction_changes / max(2.0, len(buckets) / 3.0), 0.0, 1.0)
    width_ratio = clamp(value_width / price_range, 0.0, 1.0)
    drift_ratio = clamp(abs(value_drift) / max(value_width, PRICE_BIN_SIZE), 0.0, 2.0)
    close_extension = abs(current_price - current_center) / max(value_width, PRICE_BIN_SIZE)

    balance_score = (
        (rotation_score * 34.0)
        + (width_ratio * 28.0)
        + ((1.0 - clamp(drift_ratio / 1.4, 0.0, 1.0)) * 24.0)
        + ((1.0 - clamp(efficiency, 0.0, 1.0)) * 14.0)
    )
    trend_score = (
        (clamp(efficiency, 0.0, 1.0) * 34.0)
        + (clamp(drift_ratio / 1.2, 0.0, 1.0) * 28.0)
        + (clamp(abs(net_move) / max(price_range, PRICE_BIN_SIZE), 0.0, 1.0) * 22.0)
        + (clamp(close_extension / 1.2, 0.0, 1.0) * 16.0)
    )
    transition_score = (
        (clamp(abs(balance_score - trend_score) / 100.0, 0.0, 1.0) * -20.0)
        + (clamp(abs(current_price - current_center) / max(value_width * 0.8, PRICE_BIN_SIZE), 0.0, 1.0) * 28.0)
        + (clamp(1.0 - width_ratio, 0.0, 1.0) * 18.0)
        + (clamp(1.0 - rotation_score, 0.0, 1.0) * 18.0)
        + (clamp(1.0 - abs(efficiency - 0.5) * 2.0, 0.0, 1.0) * 16.0)
    )
    balance_score = clamp(balance_score, 0.0, 100.0)
    trend_score = clamp(trend_score, 0.0, 100.0)
    transition_score = clamp(transition_score, 0.0, 100.0)

    state_kind = "Balance"
    if transition_score >= max(balance_score, trend_score) - 2.0 and transition_score >= 44.0:
        state_kind = "Transition"
    elif trend_score > balance_score + 6.0:
        state_kind = "TrendUp" if (value_drift >= 0 or net_move >= 0) else "TrendDown"

    return {
        "balanceScore": round(balance_score, 2),
        "trendScore": round(trend_score, 2),
        "transitionScore": round(transition_score, 2),
        "stateKind": state_kind,
        "valueDrift": round(value_drift, 4),
        "directionChanges": direction_changes,
        "netMove": round(net_move, 4),
        "totalAbsMove": round(total_abs_move, 4),
        "priceRange": round_price(price_range),
        "efficiency": round(efficiency, 4),
    }


def build_refs(
    *,
    kind: str,
    label: str,
    start_dt: datetime,
    end_dt: datetime,
    profile_meta: Dict[str, Any],
    previous_summary: Optional[Dict[str, Any]],
    ib_high: Optional[float],
    ib_low: Optional[float],
    high_price: float,
    low_price: float,
    excess_poor: Dict[str, Any],
) -> List[Dict[str, Any]]:
    refs: List[Dict[str, Any]] = []
    for ref_kind, key, strength in (
        ("POC", "pocPrice", 0.92),
        ("VAH", "vahPrice", 0.84),
        ("VAL", "valPrice", 0.84),
    ):
        price = profile_meta.get(key)
        if price is None:
            continue
        refs.append({
            "refKind": ref_kind,
            "price": round_price(price),
            "strength": strength,
            "validFromTs": start_dt.isoformat(),
            "validToTs": end_dt.isoformat(),
            "notesJson": {"windowKind": kind, "windowLabel": label},
        })
    if previous_summary:
        for ref_kind, key, strength in (
            ("PrevPOC", "pocPrice", 0.72),
            ("PrevVAH", "vahPrice", 0.66),
            ("PrevVAL", "valPrice", 0.66),
        ):
            price = previous_summary.get(key)
            if price is None:
                continue
            refs.append({
                "refKind": ref_kind,
                "price": round_price(price),
                "strength": strength,
                "validFromTs": start_dt.isoformat(),
                "validToTs": end_dt.isoformat(),
                "notesJson": {"windowKind": kind},
            })
    if ib_high is not None:
        refs.append({"refKind": "InitialBalanceHigh", "price": round_price(ib_high), "strength": 0.62, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    if ib_low is not None:
        refs.append({"refKind": "InitialBalanceLow", "price": round_price(ib_low), "strength": 0.62, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    refs.append({"refKind": "BracketHigh", "price": round_price(high_price), "strength": 0.58, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    refs.append({"refKind": "BracketLow", "price": round_price(low_price), "strength": 0.58, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    if excess_poor.get("excessHigh") is not None:
        refs.append({"refKind": "ExcessHigh", "price": round_price(excess_poor["excessHigh"]), "strength": 0.82, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    if excess_poor.get("excessLow") is not None:
        refs.append({"refKind": "ExcessLow", "price": round_price(excess_poor["excessLow"]), "strength": 0.82, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    if excess_poor.get("poorHigh") is not None:
        refs.append({"refKind": "PoorHigh", "price": round_price(excess_poor["poorHigh"]), "strength": 0.56, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    if excess_poor.get("poorLow") is not None:
        refs.append({"refKind": "PoorLow", "price": round_price(excess_poor["poorLow"]), "strength": 0.56, "validFromTs": start_dt.isoformat(), "validToTs": end_dt.isoformat(), "notesJson": {"windowKind": kind}})
    return refs


def decorate_events(
    *,
    end_dt: datetime,
    current_price: float,
    state_scores: Dict[str, Any],
    rejection_events: List[Dict[str, Any]],
    excess_poor: Dict[str, Any],
    previous_summary: Optional[Dict[str, Any]],
    current_center: float,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    events.extend(rejection_events)
    value_drift = float(state_scores.get("valueDrift") or 0.0)
    if value_drift > PRICE_BIN_SIZE:
        events.append({"eventKind": "ValueUp", "direction": "Up", "strength": clamp(abs(value_drift) / (PRICE_BIN_SIZE * 4.0), 0.35, 0.9), "confirmed": True, "price1": round_price(current_center)})
    elif value_drift < -PRICE_BIN_SIZE:
        events.append({"eventKind": "ValueDown", "direction": "Down", "strength": clamp(abs(value_drift) / (PRICE_BIN_SIZE * 4.0), 0.35, 0.9), "confirmed": True, "price1": round_price(current_center)})
    if state_scores.get("stateKind") == "Balance":
        events.append({"eventKind": "BalanceFormed", "direction": "Flat", "strength": clamp(float(state_scores.get("balanceScore") or 0.0) / 100.0, 0.35, 0.92), "confirmed": True, "price1": round_price(current_center)})
    if previous_summary and previous_summary.get("stateKind") == "Balance" and state_scores.get("stateKind") in {"TrendUp", "TrendDown"}:
        events.append({"eventKind": "BalanceBroken", "direction": "Up" if state_scores.get("stateKind") == "TrendUp" else "Down", "strength": 0.76, "confirmed": True, "price1": round_price(current_price)})
    if excess_poor.get("excessHigh") is not None:
        events.append({"eventKind": "ExcessHigh", "direction": "Down", "strength": 0.82, "confirmed": True, "price1": round_price(excess_poor["excessHigh"])})
    if excess_poor.get("excessLow") is not None:
        events.append({"eventKind": "ExcessLow", "direction": "Up", "strength": 0.82, "confirmed": True, "price1": round_price(excess_poor["excessLow"])})
    if excess_poor.get("poorHigh") is not None:
        events.append({"eventKind": "PoorHigh", "direction": "Up", "strength": 0.56, "confirmed": False, "price1": round_price(excess_poor["poorHigh"])})
    if excess_poor.get("poorLow") is not None:
        events.append({"eventKind": "PoorLow", "direction": "Down", "strength": 0.56, "confirmed": False, "price1": round_price(excess_poor["poorLow"])})
    for event in events:
        event["eventTs"] = end_dt.isoformat()
        event["eventTsMs"] = dt_to_ms(end_dt)
        event.setdefault("price2", None)
    return events


def classify_inventory(
    summary: Dict[str, Any],
    *,
    mid_context: Optional[Dict[str, Any]],
    long_context: Optional[Dict[str, Any]],
) -> str:
    current_price = float(summary.get("closePrice") or 0.0)
    if long_context and long_context.get("vahPrice") is not None and current_price > float(long_context["vahPrice"]) and float(summary.get("valueDrift") or 0.0) >= 0:
        return "Long"
    if long_context and long_context.get("valPrice") is not None and current_price < float(long_context["valPrice"]) and float(summary.get("valueDrift") or 0.0) <= 0:
        return "Short"
    if mid_context and mid_context.get("stateKind") in {"TrendUp", "TrendDown"}:
        mid_drift = float(mid_context.get("valueDrift") or 0.0)
        own_drift = float(summary.get("valueDrift") or 0.0)
        if mid_drift != 0 and own_drift != 0 and math.copysign(1.0, mid_drift) != math.copysign(1.0, own_drift):
            return "Correcting"
    return "Neutral"


def classify_action(
    summary: Dict[str, Any],
    *,
    mid_context: Optional[Dict[str, Any]],
    long_context: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    state_kind = str(summary.get("stateKind") or "Transition")
    location_kind = str(summary.get("locationKind") or "InValue")
    acceptance_kind = str(summary.get("acceptanceKind") or "Neutral")
    current_price = float(summary.get("closePrice") or 0.0)
    vah_price = float(summary.get("vahPrice") or current_price)
    val_price = float(summary.get("valPrice") or current_price)
    poc_price = float(summary.get("pocPrice") or current_price)
    high_price = float(summary.get("highPrice") or current_price)
    low_price = float(summary.get("lowPrice") or current_price)
    value_drift = float(summary.get("valueDrift") or 0.0)
    balance_score = float(summary.get("balanceScore") or 0.0)
    trend_score = float(summary.get("trendScore") or 0.0)
    transition_score = float(summary.get("transitionScore") or 0.0)
    refs = {item.get("refKind"): item for item in (summary.get("references") or [])}
    behavior_kind = "Neutral"
    action = "NoTrade"
    invalidation = None
    target1 = None
    target2 = None
    confidence = clamp(max(balance_score, trend_score, transition_score) / 100.0, 0.15, 0.96)

    long_state = str((long_context or {}).get("stateKind") or "")
    long_drift = float((long_context or {}).get("valueDrift") or 0.0)
    supportive_up = long_state == "TrendUp" or long_drift > 0
    supportive_down = long_state == "TrendDown" or long_drift < 0

    if location_kind in {"OutsideLower", "BelowValue"} and acceptance_kind == "Rejected" and state_kind in {"Balance", "Transition"}:
        behavior_kind = "Responsive"
        action = "ResponsiveBuy"
        invalidation = float(refs.get("ExcessLow", refs.get("PoorLow", {"price": low_price})).get("price") or low_price) - PRICE_BIN_SIZE
        target1 = poc_price
        target2 = vah_price
        confidence = clamp((balance_score / 100.0) * 0.7 + 0.22, 0.22, 0.88)
    elif location_kind in {"OutsideUpper", "AboveValue"} and acceptance_kind == "Rejected" and state_kind in {"Balance", "Transition"}:
        behavior_kind = "Responsive"
        action = "ResponsiveSell"
        invalidation = float(refs.get("ExcessHigh", refs.get("PoorHigh", {"price": high_price})).get("price") or high_price) + PRICE_BIN_SIZE
        target1 = poc_price
        target2 = val_price
        confidence = clamp((balance_score / 100.0) * 0.7 + 0.22, 0.22, 0.88)
    elif acceptance_kind == "Accepted" and state_kind == "TrendUp" and value_drift >= 0 and supportive_up:
        behavior_kind = "Initiative"
        action = "InitiativeBuy"
        invalidation = max(vah_price - (PRICE_BIN_SIZE * 2), poc_price)
        target1 = high_price + max(PRICE_BIN_SIZE * 3, abs(value_drift) * 1.5)
        target2 = target1 + max(PRICE_BIN_SIZE * 4, abs(value_drift) * 2.0)
        confidence = clamp((trend_score / 100.0) * 0.78 + 0.12, 0.25, 0.93)
    elif acceptance_kind == "Accepted" and state_kind == "TrendDown" and value_drift <= 0 and supportive_down:
        behavior_kind = "Initiative"
        action = "InitiativeSell"
        invalidation = min(val_price + (PRICE_BIN_SIZE * 2), poc_price)
        target1 = low_price - max(PRICE_BIN_SIZE * 3, abs(value_drift) * 1.5)
        target2 = target1 - max(PRICE_BIN_SIZE * 4, abs(value_drift) * 2.0)
        confidence = clamp((trend_score / 100.0) * 0.78 + 0.12, 0.25, 0.93)
    elif state_kind == "Transition" or transition_score > max(balance_score, trend_score):
        behavior_kind = "Neutral"
        action = "NoTrade"
        confidence = clamp((transition_score / 100.0) * 0.6, 0.18, 0.66)
    else:
        behavior_kind = "Neutral"
        action = "NoTrade"
        target1 = poc_price
        confidence = clamp((max(balance_score, trend_score) / 100.0) * 0.42, 0.16, 0.58)

    return {
        "behaviorKind": behavior_kind,
        "preferredAction": action,
        "biasKind": action,
        "confidence": round(confidence, 4),
        "invalidationPrice": round_price(invalidation),
        "targetPrice1": round_price(target1),
        "targetPrice2": round_price(target2),
    }


def nearest_references(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    current_price = float(summary.get("closePrice") or 0.0)
    refs = []
    for item in summary.get("references") or []:
        price = item.get("price")
        if price is None:
            continue
        refs.append({**item, "distance": round(abs(float(price) - current_price), 2)})
    refs.sort(key=lambda item: (item["distance"], -(float(item.get("strength") or 0.0))))
    return refs[:8]


def summary_note(summary: Dict[str, Any]) -> str:
    action = summary.get("preferredAction") or "NoTrade"
    state = summary.get("stateKind") or "Transition"
    location = summary.get("locationKind") or "InValue"
    acceptance = summary.get("acceptanceKind") or "Neutral"
    drift = float(summary.get("valueDrift") or 0.0)
    direction = "up" if drift > 0 else ("down" if drift < 0 else "flat")
    return f"{state} | {location} | {acceptance} | value {direction} | {action}"


def build_window_summary(
    *,
    kind: str,
    label: str,
    buckets: List[SecondBucket],
    previous_buckets: List[SecondBucket],
    start_dt: datetime,
    end_dt: datetime,
) -> Dict[str, Any]:
    if not buckets:
        return {
            "kind": kind,
            "label": label,
            "startTs": start_dt.isoformat(),
            "endTs": end_dt.isoformat(),
            "windowSeconds": max(1, int((end_dt - start_dt).total_seconds())),
            "rowsAvailable": False,
            "profile": [],
            "references": [],
            "events": [],
            "nearestReferences": [],
            "summaryText": "No auction data in this window.",
        }

    profile_bins = aggregate_profile(buckets)
    bins_payload, profile_meta = profile_summary(profile_bins)
    high_price = max(bucket.high_price for bucket in buckets)
    low_price = min(bucket.low_price for bucket in buckets)
    open_price = float(buckets[0].open_price)
    close_price = float(buckets[-1].close_price)
    current_price = close_price
    first_half_cut = max(1, len(buckets) // 2)
    early_buckets = buckets[:first_half_cut]
    late_buckets = buckets[first_half_cut:] or buckets[-1:]
    current_scores = balance_scores(
        buckets=buckets,
        early_buckets=early_buckets,
        late_buckets=late_buckets,
        profile_meta=profile_meta,
        current_price=current_price,
    )
    previous_summary = None
    if previous_buckets:
        previous_profile_meta = profile_summary(aggregate_profile(previous_buckets))[1]
        previous_scores = balance_scores(
            buckets=previous_buckets,
            early_buckets=previous_buckets[: max(1, len(previous_buckets) // 2)],
            late_buckets=previous_buckets[max(1, len(previous_buckets) // 2):] or previous_buckets[-1:],
            profile_meta=previous_profile_meta,
            current_price=float(previous_buckets[-1].close_price),
        )
        previous_summary = {
            "pocPrice": previous_profile_meta.get("pocPrice"),
            "vahPrice": previous_profile_meta.get("vahPrice"),
            "valPrice": previous_profile_meta.get("valPrice"),
            "valueCenter": previous_profile_meta.get("valueCenter"),
            "valueWidth": previous_profile_meta.get("valueWidth"),
            "stateKind": previous_scores.get("stateKind"),
        }

    ib_seconds = 60 * 60 if kind in ACTIVE_SESSION_KINDS else max(5 * 60, int(min((dt_to_ms(end_dt) or 0) - (dt_to_ms(start_dt) or 0), 15 * 60 * 1000) / 1000))
    ib_end_ms = (dt_to_ms(start_dt) or 0) + (ib_seconds * 1000)
    ib_buckets = [bucket for bucket in buckets if bucket.timestamp_ms <= ib_end_ms]
    ib_high = max((bucket.high_price for bucket in ib_buckets), default=None)
    ib_low = min((bucket.low_price for bucket in ib_buckets), default=None)

    acceptance_kind, rejection_events = rejection_flags(
        buckets=buckets,
        vah_price=profile_meta.get("vahPrice"),
        val_price=profile_meta.get("valPrice"),
        current_price=current_price,
        current_state=current_scores["stateKind"],
        value_drift=float(current_scores["valueDrift"]),
    )
    excess_poor = detect_excess_and_poor(
        bins_payload=bins_payload,
        profile_meta=profile_meta,
        current_price=current_price,
        high_price=high_price,
        low_price=low_price,
    )
    references = build_refs(
        kind=kind,
        label=label,
        start_dt=start_dt,
        end_dt=end_dt,
        profile_meta=profile_meta,
        previous_summary=previous_summary,
        ib_high=ib_high,
        ib_low=ib_low,
        high_price=high_price,
        low_price=low_price,
        excess_poor=excess_poor,
    )
    events = decorate_events(
        end_dt=end_dt,
        current_price=current_price,
        state_scores=current_scores,
        rejection_events=rejection_events,
        excess_poor=excess_poor,
        previous_summary=previous_summary,
        current_center=float(profile_meta.get("valueCenter") or current_price),
    )
    open_type = compute_open_type(kind=kind, buckets=buckets, previous_summary=previous_summary)
    bracket_position = 0.5 if high_price <= low_price else clamp((current_price - low_price) / max(high_price - low_price, PRICE_BIN_SIZE), 0.0, 1.0)
    location_kind = price_location_kind(
        current_price=current_price,
        poc_price=profile_meta.get("pocPrice"),
        vah_price=profile_meta.get("vahPrice"),
        val_price=profile_meta.get("valPrice"),
    )

    summary = {
        "kind": kind,
        "label": label,
        "startTs": start_dt.isoformat(),
        "endTs": end_dt.isoformat(),
        "startTsMs": dt_to_ms(start_dt),
        "endTsMs": dt_to_ms(end_dt),
        "windowSeconds": max(1, int((end_dt - start_dt).total_seconds())),
        "rowsAvailable": True,
        "openPrice": round_price(open_price),
        "highPrice": round_price(high_price),
        "lowPrice": round_price(low_price),
        "closePrice": round_price(close_price),
        "pocPrice": round_price(profile_meta.get("pocPrice")),
        "vahPrice": round_price(profile_meta.get("vahPrice")),
        "valPrice": round_price(profile_meta.get("valPrice")),
        "ibHigh": round_price(ib_high),
        "ibLow": round_price(ib_low),
        "stateKind": current_scores.get("stateKind"),
        "openType": open_type,
        "locationKind": location_kind,
        "acceptanceKind": acceptance_kind,
        "valueDrift": round_price(current_scores.get("valueDrift")),
        "balanceScore": current_scores.get("balanceScore"),
        "trendScore": current_scores.get("trendScore"),
        "transitionScore": current_scores.get("transitionScore"),
        "bracketPosition": round(bracket_position, 4),
        "directionChanges": int(current_scores.get("directionChanges") or 0),
        "netMove": round_price(current_scores.get("netMove")),
        "range": round_price(current_scores.get("priceRange")),
        "efficiency": current_scores.get("efficiency"),
        "profile": bins_payload,
        "references": references,
        "events": events,
        "previousSummary": previous_summary,
        "valueCenter": round_price(profile_meta.get("valueCenter")),
        "valueWidth": round_price(profile_meta.get("valueWidth")),
    }
    return summary


def current_session_window(kind: str, as_of: datetime) -> Tuple[str, datetime, datetime]:
    if kind == "london":
        label = WINDOW_LABELS["london"]
        local = as_of.astimezone(LONDON_TZ)
        start_local = local.replace(hour=8, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(hours=9)
        if local < start_local:
            start_local -= timedelta(days=1)
            end_local -= timedelta(days=1)
        elif local <= end_local:
            end_local = local
        return label, start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
    if kind == "newyork":
        label = WINDOW_LABELS["newyork"]
        local = as_of.astimezone(NEW_YORK_TZ)
        start_local = local.replace(hour=8, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(hours=9)
        if local < start_local:
            start_local -= timedelta(days=1)
            end_local -= timedelta(days=1)
        elif local <= end_local:
            end_local = local
        return label, start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
    label = WINDOW_LABELS["brokerday"]
    local = as_of.astimezone(SYDNEY_TZ)
    start_local = local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = min(local, start_local + timedelta(days=1))
    return label, start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def previous_session_window(kind: str, start_dt: datetime, end_dt: datetime) -> Tuple[datetime, datetime]:
    _ = kind
    _ = end_dt
    return start_dt - timedelta(days=1), start_dt


def build_history_snapshots(
    store: "AuctionStateStore",
    *,
    session_kinds: Iterable[str] = HISTORY_SESSION_KINDS,
) -> Dict[str, Dict[str, Any]]:
    snapshots: Dict[str, Dict[str, Any]] = {}
    for session_kind in session_kinds:
        normalized_kind = str(session_kind or "").strip().lower()
        if normalized_kind not in ACTIVE_SESSION_KINDS:
            continue
        snapshot = store.build_snapshot(focus_kind=normalized_kind)
        session_summary = snapshot.get("windows", {}).get("session")
        if session_summary and session_summary.get("rowsAvailable"):
            snapshots[normalized_kind] = snapshot
    return snapshots


def auction_history_counts(
    conn: Any,
    *,
    symbol: str,
    since: datetime,
    session_kinds: Iterable[str] = HISTORY_SESSION_KINDS,
) -> Dict[str, int]:
    kinds = [str(kind).strip().lower() for kind in session_kinds if str(kind).strip().lower() in ACTIVE_SESSION_KINDS]
    counts = {kind: 0 for kind in kinds}
    if not kinds:
        return counts
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT sessionkind, COUNT(*) AS rowcount
            FROM public.auctionhistorysession
            WHERE symbol = %s
              AND startts >= %s
              AND sessionkind = ANY(%s)
            GROUP BY sessionkind
            """,
            (symbol, since, kinds),
        )
        for row in cur.fetchall():
            counts[str(row.get("sessionkind"))] = int(row.get("rowcount") or 0)
    return counts


def delete_auction_history_range(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    session_kinds: Iterable[str] = HISTORY_SESSION_KINDS,
) -> Dict[str, int]:
    kinds = [str(kind).strip().lower() for kind in session_kinds if str(kind).strip().lower() in ACTIVE_SESSION_KINDS]
    if not kinds:
        return {"sessionsDeleted": 0, "statesDeleted": 0}
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.auctionhistorystate
            WHERE symbol = %s
              AND focuskind = ANY(%s)
              AND snapshotts >= %s
              AND snapshotts <= %s
            """,
            (symbol, kinds, start_ts, end_ts),
        )
        state_deleted = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM public.auctionhistorysession
            WHERE symbol = %s
              AND sessionkind = ANY(%s)
              AND endts >= %s
              AND startts <= %s
            """,
            (symbol, kinds, start_ts, end_ts),
        )
        session_deleted = int(cur.rowcount or 0)
    return {"sessionsDeleted": session_deleted, "statesDeleted": state_deleted}


def persist_auction_history_snapshots(
    conn: Any,
    *,
    symbol: str,
    snapshots: Dict[str, Dict[str, Any]],
    with_bins: bool = True,
    retain_state_days: int = HISTORY_STATE_RETENTION_DAYS,
) -> Dict[str, int]:
    summary = {
        "sessionsUpserted": 0,
        "binsWritten": 0,
        "refsWritten": 0,
        "eventsWritten": 0,
        "statesWritten": 0,
        "statesDeleted": 0,
    }
    if not snapshots:
        return summary

    with conn.cursor() as cur:
        for focus_kind, snapshot in snapshots.items():
            session_summary = snapshot.get("windows", {}).get("session") or {}
            if not session_summary.get("rowsAvailable"):
                continue
            as_of_ts = ms_to_dt(snapshot.get("asOfTsMs"))
            if as_of_ts is None and snapshot.get("asOfTs"):
                try:
                    as_of_ts = datetime.fromisoformat(str(snapshot["asOfTs"]))
                except ValueError:
                    as_of_ts = None
            if as_of_ts is None:
                continue

            session_kind = str(session_summary.get("sessionKind") or focus_kind or "").strip().lower()
            if session_kind not in ACTIVE_SESSION_KINDS:
                continue

            cur.execute(
                """
                INSERT INTO public.auctionhistorysession (
                    symbol, sessionkind, startts, endts, asofts, windowseconds,
                    openprice, highprice, lowprice, closeprice,
                    pocprice, vahprice, valprice, ibhigh, iblow,
                    statekind, opentype, inventorytype,
                    valuedrift, balancescore, trendscore, transitionscore,
                    summaryjson, updatedts
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s::jsonb, NOW()
                )
                ON CONFLICT (symbol, sessionkind, startts)
                DO UPDATE SET
                    endts = EXCLUDED.endts,
                    asofts = EXCLUDED.asofts,
                    windowseconds = EXCLUDED.windowseconds,
                    openprice = EXCLUDED.openprice,
                    highprice = EXCLUDED.highprice,
                    lowprice = EXCLUDED.lowprice,
                    closeprice = EXCLUDED.closeprice,
                    pocprice = EXCLUDED.pocprice,
                    vahprice = EXCLUDED.vahprice,
                    valprice = EXCLUDED.valprice,
                    ibhigh = EXCLUDED.ibhigh,
                    iblow = EXCLUDED.iblow,
                    statekind = EXCLUDED.statekind,
                    opentype = EXCLUDED.opentype,
                    inventorytype = EXCLUDED.inventorytype,
                    valuedrift = EXCLUDED.valuedrift,
                    balancescore = EXCLUDED.balancescore,
                    trendscore = EXCLUDED.trendscore,
                    transitionscore = EXCLUDED.transitionscore,
                    summaryjson = EXCLUDED.summaryjson,
                    updatedts = NOW()
                RETURNING id
                """,
                (
                    symbol,
                    session_kind,
                    session_summary.get("startTs"),
                    session_summary.get("endTs"),
                    as_of_ts,
                    session_summary.get("windowSeconds"),
                    session_summary.get("openPrice"),
                    session_summary.get("highPrice"),
                    session_summary.get("lowPrice"),
                    session_summary.get("closePrice"),
                    session_summary.get("pocPrice"),
                    session_summary.get("vahPrice"),
                    session_summary.get("valPrice"),
                    session_summary.get("ibHigh"),
                    session_summary.get("ibLow"),
                    session_summary.get("stateKind"),
                    session_summary.get("openType"),
                    session_summary.get("inventoryType"),
                    session_summary.get("valueDrift"),
                    session_summary.get("balanceScore"),
                    session_summary.get("trendScore"),
                    session_summary.get("transitionScore"),
                    psycopg2.extras.Json(session_summary),
                ),
            )
            session_id = int(cur.fetchone()[0])
            summary["sessionsUpserted"] += 1

            if with_bins:
                cur.execute(
                    "DELETE FROM public.auctionhistorybin WHERE auctionhistorysessionid = %s",
                    (session_id,),
                )
                for profile_bin in session_summary.get("profile") or []:
                    cur.execute(
                        """
                        INSERT INTO public.auctionhistorybin (
                            auctionhistorysessionid, pricebin, tickcount, timems,
                            bidhitcount, askliftcount, spreadsum,
                            l2bidvol, l2askvol, activityscore, dwellscore, deltascore
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            session_id,
                            profile_bin.get("priceBin"),
                            profile_bin.get("tickCount"),
                            profile_bin.get("timeMs"),
                            profile_bin.get("bidHitCount"),
                            profile_bin.get("askLiftCount"),
                            profile_bin.get("spreadSum"),
                            profile_bin.get("l2BidVol"),
                            profile_bin.get("l2AskVol"),
                            profile_bin.get("activityScore"),
                            profile_bin.get("dwellScore"),
                            profile_bin.get("deltaScore"),
                        ),
                    )
                    summary["binsWritten"] += 1

            cur.execute(
                "DELETE FROM public.auctionhistoryref WHERE auctionhistorysessionid = %s",
                (session_id,),
            )
            for ref in session_summary.get("references") or []:
                cur.execute(
                    """
                    INSERT INTO public.auctionhistoryref (
                        auctionhistorysessionid, refkind, price, strength, validfromts, validtots, notesjson
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        session_id,
                        ref.get("refKind"),
                        ref.get("price"),
                        ref.get("strength"),
                        ref.get("validFromTs"),
                        ref.get("validToTs"),
                        psycopg2.extras.Json(ref.get("notesJson") or {}),
                    ),
                )
                summary["refsWritten"] += 1

            cur.execute(
                "DELETE FROM public.auctionhistoryevent WHERE auctionhistorysessionid = %s",
                (session_id,),
            )
            for event in session_summary.get("events") or []:
                cur.execute(
                    """
                    INSERT INTO public.auctionhistoryevent (
                        auctionhistorysessionid, eventts, eventkind, price1, price2,
                        direction, strength, confirmed, payloadjson
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        session_id,
                        event.get("eventTs"),
                        event.get("eventKind"),
                        event.get("price1"),
                        event.get("price2"),
                        event.get("direction"),
                        event.get("strength"),
                        bool(event.get("confirmed")),
                        psycopg2.extras.Json(event),
                    ),
                )
                summary["eventsWritten"] += 1

            focus_window = snapshot.get("focusWindow") or {}
            cur.execute(
                """
                INSERT INTO public.auctionhistorystate (
                    symbol, focuskind, snapshotts, sessionkind, sessionstartts, sessionendts,
                    lastprocessedid, statekind, locationkind, acceptancekind,
                    inventorytype, biaskind, confidence, invalidationprice,
                    targetprice1, targetprice2, summaryjson
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s::jsonb
                )
                ON CONFLICT (symbol, focuskind, snapshotts)
                DO UPDATE SET
                    sessionkind = EXCLUDED.sessionkind,
                    sessionstartts = EXCLUDED.sessionstartts,
                    sessionendts = EXCLUDED.sessionendts,
                    lastprocessedid = EXCLUDED.lastprocessedid,
                    statekind = EXCLUDED.statekind,
                    locationkind = EXCLUDED.locationkind,
                    acceptancekind = EXCLUDED.acceptancekind,
                    inventorytype = EXCLUDED.inventorytype,
                    biaskind = EXCLUDED.biaskind,
                    confidence = EXCLUDED.confidence,
                    invalidationprice = EXCLUDED.invalidationprice,
                    targetprice1 = EXCLUDED.targetprice1,
                    targetprice2 = EXCLUDED.targetprice2,
                    summaryjson = EXCLUDED.summaryjson
                """,
                (
                    symbol,
                    session_kind,
                    as_of_ts,
                    session_kind,
                    session_summary.get("startTs"),
                    session_summary.get("endTs"),
                    snapshot.get("lastProcessedId"),
                    focus_window.get("stateKind"),
                    focus_window.get("locationKind"),
                    focus_window.get("acceptanceKind"),
                    focus_window.get("inventoryType"),
                    focus_window.get("biasKind"),
                    focus_window.get("confidence"),
                    focus_window.get("invalidationPrice"),
                    focus_window.get("targetPrice1"),
                    focus_window.get("targetPrice2"),
                    psycopg2.extras.Json(focus_window),
                ),
            )
            summary["statesWritten"] += 1

        if retain_state_days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(retain_state_days)))
            cur.execute(
                """
                DELETE FROM public.auctionhistorystate
                WHERE symbol = %s
                  AND snapshotts < %s
                """,
                (symbol, cutoff),
            )
            summary["statesDeleted"] = int(cur.rowcount or 0)
    return summary


class AuctionStateStore:
    def __init__(self, *, symbol: str) -> None:
        self.symbol = symbol
        self.recent_buckets: Deque[SecondBucket] = deque()
        self.current_bucket: Optional[SecondBucket] = None
        self.last_tick: Optional[Dict[str, Any]] = None
        self.last_processed_id = 0
        self.last_processed_ts_ms: Optional[int] = None

    def apply_rows(self, rows: Iterable[Dict[str, Any]]) -> None:
        for row in rows:
            tick = normalize_tick_row(row)
            if tick is None:
                continue
            if self.last_tick is not None:
                dwell_ms = max(0, min(DWELL_CAP_MS, int(tick["timestampMs"] - self.last_tick["timestampMs"])))
                if self.current_bucket is not None:
                    self.current_bucket.add_dwell(int(self.last_tick["binIndex"]), dwell_ms)
            self._roll_bucket_if_needed(tick)
            delta = 0.0
            if self.last_tick is not None:
                delta = float(tick["mid"]) - float(self.last_tick["mid"])
            if self.current_bucket is None:
                self.current_bucket = SecondBucket(
                    timestamp_ms=int(tick["timestampMs"]),
                    timestamp=tick["timestamp"],
                    open_price=float(tick["mid"]),
                    high_price=float(tick["mid"]),
                    low_price=float(tick["mid"]),
                    close_price=float(tick["mid"]),
                )
            self.current_bucket.register_tick(
                price=float(tick["mid"]),
                spread=float(tick["spread"]),
                delta=float(delta),
                bucket_bin_index=int(tick["binIndex"]),
            )
            self.last_tick = tick
            self.last_processed_id = max(self.last_processed_id, int(tick["id"]))
            self.last_processed_ts_ms = tick["timestampMs"]
        self._trim()

    def _roll_bucket_if_needed(self, tick: Dict[str, Any]) -> None:
        if self.current_bucket is None:
            return
        if int(self.current_bucket.timestamp_ms // 1000) == int(tick["timestampMs"] // 1000):
            return
        self.recent_buckets.append(self.current_bucket)
        self.current_bucket = SecondBucket(
            timestamp_ms=int(tick["timestampMs"]),
            timestamp=tick["timestamp"],
            open_price=float(tick["mid"]),
            high_price=float(tick["mid"]),
            low_price=float(tick["mid"]),
            close_price=float(tick["mid"]),
        )

    def _trim(self) -> None:
        if self.last_processed_ts_ms is None:
            return
        cutoff_ms = int(self.last_processed_ts_ms) - (CONTEXT_SECONDS * 1000)
        while self.recent_buckets and self.recent_buckets[0].timestamp_ms < cutoff_ms:
            self.recent_buckets.popleft()

    def snapshot_buckets(self) -> List[SecondBucket]:
        buckets = list(self.recent_buckets)
        if self.current_bucket is not None and (not buckets or self.current_bucket.timestamp_ms >= buckets[-1].timestamp_ms):
            buckets.append(self.current_bucket)
        return buckets

    def build_snapshot(self, *, focus_kind: str) -> Dict[str, Any]:
        buckets = self.snapshot_buckets()
        if not buckets:
            return {
                "symbol": self.symbol,
                "focusKind": focus_kind,
                "sessionKind": focus_kind if focus_kind in ACTIVE_SESSION_KINDS else "brokerday",
                "windows": {},
                "ladder": [],
                "focusWindow": None,
                "events": [],
                "asOfTs": None,
                "asOfTsMs": None,
            }

        as_of = ms_to_dt(self.last_processed_ts_ms) or buckets[-1].timestamp
        as_of_ms = dt_to_ms(as_of) or buckets[-1].timestamp_ms
        session_kind = focus_kind if focus_kind in ACTIVE_SESSION_KINDS else "brokerday"
        window_summaries: Dict[str, Dict[str, Any]] = {}

        for kind, duration_seconds in ROLLING_WINDOWS.items():
            start_ms = as_of_ms - (duration_seconds * 1000)
            current_buckets = select_window_buckets(buckets, start_ms=start_ms, end_ms=as_of_ms)
            previous_buckets = select_window_buckets(
                buckets,
                start_ms=start_ms - (duration_seconds * 1000),
                end_ms=start_ms,
            )
            window_summaries[kind] = build_window_summary(
                kind=kind,
                label=WINDOW_LABELS[kind],
                buckets=current_buckets,
                previous_buckets=previous_buckets,
                start_dt=ms_to_dt(start_ms) or buckets[0].timestamp,
                end_dt=as_of,
            )

        session_label, session_start_dt, session_end_dt = current_session_window(session_kind, as_of)
        session_buckets = select_window_buckets(
            buckets,
            start_ms=dt_to_ms(session_start_dt) or as_of_ms,
            end_ms=dt_to_ms(session_end_dt) or as_of_ms,
        )
        previous_session_start, previous_session_end = previous_session_window(session_kind, session_start_dt, session_end_dt)
        previous_session_buckets = select_window_buckets(
            buckets,
            start_ms=dt_to_ms(previous_session_start) or as_of_ms,
            end_ms=dt_to_ms(previous_session_end) or as_of_ms,
        )
        session_summary = build_window_summary(
            kind=session_kind,
            label=session_label,
            buckets=session_buckets,
            previous_buckets=previous_session_buckets,
            start_dt=session_start_dt,
            end_dt=session_end_dt,
        )
        session_summary["kind"] = "session"
        session_summary["sessionKind"] = session_kind
        session_summary["label"] = session_label
        window_summaries["session"] = session_summary

        focus_summary = session_summary if focus_kind in ACTIVE_SESSION_KINDS else window_summaries.get(focus_kind) or window_summaries["session"]
        mid_context = window_summaries.get("rolling60m") or window_summaries.get("session")
        long_context = window_summaries.get("rolling240m") or window_summaries.get("rolling24h")
        for item in window_summaries.values():
            if not item.get("rowsAvailable"):
                continue
            item["inventoryType"] = classify_inventory(item, mid_context=mid_context, long_context=long_context)
            item.update(classify_action(item, mid_context=mid_context, long_context=long_context))
            item["nearestReferences"] = nearest_references(item)
            item["summaryText"] = summary_note(item)
        focus_summary = focus_summary or {}
        events: List[Dict[str, Any]] = []
        for kind in ("rolling15m", "rolling60m", "session", "rolling240m"):
            for event in window_summaries.get(kind, {}).get("events") or []:
                event_copy = dict(event)
                event_copy["windowKind"] = kind
                event_copy["windowLabel"] = window_summaries.get(kind, {}).get("label")
                events.append(event_copy)
        events.sort(key=lambda item: (int(item.get("eventTsMs") or 0), float(item.get("strength") or 0.0)), reverse=True)
        ladder = []
        for kind in WINDOW_ORDER:
            item = window_summaries.get(kind)
            if not item:
                continue
            ladder.append({
                "kind": kind,
                "label": item.get("label") or WINDOW_LABELS.get(kind, kind),
                "stateKind": item.get("stateKind"),
                "preferredAction": item.get("preferredAction"),
                "locationKind": item.get("locationKind"),
                "valueDrift": item.get("valueDrift"),
                "acceptanceKind": item.get("acceptanceKind"),
                "confidence": item.get("confidence"),
                "rowsAvailable": item.get("rowsAvailable"),
            })
        return {
            "symbol": self.symbol,
            "asOfTs": as_of.isoformat(),
            "asOfTsMs": as_of_ms,
            "lastProcessedId": self.last_processed_id,
            "lastProcessedTsMs": self.last_processed_ts_ms,
            "focusKind": focus_kind,
            "sessionKind": session_kind,
            "windows": window_summaries,
            "ladder": ladder,
            "focusWindow": focus_summary,
            "events": events[:24],
        }

    def serialize(self) -> Dict[str, Any]:
        return {
            "lastProcessedId": int(self.last_processed_id),
            "lastProcessedTsMs": self.last_processed_ts_ms,
            "currentBucket": self.current_bucket.serialize() if self.current_bucket else None,
            "recentBuckets": [bucket.serialize() for bucket in self.recent_buckets],
        }

    def restore(self, payload: Dict[str, Any]) -> None:
        self.recent_buckets.clear()
        self.current_bucket = None
        self.last_tick = None
        self.last_processed_id = int(payload.get("lastProcessedId") or 0)
        self.last_processed_ts_ms = payload.get("lastProcessedTsMs")
        for item in payload.get("recentBuckets") or []:
            bucket = SecondBucket.from_payload(item)
            if bucket is not None:
                self.recent_buckets.append(bucket)
        current_bucket = payload.get("currentBucket")
        if current_bucket:
            self.current_bucket = SecondBucket.from_payload(current_bucket)


class AuctionService:
    def __init__(self, *, db_factory: Callable[..., Any], symbol: str) -> None:
        self._db_factory = db_factory
        self._symbol = symbol
        self._lock = threading.RLock()
        self._store = AuctionStateStore(symbol=symbol)
        self._cache_loaded = False
        self._last_persist_monotonic = 0.0
        self._last_live_snapshot: Optional[Dict[str, Any]] = None

    def start(self) -> None:
        self._restore_from_db()

    def stop(self) -> None:
        self.persist(force=True)

    def _restore_from_db(self) -> None:
        try:
            with self._db_factory(readonly=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT snapshotjson
                        FROM public.auctionsnap
                        WHERE symbol = %s
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (self._symbol,),
                    )
                    row = cur.fetchone()
        except psycopg2.Error:
            return
        if not row:
            return
        snapshot_payload = dict(row.get("snapshotjson") or {})
        service_state = snapshot_payload.get("serviceState")
        if not isinstance(service_state, dict):
            return
        with self._lock:
            self._store.restore(service_state)
            self._last_live_snapshot = snapshot_payload.get("liveSnapshot") if isinstance(snapshot_payload.get("liveSnapshot"), dict) else None
            self._cache_loaded = True

    def _query_rows_after(self, after_id: int) -> List[Dict[str, Any]]:
        with self._db_factory(readonly=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread
                    FROM public.ticks
                    WHERE symbol = %s AND id > %s
                    ORDER BY id ASC
                    """,
                    (self._symbol, int(after_id)),
                )
                return [dict(row) for row in cur.fetchall()]

    def _seed_recent_rows(self) -> List[Dict[str, Any]]:
        with self._db_factory(readonly=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT MAX(timestamp) AS last_timestamp
                    FROM public.ticks
                    WHERE symbol = %s
                    """,
                    (self._symbol,),
                )
                last_row = dict(cur.fetchone() or {})
                last_timestamp = last_row.get("last_timestamp")
                if last_timestamp is None:
                    return []
                cutoff = last_timestamp - timedelta(seconds=CONTEXT_SECONDS)
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread
                    FROM public.ticks
                    WHERE symbol = %s AND timestamp >= %s
                    ORDER BY id ASC
                    """,
                    (self._symbol, cutoff),
                )
                return [dict(row) for row in cur.fetchall()]

    def sync_live(self, *, focus_kind: str = "brokerday") -> Dict[str, Any]:
        with self._lock:
            if not self._cache_loaded:
                seed_rows = self._seed_recent_rows()
                if seed_rows:
                    self._store.apply_rows(seed_rows)
                self._cache_loaded = True
            else:
                delta_rows = self._query_rows_after(self._store.last_processed_id)
                if delta_rows:
                    self._store.apply_rows(delta_rows)
            self._last_live_snapshot = self._store.build_snapshot(focus_kind=focus_kind)
            self.persist(force=False)
            return self._last_live_snapshot

    def apply_live_rows(self, rows: Iterable[Dict[str, Any]], *, focus_kind: str) -> Dict[str, Any]:
        with self._lock:
            self._store.apply_rows(rows)
            self._last_live_snapshot = self._store.build_snapshot(focus_kind=focus_kind)
            self.persist(force=False)
            return self._last_live_snapshot

    def build_review_snapshot(self, *, rows: Iterable[Dict[str, Any]], focus_kind: str) -> Dict[str, Any]:
        store = AuctionStateStore(symbol=self._symbol)
        store.apply_rows(rows)
        return store.build_snapshot(focus_kind=focus_kind)

    def persist(self, *, force: bool) -> None:
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self._last_persist_monotonic < PERSIST_INTERVAL_SECONDS:
            return
        snapshot = self._last_live_snapshot or self._store.build_snapshot(focus_kind="brokerday")
        history_snapshots = build_history_snapshots(self._store)
        payload = {
            "serviceState": self._store.serialize(),
            "liveSnapshot": snapshot,
        }
        try:
            self._persist_snapshot(payload, snapshot, history_snapshots=history_snapshots)
        except psycopg2.Error:
            return
        self._last_persist_monotonic = now_monotonic

    def _persist_snapshot(
        self,
        payload: Dict[str, Any],
        snapshot: Dict[str, Any],
        *,
        history_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        with self._db_factory(readonly=False, autocommit=False) as conn:
            with conn.cursor() as cur:
                self._persist_live_cache(cur, payload, snapshot)
            persist_auction_history_snapshots(
                conn,
                symbol=self._symbol,
                snapshots=history_snapshots or {},
                with_bins=True,
            )
            conn.commit()

    def _persist_live_cache(self, cur: Any, payload: Dict[str, Any], snapshot: Dict[str, Any]) -> None:
        cur.execute(
            """
            INSERT INTO public.auctionsnap (symbol, asts, snapshotjson)
            VALUES (%s, NOW(), %s::jsonb)
            """,
            (self._symbol, psycopg2.extras.Json(payload)),
        )
        cur.execute(
            """
            DELETE FROM public.auctionsnap
            WHERE symbol = %s
              AND id NOT IN (
                SELECT id
                FROM public.auctionsnap
                WHERE symbol = %s
                ORDER BY id DESC
                LIMIT 6
              )
            """,
            (self._symbol, self._symbol),
        )
        cur.execute(
            """
            DELETE FROM public.auctionstate
            WHERE symbol = %s
              AND id NOT IN (
                SELECT id
                FROM public.auctionstate
                WHERE symbol = %s
                ORDER BY id DESC
                LIMIT 200
              )
            """,
            (self._symbol, self._symbol),
        )
        cur.execute("DELETE FROM public.auctionsession WHERE symbol = %s", (self._symbol,))
        for key in WINDOW_ORDER:
            item = snapshot.get("windows", {}).get(key)
            if not item or not item.get("rowsAvailable"):
                continue
            cur.execute(
                """
                INSERT INTO public.auctionsession (
                    symbol, anchorkind, startts, endts, windowseconds,
                    openprice, highprice, lowprice, closeprice,
                    pocprice, vahprice, valprice, ibhigh, iblow,
                    statekind, opentype, inventorytype,
                    valuedrift, balancescore, trendscore, transitionscore,
                    updatedts
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    NOW()
                )
                RETURNING id
                """,
                (
                    self._symbol,
                    item.get("sessionKind") if key == "session" else key,
                    item.get("startTs"),
                    item.get("endTs"),
                    item.get("windowSeconds"),
                    item.get("openPrice"),
                    item.get("highPrice"),
                    item.get("lowPrice"),
                    item.get("closePrice"),
                    item.get("pocPrice"),
                    item.get("vahPrice"),
                    item.get("valPrice"),
                    item.get("ibHigh"),
                    item.get("ibLow"),
                    item.get("stateKind"),
                    item.get("openType"),
                    item.get("inventoryType"),
                    item.get("valueDrift"),
                    item.get("balanceScore"),
                    item.get("trendScore"),
                    item.get("transitionScore"),
                ),
            )
            session_id = int(cur.fetchone()[0])
            for profile_bin in item.get("profile") or []:
                cur.execute(
                    """
                    INSERT INTO public.auctionbin (
                        auctionsessionid, pricebin, tickcount, timems,
                        bidhitcount, askliftcount, spreadsum,
                        l2bidvol, l2askvol,
                        activityscore, dwellscore, deltascore
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        session_id,
                        profile_bin.get("priceBin"),
                        profile_bin.get("tickCount"),
                        profile_bin.get("timeMs"),
                        profile_bin.get("bidHitCount"),
                        profile_bin.get("askLiftCount"),
                        profile_bin.get("spreadSum"),
                        profile_bin.get("l2BidVol"),
                        profile_bin.get("l2AskVol"),
                        profile_bin.get("activityScore"),
                        profile_bin.get("dwellScore"),
                        profile_bin.get("deltaScore"),
                    ),
                )
            for ref in item.get("references") or []:
                cur.execute(
                    """
                    INSERT INTO public.auctionref (
                        auctionsessionid, refkind, price, strength, validfromts, validtots, notesjson
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        session_id,
                        ref.get("refKind"),
                        ref.get("price"),
                        ref.get("strength"),
                        ref.get("validFromTs"),
                        ref.get("validToTs"),
                        psycopg2.extras.Json(ref.get("notesJson") or {}),
                    ),
                )
            for event in item.get("events") or []:
                cur.execute(
                    """
                    INSERT INTO public.auctionevent (
                        auctionsessionid, eventts, eventkind, price1, price2,
                        direction, strength, confirmed, payloadjson
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        session_id,
                        event.get("eventTs"),
                        event.get("eventKind"),
                        event.get("price1"),
                        event.get("price2"),
                        event.get("direction"),
                        event.get("strength"),
                        bool(event.get("confirmed")),
                        psycopg2.extras.Json(event),
                    ),
                )
        focus_window = snapshot.get("focusWindow") or {}
        cur.execute(
            """
            INSERT INTO public.auctionstate (
                symbol, asts, windowkind, statekind, locationkind, acceptancekind,
                inventorytype, biaskind, confidence, invalidationprice, targetprice1, targetprice2, summaryjson
            )
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                self._symbol,
                snapshot.get("focusKind"),
                focus_window.get("stateKind"),
                focus_window.get("locationKind"),
                focus_window.get("acceptanceKind"),
                focus_window.get("inventoryType"),
                focus_window.get("biasKind"),
                focus_window.get("confidence"),
                focus_window.get("invalidationPrice"),
                focus_window.get("targetPrice1"),
                focus_window.get("targetPrice2"),
                psycopg2.extras.Json(focus_window),
            ),
        )
