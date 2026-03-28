from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Sequence


ZIG_LEVELS = ("micro", "med", "maxi", "macro")
ZIG_LEVEL_LABELS = {
    "micro": "Micro",
    "med": "Medium",
    "maxi": "Maxi",
    "macro": "Macro",
}
ZIG_TABLES = {
    "micro": "zigmicro",
    "med": "zigmed",
    "maxi": "zigmaxi",
    "macro": "zigmacro",
}
ZIG_PARENT = {
    "micro": "med",
    "med": "maxi",
    "maxi": "macro",
    "macro": None,
}
ZIG_CHILD = {
    "micro": None,
    "med": "micro",
    "maxi": "med",
    "macro": "maxi",
}


def zig_worker_job_name(symbol: str) -> str:
    return "zig:{0}:worker".format(symbol)


def zig_backfill_job_name(symbol: str, range_token: str) -> str:
    return "zig:{0}:backfill:{1}".format(symbol, range_token)


def safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return float(fallback)
        return float(value)
    except (TypeError, ValueError):
        return float(fallback)


def dt_to_text(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def text_to_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def median_or_fallback(values: Sequence[float], fallback: float) -> float:
    cleaned = sorted(float(value) for value in values if value is not None)
    if not cleaned:
        return float(fallback)
    middle = len(cleaned) // 2
    if len(cleaned) % 2:
        return cleaned[middle]
    return (cleaned[middle - 1] + cleaned[middle]) / 2.0


def duration_seconds(start: Optional[datetime], end: Optional[datetime]) -> float:
    if start is None or end is None:
        return 0.0
    return max(0.0, float((end - start).total_seconds()))


def point_to_state(point: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if point is None:
        return None
    return {
        "tickid": int(point["tickid"]),
        "timestamp": dt_to_text(point.get("timestamp")),
        "price": safe_float(point.get("price")),
        "kind": point.get("kind"),
        "sourceid": point.get("sourceid"),
        "confirmtickid": point.get("confirmtickid"),
        "confirmtime": dt_to_text(point.get("confirmtime")),
        "seq": point.get("seq"),
    }


def point_from_state(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    return {
        "tickid": int(payload["tickid"]),
        "timestamp": text_to_dt(payload.get("timestamp")),
        "price": safe_float(payload.get("price")),
        "kind": payload.get("kind"),
        "sourceid": payload.get("sourceid"),
        "confirmtickid": payload.get("confirmtickid"),
        "confirmtime": text_to_dt(payload.get("confirmtime")),
        "seq": payload.get("seq"),
    }


def tick_mid_price(row: Dict[str, Any]) -> float:
    mid = row.get("mid")
    if mid is not None:
        return safe_float(mid)
    bid = row.get("bid")
    ask = row.get("ask")
    if bid is not None and ask is not None:
        return (safe_float(bid) + safe_float(ask)) / 2.0
    return safe_float(row.get("price"))


def make_point(
    *,
    tickid: int,
    timestamp: datetime,
    price: float,
    kind: str,
    sourceid: Optional[int] = None,
    confirmtickid: Optional[int] = None,
    confirmtime: Optional[datetime] = None,
    seq: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "tickid": int(tickid),
        "timestamp": timestamp,
        "price": safe_float(price),
        "kind": kind,
        "sourceid": sourceid,
        "confirmtickid": confirmtickid if confirmtickid is not None else int(tickid),
        "confirmtime": confirmtime if confirmtime is not None else timestamp,
        "seq": seq,
    }


def build_segment_record(
    *,
    level: str,
    start_point: Dict[str, Any],
    end_point: Dict[str, Any],
    confirmtickid: int,
    confirmtime: datetime,
    score: float,
    childcount: int = 0,
    childstartid: Optional[int] = None,
    childendid: Optional[int] = None,
) -> Dict[str, Any]:
    start_price = safe_float(start_point["price"])
    end_price = safe_float(end_point["price"])
    direction = 1 if end_price >= start_price else -1
    return {
        "level": level,
        "symbol": None,
        "starttickid": int(start_point["tickid"]),
        "endtickid": int(end_point["tickid"]),
        "confirmtickid": int(confirmtickid),
        "starttime": start_point["timestamp"],
        "endtime": end_point["timestamp"],
        "confirmtime": confirmtime,
        "startprice": start_price,
        "endprice": end_price,
        "highprice": max(start_price, end_price),
        "lowprice": min(start_price, end_price),
        "dir": direction,
        "tickcount": max(1, int(end_point["tickid"]) - int(start_point["tickid"]) + 1),
        "childcount": max(0, int(childcount)),
        "dursec": duration_seconds(start_point["timestamp"], end_point["timestamp"]),
        "amplitude": abs(end_price - start_price),
        "score": round(float(score), 6),
        "status": "confirmed",
        "childstartid": childstartid,
        "childendid": childendid,
        "parentid": None,
    }


@dataclass(frozen=True)
class StructuralSettings:
    level: str
    seed_ratio: float
    retrace_ratio: float
    leg_ratio: float
    time_ratio: float
    speed_ratio: float
    confirm_children: float
    failure_target: float
    near_noise_mult: float
    escape_noise_mult: float
    distance_weight: float
    time_weight: float
    speed_weight: float
    escape_weight: float
    rejection_weight: float
    min_score: float
    fallback_amp: float
    fallback_ticks: int


SETTINGS = {
    "micro": StructuralSettings(
        level="micro",
        seed_ratio=1.15,
        retrace_ratio=0.58,
        leg_ratio=0.38,
        time_ratio=0.28,
        speed_ratio=0.62,
        confirm_children=3.0,
        failure_target=2.0,
        near_noise_mult=1.6,
        escape_noise_mult=2.2,
        distance_weight=1.55,
        time_weight=0.42,
        speed_weight=0.86,
        escape_weight=0.54,
        rejection_weight=0.46,
        min_score=2.7,
        fallback_amp=0.45,
        fallback_ticks=18,
    ),
    "med": StructuralSettings(
        level="med",
        seed_ratio=1.1,
        retrace_ratio=0.62,
        leg_ratio=0.4,
        time_ratio=0.3,
        speed_ratio=0.64,
        confirm_children=2.0,
        failure_target=1.0,
        near_noise_mult=1.0,
        escape_noise_mult=1.0,
        distance_weight=1.48,
        time_weight=0.38,
        speed_weight=0.76,
        escape_weight=0.52,
        rejection_weight=0.48,
        min_score=2.6,
        fallback_amp=0.8,
        fallback_ticks=42,
    ),
    "maxi": StructuralSettings(
        level="maxi",
        seed_ratio=1.15,
        retrace_ratio=0.68,
        leg_ratio=0.45,
        time_ratio=0.34,
        speed_ratio=0.66,
        confirm_children=2.0,
        failure_target=1.0,
        near_noise_mult=1.0,
        escape_noise_mult=1.0,
        distance_weight=1.52,
        time_weight=0.36,
        speed_weight=0.74,
        escape_weight=0.5,
        rejection_weight=0.52,
        min_score=2.8,
        fallback_amp=1.25,
        fallback_ticks=96,
    ),
    "macro": StructuralSettings(
        level="macro",
        seed_ratio=1.2,
        retrace_ratio=0.74,
        leg_ratio=0.5,
        time_ratio=0.38,
        speed_ratio=0.7,
        confirm_children=2.0,
        failure_target=1.0,
        near_noise_mult=1.0,
        escape_noise_mult=1.0,
        distance_weight=1.58,
        time_weight=0.34,
        speed_weight=0.72,
        escape_weight=0.46,
        rejection_weight=0.56,
        min_score=3.0,
        fallback_amp=2.0,
        fallback_ticks=180,
    ),
}


class MicroZigEngine:
    def __init__(self, state: Optional[Dict[str, Any]] = None):
        self.settings = SETTINGS["micro"]
        self.seed_high: Optional[Dict[str, Any]] = None
        self.seed_low: Optional[Dict[str, Any]] = None
        self.last_confirmed: Optional[Dict[str, Any]] = None
        self.candidate: Optional[Dict[str, Any]] = None
        self.direction = 0
        self.counter_price: Optional[float] = None
        self.counter_tickid: Optional[int] = None
        self.counter_time: Optional[datetime] = None
        self.escape_ticks = 0
        self.failed_retests = 0
        self.near_band_active = False
        self.recent_amplitudes: Deque[float] = deque(maxlen=24)
        self.recent_tickcounts: Deque[float] = deque(maxlen=24)
        self.recent_speeds: Deque[float] = deque(maxlen=24)
        self.recent_spreads: Deque[float] = deque(maxlen=48)
        self.recent_moves: Deque[float] = deque(maxlen=64)
        self.last_mid: Optional[float] = None
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.seed_high = point_from_state(state.get("seedhigh"))
        self.seed_low = point_from_state(state.get("seedlow"))
        self.last_confirmed = point_from_state(state.get("lastconfirmed"))
        self.candidate = point_from_state(state.get("candidate"))
        self.direction = int(state.get("direction") or 0)
        self.counter_price = safe_float(state.get("counterprice"), None) if state.get("counterprice") is not None else None
        self.counter_tickid = int(state["countertickid"]) if state.get("countertickid") is not None else None
        self.counter_time = text_to_dt(state.get("countertime"))
        self.escape_ticks = int(state.get("escapeticks") or 0)
        self.failed_retests = int(state.get("failedretests") or 0)
        self.near_band_active = bool(state.get("nearbandactive"))
        self.recent_amplitudes = deque((safe_float(value) for value in state.get("recentamplitudes", [])), maxlen=self.recent_amplitudes.maxlen)
        self.recent_tickcounts = deque((safe_float(value) for value in state.get("recenttickcounts", [])), maxlen=self.recent_tickcounts.maxlen)
        self.recent_speeds = deque((safe_float(value) for value in state.get("recentspeeds", [])), maxlen=self.recent_speeds.maxlen)
        self.recent_spreads = deque((safe_float(value) for value in state.get("recentspreads", [])), maxlen=self.recent_spreads.maxlen)
        self.recent_moves = deque((safe_float(value) for value in state.get("recentmoves", [])), maxlen=self.recent_moves.maxlen)
        self.last_mid = safe_float(state.get("lastmid"), None) if state.get("lastmid") is not None else None

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "seedhigh": point_to_state(self.seed_high),
            "seedlow": point_to_state(self.seed_low),
            "lastconfirmed": point_to_state(self.last_confirmed),
            "candidate": point_to_state(self.candidate),
            "direction": self.direction,
            "counterprice": self.counter_price,
            "countertickid": self.counter_tickid,
            "countertime": dt_to_text(self.counter_time),
            "escapeticks": self.escape_ticks,
            "failedretests": self.failed_retests,
            "nearbandactive": self.near_band_active,
            "recentamplitudes": list(self.recent_amplitudes),
            "recenttickcounts": list(self.recent_tickcounts),
            "recentspeeds": list(self.recent_speeds),
            "recentspreads": list(self.recent_spreads),
            "recentmoves": list(self.recent_moves),
            "lastmid": self.last_mid,
        }

    def _noise_floor(self) -> float:
        spread_floor = median_or_fallback(self.recent_spreads, 0.02) * 2.2
        move_floor = median_or_fallback(self.recent_moves, 0.02) * 3.4
        return max(0.02, spread_floor, move_floor)

    def _reference_amplitude(self, current_leg: float = 0.0) -> float:
        fallback = max(self._noise_floor() * 6.0, abs(float(current_leg)) * 0.75, self.settings.fallback_amp)
        return max(self._noise_floor() * 2.0, median_or_fallback(self.recent_amplitudes, fallback))

    def _reference_ticks(self, current_ticks: int = 0) -> float:
        fallback = max(float(self.settings.fallback_ticks), float(current_ticks or 0))
        return max(2.0, median_or_fallback(self.recent_tickcounts, fallback))

    def _reference_speed(self, reference_amp: float, reference_ticks: float) -> float:
        fallback = reference_amp / max(1.0, reference_ticks)
        return max(self._noise_floor() * 0.08, median_or_fallback(self.recent_speeds, fallback))

    def _reset_reversal(self) -> None:
        self.counter_price = None
        self.counter_tickid = None
        self.counter_time = None
        self.escape_ticks = 0
        self.failed_retests = 0
        self.near_band_active = False

    def _record_segment(self, segment: Dict[str, Any]) -> None:
        amplitude = safe_float(segment["amplitude"])
        tickcount = safe_float(segment["tickcount"])
        dursec = max(0.001, safe_float(segment["dursec"]))
        self.recent_amplitudes.append(amplitude)
        self.recent_tickcounts.append(tickcount)
        self.recent_speeds.append(amplitude / max(1.0, tickcount))
        self.recent_speeds.append(amplitude / dursec)

    def _confirm_segment(self, row: Dict[str, Any], score: float) -> Dict[str, Any]:
        assert self.last_confirmed is not None
        assert self.candidate is not None
        segment = build_segment_record(
            level="micro",
            start_point=self.last_confirmed,
            end_point=self.candidate,
            confirmtickid=int(row["id"]),
            confirmtime=row["timestamp"],
            score=score,
            childcount=0,
            childstartid=None,
            childendid=None,
        )
        self._record_segment(segment)
        confirmed = self.candidate
        self.last_confirmed = confirmed
        self.direction = -1 if self.direction == 1 else 1
        if self.direction == 1:
            self.candidate = make_point(
                tickid=int(row["id"]),
                timestamp=row["timestamp"],
                price=safe_float(row["ask"]),
                kind="high",
            )
        else:
            self.candidate = make_point(
                tickid=int(row["id"]),
                timestamp=row["timestamp"],
                price=safe_float(row["bid"]),
                kind="low",
            )
        self._reset_reversal()
        return segment

    def _update_seed(self, row: Dict[str, Any]) -> None:
        high_point = make_point(
            tickid=int(row["id"]),
            timestamp=row["timestamp"],
            price=safe_float(row["ask"]),
            kind="high",
        )
        low_point = make_point(
            tickid=int(row["id"]),
            timestamp=row["timestamp"],
            price=safe_float(row["bid"]),
            kind="low",
        )
        if self.seed_high is None or safe_float(high_point["price"]) >= safe_float(self.seed_high["price"]):
            self.seed_high = high_point
        if self.seed_low is None or safe_float(low_point["price"]) <= safe_float(self.seed_low["price"]):
            self.seed_low = low_point
        if self.seed_high is None or self.seed_low is None or self.seed_high["tickid"] == self.seed_low["tickid"]:
            return

        observed_range = abs(safe_float(self.seed_high["price"]) - safe_float(self.seed_low["price"]))
        threshold = max(self._noise_floor() * 4.5, self._reference_amplitude(observed_range) * self.settings.seed_ratio)
        if observed_range < threshold:
            return

        if int(self.seed_low["tickid"]) < int(self.seed_high["tickid"]):
            self.last_confirmed = self.seed_low
            self.candidate = self.seed_high
            self.direction = 1
        else:
            self.last_confirmed = self.seed_high
            self.candidate = self.seed_low
            self.direction = -1
        self._reset_reversal()

    def process_tick(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        mid = tick_mid_price(row)
        if self.last_mid is not None:
            self.recent_moves.append(abs(mid - self.last_mid))
        self.last_mid = mid
        self.recent_spreads.append(abs(safe_float(row.get("ask")) - safe_float(row.get("bid"))))

        if self.last_confirmed is None or self.candidate is None or self.direction == 0:
            self._update_seed(row)
            return []

        ask = safe_float(row["ask"])
        bid = safe_float(row["bid"])
        noise_floor = self._noise_floor()

        if self.direction == 1:
            if ask >= safe_float(self.candidate["price"]):
                self.candidate = make_point(
                    tickid=int(row["id"]),
                    timestamp=row["timestamp"],
                    price=ask,
                    kind="high",
                )
                self._reset_reversal()
                return []
            self.counter_price = bid if self.counter_price is None else min(self.counter_price, bid)
            if self.counter_price == bid:
                self.counter_tickid = int(row["id"])
                self.counter_time = row["timestamp"]
            near_band = noise_floor * self.settings.near_noise_mult
            escape_band = noise_floor * self.settings.escape_noise_mult
            if ask >= (safe_float(self.candidate["price"]) - near_band):
                self.near_band_active = True
            elif self.near_band_active:
                self.failed_retests += 1
                self.near_band_active = False
            if bid <= (safe_float(self.candidate["price"]) - escape_band):
                self.escape_ticks += 1

            retrace = max(0.0, safe_float(self.candidate["price"]) - safe_float(self.counter_price, bid))
            leg_amp = max(0.0, safe_float(self.candidate["price"]) - safe_float(self.last_confirmed["price"]))
            ticks_since = max(1, int(row["id"]) - int(self.candidate["tickid"]))
            reference_amp = self._reference_amplitude(leg_amp)
            reference_ticks = self._reference_ticks(ticks_since)
            reference_speed = self._reference_speed(reference_amp, reference_ticks)
            distance_gate = max(noise_floor * 3.0, reference_amp * self.settings.retrace_ratio, leg_amp * self.settings.leg_ratio)
            distance_score = retrace / max(distance_gate, 0.000001)
            time_score = ticks_since / max(1.0, reference_ticks * self.settings.time_ratio)
            speed_score = (retrace / max(1.0, ticks_since)) / max(reference_speed * self.settings.speed_ratio, 0.000001)
            escape_score = self.escape_ticks / max(1.0, self.settings.confirm_children)
            rejection_score = self.failed_retests / max(1.0, self.settings.failure_target)
            family_hits = sum(score >= 1.0 for score in (time_score, speed_score, escape_score, rejection_score))
            weighted_score = (
                (distance_score * self.settings.distance_weight)
                + (min(time_score, 2.0) * self.settings.time_weight)
                + (min(speed_score, 2.0) * self.settings.speed_weight)
                + (min(escape_score, 2.0) * self.settings.escape_weight)
                + (min(rejection_score, 2.0) * self.settings.rejection_weight)
            )
            if distance_score >= 1.0 and family_hits >= 1 and weighted_score >= self.settings.min_score:
                return [self._confirm_segment(row, weighted_score)]
            return []

        if bid <= safe_float(self.candidate["price"]):
            self.candidate = make_point(
                tickid=int(row["id"]),
                timestamp=row["timestamp"],
                price=bid,
                kind="low",
            )
            self._reset_reversal()
            return []
        self.counter_price = ask if self.counter_price is None else max(self.counter_price, ask)
        if self.counter_price == ask:
            self.counter_tickid = int(row["id"])
            self.counter_time = row["timestamp"]
        near_band = noise_floor * self.settings.near_noise_mult
        escape_band = noise_floor * self.settings.escape_noise_mult
        if bid <= (safe_float(self.candidate["price"]) + near_band):
            self.near_band_active = True
        elif self.near_band_active:
            self.failed_retests += 1
            self.near_band_active = False
        if ask >= (safe_float(self.candidate["price"]) + escape_band):
            self.escape_ticks += 1

        retrace = max(0.0, safe_float(self.counter_price, ask) - safe_float(self.candidate["price"]))
        leg_amp = max(0.0, safe_float(self.last_confirmed["price"]) - safe_float(self.candidate["price"]))
        ticks_since = max(1, int(row["id"]) - int(self.candidate["tickid"]))
        reference_amp = self._reference_amplitude(leg_amp)
        reference_ticks = self._reference_ticks(ticks_since)
        reference_speed = self._reference_speed(reference_amp, reference_ticks)
        distance_gate = max(noise_floor * 3.0, reference_amp * self.settings.retrace_ratio, leg_amp * self.settings.leg_ratio)
        distance_score = retrace / max(distance_gate, 0.000001)
        time_score = ticks_since / max(1.0, reference_ticks * self.settings.time_ratio)
        speed_score = (retrace / max(1.0, ticks_since)) / max(reference_speed * self.settings.speed_ratio, 0.000001)
        escape_score = self.escape_ticks / max(1.0, self.settings.confirm_children)
        rejection_score = self.failed_retests / max(1.0, self.settings.failure_target)
        family_hits = sum(score >= 1.0 for score in (time_score, speed_score, escape_score, rejection_score))
        weighted_score = (
            (distance_score * self.settings.distance_weight)
            + (min(time_score, 2.0) * self.settings.time_weight)
            + (min(speed_score, 2.0) * self.settings.speed_weight)
            + (min(escape_score, 2.0) * self.settings.escape_weight)
            + (min(rejection_score, 2.0) * self.settings.rejection_weight)
        )
        if distance_score >= 1.0 and family_hits >= 1 and weighted_score >= self.settings.min_score:
            return [self._confirm_segment(row, weighted_score)]
        return []


class StructuralLevelEngine:
    def __init__(self, level: str, state: Optional[Dict[str, Any]] = None):
        if level not in ("med", "maxi", "macro"):
            raise ValueError("Unsupported structural level: {0}".format(level))
        self.level = level
        self.settings = SETTINGS[level]
        self.seed_high: Optional[Dict[str, Any]] = None
        self.seed_low: Optional[Dict[str, Any]] = None
        self.last_confirmed: Optional[Dict[str, Any]] = None
        self.candidate: Optional[Dict[str, Any]] = None
        self.direction = 0
        self.counter_event: Optional[Dict[str, Any]] = None
        self.opposite_count = 0
        self.continuation_failures = 0
        self.leg_start_child_id: Optional[int] = None
        self.event_seq = 0
        self.recent_amplitudes: Deque[float] = deque(maxlen=24)
        self.recent_tickcounts: Deque[float] = deque(maxlen=24)
        self.recent_dursecs: Deque[float] = deque(maxlen=24)
        self.recent_childcounts: Deque[float] = deque(maxlen=24)
        self.recent_speeds: Deque[float] = deque(maxlen=24)
        self.recent_input_amplitudes: Deque[float] = deque(maxlen=40)
        self.recent_input_tickcounts: Deque[float] = deque(maxlen=40)
        self.recent_input_dursecs: Deque[float] = deque(maxlen=40)
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.seed_high = point_from_state(state.get("seedhigh"))
        self.seed_low = point_from_state(state.get("seedlow"))
        self.last_confirmed = point_from_state(state.get("lastconfirmed"))
        self.candidate = point_from_state(state.get("candidate"))
        self.direction = int(state.get("direction") or 0)
        self.counter_event = point_from_state(state.get("counterevent"))
        self.opposite_count = int(state.get("oppositecount") or 0)
        self.continuation_failures = int(state.get("continuationfailures") or 0)
        self.leg_start_child_id = state.get("legstartchildid")
        self.event_seq = int(state.get("eventseq") or 0)
        self.recent_amplitudes = deque((safe_float(value) for value in state.get("recentamplitudes", [])), maxlen=self.recent_amplitudes.maxlen)
        self.recent_tickcounts = deque((safe_float(value) for value in state.get("recenttickcounts", [])), maxlen=self.recent_tickcounts.maxlen)
        self.recent_dursecs = deque((safe_float(value) for value in state.get("recentdursecs", [])), maxlen=self.recent_dursecs.maxlen)
        self.recent_childcounts = deque((safe_float(value) for value in state.get("recentchildcounts", [])), maxlen=self.recent_childcounts.maxlen)
        self.recent_speeds = deque((safe_float(value) for value in state.get("recentspeeds", [])), maxlen=self.recent_speeds.maxlen)
        self.recent_input_amplitudes = deque((safe_float(value) for value in state.get("recentinputamplitudes", [])), maxlen=self.recent_input_amplitudes.maxlen)
        self.recent_input_tickcounts = deque((safe_float(value) for value in state.get("recentinputtickcounts", [])), maxlen=self.recent_input_tickcounts.maxlen)
        self.recent_input_dursecs = deque((safe_float(value) for value in state.get("recentinputdursecs", [])), maxlen=self.recent_input_dursecs.maxlen)

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "seedhigh": point_to_state(self.seed_high),
            "seedlow": point_to_state(self.seed_low),
            "lastconfirmed": point_to_state(self.last_confirmed),
            "candidate": point_to_state(self.candidate),
            "direction": self.direction,
            "counterevent": point_to_state(self.counter_event),
            "oppositecount": self.opposite_count,
            "continuationfailures": self.continuation_failures,
            "legstartchildid": self.leg_start_child_id,
            "eventseq": self.event_seq,
            "recentamplitudes": list(self.recent_amplitudes),
            "recenttickcounts": list(self.recent_tickcounts),
            "recentdursecs": list(self.recent_dursecs),
            "recentchildcounts": list(self.recent_childcounts),
            "recentspeeds": list(self.recent_speeds),
            "recentinputamplitudes": list(self.recent_input_amplitudes),
            "recentinputtickcounts": list(self.recent_input_tickcounts),
            "recentinputdursecs": list(self.recent_input_dursecs),
        }

    def _record_input(self, child: Dict[str, Any]) -> Dict[str, Any]:
        self.event_seq += 1
        self.recent_input_amplitudes.append(safe_float(child.get("amplitude")))
        self.recent_input_tickcounts.append(max(1.0, safe_float(child.get("tickcount"), 1.0)))
        self.recent_input_dursecs.append(max(0.001, safe_float(child.get("dursec"), 0.001)))
        return make_point(
            tickid=int(child["endtickid"]),
            timestamp=child["endtime"],
            price=safe_float(child["endprice"]),
            kind="high" if int(child["dir"]) == 1 else "low",
            sourceid=int(child["id"]),
            confirmtickid=int(child["confirmtickid"]),
            confirmtime=child["confirmtime"],
            seq=self.event_seq,
        )

    def _reset_reversal(self) -> None:
        self.counter_event = None
        self.opposite_count = 0
        self.continuation_failures = 0

    def _input_reference_amplitude(self) -> float:
        fallback = max(self.settings.fallback_amp, median_or_fallback(self.recent_input_amplitudes, self.settings.fallback_amp))
        return max(0.05, fallback)

    def _reference_amplitude(self, current_leg: float = 0.0) -> float:
        fallback = max(abs(float(current_leg)) * 0.8, self._input_reference_amplitude() * 1.2, self.settings.fallback_amp)
        return max(0.05, median_or_fallback(self.recent_amplitudes, fallback))

    def _reference_ticks(self, current_ticks: int = 0) -> float:
        child_fallback = median_or_fallback(self.recent_input_tickcounts, self.settings.fallback_ticks / 2.0)
        fallback = max(float(self.settings.fallback_ticks), float(current_ticks or 0), child_fallback * 1.5)
        return max(2.0, median_or_fallback(self.recent_tickcounts, fallback))

    def _reference_dursec(self, current_dursec: float = 0.0) -> float:
        child_fallback = median_or_fallback(self.recent_input_dursecs, 1.0)
        fallback = max(1.0, float(current_dursec or 0.0), child_fallback * 1.75)
        return max(0.25, median_or_fallback(self.recent_dursecs, fallback))

    def _reference_speed(self, reference_amp: float, reference_dursec: float) -> float:
        fallback = reference_amp / max(0.25, reference_dursec)
        return max(0.0001, median_or_fallback(self.recent_speeds, fallback))

    def _record_segment(self, segment: Dict[str, Any]) -> None:
        amplitude = safe_float(segment["amplitude"])
        tickcount = safe_float(segment["tickcount"])
        dursec = max(0.001, safe_float(segment["dursec"]))
        childcount = max(1.0, safe_float(segment["childcount"], 1.0))
        self.recent_amplitudes.append(amplitude)
        self.recent_tickcounts.append(tickcount)
        self.recent_dursecs.append(dursec)
        self.recent_childcounts.append(childcount)
        self.recent_speeds.append(amplitude / max(1.0, tickcount))
        self.recent_speeds.append(amplitude / dursec)

    def _seed_from_event(self, event: Dict[str, Any]) -> None:
        if self.seed_high is None or (event["kind"] == "high" and safe_float(event["price"]) >= safe_float(self.seed_high["price"])):
            if event["kind"] == "high":
                self.seed_high = event
        if self.seed_low is None or (event["kind"] == "low" and safe_float(event["price"]) <= safe_float(self.seed_low["price"])):
            if event["kind"] == "low":
                self.seed_low = event
        if self.seed_high is None or self.seed_low is None or self.seed_high["tickid"] == self.seed_low["tickid"]:
            return

        observed_range = abs(safe_float(self.seed_high["price"]) - safe_float(self.seed_low["price"]))
        threshold = max(self._input_reference_amplitude() * self.settings.seed_ratio, self.settings.fallback_amp)
        if observed_range < threshold:
            return

        if int(self.seed_low["tickid"]) < int(self.seed_high["tickid"]):
            self.last_confirmed = self.seed_low
            self.candidate = self.seed_high
            self.direction = 1
        else:
            self.last_confirmed = self.seed_high
            self.candidate = self.seed_low
            self.direction = -1
        self.leg_start_child_id = self.candidate.get("sourceid")
        self._reset_reversal()

    def _confirm_segment(self, confirmation_event: Dict[str, Any], next_candidate: Dict[str, Any], score: float) -> Dict[str, Any]:
        assert self.last_confirmed is not None
        assert self.candidate is not None
        childcount = max(1, int(self.candidate["seq"]) - int(self.last_confirmed["seq"] or 0))
        segment = build_segment_record(
            level=self.level,
            start_point=self.last_confirmed,
            end_point=self.candidate,
            confirmtickid=int(confirmation_event["confirmtickid"]),
            confirmtime=confirmation_event["confirmtime"],
            score=score,
            childcount=childcount,
            childstartid=self.leg_start_child_id,
            childendid=self.candidate.get("sourceid"),
        )
        self._record_segment(segment)
        self.last_confirmed = self.candidate
        self.direction = -1 if self.direction == 1 else 1
        self.candidate = next_candidate
        self.leg_start_child_id = next_candidate.get("sourceid")
        self._reset_reversal()
        return segment

    def process_child(self, child: Dict[str, Any]) -> List[Dict[str, Any]]:
        event = self._record_input(child)
        if self.last_confirmed is None or self.candidate is None or self.direction == 0:
            self._seed_from_event(event)
            return []

        if self.direction == 1:
            if event["kind"] == "high":
                if safe_float(event["price"]) >= safe_float(self.candidate["price"]):
                    self.candidate = event
                    if self.leg_start_child_id is None:
                        self.leg_start_child_id = event.get("sourceid")
                    self._reset_reversal()
                else:
                    self.continuation_failures += 1
                return []

            self.counter_event = event if self.counter_event is None or safe_float(event["price"]) <= safe_float(self.counter_event["price"]) else self.counter_event
            self.opposite_count += 1
            retrace = max(0.0, safe_float(self.candidate["price"]) - safe_float(self.counter_event["price"]))
            leg_amp = max(0.0, safe_float(self.candidate["price"]) - safe_float(self.last_confirmed["price"]))
            tick_delay = max(1, int(event["confirmtickid"]) - int(self.candidate["confirmtickid"]))
            dursec = max(0.001, duration_seconds(self.candidate["confirmtime"], event["confirmtime"]))
            reference_amp = self._reference_amplitude(leg_amp)
            reference_ticks = self._reference_ticks(tick_delay)
            reference_dursec = self._reference_dursec(dursec)
            reference_speed = self._reference_speed(reference_amp, reference_dursec)
            distance_gate = max(self._input_reference_amplitude(), reference_amp * self.settings.retrace_ratio, leg_amp * self.settings.leg_ratio)
            distance_score = retrace / max(distance_gate, 0.000001)
            count_score = self.opposite_count / max(1.0, self.settings.confirm_children)
            rejection_score = self.continuation_failures / max(1.0, self.settings.failure_target)
            time_score = tick_delay / max(1.0, reference_ticks * self.settings.time_ratio)
            speed_score = (retrace / dursec) / max(reference_speed * self.settings.speed_ratio, 0.000001)
            family_hits = sum(score >= 1.0 for score in (count_score, rejection_score, time_score, speed_score))
            weighted_score = (
                (distance_score * self.settings.distance_weight)
                + (min(count_score, 2.0) * self.settings.escape_weight)
                + (min(rejection_score, 2.0) * self.settings.rejection_weight)
                + (min(time_score, 2.0) * self.settings.time_weight)
                + (min(speed_score, 2.0) * self.settings.speed_weight)
            )
            if distance_score >= 1.0 and family_hits >= 1 and weighted_score >= self.settings.min_score:
                next_candidate = self.counter_event or event
                return [self._confirm_segment(event, next_candidate, weighted_score)]
            return []

        if event["kind"] == "low":
            if safe_float(event["price"]) <= safe_float(self.candidate["price"]):
                self.candidate = event
                if self.leg_start_child_id is None:
                    self.leg_start_child_id = event.get("sourceid")
                self._reset_reversal()
            else:
                self.continuation_failures += 1
            return []

        self.counter_event = event if self.counter_event is None or safe_float(event["price"]) >= safe_float(self.counter_event["price"]) else self.counter_event
        self.opposite_count += 1
        retrace = max(0.0, safe_float(self.counter_event["price"]) - safe_float(self.candidate["price"]))
        leg_amp = max(0.0, safe_float(self.last_confirmed["price"]) - safe_float(self.candidate["price"]))
        tick_delay = max(1, int(event["confirmtickid"]) - int(self.candidate["confirmtickid"]))
        dursec = max(0.001, duration_seconds(self.candidate["confirmtime"], event["confirmtime"]))
        reference_amp = self._reference_amplitude(leg_amp)
        reference_ticks = self._reference_ticks(tick_delay)
        reference_dursec = self._reference_dursec(dursec)
        reference_speed = self._reference_speed(reference_amp, reference_dursec)
        distance_gate = max(self._input_reference_amplitude(), reference_amp * self.settings.retrace_ratio, leg_amp * self.settings.leg_ratio)
        distance_score = retrace / max(distance_gate, 0.000001)
        count_score = self.opposite_count / max(1.0, self.settings.confirm_children)
        rejection_score = self.continuation_failures / max(1.0, self.settings.failure_target)
        time_score = tick_delay / max(1.0, reference_ticks * self.settings.time_ratio)
        speed_score = (retrace / dursec) / max(reference_speed * self.settings.speed_ratio, 0.000001)
        family_hits = sum(score >= 1.0 for score in (count_score, rejection_score, time_score, speed_score))
        weighted_score = (
            (distance_score * self.settings.distance_weight)
            + (min(count_score, 2.0) * self.settings.escape_weight)
            + (min(rejection_score, 2.0) * self.settings.rejection_weight)
            + (min(time_score, 2.0) * self.settings.time_weight)
            + (min(speed_score, 2.0) * self.settings.speed_weight)
        )
        if distance_score >= 1.0 and family_hits >= 1 and weighted_score >= self.settings.min_score:
            next_candidate = self.counter_event or event
            return [self._confirm_segment(event, next_candidate, weighted_score)]
        return []


def segment_sort_key(row: Dict[str, Any]) -> Any:
    return (
        int(row.get("confirmtickid") or 0),
        int(row.get("endtickid") or 0),
        int(row.get("starttickid") or 0),
        int(row.get("id") or 0),
    )


class ZigPipeline:
    def __init__(self, symbol: str, state: Optional[Dict[str, Any]] = None):
        payload = state or {}
        self.symbol = symbol
        self.micro = MicroZigEngine(payload.get("micro"))
        self.med = StructuralLevelEngine("med", payload.get("med"))
        self.maxi = StructuralLevelEngine("maxi", payload.get("maxi"))
        self.macro = StructuralLevelEngine("macro", payload.get("macro"))

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "micro": self.micro.snapshot_state(),
            "med": self.med.snapshot_state(),
            "maxi": self.maxi.snapshot_state(),
            "macro": self.macro.snapshot_state(),
        }

    def process_ticks(self, rows: Sequence[Dict[str, Any]], persist) -> Dict[str, List[Dict[str, Any]]]:
        micro_rows: List[Dict[str, Any]] = []
        for row in rows:
            for segment in self.micro.process_tick(row):
                segment["symbol"] = self.symbol
                micro_rows.append(segment)
        saved_micro = sorted(persist("micro", micro_rows), key=segment_sort_key)

        med_rows: List[Dict[str, Any]] = []
        for row in saved_micro:
            for segment in self.med.process_child(row):
                segment["symbol"] = self.symbol
                med_rows.append(segment)
        saved_med = sorted(persist("med", med_rows), key=segment_sort_key)

        maxi_rows: List[Dict[str, Any]] = []
        for row in saved_med:
            for segment in self.maxi.process_child(row):
                segment["symbol"] = self.symbol
                maxi_rows.append(segment)
        saved_maxi = sorted(persist("maxi", maxi_rows), key=segment_sort_key)

        macro_rows: List[Dict[str, Any]] = []
        for row in saved_maxi:
            for segment in self.macro.process_child(row):
                segment["symbol"] = self.symbol
                macro_rows.append(segment)
        saved_macro = sorted(persist("macro", macro_rows), key=segment_sort_key)

        return {
            "micro": saved_micro,
            "med": saved_med,
            "maxi": saved_maxi,
            "macro": saved_macro,
        }
