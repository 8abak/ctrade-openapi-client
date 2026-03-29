from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Sequence


PIPELINE_VERSION = "deterministic-centered-window-v1"

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

MICRO_WINDOW = 15
MICRO_CENTER = 7

LEVEL_WINDOWS = {
    "med": 9,
    "maxi": 7,
    "macro": 5,
}

LEVEL_CENTERS = {
    "med": 4,
    "maxi": 3,
    "macro": 2,
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


def maybe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def dt_to_text(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def text_to_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


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
        "confirmtickid": int(payload["confirmtickid"]) if payload.get("confirmtickid") is not None else None,
        "confirmtime": text_to_dt(payload.get("confirmtime")),
        "seq": int(payload["seq"]) if payload.get("seq") is not None else None,
    }


def tick_to_state(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "timestamp": dt_to_text(row.get("timestamp")),
        "bid": row.get("bid"),
        "ask": row.get("ask"),
    }


def tick_from_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(payload["id"]),
        "timestamp": text_to_dt(payload.get("timestamp")),
        "bid": payload.get("bid"),
        "ask": payload.get("ask"),
    }


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
        "confirmtickid": int(confirmtickid) if confirmtickid is not None else int(tickid),
        "confirmtime": confirmtime if confirmtime is not None else timestamp,
        "seq": int(seq) if seq is not None else None,
    }


def segment_sort_key(row: Dict[str, Any]) -> Any:
    return (
        int(row.get("confirmtickid") or 0),
        int(row.get("endtickid") or 0),
        int(row.get("starttickid") or 0),
        int(row.get("id") or 0),
    )


def point_from_segment(row: Dict[str, Any]) -> Dict[str, Any]:
    return make_point(
        tickid=int(row["endtickid"]),
        timestamp=row["endtime"],
        price=safe_float(row["endprice"]),
        kind="high" if int(row["dir"]) == 1 else "low",
        sourceid=int(row["id"]) if row.get("id") is not None else None,
        confirmtickid=int(row["confirmtickid"]),
        confirmtime=row["confirmtime"],
    )


def stronger_same_side(new_point: Dict[str, Any], current_point: Dict[str, Any]) -> bool:
    if new_point["kind"] != current_point["kind"]:
        return False
    if new_point["kind"] == "high":
        return safe_float(new_point["price"]) > safe_float(current_point["price"])
    return safe_float(new_point["price"]) < safe_float(current_point["price"])


def build_segment_record(
    *,
    level: str,
    start_point: Dict[str, Any],
    end_point: Dict[str, Any],
    confirmtickid: int,
    confirmtime: datetime,
) -> Dict[str, Any]:
    start_price = safe_float(start_point["price"])
    end_price = safe_float(end_point["price"])
    direction = 1 if end_price >= start_price else -1
    if level == "micro":
        childcount = 0
        childstartid = None
        childendid = None
    else:
        start_seq = int(start_point.get("seq") or 0)
        end_seq = int(end_point.get("seq") or 0)
        childcount = max(1, end_seq - start_seq) if start_seq and end_seq else 0
        childstartid = start_point.get("sourceid")
        childendid = end_point.get("sourceid")
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
        "childcount": childcount,
        "dursec": duration_seconds(start_point["timestamp"], end_point["timestamp"]),
        "amplitude": abs(end_price - start_price),
        "score": 0.0,
        "status": "confirmed",
        "childstartid": childstartid,
        "childendid": childendid,
        "parentid": None,
    }


class AlternatingPivotEngine:
    def __init__(self, level: str):
        self.level = level
        self.last_confirmed: Optional[Dict[str, Any]] = None
        self.candidate: Optional[Dict[str, Any]] = None

    def load_state(self, state: Dict[str, Any]) -> None:
        self.last_confirmed = point_from_state(state.get("lastconfirmed"))
        self.candidate = point_from_state(state.get("candidate"))

    def snapshot_state(self) -> Dict[str, Any]:
        direction = 0
        if self.candidate:
            direction = 1 if self.candidate.get("kind") == "high" else -1
        return {
            "lastconfirmed": point_to_state(self.last_confirmed),
            "candidate": point_to_state(self.candidate),
            "direction": direction,
            "counterevent": None,
        }

    def push(self, pivot: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.last_confirmed is None and self.candidate is None:
            self.candidate = pivot
            return []

        if self.last_confirmed is None:
            assert self.candidate is not None
            if pivot["kind"] == self.candidate["kind"]:
                if stronger_same_side(pivot, self.candidate):
                    self.candidate = pivot
                return []
            self.last_confirmed = self.candidate
            self.candidate = pivot
            return []

        assert self.candidate is not None
        if pivot["kind"] == self.candidate["kind"]:
            if stronger_same_side(pivot, self.candidate):
                self.candidate = pivot
            return []

        segment = build_segment_record(
            level=self.level,
            start_point=self.last_confirmed,
            end_point=self.candidate,
            confirmtickid=int(pivot["confirmtickid"]),
            confirmtime=pivot["confirmtime"],
        )
        self.last_confirmed = self.candidate
        self.candidate = pivot
        return [segment]


class MicroZigEngine:
    def __init__(self, state: Optional[Dict[str, Any]] = None):
        self.window: Deque[Dict[str, Any]] = deque(maxlen=MICRO_WINDOW)
        self.alternating = AlternatingPivotEngine("micro")
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.window = deque((tick_from_state(item) for item in state.get("window", [])), maxlen=MICRO_WINDOW)
        self.alternating.load_state(state)

    def snapshot_state(self) -> Dict[str, Any]:
        payload = self.alternating.snapshot_state()
        payload["window"] = [tick_to_state(item) for item in self.window]
        return payload

    def _detect_center_pivot(self, confirm_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if len(self.window) < MICRO_WINDOW:
            return None
        window = list(self.window)
        center_row = window[MICRO_CENTER]
        ask_prices = [maybe_float(item.get("ask")) for item in window]
        bid_prices = [maybe_float(item.get("bid")) for item in window]
        if any(value is None for value in ask_prices) or any(value is None for value in bid_prices):
            return None

        center_ask = ask_prices[MICRO_CENTER]
        center_bid = bid_prices[MICRO_CENTER]
        assert center_ask is not None
        assert center_bid is not None

        highest = max(ask_prices)
        lowest = min(bid_prices)
        high_unique = center_ask == highest and sum(1 for value in ask_prices if value == highest) == 1
        low_unique = center_bid == lowest and sum(1 for value in bid_prices if value == lowest) == 1

        if high_unique and low_unique:
            return None
        if high_unique:
            return make_point(
                tickid=int(center_row["id"]),
                timestamp=center_row["timestamp"],
                price=center_ask,
                kind="high",
                sourceid=int(center_row["id"]),
                confirmtickid=int(confirm_row["id"]),
                confirmtime=confirm_row["timestamp"],
            )
        if low_unique:
            return make_point(
                tickid=int(center_row["id"]),
                timestamp=center_row["timestamp"],
                price=center_bid,
                kind="low",
                sourceid=int(center_row["id"]),
                confirmtickid=int(confirm_row["id"]),
                confirmtime=confirm_row["timestamp"],
            )
        return None

    def process_tick(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.window.append(
            {
                "id": int(row["id"]),
                "timestamp": row["timestamp"],
                "bid": row.get("bid"),
                "ask": row.get("ask"),
            }
        )
        candidate = self._detect_center_pivot(row)
        if candidate is None:
            return []
        return self.alternating.push(candidate)


class StructuralLevelEngine:
    def __init__(self, level: str, state: Optional[Dict[str, Any]] = None):
        if level not in LEVEL_WINDOWS:
            raise ValueError("Unsupported structural level: {0}".format(level))
        self.level = level
        self.window_size = LEVEL_WINDOWS[level]
        self.center_index = LEVEL_CENTERS[level]
        self.window: Deque[Dict[str, Any]] = deque(maxlen=self.window_size)
        self.input_seq = 0
        self.alternating = AlternatingPivotEngine(level)
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.input_seq = int(state.get("inputseq") or 0)
        self.window = deque((point_from_state(item) for item in state.get("window", [])), maxlen=self.window_size)
        self.alternating.load_state(state)

    def snapshot_state(self) -> Dict[str, Any]:
        payload = self.alternating.snapshot_state()
        payload["inputseq"] = self.input_seq
        payload["window"] = [point_to_state(item) for item in self.window]
        return payload

    def _detect_center_pivot(self, newest_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if len(self.window) < self.window_size:
            return None
        window = list(self.window)
        center = window[self.center_index]
        prices = [maybe_float(item.get("price")) for item in window]
        if any(value is None for value in prices):
            return None

        center_price = prices[self.center_index]
        assert center_price is not None
        highest = max(prices)
        lowest = min(prices)

        if center.get("kind") == "high":
            if center_price != highest or sum(1 for value in prices if value == highest) != 1:
                return None
        elif center.get("kind") == "low":
            if center_price != lowest or sum(1 for value in prices if value == lowest) != 1:
                return None
        else:
            return None

        return make_point(
            tickid=int(center["tickid"]),
            timestamp=center["timestamp"],
            price=center_price,
            kind=center["kind"],
            sourceid=center.get("sourceid"),
            confirmtickid=int(newest_event["confirmtickid"]),
            confirmtime=newest_event["confirmtime"],
            seq=center.get("seq"),
        )

    def process_child(self, child_row: Dict[str, Any]) -> List[Dict[str, Any]]:
        event = point_from_segment(child_row)
        self.input_seq += 1
        event["seq"] = self.input_seq
        self.window.append(event)
        candidate = self._detect_center_pivot(event)
        if candidate is None:
            return []
        return self.alternating.push(candidate)


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
            "version": PIPELINE_VERSION,
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
