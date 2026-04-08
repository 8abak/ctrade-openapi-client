from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def round_price(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 5)


@dataclass
class StructureBar:
    id: int
    symbol: str
    type: str
    status: str
    start_tick_id: int
    end_tick_id: int
    start_time: datetime
    end_time: datetime
    open: float
    high: float
    low: float
    close: float

    def update(self, tick: Dict[str, Any], close: float) -> None:
        self.end_tick_id = int(tick["id"])
        self.end_time = tick["timestamp"]
        self.high = max(self.high, float(tick["ask"]))
        self.low = min(self.low, float(tick["bid"]))
        self.close = close

    def serialize(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "type": self.type,
            "status": self.status,
            "startTickId": self.start_tick_id,
            "endTickId": self.end_tick_id,
            "startTimestamp": self.start_time.isoformat(),
            "endTimestamp": self.end_time.isoformat(),
            "startTimestampMs": dt_to_ms(self.start_time),
            "endTimestampMs": dt_to_ms(self.end_time),
            "open": round_price(self.open),
            "high": round_price(self.high),
            "low": round_price(self.low),
            "close": round_price(self.close),
        }


@dataclass
class RangeBox:
    id: int
    symbol: str
    status: str
    start_tick_id: int
    end_tick_id: int
    start_time: datetime
    end_time: datetime
    top: float
    bottom: float
    break_direction: Optional[str] = None
    break_tick_id: Optional[int] = None

    def update(self, tick: Dict[str, Any]) -> None:
        self.end_tick_id = int(tick["id"])
        self.end_time = tick["timestamp"]
        self.top = max(self.top, float(tick["ask"]))
        self.bottom = min(self.bottom, float(tick["bid"]))

    def close(self, tick: Dict[str, Any], direction: str) -> None:
        self.status = "closed"
        self.break_direction = direction
        self.break_tick_id = int(tick["id"])
        self.end_tick_id = int(tick["id"])
        self.end_time = tick["timestamp"]

    def serialize(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "status": self.status,
            "startTickId": self.start_tick_id,
            "endTickId": self.end_tick_id,
            "startTimestamp": self.start_time.isoformat(),
            "endTimestamp": self.end_time.isoformat(),
            "startTimestampMs": dt_to_ms(self.start_time),
            "endTimestampMs": dt_to_ms(self.end_time),
            "top": round_price(self.top),
            "bottom": round_price(self.bottom),
            "height": round_price(self.top - self.bottom),
            "breakDirection": self.break_direction,
            "breakTickId": self.break_tick_id,
        }


@dataclass
class StructureEngine:
    symbol: str
    mode: str = "range"
    alpha: float = 0.08
    bars: List[StructureBar] = field(default_factory=list)
    range_boxes: List[RangeBox] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    previous_mid: Optional[float] = None
    ewma_abs_mid_change: float = 0.0
    ewma_spread: float = 0.0
    high_ask: Optional[float] = None
    high_tick: Optional[Dict[str, Any]] = None
    low_bid: Optional[float] = None
    low_tick: Optional[Dict[str, Any]] = None
    last_pullback_size: float = 0.0
    last_range_touch: Optional[str] = None

    @property
    def active_bar(self) -> Optional[StructureBar]:
        return self.bars[-1] if self.bars and self.bars[-1].status == "active" else None

    @property
    def active_range_box(self) -> Optional[RangeBox]:
        return self.range_boxes[-1] if self.range_boxes and self.range_boxes[-1].status == "active" else None

    def process_tick(self, tick: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        tick = dict(tick)
        tick["id"] = int(tick["id"])
        tick["bid"] = float(tick["bid"])
        tick["ask"] = float(tick["ask"])
        if tick.get("mid") is None:
            tick["mid"] = (tick["bid"] + tick["ask"]) / 2.0
        tick["mid"] = float(tick["mid"])
        if tick.get("spread") is None:
            tick["spread"] = tick["ask"] - tick["bid"]
        tick["spread"] = max(0.0, float(tick["spread"]))

        self._update_noise(tick)

        if self.active_bar is None:
            bar = self._open_bar("range", tick)
            box = self._open_range_box(tick)
            self.high_ask = tick["ask"]
            self.high_tick = tick
            self.low_bid = tick["bid"]
            self.low_tick = tick
            return {"bars": [bar.serialize()], "rangeBoxes": [box.serialize()], "events": []}

        updates: Dict[str, List[Dict[str, Any]]] = {"bars": [], "rangeBoxes": [], "events": []}
        if self.mode == "up":
            self._process_up_tick(tick, updates)
        elif self.mode == "down":
            self._process_down_tick(tick, updates)
        else:
            self._process_range_tick(tick, updates)

        return updates

    def _update_noise(self, tick: Dict[str, Any]) -> None:
        mid = tick["mid"]
        if self.previous_mid is None:
            self.ewma_abs_mid_change = 0.0
        else:
            change = abs(mid - self.previous_mid)
            self.ewma_abs_mid_change = (
                self.alpha * change + (1.0 - self.alpha) * self.ewma_abs_mid_change
            )
        self.previous_mid = mid
        spread = tick["spread"]
        if self.ewma_spread <= 0:
            self.ewma_spread = spread
        else:
            self.ewma_spread = self.alpha * spread + (1.0 - self.alpha) * self.ewma_spread

    def _thresholds(self) -> Dict[str, float]:
        noise = max(self.ewma_abs_mid_change * 4.0, self.ewma_spread * 1.4, 0.01)
        move = max(noise, self.ewma_spread * 1.8, 0.02)
        return {
            "move": move,
            "range": move * 1.15,
            "reversal": max(move * 2.2, self.ewma_spread * 3.0, 0.04),
            "breakout": max(move * 1.25, self.ewma_spread * 2.0, 0.03),
        }

    def _open_bar(self, bar_type: str, tick: Dict[str, Any]) -> StructureBar:
        bar = StructureBar(
            id=len(self.bars) + 1,
            symbol=self.symbol,
            type=bar_type,
            status="active",
            start_tick_id=int(tick["id"]),
            end_tick_id=int(tick["id"]),
            start_time=tick["timestamp"],
            end_time=tick["timestamp"],
            open=tick["mid"],
            high=tick["ask"],
            low=tick["bid"],
            close=tick["mid"],
        )
        self.bars.append(bar)
        self.mode = bar_type
        return bar

    def _open_range_box(self, tick: Dict[str, Any]) -> RangeBox:
        box = RangeBox(
            id=len(self.range_boxes) + 1,
            symbol=self.symbol,
            status="active",
            start_tick_id=int(tick["id"]),
            end_tick_id=int(tick["id"]),
            start_time=tick["timestamp"],
            end_time=tick["timestamp"],
            top=tick["ask"],
            bottom=tick["bid"],
        )
        self.range_boxes.append(box)
        self.last_range_touch = None
        return box

    def _close_active_bar(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self.active_bar
        if bar is None:
            return
        bar.update(tick, tick["mid"])
        bar.status = "closed"
        updates["bars"].append(bar.serialize())

    def _event(self, event_type: str, tick: Dict[str, Any], price: float, **extra: Any) -> Dict[str, Any]:
        payload = {
            "id": len(self.events) + 1,
            "symbol": self.symbol,
            "type": event_type,
            "state": self.mode,
            "tickId": int(tick["id"]),
            "timestamp": tick["timestamp"].isoformat(),
            "timestampMs": dt_to_ms(tick["timestamp"]),
            "price": round_price(price),
        }
        payload.update(extra)
        self.events.append(payload)
        return payload

    def _append_event(
        self,
        updates: Dict[str, List[Dict[str, Any]]],
        event_type: str,
        tick: Dict[str, Any],
        price: float,
        **extra: Any,
    ) -> None:
        updates["events"].append(self._event(event_type, tick, price, **extra))

    def _start_up(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self._open_bar("up", tick)
        self.high_ask = tick["ask"]
        self.high_tick = tick
        self.low_bid = tick["bid"]
        self.low_tick = tick
        self.last_pullback_size = 0.0
        updates["bars"].append(bar.serialize())

    def _start_down(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self._open_bar("down", tick)
        self.high_ask = tick["ask"]
        self.high_tick = tick
        self.low_bid = tick["bid"]
        self.low_tick = tick
        self.last_pullback_size = 0.0
        updates["bars"].append(bar.serialize())

    def _start_range(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self._open_bar("range", tick)
        box = self._open_range_box(tick)
        self.high_ask = tick["ask"]
        self.high_tick = tick
        self.low_bid = tick["bid"]
        self.low_tick = tick
        self.last_pullback_size = 0.0
        updates["bars"].append(bar.serialize())
        updates["rangeBoxes"].append(box.serialize())

    def _process_up_tick(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self.active_bar
        if bar is None:
            self._start_up(tick, updates)
            return
        bar.update(tick, tick["mid"])
        thresholds = self._thresholds()

        if self.high_ask is None or tick["ask"] > self.high_ask + thresholds["move"]:
            self.high_ask = tick["ask"]
            self.high_tick = tick
            self.last_pullback_size = 0.0
            self._append_event(updates, "highexpand", tick, tick["ask"])
            updates["bars"].append(bar.serialize())
            return

        counter_move = max(0.0, (self.high_ask or tick["ask"]) - tick["bid"])
        if counter_move >= thresholds["reversal"]:
            self._append_event(updates, "reversalstart", tick, tick["bid"], fromState="up", toState="down")
            self._close_active_bar(tick, updates)
            self._start_down(tick, updates)
            self._append_event(updates, "lowexpand", tick, tick["bid"])
            return

        if counter_move >= thresholds["range"]:
            self._append_event(updates, "pullback", tick, tick["bid"], fromState="up")
            self._close_active_bar(tick, updates)
            self._start_range(tick, updates)
            return

        if counter_move >= thresholds["move"] and counter_move - self.last_pullback_size >= thresholds["move"]:
            self.last_pullback_size = counter_move
            self._append_event(updates, "pullback", tick, tick["bid"], fromState="up")
        updates["bars"].append(bar.serialize())

    def _process_down_tick(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self.active_bar
        if bar is None:
            self._start_down(tick, updates)
            return
        bar.update(tick, tick["mid"])
        thresholds = self._thresholds()

        if self.low_bid is None or tick["bid"] < self.low_bid - thresholds["move"]:
            self.low_bid = tick["bid"]
            self.low_tick = tick
            self.last_pullback_size = 0.0
            self._append_event(updates, "lowexpand", tick, tick["bid"])
            updates["bars"].append(bar.serialize())
            return

        counter_move = max(0.0, tick["ask"] - (self.low_bid or tick["bid"]))
        if counter_move >= thresholds["reversal"]:
            self._append_event(updates, "reversalstart", tick, tick["ask"], fromState="down", toState="up")
            self._close_active_bar(tick, updates)
            self._start_up(tick, updates)
            self._append_event(updates, "highexpand", tick, tick["ask"])
            return

        if counter_move >= thresholds["range"]:
            self._append_event(updates, "pullback", tick, tick["ask"], fromState="down")
            self._close_active_bar(tick, updates)
            self._start_range(tick, updates)
            return

        if counter_move >= thresholds["move"] and counter_move - self.last_pullback_size >= thresholds["move"]:
            self.last_pullback_size = counter_move
            self._append_event(updates, "pullback", tick, tick["ask"], fromState="down")
        updates["bars"].append(bar.serialize())

    def _process_range_tick(self, tick: Dict[str, Any], updates: Dict[str, List[Dict[str, Any]]]) -> None:
        bar = self.active_bar
        box = self.active_range_box
        if bar is None or box is None:
            self._start_range(tick, updates)
            return

        thresholds = self._thresholds()
        prior_top = box.top
        prior_bottom = box.bottom
        bar.update(tick, tick["mid"])

        if tick["ask"] > prior_top + thresholds["breakout"]:
            self._append_event(updates, "rangebreakup", tick, tick["ask"], boxId=box.id)
            box.close(tick, "up")
            updates["rangeBoxes"].append(box.serialize())
            self._close_active_bar(tick, updates)
            self._start_up(tick, updates)
            self._append_event(updates, "highexpand", tick, tick["ask"])
            return

        if tick["bid"] < prior_bottom - thresholds["breakout"]:
            self._append_event(updates, "rangebreakdown", tick, tick["bid"], boxId=box.id)
            box.close(tick, "down")
            updates["rangeBoxes"].append(box.serialize())
            self._close_active_bar(tick, updates)
            self._start_down(tick, updates)
            self._append_event(updates, "lowexpand", tick, tick["bid"])
            return

        box.update(tick)
        if tick["ask"] >= prior_top and self.last_range_touch != "top":
            self.last_range_touch = "top"
            self._append_event(updates, "rangetop", tick, tick["ask"], boxId=box.id)
        elif tick["bid"] <= prior_bottom and self.last_range_touch != "bottom":
            self.last_range_touch = "bottom"
            self._append_event(updates, "rangebottom", tick, tick["bid"], boxId=box.id)
        updates["bars"].append(bar.serialize())
        updates["rangeBoxes"].append(box.serialize())

    def snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        return {
            "structureBars": [bar.serialize() for bar in self.bars],
            "rangeBoxes": [box.serialize() for box in self.range_boxes],
            "structureEvents": list(self.events),
        }


def replay_ticks(symbol: str, rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    engine = StructureEngine(symbol=symbol)
    for row in rows:
        engine.process_tick(row)
    return engine.snapshot()
