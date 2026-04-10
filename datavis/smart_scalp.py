from __future__ import annotations

import copy
import math
import threading
import time
from collections import deque
from datetime import datetime
from statistics import mean
from typing import Any, Callable, Deque, Dict, List, Optional


class SmartScalpError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "SMART_SCALP_ERROR",
        status_code: int = 400,
    ):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clamp_int(value: Any, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except Exception:
        number = minimum
    return max(minimum, min(maximum, number))


def _clamp_float(value: Any, minimum: float, maximum: float) -> float:
    try:
        number = float(value)
    except Exception:
        number = minimum
    return max(minimum, min(maximum, number))


def _tick_mid(row: Dict[str, Any]) -> Optional[float]:
    mid = row.get("mid")
    if mid is not None:
        try:
            value = float(mid)
        except Exception:
            value = math.nan
        if math.isfinite(value) and value > 0:
            return value
    bid = row.get("bid")
    ask = row.get("ask")
    try:
        bid_value = float(bid) if bid is not None else math.nan
        ask_value = float(ask) if ask is not None else math.nan
    except Exception:
        return None
    if math.isfinite(bid_value) and math.isfinite(ask_value) and bid_value > 0 and ask_value > 0:
        return (bid_value + ask_value) / 2.0
    return None


def _tick_spread(row: Dict[str, Any]) -> Optional[float]:
    spread = row.get("spread")
    if spread is not None:
        try:
            value = float(spread)
        except Exception:
            value = math.nan
        if math.isfinite(value) and value >= 0:
            return value
    bid = row.get("bid")
    ask = row.get("ask")
    try:
        bid_value = float(bid) if bid is not None else math.nan
        ask_value = float(ask) if ask is not None else math.nan
    except Exception:
        return None
    if math.isfinite(bid_value) and math.isfinite(ask_value) and ask_value >= bid_value:
        return ask_value - bid_value
    return None


def _tick_timestamp_ms(row: Dict[str, Any]) -> Optional[int]:
    raw = row.get("timestampMs")
    try:
        if raw is not None:
            value = int(raw)
            if value > 0:
                return value
    except Exception:
        pass
    raw = row.get("timestamp")
    if isinstance(raw, datetime):
        return int(raw.timestamp() * 1000)
    if isinstance(raw, str):
        try:
            return int(datetime.fromisoformat(raw).timestamp() * 1000)
        except Exception:
            return None
    return None


def _mean(values: List[float]) -> float:
    return float(mean(values)) if values else 0.0


def _price_metrics(
    ticks: List[Dict[str, Any]],
    *,
    direction: int,
) -> Optional[Dict[str, float]]:
    if len(ticks) < 2:
        return None
    mids: List[float] = []
    timestamps: List[int] = []
    spreads: List[float] = []
    for row in ticks:
        mid = _tick_mid(row)
        ts = _tick_timestamp_ms(row)
        if mid is None or ts is None:
            return None
        mids.append(mid)
        timestamps.append(ts)
        spread = _tick_spread(row)
        if spread is not None:
            spreads.append(spread)
    deltas: List[float] = []
    signed_deltas: List[float] = []
    abs_deltas: List[float] = []
    velocities: List[float] = []
    favorable_steps = 0
    for index in range(1, len(mids)):
        price_delta = mids[index] - mids[index - 1]
        dt_seconds = max(0.001, (timestamps[index] - timestamps[index - 1]) / 1000.0)
        signed = float(direction) * float(price_delta)
        deltas.append(price_delta)
        signed_deltas.append(signed)
        abs_deltas.append(abs(price_delta))
        velocities.append(abs(price_delta) / dt_seconds)
        if signed > 0:
            favorable_steps += 1
    total_move = mids[-1] - mids[0]
    signed_move = float(direction) * float(total_move)
    duration_seconds = max(0.001, (timestamps[-1] - timestamps[0]) / 1000.0)
    favorable_moves = [value for value in signed_deltas if value > 0]
    adverse_moves = [-value for value in signed_deltas if value < 0]
    return {
        "signedMove": signed_move,
        "absMove": abs(total_move),
        "velocity": abs(total_move) / duration_seconds,
        "avgAbsDelta": _mean(abs_deltas),
        "avgVelocity": _mean(velocities),
        "favorableRatio": favorable_steps / float(max(1, len(deltas))),
        "favorableAvg": _mean(favorable_moves),
        "adverseAvg": _mean(adverse_moves),
        "range": max(mids) - min(mids),
        "avgSpread": _mean(spreads),
        "currentSpread": spreads[-1] if spreads else 0.0,
        "currentMid": mids[-1],
    }


class SmartScalpService:
    def __init__(
        self,
        *,
        symbol: str,
        fetch_ticks_after: Callable[[int, int], List[Dict[str, Any]]],
        fetch_recent_ticks: Callable[[int], List[Dict[str, Any]]],
        fetch_latest_tick: Callable[[], Optional[Dict[str, Any]]],
        fetch_snapshot: Callable[[], Dict[str, Any]],
        fetch_broker_status: Callable[[], Dict[str, Any]],
        place_market_order: Callable[..., Dict[str, Any]],
        close_position: Callable[..., Dict[str, Any]],
        smart_lot_size: float = 0.01,
    ):
        self._symbol = symbol
        self._fetch_ticks_after = fetch_ticks_after
        self._fetch_recent_ticks = fetch_recent_ticks
        self._fetch_latest_tick = fetch_latest_tick
        self._fetch_snapshot = fetch_snapshot
        self._fetch_broker_status = fetch_broker_status
        self._place_market_order = place_market_order
        self._close_position = close_position
        self._smart_lot_size = float(smart_lot_size)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._tick_history: Deque[Dict[str, Any]] = deque(maxlen=96)
        self._config = self._default_config()
        self._context = self._default_context()
        self._state = self._default_state()
        self._last_snapshot: Optional[Dict[str, Any]] = None
        self._last_snapshot_at_ms = 0
        self._poll_seconds = 0.05
        self._idle_seconds = 0.20
        self._auth_valid_until_ms = 0

    def _default_config(self) -> Dict[str, Any]:
        return {
            "showSummary": True,
            "entryBaselineWindow": 24,
            "entryTriggerWindow": 4,
            "entryTriggerThreshold": 3.4,
            "entryVelocityThreshold": 2.3,
            "entryMinMove": 0.10,
            "entryMinDirectionRatio": 0.75,
            "entryMaxSpreadFactor": 1.8,
            "entryMinActiveRange": 0.03,
            "closeBaselineWindow": 16,
            "closeTriggerWindow": 4,
            "closeWeakeningThreshold": 0.42,
            "closeReversalThreshold": 0.85,
            "closeMinPullback": 0.06,
            "minimumProfit": 0.30,
            "cooldownSeconds": 6,
            "maxHoldSeconds": 0,
            "snapshotRefreshMs": 700,
            "tickHistorySize": 96,
            "evaluationBatchSize": 32,
        }

    def _default_context(self) -> Dict[str, Any]:
        return {
            "page": "live",
            "mode": "live",
            "run": "stop",
            "enabled": False,
            "reason": "Smart scalping requires Live + Run.",
            "updatedAtMs": _now_ms(),
        }

    def _default_state(self) -> Dict[str, Any]:
        return {
            "armed": {"buy": False, "sell": False, "close": False},
            "backendState": "idle",
            "statusText": "Idle",
            "availabilityReason": "Smart scalping requires Live + Run.",
            "cooldownUntilMs": 0,
            "cooldownRemainingMs": 0,
            "lastTickId": 0,
            "lastTickTimestampMs": None,
            "lastActionId": 0,
            "lastTradeMutationId": 0,
            "lastAction": None,
            "lastTriggerReason": None,
            "lastTriggerAtMs": None,
            "currentPosition": None,
            "smartPosition": None,
            "openPositionCount": 0,
            "snapshotAtMs": None,
            "evaluation": None,
            "error": None,
            "updatedAtMs": _now_ms(),
        }

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, name="datavis-smart-scalp", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

    def set_context(self, *, page: str, mode: str, run: str) -> Dict[str, Any]:
        with self._lock:
            normalized_page = (page or "live").strip().lower() or "live"
            normalized_mode = (mode or "live").strip().lower() or "live"
            normalized_run = (run or "stop").strip().lower() or "stop"
            enabled = normalized_page == "live" and normalized_mode == "live" and normalized_run == "run"
            reason = "" if enabled else "Smart scalping is available only on the live chart in Live + Run."
            self._context = {
                "page": normalized_page,
                "mode": normalized_mode,
                "run": normalized_run,
                "enabled": enabled,
                "reason": reason,
                "updatedAtMs": _now_ms(),
            }
            self._state["error"] = None
            if not enabled:
                self._clear_armed_locked(
                    reason=reason or "Smart scalping unavailable.",
                    backend_state="not_available",
                )
            else:
                self._touch_locked("Context synced.")
            return self.snapshot_state()

    def touch_auth(self, *, ttl_ms: int = 30000) -> None:
        with self._lock:
            self._auth_valid_until_ms = max(self._auth_valid_until_ms, _now_ms() + max(1000, int(ttl_ms)))

    def update_config(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            config = dict(self._config)
            if "showSummary" in updates:
                config["showSummary"] = bool(updates.get("showSummary"))
            if "entryBaselineWindow" in updates:
                config["entryBaselineWindow"] = _clamp_int(updates.get("entryBaselineWindow"), 8, 120)
            if "entryTriggerWindow" in updates:
                config["entryTriggerWindow"] = _clamp_int(updates.get("entryTriggerWindow"), 2, 12)
            if "entryTriggerThreshold" in updates:
                config["entryTriggerThreshold"] = _clamp_float(updates.get("entryTriggerThreshold"), 1.2, 8.0)
            if "entryVelocityThreshold" in updates:
                config["entryVelocityThreshold"] = _clamp_float(updates.get("entryVelocityThreshold"), 1.1, 8.0)
            if "entryMinMove" in updates:
                config["entryMinMove"] = _clamp_float(updates.get("entryMinMove"), 0.01, 5.0)
            if "entryMinDirectionRatio" in updates:
                config["entryMinDirectionRatio"] = _clamp_float(updates.get("entryMinDirectionRatio"), 0.50, 1.0)
            if "entryMaxSpreadFactor" in updates:
                config["entryMaxSpreadFactor"] = _clamp_float(updates.get("entryMaxSpreadFactor"), 1.0, 5.0)
            if "entryMinActiveRange" in updates:
                config["entryMinActiveRange"] = _clamp_float(updates.get("entryMinActiveRange"), 0.0, 5.0)
            if "closeBaselineWindow" in updates:
                config["closeBaselineWindow"] = _clamp_int(updates.get("closeBaselineWindow"), 6, 120)
            if "closeTriggerWindow" in updates:
                config["closeTriggerWindow"] = _clamp_int(updates.get("closeTriggerWindow"), 2, 12)
            if "closeWeakeningThreshold" in updates:
                config["closeWeakeningThreshold"] = _clamp_float(updates.get("closeWeakeningThreshold"), 0.05, 1.0)
            if "closeReversalThreshold" in updates:
                config["closeReversalThreshold"] = _clamp_float(updates.get("closeReversalThreshold"), 0.10, 3.0)
            if "closeMinPullback" in updates:
                config["closeMinPullback"] = _clamp_float(updates.get("closeMinPullback"), 0.0, 5.0)
            if "minimumProfit" in updates:
                config["minimumProfit"] = _clamp_float(updates.get("minimumProfit"), 0.01, 20.0)
            if "cooldownSeconds" in updates:
                config["cooldownSeconds"] = _clamp_int(updates.get("cooldownSeconds"), 0, 120)
            if "maxHoldSeconds" in updates:
                config["maxHoldSeconds"] = _clamp_int(updates.get("maxHoldSeconds"), 0, 600)
            if "snapshotRefreshMs" in updates:
                config["snapshotRefreshMs"] = _clamp_int(updates.get("snapshotRefreshMs"), 250, 5000)
            if "tickHistorySize" in updates:
                config["tickHistorySize"] = _clamp_int(updates.get("tickHistorySize"), 24, 240)
            if "evaluationBatchSize" in updates:
                config["evaluationBatchSize"] = _clamp_int(updates.get("evaluationBatchSize"), 4, 128)
            minimum_history = max(
                config["entryBaselineWindow"] + config["entryTriggerWindow"] + 4,
                config["closeBaselineWindow"] + config["closeTriggerWindow"] + 4,
            )
            config["tickHistorySize"] = max(config["tickHistorySize"], minimum_history)
            self._config = config
            self._tick_history = deque(list(self._tick_history)[-config["tickHistorySize"] :], maxlen=config["tickHistorySize"])
            self._state["error"] = None
            self._touch_locked("Smart scalp settings updated.")
            return self.snapshot_state()

    def arm_entry(self, *, side: str, armed: bool) -> Dict[str, Any]:
        normalized_side = "sell" if (side or "").strip().lower() == "sell" else "buy"
        with self._lock:
            self._require_context_enabled_locked()
            snapshot = self._refresh_snapshot_locked(force=True)
            positions = list(snapshot.get("positions") or [])
            if positions:
                raise SmartScalpError(
                    "Smart entry is unavailable while a position is open.",
                    code="SMART_ENTRY_POSITION_OPEN",
                    status_code=409,
                )
            if not self._broker_ready_locked():
                raise SmartScalpError(
                    self._broker_reason_locked(),
                    code="SMART_ENTRY_BROKER_UNAVAILABLE",
                    status_code=503,
                )
            self._state["error"] = None
            opposite_side = "buy" if normalized_side == "sell" else "sell"
            self._state["armed"][normalized_side] = bool(armed)
            if armed:
                self._state["armed"][opposite_side] = False
                self._seed_recent_ticks_locked()
                self._state["backendState"] = "armed_" + normalized_side
                self._state["statusText"] = "Smart " + normalized_side.upper() + " armed."
                self._state["availabilityReason"] = ""
            else:
                self._state["backendState"] = "idle"
                self._state["statusText"] = "Smart " + normalized_side.upper() + " disarmed."
            self._touch_locked(self._state["statusText"])
            return self.snapshot_state()

    def arm_close(self, *, armed: bool) -> Dict[str, Any]:
        with self._lock:
            self._require_context_enabled_locked()
            snapshot = self._refresh_snapshot_locked(force=True)
            positions = list(snapshot.get("positions") or [])
            if armed:
                if len(positions) != 1:
                    raise SmartScalpError(
                        "Smart Close requires exactly one open position.",
                        code="SMART_CLOSE_POSITION_REQUIRED",
                        status_code=409,
                    )
                if not self._broker_ready_locked():
                    raise SmartScalpError(
                        self._broker_reason_locked(),
                        code="SMART_CLOSE_BROKER_UNAVAILABLE",
                        status_code=503,
                    )
                self._state["error"] = None
                self._seed_recent_ticks_locked()
                position = positions[0]
                self._state["armed"]["close"] = True
                self._state["backendState"] = "armed_close"
                self._state["statusText"] = "Smart Close armed."
                self._state["currentPosition"] = self._position_summary(position)
                self._reset_smart_position_locked(position)
            else:
                self._state["armed"]["close"] = False
                self._state["backendState"] = "idle"
                self._state["statusText"] = "Smart Close disarmed."
                self._clear_smart_position_locked()
            self._touch_locked(self._state["statusText"])
            return self.snapshot_state()

    def reset(self, *, reason: str) -> Dict[str, Any]:
        with self._lock:
            self._clear_armed_locked(reason=reason, backend_state="idle")
            return self.snapshot_state()

    def snapshot_state(self) -> Dict[str, Any]:
        with self._lock:
            state = copy.deepcopy(self._state)
            state["cooldownRemainingMs"] = max(0, int(state.get("cooldownUntilMs") or 0) - _now_ms())
            return {
                "symbol": self._symbol,
                "smartLotSize": self._smart_lot_size,
                "context": copy.deepcopy(self._context),
                "config": copy.deepcopy(self._config),
                "state": state,
                "broker": self._safe_broker_status_locked(),
                "serverTimeMs": _now_ms(),
            }

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                should_work = self._should_work()
                if not should_work:
                    time.sleep(self._idle_seconds)
                    continue
                with self._lock:
                    current_last_id = int(self._state.get("lastTickId") or 0)
                    batch_size = int(self._config.get("evaluationBatchSize") or 32)
                rows = self._fetch_ticks_after(current_last_id, batch_size)
                if not rows:
                    time.sleep(self._poll_seconds)
                    continue
                for row in rows:
                    if self._stop_event.is_set():
                        break
                    with self._lock:
                        self._ingest_tick_locked(row)
                        self._evaluate_locked()
            except Exception as exc:
                with self._lock:
                    self._state["error"] = str(exc) or "Smart scalp worker error."
                    self._touch_locked(self._state["error"])
                time.sleep(self._idle_seconds)

    def _should_work(self) -> bool:
        with self._lock:
            if not self._context.get("enabled"):
                return False
            if not self._auth_active_locked():
                self._clear_armed_locked(reason="Trade session heartbeat expired.", backend_state="idle")
                return False
            armed = self._state.get("armed") or {}
            return bool(armed.get("buy") or armed.get("sell") or armed.get("close"))

    def _ingest_tick_locked(self, row: Dict[str, Any]) -> None:
        normalized = {
            "id": int(row.get("id") or 0),
            "timestamp": row.get("timestamp"),
            "timestampMs": _tick_timestamp_ms(row),
            "bid": row.get("bid"),
            "ask": row.get("ask"),
            "mid": _tick_mid(row),
            "spread": _tick_spread(row),
        }
        if normalized["id"] <= 0 or normalized["mid"] is None:
            return
        self._tick_history.append(normalized)
        self._state["lastTickId"] = normalized["id"]
        self._state["lastTickTimestampMs"] = normalized["timestampMs"]
        self._touch_locked(None)

    def _evaluate_locked(self) -> None:
        if not self._context.get("enabled"):
            return
        snapshot = self._refresh_snapshot_locked()
        positions = list(snapshot.get("positions") or [])
        self._state["openPositionCount"] = len(positions)
        self._state["currentPosition"] = self._position_summary(positions[0]) if len(positions) == 1 else None
        self._state["snapshotAtMs"] = self._last_snapshot_at_ms
        self._state["availabilityReason"] = "" if self._broker_ready_locked() else self._broker_reason_locked()

        if len(positions) != 1 and self._state["armed"].get("close"):
            self._state["armed"]["close"] = False
            self._clear_smart_position_locked()
            self._state["backendState"] = "idle"
            self._state["statusText"] = "Smart Close disarmed because no single open position is available."

        if positions and (self._state["armed"].get("buy") or self._state["armed"].get("sell")):
            self._state["armed"]["buy"] = False
            self._state["armed"]["sell"] = False
            self._state["backendState"] = "idle"
            self._state["statusText"] = "Smart entry disarmed because a position is already open."

        cooldown_remaining = max(0, int(self._state.get("cooldownUntilMs") or 0) - _now_ms())
        self._state["cooldownRemainingMs"] = cooldown_remaining
        if cooldown_remaining > 0:
            if str(self._state["backendState"]).startswith("armed_"):
                self._state["backendState"] = "cooldown"
            return

        if self._state["armed"].get("buy"):
            evaluation = self._evaluate_entry_locked("buy")
            if evaluation:
                self._execute_entry_locked("buy", evaluation)
                return
        if self._state["armed"].get("sell"):
            evaluation = self._evaluate_entry_locked("sell")
            if evaluation:
                self._execute_entry_locked("sell", evaluation)
                return
        if self._state["armed"].get("close"):
            evaluation = self._evaluate_close_locked(positions[0] if len(positions) == 1 else None)
            if evaluation:
                self._execute_close_locked(positions[0], evaluation)

    def _evaluate_entry_locked(self, side: str) -> Optional[Dict[str, Any]]:
        direction = 1 if side == "buy" else -1
        baseline_window = int(self._config["entryBaselineWindow"])
        trigger_window = int(self._config["entryTriggerWindow"])
        needed = baseline_window + trigger_window + 2
        history = list(self._tick_history)
        if len(history) < needed:
            self._state["evaluation"] = {
                "type": "entry",
                "side": side,
                "status": "waiting_history",
                "ticksNeeded": needed,
                "ticksAvailable": len(history),
            }
            return None
        baseline_slice = history[-(baseline_window + trigger_window + 1) : -(trigger_window)]
        recent_slice = history[-(trigger_window + 1) :]
        baseline_metrics = _price_metrics(baseline_slice, direction=direction)
        recent_metrics = _price_metrics(recent_slice, direction=direction)
        if not baseline_metrics or not recent_metrics:
            return None
        if baseline_metrics["currentSpread"] > 0 and recent_metrics["currentSpread"] > baseline_metrics["currentSpread"] * float(self._config["entryMaxSpreadFactor"]):
            self._state["evaluation"] = {
                "type": "entry",
                "side": side,
                "status": "spread_blocked",
                "currentSpread": recent_metrics["currentSpread"],
                "baselineSpread": baseline_metrics["currentSpread"],
            }
            return None
        if baseline_metrics["range"] < float(self._config["entryMinActiveRange"]) and recent_metrics["signedMove"] < float(self._config["entryMinMove"]):
            self._state["evaluation"] = {
                "type": "entry",
                "side": side,
                "status": "flat_market",
                "baselineRange": baseline_metrics["range"],
                "recentMove": recent_metrics["signedMove"],
            }
            return None
        baseline_move = max(0.000001, baseline_metrics["avgAbsDelta"])
        baseline_velocity = max(0.000001, baseline_metrics["avgVelocity"])
        move_ratio = recent_metrics["signedMove"] / baseline_move
        velocity_ratio = recent_metrics["velocity"] / baseline_velocity
        move_threshold = max(float(self._config["entryMinMove"]), baseline_move * float(self._config["entryTriggerThreshold"]))
        if (
            recent_metrics["signedMove"] >= move_threshold
            and velocity_ratio >= float(self._config["entryVelocityThreshold"])
            and recent_metrics["favorableRatio"] >= float(self._config["entryMinDirectionRatio"])
        ):
            return {
                "kind": "entry",
                "side": side,
                "recentMove": recent_metrics["signedMove"],
                "baselineMove": baseline_move,
                "moveRatio": move_ratio,
                "velocityRatio": velocity_ratio,
                "favorableRatio": recent_metrics["favorableRatio"],
                "currentSpread": recent_metrics["currentSpread"],
                "reason": (
                    "Recent {side} burst {move:.3f} exceeded baseline {baseline:.4f} by {move_ratio:.2f}x "
                    "with velocity {velocity_ratio:.2f}x."
                ).format(
                    side=side,
                    move=recent_metrics["signedMove"],
                    baseline=baseline_move,
                    move_ratio=move_ratio,
                    velocity_ratio=velocity_ratio,
                ),
            }
        self._state["evaluation"] = {
            "type": "entry",
            "side": side,
            "status": "armed_waiting",
            "recentMove": round(recent_metrics["signedMove"], 5),
            "moveThreshold": round(move_threshold, 5),
            "moveRatio": round(move_ratio, 3),
            "velocityRatio": round(velocity_ratio, 3),
            "favorableRatio": round(recent_metrics["favorableRatio"], 3),
        }
        return None

    def _evaluate_close_locked(self, position: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not position:
            return None
        baseline_window = int(self._config["closeBaselineWindow"])
        trigger_window = int(self._config["closeTriggerWindow"])
        needed = baseline_window + trigger_window + 2
        history = list(self._tick_history)
        if len(history) < needed:
            self._state["evaluation"] = {
                "type": "close",
                "status": "waiting_history",
                "ticksNeeded": needed,
                "ticksAvailable": len(history),
            }
            return None
        pnl = float(position.get("netUnrealizedPnl") or 0.0)
        if pnl <= float(self._config["minimumProfit"]):
            self._state["evaluation"] = {
                "type": "close",
                "status": "waiting_profit",
                "profit": round(pnl, 4),
                "minimumProfit": float(self._config["minimumProfit"]),
            }
            self._update_smart_position_peak_locked(position)
            return None
        self._update_smart_position_peak_locked(position)
        direction = 1 if str(position.get("side") or "").lower() == "buy" else -1
        baseline_slice = history[-(baseline_window + trigger_window + 1) : -(trigger_window)]
        recent_slice = history[-(trigger_window + 1) :]
        baseline_metrics = _price_metrics(baseline_slice, direction=direction)
        recent_metrics = _price_metrics(recent_slice, direction=direction)
        if not baseline_metrics or not recent_metrics:
            return None
        favorable_baseline = max(0.000001, baseline_metrics["favorableAvg"], baseline_metrics["avgAbsDelta"])
        weak_threshold = favorable_baseline * float(self._config["closeWeakeningThreshold"])
        reversal_threshold = favorable_baseline * float(self._config["closeReversalThreshold"])
        smart_position = self._state.get("smartPosition") or {}
        peak_profit = float(smart_position.get("peakProfit") or pnl)
        pullback = max(0.0, peak_profit - pnl)
        open_timestamp_ms = int(position.get("openTimestampMs") or smart_position.get("enteredAtMs") or 0)
        held_seconds = max(0.0, (_now_ms() - open_timestamp_ms) / 1000.0) if open_timestamp_ms else 0.0
        if int(self._config["maxHoldSeconds"]) > 0 and held_seconds >= float(self._config["maxHoldSeconds"]) and pnl > 0:
            return {
                "kind": "close",
                "reason": "Max smart hold time reached with open profit.",
                "profit": pnl,
                "heldSeconds": held_seconds,
                "peakProfit": peak_profit,
                "pullback": pullback,
            }
        if recent_metrics["signedMove"] <= 0 and pullback >= float(self._config["closeMinPullback"]):
            return {
                "kind": "close",
                "reason": "Momentum reversed after profit and pullback from peak.",
                "profit": pnl,
                "peakProfit": peak_profit,
                "pullback": pullback,
                "signedMove": recent_metrics["signedMove"],
            }
        if recent_metrics["signedMove"] <= weak_threshold and (
            recent_metrics["favorableRatio"] < 0.5 or pullback >= float(self._config["closeMinPullback"])
        ):
            return {
                "kind": "close",
                "reason": "Momentum weakened after profit exceeded the smart close floor.",
                "profit": pnl,
                "peakProfit": peak_profit,
                "pullback": pullback,
                "signedMove": recent_metrics["signedMove"],
                "weakThreshold": weak_threshold,
            }
        if recent_metrics["adverseAvg"] >= reversal_threshold:
            return {
                "kind": "close",
                "reason": "Adverse counter-move expanded beyond the close reversal threshold.",
                "profit": pnl,
                "peakProfit": peak_profit,
                "pullback": pullback,
                "adverseAvg": recent_metrics["adverseAvg"],
                "reversalThreshold": reversal_threshold,
            }
        self._state["evaluation"] = {
            "type": "close",
            "status": "armed_waiting",
            "profit": round(pnl, 4),
            "peakProfit": round(peak_profit, 4),
            "pullback": round(pullback, 4),
            "recentMove": round(recent_metrics["signedMove"], 5),
            "weakThreshold": round(weak_threshold, 5),
            "favorableRatio": round(recent_metrics["favorableRatio"], 3),
        }
        return None

    def _execute_entry_locked(self, side: str, evaluation: Dict[str, Any]) -> None:
        self._state["armed"]["buy"] = False
        self._state["armed"]["sell"] = False
        self._state["backendState"] = "triggered_entry"
        self._state["lastTriggerReason"] = evaluation["reason"]
        self._state["lastTriggerAtMs"] = _now_ms()
        self._state["statusText"] = "Smart " + side.upper() + " triggered."
        try:
            result = self._place_market_order(side=side, volume=self._smart_lot_size, stop_loss=None, take_profit=None)
            self._state["cooldownUntilMs"] = _now_ms() + int(self._config["cooldownSeconds"]) * 1000
            self._state["lastTradeMutationId"] = int(self._state["lastTradeMutationId"]) + 1
            self._record_action_locked(
                kind="entry",
                side=side,
                status="triggered",
                reason=evaluation["reason"],
                result=result,
            )
            position_id = None
            result_position = result.get("position") if isinstance(result, dict) else None
            if isinstance(result_position, dict):
                position_id = result_position.get("positionId")
            self._state["smartPosition"] = {
                "side": side,
                "positionId": position_id,
                "enteredAtMs": _now_ms(),
                "peakProfit": 0.0,
            }
        except Exception as exc:
            self._record_action_locked(
                kind="entry",
                side=side,
                status="failed",
                reason=str(exc) or "Smart entry failed.",
                result=None,
            )
            self._state["error"] = str(exc) or "Smart entry failed."
            self._state["statusText"] = "Smart " + side.upper() + " trigger failed."

    def _execute_close_locked(self, position: Dict[str, Any], evaluation: Dict[str, Any]) -> None:
        position_id = int(position.get("positionId") or 0)
        volume = int(position.get("volume") or 0)
        if position_id <= 0 or volume <= 0:
            self._state["armed"]["close"] = False
            self._state["statusText"] = "Smart Close disarmed because the position is no longer valid."
            return
        self._state["armed"]["close"] = False
        self._state["backendState"] = "triggered_close"
        self._state["lastTriggerReason"] = evaluation["reason"]
        self._state["lastTriggerAtMs"] = _now_ms()
        self._state["statusText"] = "Smart Close triggered."
        try:
            result = self._close_position(position_id=position_id, volume=volume)
            self._state["cooldownUntilMs"] = _now_ms() + int(self._config["cooldownSeconds"]) * 1000
            self._state["lastTradeMutationId"] = int(self._state["lastTradeMutationId"]) + 1
            self._record_action_locked(
                kind="close",
                side=str(position.get("side") or "").lower() or None,
                status="triggered",
                reason=evaluation["reason"],
                result=result,
            )
            self._clear_smart_position_locked()
        except Exception as exc:
            self._record_action_locked(
                kind="close",
                side=str(position.get("side") or "").lower() or None,
                status="failed",
                reason=str(exc) or "Smart close failed.",
                result=None,
            )
            self._state["error"] = str(exc) or "Smart close failed."
            self._state["statusText"] = "Smart Close trigger failed."

    def _record_action_locked(
        self,
        *,
        kind: str,
        side: Optional[str],
        status: str,
        reason: str,
        result: Optional[Dict[str, Any]],
    ) -> None:
        self._state["lastActionId"] = int(self._state["lastActionId"]) + 1
        self._state["lastAction"] = {
            "id": int(self._state["lastActionId"]),
            "kind": kind,
            "side": side,
            "status": status,
            "reason": reason,
            "result": result,
            "timestampMs": _now_ms(),
        }
        if status == "triggered":
            self._state["error"] = None
        self._state["backendState"] = "cooldown" if int(self._state.get("cooldownUntilMs") or 0) > _now_ms() else "idle"
        self._touch_locked(reason)

    def _seed_recent_ticks_locked(self) -> None:
        history_size = int(self._config["tickHistorySize"])
        rows = self._fetch_recent_ticks(history_size)
        self._tick_history = deque(maxlen=history_size)
        for row in rows:
            normalized = {
                "id": int(row.get("id") or 0),
                "timestamp": row.get("timestamp"),
                "timestampMs": _tick_timestamp_ms(row),
                "bid": row.get("bid"),
                "ask": row.get("ask"),
                "mid": _tick_mid(row),
                "spread": _tick_spread(row),
            }
            if normalized["id"] > 0 and normalized["mid"] is not None:
                self._tick_history.append(normalized)
        latest = self._fetch_latest_tick()
        if latest:
            self._state["lastTickId"] = int(latest.get("id") or self._state.get("lastTickId") or 0)
            self._state["lastTickTimestampMs"] = _tick_timestamp_ms(latest)

    def _refresh_snapshot_locked(self, *, force: bool = False) -> Dict[str, Any]:
        snapshot_ttl = int(self._config.get("snapshotRefreshMs") or 700)
        now = _now_ms()
        if not force and self._last_snapshot is not None and now - self._last_snapshot_at_ms < snapshot_ttl:
            return self._last_snapshot
        snapshot = self._fetch_snapshot()
        self._last_snapshot = snapshot
        self._last_snapshot_at_ms = now
        return snapshot

    def _broker_ready_locked(self) -> bool:
        broker = self._safe_broker_status_locked()
        return bool(broker.get("ready"))

    def _broker_reason_locked(self) -> str:
        broker = self._safe_broker_status_locked()
        if broker.get("reason"):
            return str(broker["reason"])
        return "Broker state unavailable."

    def _safe_broker_status_locked(self) -> Dict[str, Any]:
        try:
            return dict(self._fetch_broker_status() or {})
        except Exception as exc:
            return {
                "configured": False,
                "ready": False,
                "reason": str(exc) or "Broker state unavailable.",
                "state": "error",
            }

    def _position_summary(self, position: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "positionId": int(position.get("positionId") or 0),
            "side": str(position.get("side") or "").lower() or None,
            "volume": int(position.get("volume") or 0),
            "volumeLots": position.get("volumeLots"),
            "entryPrice": position.get("entryPrice"),
            "netUnrealizedPnl": position.get("netUnrealizedPnl"),
            "openTimestampMs": position.get("openTimestampMs"),
            "openTimestamp": position.get("openTimestamp"),
        }

    def _reset_smart_position_locked(self, position: Dict[str, Any]) -> None:
        self._state["smartPosition"] = {
            "side": str(position.get("side") or "").lower() or None,
            "positionId": int(position.get("positionId") or 0),
            "enteredAtMs": int(position.get("openTimestampMs") or _now_ms()),
            "peakProfit": float(position.get("netUnrealizedPnl") or 0.0),
        }

    def _update_smart_position_peak_locked(self, position: Dict[str, Any]) -> None:
        if not self._state.get("smartPosition"):
            self._reset_smart_position_locked(position)
            return
        smart_position = dict(self._state["smartPosition"])
        if int(smart_position.get("positionId") or 0) != int(position.get("positionId") or 0):
            self._reset_smart_position_locked(position)
            return
        smart_position["peakProfit"] = max(
            float(smart_position.get("peakProfit") or 0.0),
            float(position.get("netUnrealizedPnl") or 0.0),
        )
        self._state["smartPosition"] = smart_position

    def _clear_smart_position_locked(self) -> None:
        self._state["smartPosition"] = None

    def _require_context_enabled_locked(self) -> None:
        if self._context.get("enabled"):
            if self._auth_active_locked():
                return
            raise SmartScalpError(
                "Trade session heartbeat expired.",
                code="SMART_SCALP_AUTH_EXPIRED",
                status_code=401,
            )
        raise SmartScalpError(
            str(self._context.get("reason") or "Smart scalping unavailable."),
            code="SMART_SCALP_NOT_AVAILABLE",
            status_code=409,
        )

    def _auth_active_locked(self) -> bool:
        return int(self._auth_valid_until_ms or 0) > _now_ms()

    def _clear_armed_locked(self, *, reason: str, backend_state: str) -> None:
        self._state["armed"] = {"buy": False, "sell": False, "close": False}
        self._state["backendState"] = backend_state
        self._state["statusText"] = reason
        self._state["availabilityReason"] = reason
        self._clear_smart_position_locked()
        self._touch_locked(reason)

    def _touch_locked(self, message: Optional[str]) -> None:
        self._state["updatedAtMs"] = _now_ms()
