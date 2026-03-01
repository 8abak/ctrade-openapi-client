from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import pvariance
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


AUS_TZ = ZoneInfo("Australia/Sydney")
UTC = timezone.utc


@dataclass(frozen=True)
class StrategyConfig:
    n: int = 60
    r2_min: float = 0.86
    s_min: float = 0.001
    spread_mult: float = 2.0
    sigma_mult: float = 1.5
    cooldown_sec: int = 60
    tp_dollars: float = 1.50
    max_hold_sec: int = 300
    buffer_min: float = 0.05
    min_sl_distance: float = 0.01

    def to_json(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FeatureState:
    slope_kal: float
    slope_k2: float
    r2_kal: float
    r2_k2: float
    sigma_k2: float


@dataclass
class Trade:
    side: str  # "long" | "short"
    entry_ts: datetime
    exit_ts: datetime
    entry_tick_id: int
    exit_tick_id: int
    entry_price: float
    exit_price: float
    sl_price: float
    tp_price: float
    hold_sec: int
    pnl: float
    exit_reason: str  # "tp" | "sl" | "timeout"
    stopout: bool


@dataclass
class OpenSignal:
    side: str
    ts: datetime
    tick_id: int
    entry_price: float
    sl_price: float
    tp_price: float


def to_finite(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n == n and n not in (float("inf"), float("-inf")) else None


def ensure_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        dt = v
    else:
        dt = datetime.fromisoformat(str(v))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def session_label_for_start(ts: datetime) -> date:
    return ensure_dt(ts).astimezone(AUS_TZ).date()


def split_sessions_by_gap(ticks: List[Dict[str, Any]], gap_sec: int) -> List[Dict[str, Any]]:
    if not ticks:
        return []
    threshold = max(1, int(gap_sec))
    sessions: List[Dict[str, Any]] = []
    current_ticks: List[Dict[str, Any]] = [ticks[0]]
    prev_ts = ensure_dt(ticks[0]["timestamp"])

    for tick in ticks[1:]:
        ts = ensure_dt(tick["timestamp"])
        if (ts - prev_ts).total_seconds() >= threshold:
            sessions.append(
                {
                    "start_ts": ensure_dt(current_ticks[0]["timestamp"]),
                    "end_ts": ensure_dt(current_ticks[-1]["timestamp"]),
                    "ticks": current_ticks,
                }
            )
            current_ticks = [tick]
        else:
            current_ticks.append(tick)
        prev_ts = ts

    sessions.append(
        {
            "start_ts": ensure_dt(current_ticks[0]["timestamp"]),
            "end_ts": ensure_dt(current_ticks[-1]["timestamp"]),
            "ticks": current_ticks,
        }
    )
    return sessions


def _linear_regression_time(ts_values: List[datetime], values: List[float]) -> Tuple[float, float, float]:
    # y = a + b*x, where x = elapsed seconds from first tick in the window.
    n = len(values)
    if n < 2:
        return 0.0, 0.0, 0.0

    t0 = ensure_dt(ts_values[0])
    xs = [(ensure_dt(ts) - t0).total_seconds() for ts in ts_values]
    x_mean = sum(xs) / n
    y_mean = sum(values) / n

    sxx = 0.0
    sxy = 0.0
    for x, y in zip(xs, values):
        dx = x - x_mean
        sxx += dx * dx
        sxy += dx * (y - y_mean)

    if sxx <= 0:
        return 0.0, 0.0, 0.0

    slope = sxy / sxx
    intercept = y_mean - (slope * x_mean)

    ss_tot = 0.0
    ss_res = 0.0
    for x, y in zip(xs, values):
        y_hat = intercept + (slope * x)
        ss_tot += (y - y_mean) ** 2
        ss_res += (y - y_hat) ** 2

    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-12 else 1.0
    sigma = (ss_res / max(1, n - 2)) ** 0.5
    return slope, max(0.0, min(1.0, r2)), sigma


def compute_features(
    ts_window: List[datetime],
    kal_window: List[float],
    k2_window: List[float],
) -> Optional[FeatureState]:
    if len(kal_window) != len(k2_window) or len(kal_window) != len(ts_window) or len(kal_window) < 2:
        return None
    slope_kal, r2_kal, _ = _linear_regression_time(ts_window, kal_window)
    slope_k2, r2_k2, sigma_k2 = _linear_regression_time(ts_window, k2_window)
    return FeatureState(
        slope_kal=slope_kal,
        slope_k2=slope_k2,
        r2_kal=r2_kal,
        r2_k2=r2_k2,
        sigma_k2=sigma_k2,
    )


def run_backtest_ticks(ticks: List[Dict[str, Any]], cfg: StrategyConfig) -> List[Trade]:
    eng = StrategyEngine(cfg)
    trades: List[Trade] = []
    for t in ticks:
        evt = eng.process_tick(t)
        if evt["closed"] is not None:
            trades.append(evt["closed"])
    return trades


def summarize_trades(
    *,
    trades: List[Trade],
    trading_day: date,
    session_start_ts: datetime,
    session_end_ts: datetime,
    symbol: str,
    config: StrategyConfig,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    wins = sum(1 for t in trades if t.pnl > 0)
    losses = sum(1 for t in trades if t.pnl <= 0)
    trades_count = len(trades)
    total_profit = sum(t.pnl for t in trades)
    hold_secs = [t.hold_sec for t in trades] or [0]
    stopouts = sum(1 for t in trades if t.stopout)
    win_rate = (wins / trades_count) if trades_count else 0.0

    return {
        "trading_day": trading_day,
        "session_start_ts": session_start_ts,
        "session_end_ts": session_end_ts,
        "symbol": symbol,
        "config": config.to_json(),
        "trades_count": trades_count,
        "wins_count": wins,
        "losses_count": losses,
        "win_rate": win_rate,
        "total_profit": total_profit,
        "avg_hold_sec": sum(hold_secs) / len(hold_secs),
        "max_hold_sec": int(max(hold_secs)),
        "stopouts_count": stopouts,
        "notes": notes,
    }


def profit_variance(rows: List[Dict[str, Any]]) -> float:
    vals = [float(r.get("total_profit", 0.0)) for r in rows]
    if len(vals) < 2:
        return 0.0
    return float(pvariance(vals))


class StrategyEngine:
    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.ts_win: List[datetime] = []
        self.kal_win: List[float] = []
        self.k2_win: List[float] = []
        self.open_pos: Optional[Dict[str, Any]] = None
        self.cooldown_until: Optional[datetime] = None

    def process_tick(self, t: Dict[str, Any]) -> Dict[str, Optional[Any]]:
        ts = ensure_dt(t["timestamp"])
        bid = to_finite(t.get("bid"))
        ask = to_finite(t.get("ask"))
        kal = to_finite(t.get("kal"))
        k2 = to_finite(t.get("k2"))
        spread = to_finite(t.get("spread"))
        tick_id = int(t["id"])

        if spread is None and bid is not None and ask is not None:
            spread = max(0.0, ask - bid)
        spread = spread if spread is not None else 0.0

        if kal is not None and k2 is not None:
            self.ts_win.append(ts)
            self.kal_win.append(kal)
            self.k2_win.append(k2)
            if len(self.kal_win) > self.cfg.n:
                self.ts_win.pop(0)
                self.kal_win.pop(0)
                self.k2_win.pop(0)

        feats = (
            compute_features(self.ts_win, self.kal_win, self.k2_win)
            if len(self.kal_win) >= self.cfg.n
            else None
        )
        opened: Optional[OpenSignal] = None
        closed: Optional[Trade] = None

        if self.open_pos is not None:
            side = self.open_pos["side"]
            entry_ts = self.open_pos["entry_ts"]
            timeout_ts = entry_ts + timedelta(seconds=self.cfg.max_hold_sec)
            timeout_hit = ts >= timeout_ts
            hold_sec = int((min(ts, timeout_ts) - entry_ts).total_seconds())

            close_reason = None
            close_px = None
            close_ts = ts
            if side == "long" and bid is not None:
                if bid <= self.open_pos["sl_price"]:
                    close_reason = "sl"
                    close_px = self.open_pos["sl_price"]
                elif bid >= self.open_pos["tp_price"]:
                    close_reason = "tp"
                    close_px = self.open_pos["tp_price"]
                elif timeout_hit:
                    close_reason = "timeout"
                    # Timeout can occur during a gap; clamp timestamp to timeout_ts.
                    close_px = bid
                    close_ts = timeout_ts
            elif side == "short" and ask is not None:
                if ask >= self.open_pos["sl_price"]:
                    close_reason = "sl"
                    close_px = self.open_pos["sl_price"]
                elif ask <= self.open_pos["tp_price"]:
                    close_reason = "tp"
                    close_px = self.open_pos["tp_price"]
                elif timeout_hit:
                    close_reason = "timeout"
                    close_px = ask
                    close_ts = timeout_ts

            if close_reason is not None and close_px is not None:
                pnl = (close_px - self.open_pos["entry_price"]) if side == "long" else (self.open_pos["entry_price"] - close_px)
                closed = Trade(
                    side=side,
                    entry_ts=entry_ts,
                    exit_ts=close_ts,
                    entry_tick_id=self.open_pos["entry_tick_id"],
                    exit_tick_id=tick_id,
                    entry_price=self.open_pos["entry_price"],
                    exit_price=close_px,
                    sl_price=self.open_pos["sl_price"],
                    tp_price=self.open_pos["tp_price"],
                    hold_sec=max(0, min(self.cfg.max_hold_sec, hold_sec)),
                    pnl=pnl,
                    exit_reason=close_reason,
                    stopout=(close_reason == "sl"),
                )
                self.open_pos = None
                self.cooldown_until = close_ts + timedelta(seconds=self.cfg.cooldown_sec)

        if self.open_pos is None and feats is not None and k2 is not None:
            if self.cooldown_until is None or ts >= self.cooldown_until:
                long_ok = (
                    feats.slope_kal > self.cfg.s_min
                    and feats.slope_k2 > self.cfg.s_min
                    and feats.r2_kal >= self.cfg.r2_min
                    and feats.r2_k2 >= self.cfg.r2_min
                )
                short_ok = (
                    feats.slope_kal < -self.cfg.s_min
                    and feats.slope_k2 < -self.cfg.s_min
                    and feats.r2_kal >= self.cfg.r2_min
                    and feats.r2_k2 >= self.cfg.r2_min
                )
                if long_ok or short_ok:
                    buffer = max(
                        self.cfg.buffer_min,
                        self.cfg.spread_mult * spread,
                        self.cfg.sigma_mult * feats.sigma_k2,
                    )
                    side = "long" if long_ok else "short"
                    if side == "long" and ask is not None:
                        entry = ask
                        sl = min(entry - self.cfg.min_sl_distance, k2 - buffer)
                        tp = entry + self.cfg.tp_dollars
                    elif side == "short" and bid is not None:
                        entry = bid
                        sl = max(entry + self.cfg.min_sl_distance, k2 + buffer)
                        tp = entry - self.cfg.tp_dollars
                    else:
                        return {"opened": None, "closed": closed}

                    self.open_pos = {
                        "side": side,
                        "entry_ts": ts,
                        "entry_tick_id": tick_id,
                        "entry_price": entry,
                        "sl_price": sl,
                        "tp_price": tp,
                    }
                    opened = OpenSignal(
                        side=side,
                        ts=ts,
                        tick_id=tick_id,
                        entry_price=entry,
                        sl_price=sl,
                        tp_price=tp,
                    )

        return {"opened": opened, "closed": closed}
