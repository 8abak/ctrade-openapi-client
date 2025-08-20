# ml/features.py  — REPLACE ENTIRE FILE
import math
from collections import deque
from typing import List, Dict, Any, Tuple

from sqlalchemy import text

from .db import db_conn, upsert_many

# ----------------------------
# Helpers (kept local to avoid extra imports)
# ----------------------------

def _ew_std_update(curr_val: float, prev_val: float, prev_std: float, alpha: float) -> float:
    """Exponentially-weighted std of price changes (dollars)."""
    if prev_std is None or not math.isfinite(prev_std):
        prev_std = 0.0
    if prev_val is None or not math.isfinite(prev_val) or not math.isfinite(curr_val):
        return prev_std
    diff = float(curr_val) - float(prev_val)
    prev_var = prev_std * prev_std
    var = (1.0 - alpha) * prev_var + alpha * (diff * diff)
    if var < 0.0:
        var = 0.0
    return math.sqrt(var)

def _rsi(values: deque, gains: deque, losses: deque, new_val: float, prev_val: float, period: int = 14) -> float:
    """Causal RSI using Wilder's smoothing."""
    change = new_val - prev_val
    gains.append(max(0.0, change))
    losses.append(max(0.0, -change))
    if len(gains) > period:
        gains.popleft()
        losses.popleft()
    if len(gains) < period:
        return 50.0
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def _stoch_kd(window_vals: deque, period_k: int = 14, period_d: int = 3) -> Tuple[float, float]:
    if not window_vals:
        return 50.0, 50.0
    hi = max(window_vals)
    lo = min(window_vals)
    close = window_vals[-1]
    if hi == lo:
        k = 50.0
    else:
        k = 100.0 * (close - lo) / (hi - lo)
    # simple %D as SMA(period_d) over last period_d of K; for simplicity maintain via window
    # Here we approximate by taking middle of range if insufficient history
    d = k
    return k, d

def _simple_hilbert(level_window: deque) -> Tuple[float, float]:
    """Tiny causal surrogate: 16-sample sliding DFT for dominant 1-cycle component."""
    n = len(level_window)
    if n < 8:
        return 0.0, 0.0
    # project onto a single cycle over the window
    # x_t ~ A cos(2πt/n) + B sin(2πt/n)
    a = 0.0; b = 0.0
    for i, v in enumerate(level_window):
        ang = 2.0 * math.pi * i / n
        a += v * math.cos(ang)
        b += v * math.sin(ang)
    amp = math.sqrt(a*a + b*b) / max(1.0, n)
    phase = math.atan2(b, a)
    return amp, phase

def _linreg_r2(idx_window: deque, y_window: deque) -> float:
    m = len(y_window)
    if m < 5:
        return 0.0
    xs = list(idx_window)
    ys = list(y_window)
    mean_x = sum(xs)/m
    mean_y = sum(ys)/m
    num = sum((xs[i]-mean_x)*(ys[i]-mean_y) for i in range(m))
    den = sum((xs[i]-mean_x)**2 for i in range(m))
    if den == 0:
        return 0.0
    beta = num/den
    alpha = mean_y - beta*mean_x
    ss_tot = sum((ys[i]-mean_y)**2 for i in range(m))
    ss_res = sum((ys[i] - (alpha + beta*xs[i]))**2 for i in range(m))
    if ss_tot <= 0:
        return 0.0
    r2 = max(0.0, 1.0 - ss_res/ss_tot)
    return r2

def _tod_bucket(ts) -> int:
    """Small time-of-day buckets roughly for Sydney/London/NY sessions (UTC-based heuristic)."""
    # Map UTC hour to 3 rough sessions:
    # Sydney ~ 20:00–06:00 UTC, London ~ 07:00–16:00 UTC, New York ~ 13:00–22:00 UTC
    # Buckets: 1=SYD, 2=LON, 3=NY, 0=other
    h = int(ts.hour)
    if (h >= 20) or (h <= 6):
        return 1
    if 7 <= h <= 16:
        return 2
    if 13 <= h <= 22:
        return 3
    return 0

# ----------------------------
# Feature builder (past-only)
# ----------------------------

FEATURE_LIST = [
    "level","slope","residual","vol_ewstd","vol_ewstd_long",
    "r50","r200","r1000","rsi","stoch_k","stoch_d",
    "hilbert_amp","hilbert_phase","vwap_dist","r2_lin","tod_bucket"
]

def build_features_range(start: int, end: int) -> int:
    """
    Build ml_features rows for [start..end] from kalman_states.
    Strictly causal: each tick uses only past information.
    """
    # Pull kalman states (includes price, timestamp)
    with db_conn() as conn:
        rows = conn.execute(text("""
            SELECT tickid, timestamp, price, level, slope, var
            FROM kalman_states
            WHERE tickid BETWEEN :s AND :e
            ORDER BY tickid
        """), {"s": int(start), "e": int(end)}).mappings().all()

    if not rows:
        return 0

    # Windows
    w50 = deque(maxlen=50)
    w200 = deque(maxlen=200)
    w1000 = deque(maxlen=1000)
    w_rsi_g = deque(maxlen=14)
    w_rsi_l = deque(maxlen=14)
    w_stoch = deque(maxlen=14)
    w_hil = deque(maxlen=16)
    w_lin_y = deque(maxlen=100)
    w_lin_x = deque(maxlen=100)

    # VWAP session accumulators
    sess_sum = 0.0
    sess_n = 0
    prev_day = None

    # EW std trackers
    vol_short = 0.0
    vol_long = 0.0
    last_price = float(rows[0]["price"])

    out = []
    for i, r in enumerate(rows):
        tid = int(r["tickid"])
        ts = r["timestamp"]  # tz-aware
        price = float(r["price"])
        level = float(r["level"])
        slope = float(r["slope"])

        # session reset on day change (UTC)
        day = ts.date()
        if prev_day is None or day != prev_day:
            sess_sum = 0.0
            sess_n = 0
            prev_day = day

        sess_sum += price
        sess_n += 1
        vwap = sess_sum / max(1, sess_n)
        vwap_dist = level - vwap

        # windows update (using *past* values; we push current level then derive features from windows that include current)
        w50.append(level); w200.append(level); w1000.append(level)
        w_stoch.append(level); w_hil.append(level)
        w_lin_y.append(level); w_lin_x.append(float(tid))

        # returns
        r50 = (level - w50[0]) if len(w50) == w50.maxlen else 0.0
        r200 = (level - w200[0]) if len(w200) == w200.maxlen else 0.0
        r1000 = (level - w1000[0]) if len(w1000) == w1000.maxlen else 0.0

        # EW volatility (of raw price changes) short/long
        vol_short = _ew_std_update(price, last_price, vol_short, alpha=0.05)
        vol_long  = _ew_std_update(price, last_price, vol_long,  alpha=0.005)
        last_price = price

        # residual
        residual = price - level

        # RSI
        rsi = _rsi(w_stoch, w_rsi_g, w_rsi_l, level, w_stoch[-2] if len(w_stoch) > 1 else level, period=14)

        # Stoch
        stoch_k, stoch_d = _stoch_kd(w_stoch, period_k=14, period_d=3)

        # Hilbert surrogate
        hil_amp, hil_phase = _simple_hilbert(w_hil)

        # R^2 on last N levels versus tickid
        r2_lin = _linreg_r2(w_lin_x, w_lin_y)

        # TOD bucket
        tod_bucket = _tod_bucket(ts)

        # Persist row
        out.append({
            "tickid": tid,
            "timestamp": ts,
            "level": level,
            "slope": slope,
            "residual": residual,
            "vol_ewstd": float(vol_short),
            "vol_ewstd_long": float(vol_long),
            "r50": float(r50),
            "r200": float(r200),
            "r1000": float(r1000),
            "rsi": float(rsi),
            "stoch_k": float(stoch_k),
            "stoch_d": float(stoch_d),
            "hilbert_amp": float(hil_amp),
            "hilbert_phase": float(hil_phase),
            "vwap_dist": float(vwap_dist),
            "r2_lin": float(r2_lin),
            "tod_bucket": int(tod_bucket),
        })

        # Flush periodically
        if len(out) >= 5000:
            upsert_many("ml_features", out, conflict_key="tickid")
            out.clear()

    if out:
        upsert_many("ml_features", out, conflict_key="tickid")

    return len(rows)
