# ml/utils.py
from typing import List, Tuple, Optional
import math
import numpy as np

def ew_std(values: List[float], alpha: float=0.01) -> List[float]:
    mean = None
    var = 0.0
    out = []
    for v in values:
        if mean is None:
            mean = v
            var = 0.0
        else:
            d = v - mean
            mean += alpha * d
            var = (1 - alpha) * (var + alpha * d * d)
        out.append(math.sqrt(max(var, 1e-12)))
    return out

def rolling_return(arr: np.ndarray, n: int) -> np.ndarray:
    if n <= 0:
        return np.zeros_like(arr)
    out = np.full_like(arr, np.nan, dtype=float)
    out[n:] = (arr[n:] - arr[:-n])
    return out

def rsi(prices: np.ndarray, period: int=14) -> np.ndarray:
    if prices.size < period + 1:
        return np.full_like(prices, np.nan, dtype=float)
    deltas = np.diff(prices, prepend=prices[0])
    up = np.where(deltas > 0, deltas, 0.0)
    dn = np.where(deltas < 0, -deltas, 0.0)
    alpha = 1.0 / period
    def ew(x):
        s = 0.0; out=[]
        for v in x:
            s = alpha * v + (1 - alpha) * s
            out.append(s)
        return np.array(out)
    avg_up = ew(up)
    avg_dn = ew(dn)
    rs = np.where(avg_dn==0, np.inf, avg_up/avg_dn)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def stoch_kd(prices: np.ndarray, window: int=14) -> Tuple[np.ndarray, np.ndarray]:
    n = prices.size
    k = np.full(n, np.nan)
    for i in range(n):
        a = max(0, i - window + 1)
        win = prices[a:i+1]
        lo, hi = np.min(win), np.max(win)
        rng = hi - lo if hi > lo else 1e-9
        k[i] = 100.0 * (prices[i] - lo) / rng
    # %D = 3-period SMA of %K
    d = np.full(n, np.nan)
    for i in range(n):
        a = max(0, i-2)
        d[i] = np.nanmean(k[a:i+1])
    return k, d

def hilbert_amp_phase(residual: np.ndarray, window: int=32) -> Tuple[np.ndarray, np.ndarray]:
    """
    Fallback amplitude/phase estimate without SciPy: amplitude ~ local std, phase via arctan2(diff, std).
    """
    n = residual.size
    amp = np.full(n, np.nan)
    phase = np.full(n, np.nan)
    for i in range(n):
        a = max(0, i - window + 1)
        win = residual[a:i+1]
        std = float(np.std(win)) if win.size else 0.0
        amp[i] = std
        dv = residual[i] - residual[i-1] if i > 0 else 0.0
        phase[i] = math.atan2(dv, std if std>1e-9 else 1e-9)
    return amp, phase

def rolling_r2(y: np.ndarray, window: int=100) -> np.ndarray:
    n = y.size
    out = np.full(n, np.nan)
    for i in range(n):
        a = max(0, i - window + 1)
        yy = y[a:i+1]
        if yy.size < 3:
            continue
        x = np.arange(yy.size)
        # linear fit
        A = np.vstack([x, np.ones_like(x)]).T
        m, c = np.linalg.lstsq(A, yy, rcond=None)[0]
        yhat = m*x + c
        ss_res = np.sum((yy - yhat)**2)
        ss_tot = np.sum((yy - np.mean(yy))**2) + 1e-12
        out[i] = 1.0 - ss_res/ss_tot
    return out

def session_vwap_distance(ticks: np.ndarray, price: np.ndarray) -> np.ndarray:
    """
    Simple 'session VWAP' without volume: cumulative mean since local midnight.
    Use tick timestamps (epoch seconds) to detect day reset.
    """
    n = price.size
    out = np.full(n, np.nan)
    cur_sum = 0.0
    cur_cnt = 0
    prev_day = None
    for i in range(n):
        ts = ticks[i]
        # UTC epoch -> local day boundary can be approximated by epoch//86400; acceptable for visualization
        day = int(ts // 86400)
        if prev_day is None or day != prev_day:
            cur_sum = 0.0
            cur_cnt = 0
            prev_day = day
        cur_sum += price[i]
        cur_cnt += 1
        vwap = cur_sum / max(cur_cnt, 1)
        out[i] = price[i] - vwap
    return out

def tod_bucket_from_epoch(epoch_s: float) -> int:
    """
    Time-of-day buckets (Sydney/London/NY approximations). Return 0..5
    """
    # Map epoch seconds to hour-of-day UTC; simple bucketization robust enough for features
    h = int((epoch_s % 86400) // 3600)
    # rough session blocks
    if 20 <= h or h < 2:   # Sydney morning-ish (UTC evening)
        return 0
    if 2 <= h < 6:
        return 1
    if 6 <= h < 10:       # London open build-up
        return 2
    if 10 <= h < 14:      # London/NY overlap
        return 3
    if 14 <= h < 18:      # NY midday
        return 4
    return 5              # NY close / Asia pre-open
