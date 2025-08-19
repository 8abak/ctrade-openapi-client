# ml/features.py
from typing import List, Dict, Any
import numpy as np

from .db import upsert_many
from .utils import ew_std, rolling_return, rsi, stoch_kd, hilbert_amp_phase, session_vwap_distance, rolling_r2

FEATURE_COLUMNS = [
    "level","slope","residual","vol_ewstd","vol_ewstd_long",
    "r50","r200","r1000","rsi","stoch_k","stoch_d",
    "hilbert_amp","hilbert_phase","vwap_dist","r2_lin","tod_bucket"
]

def build_features(kalman_rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    if not kalman_rows:
        return []
    # keep causal order
    kalman_rows = sorted(kalman_rows, key=lambda r: r["tickid"])
    level = np.array([float(r["level"]) for r in kalman_rows], dtype=float)
    price = np.array([float(r["price"]) for r in kalman_rows], dtype=float)
    slope = np.array([float(r["slope"]) for r in kalman_rows], dtype=float)
    ts = np.array([float(r["timestamp"].timestamp()) for r in kalman_rows], dtype=float)

    residual = price - level
    vol_short = np.array(ew_std(residual.tolist(), alpha=0.02))
    vol_long  = np.array(ew_std(residual.tolist(), alpha=0.002))

    r50  = rolling_return(level, 50)
    r200 = rolling_return(level, 200)
    r1000= rolling_return(level, 1000)

    rsi14 = rsi(level, period=14)
    k, d = stoch_kd(level, window=14)
    amp, phase = hilbert_amp_phase(residual, window=32)
    vwap_dist = session_vwap_distance(ts, price)
    r2 = rolling_r2(level, window=100)

    # TOD bucket
    from .utils import tod_bucket_from_epoch
    tod = np.array([tod_bucket_from_epoch(t) for t in ts], dtype=int)

    rows = []
    for i, r in enumerate(kalman_rows):
        rows.append({
            "tickid": int(r["tickid"]),
            "timestamp": r["timestamp"],
            "level": float(level[i]),
            "slope": float(slope[i]),
            "residual": float(residual[i]),
            "vol_ewstd": float(vol_short[i]) if np.isfinite(vol_short[i]) else None,
            "vol_ewstd_long": float(vol_long[i]) if np.isfinite(vol_long[i]) else None,
            "r50": float(r50[i]) if np.isfinite(r50[i]) else None,
            "r200": float(r200[i]) if np.isfinite(r200[i]) else None,
            "r1000": float(r1000[i]) if np.isfinite(r1000[i]) else None,
            "rsi": float(rsi14[i]) if np.isfinite(rsi14[i]) else None,
            "stoch_k": float(k[i]) if np.isfinite(k[i]) else None,
            "stoch_d": float(d[i]) if np.isfinite(d[i]) else None,
            "hilbert_amp": float(amp[i]) if np.isfinite(amp[i]) else None,
            "hilbert_phase": float(phase[i]) if np.isfinite(phase[i]) else None,
            "vwap_dist": float(vwap_dist[i]) if np.isfinite(vwap_dist[i]) else None,
            "r2_lin": float(r2[i]) if np.isfinite(r2[i]) else None,
            "tod_bucket": int(tod[i])
        })
    return rows

def persist_features(rows: List[Dict[str,Any]]) -> int:
    # Skip initial rows until minimum history available is already embedded by NaNs; DB can accept NULLs.
    return upsert_many("ml_features", rows)

