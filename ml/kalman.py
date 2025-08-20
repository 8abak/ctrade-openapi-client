# ml/kalman.py — REPLACE ENTIRE FILE (adds deadband smoothing + compat alias)
import os
import math
import time
from typing import List, Dict, Any

import numpy as np

from .db import db_conn, upsert_many, fetch_ticks
from .utils import ewstd

# ENV knobs
DEADBAND = float(os.getenv("KALMAN_DEADBAND", "0.50"))  # dollars to ignore
ALPHA_VOL = float(os.getenv("KALMAN_VOL_ALPHA", "0.02"))  # EW-std alpha
Q_SCALE   = float(os.getenv("KALMAN_Q_SCALE", "0.002"))   # process noise multiplier
R_SCALE   = float(os.getenv("KALMAN_R_SCALE", "1.0"))     # measurement noise multiplier
OUTLIER_SIGMA = float(os.getenv("KALMAN_OUTLIER_SIGMA", "4.0"))
OUTLIER_R_MULT = float(os.getenv("KALMAN_OUTLIER_R_MULT", "9.0"))

# State: x=[level, slope]
F = np.array([[1.0, 1.0],
              [0.0, 1.0]])
H = np.array([[1.0, 0.0]])
I = np.eye(2)

def _soft_deadband(v: float, band: float) -> float:
    a = abs(v)
    if a <= band:
        return 0.0
    return math.copysign(a - band, v)

def run_kalman(start: int, end: int) -> int:
    """
    Online, causal 2-state Kalman (level+slope) with:
      • Adaptive Q/R from EW-std of raw price (on price diffs)
      • Robust R inflation for > OUTLIER_SIGMA*σ innovations
      • **Deadband on innovation**: ignore sub-$DEADBAND moves
    Writes to kalman_states with UPSERTs.
    """
    t0 = time.time()
    ticks: List[Dict[str, Any]] = fetch_ticks(start, end)
    if not ticks:
        print(f"[kalman] no ticks in {start}-{end}")
        return 0

    p0 = float(ticks[0]["price"])
    x = np.array([p0, 0.0], dtype=float)
    P = np.diag([1.0, 1.0])

    vol = 1e-6
    last_price = p0

    rows = []
    for i, r in enumerate(ticks):
        tid = int(r["tickid"])
        ts = r["timestamp"]
        z = float(r["price"])

        # EW volatility (of price changes)
        if i > 0:
            vol = ewstd(z, last_price, vol, alpha=ALPHA_VOL)
        last_price = z

        # Predict
        x_pred = F @ x
        P_pred = F @ P @ F.T

        # Adaptive noises
        q = max(1e-12, (vol * Q_SCALE))
        Q = np.array([[q, 0.0], [0.0, q]])
        r_meas = max(1e-9, (vol * R_SCALE))
        Rm = np.array([[r_meas]])

        # Innovation & deadband
        inov = float(z - (H @ x_pred)[0])
        inov_db = _soft_deadband(inov, DEADBAND)

        # Outlier handling (on raw inov)
        if vol > 0:
            zscore = abs(inov) / (vol + 1e-12)
            if zscore > OUTLIER_SIGMA:
                Rm = Rm * OUTLIER_R_MULT

        # If deadband zeroed it, squash gain by inflating R massively
        if inov_db == 0.0:
            R_eff = Rm * 1e6
            y = np.array([[0.0]])
        else:
            R_eff = Rm
            y = np.array([[inov_db]])

        # Update
        S = H @ P_pred @ H.T + R_eff
        K = (P_pred @ H.T) @ np.linalg.inv(S)
        x = x_pred + (K @ y).reshape(2)
        P = (I - K @ H) @ P_pred + Q  # keep P positive & prevent collapse

        rows.append({
            "tickid": tid,
            "timestamp": ts,
            "price": z,
            "level": float(x[0]),
            "slope": float(x[1]),
            "var": float(P[0, 0])
        })

        if len(rows) >= 5000:
            upsert_many("kalman_states", rows, conflict_key="tickid")
            rows.clear()

    if rows:
        upsert_many("kalman_states", rows, conflict_key="tickid")

    print(f"[kalman] {start}-{end}: {end-start+1} ticks | deadband=${DEADBAND:.2f} | took {time.time()-t0:.2f}s")
    return end - start + 1

# ---- compatibility alias (older code imports persist_kalman) ----
def persist_kalman(start: int, end: int) -> int:
    return run_kalman(start, end)
