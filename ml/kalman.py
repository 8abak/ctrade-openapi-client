# ml/kalman.py — REPLACE ENTIRE FILE
import os
import math
import time
from typing import List, Dict, Any, Tuple

import numpy as np

from .db import db_conn, upsert_many, fetch_ticks
from .utils import ewstd

# ENV knobs
DEADBAND = float(os.getenv("KALMAN_DEADBAND", "0.50"))  # dollars
ALPHA_VOL = float(os.getenv("KALMAN_VOL_ALPHA", "0.02"))  # EW-std alpha
Q_SCALE   = float(os.getenv("KALMAN_Q_SCALE", "0.002"))   # process noise multiplier
R_SCALE   = float(os.getenv("KALMAN_R_SCALE", "1.0"))     # measurement noise multiplier
OUTLIER_SIGMA = float(os.getenv("KALMAN_OUTLIER_SIGMA", "4.0"))
OUTLIER_R_MULT = float(os.getenv("KALMAN_OUTLIER_R_MULT", "9.0"))

# State: x=[level, slope], P (2x2)
F = np.array([[1.0, 1.0],
              [0.0, 1.0]])
H = np.array([[1.0, 0.0]])  # observe price ~ level
I = np.eye(2)

def soft_deadband(v: float, band: float) -> float:
    """Soft-threshold innovation by 'band' (in price units)."""
    a = abs(v)
    if a <= band:
        return 0.0
    return math.copysign(a - band, v)

def run_kalman(start: int, end: int) -> int:
    """
    Online causal 2-state Kalman with:
      - adaptive Q & R from EW-std of raw price
      - robust R inflation for outliers
      - **deadband on innovation** to suppress sub-$DEADBAND wiggles
    Writes kalman_states (UPSERT).
    """
    t0 = time.time()
    ticks: List[Dict[str, Any]] = fetch_ticks(start, end)
    if not ticks:
        print(f"[kalman] no ticks in {start}-{end}")
        return 0

    # Initial state from first price
    p0 = float(ticks[0]["price"])
    x = np.array([p0, 0.0], dtype=float)
    P = np.diag([1.0, 1.0])  # fairly tight; we’ll adapt quickly

    # EW volatility tracker on raw price (dollars)
    vol = 1e-6
    last_price = p0

    rows = []
    for i, r in enumerate(ticks):
        tid = int(r["tickid"])
        ts = r["timestamp"]
        z = float(r["price"])

        # Update EW-std of price (on price diffs tends to be smaller, but here we keep it simple)
        if i > 0:
            vol = ewstd(z, last_price, vol, alpha=ALPHA_VOL)
        last_price = z

        # Predict
        x_pred = F @ x
        P_pred = F @ P @ F.T

        # Adaptive noise: scale with vol (dollars)
        # Q: let slope wander slowly; small Q keeps line smooth
        q = max(1e-12, (vol * Q_SCALE))
        Q = np.array([[q, 0.0],
                      [0.0, q]])

        # Measurement noise from recent vol
        r_meas = max(1e-9, (vol * R_SCALE))
        Rm = np.array([[r_meas]])

        # Innovation with soft deadband
        inov = float(z - (H @ x_pred)[0])
        inov_db = soft_deadband(inov, DEADBAND)

        # Outlier inflation (on raw innovation before deadband) to keep big jumps stable
        if vol > 0:
            zscore = abs(inov) / (vol + 1e-12)
            if zscore > OUTLIER_SIGMA:
                Rm = Rm * OUTLIER_R_MULT

        # If deadband zeroed the innovation, keep the prediction (minimal correction)
        # Achieve this by inflating R heavily for the update step to reduce Kalman gain
        if inov_db == 0.0:
            R_eff = Rm * 1e6
            y = np.array([[0.0]])  # no correction
        else:
            R_eff = Rm
            y = np.array([[inov_db]])

        # Kalman update
        S = H @ P_pred @ H.T + R_eff
        K = (P_pred @ H.T) @ np.linalg.inv(S)
        x = x_pred + (K @ y).reshape(2)
        P = (I - K @ H) @ P_pred + Q  # Joseph form simplified with added Q to keep P from collapsing

        # Persist
        rows.append({
            "tickid": tid,
            "timestamp": ts,
            "price": z,
            "level": float(x[0]),
            "slope": float(x[1]),
            "var": float(P[0,0])
        })

        # Periodic flush (avoid huge single executemany)
        if len(rows) >= 5000:
            upsert_many("kalman_states", rows, conflict_key="tickid")
            rows.clear()

    if rows:
        upsert_many("kalman_states", rows, conflict_key="tickid")

    took = time.time() - t0
    print(f"[kalman] {start}-{end}: {end-start+1} ticks, deadband={DEADBAND:.2f}, vol_alpha={ALPHA_VOL}, Q_SCALE={Q_SCALE}, {took:.2f}s")
    return end - start + 1
