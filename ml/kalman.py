# ml/kalman.py
from typing import List, Dict, Any, Iterable
import math

from .db import upsert_many

class OnlineKalman:
    """
    2-state Kalman: state = [level, slope]
    F = [[1,1],[0,1]], H = [1,0]
    Adaptive Q,R via EW std of price. Robust: inflate R when |residual|>4*sigma.
    """
    def __init__(self, q_base: float=1e-4, r_base: float=1e-3, alpha: float=0.01):
        self.level = None
        self.slope = 0.0
        self.P00 = 1.0
        self.P01 = 0.0
        self.P10 = 0.0
        self.P11 = 1.0
        self.q_base = q_base
        self.r_base = r_base
        self.alpha = alpha
        self.ew_mean = None
        self.ew_var = None

    def _ew_update(self, x: float):
        if self.ew_mean is None:
            self.ew_mean = x
            self.ew_var = 0.0
        else:
            d = x - self.ew_mean
            self.ew_mean += self.alpha * d
            self.ew_var = (1 - self.alpha) * (self.ew_var + self.alpha * d * d)

    def update(self, z: float):
        # EW stats for adaptive noise
        self._ew_update(z)
        sigma = math.sqrt(max(self.ew_var, 1e-12))
        Q = self.q_base * (1.0 + sigma)
        R = self.r_base * (1.0 + sigma)

        if self.level is None:
            self.level = z
            self.slope = 0.0
            self.P00, self.P11 = 1.0, 1.0
            self.P01 = self.P10 = 0.0

        # Predict
        x0 = self.level + self.slope  # level'
        x1 = self.slope               # slope'
        P00p = self.P00 + 2*self.P01 + self.P11 + Q
        P01p = self.P01 + self.P11
        P10p = P01p
        P11p = self.P11 + Q

        # Update
        y = z - x0  # residual
        S = P00p + R
        K0 = P00p / S
        K1 = P10p / S

        # Robust: inflate R if big residual
        if abs(y) > 4.0 * sigma:
            R_big = R * 9.0
            S = P00p + R_big
            K0 = P00p / S
            K1 = P10p / S

        self.level = x0 + K0 * y
        self.slope = x1 + K1 * y

        self.P00 = (1 - K0) * P00p
        self.P01 = (1 - K0) * P01p
        self.P10 = -K1 * P00p + P10p
        self.P11 = -K1 * P01p + P11p

        var = max(self.P00, 1e-12)
        return self.level, self.slope, var

def run_kalman(records: Iterable[Dict[str, Any]], alpha: float=0.01) -> List[Dict[str, Any]]:
    kf = OnlineKalman(alpha=alpha)
    out = []
    for r in records:
        lvl, slp, var = kf.update(float(r["price"]))
        out.append({
            "tickid": int(r["tickid"]),
            "timestamp": r["timestamp"],
            "price": float(r["price"]),
            "level": float(lvl),
            "slope": float(slp),
            "var": float(var)
        })
    return out

def persist_kalman(rows: List[Dict[str, Any]]) -> int:
    return upsert_many("kalman_states", rows)
