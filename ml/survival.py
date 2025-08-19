# ml/survival.py
from typing import Dict, List, Tuple
import numpy as np

def survival_curve_up(prices: np.ndarray, step: float=0.10, maxd: float=5.0, horizon: int=10000) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute S(d) = P(max future upmove >= d) within horizon.
    Returns (d_grid, S_values) where d in [0, maxd].
    """
    n = prices.size
    max_up = np.zeros(n)
    for i in range(n):
        j = min(n, i + 1 + horizon)
        future = prices[i:j]
        if future.size <= 1:
            max_up[i] = 0.0
        else:
            max_up[i] = float(np.max(future) - prices[i])
    ds = np.arange(0.0, maxd + 1e-9, step)
    S = np.array([float(np.mean(max_up >= d)) for d in ds])
    # enforce monotonicity (non-increasing in d)
    for k in range(1, S.size):
        if S[k] > S[k-1]:
            S[k] = S[k-1]
    return ds, S

def compact_curve(ds: np.ndarray, S: np.ndarray) -> List[List[float]]:
    return [[float(d), float(s)] for d, s in zip(ds, S)]
