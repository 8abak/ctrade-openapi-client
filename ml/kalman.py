# PATH: ml/kalman.py
#
# Simple 1D scalar Kalman filter for price smoothing.
# Model:
#   x_t   = x_{t-1}                    + w_t    (process noise, var = q)
#   z_t   = x_t                        + v_t    (measurement noise, var = r)
#
# We use this to compute ticks.kal from ticks.mid, walking forward.

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class ScalarKalmanConfig:
  process_var: float = 1e-4  # Q – how fast true price can drift
  meas_var: float = 1e-2     # R – how noisy the observed mid is
  init_var: float = 1.0      # initial variance P_0


class ScalarKalmanFilter:
  def __init__(self, cfg: ScalarKalmanConfig):
    self.cfg = cfg
    self.x: Optional[float] = None  # current state estimate
    self.P: Optional[float] = None  # current variance

  def reset(self, x0: float, P0: Optional[float] = None) -> None:
    self.x = float(x0)
    self.P = float(P0 if P0 is not None else self.cfg.init_var)

  def step(self, z: float) -> float:
    """
    Advance filter with new observation z, return updated state x_t.
    If filter was not initialised yet, we initialise x0 = z.
    """

    q = self.cfg.process_var
    r = self.cfg.meas_var

    if self.x is None or self.P is None:
      # cold start
      self.x = float(z)
      self.P = float(self.cfg.init_var)

    # Predict
    x_prior = self.x               # x_{t|t-1}
    P_prior = self.P + q           # P_{t|t-1}

    # Update
    K = P_prior / (P_prior + r)    # Kalman gain
    x_post = x_prior + K * (z - x_prior)
    P_post = (1.0 - K) * P_prior

    self.x = x_post
    self.P = P_post
    return x_post

  def state(self) -> Tuple[Optional[float], Optional[float]]:
    return self.x, self.P
