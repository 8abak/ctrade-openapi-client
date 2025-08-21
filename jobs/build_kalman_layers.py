# jobs/build_kalman_layers.py
import os, argparse, math
from typing import Tuple
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# -------- Kalman (scalar, random-walk) --------
def kalman_pass(y: np.ndarray, q: float, r: float, deadband: float = 0.0,
                x0: float | None = None, P0: float | None = None) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Simple 1D Kalman filter with A=1, H=1.
    Returns: (x_filt, P_filt, x_pred, P_pred) arrays (for RTS).
    """
    y = np.asarray(y, float)
    n = len(y)
    x_f = np.empty(n); P_f = np.empty(n)
    x_p = np.empty(n); P_p = np.empty(n)

    x = y[0] if x0 is None else x0
    P = (r if P0 is None else P0)

    for i, z in enumerate(y):
        # predict
        x_pred = x
        P_pred = P + q
        # update
        innov = z - x_pred
        if abs(innov) < deadband:
            innov = 0.0
        S = P_pred + r
        K = P_pred / S
        x = x_pred + K * innov
        P = (1 - K) * P_pred

        x_p[i], P_p[i] = x_pred, P_pred
        x_f[i], P_f[i] = x, P
    return x_f, P_f, x_p, P_p

# -------- RTS smoother for the scalar model --------
def rts_smoother(x_f: np.ndarray, P_f: np.ndarray, x_p: np.ndarray, P_p: np.ndarray) -> np.ndarray:
    n = len(x_f)
    xs = np.copy(x_f)
    Ps = np.copy(P_f)
    for t in range(n - 2, -1, -1):
        C = P_f[t] / P_p[t + 1]
        xs[t] = x_f[t] + C * (xs[t + 1] - x_p[t + 1])
        Ps[t] = P_f[t] + C * (Ps[t + 1] - P_p[t + 1]) * C
    return xs

# -------- Constant-velocity Kalman on k1 --------
def kalman_cv_pass(z: np.ndarray, q_pos: float, q_vel: float, r: float,
                   x0: np.ndarray | None = None, P0: np.ndarray | None = None, dt: float = 1.0) -> np.ndarray:
    """
    2-state constant-velocity model:
      x = [pos, vel]
      F = [[1, dt],[0,1]], H = [1, 0]
    z: measurements (k1)
    Returns the filtered position array (k2_cv).
    """
    z = np.asarray(z, float)
    n = len(z)
    F = np.array([[1.0, dt],[0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.diag([q_pos, q_vel])
    R = np.array([[r]])

    x = np.array([z[0], 0.0]) if x0 is None else x0.astype(float)
    P = np.eye(2) if P0 is None else P.astype(float)

    out = np.empty(n)
    for i in range(n):
        # predict
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q
        # update
        y = z[i] - (H @ x_pred)[0]
        S = H @ P_pred @ H.T + R
        K = P_pred @ H.T @ np.linalg.inv(S)
        x = x_pred + (K.flatten() * y)
        P = (np.eye(2) - K @ H) @ P_pred
        out[i] = x[0]
    return out

# -------- Robust noises from data --------
def robust_qr_from(y: np.ndarray) -> Tuple[float, float]:
    dy = np.diff(y, prepend=y[0])
    mad_dy = np.median(np.abs(dy - np.median(dy))) + 1e-9
    mad_y  = np.median(np.abs(y  - np.median(y))) + 1e-9
    q = mad_dy**2
    r = mad_y**2
    return q, r

def fetch_ticks(engine, start_id: int, end_id: int) -> tuple[np.ndarray, np.ndarray]:
    sql = text("""
        SELECT id, COALESCE(mid, (bid+ask)/2.0) AS mid
        FROM ticks
        WHERE id BETWEEN :s AND :e
        ORDER BY id ASC
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"s": start_id, "e": end_id}).fetchall()
    ids = np.array([r[0] for r in rows], dtype=np.int64)
    mids = np.array([float(r[1]) for r in rows], dtype=np.float64)
    return ids, mids

def last_saved(engine) -> Tuple[int | None, float | None, float | None]:
    sql = text("SELECT tickid, k1, k2_cv FROM kalman_layers ORDER BY tickid DESC LIMIT 1;")
    with engine.begin() as conn:
        row = conn.execute(sql).fetchone()
        if row:
            return int(row[0]), float(row[1]), float(row[2])
        return None, None, None

def upsert(engine, ids: np.ndarray, k1: np.ndarray, k1_rts: np.ndarray, k2_cv: np.ndarray):
    rows = [{"tickid": int(i), "k1": float(a), "k1_rts": float(b), "k2_cv": float(c)}
            for i, a, b, c in zip(ids, k1, k1_rts, k2_cv)]
    sql = text("""
        INSERT INTO kalman_layers (tickid, k1, k1_rts, k2_cv)
        VALUES (:tickid, :k1, :k1_rts, :k2_cv)
        ON CONFLICT (tickid) DO UPDATE
          SET k1 = EXCLUDED.k1,
              k1_rts = EXCLUDED.k1_rts,
              k2_cv = EXCLUDED.k2_cv;
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end",   type=int, required=True)
    args = ap.parse_args()

    engine = create_engine(DB_URL)

    # If we're resuming, ensure we start after the last saved
    last_tickid, last_k1, last_k2 = last_saved(engine)
    start_id = args.start
    if last_tickid is not None and last_tickid >= args.start:
        start_id = last_tickid + 1

    ids, mids = fetch_ticks(engine, start_id, args.end)
    if len(ids) == 0:
        print("Nothing to do."); return

    # Estimate noises from current window
    q1, r1 = robust_qr_from(mids)

    # Seed from previous run if available
    x0_k1 = last_k1 if last_k1 is not None else None

    # Pass 1: scalar Kalman
    k1, P_f, x_p, P_p = kalman_pass(mids, q=q1, r=r1, deadband=0.0, x0=x0_k1, P0=r1)

    # RTS smoother on k1
    k1_rts = rts_smoother(k1, P_f, x_p, P_p)

    # Constant-velocity Kalman using k1 as measurement
    # Make velocity noise much smaller than position noise to favor smooth slopes
    qpos = max(q1, 1e-9)
    qvel = max(0.01 * qpos, 1e-10)
    k2_cv = kalman_cv_pass(k1, q_pos=qpos, q_vel=qvel, r=r1,
                           x0=(np.array([last_k2, 0.0]) if (last_tickid is not None and last_k2 is not None) else None),
                           P0=None)

    upsert(engine, ids, k1, k1_rts, k2_cv)
    print(f"✔ upserted {len(ids)} rows into kalman_layers ({ids[0]}..{ids[-1]})")

if __name__ == "__main__":
    main()
