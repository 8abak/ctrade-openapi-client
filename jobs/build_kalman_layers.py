# jobs/build_kalman_layers.py
import os, argparse
from typing import Tuple, Optional, List
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# ---------------- DB helpers ----------------
def fetch_ticks(engine, start_id: int, end_id: int) -> Tuple[np.ndarray, np.ndarray]:
    sql = text("""
        SELECT id, COALESCE(mid, (bid+ask)/2.0) AS mid
        FROM ticks
        WHERE id BETWEEN :s AND :e
        ORDER BY id ASC
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"s": start_id, "e": end_id}).fetchall()
    ids  = np.array([r[0] for r in rows], dtype=np.int64)
    mids = np.array([float(r[1]) for r in rows], dtype=np.float64)
    return ids, mids

def last_saved(engine) -> Optional[int]:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT tickid FROM kalman_layers ORDER BY tickid DESC LIMIT 1;")).fetchone()
    return int(row[0]) if row else None

def upsert(engine, ids: np.ndarray, k1: np.ndarray, k1_rts: np.ndarray, k2_cv: np.ndarray) -> None:
    rows = [{"tickid": int(i), "k1": float(a), "k1_rts": float(b), "k2_cv": float(c)}
            for i, a, b, c in zip(ids, k1, k1_rts, k2_cv)]
    sql = text("""
        INSERT INTO kalman_layers (tickid, k1, k1_rts, k2_cv)
        VALUES (:tickid, :k1, :k1_rts, :k2_cv)
        ON CONFLICT (tickid) DO UPDATE
        SET k1 = EXCLUDED.k1, k1_rts = EXCLUDED.k1_rts, k2_cv = EXCLUDED.k2_cv;
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

# ---------------- Robust scales ----------------
def robust_scales(y: np.ndarray) -> Tuple[float, float]:
    dy = np.diff(y, prepend=y[0])
    mad_dy = np.median(np.abs(dy - np.median(dy))) + 1e-9
    mad_y  = np.median(np.abs(y  - np.median(y)))  + 1e-9
    return mad_dy**2, mad_y**2

# ---------------- k1: CV Kalman, predictive straight segments ----------------
# Emits PRIOR (prediction) each step -> near zero-lag.
# Updates are gated by sigma deadband; on update we slightly boost gain to snap slope.
K1_Q_SCALE       = float(os.getenv("K1_Q_SCALE", "1e-3"))  # small -> straighter between turns
K1_R_SCALE       = float(os.getenv("K1_R_SCALE", "1.0"))   # from dy
K1_DEADBAND_SIG  = float(os.getenv("K1_DEADBAND_SIG", "0.55"))
K1_GAIN_MULT     = float(os.getenv("K1_GAIN_MULT", "1.35"))  # >1 boosts update when a turn hits; clamp inside

def kalman_cv_predictive(y: np.ndarray,
                         q_scale: float, r_scale: float,
                         deadband_sig: float, gain_mult: float,
                         dt: float = 1.0) -> np.ndarray:
    y = np.asarray(y, float)
    n = len(y)
    if n == 0:
        return np.array([], float)

    var_dy, _ = robust_scales(y)
    q_pos = max(1e-12, var_dy * q_scale)
    q_vel = max(1e-12, q_pos * 0.10)
    r     = max(1e-12, var_dy * r_scale)

    F = np.array([[1.0, dt], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.diag([q_pos, q_vel])
    R = np.array([[r]])
    I = np.eye(2)

    x = np.array([y[0], 0.0], dtype=float)
    P = np.eye(2)

    out = np.empty(n, float)

    for i in range(n):
        # Predict (PRIOR) — this is what we PLOT to avoid lag and keep straight segments
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q
        out[i] = x_pred[0]   # <- plot PRIOR => line segments with current slope

        # Innovation & gate
        z = y[i]
        innov = z - float((H @ x_pred)[0])
        S = float((H @ P_pred @ H.T + R)[0, 0])
        if abs(innov) < deadband_sig * np.sqrt(S):
            # Skip update -> keep slope; next step continues same straight line
            x, P = x_pred, P_pred
            continue

        # Do an update, with a slight gain boost to be more "snappy" at regime changes
        K = (P_pred @ H.T) / S
        if gain_mult != 1.0:
            K = np.clip(K * gain_mult, 0.0, 1.0)

        x = x_pred + (K.flatten() * innov)
        P = (I - K @ H) @ P_pred

    return out

# ---------------- Big-Move: HH/LL ZigZag (piecewise straight) ----------------
BM_ABS      = float(os.getenv("BM_ABS", "2.0"))   # absolute $
BM_SIGMA    = float(os.getenv("BM_SIGMA", "3.0")) # sigma based on MAD(dy)

def zigzag_bigmove(y: np.ndarray, abs_thr: float, sigma_mult: float) -> np.ndarray:
    y = np.asarray(y, float); n = len(y)
    if n == 0:
        return np.array([], float)

    var_dy, _ = robust_scales(y)
    thr = max(abs_thr, sigma_mult * np.sqrt(var_dy))

    piv_idx: List[int] = [0]
    piv_val: List[float] = [y[0]]
    direction = 0  # 0 unknown, +1 up, -1 down
    cand_hi = y[0]; i_hi = 0
    cand_lo = y[0]; i_lo = 0

    for i in range(1, n):
        p = y[i]
        if p > cand_hi: cand_hi, i_hi = p, i
        if p < cand_lo: cand_lo, i_lo = p, i

        if direction >= 0 and (cand_hi - p) >= thr:   # confirm DOWN
            piv_idx.append(i_hi); piv_val.append(cand_hi)
            direction = -1
            cand_lo, i_lo = p, i
            cand_hi, i_hi = p, i
            continue
        if direction <= 0 and (p - cand_lo) >= thr:   # confirm UP
            piv_idx.append(i_lo); piv_val.append(cand_lo)
            direction = +1
            cand_hi, i_hi = p, i
            cand_lo, i_lo = p, i
            continue

    last_i = i_hi if direction == +1 else (i_lo if direction == -1 else n - 1)
    if piv_idx[-1] != last_i:
        piv_idx.append(last_i); piv_val.append(y[last_i])

    out = np.empty(n, float)
    for a in range(len(piv_idx) - 1):
        i0, i1 = piv_idx[a], piv_idx[a + 1]
        y0, y1 = piv_val[a], piv_val[a + 1]
        if i1 == i0:
            out[i0] = y0
            continue
        t = np.linspace(0.0, 1.0, i1 - i0 + 1)
        out[i0:i1 + 1] = y0 + (y1 - y0) * t
    if piv_idx[0] > 0:
        out[:piv_idx[0]] = piv_val[0]
    return out

# ---------------- RTS on Big-Move ----------------
RTS_Q_SCALE = float(os.getenv("RTS_Q_SCALE", "0.5"))
RTS_R_SCALE = float(os.getenv("RTS_R_SCALE", "0.5"))

def kalman_rw_and_rts(obs: np.ndarray, q_scale: float, r_scale: float) -> np.ndarray:
    """Scalar RW Kalman on obs, then RTS smooth."""
    y = np.asarray(obs, float)
    n = len(y)
    if n == 0: return np.array([], float)

    var_dy, _ = robust_scales(y)
    q = max(1e-12, var_dy * q_scale)
    r = max(1e-12, var_dy * r_scale)

    x_f = np.empty(n); P_f = np.empty(n)
    x_p = np.empty(n); P_p = np.empty(n)

    x = y[0]; P = r
    for i, z in enumerate(y):
        x_pred = x
        P_pred = P + q
        S = P_pred + r
        K = P_pred / S
        x = x_pred + K * (z - x_pred)
        P = (1.0 - K) * P_pred
        x_p[i], P_p[i] = x_pred, P_pred
        x_f[i], P_f[i] = x, P

    # RTS
    xs = np.copy(x_f); Ps = np.copy(P_f)
    for t in range(n - 2, -1, -1):
        C = P_f[t] / P_p[t + 1]
        xs[t] = x_f[t] + C * (xs[t + 1] - x_p[t + 1])
        Ps[t] = P_f[t] + C * (Ps[t + 1] - P_p[t + 1]) * C
    return xs

# ---------------- main ----------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end",   type=int, required=True)
    args = ap.parse_args()

    engine = create_engine(DB_URL)
    resume = last_saved(engine)
    start_id = args.start if (resume is None or resume < args.start) else (resume + 1)

    ids, mids = fetch_ticks(engine, start_id, args.end)
    if len(ids) == 0:
        print("Nothing to do."); return

    # k2_cv: Big-Move (piecewise straight)
    k_big = zigzag_bigmove(mids, BM_ABS, BM_SIGMA)

    # k1: predictive straight segments (CV Kalman, prior output)
    k1 = kalman_cv_predictive(mids, K1_Q_SCALE, K1_R_SCALE, K1_DEADBAND_SIG, K1_GAIN_MULT)

    # k1_rts: RTS **of Big-Move**, not of k1
    k1_rts = kalman_rw_and_rts(k_big, RTS_Q_SCALE, RTS_R_SCALE)

    upsert(engine, ids, k1, k1_rts, k_big)
    print(f"✔ upserted {len(ids)} rows into kalman_layers ({ids[0]}..{ids[-1]})")

if __name__ == "__main__":
    main()
