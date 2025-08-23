# jobs/build_kalman_layers.py
import os, argparse
from typing import Tuple, Optional, List
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# ---------- DB ----------
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
        row = conn.execute(text("SELECT tickid FROM kalman_layers ORDER BY tickid DESC LIMIT 1")).fetchone()
    return int(row[0]) if row else None

def upsert(engine, ids: np.ndarray, k1: np.ndarray, k1_rts: np.ndarray, k2_cv: np.ndarray) -> None:
    rows = [{"tickid": int(i), "k1": float(a), "k1_rts": float(b), "k2_cv": float(c)}
            for i, a, b, c in zip(ids, k1, k1_rts, k2_cv)]
    sql = text("""
        INSERT INTO kalman_layers (tickid, k1, k1_rts, k2_cv)
        VALUES (:tickid, :k1, :k1_rts, :k2_cv)
        ON CONFLICT (tickid) DO UPDATE
        SET k1 = EXCLUDED.k1, k1_rts = EXCLUDED.k1_rts, k2_cv = EXCLUDED.k2_cv
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

# ---------- Robust scale ----------
def var_dy(y: np.ndarray) -> float:
    dy = np.diff(y, prepend=y[0])
    mad = np.median(np.abs(dy - np.median(dy))) + 1e-12
    return mad * mad

# ---------- k1: predictive CV Kalman (straight segments, low lag) ----------
K1_Q_SCALE      = float(os.getenv("K1_Q_SCALE", "1e-3"))  # smaller = straighter
K1_R_SCALE      = float(os.getenv("K1_R_SCALE", "1.0"))   # measurement noise from dy
K1_DEADBAND_SIG = float(os.getenv("K1_DEADBAND_SIG", "0.5"))
K1_GAIN_MULT    = float(os.getenv("K1_GAIN_MULT", "1.3"))

def cv_kalman_predictive(y: np.ndarray) -> np.ndarray:
    y = np.asarray(y, float); n = len(y)
    if n == 0: return np.array([], float)

    vd   = var_dy(y)
    qpos = max(1e-12, vd * K1_Q_SCALE)
    qvel = max(1e-12, qpos * 0.1)
    r    = max(1e-12, vd * K1_R_SCALE)
    sigR = np.sqrt(r)

    F = np.array([[1.0, 1.0], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.diag([qpos, qvel])
    R = np.array([[r]])
    I = np.eye(2)

    x = np.array([y[0], 0.0], float)
    # start with small covariance so we don't set a huge gate on tick 1
    P = np.diag([r, r])

    out = np.empty(n, float)
    for i in range(n):
        # PRIOR (plotted)
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q
        out[i] = x_pred[0]

        # gate on measurement sigma ONLY (not S), so threshold is in real $ units
        z = y[i]
        innov = z - float((H @ x_pred)[0])
        if abs(innov) < K1_DEADBAND_SIG * sigR:
            x, P = x_pred, P_pred
            continue

        # update with mild gain boost so slope snaps when a turn is real
        S = float((H @ P_pred @ H.T + R)[0, 0])
        K = (P_pred @ H.T) / S
        if K1_GAIN_MULT != 1.0:
            K = np.clip(K * K1_GAIN_MULT, 0.0, 1.0)

        x = x_pred + (K.flatten() * innov)
        P = (I - K @ H) @ P_pred

    return out

# ---------- k2_cv: Big-Move (confirmed ZigZag, extrapolated tail; never zeros) ----------
BM_ABS   = float(os.getenv("BM_ABS", "2.0"))   # dollars
BM_SIGMA = float(os.getenv("BM_SIGMA", "3.0")) # sigma on MAD(dy)

def zigzag_confirmed_extrap(y: np.ndarray) -> np.ndarray:
    y = np.asarray(y, float); n = len(y)
    if n == 0: return np.array([], float)

    thr = max(BM_ABS, BM_SIGMA * np.sqrt(var_dy(y)))

    piv_i: List[int] = [0]
    piv_v: List[float] = [y[0]]
    direction = 0
    hi, i_hi = y[0], 0
    lo, i_lo = y[0], 0

    for i in range(1, n):
        p = y[i]
        if p > hi: hi, i_hi = p, i
        if p < lo: lo, i_lo = p, i
        if direction >= 0 and (hi - p) >= thr:         # confirm DOWN
            piv_i.append(i_hi); piv_v.append(hi)
            direction = -1
            lo, i_lo = p, i; hi, i_hi = p, i
        elif direction <= 0 and (p - lo) >= thr:       # confirm UP
            piv_i.append(i_lo); piv_v.append(lo)
            direction = +1
            hi, i_hi = p, i; lo, i_lo = p, i

    out = np.empty(n, float)
    # draw confirmed segments
    for a in range(len(piv_i) - 1):
        i0, i1 = piv_i[a], piv_i[a + 1]
        y0, y1 = piv_v[a], piv_v[a + 1]
        t = np.linspace(0.0, 1.0, i1 - i0 + 1)
        out[i0:i1 + 1] = y0 + (y1 - y0) * t

    # tail = slope of last confirmed segment
    last_start = piv_i[-2] if len(piv_i) >= 2 else 0
    last_end   = piv_i[-1]
    y0, y1 = y[last_start], y[last_end]
    denom = max(1, last_end - last_start)
    m = (y1 - y0) / denom
    idx = np.arange(last_end, n)
    out[last_end:] = y1 + m * (idx - last_end)

    # if only one pivot, fill head flat
    if len(piv_i) == 1:
        out[:last_end] = y0
    return out

# ---------- k1_rts: CV Kalman + RTS on Big-Move (curved & smoother) ----------
RTS_QPOS_SCALE = float(os.getenv("RTS_QPOS_SCALE", "0.5"))
RTS_QVEL_SCALE = float(os.getenv("RTS_QVEL_SCALE", "1.0"))
RTS_R_SCALE    = float(os.getenv("RTS_R_SCALE", "0.2"))

def cv_rts_on_series(obs: np.ndarray, mids_for_scale: np.ndarray) -> np.ndarray:
    z = np.asarray(obs, float); n = len(z)
    if n == 0: return np.array([], float)

    vd = var_dy(mids_for_scale)  # scale from real price noise
    qpos = max(1e-12, vd * RTS_QPOS_SCALE)
    qvel = max(1e-12, vd * RTS_QVEL_SCALE)
    r    = max(1e-12, vd * RTS_R_SCALE)

    F = np.array([[1.0, 1.0], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    Q = np.diag([qpos, qvel])
    R = np.array([[r]])
    I = np.eye(2)

    x_f = np.empty((n, 2)); P_f = np.empty((n, 2, 2))
    x_p = np.empty((n, 2)); P_p = np.empty((n, 2, 2))

    x = np.array([z[0], 0.0], float)
    P = np.diag([r, r])

    # ---- forward pass ----
    for i in range(n):
        # predict
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q

        # update (use scalar S and flatten K to avoid shape issues)
        y_t = z[i] - (H @ x_pred)[0]
        S   = float((H @ P_pred @ H.T + R)[0, 0])
        K   = (P_pred @ H.T) / S          # shape (2,1)
        x   = x_pred + K.flatten() * y_t  # shape (2,)
        P   = (I - K @ H) @ P_pred

        x_f[i], P_f[i] = x, P
        x_p[i], P_p[i] = x_pred, P_pred

    # ---- RTS smoother ----
    xs = x_f.copy(); Ps = P_f.copy()
    for t in range(n - 2, -1, -1):
        C = P_f[t] @ F.T @ np.linalg.inv(P_p[t + 1])
        xs[t] = x_f[t] + C @ (xs[t + 1] - x_p[t + 1])
        Ps[t] = P_f[t] + C @ (Ps[t + 1] - P_p[t + 1]) @ C.T

    return xs[:, 0]


# ---------- main ----------
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

    # k2_cv: Big-Move
    k_big = zigzag_confirmed_extrap(mids)

    # k1: predictive CV Kalman (now actually updates)
    k1 = cv_kalman_predictive(mids)

    # k1_rts: CV Kalman + RTS on Big-Move (smoother & different)
    k1_rts = cv_rts_on_series(k_big, mids_for_scale=mids)

    upsert(engine, ids, k1, k1_rts, k_big)
    print(f"✔ upserted {len(ids)} rows into kalman_layers ({ids[0]}..{ids[-1]})")

if __name__ == "__main__":
    main()
