# jobs/build_kalman_layers.py
import os, argparse
from typing import Tuple, Optional
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# === ENV knobs ===
# Old Kalman (straight-edge) feel
K1_Q_SCALE      = float(os.getenv("K1_Q_SCALE",      "1e-3"))   # tiny -> long straight segments
K1_R_SCALE      = float(os.getenv("K1_R_SCALE",      "1.0"))
K1_DEADBAND_SIG = float(os.getenv("K1_DEADBAND_SIG", "0.75"))   # gate by |innov| < deadband * sqrt(S)

# RTS (keep as-is on scalar KF)
RTS_Q_SCALE     = float(os.getenv("RTS_Q_SCALE",     "1.0"))
RTS_R_SCALE     = float(os.getenv("RTS_R_SCALE",     "1.0"))

# Big-Move tracker (very low reactivity)
BM_Q_SCALE      = float(os.getenv("BM_Q_SCALE",      "1e-5"))   # even smaller process noise
BM_R_SCALE      = float(os.getenv("BM_R_SCALE",      "1.0"))
BM_DEADBAND_SIG = float(os.getenv("BM_DEADBAND_SIG", "3.0"))    # only react to ~3σ (or more) surprises
BM_MIN_ABS      = float(os.getenv("BM_MIN_ABS",      "2.0"))    # or absolute >= $2 move (edit to taste)

# -------------------- Utilities --------------------
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

def last_saved(engine) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    # keep last of k1 and k2_cv for warm start
    sql = text("""SELECT tickid, k1, k2_cv FROM kalman_layers ORDER BY tickid DESC LIMIT 1;""")
    with engine.begin() as conn:
        row = conn.execute(sql).fetchone()
    if row:
        return int(row[0]), float(row[1]), float(row[2])
    return None, None, None

def upsert(engine, ids: np.ndarray, k1: np.ndarray, k1_rts: np.ndarray, k2_cv: np.ndarray) -> None:
    rows = [
        {"tickid": int(i), "k1": float(a), "k1_rts": float(b), "k2_cv": float(c)}
        for i, a, b, c in zip(ids, k1, k1_rts, k2_cv)
    ]
    sql = text("""
        INSERT INTO kalman_layers (tickid, k1, k1_rts, k2_cv)
        VALUES (:tickid, :k1, :k1_rts, :k2_cv)
        ON CONFLICT (tickid) DO UPDATE
        SET k1 = EXCLUDED.k1, k1_rts = EXCLUDED.k1_rts, k2_cv = EXCLUDED.k2_cv;
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

# -------------------- Old-style Kalman (line-1) --------------------
# 2-state constant-velocity KF with deadbanded innovation (skip small updates)
def kalman_cv_deadband(y: np.ndarray, q_scale: float, r_scale: float, deadband_sig: float,
                       x0: Optional[np.ndarray] = None, P0: Optional[np.ndarray] = None,
                       dt: float = 1.0) -> np.ndarray:
    y = np.asarray(y, float)
    n = len(y)

    F = np.array([[1.0, dt], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])
    I = np.eye(2)

    # robust noise scales from data (like your current file)
    dy = np.diff(y, prepend=y[0])
    mad_dy = np.median(np.abs(dy - np.median(dy))) + 1e-9
    mad_y  = np.median(np.abs(y  - np.median(y)))  + 1e-9
    q_pos = (mad_dy ** 2) * q_scale
    q_vel = max(1e-8, q_pos * 0.1)
    r     = (mad_y  ** 2) * r_scale

    Q = np.diag([q_pos, q_vel])
    R = np.array([[r]])

    if x0 is None:
        x = np.array([y[0], 0.0], dtype=float)
    else:
        x = x0.astype(float)
    if P0 is None:
        P = np.eye(2)
    else:
        P = P0.astype(float)

    out = np.empty(n)
    for i in range(n):
        # Predict
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q

        # Innovation
        z = y[i]
        innov = z - float((H @ x_pred)[0])
        S = float((H @ P_pred @ H.T + R)[0, 0])

        # Gate small innovations -> skip update (keeps straight segments + crisp corners)
        if abs(innov) < deadband_sig * np.sqrt(S):
            x = x_pred
            P = P_pred
        else:
            K = (P_pred @ H.T) / S
            x = x_pred + (K.flatten() * innov)
            P = (np.eye(2) - K @ H) @ P_pred

        out[i] = x[0]
    return out

# -------------------- Scalar KF + RTS (line-2, unchanged behaviour) --------------------
def kalman_pass_scalar(y: np.ndarray, q: float, r: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    y = np.asarray(y, float)
    n = len(y)
    x_f = np.empty(n); P_f = np.empty(n)
    x_p = np.empty(n); P_p = np.empty(n)
    x = y[0]; P = r
    for i, z in enumerate(y):
        # predict (A=1)
        x_pred = x
        P_pred = P + q
        # update (H=1)
        S = P_pred + r
        K = P_pred / S
        x = x_pred + K * (z - x_pred)
        P = (1.0 - K) * P_pred
        x_p[i], P_p[i] = x_pred, P_pred
        x_f[i], P_f[i] = x, P
    return x_f, P_f, x_p, P_p

def rts_smoother_scalar(x_f, P_f, x_p, P_p) -> np.ndarray:
    n = len(x_f)
    xs = np.copy(x_f)
    Ps = np.copy(P_f)
    for t in range(n - 2, -1, -1):
        C = P_f[t] / P_p[t + 1]
        xs[t] = x_f[t] + C * (xs[t + 1] - x_p[t + 1])
        Ps[t] = P_f[t] + C * (Ps[t + 1] - P_p[t + 1]) * C
    return xs

# -------------------- Big-Move tracker (line-3) --------------------
# Same CV model, but with *huge* deadband and optional absolute threshold: reacts only to very big moves
def bigmove_tracker(y: np.ndarray, q_scale: float, r_scale: float, deadband_sig: float, min_abs: float,
                    x0: Optional[np.ndarray] = None, P0: Optional[np.ndarray] = None,
                    dt: float = 1.0) -> np.ndarray:
    y = np.asarray(y, float)
    n = len(y)

    F = np.array([[1.0, dt], [0.0, 1.0]])
    H = np.array([[1.0, 0.0]])

    dy = np.diff(y, prepend=y[0])
    mad_dy = np.median(np.abs(dy - np.median(dy))) + 1e-9
    mad_y  = np.median(np.abs(y  - np.median(y)))  + 1e-9
    q_pos = (mad_dy ** 2) * q_scale
    q_vel = max(1e-8, q_pos * 0.1)
    r     = (mad_y  ** 2) * r_scale

    Q = np.diag([q_pos, q_vel])
    R = np.array([[r]])

    if x0 is None:
        x = np.array([y[0], 0.0], dtype=float)
    else:
        x = x0.astype(float)
    if P0 is None:
        P = np.eye(2)
    else:
        P = P0.astype(float)

    out = np.empty(n)
    for i in range(n):
        # Predict
        x_pred = F @ x
        P_pred = F @ P @ F.T + Q

        # Innovation and gates
        z = y[i]
        innov = z - float((H @ x_pred)[0])
        S = float((H @ P_pred @ H.T + R)[0, 0])

        # Use BOTH a sigma gate and an absolute-dollar gate
        if (abs(innov) < deadband_sig * np.sqrt(S)) and (abs(innov) < min_abs):
            x = x_pred
            P = P_pred
        else:
            K = (P_pred @ H.T) / S
            x = x_pred + (K.flatten() * innov)
            P = (np.eye(2) - K @ H) @ P_pred

        out[i] = x[0]
    return out

# -------------------- Main --------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end",   type=int, required=True)
    args = ap.parse_args()

    engine = create_engine(DB_URL)

    # Resume if needed
    last_tickid, last_k1, last_k2cv = last_saved(engine)
    start_id = args.start if (last_tickid is None or last_tickid < args.start) else (last_tickid + 1)

    ids, mids = fetch_ticks(engine, start_id, args.end)
    if len(ids) == 0:
        print("Nothing to do."); return

    # === Line-1: Old straight-edge Kalman (deadbanded CV) ===
    x0_k1 = None if last_k1 is None else np.array([last_k1, 0.0], dtype=float)
    k1 = kalman_cv_deadband(
        mids, q_scale=K1_Q_SCALE, r_scale=K1_R_SCALE, deadband_sig=K1_DEADBAND_SIG,
        x0=x0_k1, P0=None, dt=1.0
    )

    # === Line-2: scalar KF + RTS (keep behaviour) ===
    # robust q/r estimate from data, then scaled
    dy = np.diff(mids, prepend=mids[0])
    mad_dy = np.median(np.abs(dy - np.median(dy))) + 1e-9
    mad_y  = np.median(np.abs(mids - np.median(mids))) + 1e-9
    q1 = (mad_dy ** 2) * RTS_Q_SCALE
    r1 = (mad_y  ** 2) * RTS_R_SCALE
    k1_f, P_f, x_p, P_p = kalman_pass_scalar(mids, q=q1, r=r1)
    k1_rts = rts_smoother_scalar(k1_f, P_f, x_p, P_p)

    # === Line-3: Big-Move tracker (stored in k2_cv column) ===
    x0_bm = None if last_k2cv is None else np.array([last_k2cv, 0.0], dtype=float)
    k_big = bigmove_tracker(
        mids, q_scale=BM_Q_SCALE, r_scale=BM_R_SCALE,
        deadband_sig=BM_DEADBAND_SIG, min_abs=BM_MIN_ABS,
        x0=x0_bm, P0=None, dt=1.0
    )

    # Upsert
    upsert(engine, ids, k1, k1_rts, k_big)
    print(f"✔ upserted {len(ids)} rows into kalman_layers ({ids[0]}..{ids[-1]})")

if __name__ == "__main__":
    main()
