# jobs/build_kalman_layers.py
import os, argparse
from typing import Tuple, Optional, List
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# ---------------- DB ----------------
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

# ------------- Robust scales -------------
def robust_scales(y: np.ndarray) -> Tuple[float, float]:
    """Return (var_dy, var_y) using MAD, with tiny floor."""
    dy = np.diff(y, prepend=y[0])
    mad_dy = np.median(np.abs(dy - np.median(dy))) + 1e-9
    mad_y  = np.median(np.abs(y  - np.median(y)))  + 1e-9
    return mad_dy**2, mad_y**2

# ------------- Simple scalar Kalman (less noisy) -------------
# Model: x_t = x_{t-1} + w,   z_t = x_t + v   (A=1, H=1)
K1_Q_SCALE = float(os.getenv("K1_Q_SCALE", "0.02"))  # smaller = smoother
K1_R_SCALE = float(os.getenv("K1_R_SCALE", "1.0"))   # usually leave =1.0

def kalman_scalar_rw(y: np.ndarray, q_scale: float, r_scale: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    y = np.asarray(y, float)
    n = len(y)
    var_dy, _ = robust_scales(y)
    q = max(1e-12, var_dy * q_scale)
    r = max(1e-12, var_dy * r_scale)

    x_f = np.empty(n); P_f = np.empty(n)
    x_p = np.empty(n); P_p = np.empty(n)

    x = y[0]; P = r
    for i, z in enumerate(y):
        # predict
        x_pred = x
        P_pred = P + q
        # update
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

# ------------- Big-Move: straight segments via ZigZag pivots -------------
# Sensitive to HH/LL; bends only when a move >= max(ABS, SIGMA*mad_dy) confirms a pivot.
BM_ABS      = float(os.getenv("BM_ABS", "2.0"))   # absolute $ threshold
BM_SIGMA    = float(os.getenv("BM_SIGMA", "3.0")) # sigma multiple on MAD(dy)

def zigzag_bigmove(y: np.ndarray, abs_thr: float, sigma_mult: float) -> np.ndarray:
    y = np.asarray(y, float); n = len(y)
    if n == 0:
        return np.array([], float)

    var_dy, _ = robust_scales(y)
    sig_thr = sigma_mult * np.sqrt(var_dy)
    thr = max(abs_thr, sig_thr)

    # pivot detection (classic HH/LL with confirmation)
    piv_idx: List[int] = [0]
    piv_val: List[float] = [y[0]]
    direction = 0  # 0=unknown, +1=up, -1=down
    cand_hi = y[0]; i_hi = 0
    cand_lo = y[0]; i_lo = 0

    for i in range(1, n):
        p = y[i]

        # track extremes
        if p > cand_hi: cand_hi, i_hi = p, i
        if p < cand_lo: cand_lo, i_lo = p, i

        if direction >= 0:
            # wait for a confirmed DOWN move from latest high
            if (cand_hi - p) >= thr:
                piv_idx.append(i_hi); piv_val.append(cand_hi)
                direction = -1
                cand_lo, i_lo = p, i
                cand_hi, i_hi = p, i  # reset other side too
                continue

        if direction <= 0:
            # wait for a confirmed UP move from latest low
            if (p - cand_lo) >= thr:
                piv_idx.append(i_lo); piv_val.append(cand_lo)
                direction = +1
                cand_hi, i_hi = p, i
                cand_lo, i_lo = p, i
                continue

    # close with last extreme to avoid unfinished tail
    last_i = i_hi if direction == +1 else (i_lo if direction == -1 else n - 1)
    last_v = y[last_i]
    if piv_idx[-1] != last_i:
        piv_idx.append(last_i); piv_val.append(last_v)

    # piecewise-linear fill between pivots
    out = np.empty(n, float)
    for a, b in zip(range(len(piv_idx) - 1), range(1, len(piv_idx))):
        i0, i1 = piv_idx[a], piv_idx[b]
        y0, y1 = piv_val[a], piv_val[b]
        if i1 == i0:
            out[i0] = y0
            continue
        t = np.linspace(0.0, 1.0, i1 - i0 + 1)
        seg = y0 + (y1 - y0) * t
        out[i0:i1 + 1] = seg

    # fill any head gap (shouldn't happen, but safe)
    if piv_idx[0] > 0:
        out[:piv_idx[0]] = piv_val[0]

    return out

# ------------- main -------------
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

    # k1: simple scalar Kalman (less noisy)
    k1, P_f, x_p, P_p = kalman_scalar_rw(mids, q_scale=K1_Q_SCALE, r_scale=K1_R_SCALE)

    # k1_rts: RTS on that Kalman track
    k1_rts = rts_smoother_scalar(k1, P_f, x_p, P_p)

    # k2_cv: Big-Move (piecewise straight lines)
    k_big = zigzag_bigmove(mids, abs_thr=BM_ABS, sigma_mult=BM_SIGMA)

    upsert(engine, ids, k1, k1_rts, k_big)
    print(f"✔ upserted {len(ids)} rows into kalman_layers ({ids[0]}..{ids[-1]})")

if __name__ == "__main__":
    main()
