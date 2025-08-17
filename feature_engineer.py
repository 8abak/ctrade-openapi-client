from sqlalchemy import create_engine, text
from ml_config import DATABASE_URL, ZZ_REVERSAL_FRACTION
import pandas as pd
import numpy as np

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def _zig(zig_id):
    with engine.begin() as c:
        return dict(c.execute(text("SELECT * FROM zigzags WHERE id=:i"), {"i": zig_id}).first()._mapping)

def _ticks(st, en):
    q = text("""
        SELECT id AS tickid, timestamp AS ts, COALESCE(mid,(bid+ask)/2.0) AS mid
        FROM ticks
        WHERE id BETWEEN :s AND :e
        ORDER BY id
    """)
    with engine.begin() as c:
        return pd.read_sql(q, c, params={"s": st, "e": en})

def compute_zig_features(zig_id):
    z = _zig(zig_id)
    df = _ticks(z["start_tickid"], z["end_tickid"])
    mid = df["mid"].to_numpy()
    start, end = mid[0], mid[-1]
    sign = 1 if z["direction"] == "up" else -1

    duration = max(1, int((df["ts"].iloc[-1] - df["ts"].iloc[0]).total_seconds()))
    ret = np.diff(mid, prepend=mid[0])

    std_mid = float(np.std(mid))
    realized_vol = float(np.sqrt(np.sum(ret**2)) / len(ret))
    progress = sign * (mid - start)
    mae = float(progress.min())

    if sign == 1:
        runmax = np.maximum.accumulate(progress)
        mdd = float((progress - runmax).min())
    else:
        runmin = np.minimum.accumulate(progress)
        mdd = float((progress - runmin).min())

    sql = text("""
        INSERT INTO zig_features(zig_id,price_change,abs_change,slope_per_sec,
                                 mean_spread,std_mid,realized_vol,mae,mdd)
        VALUES(:i,:pc,:ac,:sl,NULL,:std,:rv,:mae,:mdd)
        ON CONFLICT (zig_id) DO UPDATE SET
          price_change=EXCLUDED.price_change,
          abs_change=EXCLUDED.abs_change,
          slope_per_sec=EXCLUDED.slope_per_sec,
          std_mid=EXCLUDED.std_mid,
          realized_vol=EXCLUDED.realized_vol,
          mae=EXCLUDED.mae,
          mdd=EXCLUDED.mdd
    """)
    with engine.begin() as c:
        c.execute(sql, {
            "i": zig_id,
            "pc": float(end - start),
            "ac": float(abs(end - start)),
            "sl": float(sign * (end - start) / duration),
            "std": std_mid,
            "rv": realized_vol,
            "mae": mae,
            "mdd": mdd
        })

def _earliest_no_return_idx(mid, direction, rev_abs):
    if direction == "up":
        fut_min = np.minimum.accumulate(mid[::-1])[::-1]
        draw = mid - fut_min
    else:
        fut_max = np.maximum.accumulate(mid[::-1])[::-1]
        draw = fut_max - mid
    ok = draw <= rev_abs + 1e-12
    if ok.any():
        i = int(np.argmax(ok))
        return i if ok[i] else None
    return None

def compute_tick_features_and_labels(zig_id):
    z = _zig(zig_id)
    df = _ticks(z["start_tickid"], z["end_tickid"])
    if df.empty:
        return

    mid = df["mid"].to_numpy()
    n = len(df)
    sign = 1 if z["direction"] == "up" else -1
    start = mid[0]

    progress = sign * (mid - start)
    progress_norm = (progress - progress.min()) / max(progress.max() - progress.min(), 1e-9)

    ret1 = np.diff(mid, prepend=mid[0])
    ret5 = mid - np.r_[np.full(5, mid[0]), mid[:-5]]
    vol20 = pd.Series(ret1).rolling(20, min_periods=1).std().to_numpy()

    if sign == 1:
        runmax = np.maximum.accumulate(progress)
        drawdown = progress - runmax
    else:
        runmin = np.minimum.accumulate(progress)
        drawdown = progress - runmin

    pos_ratio = np.linspace(0, 1, n)
    seconds_since = (df["ts"] - df["ts"].iloc[0]).dt.total_seconds().astype(int).to_numpy()

    rev_abs = float(z["threshold"]) * ZZ_REVERSAL_FRACTION
    i0 = _earliest_no_return_idx(mid, z["direction"], rev_abs)

    target = np.zeros(n, dtype=bool)
    if i0 is not None:
        target[i0:] = True

    with engine.begin() as c:
        if i0 is not None:
            c.execute(text("""
                INSERT INTO no_return_points(zig_id,tickid,ts)
                VALUES(:z,:t,:ts)
                ON CONFLICT (zig_id) DO UPDATE SET tickid=EXCLUDED.tickid, ts=EXCLUDED.ts
            """), {"z": zig_id, "t": int(df["tickid"].iloc[i0]), "ts": df["ts"].iloc[i0]})

        c.execute(text("DELETE FROM tick_features WHERE zig_id=:z"), {"z": zig_id})

        ins = text("""
            INSERT INTO tick_features(zig_id,tickid,ts,pos_ratio,progress_norm,
                                      ret1,ret5,vol20,drawdown,seconds_since,target_no_return)
            VALUES(:z,:tid,:ts,:pr,:pn,:r1,:r5,:v,:dd,:s,:t)
        """)
        for i in range(n):
            c.execute(ins, {
                "z": zig_id,
                "tid": int(df["tickid"].iloc[i]),
                "ts": df["ts"].iloc[i],
                "pr": float(pos_ratio[i]),
                "pn": float(progress_norm[i]),
                "r1": float(ret1[i]),
                "r5": float(ret5[i]),
                "v":  float(0.0 if np.isnan(vol20[i]) else vol20[i]),
                "dd": float(drawdown[i]),
                "s":  int(seconds_since[i]),
                "t":  bool(target[i])
            })
