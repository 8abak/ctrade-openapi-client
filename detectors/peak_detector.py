# detectors/peak_detector.py
import os
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine, text
from scipy.signal import find_peaks

ENGINE = create_engine(os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"))

def _fetch_ticks(start_ts, end_ts):
    with ENGINE.connect() as conn:
        df = pd.read_sql(text("""
          SELECT id, timestamp, mid
          FROM ticks
          WHERE timestamp BETWEEN :a AND :b
          ORDER BY timestamp
        """), conn, params={"a": start_ts, "b": end_ts})
    return df

def _resample_mid(df: pd.DataFrame, rule='1S') -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    s = (df.set_index('timestamp')['mid']
           .resample(rule).last().ffill())
    return s

def _find(series: pd.Series, scale: int):
    if series.empty:
        return []

    if scale == 1:  # small detection
        kwargs_hi = dict(prominence=0.8, distance=45, width=3)
        kwargs_lo = dict(prominence=0.8, distance=45, width=3)
    else:           # big detection
        kwargs_hi = dict(prominence=1.8, distance=90, width=6)
        kwargs_lo = dict(prominence=1.8, distance=90, width=6)

    y = series.values
    hi_idx, hi_props = find_peaks(y, **kwargs_hi)
    lo_idx, lo_props = find_peaks(-y, **kwargs_lo)

    peaks = []
    for i in range(len(hi_idx)):
        j = hi_idx[i]
        peaks.append((series.index[j], +1, float(series.iloc[j]),
                      float(hi_props["prominences"][i]),
                      float(hi_props["widths"][i])))
    for i in range(len(lo_idx)):
        j = lo_idx[i]
        peaks.append((series.index[j], -1, float(series.iloc[j]),
                      float(lo_props["prominences"][i]),
                      float(lo_props["widths"][i])))
    peaks.sort(key=lambda r: r[0])
    return peaks

def _ts_to_tickid(conn, ts_list):
    if not ts_list:
        return {}
    a = min(ts_list) - pd.Timedelta(minutes=5)
    b = max(ts_list) + pd.Timedelta(minutes=5)
    rows = conn.execute(text("""
      SELECT id, timestamp FROM ticks
      WHERE timestamp BETWEEN :a AND :b
      ORDER BY timestamp
    """), {"a": a, "b": b}).fetchall()
    if not rows:
        return {}
    tss = np.array([r[1] for r in rows], dtype="datetime64[ns]")
    ids = np.array([r[0] for r in rows])
    out = {}
    for ts in ts_list:
        i = int(np.argmin(np.abs(tss - np.datetime64(ts.to_pydatetime()))))
        out[ts] = int(ids[i])
    return out

def detect_and_store(start_ts, end_ts):
    df = _fetch_ticks(start_ts, end_ts)
    s = _resample_mid(df, '1S')
    if s.empty:
        return 0, 0
    small = _find(s, 1)
    big   = _find(s, 2)

    with ENGINE.begin() as conn:
        tsmap = _ts_to_tickid(conn, [p[0] for p in (small + big)])
        # insert peaks (idempotent due to peaks_uniq)
        if small:
            conn.execute(text("""
              INSERT INTO peaks (ts, tickid, price, kind, scale, prominence, width)
              VALUES (:ts,:tid,:pr,:kd,1,:prom,:w)
              ON CONFLICT DO NOTHING
            """), [
                {"ts": ts, "tid": tsmap.get(ts), "pr": pr, "kd": kd, "prom": prom, "w": w}
                for (ts, kd, pr, prom, w) in small if ts in tsmap
            ])
        if big:
            conn.execute(text("""
              INSERT INTO peaks (ts, tickid, price, kind, scale, prominence, width)
              VALUES (:ts,:tid,:pr,:kd,2,:prom,:w)
              ON CONFLICT DO NOTHING
            """), [
                {"ts": ts, "tid": tsmap.get(ts), "pr": pr, "kd": kd, "prom": prom, "w": w}
                for (ts, kd, pr, prom, w) in big if ts in tsmap
            ])
    return len(small), len(big)
