#!/usr/bin/env python3
import os, json, math, psycopg2, pytz
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time
from psycopg2.extras import execute_values
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestRegressor
import pickle

AEST = pytz.FixedOffset(600)  # +10:00
DB = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)

MODELS_DIR = "./models"
os.makedirs(MODELS_DIR, exist_ok=True)

def db():
    return psycopg2.connect(**DB)

def sydney_day_window(d):
    start = AEST.localize(datetime.combine(d, time(8,0,0)))
    end   = start + timedelta(days=1) - timedelta(hours=1)  # 07:00 next day
    return start, end

def load_ticks(conn, t0, t1):
    q = """
      SELECT id, timestamp, mid
      FROM ticks
      WHERE timestamp >= %s AND timestamp < %s
      ORDER BY id
    """
    df = pd.read_sql(q, conn, params=[t0, t1])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(AEST)
    df["mid"] = df["mid"].astype(float)
    return df

def zigzag(df, dx):
    # price-based zigzag: flips when we move >= dx from last extreme
    # returns two arrays of indices for turning points and a list of segments
    if df.empty: return [], []
    p = df["mid"].values
    idx = df.index.values
    piv_i, piv_p = [idx[0]], [p[0]]
    last_ext_i, last_ext_p = idx[0], p[0]
    direction = 0  # 0 unknown, +1 up, -1 down

    for i in range(1, len(p)):
        if direction >= 0 and p[i] >= last_ext_p:  # continue up
            last_ext_i, last_ext_p = idx[i], p[i]
        if direction <= 0 and p[i] <= last_ext_p:  # continue down
            last_ext_i, last_ext_p = idx[i], p[i]

        move = p[i] - piv_p[-1]
        if direction <= 0 and move >= dx:
            # trough -> new peak
            piv_i.append(idx[i]); piv_p.append(p[i]); direction = +1
        elif direction >= 0 and move <= -dx:
            # peak -> new trough
            piv_i.append(idx[i]); piv_p.append(p[i]); direction = -1

        if direction == 0:
            direction = +1 if p[i] > p[0] else -1

    # ensure last extreme included
    if piv_i[-1] != last_ext_i:
        piv_i.append(last_ext_i); piv_p.append(last_ext_p)

    return np.array(piv_i), np.array(piv_p)

def to_points_and_segments(df, piv_i, piv_p, level, run_day):
    # points
    points = []
    segs   = []
    for k in range(len(piv_i)):
        kind = "peak" if (k>0 and piv_p[k] > piv_p[k-1]) else ("trough" if k>0 else ("peak" if piv_p[0] >= piv_p[1] else "trough"))
        row = df.loc[piv_i[k]]
        points.append( (level, int(row["id"]), row["timestamp"].to_pydatetime(), float(piv_p[k]), kind, run_day) )

    # segments
    for k in range(1, len(piv_i)):
        s = df.loc[piv_i[k-1]]; e = df.loc[piv_i[k]]
        p0, p1 = float(piv_p[k-1]), float(piv_p[k])
        direction = 1 if p1>p0 else -1
        lo, hi = (p0, p1) if p0<=p1 else (p1, p0)
        segs.append( dict(
            start_tick_id=int(s["id"]), end_tick_id=int(e["id"]),
            start_ts=s["timestamp"].to_pydatetime(), end_ts=e["timestamp"].to_pydatetime(),
            start_price=p0, end_price=p1, high_price=hi, low_price=lo,
            direction=direction, duration_s=int((e["timestamp"]-s["timestamp"]).total_seconds()),
            num_ticks=int(e.name - s.name + 1), run_day=run_day
        ))
    return points, segs

def write_points(conn, rows):
    if not rows: return
    sql = """
      INSERT INTO zigzag_points(level, tick_id, ts, price, kind, run_day)
      VALUES %s
      ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def write_segs(conn, segs, table):
    if not segs: return
    cols = ("start_tick_id","end_tick_id","start_ts","end_ts","start_price","end_price",
            "high_price","low_price","direction","duration_s","num_ticks","run_day")
    tpl = [tuple(s[c] for c in cols) for s in segs]
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, sql, tpl)
    conn.commit()

def engineer_features(df):
    df = df.copy()
    df["ret_1s"] = df["mid"].diff()
    df["ret_5s"] = df["mid"].diff(5)
    df["ret_30s"]= df["mid"].diff(30)
    for w in (60, 300, 900):
        df[f"vol_{w}s"] = df["ret_1s"].rolling(w, min_periods=5).std()
        df[f"sma_{w}s"] = df["mid"].rolling(w, min_periods=5).mean()
    df["hour"] = df["timestamp"].dt.hour
    df = df.dropna()
    return df

def load_params(conn):
    q = "SELECT micro_dx, medium_dx, maxi_dx, medium_max_minutes, maxi_min_minutes FROM trend_params ORDER BY id DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(q)
        m = cur.fetchone()
    return dict(micro_dx=float(m[0]), medium_dx=float(m[1]), maxi_dx=float(m[2]),
                medium_max_minutes=int(m[3]), maxi_min_minutes=int(m[4]))

def largest_segment(segs):
    if not segs: return None
    s = max(segs, key=lambda x: abs(x["end_price"]-x["start_price"]))
    mag = abs(s["end_price"]-s["start_price"])
    return dict(dir=1 if s["end_price"]>s["start_price"] else -1, mag=mag)

def fit_or_load(level):
    p_dir = os.path.join(MODELS_DIR, f"{level}_dir.pkl")
    p_mag = os.path.join(MODELS_DIR, f"{level}_mag.pkl")
    mdl_dir = pickle.load(open(p_dir,"rb")) if os.path.exists(p_dir) else LogisticRegression(max_iter=1000)
    mdl_mag = pickle.load(open(p_mag,"rb")) if os.path.exists(p_mag) else RandomForestRegressor(n_estimators=200, random_state=42)
    return mdl_dir, mdl_mag, p_dir, p_mag

def main():
    start_day = datetime(2025,6,17, tzinfo=AEST).date()
    today = datetime.now(tz=AEST).date()

    with db() as conn:
        params = load_params(conn)

        cur_day = start_day
        while cur_day < today:
            day0, day1 = sydney_day_window(cur_day)

            # 1) make/run record
            with conn.cursor() as cur:
                cur.execute("INSERT INTO model_runs(day_start, day_end) VALUES (%s,%s) RETURNING id", (day0, day1))
                run_id = cur.fetchone()[0]
            conn.commit()

            # 2) load data + features
            df = load_ticks(conn, day0, day1 + timedelta(hours=1))
            if df.empty:
                cur_day += timedelta(days=1)
                continue
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(AEST)
            df = df.reset_index(drop=True)
            fdf = engineer_features(df)

            # 3) build labels for yesterday using zigzags
            piv_i, piv_p = zigzag(df, params["micro_dx"])
            points, segs = to_points_and_segments(df, piv_i, piv_p, "micro", cur_day)
            write_points(conn, points); write_segs(conn, segs, "micro_trends")

            piv_i, piv_p = zigzag(df, params["medium_dx"])
            points, segs = to_points_and_segments(df, piv_i, piv_p, "medium", cur_day)
            # prune by duration for medium
            segs = [s for s in segs if s["duration_s"] <= params["medium_max_minutes"]*60]
            write_points(conn, points); write_segs(conn, segs, "medium_trends")

            piv_i, piv_p = zigzag(df, params["maxi_dx"])
            points, segs = to_points_and_segments(df, piv_i, piv_p, "maxi", cur_day)
            write_points(conn, points); write_segs(conn, segs, "maxi_trends")

            # 4) prepare training data (aggregate features daily)
            X = fdf[["ret_1s","ret_5s","ret_30s","vol_60s","vol_300s","vol_900s","sma_60s","sma_300s","sma_900s","hour"]].values

            # Targets from THIS day for training the NEXT prediction
            labels = {}
            for lvl, table in (("micro","micro_trends"), ("medium","medium_trends"), ("maxi","maxi_trends")):
                with conn.cursor() as cur:
                    cur.execute(f"SELECT start_price,end_price FROM {table} WHERE run_day=%s", (cur_day,))
                    segs_db = [dict(start_price=r[0], end_price=r[1]) for r in cur.fetchall()]
                top = largest_segment(segs_db)
                if top:
                    labels[lvl] = top

            # Train & predict next day
            for lvl in ("micro","medium","maxi"):
                mdl_dir, mdl_mag, p_dir, p_mag = fit_or_load(lvl)
                if lvl in labels:
                    y_dir = np.array([labels[lvl]["dir"]]*(len(X)))
                    y_mag = np.array([labels[lvl]["mag"]]*(len(X)))
                    mdl_dir.fit(X, (y_dir>0).astype(int))
                    mdl_mag.fit(X, y_mag)
                    pickle.dump(mdl_dir, open(p_dir,"wb"))
                    pickle.dump(mdl_mag, open(p_mag,"wb"))

                # Use last 5 minutes as "current state" to issue a next-day prediction
                X_last = X[-300:] if len(X) >= 300 else X
                p_dir1 = mdl_dir.predict_proba(X_last).mean(axis=0)
                p_up = float(p_dir1[1])
                pred_dir = 1 if p_up>=0.5 else -1
                pred_mag = float(mdl_mag.predict(X_last).mean())

                with conn.cursor() as cur:
                    cur.execute("""
                      INSERT INTO predictions(run_id, level, predicted_dir, predicted_mag)
                      VALUES (%s,%s,%s,%s)
                    """, (run_id, lvl, pred_dir, pred_mag))
                conn.commit()

            # We’ll evaluate this run the next morning after 07:00 when the day closes.
            cur_day += timedelta(days=1)

if __name__ == "__main__":
    main()
