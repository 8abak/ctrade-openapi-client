# make_dataset.py — build ml_features_tick, ml_labels_small, ml_labels_big
# Reads your DB (ticks + micro/medium/maxi via tables or zigzag_points) and
# writes feature/label tables idempotently (UPSERT).
#
# Usage:
#   python make_dataset.py --date 2025-06-17
#   python make_dataset.py --start 2025-06-01 --end 2025-06-30

import os
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Rolling/feature windows
MA_FAST = 20
MA_SLOW = 100
MOM_WINS = [5, 20]
ATR_S = 14
ATR_M = 100
VWAP_WIN = 200           # pseudo-VWAP (equal vol per tick)
SMALL_HORIZON_TICKS = 600
SESSION_BREAKS = [(0,8,"SYD"), (8,13,"TOK"), (13,21,"LON"), (21,24,"NY")]

# ---------- HELPERS ----------
def q(sql, params=None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execmany(sql, rows):
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def table_columns(table) -> set:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = :t
    ORDER BY ordinal_position
    """
    try:
        return set(q(sql, {"t": table})["column_name"].tolist())
    except Exception:
        return set()

def ensure_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS ml_features_tick(
      tickid        BIGINT PRIMARY KEY,
      timestamp     TIMESTAMPTZ NOT NULL,
      mid           DOUBLE PRECISION NOT NULL,
      bid           DOUBLE PRECISION NOT NULL,
      ask           DOUBLE PRECISION NOT NULL,
      spread        DOUBLE PRECISION,
      vwap_dist     DOUBLE PRECISION,
      mom_5         DOUBLE PRECISION,
      mom_20        DOUBLE PRECISION,
      ma_fast       DOUBLE PRECISION,
      ma_slow       DOUBLE PRECISION,
      atr_s         DOUBLE PRECISION,
      atr_m         DOUBLE PRECISION,
      session_id    SMALLINT,
      micro_state   SMALLINT,
      maxi_state    SMALLINT,
      day_key       DATE
    );

    CREATE TABLE IF NOT EXISTS ml_labels_small(
      tickid        BIGINT PRIMARY KEY,
      timestamp     TIMESTAMPTZ NOT NULL,
      s_next_hold   SMALLINT NOT NULL,     -- 1=counter-micro HOLDS & flips medium; 0=BREAKS beyond last medium extreme
      horizon_ticks INTEGER NOT NULL,
      day_key       DATE
    );

    CREATE TABLE IF NOT EXISTS ml_labels_big(
      ref_id        BIGINT PRIMARY KEY,    -- medium pivot id OR tickid if forward-filled later
      timestamp     TIMESTAMPTZ NOT NULL,
      b_regime      SMALLINT NOT NULL,     -- 1=up, -1=down, 0=nt
      is_pivot_row  BOOLEAN DEFAULT FALSE,
      day_key       DATE
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";\n\n"):
            if stmt.strip():
                conn.execute(text(stmt))

def session_bucket(ts: pd.Timestamp) -> int:
    h = ts.hour + ts.minute/60.0
    for i,(a,b,_) in enumerate(SESSION_BREAKS):
        if a <= h < b:
            return i
    return len(SESSION_BREAKS) - 1

def add_rolls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("tickid").copy()
    df["spread"] = (df["ask"] - df["bid"]).astype(float)
    for w in MOM_WINS:
        df[f"mom_{w}"] = df["mid"].diff(w)
    df["ma_fast"] = df["mid"].rolling(MA_FAST, min_periods=1).mean()
    df["ma_slow"] = df["mid"].rolling(MA_SLOW, min_periods=1).mean()
    df["atr_s"] = df["mid"].diff().abs().rolling(ATR_S, min_periods=1).mean()
    df["atr_m"] = df["mid"].diff().abs().rolling(ATR_M, min_periods=1).mean()
    vwap = df["mid"].rolling(VWAP_WIN, min_periods=1).mean()
    df["vwap_dist"] = df["mid"] - vwap
    df["session_id"] = df["timestamp"].apply(lambda x: session_bucket(pd.Timestamp(x))).astype(int)
    return df

# ---------- LOADERS ----------
def load_ticks(day: str) -> pd.DataFrame:
    sql = """
    SELECT id AS tickid, timestamp, bid, ask, mid
    FROM ticks
    WHERE DATE(timestamp)=:d
    ORDER BY id
    """
    df = q(sql, {"d": day})
    print(f"ticks: {len(df)} for {day}")
    return df

def load_trends(table: str, day: str, day_min_id: int, day_max_id: int) -> pd.DataFrame:
    cols = table_columns(table)

    # Case A: segment-style table with start/end tickids
    if {"id","start_tickid","end_tickid","direction"} <= cols:
        sql = f"""
        SELECT id, start_tickid, end_tickid, direction,
               {"start_ts" if "start_ts" in cols else "NULL::timestamptz AS start_ts"},
               {"end_ts"   if "end_ts"   in cols else "NULL::timestamptz AS end_ts"},
               {"start_price" if "start_price" in cols else "NULL::double precision AS start_price"},
               {"end_price"   if "end_price"   in cols else "NULL::double precision AS end_price"}
        FROM {table}
        WHERE end_tickid >= :min_id AND start_tickid <= :max_id
        ORDER BY start_tickid
        """
        df = q(sql, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table}: {len(df)} segments by tickid overlap")
        return df

    # Case B: legacy naming
    if {"id","start_id","end_id","direction"} <= cols:
        sql = f"""
        SELECT id, start_id AS start_tickid, end_id AS end_tickid, direction,
               NULL::timestamptz AS start_ts, NULL::timestamptz AS end_ts,
               NULL::double precision AS start_price, NULL::double precision AS end_price
        FROM {table}
        WHERE end_id >= :min_id AND start_id <= :max_id
        ORDER BY start_id
        """
        df = q(sql, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table} (legacy): {len(df)} segments by tickid overlap")
        return df

    # Otherwise fall back to zigzag_points
    if table == "medium_trends":
        return load_medium_from_points(day, day_min_id, day_max_id)
    if table == "micro_trends":
        return load_micro_from_points(day, day_min_id, day_max_id)

    print(f"{table}: unsupported schema {cols}, returning empty")
    return pd.DataFrame()

def load_medium_from_points(day: str, day_min_id: int, day_max_id: int) -> pd.DataFrame:
    cols = table_columns("zigzag_points")
    req = {"id","level","tickid","direction","price","timestamp"}
    if not req <= cols:
        print("zigzag_points missing required columns for medium; empty")
        return pd.DataFrame()
    pts = q("""
        SELECT id, level, tickid, direction, price, timestamp
        FROM zigzag_points
        WHERE level='medium' AND tickid BETWEEN :min_id AND :max_id
        ORDER BY tickid
    """, {"min_id": day_min_id, "max_id": day_max_id})
    if pts.empty:
        print("zigzag_points (medium): 0 points by tickid overlap")
        return pts
    pts["next_tickid"] = pts["tickid"].shift(-1)
    pts["start_tickid"] = pts["tickid"]
    pts["end_tickid"] = pts["next_tickid"]
    pts["start_ts"] = pts["timestamp"]
    pts["end_ts"] = pts["timestamp"].shift(-1)
    pts["start_price"] = pts["price"]
    pts["end_price"] = pts["price"].shift(-1)
    segs = pts.dropna(subset=["end_tickid"]).copy()
    segs = segs[["id","start_tickid","end_tickid","direction","start_ts","end_ts","start_price","end_price"]]
    print(f"zigzag_points → medium segments: {len(segs)}")
    return segs

def load_micro_from_points(day: str, day_min_id: int, day_max_id: int) -> pd.DataFrame:
    cols = table_columns("zigzag_points")
    req = {"id","level","tickid","direction","price","timestamp"}
    if not req <= cols:
        print("zigzag_points missing required columns for micro; empty")
        return pd.DataFrame()
    df = q("""
        SELECT id, tickid, direction, price, timestamp
        FROM zigzag_points
        WHERE level='micro' AND tickid BETWEEN :min_id AND :max_id
        ORDER BY tickid
    """, {"min_id": day_min_id, "max_id": day_max_id})
    print(f"zigzag_points → micro points: {len(df)}")
    return df

# ---------- LABELS ----------
# Small trend (your 1.1 rule):
# Inside a medium DOWN leg, after a MICRO UP appears:
#   label = 1 if the next medium reversal to UP occurs BEFORE price breaks below the leg's lowest low so far.
#   else 0.
# Mirror for medium UP leg.
def label_small(day_ticks: pd.DataFrame, micro_pts: pd.DataFrame, medium_segs: pd.DataFrame) -> pd.DataFrame:
    if day_ticks.empty or micro_pts.empty or medium_segs.empty:
        return pd.DataFrame(columns=["tickid","timestamp","s_next_hold","horizon_ticks","day_key"])

    ticks = day_ticks.set_index("tickid")
    medium_segs = medium_segs.sort_values("start_tickid").reset_index(drop=True)

    out = []
    for _, mu in micro_pts.iterrows():
        t_id = int(mu["tickid"])
        if t_id not in ticks.index:
            continue
        ts = ticks.loc[t_id, "timestamp"]

        # find medium segment that contains this micro point
        seg = medium_segs[(medium_segs["start_tickid"] <= t_id) & (t_id <= medium_segs["end_tickid"])]
        if seg.empty:
            continue
        seg = seg.iloc[0]
        seg_dir = seg["direction"]  # 'up' or 'dn'
        seg_start = int(seg["start_tickid"])
        seg_end = int(seg["end_tickid"])

        leg_prices_to_here = ticks.loc[seg_start:t_id, "mid"]

        if seg_dir == "dn":
            medium_extreme = float(leg_prices_to_here.min())
            forward_ticks = ticks.loc[t_id:seg_end]
            break_first = (forward_ticks["mid"].min() < medium_extreme - 1e-12)
            # did next segment flip up?
            next_seg_idx = medium_segs.index[medium_segs["start_tickid"] == seg_end]
            flipped = False
            if len(next_seg_idx) > 0:
                nxt_idx = int(next_seg_idx[0])
                if nxt_idx < len(medium_segs):
                    flipped = (medium_segs.iloc[nxt_idx]["direction"] == "up")
            s_next_hold = 1 if (flipped and not break_first) else 0
        else:  # seg_dir == 'up'
            medium_extreme = float(leg_prices_to_here.max())
            forward_ticks = ticks.loc[t_id:seg_end]
            break_first = (forward_ticks["mid"].max() > medium_extreme + 1e-12)
            next_seg_idx = medium_segs.index[medium_segs["start_tickid"] == seg_end]
            flipped = False
            if len(next_seg_idx) > 0:
                nxt_idx = int(next_seg_idx[0])
                if nxt_idx < len(medium_segs):
                    flipped = (medium_segs.iloc[nxt_idx]["direction"] == "dn")
            s_next_hold = 1 if (flipped and not break_first) else 0

        horizon = int(min(len(ticks.loc[t_id:]), SMALL_HORIZON_TICKS))
        out.append({
            "tickid": t_id,
            "timestamp": ts,
            "s_next_hold": int(s_next_hold),
            "horizon_ticks": horizon,
            "day_key": pd.to_datetime(ts).date()
        })

    return pd.DataFrame(out)

# Big trend: regime at medium pivots (HH/HL vs LH/LL).
# Mark only pivot rows; forward-fill to ticks later if desired.
def label_big(medium_segs: pd.DataFrame, day: str) -> pd.DataFrame:
    if medium_segs.empty:
        return pd.DataFrame(columns=["ref_id","timestamp","b_regime","is_pivot_row","day_key"])
    segs = medium_segs.sort_values("start_tickid").reset_index(drop=True).copy()

    pivots = []
    for _, r in segs.iterrows():
        pivots.append({
            "ref_id": int(r["id"]),
            "tickid": int(r["start_tickid"]),
            "timestamp": r.get("start_ts", None),
            "price": r.get("start_price", None)
        })

    dfp = pd.DataFrame(pivots).dropna(subset=["tickid"]).sort_values("tickid").reset_index(drop=True)
    if dfp.empty:
        return pd.DataFrame(columns=["ref_id","timestamp","b_regime","is_pivot_row","day_key"])

    if dfp["price"].isna().any():
        dfp["b_regime"] = 0
    else:
        b = []
        for i in range(len(dfp)):
            if i < 2:
                b.append(0)
                continue
            p0, p1, p2 = dfp.loc[i-2, "price"], dfp.loc[i-1, "price"], dfp.loc[i, "price"]
            if p2 > p1 and p1 > p0:
                b.append(1)      # up
            elif p2 < p1 and p1 < p0:
                b.append(-1)     # down
            else:
                b.append(0)      # neutral/transition
        dfp["b_regime"] = b

    dfp["is_pivot_row"] = True
    dfp["day_key"] = pd.to_datetime(day).date()
    dfp["timestamp"] = pd.to_datetime(dfp["timestamp"])
    return dfp[["ref_id","timestamp","b_regime","is_pivot_row","day_key"]]

# ---------- MAIN BUILD ----------
def build_day(day: str):
    print(f"🛠  Building dataset for {day}")
    ensure_tables()

    ticks = load_ticks(day)
    if ticks.empty:
        print("No ticks for this day.")
        return

    feats = add_rolls(ticks.copy())
    feats["micro_state"] = 0  # placeholders for your encoded states (optional)
    feats["maxi_state"]  = 0
    feats["day_key"] = pd.to_datetime(feats["timestamp"]).dt.date

    # Upsert features
    execmany("""
        INSERT INTO ml_features_tick
        (tickid,timestamp,mid,bid,ask,spread,vwap_dist,mom_5,mom_20,ma_fast,ma_slow,atr_s,atr_m,session_id,micro_state,maxi_state,day_key)
        VALUES
        (:tickid,:timestamp,:mid,:bid,:ask,:spread,:vwap_dist,:mom_5,:mom_20,:ma_fast,:ma_slow,:atr_s,:atr_m,:session_id,:micro_state,:maxi_state,:day_key)
        ON CONFLICT (tickid) DO UPDATE SET
          timestamp=EXCLUDED.timestamp, mid=EXCLUDED.mid, bid=EXCLUDED.bid, ask=EXCLUDED.ask,
          spread=EXCLUDED.spread, vwap_dist=EXCLUDED.vwap_dist, mom_5=EXCLUDED.mom_5,
          mom_20=EXCLUDED.mom_20, ma_fast=EXCLUDED.ma_fast, ma_slow=EXCLUDED.ma_slow,
          atr_s=EXCLUDED.atr_s, atr_m=EXCLUDED.atr_m, session_id=EXCLUDED.session_id,
          micro_state=EXCLUDED.micro_state, maxi_state=EXCLUDED.maxi_state, day_key=EXCLUDED.day_key
    """, feats.to_dict(orient="records"))
    print(f"✔  ml_features_tick upserted: {len(feats)}")

    # Determine tick-id range for zig overlap
    day_min_id = int(feats["tickid"].min())
    day_max_id = int(feats["tickid"].max())

    tables = q("SELECT tablename FROM pg_tables WHERE schemaname='public'")["tablename"].tolist()

    # Medium segments
    if "medium_trends" in tables:
        medium = load_trends("medium_trends", day, day_min_id, day_max_id)
    else:
        medium = load_medium_from_points(day, day_min_id, day_max_id)

    # Micro points
    if "micro_trends" in tables:
        micro = load_trends("micro_trends", day, day_min_id, day_max_id)
        # normalize to required columns
        if "tickid" not in micro.columns and "start_tickid" in micro.columns:
            micro = micro.rename(columns={"start_tickid": "tickid"})
        if "timestamp" not in micro.columns:
            micro = micro.merge(ticks[["tickid","timestamp","mid"]], on="tickid", how="left").rename(columns={"mid":"price"})
    else:
        micro = load_micro_from_points(day, day_min_id, day_max_id)

    # Labels: Small
    small = label_small(ticks, micro, medium)
    execmany("""
        INSERT INTO ml_labels_small (tickid,timestamp,s_next_hold,horizon_ticks,day_key)
        VALUES (:tickid,:timestamp,:s_next_hold,:horizon_ticks,:day_key)
        ON CONFLICT (tickid) DO UPDATE SET
          timestamp=EXCLUDED.timestamp,
          s_next_hold=EXCLUDED.s_next_hold,
          horizon_ticks=EXCLUDED.horizon_ticks,
          day_key=EXCLUDED.day_key
    """, small.to_dict(orient="records"))
    print(f"✔  ml_labels_small upserted: {len(small)}")

    # Labels: Big
    big = label_big(medium, day)
    execmany("""
        INSERT INTO ml_labels_big (ref_id,timestamp,b_regime,is_pivot_row,day_key)
        VALUES (:ref_id,:timestamp,:b_regime,:is_pivot_row,:day_key)
        ON CONFLICT (ref_id) DO UPDATE SET
          timestamp=EXCLUDED.timestamp,
          b_regime=EXCLUDED.b_regime,
          is_pivot_row=EXCLUDED.is_pivot_row,
          day_key=EXCLUDED.day_key
    """, big.to_dict(orient="records"))
    print(f"✔  ml_labels_big upserted: {len(big)}")

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD")
    a = p.parse_args()

    if a.date:
        build_day(a.date)
        return

    if a.start and a.end:
        d0 = datetime.fromisoformat(a.start).date()
        d1 = datetime.fromisoformat(a.end).date()
        cur = d0
        while cur <= d1:
            build_day(cur.isoformat())
            cur += timedelta(days=1)
        return

    # Default: today (UTC)
    build_day(datetime.utcnow().date().isoformat())

if __name__ == "__main__":
    main()
