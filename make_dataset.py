# make_dataset.py — build ml_features_tick, ml_labels_small, ml_labels_big
# Reads your latest DB (ticks + micro_trends/medium_trends/maxi_trends or zigzag_points)
# and writes feature/label tables idempotently (UPSERT).

import os
import math
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# ---------- CONFIG ----------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
)
engine = create_engine(DATABASE_URL)

# Rolling/feature windows
MA_FAST = 20
MA_SLOW = 100
MOM_WINS = [5, 20]
ATR_S = 14
ATR_M = 100
VWAP_WIN = 200      # pseudo-VWAP (equal vol per tick)
SMALL_HORIZON_TICKS = 600  # lookahead ceiling, used only if needed
SESSION_BREAKS = [(0,8,"SYD"), (8,13,"TOK"), (13,21,"LON"), (21,24,"NY")]

# ---------- UTILS ----------
def q(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execmany(sql, rows):
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def table_columns(table):
    sql = """
    SELECT column_name FROM information_schema.columns
    WHERE table_name = :t
    ORDER BY ordinal_position
    """
    return set(q(sql, {"t": table})["column_name"].tolist())

def ensure_tables():
    # Create feature/label/prediction tables if missing (safe to re-run)
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
      s_next_hold   SMALLINT NOT NULL,    -- 1=counter-micro HOLDS & flips medium; 0=BREAKS beyond last medium extreme
      horizon_ticks INTEGER NOT NULL,
      day_key       DATE
    );

    CREATE TABLE IF NOT EXISTS ml_labels_big(
      ref_id        BIGINT PRIMARY KEY,   -- medium pivot id OR tickid if you forward-fill later
      timestamp     TIMESTAMPTZ NOT NULL,
      b_regime      SMALLINT NOT NULL,    -- 1=up, -1=down, 0=nt
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
    for i,(a,b,_name) in enumerate(SESSION_BREAKS):
        if a <= h < b:
            return i
    return len(SESSION_BREAKS) - 1

def add_rolls(df: pd.DataFrame) -> pd.DataFrame:
    # Assumes df has mid, bid, ask, timestamp
    df = df.sort_values("tickid").copy()
    df["spread"] = (df["ask"] - df["bid"]).astype(float)
    for w in MOM_WINS:
        df[f"mom_{w}"] = df["mid"].diff(w)
    df["ma_fast"] = df["mid"].rolling(MA_FAST, min_periods=1).mean()
    df["ma_slow"] = df["mid"].rolling(MA_SLOW, min_periods=1).mean()
    df["atr_s"] = df["mid"].diff().abs().rolling(ATR_S, min_periods=1).mean()
    df["atr_m"] = df["mid"].diff().abs().rolling(ATR_M, min_periods=1).mean()
    # pseudo-VWAP (equal vol): rolling mean is fine as proxy
    vwap = df["mid"].rolling(VWAP_WIN, min_periods=1).mean()
    df["vwap_dist"] = (df["mid"] - vwap)
    df["session_id"] = df["timestamp"].apply(lambda x: session_bucket(pd.Timestamp(x))).astype(int)
    return df

def pick_cols(d: dict, keys):
    return {k: d[k] for k in keys if k in d}

# ---------- LOAD DAY SCOPE ----------
def load_ticks(day: str):
    sql = """
    SELECT id AS tickid, timestamp, bid, ask, mid
    FROM ticks
    WHERE DATE(timestamp)=:d
    ORDER BY id
    """
    return q(sql, {"d": day})

def load_trends(table: str, day: str) -> pd.DataFrame:
    cols = table_columns(table)
    # Try to normalize common schemas
    sql = None
    if {"id","start_tickid","end_tickid","direction","start_ts","end_ts","start_price","end_price"} <= cols:
        sql = f"""
        SELECT id, start_tickid, end_tickid, direction, start_ts, end_ts, start_price, end_price
        FROM {table}
        WHERE DATE(start_ts)=:d OR DATE(end_ts)=:d
        ORDER BY id
        """
    elif {"id","start_id","end_id","direction"} <= cols:
        sql = f"""
        SELECT id, start_id AS start_tickid, end_id AS end_tickid, direction,
               NULL::timestamptz AS start_ts, NULL::timestamptz AS end_ts,
               NULL::double precision AS start_price, NULL::double precision AS end_price
        FROM {table}
        WHERE 1=1  -- no date fields, we'll trust tick ranges overlap the day
        ORDER BY id
        """
    else:
        # Fall back to zigzag_points by level
        if table == "medium_trends":
            return load_medium_from_points(day)
        if table == "micro_trends":
            return load_micro_from_points(day)
        raise RuntimeError(f"Unsupported schema for {table}: {cols}")
    return q(sql, {"d": day})

def load_medium_from_points(day: str) -> pd.DataFrame:
    # Expect a 'zigzag_points' table with columns: id, level, tickid, direction, price, timestamp
    cols = table_columns("zigzag_points")
    req = {"id","level","tickid","direction","price","timestamp"}
    if not req <= cols:
        raise RuntimeError("Cannot derive medium_trends: zigzag_points missing required columns")
    pts = q("""
        SELECT id, level, tickid, direction, price, timestamp
        FROM zigzag_points
        WHERE DATE(timestamp)=:d AND level='medium'
        ORDER BY tickid
    """, {"d": day})
    # Convert alternating points to segments
    if pts.empty: 
        return pts
    pts["next_tickid"] = pts["tickid"].shift(-1)
    pts["start_tickid"] = pts["tickid"]
    pts["end_tickid"] = pts["next_tickid"]
    pts["start_ts"] = pts["timestamp"]
    pts["end_ts"] = pts["timestamp"].shift(-1)
    pts["start_price"] = pts["price"]
    pts["end_price"] = pts["price"].shift(-1)
    segs = pts.dropna(subset=["end_tickid"]).copy()
    segs = segs.rename(columns={"id":"id"})
    return segs[["id","start_tickid","end_tickid","direction","start_ts","end_ts","start_price","end_price"]]

def load_micro_from_points(day: str) -> pd.DataFrame:
    cols = table_columns("zigzag_points")
    req = {"id","level","tickid","direction","price","timestamp"}
    if not req <= cols:
        raise RuntimeError("Cannot derive micro_trends: zigzag_points missing required columns")
    return q("""
        SELECT id, tickid, direction, price, timestamp
        FROM zigzag_points
        WHERE DATE(timestamp)=:d AND level='micro'
        ORDER BY tickid
    """, {"d": day})

# ---------- LABELING LOGIC ----------
# 1) Small (your 1.1 rule)
# If we are inside a medium DOWN leg, and a MICRO UP appears after the last medium DOWN pivot:
#   label at that micro-up tick = 1   if price never breaks below the last medium DOWN low
#                                    BEFORE a medium UP pivot finalizes (i.e., next medium reversal occurs first)
#   else 0 (breaks below that low first -> continuation)
# Reverse the logic symmetrically for a medium UP leg.
def label_small(day_ticks: pd.DataFrame, micro_pts: pd.DataFrame, medium_segs: pd.DataFrame) -> pd.DataFrame:
    if day_ticks.empty or micro_pts.empty or medium_segs.empty:
        return pd.DataFrame(columns=["tickid","timestamp","s_next_hold","horizon_ticks","day_key"])

    ticks = day_ticks.set_index("tickid")
    medium_segs = medium_segs.sort_values("start_tickid").reset_index(drop=True)

    out = []
    for _, mu in micro_pts.iterrows():
        t_id = int(mu["tickid"])
        ts = ticks.loc[t_id, "timestamp"]

        # find medium segment that contains this micro point
        seg = medium_segs[(medium_segs["start_tickid"] <= t_id) & (t_id <= medium_segs["end_tickid"])]
        if seg.empty:
            continue
        seg = seg.iloc[0]
        seg_dir = seg["direction"]  # expect 'up' or 'dn'
        seg_start = int(seg["start_tickid"])
        seg_end = int(seg["end_tickid"])

        # Last medium extreme price within this leg (use start_price/end_price if available; otherwise compute from ticks)
        if "start_price" in seg and not pd.isna(seg["start_price"]):
            start_price = float(seg["start_price"])
            end_price = float(seg["end_price"]) if not pd.isna(seg["end_price"]) else start_price
        else:
            leg_prices = ticks.loc[seg_start:seg_end, "mid"]
            start_price = float(ticks.loc[seg_start, "mid"])
            end_price = float(ticks.loc[seg_end, "mid"]) if seg_end in ticks.index else float(leg_prices.iloc[-1])

        # Define the "do-not-break" threshold:
        if seg_dir == "dn":
            # the last medium down extreme is the current leg's LOW so far
            leg_prices = ticks.loc[seg_start:t_id, "mid"]
            medium_extreme = float(leg_prices.min())
            # from micro up tick forward: did we first get a medium UP reversal or make a NEW low below medium_extreme?
            forward_ticks = ticks.loc[t_id:seg_end]
            break_first = (forward_ticks["mid"].min() < medium_extreme - 1e-12)
            # detect if next medium segment flips up before that happens
            next_seg_idx = medium_segs.index[medium_segs["start_tickid"] == seg_end]
            flipped = False
            if len(next_seg_idx) > 0:
                nxt_idx = int(next_seg_idx[0])
                if nxt_idx < len(medium_segs):
                    flipped = (medium_segs.iloc[nxt_idx]["direction"] == "up")
            s_next_hold = 1 if (flipped and not break_first) else 0
        else:  # seg_dir == 'up'
            leg_prices = ticks.loc[seg_start:t_id, "mid"]
            medium_extreme = float(leg_prices.max())
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

# 2) Big: regime at medium pivots (HH/HL vs LH/LL). We mark only pivot rows; you can ffill later if needed.
def label_big(medium_segs: pd.DataFrame, day: str) -> pd.DataFrame:
    if medium_segs.empty:
        return pd.DataFrame(columns=["ref_id","timestamp","b_regime","is_pivot_row","day_key"])
    segs = medium_segs.sort_values("start_tickid").reset_index(drop=True).copy()

    # Build pivot list as (pivot_id, tickid, price, direction_at_leg_start)
    pivots = []
    for i, r in segs.iterrows():
        piv_id = int(r["id"])
        tickid = int(r["start_tickid"])
        ts = r.get("start_ts", None)
        price = r.get("start_price", None)
        pivots.append({"ref_id": piv_id, "tickid": tickid, "timestamp": ts, "price": price})

    dfp = pd.DataFrame(pivots).dropna(subset=["tickid"]).sort_values("tickid").reset_index(drop=True)
    if dfp.empty:
        return pd.DataFrame(columns=["ref_id","timestamp","b_regime","is_pivot_row","day_key"])

    # Determine HH/HL vs LH/LL with simple swing logic on the pivot prices
    # If price column missing, we’ll just mark NT.
    if dfp["price"].isna().any():
        dfp["b_regime"] = 0
    else:
        b = []
        for i in range(len(dfp)):
            if i < 2:
                b.append(0)
                continue
            p0, p1, p2 = dfp.loc[i-2, "price"], dfp.loc[i-1, "price"], dfp.loc[i, "price"]
            # Compare current and previous swings
            if p2 > p1 and p1 > p0:
                b.append(1)   # up
            elif p2 < p1 and p1 < p0:
                b.append(-1)  # down
            else:
                b.append(0)
        dfp["b_regime"] = b

    dfp["is_pivot_row"] = True
    dfp["day_key"] = pd.to_datetime(day).date()
    dfp["timestamp"] = pd.to_datetime(dfp["timestamp"])
    return dfp[["ref_id","timestamp","b_regime","is_pivot_row","day_key"]]

# ---------- MAIN ----------
def build_day(day: str):
    print(f"🛠  Building dataset for {day}")
    ensure_tables()

    ticks = load_ticks(day)
    if ticks.empty:
        print("No ticks for this day.")
        return

    # Normalize and add features
    feats = add_rolls(ticks.copy())
    feats["micro_state"] = 0  # placeholders (you can map your own encoded states later)
    feats["maxi_state"]  = 0
    feats["day_key"] = pd.to_datetime(feats["timestamp"]).dt.date

    # Insert/Upsert features
    feat_rows = feats.to_dict(orient="records")
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
    """, feat_rows)
    print(f"✔  ml_features_tick upserted: {len(feat_rows)}")

    # Trends
    # Prefer dedicated tables if present; else derive from zigzag_points
    use_points = False
    if "medium_trends" in [t for t in q("SELECT tablename FROM pg_tables WHERE schemaname='public'")["tablename"].tolist()]:
        medium = load_trends("medium_trends", day)
    else:
        use_points = True
        medium = load_medium_from_points(day)

    if "micro_trends" in [t for t in q("SELECT tablename FROM pg_tables WHERE schemaname='public'")["tablename"].tolist()]:
        micro_pts = load_trends("micro_trends", day)
        # normalize to (id, tickid, timestamp, direction, price)
        if "tickid" not in micro_pts.columns and "start_tickid" in micro_pts.columns:
            micro_pts = micro_pts.rename(columns={"start_tickid":"tickid"})
        if "timestamp" not in micro_pts.columns:
            # approximate from ticks if missing
            micro_pts = micro_pts.merge(ticks[["tickid","timestamp","mid"]], on="tickid", how="left")
            micro_pts = micro_pts.rename(columns={"mid":"price"})
    else:
        micro_pts = load_micro_from_points(day)

    # Labels: Small
    small = label_small(ticks, micro_pts, medium)
    sm_rows = small.to_dict(orient="records")
    execmany("""
        INSERT INTO ml_labels_small (tickid,timestamp,s_next_hold,horizon_ticks,day_key)
        VALUES (:tickid,:timestamp,:s_next_hold,:horizon_ticks,:day_key)
        ON CONFLICT (tickid) DO UPDATE SET
          timestamp=EXCLUDED.timestamp, s_next_hold=EXCLUDED.s_next_hold,
          horizon_ticks=EXCLUDED.horizon_ticks, day_key=EXCLUDED.day_key
    """, sm_rows)
    print(f"✔  ml_labels_small upserted: {len(sm_rows)}")

    # Labels: Big (medium pivot regime)
    big = label_big(medium, day)
    bg_rows = big.to_dict(orient="records")
    execmany("""
        INSERT INTO ml_labels_big (ref_id,timestamp,b_regime,is_pivot_row,day_key)
        VALUES (:ref_id,:timestamp,:b_regime,:is_pivot_row,:day_key)
        ON CONFLICT (ref_id) DO UPDATE SET
          timestamp=EXCLUDED.timestamp, b_regime=EXCLUDED.b_regime,
          is_pivot_row=EXCLUDED.is_pivot_row, day_key=EXCLUDED.day_key
    """, bg_rows)
    print(f"✔  ml_labels_big upserted: {len(bg_rows)}")

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD (build one day)")
    p.add_argument("--start", help="YYYY-MM-DD")
    p.add_argument("--end", help="YYYY-MM-DD")
    args = p.parse_args()

    if args.date:
        build_day(args.date)
        return
    if args.start and args.end:
        d0 = datetime.fromisoformat(args.start).date()
        d1 = datetime.fromisoformat(args.end).date()
        cur = d0
        while cur <= d1:
            build_day(cur.isoformat())
            cur += timedelta(days=1)
        return
    # default: today
    build_day(datetime.utcnow().date().isoformat())

if __name__ == "__main__":
    main()
