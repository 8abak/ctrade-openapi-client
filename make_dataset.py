# make_dataset.py — builds ml_features_tick, ml_labels_small, ml_labels_big
# Schema-aware (public/web). Loads zig data by tick-id overlap. Safe if some tables are missing.
# Usage:
#   python make_dataset.py --date 2025-06-17

import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

MA_FAST, MA_SLOW = 20, 100
MOM_WINS = [5, 20]
ATR_S, ATR_M = 14, 100
VWAP_WIN = 200
SMALL_HORIZON_TICKS = 600
SESSION_BREAKS = [(0,8,"SYD"), (8,13,"TOK"), (13,21,"LON"), (21,24,"NY")]
PREF_SCHEMAS = ["public","web"]

# ---------- DB helpers ----------
def q(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execmany(sql, rows):
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def find_table(table: str):
    """Return (schema, qualified_name) for the first matching schema (public/web)."""
    df = q("""
      SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = :t
    """, {"t": table})
    for sch in PREF_SCHEMAS:
        hit = df[df["table_schema"] == sch]
        if not hit.empty:
            return sch, f'{sch}."{table}"'
    # not found
    return None, None

def table_columns(schema: str, name: str):
    if not schema or not name: return set()
    df = q("""
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = :s AND table_name = :n
      ORDER BY ordinal_position
    """, {"s": schema, "n": name})
    return set(df["column_name"].tolist())

def ensure_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS ml_features_tick(
      tickid BIGINT PRIMARY KEY, timestamp TIMESTAMPTZ NOT NULL,
      mid DOUBLE PRECISION NOT NULL, bid DOUBLE PRECISION NOT NULL, ask DOUBLE PRECISION NOT NULL,
      spread DOUBLE PRECISION, vwap_dist DOUBLE PRECISION,
      mom_5 DOUBLE PRECISION, mom_20 DOUBLE PRECISION,
      ma_fast DOUBLE PRECISION, ma_slow DOUBLE PRECISION,
      atr_s DOUBLE PRECISION, atr_m DOUBLE PRECISION,
      session_id SMALLINT, micro_state SMALLINT, maxi_state SMALLINT, day_key DATE
    );
    CREATE TABLE IF NOT EXISTS ml_labels_small(
      tickid BIGINT PRIMARY KEY, timestamp TIMESTAMPTZ NOT NULL,
      s_next_hold SMALLINT NOT NULL, horizon_ticks INTEGER NOT NULL, day_key DATE
    );
    CREATE TABLE IF NOT EXISTS ml_labels_big(
      ref_id BIGINT PRIMARY KEY, timestamp TIMESTAMPTZ NOT NULL,
      b_regime SMALLINT NOT NULL, is_pivot_row BOOLEAN DEFAULT FALSE, day_key DATE
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";\n"):
            if stmt.strip():
                conn.execute(text(stmt))

# ---------- feature utils ----------
def session_bucket(ts: pd.Timestamp) -> int:
    h = ts.hour + ts.minute/60.0
    for i,(a,b,_) in enumerate(SESSION_BREAKS):
        if a <= h < b: return i
    return len(SESSION_BREAKS) - 1

def add_rolls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("tickid").copy()
    df["spread"] = (df["ask"] - df["bid"]).astype(float)
    for w in MOM_WINS: df[f"mom_{w}"] = df["mid"].diff(w)
    df["ma_fast"] = df["mid"].rolling(MA_FAST, min_periods=1).mean()
    df["ma_slow"] = df["mid"].rolling(MA_SLOW, min_periods=1).mean()
    df["atr_s"] = df["mid"].diff().abs().rolling(ATR_S, min_periods=1).mean()
    df["atr_m"] = df["mid"].diff().abs().rolling(ATR_M, min_periods=1).mean()
    vwap = df["mid"].rolling(VWAP_WIN, min_periods=1).mean()
    df["vwap_dist"] = df["mid"] - vwap
    df["session_id"] = df["timestamp"].apply(lambda x: session_bucket(pd.Timestamp(x))).astype(int)
    return df

# ---------- loaders ----------
def load_ticks(day: str) -> pd.DataFrame:
    df = q("""
      SELECT id AS tickid, timestamp, bid, ask, mid
      FROM ticks
      WHERE DATE(timestamp)=:d
      ORDER BY id
    """, {"d": day})
    print(f"ticks: {len(df)} for {day}")
    return df

def load_segments(table_name: str, day_min_id: int, day_max_id: int) -> pd.DataFrame:
    sch, qname = find_table(table_name)
    if not qname:
        print(f"{table_name}: not found in public/web")
        return pd.DataFrame()
    cols = table_columns(sch, table_name)

    # Segment format
    if {"id","start_tickid","end_tickid","direction"} <= cols:
        df = q(f"""
          SELECT id, start_tickid, end_tickid, direction,
                 {"start_ts" if "start_ts" in cols else "NULL::timestamptz AS start_ts"},
                 {"end_ts"   if "end_ts"   in cols else "NULL::timestamptz AS end_ts"},
                 {"start_price" if "start_price" in cols else "NULL::double precision AS start_price"},
                 {"end_price"   if "end_price"   in cols else "NULL::double precision AS end_price"}
          FROM {qname}
          WHERE end_tickid >= :min_id AND start_tickid <= :max_id
          ORDER BY start_tickid
        """, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table_name}@{sch}: {len(df)} segments (tickid overlap)")
        return df

    # Legacy names
    if {"id","start_id","end_id","direction"} <= cols:
        df = q(f"""
          SELECT id, start_id AS start_tickid, end_id AS end_tickid, direction,
                 NULL::timestamptz AS start_ts, NULL::timestamptz AS end_ts,
                 NULL::double precision AS start_price, NULL::double precision AS end_price
          FROM {qname}
          WHERE end_id >= :min_id AND start_id <= :max_id
          ORDER BY start_id
        """, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table_name}@{sch} (legacy): {len(df)} segments (tickid overlap)")
        return df

    # Point-like (micro table sometimes)
    if {"id","tickid","direction"} <= cols:
        df = q(f"""
          SELECT id, tickid, direction,
                 {"timestamp" if "timestamp" in cols else "NULL::timestamptz AS timestamp"},
                 {"price" if "price" in cols else "NULL::double precision AS price"}
          FROM {qname}
          WHERE tickid BETWEEN :min_id AND :max_id
          ORDER BY tickid
        """, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table_name}@{sch}: {len(df)} points (tickid overlap)")
        return df

    print(f"{table_name}@{sch}: unsupported columns {cols} → empty")
    return pd.DataFrame()

def load_points_from_zig(level: str, day_min_id: int, day_max_id: int) -> pd.DataFrame:
    sch, qname = find_table("zigzag_points")
    if not qname:
        print("zigzag_points: not found")
        return pd.DataFrame()
    cols = table_columns(sch, "zigzag_points")
    need = {"id","level","tickid","direction"}
    if not need <= cols:
        print("zigzag_points: required columns missing")
        return pd.DataFrame()
    df = q(f"""
      SELECT id, level, tickid, direction,
             {"timestamp" if "timestamp" in cols else "NULL::timestamptz AS timestamp"},
             {"price" if "price" in cols else "NULL::double precision AS price"}
      FROM {qname}
      WHERE level=:lvl AND tickid BETWEEN :min_id AND :max_id
      ORDER BY tickid
    """, {"lvl": level, "min_id": day_min_id, "max_id": day_max_id})
    print(f"zigzag_points[{level}]@{sch}: {len(df)}")
    return df

def segments_from_medium_points(pts: pd.DataFrame) -> pd.DataFrame:
    if pts.empty: return pts
    pts = pts.sort_values("tickid").copy()
    pts["next_tickid"] = pts["tickid"].shift(-1)
    pts["start_tickid"] = pts["tickid"]
    pts["end_tickid"]   = pts["next_tickid"]
    pts["start_ts"]     = pts.get("timestamp")
    pts["end_ts"]       = pts.get("timestamp").shift(-1) if "timestamp" in pts.columns else None
    pts["start_price"]  = pts.get("price")
    pts["end_price"]    = pts.get("price").shift(-1) if "price" in pts.columns else None
    segs = pts.dropna(subset=["end_tickid"]).copy()
    return segs[["id","start_tickid","end_tickid","direction","start_ts","end_ts","start_price","end_price"]]

# ---------- labels ----------
def label_small(day_ticks: pd.DataFrame, micro_pts: pd.DataFrame, medium_segs: pd.DataFrame) -> pd.DataFrame:
    if day_ticks.empty or micro_pts.empty or medium_segs.empty:
        return pd.DataFrame(columns=["tickid","timestamp","s_next_hold","horizon_ticks","day_key"])

    ticks = day_ticks.set_index("tickid")
    medium_segs = medium_segs.sort_values("start_tickid").reset_index(drop=True)

    out = []
    for _, mu in micro_pts.iterrows():
        if "tickid" not in mu: continue
        t_id = int(mu["tickid"])
        if t_id not in ticks.index: continue
        ts = ticks.loc[t_id, "timestamp"]
        seg = medium_segs[(medium_segs["start_tickid"] <= t_id) & (t_id <= medium_segs["end_tickid"])]
        if seg.empty: continue
        seg = seg.iloc[0]
        seg_dir = seg["direction"]
        seg_start = int(seg["start_tickid"]); seg_end = int(seg["end_tickid"])

        leg_prices = ticks.loc[seg_start:t_id, "mid"]
        if seg_dir == "dn":
            extreme = float(leg_prices.min())
            forward = ticks.loc[t_id:seg_end]
            break_first = (forward["mid"].min() < extreme - 1e-12)
            next_idx = medium_segs.index[medium_segs["start_tickid"] == seg_end]
            flipped = False
            if len(next_idx) > 0:
                i = int(next_idx[0])
                if i < len(medium_segs):
                    flipped = (medium_segs.iloc[i]["direction"] == "up")
            s_next_hold = 1 if (flipped and not break_first) else 0
        else:
            extreme = float(leg_prices.max())
            forward = ticks.loc[t_id:seg_end]
            break_first = (forward["mid"].max() > extreme + 1e-12)
            next_idx = medium_segs.index[medium_segs["start_tickid"] == seg_end]
            flipped = False
            if len(next_idx) > 0:
                i = int(next_idx[0])
                if i < len(medium_segs):
                    flipped = (medium_segs.iloc[i]["direction"] == "dn")
            s_next_hold = 1 if (flipped and not break_first) else 0

        horizon = int(min(len(ticks.loc[t_id:]), SMALL_HORIZON_TICKS))
        out.append({"tickid": t_id, "timestamp": ts, "s_next_hold": int(s_next_hold),
                    "horizon_ticks": horizon, "day_key": pd.to_datetime(ts).date()})
    return pd.DataFrame(out)

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
    if "price" not in dfp or dfp["price"].isna().any():
        dfp["b_regime"] = 0
    else:
        out = []
        for i in range(len(dfp)):
            if i < 2: out.append(0); continue
            p0, p1, p2 = dfp.loc[i-2,"price"], dfp.loc[i-1,"price"], dfp.loc[i,"price"]
            out.append(1 if (p2>p1 and p1>p0) else (-1 if (p2<p1 and p1<p0) else 0))
        dfp["b_regime"] = out
    dfp["is_pivot_row"] = True
    dfp["day_key"] = pd.to_datetime(day).date()
    dfp["timestamp"] = pd.to_datetime(dfp["timestamp"])
    return dfp[["ref_id","timestamp","b_regime","is_pivot_row","day_key"]]

# ---------- build ----------
def build_day(day: str):
    print(f"🛠  Building dataset for {day}")
    ensure_tables()
    ticks = load_ticks(day)
    if ticks.empty:
        print("No ticks for this day."); return

    feats = add_rolls(ticks.copy())
    feats["micro_state"] = 0; feats["maxi_state"] = 0
    feats["day_key"] = pd.to_datetime(feats["timestamp"]).dt.date
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

    day_min_id = int(feats["tickid"].min())
    day_max_id = int(feats["tickid"].max())

    # Load medium segments (try segment tables, else derive from zigzag_points)
    medium = load_segments("medium_trends", day_min_id, day_max_id)
    if medium.empty:
        med_pts = load_points_from_zig("medium", day_min_id, day_max_id)
        medium = segments_from_medium_points(med_pts)

    # Load micro points (try table, else zigzag_points)
    micro = load_segments("micro_trends", day_min_id, day_max_id)
    if micro.empty:
        micro = load_points_from_zig("micro", day_min_id, day_max_id)
    if not micro.empty and "timestamp" not in micro.columns:
        micro = micro.merge(ticks[["tickid","timestamp","mid"]], on="tickid", how="left").rename(columns={"mid":"price"})

    # Labels
    small = label_small(ticks, micro, medium)
    execmany("""
      INSERT INTO ml_labels_small (tickid,timestamp,s_next_hold,horizon_ticks,day_key)
      VALUES (:tickid,:timestamp,:s_next_hold,:horizon_ticks,:day_key)
      ON CONFLICT (tickid) DO UPDATE SET
        timestamp=EXCLUDED.timestamp, s_next_hold=EXCLUDED.s_next_hold,
        horizon_ticks=EXCLUDED.horizon_ticks, day_key=EXCLUDED.day_key
    """, small.to_dict(orient="records"))
    print(f"✔  ml_labels_small upserted: {len(small)}")

    big = label_big(medium, day)
    execmany("""
      INSERT INTO ml_labels_big (ref_id,timestamp,b_regime,is_pivot_row,day_key)
      VALUES (:ref_id,:timestamp,:b_regime,:is_pivot_row,:day_key)
      ON CONFLICT (ref_id) DO UPDATE SET
        timestamp=EXCLUDED.timestamp, b_regime=EXCLUDED.b_regime,
        is_pivot_row=EXCLUDED.is_pivot_row, day_key=EXCLUDED.day_key
    """, big.to_dict(orient="records"))
    print(f"✔  ml_labels_big upserted: {len(big)}")

# ---------- CLI ----------
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--date")
    p.add_argument("--start")
    p.add_argument("--end")
    a = p.parse_args()

    if a.date:
        build_day(a.date); return
    if a.start and a.end:
        d0 = datetime.fromisoformat(a.start).date()
        d1 = datetime.fromisoformat(a.end).date()
        cur = d0
        while cur <= d1:
            build_day(cur.isoformat())
            cur += timedelta(days=1)
        return
    build_day(datetime.utcnow().date().isoformat())

if __name__ == "__main__":
    main()
