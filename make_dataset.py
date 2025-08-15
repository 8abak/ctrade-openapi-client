# make_dataset.py — builds ml_features_tick, ml_labels_small, ml_labels_big
# Auto-detects column names: start_tick_id/end_tick_id OR start_tickid/end_tickid.
# Converts micro SEGMENTS -> micro POINTS (using segment start as pivot).
# Labels only counter-direction micros inside a medium leg.

import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

MA_FAST, MA_SLOW = 20, 100
MOM_WINS = [5, 20]
ATR_S, ATR_M = 14, 100
VWAP_WIN = 200
SMALL_HORIZON_TICKS = 600
SESSION_BREAKS = [(0,8,"SYD"), (8,13,"TOK"), (13,21,"LON"), (21,24,"NY")]
PREF_SCHEMAS = ["public", "web"]

# ---------- DB helpers ----------
def q(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execmany(sql, rows):
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text(sql), rows)

def find_table(table: str):
    df = q("""SELECT table_schema, table_name FROM information_schema.tables WHERE table_name=:t""", {"t": table})
    for sch in PREF_SCHEMAS:
        hit = df[df["table_schema"]==sch]
        if not hit.empty:
            return sch, f'{sch}."{table}"'
    return None, None

def table_cols(schema: str, name: str) -> set:
    if not schema or not name: return set()
    return set(q("""SELECT column_name FROM information_schema.columns
                    WHERE table_schema=:s AND table_name=:n""",
                 {"s": schema, "n": name})["column_name"].tolist())

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
            if stmt.strip(): conn.execute(text(stmt))

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
    df = q("""SELECT id AS tickid, timestamp, bid, ask, mid
              FROM ticks WHERE DATE(timestamp)=:d ORDER BY id""", {"d": day})
    print(f"ticks: {len(df)} for {day}")
    return df

def resolve_col(cols: set, *candidates):
    for c in candidates:
        if c in cols: return c
    return None

def load_segments(table_name: str, day_min_id: int, day_max_id: int) -> pd.DataFrame:
    sch, qname = find_table(table_name)
    if not qname:
        print(f"{table_name}: not found in public/web")
        return pd.DataFrame()
    cols = table_cols(sch, table_name)

    start_col = resolve_col(cols, "start_tick_id", "start_tickid", "start_id")
    end_col   = resolve_col(cols, "end_tick_id",   "end_tickid",   "end_id")
    if start_col and end_col and "direction" in cols:
        sel_start_ts  = "start_ts"   if "start_ts"   in cols else "NULL::timestamptz AS start_ts"
        sel_end_ts    = "end_ts"     if "end_ts"     in cols else "NULL::timestamptz AS end_ts"
        sel_start_pr  = "start_price"if "start_price"in cols else "NULL::double precision AS start_price"
        sel_end_pr    = "end_price"  if "end_price"  in cols else "NULL::double precision AS end_price"

        df = q(f"""
          SELECT id,
                 {start_col} AS start_tickid,
                 {end_col}   AS end_tickid,
                 direction,
                 {sel_start_ts},
                 {sel_end_ts},
                 {sel_start_pr},
                 {sel_end_pr}
          FROM {qname}
          WHERE {end_col} >= :min_id AND {start_col} <= :max_id
          ORDER BY {start_col}
        """, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table_name}@{sch}: {len(df)} segments (tickid overlap)")
        return df

    # point-like fallback (rare)
    if {"id","tickid","direction"} <= cols:
        ts_sel  = "timestamp" if "timestamp" in cols else "NULL::timestamptz AS timestamp"
        pr_sel  = "price"     if "price"     in cols else "NULL::double precision AS price"
        df = q(f"""
          SELECT id, tickid, direction, {ts_sel}, {pr_sel}
          FROM {qname}
          WHERE tickid BETWEEN :min_id AND :max_id
          ORDER BY tickid
        """, {"min_id": day_min_id, "max_id": day_max_id})
        print(f"{table_name}@{sch}: {len(df)} points (tickid overlap)")
        return df

    print(f"{table_name}@{sch}: unsupported columns {cols} → empty")
    return pd.DataFrame()

def micro_segments_to_points(micro_segs: pd.DataFrame, mode: str = "end") -> pd.DataFrame:
    """
    Convert micro SEGMENTS to micro pivot POINTS.
    mode: "end" (default), "start", or "both"
    Returns columns: id, tickid, direction, timestamp, price
    """
    if micro_segs.empty:
        return pd.DataFrame(columns=["id","tickid","direction","timestamp","price"])

    df = micro_segs.copy()
    cols = set(df.columns)

    def rc(*cands):
        for c in cands:
            if c in cols: return c
        return None

    start_id = rc("start_tick_id", "start_tickid", "start_id")
    end_id   = rc("end_tick_id",   "end_tickid",   "end_id")
    start_ts = rc("start_ts")
    end_ts   = rc("end_ts")
    start_pr = rc("start_price")
    end_pr   = rc("end_price")

    def build(which):
        if which == "start":
            tick = start_id; ts = start_ts; pr = start_pr
        else:  # "end"
            tick = end_id;   ts = end_ts;   pr = end_pr
        out = pd.DataFrame({
            "id": df["id"].values,
            "tickid": df[tick].values if tick in df.columns else None,
            "direction": df["direction"].values,        # segment direction (kept for reference)
            "timestamp": df[ts].values if ts and ts in df.columns else None,
            "price": df[pr].values if pr and pr in df.columns else None,
        })
        return out

    pts = build(mode) if mode != "both" else pd.concat([build("start"), build("end")], ignore_index=True)
    pts = pts.dropna(subset=["tickid"]).copy()
    pts["tickid"] = pts["tickid"].astype(int)
    pts = pts.drop_duplicates(subset=["tickid"]).sort_values("tickid").reset_index(drop=True)
    return pts[["id","tickid","direction","timestamp","price"]]


# ---------- labels ----------
def _is_up(d):
    s = str(d).strip().lower()
    return s in {"up","u","+1","1"} or (s.replace('.','',1).lstrip('-').isdigit() and float(s) > 0)

def _is_dn(d):
    s = str(d).strip().lower()
    return s in {"dn","down","d","-1"} or (s.replace('.','',1).lstrip('-').isdigit() and float(s) < 0)

def label_small(day_ticks: pd.DataFrame, micro_pts: pd.DataFrame, medium_segs: pd.DataFrame) -> pd.DataFrame:
    """
    Label each micro pivot (preferably micro segment ENDS) inside its containing medium leg.

    s_next_hold = 1  if the next medium leg flips direction BEFORE price breaks the current leg's
                      extreme-so-far (i.e., that micro 'held' and the medium turned)
                = 0  otherwise (the leg makes a new extreme before flipping).

    Implementation notes:
    - We detect the 'next medium segment' by taking the next row in the sorted medium table,
      not by expecting start_tickid == previous end_tickid.
    - No counter-direction filtering: evaluate every micro END inside the leg.
    """
    if day_ticks.empty or micro_pts.empty or medium_segs.empty:
        return pd.DataFrame(columns=["tickid","timestamp","s_next_hold","horizon_ticks","day_key"])

    ticks = day_ticks.set_index("tickid")
    segs  = medium_segs.sort_values("start_tickid").reset_index(drop=True)

    TOL = 1e-12
    out = []

    for _, mu in micro_pts.iterrows():
        if "tickid" not in mu or pd.isna(mu["tickid"]):
            continue
        t_id = int(mu["tickid"])
        if t_id not in ticks.index:
            continue

        ts = ticks.loc[t_id, "timestamp"]

        # Find containing medium leg and its index
        mask = (segs["start_tickid"] <= t_id) & (t_id <= segs["end_tickid"])
        if not mask.any():
            continue
        seg_idx = int(mask.idxmax())
        seg = segs.iloc[seg_idx]

        seg_dir   = str(seg["direction"]).lower()   # 'up' or 'dn'
        seg_start = int(seg["start_tickid"])
        seg_end   = int(seg["end_tickid"])
        if seg_start > seg_end:  # safety
            seg_start, seg_end = seg_end, seg_start

        # Past-to-now slice to compute the leg's extreme so far
        if seg_start in ticks.index:
            leg_slice = ticks.loc[seg_start:t_id, "mid"]
        else:
            leg_slice = ticks.loc[:t_id, "mid"]
        if leg_slice.empty:
            leg_slice = pd.Series([ticks.loc[t_id, "mid"]])

        # Forward slice (from this micro to the end of the leg)
        if seg_end in ticks.index:
            fwd_slice = ticks.loc[t_id:seg_end, "mid"]
        else:
            fwd_slice = ticks.loc[t_id:, "mid"]

        # Break test vs extreme-so-far
        if seg_dir.startswith("dn"):
            extreme = float(leg_slice.min())
            broke   = (not fwd_slice.empty) and (float(fwd_slice.min()) < extreme - TOL)
        else:
            extreme = float(leg_slice.max())
            broke   = (not fwd_slice.empty) and (float(fwd_slice.max()) > extreme + TOL)

        # Did the NEXT medium segment flip direction?
        flipped = False
        if seg_idx + 1 < len(segs):
            next_dir = str(segs.iloc[seg_idx + 1]["direction"]).lower()
            flipped  = (next_dir != seg_dir)

        s_next_hold = 1 if (flipped and not broke) else 0
        horizon = int(min(len(ticks.loc[t_id:]), SMALL_HORIZON_TICKS))

        out.append({
            "tickid": t_id,
            "timestamp": ts,
            "s_next_hold": int(s_next_hold),
            "horizon_ticks": horizon,
            "day_key": pd.to_datetime(ts).date()
        })

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
    if ticks.empty: print("No ticks for this day."); return

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

    # Medium segments
    medium = load_segments("medium_trends", day_min_id, day_max_id)

    # Micro: try segment table then convert to points; if empty, fall back to zig points (rare)
    micro_segs = load_segments("micro_trends", day_min_id, day_max_id)
    micro_pts = micro_segments_to_points(micro_segs, mode="end")
    if micro_pts.empty:
        # optional: zigzag_points fallback (only if you actually keep them)
        sch, qname = find_table("zigzag_points")
        if qname:
            cols = table_cols(sch, "zigzag_points")
            if {"id","level","tickid","direction"} <= cols:
                pts = q(f"""
                  SELECT id, tickid, direction,
                         {"timestamp" if "timestamp" in cols else "NULL::timestamptz AS timestamp"},
                         {"price" if "price" in cols else "NULL::double precision AS price"}
                  FROM {qname}
                  WHERE level='micro' AND tickid BETWEEN :min_id AND :max_id
                  ORDER BY tickid
                """, {"min_id": day_min_id, "max_id": day_max_id})
                micro_pts = pts

    # Labels
    small = label_small(ticks, micro_pts, medium)
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
            build_day(cur.isoformat()); cur += timedelta(days=1)
        return
    build_day(datetime.utcnow().date().isoformat())

if __name__ == "__main__":
    main()
