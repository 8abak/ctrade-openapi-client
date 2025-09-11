#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZigZag live builder for min/mid/max (with prices).

- Resumes from last completed leg in each table.
- Processes ticks in batches, then tails live.
- Emits a leg only when the opposite threshold confirms.
- Writes start_price/end_price and span rounded to 2dp.
- Optional gap reset across long time gaps (disabled by default).

Env:
  DATABASE_URL   (psycopg2 DSN, required)
  TICKS_TS_COL   default: "timestamp" (falls back to "ts" if not found)
  TICKS_PX_COL   default: "mid" (falls back to "price", then "bid")
  ZIG_BATCH      batch size per loop (default 2000)
  ZIG_SLEEP      idle sleep seconds (default 1.0)
  ZIG_GAP_SEC    reset leg if (ts - last_ts) > GAP (default 0 => disabled)
  LOG_EVERY      print progress every N batches (default 30)
"""
import os
import time
from typing import Optional, Dict, Tuple

import psycopg2
from psycopg2 import sql

# ---------- config ----------
DSN      = os.environ.get("DATABASE_URL")
TS_COL   = os.environ.get("TICKS_TS_COL")   # if None, we auto-detect
PX_COL   = os.environ.get("TICKS_PX_COL")   # if None, we auto-detect
BATCH    = int(os.environ.get("ZIG_BATCH", "2000"))
SLEEP    = float(os.environ.get("ZIG_SLEEP", "1.0"))
GAP_SEC  = int(os.environ.get("ZIG_GAP_SEC", "0"))  # 0 = disabled
LOG_EVERY= int(os.environ.get("LOG_EVERY", "30"))

# thresholds (same as before: micro->min=0.30, medi->mid=1.50, maxi->max=9.00)
THRESHOLDS: Dict[str, float] = {"min": 0.30, "mid": 1.50, "max": 9.00}


def get_conn():
    if not DSN:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DSN, application_name="zigbuilder")


DDL = """
CREATE TABLE IF NOT EXISTS {t}(
  id           bigserial PRIMARY KEY,
  start_id     bigint      NOT NULL,
  end_id       bigint      NOT NULL,
  start_ts     timestamptz NOT NULL,
  end_ts       timestamptz NOT NULL,
  start_price  numeric(12,5) NOT NULL,
  end_price    numeric(12,5) NOT NULL,
  dir          text        NOT NULL CHECK (dir IN ('up','dn')),
  span         numeric(12,2) NOT NULL,
  len          integer     NOT NULL,
  UNIQUE (start_id, end_id)
);
CREATE INDEX IF NOT EXISTS {t}_start_id_idx ON {t}(start_id);
CREATE INDEX IF NOT EXISTS {t}_end_id_idx   ON {t}(end_id);

CREATE TABLE IF NOT EXISTS zig_state(
  name         text PRIMARY KEY,
  last_tick_id bigint NOT NULL DEFAULT 0
);
"""


def ensure_schema(cur):
    for name in THRESHOLDS:
        cur.execute(DDL.format(t=sql.Identifier(name).string))


def detect_tick_cols(cur) -> Tuple[str, str]:
    """
    Returns (ts_col, px_col) from ticks.
    """
    # time
    if TS_COL:
        ts = TS_COL
    else:
        cur.execute("""
          SELECT column_name FROM information_schema.columns
          WHERE table_name='ticks' AND column_name IN ('timestamp','ts')
          ORDER BY CASE column_name WHEN 'timestamp' THEN 0 ELSE 1 END
          LIMIT 1""")
        r = cur.fetchone()
        if not r:
            raise RuntimeError("ticks table must have a 'timestamp' or 'ts' column")
        ts = r[0]

    # price
    if PX_COL:
        px = PX_COL
    else:
        cur.execute("""
          SELECT column_name FROM information_schema.columns
          WHERE table_name='ticks' AND column_name IN ('mid','price','bid')
          ORDER BY CASE column_name WHEN 'mid' THEN 0 WHEN 'price' THEN 1 ELSE 2 END
          LIMIT 1""")
        r = cur.fetchone()
        if not r:
            raise RuntimeError("ticks table must have 'mid' or 'price' or 'bid'")
        px = r[0]

    return ts, px


class State:
    __slots__ = (
        "s_id", "s_ts", "s_px",   # start anchor
        "x_id", "x_ts", "x_px",   # current extreme
        "dir",                    # 'up'|'dn'|None
        "last_ts",                # last tick ts (gap detect)
        "last_tick_id"            # resume cursor
    )
    def __init__(self):
        self.s_id = self.s_ts = self.s_px = None
        self.x_id = self.x_ts = self.x_px = None
        self.dir = None
        self.last_ts = None
        self.last_tick_id = 0


def load_tail_state(cur, table: str, ts_col: str, px_col: str) -> State:
    st = State()
    # read cursor
    cur.execute("SELECT last_tick_id FROM zig_state WHERE name=%s", (table,))
    r = cur.fetchone()
    st.last_tick_id = r[0] if r else 0

    # resume from last completed leg end
    cur.execute(sql.SQL("SELECT end_id FROM {} ORDER BY id DESC LIMIT 1")
                .format(sql.Identifier(table)))
    leg = cur.fetchone()
    if leg:
        end_id = leg[0]
        q = sql.SQL("SELECT id,{ts} AS ts,{px} AS px FROM ticks WHERE id=%s")\
              .format(ts=sql.Identifier(ts_col), px=sql.Identifier(px_col))
        cur.execute(q, (end_id,))
        t = cur.fetchone()
        if t:
            st.s_id = st.x_id = t[0]
            st.s_ts = st.x_ts = t[1]
            st.s_px = st.x_px = t[2]
            st.dir = None
            st.last_ts = t[1]
            st.last_tick_id = max(st.last_tick_id, t[0])
    return st


def upsert_state(cur, name: str, last_tick_id: int):
    cur.execute("""
      INSERT INTO zig_state(name,last_tick_id)
      VALUES (%s,%s)
      ON CONFLICT (name) DO UPDATE SET last_tick_id = EXCLUDED.last_tick_id
    """, (name, last_tick_id))


def insert_leg(cur, table: str,
               s_id, s_ts, s_px,
               x_id, x_ts, x_px,
               direction: str):
    ins = sql.SQL("""
      INSERT INTO {}(
        start_id,end_id,start_ts,end_ts,start_price,end_price,dir,span,len
      ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (start_id,end_id) DO NOTHING
    """).format(sql.Identifier(table))
    span = round(float(x_px) - float(s_px), 2)
    length = int(x_id - s_id + 1)
    cur.execute(ins, (s_id, x_id, s_ts, x_ts, s_px, x_px, direction, span, length))


def handle_tick(cur, table: str, thr: float, st: State,
                tid: int, ts, px):
    # prime
    if st.s_id is None:
        st.s_id = st.x_id = tid
        st.s_ts = st.x_ts = ts
        st.s_px = st.x_px = px
        st.dir = None
        st.last_ts = ts
        st.last_tick_id = tid
        return

    # optional gap reset
    if GAP_SEC > 0 and st.last_ts is not None:
        dt = (ts - st.last_ts).total_seconds()
        if dt > GAP_SEC:
            # close the current leg at extreme (if there is movement)
            if st.s_id != st.x_id:
                insert_leg(cur, table, st.s_id, st.s_ts, st.s_px,
                           st.x_id, st.x_ts, st.x_px,
                           'up' if (st.x_px >= st.s_px) else 'dn')
            # restart from this tick
            st.s_id = st.x_id = tid
            st.s_ts = st.x_ts = ts
            st.s_px = st.x_px = px
            st.dir = None
            st.last_ts = ts
            st.last_tick_id = tid
            return

    if st.dir is None:
        # wait for first confirmed direction
        if (px - st.s_px) >= thr:
            st.dir = 'up'
            if px >= st.x_px:
                st.x_id = tid; st.x_ts = ts; st.x_px = px
        elif (st.s_px - px) >= thr:
            st.dir = 'dn'
            if px <= st.x_px:
                st.x_id = tid; st.x_ts = ts; st.x_px = px
        else:
            if px > st.x_px:
                st.x_id = tid; st.x_ts = ts; st.x_px = px
            if px < st.x_px:
                st.x_id = tid; st.x_ts = ts; st.x_px = px

    elif st.dir == 'up':
        if px >= st.x_px:
            st.x_id = tid; st.x_ts = ts; st.x_px = px
        elif (st.x_px - px) >= thr:
            # reversal confirmed → emit UP leg
            insert_leg(cur, table, st.s_id, st.s_ts, st.s_px,
                       st.x_id, st.x_ts, st.x_px, 'up')
            # new start is extreme; flip direction
            st.s_id = st.x_id; st.s_ts = st.x_ts; st.s_px = st.x_px
            st.dir = 'dn'
            st.x_id = tid; st.x_ts = ts; st.x_px = px

    else:  # st.dir == 'dn'
        if px <= st.x_px:
            st.x_id = tid; st.x_ts = ts; st.x_px = px
        elif (px - st.x_px) >= thr:
            # reversal confirmed → emit DN leg
            insert_leg(cur, table, st.s_id, st.s_ts, st.s_px,
                       st.x_id, st.x_ts, st.x_px, 'dn')
            # new start is extreme; flip direction
            st.s_id = st.x_id; st.s_ts = st.x_ts; st.s_px = st.x_px
            st.dir = 'up'
            st.x_id = tid; st.x_ts = ts; st.x_px = px

    st.last_ts = ts
    st.last_tick_id = tid


def process_batch(cur, table: str, thr: float, st: State,
                  ts_col: str, px_col: str) -> bool:
    """
    Pull next BATCH ticks and update state.
    Returns True if any ticks were processed (work done).
    """
    q = sql.SQL("""
        SELECT id, {ts} AS ts, {px} AS px
        FROM ticks
        WHERE id > %s
        ORDER BY id ASC
        LIMIT %s
    """).format(ts=sql.Identifier(ts_col), px=sql.Identifier(px_col))

    cur.execute(q, (st.last_tick_id, BATCH))
    rows = cur.fetchall()
    if not rows:
        return False

    for tid, ts, px in rows:
        handle_tick(cur, table, thr, st, tid, ts, px)

    upsert_state(cur, table, rows[-1][0])
    return True


def main():
    with get_conn() as c:
        with c.cursor() as cur:
            ensure_schema(cur)
            ts_col, px_col = detect_tick_cols(cur)
            states = {name: load_tail_state(cur, name, ts_col, px_col)
                      for name in THRESHOLDS}
        c.commit()

    batches = 0
    while True:
        did_work = False
        with get_conn() as c:
            with c.cursor() as cur:
                ts_col, px_col = detect_tick_cols(cur)  # in case schema changed
                for name, thr in THRESHOLDS.items():
                    did_work |= process_batch(cur, name, thr,
                                              states[name], ts_col, px_col)
            c.commit()

        batches += 1
        if batches % LOG_EVERY == 0:
            print(f"[zigbuilder] heartbeat: processed {batches} batches; "
                  f"last ids: " +
                  ", ".join(f"{k}:{states[k].last_tick_id}" for k in THRESHOLDS))

        if not did_work:
            time.sleep(SLEEP)


if __name__ == "__main__":
    main()
