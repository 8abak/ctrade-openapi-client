#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental zigzag builder for micro/medi/maxi with 4-minute gap resets.
Reads ticks tail and maintains range legs in three tables.
"""
import os, time, psycopg2
import psycopg2.extras as extras
from datetime import timedelta

DSN = os.environ.get("DATABASE_URL")  # e.g. postgres://user:pass@host:5432/db
THRESHOLDS = {
    "micro": 0.30,
    "medi":  1.50,
    "maxi":  9.00,
}
GAP_SEC = 4*60
BATCH = 2000
SLEEP = 1.0

def conn():
    return psycopg2.connect(DSN, application_name="zigbuilder")

DDL = """
CREATE TABLE IF NOT EXISTS {t}(
  id        bigserial PRIMARY KEY,
  start_id  bigint NOT NULL,
  end_id    bigint NOT NULL,
  start_ts  timestamptz NOT NULL,
  end_ts    timestamptz NOT NULL,
  dir       text NOT NULL CHECK (dir IN ('up','dn')),
  span      numeric NOT NULL,
  len       integer NOT NULL,
  UNIQUE(start_id,end_id)
);
CREATE INDEX IF NOT EXISTS {t}_start ON {t}(start_id);
CREATE INDEX IF NOT EXISTS {t}_end   ON {t}(end_id);

CREATE TABLE IF NOT EXISTS zig_state(
  name text PRIMARY KEY,
  last_tick_id bigint NOT NULL DEFAULT 0
);
"""

class State:
    __slots__ = ("s_id","s_ts","s_px","x_id","x_ts","x_px","dir","last_ts","last_tick_id")
    def __init__(self):
        self.s_id=self.x_id=None
        self.s_ts=self.x_ts=None
        self.s_px=self.x_px=None
        self.dir=None
        self.last_ts=None
        self.last_tick_id=0

def ensure_schema(cur):
    for t in THRESHOLDS.keys():
        cur.execute(DDL.format(t=extras.AsIs(t)))

def load_tail_state(cur, name: str) -> State:
    st = State()
    # last processed tick id
    cur.execute("SELECT last_tick_id FROM zig_state WHERE name=%s", (name,))
    r = cur.fetchone()
    st.last_tick_id = r[0] if r else 0

    # last leg
    cur.execute(f"""
      SELECT start_id,end_id,end_ts,dir
      FROM {name}
      ORDER BY id DESC LIMIT 1""")
    leg = cur.fetchone()
    if leg:
        end_id = leg[1]
        cur.execute("SELECT id, ts, mid FROM ticks WHERE id=%s", (end_id,))
        t = cur.fetchone()
        if t:
            st.s_id = t[0]; st.s_ts = t[1]; st.s_px = t[2]
            st.x_id = t[0]; st.x_ts = t[1]; st.x_px = t[2]
            st.dir  = None
            st.last_ts = t[1]
            st.last_tick_id = max(st.last_tick_id, t[0])
    return st

def upsert_state(cur, name, last_id):
    cur.execute("""
      INSERT INTO zig_state(name,last_tick_id)
      VALUES (%s,%s)
      ON CONFLICT (name) DO UPDATE SET last_tick_id=excluded.last_tick_id
    """, (name, last_id))

def insert_leg(cur, table, s_id,s_ts,s_px, x_id,x_ts,x_px, direction):
    span = x_px - s_px
    length = int(x_id - s_id + 1)
    cur.execute(f"""
      INSERT INTO {table}(start_id,end_id,start_ts,end_ts,dir,span,len)
      VALUES (%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (start_id,end_id) DO NOTHING
    """, (s_id,x_id,s_ts,x_ts,direction,span,length))

def handle_tick(cur, table, thr, st: State, tick):
    tid, ts, px = tick
    if st.s_id is None:
        st.s_id=tid; st.s_ts=ts; st.s_px=px
        st.x_id=tid; st.x_ts=ts; st.x_px=px
        st.dir=None; st.last_ts=ts
        st.last_tick_id = tid
        return

    # gap reset
    if (ts - st.last_ts).total_seconds() > GAP_SEC:
        if st.s_id != st.x_id:
            insert_leg(cur, table, st.s_id,st.s_ts,st.s_px, st.x_id,st.x_ts,st.x_px, st.dir or ('up' if st.x_px>=st.s_px else 'dn'))
        st.s_id=tid; st.s_ts=ts; st.s_px=px
        st.x_id=tid; st.x_ts=ts; st.x_px=px
        st.dir=None; st.last_ts=ts
        st.last_tick_id = tid
        return

    if st.dir is None:
        if (px - st.s_px) >= thr:
            st.dir='up'; st.x_id=tid; st.x_ts=ts; st.x_px=px
        elif (st.s_px - px) >= thr:
            st.dir='dn'; st.x_id=tid; st.x_ts=ts; st.x_px=px
        else:
            if px > st.x_px:  st.x_id=tid; st.x_ts=ts; st.x_px=px
            if px < st.x_px:  st.x_id=tid; st.x_ts=ts; st.x_px=px

    elif st.dir=='up':
        if px >= st.x_px:
            st.x_id=tid; st.x_ts=ts; st.x_px=px
        elif (st.x_px - px) >= thr:
            insert_leg(cur, table, st.s_id,st.s_ts,st.s_px, st.x_id,st.x_ts,st.x_px, 'up')
            st.s_id=st.x_id; st.s_ts=st.x_ts; st.s_px=st.x_px
            st.dir='dn'; st.x_id=tid; st.x_ts=ts; st.x_px=px

    else: # dir == 'dn'
        if px <= st.x_px:
            st.x_id=tid; st.x_ts=ts; st.x_px=px
        elif (px - st.x_px) >= thr:
            insert_leg(cur, table, st.s_id,st.s_ts,st.s_px, st.x_id,st.x_ts,st.x_px, 'dn')
            st.s_id=st.x_id; st.s_ts=st.x_ts; st.s_px=st.x_px
            st.dir='up'; st.x_id=tid; st.x_ts=ts; st.x_px=px

    st.last_ts = ts
    st.last_tick_id = tid

def process_once(cur, name, thr, st: State):
    # read next batch of ticks
    cur.execute("SELECT id, ts, mid FROM ticks WHERE id > %s ORDER BY id ASC LIMIT %s",
                (st.last_tick_id, BATCH))
    rows = cur.fetchall()
    if not rows:
        return False
    for r in rows:
        handle_tick(cur, name, thr, st, r)
    upsert_state(cur, name, rows[-1][0])
    return True

def main():
    if not DSN:
        raise RuntimeError("DATABASE_URL not set")
    with conn() as c:
        c.autocommit = False
        with c.cursor() as cur:
            ensure_schema(cur)
            states = {name: load_tail_state(cur, name) for name in THRESHOLDS}
            c.commit()

    while True:
        with conn() as c:
            with c.cursor() as cur:
                any_work = False
                for name,thr in THRESHOLDS.items():
                    st = states[name]
                    worked = process_once(cur, name, thr, st)
                    any_work = any_work or worked
                c.commit()
        if not any_work:
            time.sleep(SLEEP)

if __name__ == "__main__":
    main()
