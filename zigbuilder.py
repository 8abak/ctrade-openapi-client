#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZigZag live builder for micro/medi/maxi with 4-minute gap resets.
ticks schema: id | symbol | timestamp | bid | ask | atr | mid
Env overrides: DATABASE_URL, TICKS_TS_COL, TICKS_PX_COL, ZIG_GAP_SEC, ZIG_BATCH, ZIG_SLEEP
"""
import os, time
import psycopg2
from psycopg2 import sql

DSN = os.environ.get("DATABASE_URL")
TS_COL = os.environ.get("TICKS_TS_COL", "timestamp")
PX_COL = os.environ.get("TICKS_PX_COL", "mid")

THRESHOLDS = {"micro": 0.30, "medi": 1.50, "maxi": 9.00}
GAP_SEC = int(os.environ.get("ZIG_GAP_SEC", "240"))
BATCH   = int(os.environ.get("ZIG_BATCH", "2000"))
SLEEP   = float(os.environ.get("ZIG_SLEEP", "1.0"))

def conn():
    if not DSN: raise RuntimeError("DATABASE_URL not set")
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
    def __init__(self): self.s_id=self.x_id=None; self.s_ts=self.x_ts=None; self.s_px=self.x_px=None; self.dir=None; self.last_ts=None; self.last_tick_id=0

def ensure_schema(cur):
    for t in THRESHOLDS:
        cur.execute(DDL.format(t=sql.Identifier(t).string))

def load_tail_state(cur, name: str) -> State:
    st = State()
    cur.execute("SELECT last_tick_id FROM zig_state WHERE name=%s", (name,))
    r = cur.fetchone()
    st.last_tick_id = r[0] if r else 0

    cur.execute(sql.SQL("SELECT start_id,end_id FROM {} ORDER BY id DESC LIMIT 1").format(sql.Identifier(name)))
    leg = cur.fetchone()
    if leg:
        end_id = leg[1]
        q = sql.SQL('SELECT id, {ts} AS ts, {px} AS px FROM ticks WHERE id=%s').format(ts=sql.Identifier(TS_COL), px=sql.Identifier(PX_COL))
        cur.execute(q, (end_id,))
        t = cur.fetchone()
        if t:
            st.s_id=t[0]; st.s_ts=t[1]; st.s_px=t[2]
            st.x_id=t[0]; st.x_ts=t[1]; st.x_px=t[2]
            st.dir=None; st.last_ts=t[1]; st.last_tick_id=max(st.last_tick_id, t[0])
    return st

def upsert_state(cur, name, last_id):
    cur.execute("""INSERT INTO zig_state(name,last_tick_id)
                   VALUES (%s,%s)
                   ON CONFLICT (name) DO UPDATE SET last_tick_id=EXCLUDED.last_tick_id""",
                (name, last_id))

def insert_leg(cur, table, s_id,s_ts,s_px, x_id,x_ts,x_px, direction):
    q = sql.SQL("""INSERT INTO {}(start_id,end_id,start_ts,end_ts,dir,span,len)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (start_id,end_id) DO NOTHING""").format(sql.Identifier(table))
    span = float(x_px) - float(s_px)
    length = int(x_id - s_id + 1)
    cur.execute(q, (s_id,x_id,s_ts,x_ts,direction,span,length))

def handle_tick(cur, table, thr, st: State, tick):
    tid, ts, px = tick
    if st.s_id is None:
        st.s_id=tid; st.s_ts=ts; st.s_px=px
        st.x_id=tid; st.x_ts=ts; st.x_px=px
        st.dir=None; st.last_ts=ts; st.last_tick_id=tid
        return
    if (ts - st.last_ts).total_seconds() > GAP_SEC:
        if st.s_id != st.x_id:
            insert_leg(cur, table, st.s_id,st.s_ts,st.s_px, st.x_id,st.x_ts,st.x_px, st.dir or ('up' if st.x_px>=st.s_px else 'dn'))
        st.s_id=tid; st.s_ts=ts; st.s_px=px
        st.x_id=tid; st.x_ts=ts; st.x_px=px
        st.dir=None; st.last_ts=ts; st.last_tick_id=tid
        return

    if st.dir is None:
        if (px - st.s_px) >= thr: st.dir='up'; st.x_id=tid; st.x_ts=ts; st.x_px=px
        elif (st.s_px - px) >= thr: st.dir='dn'; st.x_id=tid; st.x_ts=ts; st.x_px=px
        else:
            if px > st.x_px: st.x_id=tid; st.x_ts=ts; st.x_px=px
            if px < st.x_px: st.x_id=tid; st.x_ts=ts; st.x_px=px
    elif st.dir=='up':
        if px >= st.x_px: st.x_id=tid; st.x_ts=ts; st.x_px=px
        elif (st.x_px - px) >= thr:
            insert_leg(cur, table, st.s_id,st.s_ts,st.s_px, st.x_id,st.x_ts,st.x_px, 'up')
            st.s_id=st.x_id; st.s_ts=st.x_ts; st.s_px=st.x_px
            st.dir='dn'; st.x_id=tid; st.x_ts=ts; st.x_px=px
    else:  # dn
        if px <= st.x_px: st.x_id=tid; st.x_ts=ts; st.x_px=px
        elif (px - st.x_px) >= thr:
            insert_leg(cur, table, st.s_id,st.s_ts,st.s_px, st.x_id,st.x_ts,st.x_px, 'dn')
            st.s_id=st.x_id; st.s_ts=st.x_ts; st.s_px=st.x_px
            st.dir='up'; st.x_id=tid; st.x_ts=ts; st.x_px=px

    st.last_ts=ts; st.last_tick_id=tid

def process_once(cur, name, thr, st: State):
    q = sql.SQL("""SELECT id, {ts} AS ts, {px} AS px
                   FROM ticks WHERE id > %s ORDER BY id ASC LIMIT %s""").format(
        ts=sql.Identifier(TS_COL), px=sql.Identifier(PX_COL))
    cur.execute(q, (st.last_tick_id, BATCH))
    rows = cur.fetchall()
    if not rows: return False
    for r in rows: handle_tick(cur, name, thr, st, r)
    upsert_state(cur, name, rows[-1][0])
    return True

def main():
    with conn() as c:
        with c.cursor() as cur:
            ensure_schema(cur)
            states = {name: load_tail_state(cur, name) for name in THRESHOLDS}
            c.commit()
    while True:
        with conn() as c:
            with c.cursor() as cur:
                worked = False
                for name,thr in THRESHOLDS.items():
                    worked |= process_once(cur, name, thr, states[name])
                c.commit()
        if not worked: time.sleep(SLEEP)

if __name__ == "__main__":
    main()
