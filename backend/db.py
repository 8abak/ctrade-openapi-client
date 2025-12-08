# PATH: backend/db.py
import os
import psycopg2
import psycopg2.extras
from typing import List, Optional, Dict, Any

# Tiny DB helper used by backend modules
# Reads DATABASE_URL or falls back to a sensible local default.
DEFAULT_URL = "postgresql://babak:babak33044@localhost:5432/trading"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_URL)

def get_conn():
    # autocommit for simple SELECT/INSERT/UPDATE flows
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def dict_cur(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if not row:
            return None
        return row[0]

def columns_exist(conn, table, cols):
    q = """
    SELECT column_name FROM information_schema.columns
    WHERE table_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(q, (table,))
        have = {r[0] for r in cur.fetchall()}
    return {c for c in cols if c in have}

def detect_ts_col(conn):
    """prefer 'ts' then 'timestamp' then 'time' then 'created_at'"""
    prefs = ["ts", "timestamp", "time", "created_at"]
    have = columns_exist(conn, "ticks", prefs)
    for p in prefs:
        if p in have:
            return p
    raise RuntimeError("No timestamp column found in ticks")

def detect_mid_expr(conn):
    """priority: price -> mid -> (bid+ask)/2.0"""
    have = columns_exist(conn, "ticks", ["price", "mid", "bid", "ask"])
    if "price" in have:
        return "price"
    if "mid" in have:
        return "mid"
    if {"bid","ask"}.issubset(have):
        return "(bid+ask)/2.0"
    raise RuntimeError("No price / bid+ask / mid columns found in ticks")

def detect_bid_ask(conn):
    """return tuple (has_bid, has_ask) booleans"""
    have = columns_exist(conn, "ticks", ["bid", "ask"])
    return ("bid" in have, "ask" in have)
