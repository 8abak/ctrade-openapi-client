# PATH: backend/db.py
import os
import time
import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

# Tiny DB helper built on psycopg2 + env vars.
# Exposes:
#   get_conn()                         -> psycopg2 connection from pool
#   db()                               -> contextmanager yielding (conn, cur)
#   q(sql, params=None)                -> list of tuples
#   q_dicts(sql, params=None)          -> list[dict]
#   exec_sql(sql, params=None)         -> rowcount
#   tick_sql_fields()                  -> "id, <ts> as ts, <mid> as mid"
#   tick_ts_col()                      -> the chosen ts column
#   tick_mid_expr()                    -> the chosen mid SQL expression
#
# Auto-detects tick schema supporting:
#   - ts TIMESTAMPTZ  OR  timestamp TIMESTAMPTZ
#   - mid NUMERIC (preferred) OR price NUMERIC OR (bid NUMERIC and ask NUMERIC)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://babak:babak33044@localhost:5432/trading",
)

_MIN = int(os.getenv("DB_POOL_MIN", "1"))
_MAX = int(os.getenv("DB_POOL_MAX", "10"))

_pool_lock = threading.Lock()
_pool: Optional[ThreadedConnectionPool] = None

# Cached tick schema choices
_TICK_TS_COL: Optional[str] = None
_TICK_MID_EXPR: Optional[str] = None


def _ensure_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(
                    minconn=_MIN, maxconn=_MAX, dsn=DATABASE_URL
                )
    return _pool


def get_conn():
    return _ensure_pool().getconn()


def put_conn(conn):
    try:
        _ensure_pool().putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


@contextmanager
def db():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                yield conn, cur
    finally:
        put_conn(conn)


def q(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple]:
    with db() as (_, cur):
        cur.execute(sql, params or ())
        return cur.fetchall()


def q_dicts(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    with db() as (_, cur):
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def exec_sql(sql: str, params: Optional[Tuple[Any, ...]] = None) -> int:
    with db() as (_, cur):
        cur.execute(sql, params or ())
        return cur.rowcount


def _detect_tick_schema():
    global _TICK_TS_COL, _TICK_MID_EXPR
    cols = set(
        c["column_name"]
        for c in q_dicts(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='ticks'
            """
        )
    )
    # ts / timestamp
    if "ts" in cols:
        _TICK_TS_COL = "ts"
    elif "timestamp" in cols:
        _TICK_TS_COL = "timestamp"
    elif "time" in cols:
        _TICK_TS_COL = "time"
    elif "created_at" in cols:
        _TICK_TS_COL = "created_at"
    else:
        # fallback; the SQL may error if not present, but we try...
        _TICK_TS_COL = "ts"

    # mid expression
    if "mid" in cols:
        _TICK_MID_EXPR = "mid"
    elif "price" in cols:
        _TICK_MID_EXPR = "price"
    elif "bid" in cols and "ask" in cols:
        _TICK_MID_EXPR = "(bid + ask) / 2.0"
    else:
        # very last resort
        _TICK_MID_EXPR = "price"


def tick_ts_col() -> str:
    if _TICK_TS_COL is None:
        _detect_tick_schema()
    return _TICK_TS_COL  # type: ignore


def tick_mid_expr() -> str:
    if _TICK_MID_EXPR is None:
        _detect_tick_schema()
    return _TICK_MID_EXPR  # type: ignore


def tick_sql_fields() -> str:
    # Returns an SQL select-list segment for ticks: "id, <ts> as ts, <mid> as mid"
    return f"id, {tick_ts_col()} as ts, {tick_mid_expr()} as mid"


def last_tick_id() -> Optional[int]:
    rows = q("SELECT id FROM ticks ORDER BY id DESC LIMIT 1")
    if not rows:
        return None
    return int(rows[0][0])


def sleep_throttle(seconds: float = 0.1):
    # Small sleep to avoid CPU spikes between segments
    time.sleep(seconds)
