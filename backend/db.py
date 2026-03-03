# PATH: backend/db.py
import os
import json
from datetime import date, datetime
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
    if {"bid", "ask"}.issubset(have):
        return "(bid+ask)/2.0"
    raise RuntimeError("No price / bid+ask / mid columns found in ticks")


def detect_bid_ask(conn):
    """return tuple (has_bid, has_ask) booleans"""
    have = columns_exist(conn, "ticks", ["bid", "ask"])
    return ("bid" in have, "ask" in have)


def fetch_ticks_for_range(conn, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Dict[str, Any]]:
    """
    Fetch ticks for a symbol and time range [start_ts, end_ts).
    Returns rows with: id, symbol, timestamp, bid, ask, mid, spread, kal, k2.
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread, kal, k2
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, start_ts, end_ts),
        )
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "symbol": r["symbol"],
                "timestamp": r["timestamp"],
                "bid": float(r["bid"]) if r["bid"] is not None else None,
                "ask": float(r["ask"]) if r["ask"] is not None else None,
                "mid": float(r["mid"]) if r["mid"] is not None else None,
                "spread": float(r["spread"]) if r["spread"] is not None else None,
                "kal": float(r["kal"]) if r["kal"] is not None else None,
                "k2": float(r["k2"]) if r["k2"] is not None else None,
            }
        )
    return out


def get_k2_candles_window(
    conn,
    symbol: str,
    limit: int = 500,
    from_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch k2 flip candles in ascending id order.
    Output keys match frontend API contract:
      id, start_tick_id, end_tick_id, start_ts, end_ts,
      o, h, l, c, k2o, k2c, dir, tick_count
    """
    symbol = (symbol or "").strip() or "XAUUSD"
    lim = max(1, min(int(limit), 5000))

    where = ["symbol = %s"]
    params: List[Any] = [symbol]
    if from_id is not None:
        where.append("id >= %s")
        params.append(int(from_id))

    order_sql = "ORDER BY id ASC" if from_id is not None else "ORDER BY id DESC"
    sql = f"""
    SELECT
        id,
        start_tick_id,
        end_tick_id,
        start_ts,
        end_ts,
        open  AS o,
        high  AS h,
        low   AS l,
        close AS c,
        k2_open  AS k2o,
        k2_close AS k2c,
        dir,
        tick_count
    FROM public.k2_candles
    WHERE {" AND ".join(where)}
    {order_sql}
    LIMIT %s
    """
    params.append(lim)

    with dict_cur(conn) as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    if from_id is None:
        rows = list(reversed(rows))
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "start_tick_id": int(r["start_tick_id"]) if r["start_tick_id"] is not None else None,
                "end_tick_id": int(r["end_tick_id"]) if r["end_tick_id"] is not None else None,
                "start_ts": r["start_ts"].isoformat() if r["start_ts"] is not None else None,
                "end_ts": r["end_ts"].isoformat() if r["end_ts"] is not None else None,
                "o": float(r["o"]) if r["o"] is not None else None,
                "h": float(r["h"]) if r["h"] is not None else None,
                "l": float(r["l"]) if r["l"] is not None else None,
                "c": float(r["c"]) if r["c"] is not None else None,
                "k2o": float(r["k2o"]) if r["k2o"] is not None else None,
                "k2c": float(r["k2c"]) if r["k2c"] is not None else None,
                "dir": int(r["dir"]) if r["dir"] is not None else None,
                "tick_count": int(r["tick_count"]) if r["tick_count"] is not None else None,
            }
        )
    return out


def upsert_backtest_row(
    conn,
    *,
    trading_day: date,
    session_start_ts: datetime,
    session_end_ts: datetime,
    symbol: str,
    config: Dict[str, Any],
    trades_count: int,
    wins_count: int,
    losses_count: int,
    win_rate: float,
    total_profit: float,
    avg_hold_sec: float,
    max_hold_sec: int,
    stopouts_count: int,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Insert or update one daily backtest summary for (symbol, trading_day).
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO public.backtest (
                trading_day,
                session_start_ts,
                session_end_ts,
                symbol,
                config,
                trades_count,
                wins_count,
                losses_count,
                win_rate,
                total_profit,
                avg_hold_sec,
                max_hold_sec,
                stopouts_count,
                notes
            )
            VALUES (
                %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (symbol, trading_day)
            DO UPDATE SET
                session_start_ts = EXCLUDED.session_start_ts,
                session_end_ts = EXCLUDED.session_end_ts,
                config = EXCLUDED.config,
                trades_count = EXCLUDED.trades_count,
                wins_count = EXCLUDED.wins_count,
                losses_count = EXCLUDED.losses_count,
                win_rate = EXCLUDED.win_rate,
                total_profit = EXCLUDED.total_profit,
                avg_hold_sec = EXCLUDED.avg_hold_sec,
                max_hold_sec = EXCLUDED.max_hold_sec,
                stopouts_count = EXCLUDED.stopouts_count,
                notes = EXCLUDED.notes,
                created_at = now()
            RETURNING id, trading_day, symbol
            """,
            (
                trading_day,
                session_start_ts,
                session_end_ts,
                symbol,
                json.dumps(config, separators=(",", ":")),
                int(trades_count),
                int(wins_count),
                int(losses_count),
                float(win_rate),
                float(total_profit),
                float(avg_hold_sec),
                int(max_hold_sec),
                int(stopouts_count),
                notes,
            ),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


# -------------------------------------------------------------------------
# Review / segLines helpers (NO FastAPI imports here)
# These functions are called by backend/main.py routes.
# -------------------------------------------------------------------------

def review_default_segm(conn) -> Optional[Dict[str, Any]]:
    """
    Returns the latest segm preferring one that has NO seglines yet.
    segms has no 'date' column -> we return (start_ts::date)::text as date.
    Output keys: segm_id, date, has_seglines
    """
    with dict_cur(conn) as cur:
        # Prefer latest segm with no seglines
        cur.execute(
            """
            SELECT s.id AS segm_id,
                   (s.start_ts::date)::text AS date,
                   (COUNT(l.id) > 0) AS has_seglines
            FROM public.segms s
            LEFT JOIN public.seglines l ON l.segm_id = s.id
            GROUP BY s.id
            HAVING COUNT(l.id) = 0
            ORDER BY s.id DESC
            LIMIT 1
            """
        )
        r = cur.fetchone()
        if r:
            return r

        # Fallback: latest segm
        cur.execute(
            """
            SELECT s.id AS segm_id,
                   (s.start_ts::date)::text AS date,
                   (COUNT(l.id) > 0) AS has_seglines
            FROM public.segms s
            LEFT JOIN public.seglines l ON l.segm_id = s.id
            GROUP BY s.id
            ORDER BY s.id DESC
            LIMIT 1
            """
        )
        return cur.fetchone()


def review_list_segms(conn, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Returns list for selector:
      segm_id, date, num_ticks, num_lines_active, global_max_abs_dist
    """
    limit = int(limit)
    if limit < 1:
        limit = 1
    if limit > 2000:
        limit = 2000

    with dict_cur(conn) as cur:
        cur.execute(
            """
            WITH tick_counts AS (
              SELECT segm_id, COUNT(*)::int AS num_ticks
              FROM public.segticks
              GROUP BY segm_id
            ),
            line_stats AS (
              SELECT segm_id,
                     COUNT(*) FILTER (WHERE is_active=true)::int AS num_lines_active,
                     MAX(max_abs_dist) FILTER (WHERE is_active=true) AS global_max_abs_dist
              FROM public.seglines
              GROUP BY segm_id
            )
            SELECT s.id AS segm_id,
                   (s.start_ts::date)::text AS date,
                   COALESCE(tc.num_ticks, 0) AS num_ticks,
                   COALESCE(ls.num_lines_active, 0) AS num_lines_active,
                   ls.global_max_abs_dist
            FROM public.segms s
            LEFT JOIN tick_counts tc ON tc.segm_id = s.id
            LEFT JOIN line_stats ls ON ls.segm_id = s.id
            ORDER BY s.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def review_segm_tick_range(conn, segm_id: int) -> Dict[str, Any]:
    """
    Returns: num_ticks, tick_from, tick_to for segm (from segticks.tick_id).
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int AS num_ticks,
                   MIN(tick_id) AS tick_from,
                   MAX(tick_id) AS tick_to
            FROM public.segticks
            WHERE segm_id=%s
            """,
            (int(segm_id),),
        )
        r = cur.fetchone()
        return r or {"num_ticks": 0, "tick_from": None, "tick_to": None}


def review_segm_meta(conn, segm_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns:
      segm_id, date, num_ticks, tick_from, tick_to, num_lines_active, global_max_abs_dist
    """
    segm_id = int(segm_id)

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT (start_ts::date)::text AS date
            FROM public.segms
            WHERE id=%s
            """,
            (segm_id,),
        )
        srow = cur.fetchone()
        if not srow:
            return None

    tr = review_segm_tick_range(conn, segm_id)

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE is_active=true)::int AS num_lines_active,
                   MAX(max_abs_dist) FILTER (WHERE is_active=true) AS global_max_abs_dist
            FROM public.seglines
            WHERE segm_id=%s
            """,
            (segm_id,),
        )
        lr = cur.fetchone() or {}

    return {
        "segm_id": segm_id,
        "date": srow["date"],
        "num_ticks": int(tr["num_ticks"]) if tr.get("num_ticks") is not None else 0,
        "tick_from": int(tr["tick_from"]) if tr.get("tick_from") is not None else None,
        "tick_to": int(tr["tick_to"]) if tr.get("tick_to") is not None else None,
        "num_lines_active": int(lr.get("num_lines_active")) if lr.get("num_lines_active") is not None else 0,
        "global_max_abs_dist": float(lr.get("global_max_abs_dist")) if lr.get("global_max_abs_dist") is not None else None,
    }


def review_ticks_sample(conn, segm_id: int, target_points: int = 5000) -> Dict[str, Any]:
    """
    Downsample ticks for whole segm to ~target_points using row_number stride.
    Returns:
      { segm_id, stride, points: [{id, ts, ask, bid, mid, kal, k2}, ...] }
    """
    segm_id = int(segm_id)
    target = int(target_points)
    if target < 100:
        target = 100
    if target > 50_000:
        target = 50_000

    # count total ticks
    with dict_cur(conn) as cur:
        cur.execute(
            "SELECT COUNT(*)::int AS n FROM public.segticks WHERE segm_id=%s",
            (segm_id,),
        )
        n = int(cur.fetchone()["n"])

    if n <= 0:
        return {"segm_id": segm_id, "stride": 1, "points": []}

    stride = (n + target - 1) // target
    if stride < 1:
        stride = 1

    with dict_cur(conn) as cur:
        cur.execute(
            """
            WITH ordered AS (
              SELECT t.id AS id,
                     t.timestamp AS ts,
                     t.ask, t.bid, t.mid, t.kal, t.k2,
                     ROW_NUMBER() OVER (ORDER BY t.timestamp ASC, t.id ASC) AS rn
              FROM public.segticks st
              JOIN public.ticks t ON t.id = st.tick_id
              WHERE st.segm_id=%s
            )
            SELECT id, ts, ask, bid, mid, kal, k2
            FROM ordered
            WHERE ((rn - 1) %% %s) = 0
            ORDER BY ts ASC, id ASC
            """,
            (segm_id, int(stride)),
        )
        rows = cur.fetchall()

    pts: List[Dict[str, Any]] = []
    for r in rows:
        pts.append(
            {
                "id": int(r["id"]),
                "ts": r["ts"].isoformat(),
                "ask": float(r["ask"]) if r["ask"] is not None else None,
                "bid": float(r["bid"]) if r["bid"] is not None else None,
                "mid": float(r["mid"]) if r["mid"] is not None else None,
                "kal": float(r["kal"]) if r["kal"] is not None else None,
                "k2": float(r["k2"]) if r["k2"] is not None else None,
            }
        )

    return {"segm_id": segm_id, "stride": int(stride), "points": pts}


def review_active_lines(conn, segm_id: int) -> Dict[str, Any]:
    """
    Returns active segLines for segm_id:
      { segm_id, lines: [ ... ] }
    """
    segm_id = int(segm_id)
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, parent_id, depth, iteration,
                   start_ts, end_ts, start_price, end_price,
                   num_ticks, duration_ms, max_abs_dist
            FROM public.seglines
            WHERE segm_id=%s AND is_active=true
            ORDER BY max_abs_dist DESC NULLS LAST, id ASC
            """,
            (segm_id,),
        )
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "parent_id": int(r["parent_id"]) if r["parent_id"] is not None else None,
                "depth": int(r["depth"]),
                "iteration": int(r["iteration"]),
                "start_ts": r["start_ts"].isoformat(),
                "end_ts": r["end_ts"].isoformat(),
                "start_price": float(r["start_price"]),
                "end_price": float(r["end_price"]),
                "num_ticks": int(r["num_ticks"]) if r["num_ticks"] is not None else None,
                "duration_ms": int(r["duration_ms"]) if r["duration_ms"] is not None else None,
                "max_abs_dist": float(r["max_abs_dist"]) if r["max_abs_dist"] is not None else None,
            }
        )

    return {"segm_id": segm_id, "lines": out}


def review_zig_pivots(conn, segm_id: int) -> Dict[str, Any]:
    """
    Returns zig pivots for segm_id:
      { segm_id, pivots: [ ... ] }
    """
    segm_id = int(segm_id)
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, segm_id, tick_id, ts, price, direction, pivot_index
            FROM public.zig_pivots
            WHERE segm_id=%s
            ORDER BY pivot_index ASC, ts ASC, id ASC
            """,
            (segm_id,),
        )
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "segm_id": int(r["segm_id"]),
                "tick_id": int(r["tick_id"]),
                "ts": r["ts"].isoformat() if r["ts"] is not None else None,
                "price": float(r["price"]),
                "direction": r["direction"],
                "pivot_index": int(r["pivot_index"]),
            }
        )

    return {"segm_id": segm_id, "pivots": out}
