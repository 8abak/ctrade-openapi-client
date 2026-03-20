# PATH: backend/db.py
import os
import json
from datetime import date, datetime
from decimal import Decimal
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


def table_exists(conn, table: str) -> bool:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table,),
        )
        return cur.fetchone() is not None


def table_columns(conn, table: str) -> set:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table,),
        )
        return {str(r[0]) for r in cur.fetchall()}


def _jsonable_db(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, dict):
        return {k: _jsonable_db(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable_db(v) for v in o]
    return o


def _normalize_tick_rows(
    rows: List[Dict[str, Any]],
    *,
    has_kal: bool,
    has_k2: bool,
    has_bid: bool,
    has_ask: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)

        if isinstance(item.get("mid"), Decimal):
            item["mid"] = float(item["mid"])
        if has_kal:
            if isinstance(item.get("kal"), Decimal):
                item["kal"] = float(item["kal"])
        else:
            item["kal"] = item.get("mid")

        if has_k2:
            if isinstance(item.get("k2"), Decimal):
                item["k2"] = float(item["k2"])
        else:
            item["k2"] = None

        if has_bid and isinstance(item.get("bid"), Decimal):
            item["bid"] = float(item["bid"])
        if has_ask and isinstance(item.get("ask"), Decimal):
            item["ask"] = float(item["ask"])

        item["spread"] = (
            (item.get("ask") - item.get("bid"))
            if (
                has_bid
                and has_ask
                and item.get("ask") is not None
                and item.get("bid") is not None
            )
            else None
        )

        ts = item.get("ts")
        if isinstance(ts, (datetime, date)):
            item["ts"] = ts.isoformat()

        out.append(item)
    return out


def _fetch_ticks_by_id_range(conn, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    ts_col = detect_ts_col(conn)
    mid_expr = detect_mid_expr(conn)
    has_bid, has_ask = detect_bid_ask(conn)
    has_kal = "kal" in columns_exist(conn, "ticks", ["kal"])
    has_k2 = "k2" in columns_exist(conn, "ticks", ["k2"])

    bid_sel = ", bid" if has_bid else ""
    ask_sel = ", ask" if has_ask else ""
    kal_sel = ", kal" if has_kal else ""
    k2_sel = ", k2" if has_k2 else ""

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id,
                   {ts_col} AS ts,
                   {mid_expr} AS mid
                   {kal_sel}{k2_sel}{bid_sel}{ask_sel}
            FROM public.ticks
            WHERE id BETWEEN %s AND %s
            ORDER BY id ASC
            """,
            (int(start_id), int(end_id)),
        )
        rows = cur.fetchall()

    return _normalize_tick_rows(
        [dict(r) for r in rows],
        has_kal=has_kal,
        has_k2=has_k2,
        has_bid=has_bid,
        has_ask=has_ask,
    )


def _get_tick_range_meta(conn, start_id: int, end_id: int) -> Optional[Dict[str, Any]]:
    ts_col = detect_ts_col(conn)
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT
                MIN(id) AS startid,
                MAX(id) AS endid,
                MIN({ts_col}) AS startts,
                MAX({ts_col}) AS endts,
                COUNT(*)::bigint AS tickcount
            FROM public.ticks
            WHERE id BETWEEN %s AND %s
            """,
            (int(start_id), int(end_id)),
        )
        row = cur.fetchone()

    if not row or row["startid"] is None or row["endid"] is None:
        return None

    return {
        "startid": int(row["startid"]),
        "endid": int(row["endid"]),
        "startts": row["startts"],
        "endts": row["endts"],
        "tickcount": int(row["tickcount"] or 0),
    }


def _get_overlapping_days(conn, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    if not table_exists(conn, "days"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, startid, endid, startts, endts
            FROM public.days
            WHERE endid >= %s
              AND startid <= %s
            ORDER BY id ASC
            """,
            (int(start_id), int(end_id)),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _get_pivots_for_day(conn, day_id: int) -> List[Dict[str, Any]]:
    if not table_exists(conn, "pivots"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, dayid, layer, rev, tickid, ts, px, ptype, pivotno, dayrow
            FROM public.pivots
            WHERE dayid = %s
            ORDER BY ts ASC, id ASC
            """,
            (int(day_id),),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _get_pivots_for_tick_range(conn, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    if not table_exists(conn, "pivots"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, dayid, layer, rev, tickid, ts, px, ptype, pivotno, dayrow
            FROM public.pivots
            WHERE tickid BETWEEN %s AND %s
            ORDER BY tickid ASC, id ASC
            """,
            (int(start_id), int(end_id)),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _fetch_optional_tpivots(
    conn,
    *,
    day_id: Optional[int] = None,
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
    start_id: Optional[int] = None,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not table_exists(conn, "tpivots"):
        return []

    cols = table_columns(conn, "tpivots")
    where = []
    params: List[Any] = []

    if day_id is not None and "dayid" in cols:
        where.append("dayid = %s")
        params.append(int(day_id))
    elif start_id is not None and end_id is not None and "tickid" in cols:
        where.append("tickid BETWEEN %s AND %s")
        params.extend([int(start_id), int(end_id)])
    elif start_ts is not None and end_ts is not None:
        if {"startts", "endts"}.issubset(cols):
            where.append("COALESCE(endts, startts) >= %s AND startts <= %s")
            params.extend([start_ts, end_ts])
        elif {"firstts", "lastts"}.issubset(cols):
            where.append("COALESCE(lastts, firstts) >= %s AND firstts <= %s")
            params.extend([start_ts, end_ts])
        elif "ts" in cols:
            where.append("ts BETWEEN %s AND %s")
            params.extend([start_ts, end_ts])
        elif "centerts" in cols:
            where.append("centerts BETWEEN %s AND %s")
            params.extend([start_ts, end_ts])
        elif "repts" in cols:
            where.append("repts BETWEEN %s AND %s")
            params.extend([start_ts, end_ts])
        else:
            return []
    else:
        return []

    order_col = "ts"
    if "tickid" in cols:
        order_col = "tickid"
    elif "startts" in cols:
        order_col = "startts"
    elif "firstts" in cols:
        order_col = "firstts"
    elif "centerts" in cols:
        order_col = "centerts"
    elif "repts" in cols:
        order_col = "repts"

    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT *
            FROM public.tpivots
            WHERE {' AND '.join(where)}
            ORDER BY {order_col} ASC, id ASC
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _get_tzones_for_day(conn, day_id: int) -> List[Dict[str, Any]]:
    if not table_exists(conn, "tzone"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.tzone
            WHERE dayid = %s
            ORDER BY centerts ASC, id ASC
            """,
            (int(day_id),),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _get_tzones_for_time_range(conn, start_ts: datetime, end_ts: datetime) -> List[Dict[str, Any]]:
    if not table_exists(conn, "tzone"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.tzone
            WHERE endts >= %s
              AND startts <= %s
            ORDER BY startts ASC, id ASC
            """,
            (start_ts, end_ts),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _get_tepisodes_for_day(conn, day_id: int) -> List[Dict[str, Any]]:
    if not table_exists(conn, "tepisode"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.tepisode
            WHERE dayid = %s
            ORDER BY firstts ASC, id ASC
            """,
            (int(day_id),),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def _get_tepisodes_for_time_range(conn, start_ts: datetime, end_ts: datetime) -> List[Dict[str, Any]]:
    if not table_exists(conn, "tepisode"):
        return []

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.tepisode
            WHERE lastts >= %s
              AND firstts <= %s
            ORDER BY firstts ASC, id ASC
            """,
            (start_ts, end_ts),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def list_structure_days(conn, limit: int = 60) -> List[Dict[str, Any]]:
    if not table_exists(conn, "days"):
        return []

    lim = max(1, min(int(limit), 365))
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT
                id,
                startid,
                endid,
                startts,
                endts,
                (startts::date)::text AS daydate
            FROM public.days
            ORDER BY id DESC
            LIMIT %s
            """,
            (lim,),
        )
        rows = cur.fetchall()
    return [_jsonable_db(dict(r)) for r in rows]


def get_structure_day(conn, *, day_id: int, include_ticks: bool = False) -> Dict[str, Any]:
    if not table_exists(conn, "days"):
        return {"day": None, "ticks": [], "pivots": [], "tpivots": [], "tzone": [], "tepisode": []}

    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, startid, endid, startts, endts, (startts::date)::text AS daydate
            FROM public.days
            WHERE id = %s
            """,
            (int(day_id),),
        )
        day_row = cur.fetchone()

    if not day_row:
        return {"day": None, "ticks": [], "pivots": [], "tpivots": [], "tzone": [], "tepisode": []}

    day_info = _jsonable_db(dict(day_row))
    start_id = int(day_row["startid"])
    end_id = int(day_row["endid"])
    start_ts = day_row["startts"]
    end_ts = day_row["endts"]

    ticks = _fetch_ticks_by_id_range(conn, start_id, end_id) if include_ticks else []
    pivots = _get_pivots_for_day(conn, int(day_id))
    tpivots = _fetch_optional_tpivots(conn, day_id=int(day_id), start_ts=start_ts, end_ts=end_ts)
    tzones = _get_tzones_for_day(conn, int(day_id))
    tepisodes = _get_tepisodes_for_day(conn, int(day_id))

    return {
        "mode": "day",
        "day": day_info,
        "range": {
            "startid": start_id,
            "endid": end_id,
            "startts": _jsonable_db(start_ts),
            "endts": _jsonable_db(end_ts),
        },
        "ticks": ticks,
        "pivots": pivots,
        "tpivots": tpivots,
        "tzone": tzones,
        "tepisode": tepisodes,
    }


def get_structure_window(conn, *, from_id: int, window: int, include_ticks: bool = False) -> Dict[str, Any]:
    start_id = int(from_id)
    end_id = int(from_id) + max(1, int(window)) - 1

    meta = _get_tick_range_meta(conn, start_id, end_id)
    if not meta:
        return {
            "mode": "window",
            "range": {"startid": start_id, "endid": end_id, "tickcount": 0},
            "days": [],
            "ticks": [],
            "pivots": [],
            "tpivots": [],
            "tzone": [],
            "tepisode": [],
        }

    ticks = _fetch_ticks_by_id_range(conn, start_id, end_id) if include_ticks else []
    pivots = _get_pivots_for_tick_range(conn, start_id, end_id)
    tpivots = _fetch_optional_tpivots(
        conn,
        start_id=start_id,
        end_id=end_id,
        start_ts=meta["startts"],
        end_ts=meta["endts"],
    )
    tzones = _get_tzones_for_time_range(conn, meta["startts"], meta["endts"])
    tepisodes = _get_tepisodes_for_time_range(conn, meta["startts"], meta["endts"])
    days = _get_overlapping_days(conn, start_id, end_id)

    return {
        "mode": "window",
        "range": _jsonable_db(meta),
        "days": days,
        "ticks": ticks,
        "pivots": pivots,
        "tpivots": tpivots,
        "tzone": tzones,
        "tepisode": tepisodes,
    }


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
