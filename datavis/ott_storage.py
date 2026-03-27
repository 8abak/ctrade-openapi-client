from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

import psycopg2.extras

from datavis.db import db_connection
from datavis.ott import DEFAULT_OTT_SIGNAL_MODE, OttConfig, run_ott_backtest, signal_columns


DEFAULT_SYMBOL = "XAUUSD"
SYDNEY_TZ = ZoneInfo("Australia/Sydney")


def select_tick_price_expr(source: str) -> str:
    source = source.lower()
    if source == "ask":
        return "t.ask"
    if source == "bid":
        return "t.bid"
    return "COALESCE(t.mid, ROUND(((t.bid + t.ask) / 2.0)::numeric, 2)::double precision)"


def fetch_tick_batch_after(
    symbol: str,
    after_id: int,
    limit: int,
    *,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if end_id is None:
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM public.ticks
                    WHERE symbol = %s AND id > %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (symbol, after_id, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM public.ticks
                    WHERE symbol = %s AND id > %s AND id <= %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (symbol, after_id, end_id, limit),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_tick_id_bounds(symbol: str) -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT MIN(id) AS firstid, MAX(id) AS lastid,
                       MIN(timestamp) AS firstts, MAX(timestamp) AS lastts
                FROM public.ticks
                WHERE symbol = %s
                """,
                (symbol,),
            )
            row = dict(cur.fetchone() or {})
    return row


def resolve_last_week_range(symbol: str, days: int = 7) -> Dict[str, Any]:
    now_ts = datetime.now(tz=SYDNEY_TZ)
    start_cutoff = now_ts - timedelta(days=days)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND timestamp >= %s
                ORDER BY id ASC
                LIMIT 1
                """,
                (symbol, start_cutoff),
            )
            start_row = cur.fetchone()
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND timestamp <= %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (symbol, now_ts),
            )
            end_row = cur.fetchone()

    if not start_row or not end_row:
        raise RuntimeError("No ticks available for the requested backfill range.")

    return {
        "starttickid": int(start_row["id"]),
        "endtickid": int(end_row["id"]),
        "startts": start_row["timestamp"],
        "endts": end_row["timestamp"],
        "days": days,
    }


def fetch_ott_rows_for_tick_ids(symbol: str, tick_ids: Sequence[int], config: OttConfig) -> Dict[int, Dict[str, Any]]:
    if not tick_ids:
        return {}
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT tickid, timestamp, price, mavg, fark, longstop, shortstop, dir, mt, ott, ott2, ott3,
                       supportbuy, supportsell, pricebuy, pricesell, colorbuy, colorsell
                FROM public.otttick
                WHERE symbol = %s
                  AND source = %s
                  AND matype = %s
                  AND length = %s
                  AND percent = %s
                  AND tickid = ANY(%s)
                ORDER BY tickid ASC
                """,
                (symbol, config.source, config.matype, config.length, config.percent, list(tick_ids)),
            )
            return {int(row["tickid"]): dict(row) for row in cur.fetchall()}


def fetch_ott_storage_bounds(symbol: str, config: OttConfig) -> Dict[str, Any]:
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    MIN(tickid) AS firsttickid,
                    MAX(tickid) AS lasttickid,
                    COUNT(*)::bigint AS rowcount,
                    MIN(timestamp) AS firstts,
                    MAX(timestamp) AS lastts
                FROM public.otttick
                WHERE symbol = %s
                  AND source = %s
                  AND matype = %s
                  AND length = %s
                  AND percent = %s
                """,
                (symbol, config.source, config.matype, config.length, config.percent),
            )
            row = dict(cur.fetchone() or {})
    return {
        "firsttickid": int(row["firsttickid"]) if row.get("firsttickid") is not None else None,
        "lasttickid": int(row["lasttickid"]) if row.get("lasttickid") is not None else None,
        "rowcount": int(row.get("rowcount") or 0),
        "firstts": row.get("firstts"),
        "lastts": row.get("lastts"),
    }


def save_ott_rows(rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    values = [
        (
            row["tickid"],
            row["symbol"],
            row["source"],
            row["matype"],
            row["length"],
            row["percent"],
            row["timestamp"],
            row["price"],
            row["mavg"],
            row["fark"],
            row["longstop"],
            row["shortstop"],
            row["dir"],
            row["mt"],
            row["ott"],
            row["ott2"],
            row["ott3"],
            row["supportbuy"],
            row["supportsell"],
            row["pricebuy"],
            row["pricesell"],
            row["colorbuy"],
            row["colorsell"],
        )
        for row in rows
    ]

    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO public.otttick (
                    tickid, symbol, source, matype, length, percent, timestamp, price, mavg, fark,
                    longstop, shortstop, dir, mt, ott, ott2, ott3, supportbuy, supportsell,
                    pricebuy, pricesell, colorbuy, colorsell
                )
                VALUES %s
                ON CONFLICT (tickid, symbol, source, matype, length, percent)
                DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    price = EXCLUDED.price,
                    mavg = EXCLUDED.mavg,
                    fark = EXCLUDED.fark,
                    longstop = EXCLUDED.longstop,
                    shortstop = EXCLUDED.shortstop,
                    dir = EXCLUDED.dir,
                    mt = EXCLUDED.mt,
                    ott = EXCLUDED.ott,
                    ott2 = EXCLUDED.ott2,
                    ott3 = EXCLUDED.ott3,
                    supportbuy = EXCLUDED.supportbuy,
                    supportsell = EXCLUDED.supportsell,
                    pricebuy = EXCLUDED.pricebuy,
                    pricesell = EXCLUDED.pricesell,
                    colorbuy = EXCLUDED.colorbuy,
                    colorsell = EXCLUDED.colorsell,
                    updatedat = NOW()
                """,
                values,
                page_size=min(1000, len(values)),
            )
        conn.commit()
    return len(values)


def load_job_state(symbol: str, config: OttConfig) -> Optional[Dict[str, Any]]:
    config = config.normalized()
    job_name = config.job_name(symbol)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, jobname, symbol, source, matype, length, percent, lasttickid, lastts, statejson
                FROM public.ottjobstate
                WHERE jobname = %s
                """,
                (job_name,),
            )
            row = cur.fetchone()
    if not row:
        return None
    payload = dict(row)
    state_json = payload.get("statejson")
    if isinstance(state_json, str):
        payload["statejson"] = json.loads(state_json)
    return payload


def save_job_state(
    symbol: str,
    config: OttConfig,
    lasttickid: int,
    lastts: Any,
    statejson: Dict[str, Any],
) -> None:
    config = config.normalized()
    job_name = config.job_name(symbol)
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.ottjobstate (
                    jobname, symbol, source, matype, length, percent, lasttickid, lastts, statejson
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (jobname)
                DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    source = EXCLUDED.source,
                    matype = EXCLUDED.matype,
                    length = EXCLUDED.length,
                    percent = EXCLUDED.percent,
                    lasttickid = EXCLUDED.lasttickid,
                    lastts = EXCLUDED.lastts,
                    statejson = EXCLUDED.statejson,
                    updatedat = NOW()
                """,
                (
                    job_name,
                    symbol,
                    config.source,
                    config.matype,
                    config.length,
                    config.percent,
                    lasttickid,
                    lastts,
                    json.dumps(statejson),
                ),
            )
        conn.commit()


def fetch_backtest_rows(symbol: str, config: OttConfig, start_tick_id: int, end_tick_id: int) -> List[Dict[str, Any]]:
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    o.tickid,
                    o.timestamp,
                    o.price,
                    o.mavg,
                    o.fark,
                    o.longstop,
                    o.shortstop,
                    o.dir,
                    o.mt,
                    o.ott,
                    o.ott2,
                    o.ott3,
                    o.supportbuy,
                    o.supportsell,
                    o.pricebuy,
                    o.pricesell,
                    o.colorbuy,
                    o.colorsell,
                    t.bid,
                    t.ask,
                    t.mid,
                    t.spread
                FROM public.otttick o
                JOIN public.ticks t
                  ON t.id = o.tickid
                WHERE o.symbol = %s
                  AND o.source = %s
                  AND o.matype = %s
                  AND o.length = %s
                  AND o.percent = %s
                  AND o.tickid BETWEEN %s AND %s
                ORDER BY o.tickid ASC
                """,
                (symbol, config.source, config.matype, config.length, config.percent, start_tick_id, end_tick_id),
            )
            return [dict(row) for row in cur.fetchall()]


def fetch_bootstrap_tick_rows(symbol: str, mode: str, start_id: Optional[int], window: int) -> List[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if mode == "live":
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM (
                        SELECT id, symbol, timestamp, bid, ask, mid, spread
                        FROM public.ticks
                        WHERE symbol = %s
                        ORDER BY id DESC
                        LIMIT %s
                    ) recent
                    ORDER BY id ASC
                    """,
                    (symbol, window),
                )
            else:
                if start_id is None:
                    raise RuntimeError("Review mode requires an id value.")
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM public.ticks
                    WHERE symbol = %s AND id >= %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (symbol, start_id, window),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_next_tick_rows(symbol: str, after_id: int, limit: int) -> List[Dict[str, Any]]:
    return fetch_tick_batch_after(symbol, after_id, limit, end_id=None)


def fetch_ott_sync_diagnostics(
    symbol: str,
    config: OttConfig,
    *,
    requested_start_tick_id: Optional[int] = None,
    requested_end_tick_id: Optional[int] = None,
    signalmode: Optional[str] = None,
) -> Dict[str, Any]:
    config = config.normalized()
    storage = fetch_ott_storage_bounds(symbol, config)
    job_state = load_job_state(symbol, config)

    latest_stored_tick_id = storage.get("lasttickid")
    requested_last_tick_id = requested_end_tick_id
    requested_first_tick_id = requested_start_tick_id
    requested_gap_count = 0
    if latest_stored_tick_id is not None and requested_last_tick_id is not None:
        requested_gap_count = max(0, int(requested_last_tick_id) - int(latest_stored_tick_id))

    signal_counts = None
    if signalmode and requested_first_tick_id is not None and requested_last_tick_id is not None:
        counts = fetch_signal_counts(symbol, config, signalmode, requested_first_tick_id, requested_last_tick_id)
        signal_counts = {
            "buyCount": int(counts.get("buycount") or 0),
            "sellCount": int(counts.get("sellcount") or 0),
            "totalCount": int(counts.get("buycount") or 0) + int(counts.get("sellcount") or 0),
            "signalMode": signalmode.lower(),
        }

    return {
        "storage": {
            "firstTickId": storage.get("firsttickid"),
            "lastTickId": latest_stored_tick_id,
            "rowCount": storage.get("rowcount"),
            "firstTs": storage.get("firstts"),
            "lastTs": storage.get("lastts"),
        },
        "jobState": {
            "lastTickId": int(job_state["lasttickid"]) if job_state and job_state.get("lasttickid") is not None else None,
            "lastTs": job_state.get("lastts") if job_state else None,
        },
        "requested": {
            "firstTickId": requested_first_tick_id,
            "lastTickId": requested_last_tick_id,
            "gapCountAheadOfStorage": requested_gap_count,
        },
        "latestStoredTickId": latest_stored_tick_id,
        "signalCounts": signal_counts,
    }


def find_existing_backtest_run(
    symbol: str,
    config: OttConfig,
    signalmode: str,
    start_tick_id: int,
    end_tick_id: int,
) -> Optional[Dict[str, Any]]:
    config = config.normalized()
    signalmode = (signalmode or DEFAULT_OTT_SIGNAL_MODE).lower()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, symbol, source, matype, length, percent, signalmode,
                       starttickid, endtickid, startts, endts, tradecount, grosspnl, netpnl, createdat
                FROM public.ottbacktestrun
                WHERE symbol = %s
                  AND source = %s
                  AND matype = %s
                  AND length = %s
                  AND percent = %s
                  AND signalmode = %s
                  AND starttickid = %s
                  AND endtickid = %s
                ORDER BY createdat DESC, id DESC
                LIMIT 1
                """,
                (symbol, config.source, config.matype, config.length, config.percent, signalmode, start_tick_id, end_tick_id),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def persist_backtest_run(
    symbol: str,
    config: OttConfig,
    signalmode: str,
    start_tick_id: int,
    end_tick_id: int,
    start_ts: Any,
    end_ts: Any,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    config = config.normalized()
    signalmode = (signalmode or DEFAULT_OTT_SIGNAL_MODE).lower()
    trades = payload.get("trades", [])
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.ottbacktestrun (
                    symbol, source, matype, length, percent, signalmode,
                    starttickid, endtickid, startts, endts, tradecount, grosspnl, netpnl
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, createdat
                """,
                (
                    symbol,
                    config.source,
                    config.matype,
                    config.length,
                    config.percent,
                    signalmode,
                    start_tick_id,
                    end_tick_id,
                    start_ts,
                    end_ts,
                    payload["tradecount"],
                    payload["grosspnl"],
                    payload["netpnl"],
                ),
            )
            run_id, created_at = cur.fetchone()

            if trades:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO public.ottbacktesttrade (
                        runid, entrytickid, exittickid, entryts, exitts, direction,
                        entryprice, exitprice, pnl, pnlpoints, barsorticksheld,
                        signalentrytype, signalexittype
                    )
                    VALUES %s
                    """,
                    [
                        (
                            run_id,
                            trade["entrytickid"],
                            trade["exittickid"],
                            trade["entryts"],
                            trade["exitts"],
                            trade["direction"],
                            trade["entryprice"],
                            trade["exitprice"],
                            trade["pnl"],
                            trade["pnlpoints"],
                            trade["barsorticksheld"],
                            trade["signalentrytype"],
                            trade["signalexittype"],
                        )
                        for trade in trades
                    ],
                    page_size=min(1000, len(trades)),
                )
        conn.commit()

    return {
        "id": run_id,
        "symbol": symbol,
        "source": config.source,
        "matype": config.matype,
        "length": config.length,
        "percent": config.percent,
        "signalmode": signalmode,
        "starttickid": start_tick_id,
        "endtickid": end_tick_id,
        "startts": start_ts,
        "endts": end_ts,
        "tradecount": payload["tradecount"],
        "grosspnl": payload["grosspnl"],
        "netpnl": payload["netpnl"],
        "createdat": created_at,
    }


def run_and_store_backtest(
    symbol: str,
    config: OttConfig,
    signalmode: str,
    start_tick_id: int,
    end_tick_id: int,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    signal_columns(signalmode)
    existing = find_existing_backtest_run(symbol, config, signalmode, start_tick_id, end_tick_id)
    if existing and not force:
        existing["reused"] = True
        return existing

    rows = fetch_backtest_rows(symbol, config, start_tick_id, end_tick_id)
    if not rows:
        raise RuntimeError("No stored OTT rows found for the requested backtest range.")
    payload = run_ott_backtest(rows, signalmode=signalmode)
    return persist_backtest_run(
        symbol=symbol,
        config=config,
        signalmode=signalmode,
        start_tick_id=start_tick_id,
        end_tick_id=end_tick_id,
        start_ts=rows[0]["timestamp"],
        end_ts=rows[-1]["timestamp"],
        payload=payload,
    )


def fetch_backtest_overlay(
    symbol: str,
    config: OttConfig,
    signalmode: str,
    run_start_tick_id: int,
    run_end_tick_id: int,
    visible_start_tick_id: Optional[int] = None,
    visible_end_tick_id: Optional[int] = None,
) -> Dict[str, Any]:
    signal_columns(signalmode)
    run = find_existing_backtest_run(symbol, config, signalmode, run_start_tick_id, run_end_tick_id)
    if not run:
        return {"run": None, "trades": [], "tradecount": 0}

    trade_start = visible_start_tick_id if visible_start_tick_id is not None else run_start_tick_id
    trade_end = visible_end_tick_id if visible_end_tick_id is not None else run_end_tick_id

    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, runid, entrytickid, exittickid, entryts, exitts, direction,
                       entryprice, exitprice, pnl, pnlpoints, barsorticksheld,
                       signalentrytype, signalexittype, createdat
                FROM public.ottbacktesttrade
                WHERE runid = %s
                  AND exittickid >= %s
                  AND entrytickid <= %s
                ORDER BY entrytickid ASC, id ASC
                """,
                (run["id"], trade_start, trade_end),
            )
            trades = [dict(row) for row in cur.fetchall()]

    return {"run": run, "trades": trades, "tradecount": len(trades)}


def fetch_signal_counts(symbol: str, config: OttConfig, signalmode: str, start_tick_id: int, end_tick_id: int) -> Dict[str, int]:
    config = config.normalized()
    buy_column, sell_column = signal_columns(signalmode)
    with db_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN {buy_column} THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN {sell_column} THEN 1 ELSE 0 END), 0)
                FROM public.otttick
                WHERE symbol = %s
                  AND source = %s
                  AND matype = %s
                  AND length = %s
                  AND percent = %s
                  AND tickid BETWEEN %s AND %s
                """.format(buy_column=buy_column, sell_column=sell_column),
                (symbol, config.source, config.matype, config.length, config.percent, start_tick_id, end_tick_id),
            )
            buy_count, sell_count = cur.fetchone()
    return {"buycount": int(buy_count or 0), "sellcount": int(sell_count or 0)}
