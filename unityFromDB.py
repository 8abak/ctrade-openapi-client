#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import signal
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL
from jobs.unity_core import UnityConfig, UnityEngine


STOP = False
ROOT = Path(__file__).resolve().parent


def db_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def handle_signal(_sig, _frame):
    global STOP
    STOP = True


def ensure_tables_exist(conn):
    required = {
        "unitystate",
        "unitypivot",
        "unityswing",
        "unitytick",
        "unitysignal",
        "unitytrade",
        "unityevent",
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name = ANY(%s)
            """,
            (list(required),),
        )
        have = {row[0] for row in cur.fetchall()}
    missing = sorted(required - have)
    if missing:
        raise RuntimeError(
            "Missing UNITY tables: "
            + ", ".join(missing)
            + ". Apply sql/2026-03-21-create-unity.sql first."
        )


def load_config(path: Optional[str], symbol: str) -> UnityConfig:
    if not path:
        return UnityConfig(symbol=symbol)
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    raw["symbol"] = raw.get("symbol", symbol)
    return UnityConfig(**raw)


def load_engine(conn, config: UnityConfig) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT symbol, tickid, time, mode, status, payload
            FROM public.unitystate
            WHERE symbol=%s
            """,
            (config.symbol,),
        )
        row = cur.fetchone()
    if not row:
        return {"tickid": 0, "engine": UnityEngine(config=config), "mode": "idle", "status": "idle"}
    payload = row["payload"] or {}
    engine = UnityEngine(config=config, state=payload)
    return {
        "tickid": int(row["tickid"] or 0),
        "engine": engine,
        "mode": row["mode"] or "live",
        "status": row["status"] or "idle",
    }


def save_engine(conn, *, symbol: str, tickid: int, ticktime, mode: str, status: str, engine: UnityEngine):
    payload = json.dumps(engine.export_state(), separators=(",", ":"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.unitystate (symbol, tickid, time, mode, status, payload, updated)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, now())
            ON CONFLICT (symbol) DO UPDATE SET
                tickid = EXCLUDED.tickid,
                time = EXCLUDED.time,
                mode = EXCLUDED.mode,
                status = EXCLUDED.status,
                payload = EXCLUDED.payload,
                updated = now()
            """,
            (symbol, int(tickid), ticktime, mode, status, payload),
        )


def fetch_batch(conn, *, symbol: str, after_id: int, limit: int, to_id: Optional[int]):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if to_id is None:
            cur.execute(
                """
                SELECT id, timestamp, bid, ask, mid, spread
                FROM public.ticks
                WHERE symbol=%s
                  AND id>%s
                ORDER BY id ASC
                LIMIT %s
                """,
                (symbol, int(after_id), int(limit)),
            )
        else:
            cur.execute(
                """
                SELECT id, timestamp, bid, ask, mid, spread
                FROM public.ticks
                WHERE symbol=%s
                  AND id>%s
                  AND id<=%s
                ORDER BY id ASC
                LIMIT %s
                """,
                (symbol, int(after_id), int(to_id), int(limit)),
            )
        return cur.fetchall()


def fetch_head_id(conn, *, symbol: str, to_id: Optional[int] = None) -> int:
    with conn.cursor() as cur:
        if to_id is None:
            cur.execute("SELECT COALESCE(MAX(id), 0) FROM public.ticks WHERE symbol=%s", (symbol,))
        else:
            cur.execute(
                "SELECT COALESCE(MAX(id), 0) FROM public.ticks WHERE symbol=%s AND id<=%s",
                (symbol, int(to_id)),
            )
        return int(cur.fetchone()[0] or 0)


def delete_symbol_data(conn, *, symbol: str, from_id: Optional[int]):
    clauses = ["symbol=%s"]
    params: List[Any] = [symbol]
    if from_id is not None:
        clauses.append("tickid >= %s")
        params.append(int(from_id))
    tick_where = " AND ".join(clauses)

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM public.unityevent WHERE {tick_where.replace('tickid', 'COALESCE(tickid, signaltickid)')}", tuple(params))
        cur.execute(f"DELETE FROM public.unitytrade WHERE {tick_where.replace('tickid', 'signaltickid')}", tuple(params))
        cur.execute(f"DELETE FROM public.unitysignal WHERE {tick_where}", tuple(params))
        cur.execute(f"DELETE FROM public.unitytick WHERE {tick_where}", tuple(params))
        cur.execute(f"DELETE FROM public.unityswing WHERE {tick_where.replace('tickid', 'endtick')}", tuple(params))
        cur.execute(f"DELETE FROM public.unitypivot WHERE {tick_where}", tuple(params))
        if from_id is None:
            cur.execute("DELETE FROM public.unitystate WHERE symbol=%s", (symbol,))
        else:
            cur.execute(
                """
                UPDATE public.unitystate
                SET tickid = LEAST(tickid, %s),
                    time = NULL,
                    status = 'reset',
                    payload = '{}'::jsonb,
                    updated = now()
                WHERE symbol = %s
                """,
                (int(from_id) - 1, symbol),
            )


def apply_tick_rows(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    vals = [
        (
            row["symbol"],
            int(row["tickid"]),
            row["time"],
            float(row["price"]),
            float(row["spread"]),
            float(row["noise"]),
            float(row["thresh"]),
            int(row["legtick"]),
            int(row["legdir"]),
            float(row["legeff"]),
            float(row["legmultiple"]),
            float(row["causalscore"]),
            str(row["causalstate"]),
            int(row["causalzone"]),
            str(row["cleanstate"]),
            int(row["cleanzone"]),
            int(row["swingtick"]) if row["swingtick"] is not None else None,
            float(row["cleanconviction"]),
            row["revised"],
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unitytick (
                symbol, tickid, time, price, spread, noise, thresh, legtick, legdir,
                legeff, legmultiple, causalscore, causalstate, causalzone,
                cleanstate, cleanzone, swingtick, cleanconviction, revised
            )
            VALUES %s
            ON CONFLICT (symbol, tickid) DO UPDATE SET
                time = EXCLUDED.time,
                price = EXCLUDED.price,
                spread = EXCLUDED.spread,
                noise = EXCLUDED.noise,
                thresh = EXCLUDED.thresh,
                legtick = EXCLUDED.legtick,
                legdir = EXCLUDED.legdir,
                legeff = EXCLUDED.legeff,
                legmultiple = EXCLUDED.legmultiple,
                causalscore = EXCLUDED.causalscore,
                causalstate = EXCLUDED.causalstate,
                causalzone = EXCLUDED.causalzone,
                cleanstate = EXCLUDED.cleanstate,
                cleanzone = EXCLUDED.cleanzone,
                swingtick = EXCLUDED.swingtick,
                cleanconviction = EXCLUDED.cleanconviction,
                revised = EXCLUDED.revised
            """,
            vals,
            page_size=min(1000, len(vals)),
        )


def apply_pivots(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    vals = [
        (
            row["symbol"],
            int(row["tickid"]),
            row["time"],
            float(row["price"]),
            str(row["kind"]),
            float(row["noise"]),
            float(row["thresh"]),
            str(row["state"]),
            int(row["legtick"]),
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unitypivot (
                symbol, tickid, time, price, kind, noise, thresh, state, legtick
            )
            VALUES %s
            ON CONFLICT (symbol, tickid, kind) DO NOTHING
            """,
            vals,
            page_size=min(1000, len(vals)),
        )


def replace_swings(conn, *, symbol: str, dirty_from: Optional[int], rows: List[Dict[str, Any]]):
    if dirty_from is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.unityswing WHERE symbol=%s AND endtick >= %s",
            (symbol, int(dirty_from)),
        )
    if not rows:
        return
    vals = [
        (
            row["symbol"],
            int(row["starttick"]),
            int(row["endtick"]),
            row["starttime"],
            row["endtime"],
            float(row["startprice"]),
            float(row["endprice"]),
            str(row["state"]),
            int(row["ticks"]),
            float(row["move"]),
            float(row["efficiency"]),
            float(row["multiple"]),
            float(row["conviction"]),
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unityswing (
                symbol, starttick, endtick, starttime, endtime, startprice, endprice,
                state, ticks, move, efficiency, multiple, conviction
            )
            VALUES %s
            """,
            vals,
            page_size=min(1000, len(vals)),
        )


def apply_signals(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    vals = [
        (
            row["symbol"],
            int(row["tickid"]),
            row["time"],
            str(row["side"]),
            str(row["state"]),
            float(row["price"]),
            float(row["score"]),
            bool(row["favored"]),
            str(row["reason"]),
            json.dumps(row["detail"], separators=(",", ":")),
            json.dumps(row["context"], separators=(",", ":")),
            bool(row["used"]),
            row["skipreason"],
            str(row["status"]),
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unitysignal (
                symbol, tickid, time, side, state, price, score, favored,
                reason, detail, context, used, skipreason, status
            )
            VALUES %s
            ON CONFLICT (symbol, tickid, side) DO UPDATE SET
                state = EXCLUDED.state,
                price = EXCLUDED.price,
                score = EXCLUDED.score,
                favored = EXCLUDED.favored,
                reason = EXCLUDED.reason,
                detail = EXCLUDED.detail,
                context = EXCLUDED.context,
                used = EXCLUDED.used,
                skipreason = EXCLUDED.skipreason,
                status = EXCLUDED.status
            """,
            vals,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s)",
            page_size=min(1000, len(vals)),
        )


def apply_trades(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    vals = [
        (
            row["symbol"],
            int(row["signaltickid"]),
            str(row["side"]),
            str(row["state"]),
            int(row["opentick"]),
            row["opentime"],
            float(row["openprice"]),
            int(row["pivottickid"]),
            float(row["pivotprice"]),
            float(row["buffer"]),
            float(row["risk"]),
            float(row["stopprice"]),
            float(row["targetprice"]),
            bool(row["bearmed"]),
            bool(row["trailarmed"]),
            float(row["bestprice"]) if row.get("bestprice") is not None else None,
            float(row["bestfavor"]),
            float(row["bestadverse"]),
            row.get("closetick"),
            row.get("closetime"),
            float(row["closeprice"]) if row.get("closeprice") is not None else None,
            float(row["pnl"]) if row.get("pnl") is not None else None,
            row.get("exitreason"),
            str(row["status"]),
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unitytrade (
                symbol, signaltickid, side, state, opentick, opentime, openprice,
                pivottickid, pivotprice, buffer, risk, stopprice, targetprice,
                bearmed, trailarmed, bestprice, bestfavor, bestadverse,
                closetick, closetime, closeprice, pnl, exitreason, status
            )
            VALUES %s
            ON CONFLICT (symbol, signaltickid) DO UPDATE SET
                state = EXCLUDED.state,
                stopprice = EXCLUDED.stopprice,
                targetprice = EXCLUDED.targetprice,
                bearmed = EXCLUDED.bearmed,
                trailarmed = EXCLUDED.trailarmed,
                bestprice = EXCLUDED.bestprice,
                bestfavor = EXCLUDED.bestfavor,
                bestadverse = EXCLUDED.bestadverse,
                closetick = EXCLUDED.closetick,
                closetime = EXCLUDED.closetime,
                closeprice = EXCLUDED.closeprice,
                pnl = EXCLUDED.pnl,
                exitreason = EXCLUDED.exitreason,
                status = EXCLUDED.status
            """,
            vals,
            page_size=min(1000, len(vals)),
        )


def apply_events(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    vals = [
        (
            row["symbol"],
            int(row["signaltickid"]),
            int(row["tickid"]),
            row["time"],
            str(row["kind"]),
            float(row["price"]) if row.get("price") is not None else None,
            float(row["stopprice"]) if row.get("stopprice") is not None else None,
            float(row["targetprice"]) if row.get("targetprice") is not None else None,
            str(row["reason"]),
            json.dumps(row["detail"], separators=(",", ":")),
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unityevent (
                symbol, signaltickid, tickid, time, kind, price, stopprice, targetprice, reason, detail
            )
            VALUES %s
            """,
            vals,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
            page_size=min(1000, len(vals)),
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Incremental UNITY labeler, signaler, and paper journal.")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--mode", choices=["backfill", "live"], default="backfill")
    p.add_argument("--fromid", type=int, default=None)
    p.add_argument("--toid", type=int, default=None)
    p.add_argument("--batch", type=int, default=2000)
    p.add_argument("--idle", type=float, default=1.0)
    p.add_argument("--reset", action="store_true")
    p.add_argument("--once", action="store_true")
    p.add_argument("--config", default=None, help="Optional JSON config file for UnityConfig fields.")
    return p.parse_args()


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    args = parse_args()
    config = load_config(args.config, args.symbol)
    conn = db_connect()
    ensure_tables_exist(conn)

    if args.reset:
        delete_symbol_data(conn, symbol=config.symbol, from_id=args.fromid)
        conn.commit()

    loaded = load_engine(conn, config)
    engine: UnityEngine = loaded["engine"]
    last_id = loaded["tickid"]
    if args.fromid is not None:
        last_id = max(last_id, int(args.fromid) - 1) if not args.reset else int(args.fromid) - 1

    processed_since = 0
    signal_since = 0
    trade_since = 0
    stats_at = time.time()

    print(
        f"unity start symbol={config.symbol} mode={args.mode} from_id={last_id} to_id={args.toid}",
        flush=True,
    )

    try:
        while not STOP:
            rows = fetch_batch(
                conn,
                symbol=config.symbol,
                after_id=last_id,
                limit=args.batch,
                to_id=args.toid,
            )
            if not rows:
                if args.mode == "backfill" or args.once:
                    break
                time.sleep(max(0.2, args.idle))
                now = time.time()
                if now - stats_at >= 5.0:
                    head = fetch_head_id(conn, symbol=config.symbol, to_id=args.toid)
                    behind = max(0, head - last_id)
                    rate = processed_since / max(1e-6, now - stats_at)
                    print(
                        f"unity stats rate={rate:.1f}/s signals={signal_since} trades={trade_since} last_id={last_id} behind={behind}",
                        flush=True,
                    )
                    processed_since = 0
                    signal_since = 0
                    trade_since = 0
                    stats_at = now
                continue

            batch_start = time.time()
            for row in rows:
                engine.process_tick(dict(row))

            changes = engine.drain_changes()
            apply_tick_rows(conn, changes["ticks"])
            apply_pivots(conn, changes["pivots"])
            replace_swings(conn, symbol=config.symbol, dirty_from=changes["swingdirtyfrom"], rows=changes["swings"])
            apply_signals(conn, changes["signals"])
            apply_trades(conn, changes["trades"])
            apply_events(conn, changes["events"])

            last_tick = rows[-1]
            last_id = int(last_tick["id"])
            save_engine(
                conn,
                symbol=config.symbol,
                tickid=last_id,
                ticktime=last_tick["timestamp"],
                mode=args.mode,
                status="running" if args.mode == "live" else "done",
                engine=engine,
            )
            conn.commit()

            processed_since += len(rows)
            signal_since += len(changes["signals"])
            trade_since += sum(1 for row in changes["trades"] if row["status"] == "closed")

            now = time.time()
            if now - stats_at >= 5.0:
                rate = processed_since / max(1e-6, now - stats_at)
                head = fetch_head_id(conn, symbol=config.symbol, to_id=args.toid)
                behind = max(0, head - last_id)
                print(
                    f"unity stats processed={processed_since} rate={rate:.1f}/s signals={signal_since} closed={trade_since} last_id={last_id} behind={behind} batch_ms={(time.time() - batch_start) * 1000.0:.2f}",
                    flush=True,
                )
                processed_since = 0
                signal_since = 0
                trade_since = 0
                stats_at = now

            if args.once:
                break
            if args.mode == "backfill" and args.toid is not None and last_id >= int(args.toid):
                break

    finally:
        try:
            save_engine(
                conn,
                symbol=config.symbol,
                tickid=last_id,
                ticktime=None,
                mode=args.mode,
                status="stopped" if STOP else "idle",
                engine=engine,
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        conn.close()

    print(f"unity stopped last_id={last_id}", flush=True)


if __name__ == "__main__":
    main()
