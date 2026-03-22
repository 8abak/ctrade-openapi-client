#!/usr/bin/env python3
from __future__ import annotations

import argparse
import signal
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL
from jobs.unity_shadow import evaluate_candidate


STOP = False


def db_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def handle_signal(_sig, _frame):
    global STOP
    STOP = True


def ensure_tables_exist(conn):
    required = {
        "unitycandidate",
        "unitycandoutcome",
        "unitycandscenario",
        "unitytick",
        "ticks",
        "days",
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
            "Missing UNITY candidate tables: "
            + ", ".join(missing)
            + ". Apply sql/2026-03-22-create-unity-candidate.sql first."
        )


def fetch_unity_head(conn, *, symbol: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(tickid), 0) FROM public.unitytick WHERE symbol=%s",
            (symbol,),
        )
        return int(cur.fetchone()[0] or 0)


def fetch_unresolved_candidates(conn, *, symbol: str, limit: int) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.*
            FROM public.unitycandidate c
            LEFT JOIN public.unitycandoutcome o
              ON o.candidateid = c.id
            WHERE c.symbol=%s
              AND (
                    o.id IS NULL
                 OR o.status='unresolved'
                  )
            ORDER BY c.signaltickid ASC, c.id ASC
            LIMIT %s
            """,
            (symbol, int(limit)),
        )
        return cur.fetchall()


def fetch_day_end(conn, *, tickid: int) -> Optional[Tuple[int, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT endid, endts
            FROM public.days
            WHERE startid <= %s
              AND endid >= %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(tickid), int(tickid)),
        )
        row = cur.fetchone()
    if not row:
        return None
    return int(row["endid"]), row["endts"]


def fetch_future_rows(conn, *, symbol: str, signaltickid: int, maxid: int) -> List[Dict[str, Any]]:
    if int(maxid) <= int(signaltickid):
        return []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                t.id,
                t.timestamp,
                t.bid,
                t.ask,
                t.mid,
                u.causalstate
            FROM public.ticks t
            LEFT JOIN public.unitytick u
              ON u.symbol = t.symbol
             AND u.tickid = t.id
            WHERE t.symbol=%s
              AND t.id > %s
              AND t.id <= %s
            ORDER BY t.id ASC
            """,
            (symbol, int(signaltickid), int(maxid)),
        )
        return cur.fetchall()


def upsert_outcome(conn, *, candidateid: int, timeoutsec: int, outcome: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.unitycandoutcome (
                candidateid, timeoutsec, tpprice, slprice, firsthit,
                resolvetickid, resolvetime, resolveseconds,
                mfe, mae, bestfavor, bestadverse, pnl, wouldwin, status, updated
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (candidateid) DO UPDATE SET
                timeoutsec = EXCLUDED.timeoutsec,
                tpprice = EXCLUDED.tpprice,
                slprice = EXCLUDED.slprice,
                firsthit = EXCLUDED.firsthit,
                resolvetickid = EXCLUDED.resolvetickid,
                resolvetime = EXCLUDED.resolvetime,
                resolveseconds = EXCLUDED.resolveseconds,
                mfe = EXCLUDED.mfe,
                mae = EXCLUDED.mae,
                bestfavor = EXCLUDED.bestfavor,
                bestadverse = EXCLUDED.bestadverse,
                pnl = EXCLUDED.pnl,
                wouldwin = EXCLUDED.wouldwin,
                status = EXCLUDED.status,
                updated = now()
            """,
            (
                int(candidateid),
                int(timeoutsec),
                outcome.get("tpprice"),
                outcome.get("slprice"),
                outcome["firsthit"],
                outcome.get("resolvetickid"),
                outcome.get("resolvetime"),
                outcome.get("resolveseconds"),
                outcome.get("mfe"),
                outcome.get("mae"),
                outcome.get("bestfavor"),
                outcome.get("bestadverse"),
                outcome.get("pnl"),
                outcome.get("wouldwin"),
                outcome["status"],
            ),
        )


def upsert_scenarios(conn, *, candidateid: int, timeoutsec: int, scenarios: List[Dict[str, Any]]):
    if not scenarios:
        return
    vals = [
        (
            int(candidateid),
            str(row["code"]),
            int(timeoutsec),
            float(row["tpmult"]),
            float(row["slmult"]),
            row.get("tpprice"),
            row.get("slprice"),
            str(row["firsthit"]),
            row.get("resolvetickid"),
            row.get("resolvetime"),
            row.get("resolveseconds"),
            row.get("mfe"),
            row.get("mae"),
            row.get("bestfavor"),
            row.get("bestadverse"),
            row.get("pnl"),
            row.get("wouldwin"),
            str(row["status"]),
        )
        for row in scenarios
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.unitycandscenario (
                candidateid, code, timeoutsec, tpmult, slmult,
                tpprice, slprice, firsthit, resolvetickid, resolvetime,
                resolveseconds, mfe, mae, bestfavor, bestadverse, pnl, wouldwin, status, updated
            )
            VALUES %s
            ON CONFLICT (candidateid, code) DO UPDATE SET
                timeoutsec = EXCLUDED.timeoutsec,
                tpmult = EXCLUDED.tpmult,
                slmult = EXCLUDED.slmult,
                tpprice = EXCLUDED.tpprice,
                slprice = EXCLUDED.slprice,
                firsthit = EXCLUDED.firsthit,
                resolvetickid = EXCLUDED.resolvetickid,
                resolvetime = EXCLUDED.resolvetime,
                resolveseconds = EXCLUDED.resolveseconds,
                mfe = EXCLUDED.mfe,
                mae = EXCLUDED.mae,
                bestfavor = EXCLUDED.bestfavor,
                bestadverse = EXCLUDED.bestadverse,
                pnl = EXCLUDED.pnl,
                wouldwin = EXCLUDED.wouldwin,
                status = EXCLUDED.status,
                updated = now()
            """,
            vals,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())",
            page_size=min(200, len(vals)),
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Resolve UNITY candidate baseline and scenario outcomes.")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--batch", type=int, default=200)
    p.add_argument("--idle", type=float, default=2.0)
    p.add_argument("--timeoutsec", type=int, default=900)
    p.add_argument("--once", action="store_true")
    return p.parse_args()


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    args = parse_args()
    conn = db_connect()
    ensure_tables_exist(conn)

    print(
        f"unityresolver start symbol={args.symbol} batch={args.batch} timeoutsec={args.timeoutsec}",
        flush=True,
    )

    processed_since = 0
    resolved_since = 0
    stats_at = time.time()

    try:
        while not STOP:
            try:
                headid = fetch_unity_head(conn, symbol=args.symbol)
                candidates = fetch_unresolved_candidates(conn, symbol=args.symbol, limit=args.batch)
                if not candidates:
                    if args.once:
                        break
                    time.sleep(max(0.5, args.idle))
                    now = time.time()
                    if now - stats_at >= 5.0:
                        print(
                            f"unityresolver stats processed={processed_since} resolved={resolved_since} headid={headid}",
                            flush=True,
                        )
                        processed_since = 0
                        resolved_since = 0
                        stats_at = now
                    continue

                batch_resolved = 0
                for candidate in candidates:
                    candidateid = int(candidate["id"])
                    signaltickid = int(candidate["signaltickid"])
                    day_info = fetch_day_end(conn, tickid=signaltickid)
                    dayendtickid = day_info[0] if day_info else None
                    maxid = headid if dayendtickid is None else min(headid, int(dayendtickid))
                    future_rows = fetch_future_rows(
                        conn,
                        symbol=args.symbol,
                        signaltickid=signaltickid,
                        maxid=maxid,
                    )
                    baseline, scenarios = evaluate_candidate(
                        dict(candidate),
                        future_rows,
                        timeoutsec=int(args.timeoutsec),
                        dayendtickid=dayendtickid,
                    )
                    upsert_outcome(
                        conn,
                        candidateid=candidateid,
                        timeoutsec=int(args.timeoutsec),
                        outcome=baseline,
                    )
                    upsert_scenarios(
                        conn,
                        candidateid=candidateid,
                        timeoutsec=int(args.timeoutsec),
                        scenarios=scenarios,
                    )
                    processed_since += 1
                    if baseline["status"] == "resolved":
                        batch_resolved += 1

                conn.commit()
                resolved_since += batch_resolved

                now = time.time()
                if now - stats_at >= 5.0:
                    print(
                        f"unityresolver stats processed={processed_since} resolved={resolved_since} headid={headid}",
                        flush=True,
                    )
                    processed_since = 0
                    resolved_since = 0
                    stats_at = now
                if args.once:
                    break
                time.sleep(0.05)
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(
                    f"unityresolver error: {exc}",
                    flush=True,
                )
                time.sleep(max(1.0, args.idle))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("unityresolver stopped", flush=True)


if __name__ == "__main__":
    main()
